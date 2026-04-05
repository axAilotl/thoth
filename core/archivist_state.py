"""Durable archivist topic state and dirty checking."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Mapping

from .archivist_selection import ArchivistCandidate
from .archivist_topics import ArchivistTopicDefinition
from .metadata_db import MetadataDB, get_metadata_db

ARCHIVIST_STATE_KEY_PREFIX = "archivist.topic."


class ArchivistTopicStateError(ValueError):
    """Raised when archivist topic state is missing or malformed."""


@dataclass(frozen=True)
class ArchivistSourceSnapshot:
    """Deterministic snapshot of a selected source set."""

    source_keys: tuple[str, ...]
    source_hashes: dict[str, str]
    fingerprint: str
    candidate_count: int


@dataclass(frozen=True)
class ArchivistTopicState:
    """Durable state for a single archivist topic."""

    topic_id: str
    last_run_at: str | None = None
    last_success_at: str | None = None
    last_source_keys: tuple[str, ...] = ()
    last_source_hashes: dict[str, str] | None = None
    last_source_fingerprint: str | None = None
    last_candidate_count: int = 0
    last_model_provider: str | None = None
    last_model: str | None = None
    force_requested_at: str | None = None
    force_reason: str | None = None

    def __post_init__(self) -> None:
        if self.last_source_hashes is None:
            object.__setattr__(self, "last_source_hashes", {})


@dataclass(frozen=True)
class ArchivistDirtyCheckResult:
    """Decision about whether a topic should be recompiled."""

    should_run: bool
    reason: str
    forced: bool
    dirty: bool
    due: bool
    next_due_at: str | None
    snapshot: ArchivistSourceSnapshot
    model_provider: str | None
    model: str | None


def archivist_topic_state_key(topic_id: str) -> str:
    return f"{ARCHIVIST_STATE_KEY_PREFIX}{topic_id}"


def load_archivist_topic_state(
    topic_id: str,
    *,
    db: MetadataDB | None = None,
) -> ArchivistTopicState:
    """Load durable archivist state, failing closed on malformed payloads."""

    metadata_db = db or get_metadata_db()
    payload = metadata_db.get_automation_state(archivist_topic_state_key(topic_id))
    if payload is None:
        return ArchivistTopicState(topic_id=topic_id)
    return _parse_archivist_state(topic_id, payload)


def request_archivist_topic_force(
    topic: ArchivistTopicDefinition,
    *,
    db: MetadataDB | None = None,
    requested_at: str | None = None,
    reason: str | None = None,
) -> ArchivistTopicState:
    """Persist a manual force request for a topic."""

    if not topic.allow_manual_force:
        raise ArchivistTopicStateError(
            f"Manual force is disabled for archivist topic {topic.id}"
        )

    metadata_db = db or get_metadata_db()
    existing = load_archivist_topic_state(topic.id, db=metadata_db)
    updated = ArchivistTopicState(
        topic_id=topic.id,
        last_run_at=existing.last_run_at,
        last_success_at=existing.last_success_at,
        last_source_keys=existing.last_source_keys,
        last_source_hashes=dict(existing.last_source_hashes or {}),
        last_source_fingerprint=existing.last_source_fingerprint,
        last_candidate_count=existing.last_candidate_count,
        last_model_provider=existing.last_model_provider,
        last_model=existing.last_model,
        force_requested_at=requested_at or _now_iso(),
        force_reason=reason,
    )
    _store_archivist_topic_state(updated, db=metadata_db)
    return updated


def clear_archivist_topic_force(
    topic_id: str,
    *,
    db: MetadataDB | None = None,
) -> ArchivistTopicState:
    """Clear a pending manual force request without mutating other state."""

    metadata_db = db or get_metadata_db()
    existing = load_archivist_topic_state(topic_id, db=metadata_db)
    cleared = ArchivistTopicState(
        topic_id=topic_id,
        last_run_at=existing.last_run_at,
        last_success_at=existing.last_success_at,
        last_source_keys=existing.last_source_keys,
        last_source_hashes=dict(existing.last_source_hashes or {}),
        last_source_fingerprint=existing.last_source_fingerprint,
        last_candidate_count=existing.last_candidate_count,
        last_model_provider=existing.last_model_provider,
        last_model=existing.last_model,
        force_requested_at=None,
        force_reason=None,
    )
    _store_archivist_topic_state(cleared, db=metadata_db)
    return cleared


def snapshot_archivist_candidates(
    candidates: tuple[ArchivistCandidate, ...] | list[ArchivistCandidate],
) -> ArchivistSourceSnapshot:
    """Build a deterministic snapshot of selected archivist candidates."""

    normalized = sorted(
        (
            {
                "candidate_key": candidate.candidate_key,
                "source_hash": candidate.source_hash,
            }
            for candidate in candidates
        ),
        key=lambda item: item["candidate_key"],
    )
    source_keys = tuple(item["candidate_key"] for item in normalized)
    source_hashes = {
        item["candidate_key"]: item["source_hash"]
        for item in normalized
    }
    fingerprint = hashlib.sha256(
        json.dumps(normalized, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return ArchivistSourceSnapshot(
        source_keys=source_keys,
        source_hashes=source_hashes,
        fingerprint=fingerprint,
        candidate_count=len(normalized),
    )


def evaluate_archivist_dirty_check(
    topic: ArchivistTopicDefinition,
    candidates: tuple[ArchivistCandidate, ...] | list[ArchivistCandidate],
    *,
    route: tuple[str, str, dict[str, Any]] | None,
    db: MetadataDB | None = None,
    now: datetime | None = None,
) -> ArchivistDirtyCheckResult:
    """Decide whether a topic should run based on state, cadence, and sources."""

    metadata_db = db or get_metadata_db()
    state = load_archivist_topic_state(topic.id, db=metadata_db)
    snapshot = snapshot_archivist_candidates(tuple(candidates))
    provider = route[0] if route else None
    model = route[1] if route else None
    next_due_at = _compute_next_due_at(state.last_success_at, topic.cadence_hours)
    now_dt = now or datetime.now(timezone.utc)

    if state.force_requested_at:
        return ArchivistDirtyCheckResult(
            should_run=True,
            reason="manual_force",
            forced=True,
            dirty=False,
            due=False,
            next_due_at=next_due_at,
            snapshot=snapshot,
            model_provider=provider,
            model=model,
        )

    if state.last_success_at is None:
        return ArchivistDirtyCheckResult(
            should_run=True,
            reason="initial_run",
            forced=False,
            dirty=False,
            due=False,
            next_due_at=next_due_at,
            snapshot=snapshot,
            model_provider=provider,
            model=model,
        )

    if snapshot.fingerprint != state.last_source_fingerprint:
        return ArchivistDirtyCheckResult(
            should_run=True,
            reason="sources_changed",
            forced=False,
            dirty=True,
            due=False,
            next_due_at=next_due_at,
            snapshot=snapshot,
            model_provider=provider,
            model=model,
        )

    if provider != state.last_model_provider or model != state.last_model:
        return ArchivistDirtyCheckResult(
            should_run=True,
            reason="route_changed",
            forced=False,
            dirty=True,
            due=False,
            next_due_at=next_due_at,
            snapshot=snapshot,
            model_provider=provider,
            model=model,
        )

    if next_due_at is not None:
        due_dt = _parse_datetime(next_due_at)
        compare_now, compare_due = _normalize_datetime_pair(now_dt, due_dt)
        if compare_now >= compare_due:
            return ArchivistDirtyCheckResult(
                should_run=True,
                reason="cadence_due",
                forced=False,
                dirty=False,
                due=True,
                next_due_at=next_due_at,
                snapshot=snapshot,
                model_provider=provider,
                model=model,
            )

    return ArchivistDirtyCheckResult(
        should_run=False,
        reason="up_to_date",
        forced=False,
        dirty=False,
        due=False,
        next_due_at=next_due_at,
        snapshot=snapshot,
        model_provider=provider,
        model=model,
    )


def record_archivist_topic_run(
    topic: ArchivistTopicDefinition,
    candidates: tuple[ArchivistCandidate, ...] | list[ArchivistCandidate],
    *,
    route: tuple[str, str, dict[str, Any]] | None,
    db: MetadataDB | None = None,
    run_at: str | None = None,
    succeeded: bool = True,
) -> ArchivistTopicState:
    """Persist a completed archivist topic run."""

    metadata_db = db or get_metadata_db()
    existing = load_archivist_topic_state(topic.id, db=metadata_db)
    snapshot = snapshot_archivist_candidates(tuple(candidates))
    provider = route[0] if route else None
    model = route[1] if route else None
    recorded_at = run_at or _now_iso()

    updated = ArchivistTopicState(
        topic_id=topic.id,
        last_run_at=recorded_at,
        last_success_at=recorded_at if succeeded else existing.last_success_at,
        last_source_keys=snapshot.source_keys,
        last_source_hashes=dict(snapshot.source_hashes),
        last_source_fingerprint=snapshot.fingerprint,
        last_candidate_count=snapshot.candidate_count,
        last_model_provider=provider,
        last_model=model,
        force_requested_at=None,
        force_reason=None,
    )
    _store_archivist_topic_state(updated, db=metadata_db)
    return updated


def _store_archivist_topic_state(
    state: ArchivistTopicState,
    *,
    db: MetadataDB,
) -> None:
    db.upsert_automation_state(
        archivist_topic_state_key(state.topic_id),
        {
            "topic_id": state.topic_id,
            "last_run_at": state.last_run_at,
            "last_success_at": state.last_success_at,
            "last_source_keys": list(state.last_source_keys),
            "last_source_hashes": dict(state.last_source_hashes or {}),
            "last_source_fingerprint": state.last_source_fingerprint,
            "last_candidate_count": state.last_candidate_count,
            "last_model_provider": state.last_model_provider,
            "last_model": state.last_model,
            "force_requested_at": state.force_requested_at,
            "force_reason": state.force_reason,
        },
    )


def _parse_archivist_state(
    topic_id: str,
    payload: Mapping[str, Any],
) -> ArchivistTopicState:
    if not isinstance(payload, Mapping):
        raise ArchivistTopicStateError(
            f"Archivist state for {topic_id} must be a JSON object"
        )

    raw_topic_id = payload.get("topic_id")
    if raw_topic_id and str(raw_topic_id) != topic_id:
        raise ArchivistTopicStateError(
            f"Archivist state topic id mismatch: expected {topic_id}, got {raw_topic_id}"
        )

    raw_source_keys = payload.get("last_source_keys") or []
    if not isinstance(raw_source_keys, list):
        raise ArchivistTopicStateError(
            f"Archivist state last_source_keys for {topic_id} must be a list"
        )

    raw_source_hashes = payload.get("last_source_hashes") or {}
    if not isinstance(raw_source_hashes, dict):
        raise ArchivistTopicStateError(
            f"Archivist state last_source_hashes for {topic_id} must be an object"
        )

    raw_candidate_count = payload.get("last_candidate_count", 0)
    try:
        candidate_count = int(raw_candidate_count)
    except (TypeError, ValueError) as exc:
        raise ArchivistTopicStateError(
            f"Archivist state last_candidate_count for {topic_id} must be an integer"
        ) from exc

    return ArchivistTopicState(
        topic_id=topic_id,
        last_run_at=_optional_text(payload.get("last_run_at")),
        last_success_at=_optional_text(payload.get("last_success_at")),
        last_source_keys=tuple(str(item) for item in raw_source_keys),
        last_source_hashes={str(key): str(value) for key, value in raw_source_hashes.items()},
        last_source_fingerprint=_optional_text(payload.get("last_source_fingerprint")),
        last_candidate_count=candidate_count,
        last_model_provider=_optional_text(payload.get("last_model_provider")),
        last_model=_optional_text(payload.get("last_model")),
        force_requested_at=_optional_text(payload.get("force_requested_at")),
        force_reason=_optional_text(payload.get("force_reason")),
    )


def _compute_next_due_at(last_success_at: str | None, cadence_hours: float) -> str | None:
    if last_success_at is None:
        return None
    base = _parse_datetime(last_success_at)
    return (base + timedelta(hours=float(cadence_hours))).isoformat()


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ArchivistTopicStateError(f"Invalid archivist timestamp: {value}") from exc


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_datetime_pair(left: datetime, right: datetime) -> tuple[datetime, datetime]:
    if left.tzinfo is None and right.tzinfo is None:
        return left, right
    if left.tzinfo is None:
        left = left.replace(tzinfo=timezone.utc)
    if right.tzinfo is None:
        right = right.replace(tzinfo=timezone.utc)
    return left, right
