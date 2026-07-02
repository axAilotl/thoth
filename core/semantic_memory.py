"""Semantic memory candidate store.

This module stores reviewable memory candidates and their evidence only. It does
not promote candidates into durable wiki facts.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .metadata_db import (
    MetadataDB,
    SEMANTIC_MEMORY_CANDIDATE_STATUSES,
    SEMANTIC_MEMORY_CANDIDATE_TYPES,
    get_metadata_db,
)
from .semantic_memory_promotion import (
    SEMANTIC_MEMORY_PROMOTION_METADATA_KEY,
    SemanticMemoryPromotionConfigError,
    SemanticMemoryPromotionDecision,
    SemanticMemoryPromotionPolicy,
    metadata_flag_enabled,
    semantic_text_fingerprint,
)


JsonObject = dict[str, Any]
SEMANTIC_MEMORY_ALLOWED_CANDIDATE_TYPES = tuple(SEMANTIC_MEMORY_CANDIDATE_TYPES)
SEMANTIC_MEMORY_ALLOWED_STATUSES = tuple(SEMANTIC_MEMORY_CANDIDATE_STATUSES)

_ALLOWED_STATUS_TRANSITIONS: dict[str, frozenset[str]] = {
    "proposed": frozenset({"confirmed", "rejected", "promoted", "superseded"}),
    "confirmed": frozenset({"rejected", "promoted", "superseded"}),
    "rejected": frozenset(),
    "promoted": frozenset(),
    "superseded": frozenset(),
}


class SemanticMemoryError(ValueError):
    """Base error for semantic memory persistence failures."""


class SemanticMemoryValidationError(SemanticMemoryError):
    """Raised when candidate or evidence input is invalid."""


class SemanticMemoryTransitionError(SemanticMemoryValidationError):
    """Raised when a candidate type or status transition is not allowed."""


def _new_id() -> str:
    return str(uuid.uuid4())


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_required(value: Any, field_name: str) -> str:
    text = _clean_optional(value)
    if not text:
        raise SemanticMemoryValidationError(f"{field_name} is required")
    return text


def _validate_candidate_type(value: Any) -> str:
    candidate_type = _clean_required(value, "candidate_type")
    if candidate_type not in SEMANTIC_MEMORY_ALLOWED_CANDIDATE_TYPES:
        allowed = ", ".join(SEMANTIC_MEMORY_ALLOWED_CANDIDATE_TYPES)
        raise SemanticMemoryValidationError(
            f"candidate_type must be one of: {allowed}"
        )
    return candidate_type


def _validate_status(value: Any) -> str:
    status = _clean_required(value, "status")
    if status not in SEMANTIC_MEMORY_ALLOWED_STATUSES:
        allowed = ", ".join(SEMANTIC_MEMORY_ALLOWED_STATUSES)
        raise SemanticMemoryValidationError(f"status must be one of: {allowed}")
    return status


def _validate_confidence(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    try:
        confidence = float(value)
    except (TypeError, ValueError) as exc:
        raise SemanticMemoryValidationError(
            f"{field_name} must be a number between 0 and 1"
        ) from exc
    if confidence < 0.0 or confidence > 1.0:
        raise SemanticMemoryValidationError(
            f"{field_name} must be a number between 0 and 1"
        )
    return confidence


def _json_object(value: Mapping[str, Any] | None, field_name: str) -> JsonObject:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise SemanticMemoryValidationError(f"{field_name} must be a JSON object")
    return dict(value)


def _json_param(value: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(value or {}), ensure_ascii=False, sort_keys=True)


def _read_json_object(value: Any, field_name: str) -> JsonObject:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError as exc:
        raise SemanticMemoryValidationError(
            f"{field_name} must contain a JSON object"
        ) from exc
    if not isinstance(payload, Mapping):
        raise SemanticMemoryValidationError(f"{field_name} must contain a JSON object")
    return dict(payload)


def _validated_limit(value: int | None) -> int | None:
    if value is None:
        return None
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise SemanticMemoryValidationError("limit must be a positive integer") from exc
    if limit < 1:
        raise SemanticMemoryValidationError("limit must be a positive integer")
    return limit


def _validate_status_transition(current_status: str, requested_status: str) -> None:
    if current_status == requested_status:
        return
    allowed = _ALLOWED_STATUS_TRANSITIONS.get(current_status, frozenset())
    if requested_status not in allowed:
        raise SemanticMemoryTransitionError(
            f"cannot transition semantic memory candidate from "
            f"{current_status!r} to {requested_status!r}"
        )


@dataclass(frozen=True)
class SemanticMemoryCandidate:
    """Reviewable semantic memory candidate."""

    candidate_type: str
    text: str
    candidate_id: str = field(default_factory=_new_id)
    status: str = "proposed"
    subject: str = ""
    predicate: str = ""
    object_value: str = ""
    entity_id: str | None = None
    entity_type: str | None = None
    entity_name: str | None = None
    confidence: float | None = None
    privacy_class: str = "unspecified"
    supersedes_candidate_id: str | None = None
    superseded_by_candidate_id: str | None = None
    metadata: JsonObject = field(default_factory=dict)
    write_provenance: JsonObject = field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None
    status_updated_at: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "candidate_id", _clean_required(self.candidate_id, "candidate_id")
        )
        object.__setattr__(
            self, "candidate_type", _validate_candidate_type(self.candidate_type)
        )
        object.__setattr__(self, "status", _validate_status(self.status))
        object.__setattr__(self, "text", _clean_required(self.text, "text"))
        object.__setattr__(self, "subject", _clean_optional(self.subject) or "")
        object.__setattr__(self, "predicate", _clean_optional(self.predicate) or "")
        object.__setattr__(
            self, "object_value", _clean_optional(self.object_value) or ""
        )
        object.__setattr__(self, "entity_id", _clean_optional(self.entity_id))
        object.__setattr__(self, "entity_type", _clean_optional(self.entity_type))
        object.__setattr__(self, "entity_name", _clean_optional(self.entity_name))
        object.__setattr__(
            self, "privacy_class", _clean_optional(self.privacy_class) or "unspecified"
        )
        object.__setattr__(
            self,
            "supersedes_candidate_id",
            _clean_optional(self.supersedes_candidate_id),
        )
        object.__setattr__(
            self,
            "superseded_by_candidate_id",
            _clean_optional(self.superseded_by_candidate_id),
        )
        if self.supersedes_candidate_id == self.candidate_id:
            raise SemanticMemoryValidationError(
                "supersedes_candidate_id must not point to the same candidate"
            )
        if self.superseded_by_candidate_id == self.candidate_id:
            raise SemanticMemoryValidationError(
                "superseded_by_candidate_id must not point to the same candidate"
            )
        if self.status == "superseded" and not self.superseded_by_candidate_id:
            raise SemanticMemoryValidationError(
                "superseded candidates require superseded_by_candidate_id"
            )
        object.__setattr__(
            self, "confidence", _validate_confidence(self.confidence, "confidence")
        )
        object.__setattr__(
            self, "metadata", _json_object(self.metadata, "metadata")
        )
        object.__setattr__(
            self,
            "write_provenance",
            _json_object(self.write_provenance, "write_provenance"),
        )
        object.__setattr__(self, "created_at", _clean_optional(self.created_at))
        object.__setattr__(self, "updated_at", _clean_optional(self.updated_at))
        object.__setattr__(
            self, "status_updated_at", _clean_optional(self.status_updated_at)
        )


@dataclass(frozen=True)
class SemanticMemoryEvidence:
    """Evidence link supporting a semantic memory candidate."""

    candidate_id: str
    evidence_id: str = field(default_factory=_new_id)
    artifact_id: str | None = None
    artifact_type: str | None = None
    capture_event_id: str | None = None
    source_path: str | None = None
    source_timestamp: str | None = None
    evidence_text: str = ""
    confidence: float | None = None
    privacy_class: str = "unspecified"
    metadata: JsonObject = field(default_factory=dict)
    write_provenance: JsonObject = field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "evidence_id", _clean_required(self.evidence_id, "evidence_id")
        )
        object.__setattr__(
            self, "candidate_id", _clean_required(self.candidate_id, "candidate_id")
        )
        object.__setattr__(self, "artifact_id", _clean_optional(self.artifact_id))
        object.__setattr__(self, "artifact_type", _clean_optional(self.artifact_type))
        object.__setattr__(
            self, "capture_event_id", _clean_optional(self.capture_event_id)
        )
        object.__setattr__(self, "source_path", _clean_optional(self.source_path))
        object.__setattr__(
            self, "source_timestamp", _clean_optional(self.source_timestamp)
        )
        if not (self.artifact_id or self.capture_event_id or self.source_path):
            raise SemanticMemoryValidationError(
                "evidence requires artifact_id, capture_event_id, or source_path"
            )
        object.__setattr__(
            self, "evidence_text", _clean_optional(self.evidence_text) or ""
        )
        object.__setattr__(
            self, "confidence", _validate_confidence(self.confidence, "confidence")
        )
        object.__setattr__(
            self, "privacy_class", _clean_optional(self.privacy_class) or "unspecified"
        )
        object.__setattr__(
            self, "metadata", _json_object(self.metadata, "metadata")
        )
        object.__setattr__(
            self,
            "write_provenance",
            _json_object(self.write_provenance, "write_provenance"),
        )
        object.__setattr__(self, "created_at", _clean_optional(self.created_at))
        object.__setattr__(self, "updated_at", _clean_optional(self.updated_at))


class SemanticMemoryStore:
    """SQLite-backed repository for semantic memory candidates and evidence."""

    def __init__(
        self,
        db: MetadataDB | None = None,
        *,
        promotion_policy: SemanticMemoryPromotionPolicy | None = None,
    ):
        self.db = db or get_metadata_db()
        self.promotion_policy = (
            promotion_policy or SemanticMemoryPromotionPolicy.from_config()
        )
        self.db.ensure_semantic_memory_tables()

    def add_candidate(
        self,
        candidate: SemanticMemoryCandidate,
        *,
        evidence: Iterable[SemanticMemoryEvidence] = (),
    ) -> SemanticMemoryCandidate:
        """Insert a new candidate and optional evidence links."""
        evidence_items = tuple(evidence)
        candidate = self._candidate_with_timestamps(candidate, existing=None)
        with self.db._get_connection() as conn:
            self._validate_candidate_links(conn, candidate)
            self._validate_rejected_candidate_reappearance(
                conn,
                candidate,
                evidence_items,
            )
            if candidate.status == "promoted":
                promotion_decision = self._promotion_decision_in_connection(
                    conn,
                    candidate,
                    evidence_items=evidence_items,
                    explicit_status=candidate.status,
                )
                self._require_promotion_allowed(promotion_decision)
                candidate = self._candidate_with_promotion_audit(
                    candidate,
                    promotion_decision,
                )
            self._insert_candidate(conn, candidate)
            for evidence_item in evidence_items:
                evidence_item = self._evidence_for_candidate(
                    evidence_item,
                    candidate_id=candidate.candidate_id,
                )
                self._insert_evidence(conn, evidence_item)
            return self._get_candidate_in_connection(conn, candidate.candidate_id)

    def update_candidate(
        self,
        candidate: SemanticMemoryCandidate,
    ) -> SemanticMemoryCandidate:
        """Update candidate content while preserving immutable type semantics."""
        with self.db._get_connection() as conn:
            existing = self._get_candidate_in_connection(conn, candidate.candidate_id)
            if existing.candidate_type != candidate.candidate_type:
                raise SemanticMemoryTransitionError(
                    "semantic memory candidate_type is immutable"
                )
            if existing.status != candidate.status:
                raise SemanticMemoryTransitionError(
                    "semantic memory status changes require transition_candidate"
                )
            _validate_status_transition(existing.status, candidate.status)
            candidate = self._candidate_with_timestamps(candidate, existing=existing)
            self._validate_candidate_links(conn, candidate)
            if existing.status != "rejected":
                self._validate_rejected_candidate_reappearance(
                    conn,
                    candidate,
                    self._list_evidence_in_connection(conn, candidate.candidate_id),
                    exclude_candidate_id=candidate.candidate_id,
                )
            self._update_candidate_row(conn, candidate)
            return self._get_candidate_in_connection(conn, candidate.candidate_id)

    def transition_candidate(
        self,
        candidate_id: str,
        status: str,
        *,
        superseded_by_candidate_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        write_provenance: Mapping[str, Any] | None = None,
        transitioned_at: str | None = None,
    ) -> SemanticMemoryCandidate:
        """Move a candidate through an allowed review state transition."""
        candidate_id = _clean_required(candidate_id, "candidate_id")
        status = _validate_status(status)
        timestamp = _clean_optional(transitioned_at) or _now_iso()
        metadata_patch = _json_object(metadata, "metadata")
        provenance_patch = _json_object(write_provenance, "write_provenance")

        with self.db._get_connection() as conn:
            existing = self._get_candidate_in_connection(conn, candidate_id)
            _validate_status_transition(existing.status, status)
            superseded_by_candidate_id = _clean_optional(superseded_by_candidate_id)
            promotion_decision: SemanticMemoryPromotionDecision | None = None
            if existing.status != "promoted" and status == "promoted":
                promotion_decision = self._promotion_decision_in_connection(
                    conn,
                    existing,
                    explicit_status=existing.status,
                )
                self._require_promotion_allowed(promotion_decision)

            if status == "superseded":
                superseded_by_candidate_id = (
                    superseded_by_candidate_id or existing.superseded_by_candidate_id
                )
                if not superseded_by_candidate_id:
                    raise SemanticMemoryTransitionError(
                        "superseded candidates require superseded_by_candidate_id"
                    )
                if superseded_by_candidate_id == candidate_id:
                    raise SemanticMemoryTransitionError(
                        "superseded_by_candidate_id must not point to the same candidate"
                    )
                self._require_candidate_exists(conn, superseded_by_candidate_id)
            elif superseded_by_candidate_id:
                raise SemanticMemoryTransitionError(
                    "superseded_by_candidate_id is only valid for superseded status"
                )

            transition_record: JsonObject = {
                "from": existing.status,
                "to": status,
                "at": timestamp,
            }
            if provenance_patch:
                transition_record["write_provenance"] = provenance_patch
            next_metadata = {**existing.metadata, **metadata_patch}
            if promotion_decision:
                transition_record[SEMANTIC_MEMORY_PROMOTION_METADATA_KEY] = (
                    promotion_decision.to_metadata()
                )
                next_metadata[SEMANTIC_MEMORY_PROMOTION_METADATA_KEY] = (
                    promotion_decision.to_metadata()
                )
            existing_transition_history = existing.write_provenance.get(
                "status_transitions"
            )
            if not isinstance(existing_transition_history, list):
                existing_transition_history = []
            next_provenance = {
                **existing.write_provenance,
                "last_status_transition": transition_record,
                "status_transitions": [
                    *existing_transition_history,
                    transition_record,
                ],
            }
            conn.execute(
                """
                UPDATE semantic_memory_candidates
                SET status = ?,
                    superseded_by_candidate_id = ?,
                    metadata_json = ?,
                    write_provenance_json = ?,
                    updated_at = ?,
                    status_updated_at = ?
                WHERE candidate_id = ?
                """,
                (
                    status,
                    superseded_by_candidate_id,
                    _json_param(next_metadata),
                    _json_param(next_provenance),
                    timestamp,
                    timestamp,
                    candidate_id,
                ),
            )
            return self._get_candidate_in_connection(conn, candidate_id)

    def promote_candidate(
        self,
        candidate_id: str,
        *,
        metadata: Mapping[str, Any] | None = None,
        write_provenance: Mapping[str, Any] | None = None,
        promoted_at: str | None = None,
    ) -> SemanticMemoryCandidate:
        """Promote a candidate only when the configured evidence gate allows it."""
        return self.transition_candidate(
            candidate_id,
            "promoted",
            metadata=metadata,
            write_provenance=write_provenance,
            transitioned_at=promoted_at,
        )

    def evaluate_promotion(
        self,
        candidate_id: str,
    ) -> SemanticMemoryPromotionDecision:
        """Evaluate whether a candidate is eligible for semantic promotion."""
        candidate_id = _clean_required(candidate_id, "candidate_id")
        with self.db._get_connection() as conn:
            candidate = self._get_candidate_in_connection(conn, candidate_id)
            return self._promotion_decision_in_connection(
                conn,
                candidate,
                explicit_status=candidate.status,
            )

    def add_evidence(self, evidence: SemanticMemoryEvidence) -> SemanticMemoryEvidence:
        """Insert an evidence link for an existing candidate."""
        evidence = self._evidence_with_timestamps(evidence)
        with self.db._get_connection() as conn:
            self._insert_evidence(conn, evidence)
            return self._get_evidence_in_connection(conn, evidence.evidence_id)

    def get_candidate(self, candidate_id: str) -> SemanticMemoryCandidate | None:
        """Fetch a semantic memory candidate by ID."""
        candidate_id = _clean_required(candidate_id, "candidate_id")
        with self.db._get_connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM semantic_memory_candidates
                WHERE candidate_id = ?
                """,
                (candidate_id,),
            ).fetchone()
            return self._candidate_from_row(row) if row else None

    def list_candidates(
        self,
        *,
        candidate_type: str | None = None,
        status: str | None = None,
        entity_id: str | None = None,
        entity_type: str | None = None,
        artifact_id: str | None = None,
        artifact_type: str | None = None,
        capture_event_id: str | None = None,
        limit: int | None = None,
    ) -> tuple[SemanticMemoryCandidate, ...]:
        """List candidates filtered by candidate, entity, or evidence fields."""
        candidate_type = (
            _validate_candidate_type(candidate_type) if candidate_type else None
        )
        status = _validate_status(status) if status else None
        limit = _validated_limit(limit)

        joins: list[str] = []
        where: list[str] = []
        params: list[Any] = []
        if artifact_id or artifact_type or capture_event_id:
            joins.append(
                "JOIN semantic_memory_evidence AS e ON e.candidate_id = c.candidate_id"
            )
        if candidate_type:
            where.append("c.candidate_type = ?")
            params.append(candidate_type)
        if status:
            where.append("c.status = ?")
            params.append(status)
        if entity_id:
            where.append("c.entity_id = ?")
            params.append(_clean_required(entity_id, "entity_id"))
        if entity_type:
            where.append("c.entity_type = ?")
            params.append(_clean_required(entity_type, "entity_type"))
        if artifact_id:
            where.append("e.artifact_id = ?")
            params.append(_clean_required(artifact_id, "artifact_id"))
        if artifact_type:
            where.append("e.artifact_type = ?")
            params.append(_clean_required(artifact_type, "artifact_type"))
        if capture_event_id:
            where.append("e.capture_event_id = ?")
            params.append(_clean_required(capture_event_id, "capture_event_id"))

        sql = "SELECT DISTINCT c.* FROM semantic_memory_candidates AS c"
        if joins:
            sql += " " + " ".join(joins)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY c.updated_at DESC, c.candidate_id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        with self.db._get_connection() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
            return tuple(self._candidate_from_row(row) for row in rows)

    def list_evidence(
        self,
        *,
        candidate_id: str | None = None,
        artifact_id: str | None = None,
        artifact_type: str | None = None,
        capture_event_id: str | None = None,
        source_path: str | None = None,
        entity_id: str | None = None,
        entity_type: str | None = None,
        candidate_type: str | None = None,
        candidate_status: str | None = None,
        limit: int | None = None,
    ) -> tuple[SemanticMemoryEvidence, ...]:
        """List evidence links, including filters through candidate attributes."""
        candidate_type = (
            _validate_candidate_type(candidate_type) if candidate_type else None
        )
        candidate_status = (
            _validate_status(candidate_status) if candidate_status else None
        )
        limit = _validated_limit(limit)

        where: list[str] = []
        params: list[Any] = []
        if candidate_id:
            where.append("e.candidate_id = ?")
            params.append(_clean_required(candidate_id, "candidate_id"))
        if artifact_id:
            where.append("e.artifact_id = ?")
            params.append(_clean_required(artifact_id, "artifact_id"))
        if artifact_type:
            where.append("e.artifact_type = ?")
            params.append(_clean_required(artifact_type, "artifact_type"))
        if capture_event_id:
            where.append("e.capture_event_id = ?")
            params.append(_clean_required(capture_event_id, "capture_event_id"))
        if source_path:
            where.append("e.source_path = ?")
            params.append(_clean_required(source_path, "source_path"))
        if entity_id:
            where.append("c.entity_id = ?")
            params.append(_clean_required(entity_id, "entity_id"))
        if entity_type:
            where.append("c.entity_type = ?")
            params.append(_clean_required(entity_type, "entity_type"))
        if candidate_type:
            where.append("c.candidate_type = ?")
            params.append(candidate_type)
        if candidate_status:
            where.append("c.status = ?")
            params.append(candidate_status)

        sql = """
            SELECT e.*
            FROM semantic_memory_evidence AS e
            JOIN semantic_memory_candidates AS c
              ON c.candidate_id = e.candidate_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY COALESCE(e.source_timestamp, e.created_at) DESC, e.evidence_id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        with self.db._get_connection() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
            return tuple(self._evidence_from_row(row) for row in rows)

    def _candidate_with_timestamps(
        self,
        candidate: SemanticMemoryCandidate,
        *,
        existing: SemanticMemoryCandidate | None,
    ) -> SemanticMemoryCandidate:
        now = _now_iso()
        created_at = existing.created_at if existing else candidate.created_at or now
        updated_at = candidate.updated_at or now
        if existing and candidate.status == existing.status:
            status_updated_at = (
                candidate.status_updated_at or existing.status_updated_at or now
            )
        else:
            status_updated_at = candidate.status_updated_at or now
        return SemanticMemoryCandidate(
            candidate_id=candidate.candidate_id,
            candidate_type=candidate.candidate_type,
            status=candidate.status,
            subject=candidate.subject,
            predicate=candidate.predicate,
            object_value=candidate.object_value,
            text=candidate.text,
            entity_id=candidate.entity_id,
            entity_type=candidate.entity_type,
            entity_name=candidate.entity_name,
            confidence=candidate.confidence,
            privacy_class=candidate.privacy_class,
            supersedes_candidate_id=candidate.supersedes_candidate_id,
            superseded_by_candidate_id=candidate.superseded_by_candidate_id,
            metadata=candidate.metadata,
            write_provenance=candidate.write_provenance,
            created_at=created_at,
            updated_at=updated_at,
            status_updated_at=status_updated_at,
        )

    def _evidence_with_timestamps(
        self,
        evidence: SemanticMemoryEvidence,
    ) -> SemanticMemoryEvidence:
        now = _now_iso()
        return SemanticMemoryEvidence(
            evidence_id=evidence.evidence_id,
            candidate_id=evidence.candidate_id,
            artifact_id=evidence.artifact_id,
            artifact_type=evidence.artifact_type,
            capture_event_id=evidence.capture_event_id,
            source_path=evidence.source_path,
            source_timestamp=evidence.source_timestamp,
            evidence_text=evidence.evidence_text,
            confidence=evidence.confidence,
            privacy_class=evidence.privacy_class,
            metadata=evidence.metadata,
            write_provenance=evidence.write_provenance,
            created_at=evidence.created_at or now,
            updated_at=evidence.updated_at or now,
        )

    def _candidate_with_promotion_audit(
        self,
        candidate: SemanticMemoryCandidate,
        decision: SemanticMemoryPromotionDecision,
    ) -> SemanticMemoryCandidate:
        decision_metadata = decision.to_metadata()
        return SemanticMemoryCandidate(
            candidate_id=candidate.candidate_id,
            candidate_type=candidate.candidate_type,
            status=candidate.status,
            subject=candidate.subject,
            predicate=candidate.predicate,
            object_value=candidate.object_value,
            text=candidate.text,
            entity_id=candidate.entity_id,
            entity_type=candidate.entity_type,
            entity_name=candidate.entity_name,
            confidence=candidate.confidence,
            privacy_class=candidate.privacy_class,
            supersedes_candidate_id=candidate.supersedes_candidate_id,
            superseded_by_candidate_id=candidate.superseded_by_candidate_id,
            metadata={
                **candidate.metadata,
                SEMANTIC_MEMORY_PROMOTION_METADATA_KEY: decision_metadata,
            },
            write_provenance=candidate.write_provenance,
            created_at=candidate.created_at,
            updated_at=candidate.updated_at,
            status_updated_at=candidate.status_updated_at,
        )

    def _promotion_decision_in_connection(
        self,
        conn: sqlite3.Connection,
        candidate: SemanticMemoryCandidate,
        *,
        evidence_items: Iterable[SemanticMemoryEvidence] | None = None,
        explicit_status: str | None = None,
    ) -> SemanticMemoryPromotionDecision:
        evidence_tuple = (
            tuple(evidence_items)
            if evidence_items is not None
            else self._list_evidence_in_connection(conn, candidate.candidate_id)
        )
        source_keys = {
            self._evidence_source_key(evidence_item)
            for evidence_item in evidence_tuple
        }
        trusted_structured_input = self._is_trusted_structured_input(
            candidate,
            evidence_tuple,
        )
        explicitly_confirmed = explicit_status == "confirmed"
        if not evidence_tuple:
            reason = "missing_evidence"
            allowed = False
        elif explicitly_confirmed:
            reason = "explicit_confirmation"
            allowed = True
        elif trusted_structured_input:
            reason = "trusted_structured_input"
            allowed = True
        elif (
            len(evidence_tuple) >= self.promotion_policy.min_evidence_count
            and len(source_keys) >= self.promotion_policy.min_distinct_sources
        ):
            reason = "repeated_evidence"
            allowed = True
        else:
            reason = "insufficient_evidence"
            allowed = False
        return SemanticMemoryPromotionDecision(
            allowed=allowed,
            reason=reason,
            candidate_status=explicit_status or candidate.status,
            evidence_count=len(evidence_tuple),
            distinct_source_count=len(source_keys),
            min_evidence_count=self.promotion_policy.min_evidence_count,
            min_distinct_sources=self.promotion_policy.min_distinct_sources,
            explicitly_confirmed=explicitly_confirmed,
            trusted_structured_input=trusted_structured_input,
        )

    def _require_promotion_allowed(
        self,
        decision: SemanticMemoryPromotionDecision,
    ) -> None:
        if decision.allowed:
            return
        if decision.evidence_count == 0:
            raise SemanticMemoryTransitionError(
                "semantic memory promotion requires at least one evidence item; "
                "explicit confirmation and trusted structured input are only valid "
                "for candidates with durable evidence"
            )
        raise SemanticMemoryTransitionError(
            "semantic memory promotion requires explicit confirmation, "
            "trusted structured input, or repeated evidence; "
            f"found {decision.evidence_count} evidence item(s) from "
            f"{decision.distinct_source_count} distinct source(s), "
            f"thresholds are {decision.min_evidence_count} evidence item(s) "
            f"and {decision.min_distinct_sources} distinct source(s)"
        )

    def _is_trusted_structured_input(
        self,
        candidate: SemanticMemoryCandidate,
        evidence_items: tuple[SemanticMemoryEvidence, ...],
    ) -> bool:
        keys = self.promotion_policy.trusted_structured_metadata_keys
        if metadata_flag_enabled(candidate.metadata, keys):
            return True
        trusted_artifact_types = set(
            self.promotion_policy.trusted_structured_artifact_types
        )
        for evidence_item in evidence_items:
            if evidence_item.artifact_type in trusted_artifact_types:
                return True
            if metadata_flag_enabled(evidence_item.metadata, keys):
                return True
        return False

    def _validate_rejected_candidate_reappearance(
        self,
        conn: sqlite3.Connection,
        candidate: SemanticMemoryCandidate,
        evidence_items: tuple[SemanticMemoryEvidence, ...],
        *,
        exclude_candidate_id: str | None = None,
    ) -> None:
        rejected_candidates = self._equivalent_rejected_candidates(
            conn,
            candidate,
            exclude_candidate_id=exclude_candidate_id,
        )
        if not rejected_candidates:
            return
        new_source_keys = {
            self._evidence_source_key(evidence_item)
            for evidence_item in evidence_items
        }
        rejected_source_keys: set[str] = set()
        for rejected_candidate in rejected_candidates:
            rejected_source_keys.update(
                self._evidence_source_key(evidence_item)
                for evidence_item in self._list_evidence_in_connection(
                    conn,
                    rejected_candidate.candidate_id,
                )
            )
        if new_source_keys.difference(rejected_source_keys):
            return
        raise SemanticMemoryValidationError(
            "semantic memory candidate matches a rejected candidate and requires "
            "new evidence before reappearing"
        )

    def _evidence_for_candidate(
        self,
        evidence: SemanticMemoryEvidence,
        *,
        candidate_id: str,
    ) -> SemanticMemoryEvidence:
        if evidence.candidate_id != candidate_id:
            raise SemanticMemoryValidationError(
                "evidence candidate_id must match the candidate being inserted"
            )
        return self._evidence_with_timestamps(evidence)

    def _validate_candidate_links(
        self,
        conn: sqlite3.Connection,
        candidate: SemanticMemoryCandidate,
    ) -> None:
        if candidate.supersedes_candidate_id:
            self._require_candidate_exists(conn, candidate.supersedes_candidate_id)
        if candidate.superseded_by_candidate_id:
            self._require_candidate_exists(conn, candidate.superseded_by_candidate_id)

    def _equivalent_rejected_candidates(
        self,
        conn: sqlite3.Connection,
        candidate: SemanticMemoryCandidate,
        *,
        exclude_candidate_id: str | None = None,
    ) -> tuple[SemanticMemoryCandidate, ...]:
        fingerprint = self._candidate_fingerprint(candidate)
        excluded_id = _clean_optional(exclude_candidate_id)
        rows = conn.execute(
            """
            SELECT *
            FROM semantic_memory_candidates
            WHERE status = 'rejected'
              AND candidate_type = ?
            """,
            (candidate.candidate_type,),
        ).fetchall()
        return tuple(
            existing
            for existing in (self._candidate_from_row(row) for row in rows)
            if existing.candidate_id != excluded_id
            if self._candidate_fingerprint(existing) == fingerprint
        )

    def _candidate_fingerprint(
        self,
        candidate: SemanticMemoryCandidate,
    ) -> tuple[str, ...]:
        entity = candidate.entity_id or ":".join(
            part
            for part in (
                semantic_text_fingerprint(candidate.entity_type),
                semantic_text_fingerprint(candidate.entity_name),
            )
            if part
        )
        subject = semantic_text_fingerprint(candidate.subject)
        predicate = semantic_text_fingerprint(candidate.predicate)
        object_value = semantic_text_fingerprint(candidate.object_value)
        if subject and predicate and object_value:
            return (
                "triple",
                candidate.candidate_type,
                entity,
                subject,
                predicate,
                object_value,
            )
        return (
            "text",
            candidate.candidate_type,
            entity,
            semantic_text_fingerprint(candidate.text),
        )

    def _evidence_source_key(self, evidence: SemanticMemoryEvidence) -> str:
        if evidence.artifact_id:
            return f"artifact:{evidence.artifact_type or ''}:{evidence.artifact_id}"
        if evidence.capture_event_id:
            return f"capture_event:{evidence.capture_event_id}"
        if evidence.source_path:
            return f"source_path:{evidence.source_path}"
        return f"evidence:{evidence.evidence_id}"

    def _list_evidence_in_connection(
        self,
        conn: sqlite3.Connection,
        candidate_id: str,
    ) -> tuple[SemanticMemoryEvidence, ...]:
        rows = conn.execute(
            """
            SELECT *
            FROM semantic_memory_evidence
            WHERE candidate_id = ?
            """,
            (_clean_required(candidate_id, "candidate_id"),),
        ).fetchall()
        return tuple(self._evidence_from_row(row) for row in rows)

    def _require_candidate_exists(
        self,
        conn: sqlite3.Connection,
        candidate_id: str,
    ) -> None:
        row = conn.execute(
            """
            SELECT 1
            FROM semantic_memory_candidates
            WHERE candidate_id = ?
            """,
            (_clean_required(candidate_id, "candidate_id"),),
        ).fetchone()
        if row is None:
            raise SemanticMemoryValidationError(
                f"semantic memory candidate does not exist: {candidate_id}"
            )

    def _insert_candidate(
        self,
        conn: sqlite3.Connection,
        candidate: SemanticMemoryCandidate,
    ) -> None:
        conn.execute(
            """
            INSERT INTO semantic_memory_candidates (
                candidate_id,
                candidate_type,
                status,
                subject,
                predicate,
                object_text,
                text,
                entity_id,
                entity_type,
                entity_name,
                confidence,
                privacy_class,
                supersedes_candidate_id,
                superseded_by_candidate_id,
                metadata_json,
                write_provenance_json,
                created_at,
                updated_at,
                status_updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._candidate_params(candidate),
        )

    def _update_candidate_row(
        self,
        conn: sqlite3.Connection,
        candidate: SemanticMemoryCandidate,
    ) -> None:
        conn.execute(
            """
            UPDATE semantic_memory_candidates
            SET status = ?,
                subject = ?,
                predicate = ?,
                object_text = ?,
                text = ?,
                entity_id = ?,
                entity_type = ?,
                entity_name = ?,
                confidence = ?,
                privacy_class = ?,
                supersedes_candidate_id = ?,
                superseded_by_candidate_id = ?,
                metadata_json = ?,
                write_provenance_json = ?,
                updated_at = ?,
                status_updated_at = ?
            WHERE candidate_id = ?
            """,
            (
                candidate.status,
                candidate.subject,
                candidate.predicate,
                candidate.object_value,
                candidate.text,
                candidate.entity_id or "",
                candidate.entity_type or "",
                candidate.entity_name or "",
                candidate.confidence,
                candidate.privacy_class,
                candidate.supersedes_candidate_id,
                candidate.superseded_by_candidate_id,
                _json_param(candidate.metadata),
                _json_param(candidate.write_provenance),
                candidate.updated_at,
                candidate.status_updated_at,
                candidate.candidate_id,
            ),
        )

    def _candidate_params(
        self,
        candidate: SemanticMemoryCandidate,
    ) -> tuple[Any, ...]:
        return (
            candidate.candidate_id,
            candidate.candidate_type,
            candidate.status,
            candidate.subject,
            candidate.predicate,
            candidate.object_value,
            candidate.text,
            candidate.entity_id or "",
            candidate.entity_type or "",
            candidate.entity_name or "",
            candidate.confidence,
            candidate.privacy_class,
            candidate.supersedes_candidate_id,
            candidate.superseded_by_candidate_id,
            _json_param(candidate.metadata),
            _json_param(candidate.write_provenance),
            candidate.created_at,
            candidate.updated_at,
            candidate.status_updated_at,
        )

    def _insert_evidence(
        self,
        conn: sqlite3.Connection,
        evidence: SemanticMemoryEvidence,
    ) -> None:
        self._require_candidate_exists(conn, evidence.candidate_id)
        conn.execute(
            """
            INSERT INTO semantic_memory_evidence (
                evidence_id,
                candidate_id,
                artifact_id,
                artifact_type,
                capture_event_id,
                source_path,
                source_timestamp,
                evidence_text,
                confidence,
                privacy_class,
                metadata_json,
                write_provenance_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence.evidence_id,
                evidence.candidate_id,
                evidence.artifact_id,
                evidence.artifact_type,
                evidence.capture_event_id,
                evidence.source_path,
                evidence.source_timestamp,
                evidence.evidence_text,
                evidence.confidence,
                evidence.privacy_class,
                _json_param(evidence.metadata),
                _json_param(evidence.write_provenance),
                evidence.created_at,
                evidence.updated_at,
            ),
        )

    def _get_candidate_in_connection(
        self,
        conn: sqlite3.Connection,
        candidate_id: str,
    ) -> SemanticMemoryCandidate:
        row = conn.execute(
            """
            SELECT *
            FROM semantic_memory_candidates
            WHERE candidate_id = ?
            """,
            (_clean_required(candidate_id, "candidate_id"),),
        ).fetchone()
        if row is None:
            raise SemanticMemoryValidationError(
                f"semantic memory candidate does not exist: {candidate_id}"
            )
        return self._candidate_from_row(row)

    def _get_evidence_in_connection(
        self,
        conn: sqlite3.Connection,
        evidence_id: str,
    ) -> SemanticMemoryEvidence:
        row = conn.execute(
            """
            SELECT *
            FROM semantic_memory_evidence
            WHERE evidence_id = ?
            """,
            (_clean_required(evidence_id, "evidence_id"),),
        ).fetchone()
        if row is None:
            raise SemanticMemoryValidationError(
                f"semantic memory evidence does not exist: {evidence_id}"
            )
        return self._evidence_from_row(row)

    def _candidate_from_row(self, row: sqlite3.Row) -> SemanticMemoryCandidate:
        return SemanticMemoryCandidate(
            candidate_id=row["candidate_id"],
            candidate_type=row["candidate_type"],
            status=row["status"],
            subject=row["subject"],
            predicate=row["predicate"],
            object_value=row["object_text"],
            text=row["text"],
            entity_id=row["entity_id"] or None,
            entity_type=row["entity_type"] or None,
            entity_name=row["entity_name"] or None,
            confidence=row["confidence"],
            privacy_class=row["privacy_class"],
            supersedes_candidate_id=row["supersedes_candidate_id"],
            superseded_by_candidate_id=row["superseded_by_candidate_id"],
            metadata=_read_json_object(row["metadata_json"], "metadata_json"),
            write_provenance=_read_json_object(
                row["write_provenance_json"], "write_provenance_json"
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            status_updated_at=row["status_updated_at"],
        )

    def _evidence_from_row(self, row: sqlite3.Row) -> SemanticMemoryEvidence:
        return SemanticMemoryEvidence(
            evidence_id=row["evidence_id"],
            candidate_id=row["candidate_id"],
            artifact_id=row["artifact_id"],
            artifact_type=row["artifact_type"],
            capture_event_id=row["capture_event_id"],
            source_path=row["source_path"],
            source_timestamp=row["source_timestamp"],
            evidence_text=row["evidence_text"],
            confidence=row["confidence"],
            privacy_class=row["privacy_class"],
            metadata=_read_json_object(row["metadata_json"], "metadata_json"),
            write_provenance=_read_json_object(
                row["write_provenance_json"], "write_provenance_json"
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


__all__ = [
    "SEMANTIC_MEMORY_ALLOWED_CANDIDATE_TYPES",
    "SEMANTIC_MEMORY_ALLOWED_STATUSES",
    "SEMANTIC_MEMORY_PROMOTION_METADATA_KEY",
    "SemanticMemoryCandidate",
    "SemanticMemoryError",
    "SemanticMemoryEvidence",
    "SemanticMemoryPromotionConfigError",
    "SemanticMemoryPromotionDecision",
    "SemanticMemoryPromotionPolicy",
    "SemanticMemoryStore",
    "SemanticMemoryTransitionError",
    "SemanticMemoryValidationError",
]
