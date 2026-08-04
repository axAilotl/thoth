"""Pure review policy helpers for ingestion artifact queue rows."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .artifact_identity import native_id_candidates_for_artifact_type
from .config import config

INGESTION_ACTIVE_REVIEW_STATUSES = (
    "needs_review",
    "blocked",
    "failed",
)
INGESTION_CLOSED_REVIEW_STATUSES = (
    "reviewed",
    "rejected",
)
INGESTION_REVIEW_STATUSES = frozenset(
    (*INGESTION_ACTIVE_REVIEW_STATUSES, *INGESTION_CLOSED_REVIEW_STATUSES)
)
DEFAULT_INGESTION_REVIEW_PAYLOAD_BYTES = 50 * 1024 * 1024
SUPPORTED_INGESTION_ARTIFACT_TYPES = frozenset(
    {
        "tweet",
        "paper",
        "repository",
        "web_clipper",
        "markdown",
        "video",
        "transcript",
    }
)


def structural_review_for_ingestion(
    *,
    artifact_type: str,
    payload_json: str,
) -> dict[str, Any] | None:
    """Return review metadata when a queue payload should not enter processing."""
    try:
        payload = json.loads(payload_json)
    except Exception as exc:
        return {
            "category": "malformed_payload",
            "reason": "payload_json is not valid JSON",
            "error": f"artifact review required: malformed payload JSON: {exc}",
            "error_type": exc.__class__.__name__,
        }
    if not isinstance(payload, dict):
        return {
            "category": "malformed_payload",
            "reason": "payload_json must decode to an object",
            "error": (
                "artifact review required: payload_json must decode to an object, "
                f"got {payload.__class__.__name__}"
            ),
            "error_type": "InvalidPayloadType",
        }

    payload_limit = _ingestion_review_payload_limit_bytes()
    if payload_limit is not None:
        payload_size = len(payload_json.encode("utf-8"))
        if payload_size > payload_limit:
            return {
                "category": "oversized_payload",
                "reason": "queue payload exceeds review size limit",
                "error": (
                    "artifact review required: queue payload is oversized "
                    f"({payload_size} bytes > {payload_limit} bytes)"
                ),
                "error_type": "OversizedPayload",
                "metadata": {
                    "payload_size_bytes": payload_size,
                    "limit_bytes": payload_limit,
                },
            }

        for field, size_bytes in sorted(_declared_payload_sizes(payload).items()):
            if size_bytes > payload_limit:
                return {
                    "category": "oversized_payload",
                    "reason": f"{field} exceeds review size limit",
                    "error": (
                        "artifact review required: declared artifact payload is "
                        f"oversized ({field}={size_bytes} bytes > "
                        f"{payload_limit} bytes)"
                    ),
                    "error_type": "OversizedPayload",
                    "metadata": {
                        "declared_size_field": field,
                        "declared_size_bytes": size_bytes,
                        "limit_bytes": payload_limit,
                    },
                }

    normalized_type = str(artifact_type or "").strip().lower()
    if normalized_type not in SUPPORTED_INGESTION_ARTIFACT_TYPES:
        return {
            "category": "unsupported_artifact_type",
            "reason": f"unsupported artifact type {artifact_type!r}",
            "error": (
                "artifact review required: unsupported artifact type "
                f"{artifact_type!r}"
            ),
            "error_type": "UnsupportedArtifactType",
        }

    if not any(
        str(value or "").strip()
        for value in _artifact_id_candidates_for_type(normalized_type, payload)
    ):
        return {
            "category": "incomplete_payload",
            "reason": f"{normalized_type} payload is missing a native artifact id",
            "error": (
                "artifact review required: incomplete payload is missing a native "
                f"artifact id for {normalized_type}"
            ),
            "error_type": "IncompletePayload",
        }
    return None


def append_ingestion_review_event(
    review_json: str | None,
    *,
    action: str,
    status: str,
    reason: str,
    actor: str | None = None,
    previous_status: str | None = None,
    category: str | None = None,
    error: str | None = None,
    error_type: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    at: str | None = None,
) -> str:
    payload = _json_payload(review_json)
    state = payload.get("state")
    if not isinstance(state, dict):
        state = {}
    events = payload.get("events")
    if not isinstance(events, list):
        events = []

    event_at = at or datetime.now().isoformat()
    event = _compact_review_event(
        {
            "action": action,
            "actor": actor,
            "at": event_at,
            "from": previous_status,
            "to": status,
            "status": status,
            "category": category,
            "reason": reason,
            "error": {
                key: value
                for key, value in {
                    "type": error_type,
                    "message": error,
                }.items()
                if value
            },
            "metadata": dict(metadata or {}),
        }
    )
    events.append(event)
    payload["events"] = events
    payload["state"] = _compact_review_event(
        {
            **state,
            "status": status,
            "last_action": action,
            "category": category or state.get("category"),
            "reason": reason,
            "error": error,
            "error_type": error_type,
            "actor": actor or state.get("actor"),
            "updated_at": event_at,
            "first_seen_at": state.get("first_seen_at") or event_at,
            "metadata": dict(metadata or state.get("metadata") or {}),
        }
    )
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _json_payload(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _positive_int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _ingestion_review_payload_limit_bytes() -> int | None:
    for key in (
        "ingestion.max_review_payload_bytes",
        "ingestion.max_payload_bytes",
        "connectors.budgets.defaults.max_bytes_per_file",
        "connectors.budgets.max_bytes_per_file",
    ):
        parsed = _positive_int_or_none(config.get(key))
        if parsed is not None:
            return parsed
    return DEFAULT_INGESTION_REVIEW_PAYLOAD_BYTES


def _declared_payload_sizes(payload: Mapping[str, Any]) -> dict[str, int]:
    sizes: dict[str, int] = {}
    candidates: list[tuple[str, Any]] = [
        ("raw_payload_size_bytes", payload.get("raw_payload_size_bytes")),
        ("source_size_bytes", payload.get("source_size_bytes")),
        ("size_bytes", payload.get("size_bytes")),
    ]
    raw_payload = payload.get("raw_payload")
    if isinstance(raw_payload, Mapping):
        candidates.append(("raw_payload.size_bytes", raw_payload.get("size_bytes")))
    custom_metadata = payload.get("custom_metadata")
    if isinstance(custom_metadata, Mapping):
        candidates.extend(
            (
                (f"custom_metadata.{key}", custom_metadata.get(key))
                for key in ("raw_payload_size_bytes", "source_size_bytes", "size_bytes")
            )
        )
    for label, value in candidates:
        parsed = _positive_int_or_none(value)
        if parsed is not None:
            sizes[label] = parsed
    return sizes


def _artifact_id_candidates_for_type(
    artifact_type: str,
    payload: Mapping[str, Any],
) -> tuple[Any, ...]:
    if artifact_type not in SUPPORTED_INGESTION_ARTIFACT_TYPES:
        return ()
    return native_id_candidates_for_artifact_type(artifact_type, payload)


def _compact_review_event(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in event.items()
        if value not in (None, "", {}, [])
    }
