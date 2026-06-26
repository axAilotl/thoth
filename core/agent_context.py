"""Cited context helpers for agent-facing query surfaces."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .hybrid_search import HybridSearchHit
from .metadata_db import IngestionQueueEntry
from .prompt_security import (
    THOTH_REDACTION_METADATA_KEY,
    THOTH_SECURITY_AUDIT_KEY,
    THOTH_SECURITY_FINDINGS_KEY,
    THOTH_SECURITY_FINDING_COUNT_KEY,
    THOTH_SECURITY_PATTERN_IDS_KEY,
    THOTH_SECURITY_POLICY_KEY,
    prompt_security_requires_review,
)

_BLOCKING_SECURITY_STATUSES = {
    "blocked",
    "failed",
    "needs_review",
    "quarantined",
    "reviewed",
    "rejected",
}


def artifact_security_state(entry: IngestionQueueEntry) -> dict[str, Any]:
    metadata = _security_metadata_from_payload(entry.payload_json)
    policy = metadata.get(THOTH_SECURITY_POLICY_KEY)
    policy_payload = dict(policy) if isinstance(policy, Mapping) else {}
    entry_status = str(entry.status or "").strip().lower()
    policy_status = str(policy_payload.get("status") or "allowed").strip().lower()
    status = entry_status if entry_status in _BLOCKING_SECURITY_STATUSES else policy_status
    findings = metadata.get(THOTH_SECURITY_FINDINGS_KEY)
    pattern_ids = (
        metadata.get(THOTH_SECURITY_PATTERN_IDS_KEY)
        or policy_payload.get("pattern_ids")
        or []
    )
    return _compact_mapping(
        {
            "status": status,
            "requires_review": entry_requires_security_review(entry),
            "queue_status": entry.status,
            "policy": policy_payload,
            "finding_count": metadata.get(THOTH_SECURITY_FINDING_COUNT_KEY)
            or (len(findings) if isinstance(findings, list) else None),
            "pattern_ids": _string_list(pattern_ids),
        }
    )


def artifact_trust_state(entry: IngestionQueueEntry) -> dict[str, Any]:
    payload = _json_object(entry.payload_json)
    security = artifact_security_state(entry)
    explicit_score = payload.get("source_trust_score") or payload.get("trust_score")
    explicit_reason = payload.get("source_trust_reason") or payload.get("trust_reason")
    if explicit_score is not None:
        try:
            score = float(explicit_score)
        except (TypeError, ValueError):
            score = 0.0
    elif bool(security.get("requires_review")):
        score = 0.0 if security.get("status") == "blocked" else 0.25
    else:
        score = 1.0
    return {
        "score": score,
        "reason": str(explicit_reason or f"queue_status_{entry.status}"),
        "influence_sources": [],
    }


def artifact_citations(
    entry: IngestionQueueEntry,
    *,
    canonical_record: Mapping[str, Any] | None = None,
    provenance: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    payload = _json_object(entry.payload_json)
    canonical = dict(canonical_record or {})
    provenance_payload = dict(provenance or {})
    raw_payload = provenance_payload.get("raw_payload")
    raw_payload_path = (
        raw_payload.get("path")
        if isinstance(raw_payload, Mapping)
        else None
    )
    source_path = _first_string(
        raw_payload_path
        or payload.get("raw_payload_path")
        or payload.get("source_path")
        or payload.get("path")
        or payload.get("url")
    )
    return [
        _compact_mapping(
            {
                "kind": "artifact",
                "artifact_id": entry.artifact_id,
                "canonical_artifact_id": canonical.get("artifact_id")
                or payload.get("id")
                or payload.get("artifact_id"),
                "artifact_type": entry.artifact_type,
                "source": entry.source,
                "source_type": payload.get("source_type") or entry.source,
                "source_id": payload.get("source_id"),
                "source_path": source_path,
                "title": canonical.get("title")
                or payload.get("title")
                or payload.get("repo_name")
                or payload.get("name"),
                "queue_status": entry.status,
            }
        )
    ]


def hybrid_hit_citations(
    hit: HybridSearchHit | Mapping[str, Any],
) -> list[dict[str, Any]]:
    result_type = _hit_value(hit, "result_type")
    provenance = _hit_mapping(hit, "provenance")
    if result_type == "wiki_page":
        return [
            _compact_mapping(
                {
                    "kind": "wiki_page",
                    "result_id": _hit_value(hit, "result_id"),
                    "slug": _hit_value(hit, "slug"),
                    "title": _hit_value(hit, "title"),
                    "page_path": _hit_value(hit, "page_path")
                    or provenance.get("page_path"),
                    "source_type": _hit_value(hit, "source_type")
                    or provenance.get("source_type"),
                    "source_id": _hit_value(hit, "source_id"),
                    "source_path": provenance.get("source_path"),
                    "source_paths": provenance.get("source_paths"),
                    "artifact_id": _hit_value(hit, "artifact_id")
                    or provenance.get("artifact_id"),
                    "event_ids": provenance.get("event_ids"),
                }
            )
        ]
    if result_type == "artifact":
        return [
            _compact_mapping(
                {
                    "kind": "artifact",
                    "result_id": _hit_value(hit, "result_id"),
                    "artifact_id": _hit_value(hit, "artifact_id")
                    or provenance.get("artifact_id"),
                    "artifact_type": provenance.get("artifact_type"),
                    "title": _hit_value(hit, "title"),
                    "source": provenance.get("source"),
                    "source_type": _hit_value(hit, "source_type")
                    or provenance.get("source_type"),
                    "source_id": _hit_value(hit, "source_id")
                    or provenance.get("source_id"),
                    "source_path": provenance.get("source_path"),
                    "queue_status": provenance.get("queue_status"),
                }
            )
        ]
    if result_type == "capture_event":
        return [
            _compact_mapping(
                {
                    "kind": "capture_event",
                    "result_id": _hit_value(hit, "result_id"),
                    "event_id": _hit_value(hit, "event_id")
                    or provenance.get("event_id"),
                    "title": _hit_value(hit, "title"),
                    "source_id": _hit_value(hit, "source_id")
                    or provenance.get("source_id"),
                    "source_type": _hit_value(hit, "source_type")
                    or provenance.get("source_type"),
                    "source_name": provenance.get("source_name"),
                    "session_id": provenance.get("session_id"),
                    "native_event_id": provenance.get("native_event_id"),
                    "source_paths": provenance.get("source_paths"),
                    "raw_ref_ids": provenance.get("raw_ref_ids"),
                    "artifact_ids": provenance.get("artifact_ids"),
                    "occurred_at": provenance.get("occurred_at"),
                    "captured_at": provenance.get("captured_at"),
                }
            )
        ]
    return []


def capture_event_requires_security_review(event: Mapping[str, Any]) -> bool:
    return bool(capture_event_security_state(event).get("requires_review"))


def capture_event_security_state(event: Mapping[str, Any]) -> dict[str, Any]:
    security_state = event.get("security_state")
    security_state_payload = (
        dict(security_state) if isinstance(security_state, Mapping) else {}
    )
    event_status = str(event.get("status") or "").strip().lower()
    open_finding_count = int(security_state_payload.get("open_finding_count") or 0)
    if event_status in _BLOCKING_SECURITY_STATUSES:
        status = event_status
    elif open_finding_count:
        status = "needs_review"
    else:
        status = "allowed"
    return _compact_mapping(
        {
            "status": status,
            "requires_review": status in _BLOCKING_SECURITY_STATUSES,
            "event_status": event.get("status"),
            "finding_count": security_state_payload.get("finding_count"),
            "open_finding_count": open_finding_count,
            "max_severity": security_state_payload.get("max_severity"),
        }
    )


def capture_event_trust_state(event: Mapping[str, Any]) -> dict[str, Any]:
    provenance = event.get("provenance")
    provenance_payload = dict(provenance) if isinstance(provenance, Mapping) else {}
    security = capture_event_security_state(event)
    explicit_score = provenance_payload.get("source_trust_score") or provenance_payload.get(
        "trust_score"
    )
    explicit_reason = provenance_payload.get(
        "source_trust_reason"
    ) or provenance_payload.get("trust_reason")
    if explicit_score is not None:
        try:
            score = float(explicit_score)
        except (TypeError, ValueError):
            score = 0.0
    elif bool(security.get("requires_review")):
        score = 0.0 if security.get("status") == "blocked" else 0.25
    else:
        score = 1.0
    return {
        "score": score,
        "reason": str(explicit_reason or f"capture_security_{security['status']}"),
        "influence_sources": [],
    }


def capture_event_citations(event: Mapping[str, Any]) -> list[dict[str, Any]]:
    citations = [
        _compact_mapping(
            {
                "kind": "capture_event",
                "event_id": event.get("event_id"),
                "event_type": event.get("event_type"),
                "source_id": event.get("source_id"),
                "session_id": event.get("session_id"),
                "native_event_id": event.get("native_event_id"),
                "event_hash": event.get("event_hash"),
                "occurred_at": event.get("occurred_at"),
                "captured_at": event.get("captured_at"),
            }
        )
    ]
    source = event.get("source")
    if isinstance(source, Mapping):
        citations.append(
            _compact_mapping(
                {
                    "kind": "capture_source",
                    "source_id": source.get("source_id"),
                    "source_name": source.get("source_name"),
                    "source_type": source.get("source_type"),
                    "collector": source.get("collector"),
                    "base_uri": source.get("base_uri"),
                }
            )
        )
    for raw_ref in event.get("raw_refs") or []:
        if not isinstance(raw_ref, Mapping):
            continue
        citations.append(
            _compact_mapping(
                {
                    "kind": "raw_ref",
                    "raw_ref_id": raw_ref.get("raw_ref_id"),
                    "event_id": raw_ref.get("event_id"),
                    "source_id": raw_ref.get("source_id"),
                    "path": raw_ref.get("path"),
                    "sha256": raw_ref.get("sha256"),
                    "mime_type": raw_ref.get("mime_type"),
                }
            )
        )
    for artifact in event.get("artifacts") or []:
        if not isinstance(artifact, Mapping):
            continue
        citations.append(
            _compact_mapping(
                {
                    "kind": "artifact_link",
                    "artifact_link_id": artifact.get("artifact_link_id"),
                    "event_id": artifact.get("event_id"),
                    "raw_ref_id": artifact.get("raw_ref_id"),
                    "artifact_id": artifact.get("artifact_id"),
                    "artifact_type": artifact.get("artifact_type"),
                    "link_type": artifact.get("link_type"),
                }
            )
        )
    return citations


def entry_requires_security_review(entry: IngestionQueueEntry) -> bool:
    if entry.status in _BLOCKING_SECURITY_STATUSES:
        return True
    metadata = _security_metadata_from_payload(entry.payload_json)
    return prompt_security_requires_review(metadata)


def _hit_value(hit: HybridSearchHit | Mapping[str, Any], key: str) -> Any:
    if isinstance(hit, Mapping):
        return hit.get(key)
    return getattr(hit, key)


def _hit_mapping(hit: HybridSearchHit | Mapping[str, Any], key: str) -> dict[str, Any]:
    value = _hit_value(hit, key)
    return dict(value) if isinstance(value, Mapping) else {}


def _json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _security_metadata_from_payload(payload_json: str | None) -> dict[str, Any]:
    payload = _json_object(payload_json)
    normalized_metadata = payload.get("normalized_metadata")
    if not isinstance(normalized_metadata, Mapping):
        return {}
    return {
        key: normalized_metadata[key]
        for key in (
            THOTH_SECURITY_FINDINGS_KEY,
            THOTH_SECURITY_FINDING_COUNT_KEY,
            THOTH_SECURITY_PATTERN_IDS_KEY,
            THOTH_SECURITY_POLICY_KEY,
            THOTH_SECURITY_AUDIT_KEY,
            THOTH_REDACTION_METADATA_KEY,
        )
        if normalized_metadata.get(key)
    }


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _first_string(value: Any) -> str | None:
    values = _string_list(value)
    return values[0] if values else None


def _compact_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if item not in (None, "", [], {}, ())
    }
