"""Operator review service for semantic memory candidates."""

from __future__ import annotations

import json
import logging
from typing import Any, Mapping

from .ccf_dualwrite import mirrored_queue_artifact, open_dual_write_service
from .config import config as _runtime_config
from .metadata_db import MetadataDB
from .semantic_memory import (
    SemanticMemoryCandidate,
    SemanticMemoryEvidence,
    SemanticMemoryStore,
    SemanticMemoryValidationError,
)
from .time_utils import utc_now_iso as _now_iso


logger = logging.getLogger(__name__)

JsonObject = dict[str, Any]
SEMANTIC_MEMORY_REVIEW_METADATA_KEY = "semantic_memory_review"


class SemanticMemoryReviewError(RuntimeError):
    """Raised when a semantic memory review request cannot be fulfilled."""


class SemanticMemoryReviewNotFoundError(SemanticMemoryReviewError):
    """Raised when a requested semantic memory candidate does not exist."""


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_object(value: Mapping[str, Any] | None, field_name: str) -> JsonObject:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise SemanticMemoryValidationError(f"{field_name} must be a JSON object")
    return dict(value)


class SemanticMemoryReviewService:
    """Review-facing service API for semantic memory candidates."""

    def __init__(
        self,
        *,
        store: SemanticMemoryStore | None = None,
        db: MetadataDB | None = None,
        config=None,
    ):
        if store is not None and db is not None:
            raise SemanticMemoryReviewError("store and db are mutually exclusive")
        self.store = store or SemanticMemoryStore(db)
        self.config = config if config is not None else _runtime_config

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
    ) -> dict[str, Any]:
        """List reviewable candidates with stored candidate provenance."""
        candidates = self.store.list_candidates(
            candidate_type=candidate_type,
            status=status,
            entity_id=entity_id,
            entity_type=entity_type,
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            capture_event_id=capture_event_id,
            limit=limit,
        )
        payloads = []
        for candidate in candidates:
            evidence = self.store.list_evidence(candidate_id=candidate.candidate_id)
            payloads.append(
                _candidate_payload(candidate, evidence_count=len(evidence))
            )

        return {
            "candidates": payloads,
            "total": len(payloads),
            "filters": _compact_mapping(
                {
                    "candidate_type": candidate_type,
                    "status": status,
                    "entity_id": entity_id,
                    "entity_type": entity_type,
                    "artifact_id": artifact_id,
                    "artifact_type": artifact_type,
                    "capture_event_id": capture_event_id,
                    "limit": limit,
                }
            ),
        }

    def get_candidate(self, candidate_id: str) -> dict[str, Any]:
        """Return one candidate and all stored evidence links."""
        candidate = self.store.get_candidate(candidate_id)
        if candidate is None:
            raise SemanticMemoryReviewNotFoundError(
                f"Semantic memory candidate not found: {candidate_id}"
            )
        evidence = self.store.list_evidence(candidate_id=candidate.candidate_id)
        return {
            "candidate": _candidate_payload(candidate, evidence_count=len(evidence)),
            "evidence": [_evidence_payload(item) for item in evidence],
            "total_evidence": len(evidence),
        }

    def confirm_candidate(
        self,
        candidate_id: str,
        *,
        actor: str | None = None,
        reason: str | None = None,
        reviewed_at: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Mark a proposed or confirmed candidate as explicitly confirmed."""
        candidate = self._review_transition(
            candidate_id,
            "confirmed",
            action="confirm",
            actor=actor,
            reason=reason,
            reviewed_at=reviewed_at,
            metadata=metadata,
        )
        return self.get_candidate(candidate.candidate_id)

    def reject_candidate(
        self,
        candidate_id: str,
        *,
        actor: str | None = None,
        reason: str | None = None,
        reviewed_at: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Reject a proposed or confirmed candidate."""
        candidate = self._review_transition(
            candidate_id,
            "rejected",
            action="reject",
            actor=actor,
            reason=reason,
            reviewed_at=reviewed_at,
            metadata=metadata,
        )
        return self.get_candidate(candidate.candidate_id)

    def supersede_candidate(
        self,
        candidate_id: str,
        *,
        superseded_by_candidate_id: str,
        actor: str | None = None,
        reason: str | None = None,
        reviewed_at: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Mark a candidate as superseded by another stored candidate."""
        candidate = self._review_transition(
            candidate_id,
            "superseded",
            action="supersede",
            actor=actor,
            reason=reason,
            reviewed_at=reviewed_at,
            metadata={
                **_json_object(metadata, "metadata"),
                "superseded_by_candidate_id": superseded_by_candidate_id,
            },
            superseded_by_candidate_id=superseded_by_candidate_id,
        )
        return self.get_candidate(candidate.candidate_id)

    def promote_candidate(
        self,
        candidate_id: str,
        *,
        actor: str | None = None,
        reason: str | None = None,
        reviewed_at: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Promote a candidate through the existing evidence gate."""
        self._require_candidate(candidate_id)
        review_record = _review_record(
            "promote",
            actor=actor,
            reason=reason,
            reviewed_at=reviewed_at,
            metadata=metadata,
        )
        candidate = self.store.promote_candidate(
            candidate_id,
            metadata={SEMANTIC_MEMORY_REVIEW_METADATA_KEY: review_record},
            write_provenance={SEMANTIC_MEMORY_REVIEW_METADATA_KEY: review_record},
            promoted_at=review_record["at"],
        )
        self._mirror_review(candidate, review_record)
        return self.get_candidate(candidate.candidate_id)

    def _review_transition(
        self,
        candidate_id: str,
        status: str,
        *,
        action: str,
        actor: str | None,
        reason: str | None,
        reviewed_at: str | None,
        metadata: Mapping[str, Any] | None,
        superseded_by_candidate_id: str | None = None,
    ) -> SemanticMemoryCandidate:
        self._require_candidate(candidate_id)
        review_record = _review_record(
            action,
            actor=actor,
            reason=reason,
            reviewed_at=reviewed_at,
            metadata=metadata,
        )
        candidate = self.store.transition_candidate(
            candidate_id,
            status,
            superseded_by_candidate_id=superseded_by_candidate_id,
            metadata={SEMANTIC_MEMORY_REVIEW_METADATA_KEY: review_record},
            write_provenance={SEMANTIC_MEMORY_REVIEW_METADATA_KEY: review_record},
            transitioned_at=review_record["at"],
        )
        self._mirror_review(candidate, review_record)
        return candidate

    def _mirror_review(
        self,
        candidate: SemanticMemoryCandidate,
        review_record: Mapping[str, Any],
    ) -> None:
        """Mirror the candidate assertion + review decision into CCF.

        Fail-open and ledgered: the legacy transition is already durable
        when this runs. The candidate assertion is materialized (idempotent)
        so the decision has a target; evidence Links cite the mirrored media
        artifacts behind the candidate's evidence rows. When no evidence
        artifact was ever mirrored the review cannot cite a source or
        evidence, so it is skipped with a warning rather than weakened.
        """
        service = open_dual_write_service(self.config)
        if service is None or not service.settings.mirror_review:
            return
        try:
            from ccf.dualwrite import families
            from ccf.dualwrite.conventions import ENTITY_REVISION

            evidence_ccf_ids: list[str] = []
            source_ccf_id: str | None = None
            for item in self.store.list_evidence(candidate_id=candidate.candidate_id):
                if not item.artifact_id:
                    continue
                entry = self.store.db.get_ingestion_entry(item.artifact_id)
                if entry is None:
                    continue
                try:
                    payload = json.loads(entry.payload_json or "{}")
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, Mapping):
                    continue
                resolved = mirrored_queue_artifact(service, self.config, payload)
                if resolved is None:
                    continue
                if source_ccf_id is None:
                    source_ccf_id = resolved[0]
                evidence_ccf_ids.append(resolved[1])
            if source_ccf_id is None:
                logger.warning(
                    "skipping CCF review mirror for candidate %s: "
                    "no mirrored evidence artifact",
                    candidate.candidate_id,
                )
                return

            subject_ccf_id = None
            if candidate.entity_id:
                found = families.find_mirrored_object(
                    service,
                    native_id=candidate.entity_id,
                    revision=ENTITY_REVISION,
                )
                if found is not None:
                    subject_ccf_id = found[1]

            assertion_id = families.mirror_candidate_assertion(
                service,
                source_ccf_id=source_ccf_id,
                candidate={
                    "candidate_id": candidate.candidate_id,
                    "candidate_type": candidate.candidate_type,
                    "status": candidate.status,
                    "subject": candidate.subject,
                    "predicate": candidate.predicate,
                    "object_value": candidate.object_value,
                    "text": candidate.text,
                    "confidence": candidate.confidence,
                    "entity_id": candidate.entity_id,
                },
                subject_ccf_id=subject_ccf_id,
                evidence_ccf_ids=evidence_ccf_ids,
            )
            families.mirror_review_decision(
                service,
                source_ccf_id=source_ccf_id,
                review=dict(review_record),
                target_ccf_ids=[assertion_id],
                evidence_ccf_ids=evidence_ccf_ids,
            )
        except Exception as exc:
            service.record_error(
                {
                    "family": "review",
                    "flow": "semantic_memory_review",
                    "candidate_id": candidate.candidate_id,
                    "action": review_record.get("action"),
                },
                exc,
            )

    def _require_candidate(self, candidate_id: str) -> None:
        candidate = self.store.get_candidate(candidate_id)
        if candidate is None:
            raise SemanticMemoryReviewNotFoundError(
                f"Semantic memory candidate not found: {candidate_id}"
            )


def _review_record(
    action: str,
    *,
    actor: str | None,
    reason: str | None,
    reviewed_at: str | None,
    metadata: Mapping[str, Any] | None,
) -> JsonObject:
    record: JsonObject = {
        "action": action,
        "at": _clean_optional(reviewed_at) or _now_iso(),
    }
    actor_text = _clean_optional(actor)
    reason_text = _clean_optional(reason)
    if actor_text:
        record["actor"] = actor_text
    if reason_text:
        record["reason"] = reason_text
    metadata_payload = _json_object(metadata, "metadata")
    if metadata_payload:
        record["metadata"] = metadata_payload
    return record


def _candidate_payload(
    candidate: SemanticMemoryCandidate,
    *,
    evidence_count: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "candidate_id": candidate.candidate_id,
        "candidate_type": candidate.candidate_type,
        "status": candidate.status,
        "subject": candidate.subject,
        "predicate": candidate.predicate,
        "object_value": candidate.object_value,
        "text": candidate.text,
        "entity_id": candidate.entity_id,
        "entity_type": candidate.entity_type,
        "entity_name": candidate.entity_name,
        "confidence": candidate.confidence,
        "privacy_class": candidate.privacy_class,
        "supersedes_candidate_id": candidate.supersedes_candidate_id,
        "superseded_by_candidate_id": candidate.superseded_by_candidate_id,
        "metadata": dict(candidate.metadata),
        "write_provenance": dict(candidate.write_provenance),
        "created_at": candidate.created_at,
        "updated_at": candidate.updated_at,
        "status_updated_at": candidate.status_updated_at,
    }
    if evidence_count is not None:
        payload["evidence_count"] = evidence_count
    return payload


def _evidence_payload(evidence: SemanticMemoryEvidence) -> dict[str, Any]:
    return {
        "evidence_id": evidence.evidence_id,
        "candidate_id": evidence.candidate_id,
        "artifact_id": evidence.artifact_id,
        "artifact_type": evidence.artifact_type,
        "capture_event_id": evidence.capture_event_id,
        "source_path": evidence.source_path,
        "source_timestamp": evidence.source_timestamp,
        "evidence_text": evidence.evidence_text,
        "confidence": evidence.confidence,
        "privacy_class": evidence.privacy_class,
        "metadata": dict(evidence.metadata),
        "write_provenance": dict(evidence.write_provenance),
        "created_at": evidence.created_at,
        "updated_at": evidence.updated_at,
    }


def _compact_mapping(value: Mapping[str, Any]) -> JsonObject:
    return {str(key): item for key, item in value.items() if item is not None}


__all__ = [
    "SEMANTIC_MEMORY_REVIEW_METADATA_KEY",
    "SemanticMemoryReviewError",
    "SemanticMemoryReviewNotFoundError",
    "SemanticMemoryReviewService",
]
