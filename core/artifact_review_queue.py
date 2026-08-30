"""Operator review workflow for bad ingestion artifacts."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

from .artifact_review_policy import (
    INGESTION_ACTIVE_REVIEW_STATUSES,
    INGESTION_CLOSED_REVIEW_STATUSES,
)
from .ccf_dualwrite import mirrored_queue_artifact, open_dual_write_service
from .config import config as _runtime_config
from .metadata_db import (
    IngestionQueueEntry,
    MetadataDB,
)
from .prompt_security import (
    THOTH_SECURITY_FINDINGS_KEY,
    THOTH_SECURITY_POLICY_KEY,
    prompt_security_requires_review,
)

logger = logging.getLogger(__name__)


class ArtifactReviewQueueError(RuntimeError):
    """Raised when an operator review transition cannot be applied."""


class ArtifactReviewQueueService:
    """Service API for listing and transitioning artifact review rows."""

    def __init__(self, db: MetadataDB, config=None):
        self.db = db
        self.config = config if config is not None else _runtime_config

    def list_entries(
        self,
        *,
        status: str | None = None,
        include_closed: bool = False,
        limit: int = 50,
    ) -> list[IngestionQueueEntry]:
        return self.db.list_ingestion_review_entries(
            status=status,
            include_closed=include_closed,
            limit=limit,
        )

    def retry(
        self,
        artifact_id: str,
        *,
        actor: str,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> IngestionQueueEntry:
        entry = self._get_entry(artifact_id)
        from .classification_review import (
            ClassificationReviewError,
            ClassificationReviewService,
        )

        classification = ClassificationReviewService(self.db, config=self.config)
        if classification.is_review_item(artifact_id):
            try:
                classification.approve(
                    artifact_id,
                    actor=actor,
                    reason=reason,
                )
            except ClassificationReviewError as exc:
                raise ArtifactReviewQueueError(str(exc)) from exc
            updated = self._get_entry(artifact_id)
            self._mirror_review_decision(updated, action="classification_approved")
            return updated
        if _entry_has_prompt_security_review(entry):
            approved = self.db.approve_ingestion_security_override(
                artifact_id,
                actor=actor,
                reason=reason,
            )
            if not approved:
                raise ArtifactReviewQueueError(f"Artifact not found: {artifact_id}")
        updated = self.db.retry_ingestion_review(
            artifact_id,
            actor=actor,
            reason=reason,
            metadata=metadata,
        )
        if not updated:
            raise ArtifactReviewQueueError(f"Artifact not found: {artifact_id}")
        self._mirror_review_decision(updated, action="retry")
        return updated

    def reject(
        self,
        artifact_id: str,
        *,
        actor: str,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> IngestionQueueEntry:
        from .classification_review import (
            ClassificationReviewError,
            ClassificationReviewService,
        )

        classification = ClassificationReviewService(self.db, config=self.config)
        if classification.is_review_item(artifact_id):
            try:
                classification.reject(
                    artifact_id,
                    actor=actor,
                    reason=reason,
                )
            except ClassificationReviewError as exc:
                raise ArtifactReviewQueueError(str(exc)) from exc
            updated = self._get_entry(artifact_id)
            self._mirror_review_decision(updated, action="classification_rejected")
            return updated
        updated = self.db.reject_ingestion_review(
            artifact_id,
            actor=actor,
            reason=reason,
            metadata=metadata,
        )
        if not updated:
            raise ArtifactReviewQueueError(f"Artifact not found: {artifact_id}")
        self._mirror_review_decision(updated, action="reject")
        return updated

    def mark_reviewed(
        self,
        artifact_id: str,
        *,
        actor: str,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> IngestionQueueEntry:
        from .classification_review import ClassificationReviewService

        classification = ClassificationReviewService(self.db, config=self.config)
        if classification.is_review_item(artifact_id):
            raise ArtifactReviewQueueError(
                "Classification items require approve, correct, or reject; "
                "mark-reviewed would bypass routing decision provenance"
            )
        updated = self.db.mark_ingestion_reviewed(
            artifact_id,
            actor=actor,
            reason=reason,
            metadata=metadata,
        )
        if not updated:
            raise ArtifactReviewQueueError(f"Artifact not found: {artifact_id}")
        self._mirror_review_decision(updated, action="mark_reviewed")
        return updated

    def _mirror_review_decision(
        self, entry: IngestionQueueEntry, *, action: str
    ) -> None:
        """Mirror the appended review event into CCF (fail-open, ledgered).

        The target is the mirrored media artifact of the reviewed queue
        entry, resolved through the origin index. When the artifact was
        never mirrored (capture predates the mirror) the decision cannot
        cite its target, so it is skipped with a warning rather than
        weakened.
        """
        service = open_dual_write_service(self.config)
        if service is None or not service.settings.mirror_review:
            return
        try:
            event = _latest_review_event(entry.review_json, action)
            payload = json.loads(entry.payload_json or "{}")
            if not isinstance(payload, Mapping):
                payload = {}
            target = mirrored_queue_artifact(service, self.config, payload)
            if target is None:
                logger.warning(
                    "skipping CCF review mirror for %s on %s: "
                    "artifact was never mirrored",
                    action,
                    entry.artifact_id,
                )
                return
            source_ccf_id, artifact_ccf_id = target
            from ccf.dualwrite import families

            families.mirror_review_decision(
                service,
                source_ccf_id=source_ccf_id,
                review=event,
                target_ccf_ids=[artifact_ccf_id],
                evidence_ccf_ids=[artifact_ccf_id],
            )
        except Exception as exc:
            service.record_error(
                {
                    "family": "review",
                    "flow": "artifact_review",
                    "artifact_id": entry.artifact_id,
                    "action": action,
                },
                exc,
            )

    def _get_entry(self, artifact_id: str) -> IngestionQueueEntry:
        entry = self.db.get_ingestion_entry(artifact_id)
        if not entry:
            raise ArtifactReviewQueueError(f"Artifact not found: {artifact_id}")
        return entry


def active_review_statuses() -> tuple[str, ...]:
    return tuple(INGESTION_ACTIVE_REVIEW_STATUSES)


def closed_review_statuses() -> tuple[str, ...]:
    return tuple(INGESTION_CLOSED_REVIEW_STATUSES)


def _latest_review_event(review_json: str | None, action: str) -> dict:
    """The most recent stored review event for one action (fail closed)."""
    try:
        payload = json.loads(review_json or "{}")
    except json.JSONDecodeError:
        payload = {}
    events = payload.get("events") if isinstance(payload, Mapping) else None
    if isinstance(events, list):
        for event in reversed(events):
            if isinstance(event, Mapping) and event.get("action") == action:
                return dict(event)
    raise ArtifactReviewQueueError(
        f"review event {action!r} missing from stored review audit"
    )


def _entry_has_prompt_security_review(entry: IngestionQueueEntry) -> bool:
    try:
        payload = json.loads(entry.payload_json)
    except Exception:
        return False
    if not isinstance(payload, Mapping):
        return False
    normalized_metadata = payload.get("normalized_metadata")
    if not isinstance(normalized_metadata, Mapping):
        return False
    return bool(
        normalized_metadata.get(THOTH_SECURITY_FINDINGS_KEY)
        or normalized_metadata.get(THOTH_SECURITY_POLICY_KEY)
        or prompt_security_requires_review(normalized_metadata)
    )
