"""Operator review workflow for bad ingestion artifacts."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

from .artifact_review_policy import (
    INGESTION_ACTIVE_REVIEW_STATUSES,
    INGESTION_CLOSED_REVIEW_STATUSES,
    require_review_revision,
)
from .ccf_metadata import open_metadata_service
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

    def decide(
        self, artifact_id: str, *, action: str, actor: str, reason: str,
        expected_revision: str, security_acknowledged: bool = False,
    ) -> IngestionQueueEntry:
        """Apply an explicit web decision to precisely the revision displayed.

        Security approval is a separate action, never an implicit side effect of
        retry. Classification decisions retain their dedicated routing workflow.
        """
        from .classification_review import entry_has_classification_review

        entry = self.db.get_ingestion_entry(artifact_id, raise_errors=True)
        if entry is None:
            raise ArtifactReviewQueueError("Review item no longer exists")
        require_review_revision(entry, expected_revision)
        if entry_has_classification_review(entry):
            raise ArtifactReviewQueueError("Classification review requires the classification CLI")
        security = False
        if action != "reject":
            payload = json.loads(entry.payload_json)
            if not isinstance(payload, dict):
                raise ArtifactReviewQueueError("Malformed payload must be repaired before retry")
            metadata = payload.get("normalized_metadata", {})
            if not isinstance(metadata, dict):
                raise ArtifactReviewQueueError("Malformed security metadata must be repaired before retry")
            security = prompt_security_requires_review(metadata)
        kwargs = dict(actor=actor, reason=reason, expected_revision=expected_revision)
        if action == "approve_security":
            if not security or not security_acknowledged:
                raise ArtifactReviewQueueError("Security approval requires explicit acknowledgement")
            updated = self.db.approve_ingestion_security_override(artifact_id, **kwargs)
            metadata_action = "security_override_approved"
        elif action == "retry":
            if security:
                raise ArtifactReviewQueueError("Use explicit security approval for flagged content")
            updated = self.db.transition_ingestion_review(
                artifact_id, action="retry", status="pending", clear_error=True,
                reset_attempts=True, **kwargs,
            )
            metadata_action = "retry"
        elif action == "reject":
            updated = self.db.transition_ingestion_review(
                artifact_id, action="reject", status="rejected", **kwargs,
            )
            metadata_action = "reject"
        else:
            raise ArtifactReviewQueueError("Unsupported review action")
        if updated is None:
            raise ArtifactReviewQueueError("Review item no longer exists")
        self._record_ccf_review(updated, action=metadata_action)
        return updated

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
            self._record_ccf_review(updated, action="classification_approved")
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
        self._record_ccf_review(updated, action="retry")
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
            self._record_ccf_review(updated, action="classification_rejected")
            return updated
        updated = self.db.reject_ingestion_review(
            artifact_id,
            actor=actor,
            reason=reason,
            metadata=metadata,
        )
        if not updated:
            raise ArtifactReviewQueueError(f"Artifact not found: {artifact_id}")
        self._record_ccf_review(updated, action="reject")
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
        self._record_ccf_review(updated, action="mark_reviewed")
        return updated

    def _record_ccf_review(
        self, entry: IngestionQueueEntry, *, action: str
    ) -> None:
        """Record the actual audit event after the native review transition."""
        try:
            service = open_metadata_service(self.config)
            if service is None:
                return
            service.record_artifact_review(
                artifact_id=entry.artifact_id, artifact_type=entry.artifact_type,
                payload=json.loads(entry.payload_json),
                review=_latest_review_event(entry.review_json, action),
            )
        except Exception:
            logger.exception("CCF review metadata failed for artifact %s action %s",
                             entry.artifact_id, action)

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
