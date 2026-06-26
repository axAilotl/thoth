"""Operator review workflow for bad ingestion artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .artifact_review_policy import (
    INGESTION_ACTIVE_REVIEW_STATUSES,
    INGESTION_CLOSED_REVIEW_STATUSES,
)
from .metadata_db import (
    IngestionQueueEntry,
    MetadataDB,
)
from .prompt_security import (
    THOTH_SECURITY_FINDINGS_KEY,
    THOTH_SECURITY_POLICY_KEY,
    prompt_security_requires_review,
)


class ArtifactReviewQueueError(RuntimeError):
    """Raised when an operator review transition cannot be applied."""


class ArtifactReviewQueueService:
    """Service API for listing and transitioning artifact review rows."""

    def __init__(self, db: MetadataDB):
        self.db = db

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
        return updated

    def reject(
        self,
        artifact_id: str,
        *,
        actor: str,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> IngestionQueueEntry:
        updated = self.db.reject_ingestion_review(
            artifact_id,
            actor=actor,
            reason=reason,
            metadata=metadata,
        )
        if not updated:
            raise ArtifactReviewQueueError(f"Artifact not found: {artifact_id}")
        return updated

    def mark_reviewed(
        self,
        artifact_id: str,
        *,
        actor: str,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> IngestionQueueEntry:
        updated = self.db.mark_ingestion_reviewed(
            artifact_id,
            actor=actor,
            reason=reason,
            metadata=metadata,
        )
        if not updated:
            raise ArtifactReviewQueueError(f"Artifact not found: {artifact_id}")
        return updated

    def _get_entry(self, artifact_id: str) -> IngestionQueueEntry:
        entry = self.db.get_ingestion_entry(artifact_id)
        if not entry:
            raise ArtifactReviewQueueError(f"Artifact not found: {artifact_id}")
        return entry


def active_review_statuses() -> tuple[str, ...]:
    return tuple(INGESTION_ACTIVE_REVIEW_STATUSES)


def closed_review_statuses() -> tuple[str, ...]:
    return tuple(INGESTION_CLOSED_REVIEW_STATUSES)


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
