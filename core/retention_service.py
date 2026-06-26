"""Retention inspection and deletion workflows for captured data."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .capture_event_store import (
    ArtifactLink,
    CaptureEvent,
    CaptureEventStore,
    PrivacyAnnotation,
    ProvenanceRecord,
    RawArtifactRef,
    RetentionPolicy,
)
from .metadata_db import MetadataDB
from .path_layout import PathLayout
from .wiki_io import read_frontmatter


class RetentionServiceError(RuntimeError):
    """Raised when retention operations cannot be completed safely."""


RAW_RETENTION_SCOPES = frozenset({"raw_capture"})
DISTILLED_RETENTION_SCOPES = frozenset(
    {
        "compiled_wiki",
        "derived_file",
        "embedding",
        "llm_cache",
        "summary_file",
        "transcript_cache",
        "transcript_file",
    }
)
DELETABLE_RETENTION_ACTIONS = frozenset({"delete", "expire", "purge"})
IMMEDIATE_RETENTION_ACTIONS = frozenset({"delete_now", "expire_now", "purge_now"})

_DISTILLED_OUTPUT_KEYS = {
    "markdown_path",
    "output_path",
    "page_path",
    "processed_transcript_path",
    "summary_path",
    "transcript_path",
}
_SUMMARY_TASK_MARKERS = ("summary", "summarize")
_TRANSCRIPT_TASK_MARKERS = ("transcript",)


@dataclass(frozen=True)
class RetentionTarget:
    """One concrete piece of data that can be inspected for retention state."""

    event_id: str
    target_type: str
    target_id: str
    retention_scope: str
    retention_class: str | None
    privacy_class: str | None
    eligible: bool
    eligibility_reason: str
    policy: RetentionPolicy | None = None
    path: str | None = None
    exists: bool | None = None
    raw_ref_id: str | None = None
    artifact_link_id: str | None = None
    cache_key: str | None = None
    context_id: str | None = None
    candidate_key: str | None = None
    embedding_provider: str | None = None
    embedding_model: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event_id": self.event_id,
            "target_type": self.target_type,
            "target_id": self.target_id,
            "retention_scope": self.retention_scope,
            "retention_class": self.retention_class,
            "privacy_class": self.privacy_class,
            "eligible": self.eligible,
            "eligibility_reason": self.eligibility_reason,
            "policy": _retention_policy_payload(self.policy),
            "path": self.path,
            "exists": self.exists,
            "raw_ref_id": self.raw_ref_id,
            "artifact_link_id": self.artifact_link_id,
            "cache_key": self.cache_key,
            "context_id": self.context_id,
            "candidate_key": self.candidate_key,
            "embedding_provider": self.embedding_provider,
            "embedding_model": self.embedding_model,
            "metadata": dict(self.metadata),
        }
        return {key: value for key, value in payload.items() if value is not None}


@dataclass(frozen=True)
class _PathStatus:
    path: Path
    safe: bool
    exists: bool
    reason: str | None = None


@dataclass(frozen=True)
class _DeletionOutcome:
    status: str
    message: str
    bytes_deleted: int = 0


class CaptureRetentionService:
    """Inspect and expire raw and distilled capture data."""

    def __init__(
        self,
        event_store: CaptureEventStore,
        *,
        layout: PathLayout,
        db: MetadataDB | None = None,
    ) -> None:
        self.event_store = event_store
        self.layout = layout
        self.db = db

    def inspect(
        self,
        *,
        event_id: str | None = None,
        source_id: str | None = None,
        session_id: str | None = None,
        as_of: Any = None,
    ) -> dict[str, Any]:
        """Return retention classes and eligibility for capture-backed data."""
        as_of_dt = _parse_datetime(as_of) or datetime.now(timezone.utc)
        events = self._events_for_filters(
            event_id=event_id,
            source_id=source_id,
            session_id=session_id,
        )
        targets: list[RetentionTarget] = []
        for event in events:
            targets.extend(self._targets_for_event(event, as_of=as_of_dt))
        return _inspection_payload(targets, as_of=as_of_dt)

    def expire(
        self,
        *,
        event_id: str,
        delete_raw: bool = False,
        delete_distilled: bool = False,
        dry_run: bool = True,
        reason: str | None = None,
        actor: str | None = None,
        as_of: Any = None,
    ) -> dict[str, Any]:
        """Expire eligible data and return explicit operation/audit summaries."""
        if not event_id or not str(event_id).strip():
            raise RetentionServiceError("event_id is required for retention expiry")
        if not delete_raw and not delete_distilled:
            raise RetentionServiceError(
                "choose at least one deletion scope: raw or distilled"
            )
        if not dry_run and not _clean_text(reason):
            raise RetentionServiceError("reason is required when executing expiry")

        as_of_dt = _parse_datetime(as_of) or datetime.now(timezone.utc)
        selected_scopes: set[str] = set()
        if delete_raw:
            selected_scopes.update(RAW_RETENTION_SCOPES)
        if delete_distilled:
            selected_scopes.update(DISTILLED_RETENTION_SCOPES)

        event = self.event_store.get_event(event_id)
        if event is None:
            raise RetentionServiceError(f"capture event not found: {event_id}")

        operations: list[dict[str, Any]] = []
        audit_records: list[dict[str, Any]] = []
        for target in self._targets_for_event(event, as_of=as_of_dt):
            if target.retention_scope not in selected_scopes:
                continue
            if not target.eligible:
                operations.append(
                    _operation_payload(
                        target,
                        status="skipped",
                        message=target.eligibility_reason,
                    )
                )
                continue

            if dry_run:
                operations.append(
                    _operation_payload(
                        target,
                        status="dry_run",
                        message="eligible target would be expired",
                    )
                )
                continue

            outcome = self._delete_target(target)
            audit_record = self._record_expiry_audit(
                target,
                outcome=outcome,
                actor=actor,
                reason=_clean_text(reason) or "unspecified",
                as_of=as_of_dt,
            )
            if audit_record is not None:
                audit_records.append(_provenance_payload(audit_record))
            operations.append(
                _operation_payload(
                    target,
                    status=outcome.status,
                    message=outcome.message,
                    bytes_deleted=outcome.bytes_deleted,
                    audit_record_id=audit_record.provenance_id
                    if audit_record is not None
                    else None,
                )
            )

        return _expiry_payload(
            operations,
            audit_records=audit_records,
            dry_run=dry_run,
            delete_raw=delete_raw,
            delete_distilled=delete_distilled,
            as_of=as_of_dt,
        )

    def _events_for_filters(
        self,
        *,
        event_id: str | None,
        source_id: str | None,
        session_id: str | None,
    ) -> tuple[CaptureEvent, ...]:
        if event_id:
            event = self.event_store.get_event(event_id)
            if event is None:
                raise RetentionServiceError(f"capture event not found: {event_id}")
            return (event,)
        return self.event_store.list_events(source_id=source_id, session_id=session_id)

    def _targets_for_event(
        self,
        event: CaptureEvent,
        *,
        as_of: datetime,
    ) -> list[RetentionTarget]:
        raw_refs = self.event_store.list_raw_refs(event_id=event.event_id)
        artifact_links = self.event_store.list_artifact_links(event_id=event.event_id)
        privacy_annotations = self.event_store.list_privacy_annotations(
            event_id=event.event_id
        )
        event_policy = self._policy_for(
            "event",
            event.event_id,
            fallback=None,
            as_of=as_of,
        )
        privacy_class = _privacy_class(event.privacy, privacy_annotations)
        event_retention_class = _retention_class(event.retention, event_policy)

        targets: list[RetentionTarget] = []
        for raw_ref in raw_refs:
            policy = self._policy_for(
                "raw_ref",
                raw_ref.raw_ref_id,
                fallback=event_policy,
                as_of=as_of,
            )
            targets.append(
                self._raw_target(
                    event=event,
                    raw_ref=raw_ref,
                    policy=policy,
                    privacy_class=privacy_class,
                    fallback_retention_class=event_retention_class,
                    as_of=as_of,
                )
            )

        derived_paths = self._distilled_paths_for_event(event, artifact_links)
        for link in artifact_links:
            policy = self._policy_for(
                "artifact_link",
                link.artifact_link_id,
                fallback=event_policy,
                as_of=as_of,
            )
            for path_scope, path_text in derived_paths.get(link.artifact_link_id, ()):
                target = self._path_target(
                    event=event,
                    target_type=path_scope,
                    target_id=f"{link.artifact_link_id}:{path_text}",
                    retention_scope=path_scope,
                    policy=policy,
                    fallback_retention_class=event_retention_class,
                    privacy_class=privacy_class,
                    path_text=path_text,
                    as_of=as_of,
                    artifact_link_id=link.artifact_link_id,
                    metadata={
                        "artifact_id": link.artifact_id,
                        "artifact_type": link.artifact_type,
                    },
                )
                if not self._is_raw_root_path(target.path):
                    targets.append(target)

        targets.extend(
            self._compiled_wiki_targets(
                event=event,
                event_policy=event_policy,
                fallback_retention_class=event_retention_class,
                privacy_class=privacy_class,
                as_of=as_of,
            )
        )

        if self.db is not None:
            identifiers = self._event_identifiers(event, artifact_links)
            targets.extend(
                self._llm_cache_targets(
                    event=event,
                    identifiers=identifiers,
                    event_policy=event_policy,
                    fallback_retention_class=event_retention_class,
                    privacy_class=privacy_class,
                    as_of=as_of,
                )
            )
            targets.extend(
                self._transcript_cache_targets(
                    event=event,
                    identifiers=identifiers,
                    event_policy=event_policy,
                    fallback_retention_class=event_retention_class,
                    privacy_class=privacy_class,
                    as_of=as_of,
                )
            )
            targets.extend(
                self._embedding_targets(
                    event=event,
                    identifiers=identifiers,
                    event_policy=event_policy,
                    fallback_retention_class=event_retention_class,
                    privacy_class=privacy_class,
                    as_of=as_of,
                    source_paths=tuple(
                        target.path
                        for target in targets
                        if target.path and target.retention_scope != "raw_capture"
                    ),
                )
            )

        return _dedupe_targets(targets)

    def _raw_target(
        self,
        *,
        event: CaptureEvent,
        raw_ref: RawArtifactRef,
        policy: RetentionPolicy | None,
        privacy_class: str | None,
        fallback_retention_class: str | None,
        as_of: datetime,
    ) -> RetentionTarget:
        retention_class = _retention_class(raw_ref.metadata, policy) or fallback_retention_class
        path_status = self._path_status(raw_ref.path)
        eligible, reason = self._eligibility(
            policy,
            as_of=as_of,
            path_status=path_status,
            tombstoned=_is_content_deleted(raw_ref.metadata),
        )
        return RetentionTarget(
            event_id=event.event_id,
            target_type="raw_ref",
            target_id=raw_ref.raw_ref_id,
            retention_scope="raw_capture",
            retention_class=retention_class,
            privacy_class=privacy_class,
            eligible=eligible,
            eligibility_reason=reason,
            policy=policy,
            path=str(path_status.path),
            exists=path_status.exists,
            raw_ref_id=raw_ref.raw_ref_id,
            metadata={"sha256": raw_ref.sha256, "size_bytes": raw_ref.size_bytes},
        )

    def _path_target(
        self,
        *,
        event: CaptureEvent,
        target_type: str,
        target_id: str,
        retention_scope: str,
        policy: RetentionPolicy | None,
        fallback_retention_class: str | None,
        privacy_class: str | None,
        path_text: str,
        as_of: datetime,
        artifact_link_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> RetentionTarget:
        path_status = self._path_status(path_text)
        eligible, reason = self._eligibility(
            policy,
            as_of=as_of,
            path_status=path_status,
        )
        return RetentionTarget(
            event_id=event.event_id,
            target_type=target_type,
            target_id=target_id,
            retention_scope=retention_scope,
            retention_class=_retention_class({}, policy) or fallback_retention_class,
            privacy_class=privacy_class,
            eligible=eligible,
            eligibility_reason=reason,
            policy=policy,
            path=str(path_status.path),
            exists=path_status.exists,
            artifact_link_id=artifact_link_id,
            metadata=dict(metadata or {}),
        )

    def _compiled_wiki_targets(
        self,
        *,
        event: CaptureEvent,
        event_policy: RetentionPolicy | None,
        fallback_retention_class: str | None,
        privacy_class: str | None,
        as_of: datetime,
    ) -> list[RetentionTarget]:
        pages_dir = self.layout.wiki_root / "pages"
        if not pages_dir.exists():
            return []

        targets: list[RetentionTarget] = []
        for page_path in sorted(pages_dir.glob("*.md")):
            try:
                frontmatter = read_frontmatter(page_path)
            except Exception:
                continue
            event_ids = frontmatter.get("thoth_event_ids") or frontmatter.get("event_ids")
            if not isinstance(event_ids, list) or event.event_id not in {
                str(item) for item in event_ids
            }:
                continue
            relative_id = _relative_id(page_path, self.layout.wiki_root)
            policy = self._policy_for(
                "wiki_page",
                relative_id,
                fallback=event_policy,
                as_of=as_of,
            )
            targets.append(
                self._path_target(
                    event=event,
                    target_type="wiki_page",
                    target_id=relative_id,
                    retention_scope="compiled_wiki",
                    policy=policy,
                    fallback_retention_class=fallback_retention_class,
                    privacy_class=privacy_class,
                    path_text=str(page_path),
                    as_of=as_of,
                    metadata={"slug": frontmatter.get("thoth_slug") or frontmatter.get("slug")},
                )
            )
        return targets

    def _llm_cache_targets(
        self,
        *,
        event: CaptureEvent,
        identifiers: tuple[str, ...],
        event_policy: RetentionPolicy | None,
        fallback_retention_class: str | None,
        privacy_class: str | None,
        as_of: datetime,
    ) -> list[RetentionTarget]:
        if self.db is None:
            return []
        entries = self.db.list_llm_cache_entries_for_contexts(identifiers)
        targets: list[RetentionTarget] = []
        for entry in entries:
            task_type = str(entry.get("task_type") or "").lower()
            if not (
                any(marker in task_type for marker in _SUMMARY_TASK_MARKERS)
                or any(marker in task_type for marker in _TRANSCRIPT_TASK_MARKERS)
            ):
                continue
            cache_key = str(entry.get("cache_key") or "")
            policy = self._policy_for(
                "llm_cache",
                cache_key,
                fallback=event_policy,
                as_of=as_of,
            )
            eligible, reason = self._eligibility(policy, as_of=as_of)
            targets.append(
                RetentionTarget(
                    event_id=event.event_id,
                    target_type="llm_cache",
                    target_id=cache_key,
                    retention_scope="llm_cache",
                    retention_class=_retention_class({}, policy)
                    or fallback_retention_class,
                    privacy_class=privacy_class,
                    eligible=eligible,
                    eligibility_reason=reason,
                    policy=policy,
                    exists=True,
                    cache_key=cache_key,
                    metadata={
                        "task_type": entry.get("task_type"),
                        "model_provider": entry.get("model_provider"),
                        "created_at": entry.get("created_at"),
                    },
                )
            )
        return targets

    def _transcript_cache_targets(
        self,
        *,
        event: CaptureEvent,
        identifiers: tuple[str, ...],
        event_policy: RetentionPolicy | None,
        fallback_retention_class: str | None,
        privacy_class: str | None,
        as_of: datetime,
    ) -> list[RetentionTarget]:
        if self.db is None:
            return []
        entries = self.db.list_transcript_chunks_for_contexts(identifiers)
        by_context: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            context_id = str(entry.get("context_id") or "")
            if context_id:
                by_context.setdefault(context_id, []).append(entry)
        targets: list[RetentionTarget] = []
        for context_id, context_entries in sorted(by_context.items()):
            policy = self._policy_for(
                "transcript_cache",
                context_id,
                fallback=event_policy,
                as_of=as_of,
            )
            eligible, reason = self._eligibility(policy, as_of=as_of)
            targets.append(
                RetentionTarget(
                    event_id=event.event_id,
                    target_type="transcript_cache",
                    target_id=context_id,
                    retention_scope="transcript_cache",
                    retention_class=_retention_class({}, policy)
                    or fallback_retention_class,
                    privacy_class=privacy_class,
                    eligible=eligible,
                    eligibility_reason=reason,
                    policy=policy,
                    exists=True,
                    context_id=context_id,
                    metadata={
                        "chunk_count": len(context_entries),
                        "model_providers": sorted(
                            {
                                str(entry.get("model_provider"))
                                for entry in context_entries
                                if entry.get("model_provider")
                            }
                        ),
                        "updated_at": max(
                            str(entry.get("updated_at") or "")
                            for entry in context_entries
                        ),
                    },
                )
            )
        return targets

    def _embedding_targets(
        self,
        *,
        event: CaptureEvent,
        identifiers: tuple[str, ...],
        event_policy: RetentionPolicy | None,
        fallback_retention_class: str | None,
        privacy_class: str | None,
        as_of: datetime,
        source_paths: tuple[str, ...],
    ) -> list[RetentionTarget]:
        if self.db is None:
            return []
        source_path_ids = tuple(
            _relative_id(Path(path), self.layout.vault_root)
            if Path(path).is_absolute()
            else str(path)
            for path in source_paths
        )
        documents = self.db.list_archivist_corpus_documents_for_sources(
            source_ids=identifiers,
            paths=source_path_ids,
        )
        candidate_keys = tuple(
            str(document.get("candidate_key"))
            for document in documents
            if document.get("candidate_key")
        )
        embeddings = self.db.list_archivist_corpus_embeddings_for_candidate_keys(
            candidate_keys
        )
        targets: list[RetentionTarget] = []
        for embedding in embeddings:
            candidate_key = str(embedding.get("candidate_key") or "")
            provider = str(embedding.get("provider") or "")
            model = str(embedding.get("model") or "")
            target_id = f"{candidate_key}:{provider}:{model}"
            policy = self._policy_for(
                "embedding",
                target_id,
                fallback=event_policy,
                as_of=as_of,
            )
            eligible, reason = self._eligibility(policy, as_of=as_of)
            targets.append(
                RetentionTarget(
                    event_id=event.event_id,
                    target_type="embedding",
                    target_id=target_id,
                    retention_scope="embedding",
                    retention_class=_retention_class({}, policy)
                    or fallback_retention_class,
                    privacy_class=privacy_class,
                    eligible=eligible,
                    eligibility_reason=reason,
                    policy=policy,
                    exists=True,
                    candidate_key=candidate_key,
                    embedding_provider=provider,
                    embedding_model=model,
                    metadata={"updated_at": embedding.get("updated_at")},
                )
            )
        return targets

    def _distilled_paths_for_event(
        self,
        event: CaptureEvent,
        artifact_links: tuple[ArtifactLink, ...],
    ) -> dict[str, tuple[tuple[str, str], ...]]:
        event_specs = _path_specs_from_mapping(event.payload)
        by_link: dict[str, list[tuple[str, str]]] = {}
        for link in artifact_links:
            specs = list(event_specs)
            specs.extend(_path_specs_from_mapping(link.metadata))
            if link.artifact_type.lower() in {"transcript"}:
                specs.extend(
                    _explicit_path_specs(
                        link.metadata,
                        keys=("path", "file_path", "source_path"),
                        scope="transcript_file",
                    )
                )
            by_link[link.artifact_link_id] = _dedupe_path_specs(specs)
        return {key: tuple(value) for key, value in by_link.items()}

    def _event_identifiers(
        self,
        event: CaptureEvent,
        artifact_links: tuple[ArtifactLink, ...],
    ) -> tuple[str, ...]:
        values: list[str] = [event.event_id]
        for value in (event.native_event_id, event.event_hash):
            cleaned = _clean_text(value)
            if cleaned:
                values.append(cleaned)
        for link in artifact_links:
            values.extend([link.artifact_id, link.artifact_link_id])
        return tuple(dict.fromkeys(value for value in values if value))

    def _policy_for(
        self,
        target_type: str,
        target_id: str,
        *,
        fallback: RetentionPolicy | None,
        as_of: datetime,
    ) -> RetentionPolicy | None:
        policies = self.event_store.list_retention_policies(
            target_type=target_type,
            target_id=target_id,
        )
        return _select_policy(policies, as_of=as_of) or fallback

    def _eligibility(
        self,
        policy: RetentionPolicy | None,
        *,
        as_of: datetime,
        path_status: _PathStatus | None = None,
        tombstoned: bool = False,
    ) -> tuple[bool, str]:
        if tombstoned:
            return False, "content already deleted"
        if path_status is not None and not path_status.safe:
            return False, path_status.reason or "unsafe path"
        if policy is None:
            return False, "missing retention policy"
        if policy.legal_hold:
            return False, "retention policy has legal hold"
        action = policy.action.lower()
        if action in IMMEDIATE_RETENTION_ACTIONS:
            return True, "eligible"
        if action not in DELETABLE_RETENTION_ACTIONS:
            return False, f"retention action is {policy.action}"
        due_at = _parse_datetime(policy.delete_after) or _parse_datetime(
            policy.retain_until
        )
        if due_at is None:
            return False, "retention policy has no expiry timestamp"
        if due_at > as_of:
            return False, f"not eligible until {due_at.isoformat()}"
        return True, "eligible"

    def _path_status(self, path_text: str) -> _PathStatus:
        candidate = Path(path_text).expanduser()
        if not candidate.is_absolute():
            candidate = self.layout.vault_root / candidate
        if candidate.is_symlink():
            return _PathStatus(
                path=candidate,
                safe=False,
                exists=True,
                reason="refusing to delete symlink",
            )
        resolved = candidate.resolve(strict=False)
        allowed_roots = self._allowed_roots()
        if not any(_is_relative_to(resolved, root) for root in allowed_roots):
            return _PathStatus(
                path=resolved,
                safe=False,
                exists=resolved.exists(),
                reason="path is outside configured retention roots",
            )
        if resolved.exists() and not resolved.is_file():
            return _PathStatus(
                path=resolved,
                safe=False,
                exists=True,
                reason="path is not a regular file",
            )
        return _PathStatus(path=resolved, safe=True, exists=resolved.exists())

    def _allowed_roots(self) -> tuple[Path, ...]:
        roots = (
            self.layout.vault_root,
            self.layout.raw_root,
            self.layout.library_root,
            self.layout.wiki_root,
            self.layout.system_root,
        )
        return tuple(root.resolve(strict=False) for root in roots)

    def _is_raw_root_path(self, path_text: str | None) -> bool:
        if not path_text:
            return False
        resolved = Path(path_text).expanduser()
        if not resolved.is_absolute():
            resolved = self.layout.vault_root / resolved
        return _is_relative_to(
            resolved.resolve(strict=False),
            self.layout.raw_root.resolve(strict=False),
        )

    def _delete_target(self, target: RetentionTarget) -> _DeletionOutcome:
        if target.path:
            return self._delete_file_target(target)
        if target.target_type == "llm_cache" and self.db is not None and target.cache_key:
            deleted = self.db.delete_llm_cache_entries((target.cache_key,))
            return _DeletionOutcome(
                status="deleted" if deleted else "not_found",
                message="deleted llm cache entry" if deleted else "llm cache entry not found",
            )
        if (
            target.target_type == "transcript_cache"
            and self.db is not None
            and target.context_id
        ):
            deleted = self.db.delete_transcript_chunks_for_contexts(
                (target.context_id,)
            )
            return _DeletionOutcome(
                status="deleted" if deleted else "not_found",
                message="deleted transcript cache entries"
                if deleted
                else "transcript cache entries not found",
            )
        if (
            target.target_type == "embedding"
            and self.db is not None
            and target.candidate_key
            and target.embedding_provider
            and target.embedding_model
        ):
            deleted = self.db.delete_archivist_corpus_embedding(
                candidate_key=target.candidate_key,
                provider=target.embedding_provider,
                model=target.embedding_model,
            )
            return _DeletionOutcome(
                status="deleted" if deleted else "not_found",
                message="deleted embedding row" if deleted else "embedding row not found",
            )
        return _DeletionOutcome(status="skipped", message="target has no delete handler")

    def _delete_file_target(self, target: RetentionTarget) -> _DeletionOutcome:
        if not target.path:
            return _DeletionOutcome(status="skipped", message="target has no path")
        path_status = self._path_status(target.path)
        if not path_status.safe:
            return _DeletionOutcome(
                status="skipped",
                message=path_status.reason or "unsafe path",
            )
        if not path_status.exists:
            return _DeletionOutcome(status="not_found", message="file already absent")
        size = path_status.path.stat().st_size
        path_status.path.unlink()
        return _DeletionOutcome(
            status="deleted",
            message="deleted file content",
            bytes_deleted=size,
        )

    def _record_expiry_audit(
        self,
        target: RetentionTarget,
        *,
        outcome: _DeletionOutcome,
        actor: str | None,
        reason: str,
        as_of: datetime,
    ) -> ProvenanceRecord | None:
        if outcome.status not in {"deleted", "not_found"}:
            return None
        audit_metadata = {
            "reason": reason,
            "retention_scope": target.retention_scope,
            "retention_class": target.retention_class,
            "privacy_class": target.privacy_class,
            "policy": _retention_policy_payload(target.policy),
            "target": target.to_dict(),
            "outcome": {
                "status": outcome.status,
                "message": outcome.message,
                "bytes_deleted": outcome.bytes_deleted,
            },
        }
        tombstone = {
            "content_deleted": outcome.status in {"deleted", "not_found"},
            "deleted_at": datetime.now(timezone.utc).isoformat(),
            "deleted_by": actor,
            "delete_reason": reason,
            "retention_scope": target.retention_scope,
            "operation_as_of": as_of.isoformat(),
            "outcome": outcome.status,
        }
        if target.raw_ref_id:
            self.event_store.mark_raw_ref_content_deleted(
                target.raw_ref_id,
                metadata={"retention_deletion": tombstone},
            )
        if target.artifact_link_id:
            self.event_store.mark_artifact_link_content_deleted(
                target.artifact_link_id,
                metadata={"retention_deletion": tombstone},
            )
        audit_target_type = target.target_type
        audit_target_id = target.target_id
        if target.target_type in {"llm_cache", "transcript_cache", "embedding"}:
            audit_target_type = "event"
            audit_target_id = target.event_id
        return self.event_store.upsert_provenance_record(
            ProvenanceRecord(
                target_type=audit_target_type,
                target_id=audit_target_id,
                operation="retention.expired",
                actor=actor,
                tool="capture_retention_service",
                metadata=audit_metadata,
            )
        )


def _inspection_payload(
    targets: list[RetentionTarget],
    *,
    as_of: datetime,
) -> dict[str, Any]:
    by_scope: dict[str, dict[str, int]] = {}
    for target in targets:
        bucket = by_scope.setdefault(
            target.retention_scope,
            {"total": 0, "eligible": 0},
        )
        bucket["total"] += 1
        if target.eligible:
            bucket["eligible"] += 1
    return {
        "as_of": as_of.isoformat(),
        "targets": [target.to_dict() for target in targets],
        "total": len(targets),
        "eligible": sum(1 for target in targets if target.eligible),
        "by_scope": by_scope,
    }


def _expiry_payload(
    operations: list[dict[str, Any]],
    *,
    audit_records: list[dict[str, Any]],
    dry_run: bool,
    delete_raw: bool,
    delete_distilled: bool,
    as_of: datetime,
) -> dict[str, Any]:
    by_status: dict[str, int] = {}
    by_scope: dict[str, dict[str, int]] = {}
    bytes_deleted = 0
    for operation in operations:
        status = str(operation.get("status") or "unknown")
        scope = str(operation.get("retention_scope") or "unknown")
        by_status[status] = by_status.get(status, 0) + 1
        scope_bucket = by_scope.setdefault(scope, {})
        scope_bucket[status] = scope_bucket.get(status, 0) + 1
        bytes_deleted += int(operation.get("bytes_deleted") or 0)
    return {
        "as_of": as_of.isoformat(),
        "dry_run": dry_run,
        "delete_raw": delete_raw,
        "delete_distilled": delete_distilled,
        "operations": operations,
        "audit_records": audit_records,
        "total": len(operations),
        "by_status": by_status,
        "by_scope": by_scope,
        "bytes_deleted": bytes_deleted,
    }


def _operation_payload(
    target: RetentionTarget,
    *,
    status: str,
    message: str,
    bytes_deleted: int = 0,
    audit_record_id: str | None = None,
) -> dict[str, Any]:
    payload = {
        "status": status,
        "message": message,
        "bytes_deleted": bytes_deleted,
        "audit_record_id": audit_record_id,
        **target.to_dict(),
    }
    return {key: value for key, value in payload.items() if value is not None}


def _retention_policy_payload(policy: RetentionPolicy | None) -> dict[str, Any] | None:
    if policy is None:
        return None
    return {
        "retention_id": policy.retention_id,
        "target_type": policy.target_type,
        "target_id": policy.target_id,
        "policy_name": policy.policy_name,
        "action": policy.action,
        "retain_until": _json_safe_datetime(policy.retain_until),
        "delete_after": _json_safe_datetime(policy.delete_after),
        "legal_hold": policy.legal_hold,
        "metadata": dict(policy.metadata),
    }


def _provenance_payload(record: ProvenanceRecord) -> dict[str, Any]:
    return {
        "provenance_id": record.provenance_id,
        "target_type": record.target_type,
        "target_id": record.target_id,
        "operation": record.operation,
        "actor": record.actor,
        "tool": record.tool,
        "fingerprint": record.fingerprint,
        "occurred_at": _json_safe_datetime(record.occurred_at),
        "metadata": dict(record.metadata),
    }


def _privacy_class(
    privacy: Mapping[str, Any],
    annotations: tuple[PrivacyAnnotation, ...],
) -> str | None:
    for key in ("privacy_class", "classification", "class"):
        value = _clean_text(privacy.get(key))
        if value:
            return value
    for annotation in annotations:
        value = _clean_text(annotation.classification)
        if value:
            return value
    return None


def _retention_class(
    metadata: Mapping[str, Any],
    policy: RetentionPolicy | None,
) -> str | None:
    for key in ("retention_class", "policy_name", "policy", "class"):
        value = _clean_text(metadata.get(key))
        if value:
            return value
    if policy is not None:
        return policy.policy_name
    return None


def _select_policy(
    policies: Iterable[RetentionPolicy],
    *,
    as_of: datetime,
) -> RetentionPolicy | None:
    policy_list = tuple(policies)
    if not policy_list:
        return None
    legal_hold = next((policy for policy in policy_list if policy.legal_hold), None)
    if legal_hold is not None:
        return legal_hold
    due_policy = next(
        (
            policy
            for policy in policy_list
            if policy.action.lower() in DELETABLE_RETENTION_ACTIONS
            and (
                (_parse_datetime(policy.delete_after) or _parse_datetime(policy.retain_until))
                is not None
            )
            and (
                _parse_datetime(policy.delete_after)
                or _parse_datetime(policy.retain_until)
            )
            <= as_of
        ),
        None,
    )
    if due_policy is not None:
        return due_policy
    immediate = next(
        (
            policy
            for policy in policy_list
            if policy.action.lower() in IMMEDIATE_RETENTION_ACTIONS
        ),
        None,
    )
    if immediate is not None:
        return immediate
    return policy_list[0]


def _path_specs_from_mapping(value: Mapping[str, Any]) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    for key, item in value.items():
        if key in _DISTILLED_OUTPUT_KEYS:
            scope = _scope_for_path_key(key)
            text = _clean_text(item)
            if text:
                specs.append((scope, text))
    output_paths = value.get("output_paths")
    if isinstance(output_paths, Mapping):
        for key, item in output_paths.items():
            text = _clean_text(item)
            if text:
                specs.append((_scope_for_path_key(str(key)), text))
    derived_outputs = value.get("derived_outputs")
    if isinstance(derived_outputs, (list, tuple)):
        for output in derived_outputs:
            if not isinstance(output, Mapping):
                continue
            path = _clean_text(output.get("path") or output.get("location"))
            if not path:
                continue
            output_type = str(
                output.get("output_type") or output.get("kind") or output.get("type") or ""
            )
            specs.append((_scope_for_path_key(output_type), path))
    normalized_metadata = value.get("normalized_metadata")
    if isinstance(normalized_metadata, Mapping):
        specs.extend(_path_specs_from_mapping(normalized_metadata))
    canonical_record = value.get("canonical_record")
    if isinstance(canonical_record, Mapping):
        specs.extend(_path_specs_from_mapping(canonical_record))
    return specs


def _explicit_path_specs(
    value: Mapping[str, Any],
    *,
    keys: tuple[str, ...],
    scope: str,
) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    for key in keys:
        text = _clean_text(value.get(key))
        if text:
            specs.append((scope, text))
    return specs


def _scope_for_path_key(value: str) -> str:
    normalized = value.lower()
    if "transcript" in normalized:
        return "transcript_file"
    if "summary" in normalized:
        return "summary_file"
    return "derived_file"


def _dedupe_path_specs(values: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, str]] = []
    for scope, path in values:
        key = (scope, path)
        if key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def _dedupe_targets(values: Iterable[RetentionTarget]) -> list[RetentionTarget]:
    seen: set[tuple[str, str, str]] = set()
    result: list[RetentionTarget] = []
    for target in values:
        key = (target.retention_scope, target.target_type, target.target_id)
        if key in seen:
            continue
        seen.add(key)
        result.append(target)
    return result


def _is_content_deleted(metadata: Mapping[str, Any]) -> bool:
    deletion = metadata.get("retention_deletion")
    return isinstance(deletion, Mapping) and bool(deletion.get("content_deleted"))


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    elif value is None:
        return None
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_safe_datetime(value: Any) -> Any:
    parsed = _parse_datetime(value)
    if parsed is not None:
        return parsed.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _relative_id(path: Path, root: Path) -> str:
    try:
        return path.resolve(strict=False).relative_to(root.resolve(strict=False)).as_posix()
    except ValueError:
        return str(path)
