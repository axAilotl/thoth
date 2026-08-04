"""Canonical capture lifecycle service for artifact-first ingestion."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Mapping

from .artifact_identity import native_id_from_payload
from .capture_event_store import (
    ArtifactLink,
    CaptureEvent,
    CaptureEventStore,
    CaptureSession,
    CaptureSource,
    RawArtifactRef,
)
from .config import Config, config
from .ingestion_runtime import (
    IngestionDispatchResult,
    KnowledgeArtifactRuntime,
)
from .metadata_db import IngestionQueueEntry, MetadataDB, get_metadata_db
from .path_layout import PathLayout
from .postgres import PostgresConfigError, resolve_postgres_settings
from .time_utils import utc_now_iso as _now_iso


_CAPTURE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "thoth.capture_lifecycle")


class CaptureLifecycleError(RuntimeError):
    """Raised when a capture lifecycle operation cannot complete safely."""


class CaptureLifecycleConfigError(CaptureLifecycleError):
    """Raised when required capture lifecycle dependencies are not configured."""


@dataclass(frozen=True)
class CaptureLifecycleResult:
    """Stable IDs and provenance returned from the capture lifecycle."""

    lifecycle_id: str
    queue_artifact_id: str
    artifact_id: str
    artifact_type: str
    source_name: str
    event_id: str
    source_id: str | None = None
    session_id: str | None = None
    raw_ref_id: str | None = None
    artifact_link_id: str | None = None
    queue_status: str = "pending"
    canonical_record: dict[str, Any] = field(default_factory=dict)
    dispatch_result: IngestionDispatchResult | None = None
    query_result: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "lifecycle_id": self.lifecycle_id,
            "queue_artifact_id": self.queue_artifact_id,
            "artifact_id": self.artifact_id,
            "artifact_type": self.artifact_type,
            "source_name": self.source_name,
            "event_id": self.event_id,
            "source_id": self.source_id,
            "session_id": self.session_id,
            "raw_ref_id": self.raw_ref_id,
            "artifact_link_id": self.artifact_link_id,
            "queue_status": self.queue_status,
            "canonical_record": dict(self.canonical_record),
            "dispatch_result": asdict(self.dispatch_result)
            if self.dispatch_result
            else None,
            "query_result": self.query_result,
        }


class CaptureLifecycleService(KnowledgeArtifactRuntime):
    """One service boundary for capture, queueing, processing, and query."""

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
        capture_event_store: CaptureEventStore | None = None,
    ) -> None:
        super().__init__(runtime_config, layout=layout, db=db)
        self.capture_event_store = capture_event_store

    def capture(
        self,
        *,
        artifact_type: str,
        payload: Mapping[str, Any],
        source: Mapping[str, Any] | str,
        session: Mapping[str, Any] | None = None,
        event: Mapping[str, Any] | None = None,
        raw_path: str | Path | None = None,
        queue_artifact_id: str | None = None,
        priority: int = 0,
        capabilities: Iterable[str] | None = None,
    ) -> CaptureLifecycleResult:
        """Capture and queue an artifact without invoking processors."""
        return self.capture_to_queue(
            artifact_type=artifact_type,
            payload=payload,
            source=source,
            session=session,
            event=event,
            raw_path=raw_path,
            queue_artifact_id=queue_artifact_id,
            priority=priority,
            capabilities=capabilities,
        )

    def capture_to_queue(
        self,
        *,
        artifact_type: str,
        payload: Mapping[str, Any],
        source: Mapping[str, Any] | str,
        session: Mapping[str, Any] | None = None,
        event: Mapping[str, Any] | None = None,
        raw_path: str | Path | None = None,
        queue_artifact_id: str | None = None,
        priority: int = 0,
        capabilities: Iterable[str] | None = None,
    ) -> CaptureLifecycleResult:
        """Normalize capture metadata and persist one queue entry."""
        self._validate_event_store_policy()

        artifact_kind = _required_string(artifact_type, "artifact_type").lower()
        payload_obj = _json_object(payload, "payload")
        source_obj = _source_context(source, payload_obj)
        session_obj = _json_object(session, "session") if session is not None else None
        event_obj = _json_object(event, "event") if event is not None else {}

        source_name = _required_string(
            source_obj.get("source_name")
            or source_obj.get("source")
            or source_obj.get("name")
            or payload_obj.get("source")
            or payload_obj.get("source_type"),
            "source.source_name",
        )
        source_type = _required_string(
            source_obj.get("source_type") or payload_obj.get("source_type") or source_name,
            "source.source_type",
        )
        source_id = _clean_string(source_obj.get("source_id")) or _stable_id(
            "source",
            {"source_name": source_name, "source_type": source_type},
        )

        persisted_source = self._upsert_capture_source(
            source_id=source_id,
            source_name=source_name,
            source_type=source_type,
            source=source_obj,
        )
        if persisted_source:
            source_id = persisted_source.source_id

        event_type = _clean_string(
            event_obj.get("event_type") or event_obj.get("type")
        ) or artifact_kind
        captured_at = _clean_string(
            event_obj.get("captured_at")
            or payload_obj.get("captured_at")
            or payload_obj.get("timestamp")
            or payload_obj.get("ingested_at")
        ) or _now_iso()
        occurred_at = _clean_string(
            event_obj.get("occurred_at")
            or payload_obj.get("occurred_at")
            or payload_obj.get("created_at")
            or payload_obj.get("published_at")
            or payload_obj.get("timestamp")
        )
        native_event_id = _clean_string(
            event_obj.get("native_event_id")
            or event_obj.get("native_id")
            or _native_id_from_payload(artifact_kind, payload_obj)
        )
        event_hash = _clean_string(event_obj.get("event_hash")) or _capture_hash(
            artifact_type=artifact_kind,
            source_name=source_name,
            native_event_id=native_event_id,
            payload=payload_obj,
        )
        session_id = self._resolve_session_id(
            source_id=source_id,
            session=session_obj,
            event_hash=event_hash,
        )
        event_id = _clean_string(event_obj.get("event_id")) or _stable_id(
            "event",
            {
                "source_id": source_id,
                "event_type": event_type,
                "native_event_id": native_event_id,
                "event_hash": event_hash,
            },
        )

        queue_id = _clean_string(queue_artifact_id) or _queue_id_from_payload(
            artifact_kind,
            payload_obj,
        )
        if not queue_id:
            queue_id = _stable_id(
                "artifact",
                {
                    "source_id": source_id,
                    "event_hash": event_hash,
                    "artifact_type": artifact_kind,
                },
            )

        raw_ref = self._build_raw_ref(
            raw_path=raw_path,
            source_id=source_id,
            session_id=session_id,
            event_id=event_id,
        )
        normalized_payload = self._normalize_queue_payload(
            payload_obj,
            artifact_type=artifact_kind,
            queue_artifact_id=queue_id,
            source_name=source_name,
            source_type=source_type,
            source_id=source_id,
            session_id=session_id,
            event_id=event_id,
            raw_ref=raw_ref,
        )
        entry = self._build_queue_entry(
            queue_artifact_id=queue_id,
            artifact_type=artifact_kind,
            source_name=source_name,
            payload=normalized_payload,
            priority=priority,
            capabilities=capabilities,
            captured_at=captured_at,
        )
        artifact = self.materialize_artifact(entry)
        canonical_identity = self._canonicalize_artifact(
            artifact,
            artifact_type=artifact_kind,
        )
        if canonical_identity is not None:
            normalized_payload["normalized_metadata"] = dict(
                artifact.normalized_metadata
            )
            entry = replace(
                entry,
                payload_json=_stable_json(normalized_payload),
            )
        if capabilities is None:
            entry = replace(
                entry,
                capabilities_json=json.dumps(
                    list(artifact.capabilities),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )

        capture_event = self._upsert_capture_event(
            source_id=source_id,
            session=session_obj,
            session_id=session_id,
            event_id=event_id,
            event_type=event_type,
            native_event_id=native_event_id,
            occurred_at=occurred_at,
            captured_at=captured_at,
            event_hash=event_hash,
            payload={
                **payload_obj,
                "artifact_id": artifact.id,
                "queue_artifact_id": queue_id,
                "artifact_type": artifact_kind,
            },
            event=event_obj,
        )
        if capture_event and capture_event.event_id != event_id:
            event_id = capture_event.event_id
            raw_ref = self._build_raw_ref(
                raw_path=raw_path,
                source_id=source_id,
                session_id=session_id,
                event_id=event_id,
            )
            normalized_payload = self._normalize_queue_payload(
                payload_obj,
                artifact_type=artifact_kind,
                queue_artifact_id=queue_id,
                source_name=source_name,
                source_type=source_type,
                source_id=source_id,
                session_id=session_id,
                event_id=event_id,
                raw_ref=raw_ref,
            )
            entry = replace(
                entry,
                payload_json=_stable_json(normalized_payload),
            )
            artifact = self.materialize_artifact(entry)
            canonical_identity = self._canonicalize_artifact(
                artifact,
                artifact_type=artifact_kind,
            )
            if canonical_identity is not None:
                normalized_payload["normalized_metadata"] = dict(
                    artifact.normalized_metadata
                )
                entry = replace(
                    entry,
                    payload_json=_stable_json(normalized_payload),
                )

        raw_ref_id = self._upsert_raw_ref(raw_ref)
        artifact_link_id = self._upsert_artifact_link(
            event_id=event_id,
            raw_ref_id=raw_ref_id,
            queue_artifact_id=queue_id,
            artifact_id=artifact.id,
            artifact_type=artifact_kind,
            canonical_id=canonical_identity.canonical_id
            if canonical_identity is not None
            else None,
        )
        stored_entry = self._upsert_queue_entry(entry)
        self._upsert_security_findings_from_queue_entry(
            stored_entry,
            event_id=event_id,
            raw_ref_id=raw_ref_id,
        )

        lifecycle_id = _stable_id(
            "lifecycle",
            {
                "queue_artifact_id": queue_id,
                "event_id": event_id,
                "artifact_type": artifact_kind,
            },
        )
        return CaptureLifecycleResult(
            lifecycle_id=lifecycle_id,
            queue_artifact_id=queue_id,
            artifact_id=artifact.id,
            artifact_type=artifact_kind,
            source_name=source_name,
            source_id=source_id,
            session_id=session_id,
            event_id=event_id,
            raw_ref_id=raw_ref_id,
            artifact_link_id=artifact_link_id,
            queue_status=stored_entry.status,
            canonical_record=artifact.canonical_record(),
        )

    async def run_lifecycle(
        self,
        *,
        artifact_type: str,
        payload: Mapping[str, Any],
        source: Mapping[str, Any] | str,
        session: Mapping[str, Any] | None = None,
        event: Mapping[str, Any] | None = None,
        raw_path: str | Path | None = None,
        queue_artifact_id: str | None = None,
        priority: int = 0,
        capabilities: Iterable[str] | None = None,
        process: bool = False,
        query: str | None = None,
        query_limit: int = 10,
    ) -> CaptureLifecycleResult:
        """Run capture through optional process/compile and query stages."""
        result = self.capture_to_queue(
            artifact_type=artifact_type,
            payload=payload,
            source=source,
            session=session,
            event=event,
            raw_path=raw_path,
            queue_artifact_id=queue_artifact_id,
            priority=priority,
            capabilities=capabilities,
        )

        dispatch_result = None
        if process:
            entry = self.db.get_ingestion_entry(result.queue_artifact_id)
            if entry is None:
                raise CaptureLifecycleError(
                    f"Queued artifact disappeared before processing: {result.queue_artifact_id}"
                )
            dispatch_result = await self.process_ingestion_entry(entry)

        query_result = None
        if query:
            from .agent_surface import AgentSurfaceService

            query_result = AgentSurfaceService(
                self.config,
                layout=self.layout,
                db=self.db,
            ).query_wiki(query, limit=query_limit)

        stored_entry = self.db.get_ingestion_entry(result.queue_artifact_id)
        return replace(
            result,
            queue_status=stored_entry.status if stored_entry else result.queue_status,
            dispatch_result=dispatch_result,
            query_result=query_result,
        )

    def _validate_event_store_policy(self) -> None:
        if self.capture_event_store is not None:
            return
        try:
            settings = resolve_postgres_settings(self.config)
        except PostgresConfigError as exc:
            raise CaptureLifecycleConfigError(str(exc)) from exc
        if settings.enabled:
            raise CaptureLifecycleConfigError(
                "database.capture_event_store is enabled; pass an initialized "
                "CaptureEventStore to CaptureLifecycleService"
            )

    def _upsert_capture_source(
        self,
        *,
        source_id: str,
        source_name: str,
        source_type: str,
        source: Mapping[str, Any],
    ) -> CaptureSource | None:
        if self.capture_event_store is None:
            return None
        return self.capture_event_store.upsert_source(
            CaptureSource(
                source_id=source_id,
                source_name=source_name,
                source_type=source_type,
                collector=_clean_string(source.get("collector")),
                account=_clean_string(source.get("account")),
                native_source_id=_clean_string(source.get("native_source_id")),
                base_uri=_clean_string(source.get("base_uri") or source.get("uri")),
                status=_clean_string(source.get("status")) or "active",
                config=_json_object(source.get("config"), "source.config"),
                metadata=_json_object(source.get("metadata"), "source.metadata"),
            )
        )

    def _upsert_capture_event(
        self,
        *,
        source_id: str,
        session: Mapping[str, Any] | None,
        session_id: str | None,
        event_id: str,
        event_type: str,
        native_event_id: str | None,
        occurred_at: str | None,
        captured_at: str,
        event_hash: str,
        payload: Mapping[str, Any],
        event: Mapping[str, Any],
    ) -> CaptureEvent | None:
        if self.capture_event_store is None:
            return None
        if session and session_id:
            self.capture_event_store.upsert_session(
                CaptureSession(
                    source_id=source_id,
                    session_id=session_id,
                    native_session_id=_clean_string(
                        session.get("native_session_id")
                        or session.get("native_id")
                    ),
                    session_type=_clean_string(
                        session.get("session_type") or session.get("type")
                    )
                    or "capture",
                    status=_clean_string(session.get("status")) or "open",
                    started_at=session.get("started_at"),
                    ended_at=session.get("ended_at"),
                    metadata=_json_object(session.get("metadata"), "session.metadata"),
                    provenance=_json_object(
                        session.get("provenance"),
                        "session.provenance",
                    ),
                )
            )
        return self.capture_event_store.upsert_event(
            CaptureEvent(
                source_id=source_id,
                session_id=session_id,
                event_id=event_id,
                native_event_id=native_event_id,
                event_type=event_type,
                occurred_at=occurred_at,
                captured_at=captured_at,
                event_hash=event_hash,
                payload=dict(payload),
                privacy=_json_object(event.get("privacy"), "event.privacy"),
                retention=_json_object(event.get("retention"), "event.retention"),
                provenance=_json_object(
                    event.get("provenance"),
                    "event.provenance",
                ),
            )
        )

    def _upsert_raw_ref(self, raw_ref: RawArtifactRef | None) -> str | None:
        if raw_ref is None or self.capture_event_store is None:
            return raw_ref.raw_ref_id if raw_ref else None
        return self.capture_event_store.upsert_raw_ref(raw_ref).raw_ref_id

    def _upsert_artifact_link(
        self,
        *,
        event_id: str,
        raw_ref_id: str | None,
        queue_artifact_id: str,
        artifact_id: str,
        artifact_type: str,
        canonical_id: str | None = None,
    ) -> str | None:
        if self.capture_event_store is None:
            return None
        metadata = {
            "canonical_artifact_id": artifact_id,
            "queue_artifact_id": queue_artifact_id,
        }
        if canonical_id:
            metadata["canonical_id"] = canonical_id
        link = ArtifactLink(
            artifact_link_id=_stable_id(
                "artifact-link",
                {
                    "event_id": event_id,
                    "queue_artifact_id": queue_artifact_id,
                    "artifact_type": artifact_type,
                },
            ),
            event_id=event_id,
            raw_ref_id=raw_ref_id,
            artifact_id=queue_artifact_id,
            artifact_type=artifact_type,
            metadata=metadata,
        )
        return self.capture_event_store.upsert_artifact_link(link).artifact_link_id

    def _resolve_session_id(
        self,
        *,
        source_id: str,
        session: Mapping[str, Any] | None,
        event_hash: str,
    ) -> str | None:
        if not session:
            return None
        return _clean_string(session.get("session_id")) or _stable_id(
            "session",
            {
                "source_id": source_id,
                "session_type": session.get("session_type") or session.get("type"),
                "native_session_id": session.get("native_session_id")
                or session.get("native_id"),
                "event_hash": event_hash
                if not (
                    session.get("native_session_id") or session.get("native_id")
                )
                else None,
            },
        )

    def _build_raw_ref(
        self,
        *,
        raw_path: str | Path | None,
        source_id: str,
        session_id: str | None,
        event_id: str,
    ) -> RawArtifactRef | None:
        if raw_path is None:
            return None
        raw_roots = (
            self.capture_event_store.raw_roots
            if self.capture_event_store is not None
            else (self.layout.raw_root,)
        )
        raw_ref = RawArtifactRef.from_file(
            raw_path,
            source_id=source_id,
            session_id=session_id,
            event_id=event_id,
            raw_roots=raw_roots,
        )
        return replace(
            raw_ref,
            raw_ref_id=_stable_id(
                "raw-ref",
                {
                    "source_id": source_id,
                    "sha256": raw_ref.sha256,
                    "path": raw_ref.path,
                },
            ),
        )

    def _normalize_queue_payload(
        self,
        payload: Mapping[str, Any],
        *,
        artifact_type: str,
        queue_artifact_id: str,
        source_name: str,
        source_type: str,
        source_id: str,
        session_id: str | None,
        event_id: str,
        raw_ref: RawArtifactRef | None,
    ) -> dict[str, Any]:
        normalized = dict(payload)
        normalized.setdefault("source", source_name)
        normalized.setdefault("source_type", source_type)
        if not _native_id_from_payload(artifact_type, normalized):
            normalized.setdefault("artifact_id", queue_artifact_id)
            normalized.setdefault("id", queue_artifact_id)

        metadata = _json_object(
            normalized.get("normalized_metadata"),
            "payload.normalized_metadata",
        )
        metadata.update(
            {
                "capture_event_id": event_id,
                "capture_source_id": source_id,
                "queue_artifact_id": queue_artifact_id,
            }
        )
        if session_id:
            metadata["capture_session_id"] = session_id
        normalized["normalized_metadata"] = metadata

        if raw_ref is not None and not isinstance(normalized.get("raw_payload"), Mapping):
            normalized["raw_payload"] = {
                "path": raw_ref.path,
                "sha256": raw_ref.sha256,
                "size_bytes": raw_ref.size_bytes,
                "media_type": raw_ref.mime_type,
                "immutable": raw_ref.immutable,
            }
        return normalized

    def _build_queue_entry(
        self,
        *,
        queue_artifact_id: str,
        artifact_type: str,
        source_name: str,
        payload: Mapping[str, Any],
        priority: int,
        capabilities: Iterable[str] | None,
        captured_at: str,
    ) -> IngestionQueueEntry:
        existing = self.db.get_ingestion_entry(queue_artifact_id)
        preserved_statuses = {
            "processing",
            "processed",
            "needs_review",
            "blocked",
            "failed",
            "reviewed",
            "rejected",
        }
        status = (
            existing.status
            if existing and existing.status in preserved_statuses
            else "pending"
        )
        return IngestionQueueEntry(
            artifact_id=queue_artifact_id,
            artifact_type=artifact_type,
            source=source_name,
            payload_json=_stable_json(payload),
            priority=int(priority),
            status=status,
            attempts=existing.attempts if existing else 0,
            last_error=existing.last_error if existing else None,
            next_attempt_at=existing.next_attempt_at if existing else captured_at,
            created_at=existing.created_at if existing else captured_at,
            processed_at=existing.processed_at if existing else None,
            review_json=existing.review_json if existing else None,
            capabilities_json=json.dumps(
                [str(item) for item in capabilities],
                ensure_ascii=False,
                sort_keys=True,
            )
            if capabilities is not None
            else None,
        )

    def _upsert_queue_entry(self, entry: IngestionQueueEntry) -> IngestionQueueEntry:
        if not self.db.upsert_ingestion_entry(entry):
            raise CaptureLifecycleError(
                f"Failed to persist ingestion queue entry {entry.artifact_id}"
            )
        stored_entry = self.db.get_ingestion_entry(entry.artifact_id)
        if stored_entry is None:
            raise CaptureLifecycleError(
                f"Persisted ingestion queue entry cannot be read: {entry.artifact_id}"
            )
        return stored_entry

    def _upsert_security_findings_from_queue_entry(
        self,
        entry: IngestionQueueEntry,
        *,
        event_id: str,
        raw_ref_id: str | None,
    ) -> None:
        if self.capture_event_store is None:
            return
        try:
            payload = json.loads(entry.payload_json or "{}")
        except Exception as exc:
            raise CaptureLifecycleError(
                f"Queued artifact {entry.artifact_id} has invalid payload_json"
            ) from exc
        if not isinstance(payload, Mapping):
            return
        metadata = payload.get("normalized_metadata")
        if not isinstance(metadata, Mapping):
            return
        self.capture_event_store.upsert_security_findings_from_metadata(
            metadata,
            event_id=event_id,
            raw_ref_id=raw_ref_id,
        )


def get_capture_lifecycle_service(
    runtime_config: Config | None = None,
    *,
    layout: PathLayout | None = None,
    db: MetadataDB | None = None,
    capture_event_store: CaptureEventStore | None = None,
) -> CaptureLifecycleService:
    """Build the canonical service connectors should use for captures."""
    return CaptureLifecycleService(
        runtime_config or config,
        layout=layout,
        db=db or get_metadata_db(),
        capture_event_store=capture_event_store,
    )


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_string(value: Any, field_name: str) -> str:
    text = _clean_string(value)
    if not text:
        raise ValueError(f"{field_name} is required")
    return text


def _json_object(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return dict(value)


def _source_context(
    source: Mapping[str, Any] | str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if isinstance(source, str):
        return {
            "source_name": source,
            "source_type": payload.get("source_type") or source,
        }
    if isinstance(source, Mapping):
        return dict(source)
    raise ValueError("source must be a source name or object")


def _queue_id_from_payload(
    artifact_type: str,
    payload: Mapping[str, Any],
) -> str | None:
    return _clean_string(
        payload.get("queue_artifact_id")
        or payload.get("artifact_id")
        or _native_id_from_payload(artifact_type, payload)
    )


def _native_id_from_payload(
    artifact_type: str,
    payload: Mapping[str, Any],
) -> str | None:
    return native_id_from_payload(artifact_type, payload)


def _capture_hash(
    *,
    artifact_type: str,
    source_name: str,
    native_event_id: str | None,
    payload: Mapping[str, Any],
) -> str:
    return hashlib.sha256(
        _stable_json(
            {
                "artifact_type": artifact_type,
                "source_name": source_name,
                "native_event_id": native_event_id,
                "payload": payload,
            }
        ).encode("utf-8")
    ).hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    return f"{prefix}:{uuid.uuid5(_CAPTURE_NAMESPACE, _stable_json(payload))}"


def _stable_json(value: Any) -> str:
    return json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value, key=str)]
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
