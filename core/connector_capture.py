"""Shared capture lifecycle adapter for artifact-producing connectors."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from .artifacts import KnowledgeArtifact
from .capture_event_store import CaptureEventStore
from .capture_lifecycle import CaptureLifecycleResult, CaptureLifecycleService
from .config import Config, config
from .metadata_db import MetadataDB, get_metadata_db
from .path_layout import PathLayout, build_path_layout
from .postgres import open_postgres_connection, resolve_postgres_settings
from .postgres_migrations import apply_postgres_migrations


class ConnectorCaptureQueue:
    """Queue connector artifacts through the canonical capture lifecycle."""

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
        capture_event_store: CaptureEventStore | None = None,
    ) -> None:
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.db = db or get_metadata_db()
        self.capture_event_store = capture_event_store

    @contextmanager
    def lifecycle(self) -> Iterator[CaptureLifecycleService]:
        """Yield a lifecycle service, opening the event store when enabled."""
        if self.capture_event_store is not None:
            yield self._service(self.capture_event_store)
            return

        settings = resolve_postgres_settings(self.config)
        if not settings.enabled:
            yield self._service(None)
            return

        self.layout.ensure_directories()
        with open_postgres_connection(settings) as conn:
            apply_postgres_migrations(
                conn,
                schema=settings.schema,
                lock_id=settings.migration_lock_id,
            )
            store = CaptureEventStore(
                conn,
                schema=settings.schema,
                raw_roots=connector_raw_roots(self.layout),
            )
            yield self._service(store)

    def _service(
        self,
        capture_event_store: CaptureEventStore | None,
    ) -> CaptureLifecycleService:
        return CaptureLifecycleService(
            self.config,
            layout=self.layout,
            db=self.db,
            capture_event_store=capture_event_store,
        )

    def queue_artifact(
        self,
        lifecycle: CaptureLifecycleService,
        artifact: KnowledgeArtifact,
        *,
        artifact_type: str,
        source: Mapping[str, Any] | str,
        session: Mapping[str, Any] | None = None,
        event: Mapping[str, Any] | None = None,
        raw_path: str | Path | None = None,
        priority: int = 0,
        capabilities: Iterable[str] | None = None,
    ) -> CaptureLifecycleResult:
        """Persist one artifact queue row and optional capture event records."""
        return lifecycle.capture_to_queue(
            artifact_type=artifact_type,
            payload=artifact.to_dict(),
            source=source,
            session=session,
            event=event,
            raw_path=raw_path if lifecycle.capture_event_store is not None else None,
            queue_artifact_id=artifact.id,
            priority=priority,
            capabilities=capabilities if capabilities is not None else artifact.capabilities,
        )

    def queue_payload(
        self,
        lifecycle: CaptureLifecycleService,
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
        """Persist a connector payload that has not yet been materialized."""
        return lifecycle.capture_to_queue(
            artifact_type=artifact_type,
            payload=payload,
            source=source,
            session=session,
            event=event,
            raw_path=raw_path if lifecycle.capture_event_store is not None else None,
            queue_artifact_id=queue_artifact_id,
            priority=priority,
            capabilities=capabilities,
        )


def connector_raw_roots(layout: PathLayout) -> tuple[Path, ...]:
    """Roots under which connectors may record immutable raw references."""
    return (layout.raw_root, layout.library_root, layout.vault_root)
