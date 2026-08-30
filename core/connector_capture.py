"""Shared capture lifecycle adapter for artifact-producing connectors."""

from __future__ import annotations

import hashlib
import json
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from .artifacts import KnowledgeArtifact
from .capture_event_store import CaptureEventStore
from .capture_lifecycle import CaptureLifecycleResult, CaptureLifecycleService
from .ccf_dualwrite import dual_write_requested
from .config import Config, config
from .metadata_db import MetadataDB, get_metadata_db
from .content_root_adapters import ContentAdapterError
from .content_roots import ContentRoot, ContentRootMode
from .path_layout import PathLayout, build_path_layout
from .postgres import open_postgres_connection, resolve_postgres_settings
from .postgres_migrations import apply_postgres_migrations


@dataclass(frozen=True)
class ConnectorRunContext:
    """Active connector run metadata used while queueing artifacts."""

    run_id: str
    checkpoint_id: str | None = None


_ACTIVE_CONNECTOR_RUN: ContextVar[ConnectorRunContext | None] = ContextVar(
    "thoth_active_connector_run",
    default=None,
)


@contextmanager
def connector_run_context(
    run_id: str,
    *,
    checkpoint_id: str | None = None,
) -> Iterator[ConnectorRunContext]:
    """Bind queued connector artifacts to a run history record."""
    context = ConnectorRunContext(run_id=run_id, checkpoint_id=checkpoint_id)
    token = _ACTIVE_CONNECTOR_RUN.set(context)
    try:
        yield context
    finally:
        _ACTIVE_CONNECTOR_RUN.reset(token)


def current_connector_run_context() -> ConnectorRunContext | None:
    """Return the active connector run context, if connector execution set one."""
    return _ACTIVE_CONNECTOR_RUN.get()


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
        result = lifecycle.capture_to_queue(
            artifact_type=artifact_type,
            payload=artifact.to_dict(),
            source=source,
            session=session,
            event=event,
            raw_path=_raw_path_for_stores(lifecycle, self.config, raw_path),
            queue_artifact_id=artifact.id,
            priority=priority,
            capabilities=capabilities if capabilities is not None else artifact.capabilities,
        )
        self._record_run_output(result)
        return result

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
        result = lifecycle.capture_to_queue(
            artifact_type=artifact_type,
            payload=payload,
            source=source,
            session=session,
            event=event,
            raw_path=_raw_path_for_stores(lifecycle, self.config, raw_path),
            queue_artifact_id=queue_artifact_id,
            priority=priority,
            capabilities=capabilities,
        )
        self._record_run_output(result)
        return result

    def _record_run_output(self, result: CaptureLifecycleResult) -> None:
        context = current_connector_run_context()
        if context is None:
            return
        if not self.db.record_connector_run_output(
            context.run_id,
            checkpoint_id=context.checkpoint_id,
            artifact_id=result.queue_artifact_id,
            artifact_type=result.artifact_type,
            source=result.source_name,
            queue_status=result.queue_status,
            capture_event_id=result.event_id,
            capture_source_id=result.source_id,
            raw_ref_id=result.raw_ref_id,
            artifact_link_id=result.artifact_link_id,
        ):
            raise RuntimeError(
                f"Failed to record connector output {result.queue_artifact_id} "
                f"for run {context.run_id}"
            )


def _resolve_connector_raw_root(layout: PathLayout) -> tuple[ContentRoot, str]:
    """Return the managed-inbox root containing ``layout.raw_root`` and its
    relative prefix under that root (e.g., ``raw``).
    """
    policy = layout.content_root_policy
    if policy is None:
        raise ContentAdapterError(
            "content root policy is required to resolve connector raw root"
        )
    raw_resolved = layout.raw_root.resolve()
    for root in policy.roots_by_mode(ContentRootMode.MANAGED_INBOX):
        root_resolved = root.base_path.resolve()
        try:
            prefix = raw_resolved.relative_to(root_resolved).as_posix()
        except ValueError:
            continue
        return root, prefix
    raise ContentAdapterError(
        f"configured raw root {layout.raw_root} is not inside a writable "
        f"managed-inbox content root"
    )


def connector_raw_roots(layout: PathLayout) -> tuple[Path, ...]:
    """Roots under which connectors may record immutable raw references.

    Only managed-inbox roots are eligible.  A declared policy with no writable
    managed inbox fails closed instead of falling back to legacy paths.
    """
    policy = layout.content_root_policy
    if policy is None:
        raise ContentAdapterError(
            "content root policy is required for connector raw roots"
        )
    managed = policy.roots_by_mode(ContentRootMode.MANAGED_INBOX)
    if not managed:
        raise ContentAdapterError(
            "no managed-inbox content root configured for connector raw roots"
        )
    return tuple(root.base_path for root in managed)


def _raw_path_for_stores(lifecycle, runtime_config, raw_path):
    """Keep the raw path when any capture-side store consumes it.

    The raw file is the content both the capture event store and the CCF
    dual-write mirror commit against; when neither is active the path is
    dropped exactly as before.
    """
    if lifecycle.capture_event_store is not None or dual_write_requested(
        runtime_config
    ):
        return raw_path
    return None


def write_connector_raw_json(
    layout: PathLayout,
    *,
    connector_name: str,
    native_id: str,
    payload: Any,
    subdir: str | None = None,
    captured_at: str | None = None,
) -> Path:
    """Persist immutable connector source JSON through the content carrier.

    Writes are routed to the managed-inbox content root that contains the
    configured raw root, enforcing mode, traversal, symlink, and atomic-write
    guarantees through ``ContentCarrier.write_artifact``.
    """
    policy = layout.content_root_policy
    if policy is None:
        raise ContentAdapterError(
            "content root policy is required to write connector raw JSON"
        )

    root, raw_prefix = _resolve_connector_raw_root(layout)
    carrier = policy.carrier_for(root)

    safe_connector = _safe_raw_path_part(connector_name)
    directory_parts = [raw_prefix, safe_connector] if raw_prefix else [safe_connector]
    if subdir:
        directory_parts.append(_safe_raw_path_part(subdir))
    directory_relpath = "/".join(directory_parts)

    payload_digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode(
            "utf-8"
        )
    ).hexdigest()[:12]
    artifact_relpath = (
        f"{directory_relpath}/{_safe_raw_path_part(native_id)}-{payload_digest}.json"
    )

    envelope = {
        "connector": connector_name,
        "native_id": native_id,
        "captured_at": captured_at or datetime.now().isoformat(),
        "payload": payload,
    }
    content = json.dumps(envelope, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    content_bytes = content.encode("utf-8")

    # Idempotent: reuse the existing artifact only when content is identical.
    existing = next(carrier.list_artifacts(root, prefix=artifact_relpath), None)
    if existing is not None:
        if carrier.read_artifact(existing) == content_bytes:
            return Path(existing.locator)
        raise ValueError(
            f"raw connector file already exists with different content: {artifact_relpath}"
        )

    handle = carrier.write_artifact(root, artifact_relpath, content_bytes)
    if not isinstance(handle.locator, Path):
        raise ContentAdapterError(
            f"filesystem-backed raw write produced non-path locator: {handle.locator!r}"
        )
    return Path(handle.locator)


def _safe_raw_path_part(value: str) -> str:
    text = str(value or "").strip().replace("\\", "_").replace("/", "_")
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    if not safe:
        safe = "artifact"
    if len(safe) <= 96:
        return safe
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    return f"{safe[:83].rstrip('._-')}-{digest}"
