"""Operational status dashboard data for the admin settings console."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .admin_status_capture import capture_status
from .admin_status_compile import compiler_status, stale_pages_status
from .admin_status_stuck import queue_status, stuck_work_status
from .admin_status_utils import dt_text, error_item, parse_datetime
from .admin_lineage import wiki_lineage_status
from .capture_event_store import CaptureEventStore
from .config import Config
from .llm_usage import build_llm_usage_status
from .metadata_db import MetadataDB, get_metadata_db
from .path_layout import PathLayout, build_path_layout
from .postgres import PostgresConfigError, open_postgres_connection, resolve_postgres_settings


def build_admin_status_dashboard(
    config_data: dict[str, Any],
    *,
    project_root: Path,
    layout: PathLayout | None = None,
    db: MetadataDB | None = None,
    event_store: Any | None = None,
    now: datetime | None = None,
    stale_after_days: int = 30,
) -> dict[str, Any]:
    """Build capture/compile health data from operational stores."""

    runtime_config = _as_config(config_data)
    resolved_layout = layout or build_path_layout(
        runtime_config, project_root=project_root
    )
    metadata_db = db or get_metadata_db()
    resolved_layout.ensure_directories()
    now_dt = parse_datetime(now) or datetime.now(timezone.utc)

    def build_with_store(
        active_event_store: Any | None,
        capture_error: str | None = None,
    ) -> dict[str, Any]:
        capture_payload = capture_status(
            active_event_store,
            error=capture_error,
            now=now_dt,
        )
        queue_payload = queue_status(metadata_db)
        stale_payload = stale_pages_status(
            runtime_config,
            layout=resolved_layout,
            event_store=active_event_store,
            project_root=project_root,
            stale_after_days=stale_after_days,
        )
        compiler_payload = compiler_status(
            runtime_config,
            layout=resolved_layout,
            db=metadata_db,
            project_root=project_root,
            now=now_dt,
        )
        lineage_payload = wiki_lineage_status(
            runtime_config,
            layout=resolved_layout,
            db=metadata_db,
            project_root=project_root,
            event_store=active_event_store,
        )
        llm_usage_payload = build_llm_usage_status(metadata_db)
        section_errors = [
            item
            for item in (
                error_item("capture", capture_payload.get("error")),
                error_item("queues", queue_payload.get("error")),
                error_item("stale_pages", stale_payload.get("error")),
                error_item("compiler_runs", compiler_payload.get("error")),
                error_item("lineage", lineage_payload.get("error")),
                error_item("llm_usage", llm_usage_payload.get("error")),
            )
            if item
        ]
        section_errors.extend(lineage_payload.get("errors") or [])
        stuck_payload = stuck_work_status(
            db=metadata_db,
            capture=capture_payload,
            stale_pages=stale_payload,
            compiler_runs=compiler_payload,
            now=now_dt,
        )
        degraded = (
            bool(section_errors)
            or int(stuck_payload.get("total") or 0) > 0
            or int(stale_payload.get("total") or 0) > 0
            or int((capture_payload.get("source_health") or {}).get("unhealthy") or 0) > 0
        )
        return {
            "status": "degraded" if degraded else "ok",
            "generated_at": dt_text(now_dt),
            "layout": {
                "database_path": str(resolved_layout.database_path),
                "wiki_root": str(resolved_layout.wiki_root),
                "system_root": str(resolved_layout.system_root),
            },
            "source_health": capture_payload["source_health"],
            "recent_sessions": capture_payload["recent_sessions"],
            "event_counts": capture_payload["event_counts"],
            "queue_counts": queue_payload,
            "stale_pages": stale_payload,
            "compiler_runs": compiler_payload,
            "lineage": lineage_payload,
            "llm_usage": llm_usage_payload,
            "stuck_work": stuck_payload,
            "errors": section_errors,
        }

    if event_store is not None:
        return build_with_store(event_store)

    try:
        settings = resolve_postgres_settings(runtime_config)
    except PostgresConfigError as exc:
        return build_with_store(None, capture_error=str(exc))

    if not settings.enabled:
        return build_with_store(
            None,
            capture_error="capture event store is not configured",
        )

    try:
        with open_postgres_connection(settings) as conn:
            active_store = CaptureEventStore(
                conn,
                schema=settings.schema,
                raw_roots=[resolved_layout.raw_root],
            )
            return build_with_store(active_store)
    except Exception as exc:
        return build_with_store(None, capture_error=str(exc))


def _as_config(config_data: dict[str, Any]) -> Config:
    runtime_config = Config()
    runtime_config.data = config_data
    return runtime_config
