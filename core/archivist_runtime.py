"""Archivist execution helpers shared by the API and automation scheduler."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Sequence

from .archivist_compiler import ArchivistCompileResult, ArchivistCompiler
from .config import Config
from .metadata_db import MetadataDB
from .non_live_state import validate_non_live_interval_hours
from .path_layout import build_path_layout

ARCHIVIST_JOB_NAME = "archivist"


class ArchivistRuntimeError(ValueError):
    """Raised when archivist runtime configuration or execution is invalid."""


def resolve_archivist_sync_config(config_or_data: Config | dict[str, Any]) -> dict[str, Any]:
    """Return normalized archivist automation settings."""
    config = _as_config(config_or_data)
    automation = config.get("automation.archivist", {}) or {}
    if not isinstance(automation, dict):
        raise ArchivistRuntimeError("automation.archivist must be an object")

    return {
        "enabled": bool(automation.get("enabled", False)),
        "interval_hours": validate_non_live_interval_hours(
            automation.get("interval_hours", 12),
            field_name="automation.archivist.interval_hours",
            default=12.0,
        ),
        "run_on_startup": bool(automation.get("run_on_startup", False)),
    }


async def run_archivist_topics(
    config_or_data: Config | dict[str, Any],
    *,
    project_root: Path,
    topic_ids: Sequence[str] | None = None,
    force: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    """Run the archivist compiler and return a stable API-friendly payload."""
    config = _as_config(config_or_data)
    layout = build_path_layout(config, project_root=project_root)
    layout.ensure_directories()
    compiler = ArchivistCompiler(
        config,
        project_root=project_root,
        layout=layout,
        db=MetadataDB(str(layout.database_path)),
    )
    results = await compiler.run(
        topic_ids=topic_ids,
        force=force,
        dry_run=dry_run,
        limit=limit,
    )
    return serialize_archivist_run(
        results,
        force=force,
        dry_run=dry_run,
        limit=limit,
        topic_ids=topic_ids,
    )


def serialize_archivist_run(
    results: Sequence[ArchivistCompileResult],
    *,
    force: bool,
    dry_run: bool,
    limit: int | None,
    topic_ids: Sequence[str] | None,
) -> dict[str, Any]:
    """Convert archivist results into a stable summary payload."""
    serialized_results = [
        {
            "topic_id": result.topic_id,
            "status": result.status,
            "reason": result.reason,
            "page_path": str(result.page_path) if result.page_path is not None else None,
            "candidate_count": result.candidate_count,
            "source_paths": list(result.source_paths),
            "model_provider": result.model_provider,
            "model": result.model,
        }
        for result in results
    ]

    return {
        "status": "ok",
        "force": bool(force),
        "dry_run": bool(dry_run),
        "limit": limit,
        "topic_ids": list(topic_ids) if topic_ids is not None else [],
        "results": serialized_results,
        "summary": {
            "compiled": sum(1 for result in results if result.status == "compiled"),
            "skipped": sum(1 for result in results if result.status == "skipped"),
            "dry_run": sum(1 for result in results if result.status == "dry_run"),
            "total": len(results),
        },
    }


def _as_config(config_or_data: Config | dict[str, Any]) -> Config:
    if isinstance(config_or_data, Config):
        return config_or_data

    config = Config()
    config.data = deepcopy(config_or_data)
    return config
