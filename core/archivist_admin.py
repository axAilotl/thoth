"""Archivist registry administration helpers for the settings UI."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta
import os
from pathlib import Path
import tempfile
from typing import Any

from .archivist_state import (
    clear_archivist_topic_force,
    load_archivist_topic_state,
    request_archivist_topic_force,
)
from .archivist_topics import (
    ArchivistTopicConfigError,
    ArchivistTopicDefinition,
    load_archivist_topic_registry,
    resolve_archivist_topics_path,
    seed_archivist_topic_registry_from_example,
)
from .config import Config
from .metadata_db import MetadataDB
from .path_layout import build_path_layout


class ArchivistAdminError(ValueError):
    """Raised when the archivist admin surface cannot complete safely."""


def build_archivist_admin_payload(
    config_data: dict[str, Any],
    *,
    project_root: Path,
) -> dict[str, Any]:
    """Return raw and parsed archivist registry state for the settings UI."""

    config = _as_config(config_data)
    registry_path = resolve_archivist_topics_path(config, project_root=project_root)
    existed_before = registry_path.exists()
    seeded_path = seed_archivist_topic_registry_from_example(
        config,
        project_root=project_root,
    )
    seeded_from_example = bool(
        seeded_path is not None and seeded_path == registry_path and not existed_before
    )
    explicit_path = bool(str(config.get("paths.archivist_topics_file", "") or "").strip())
    payload: dict[str, Any] = {
        "registry_path": str(registry_path),
        "exists": registry_path.exists(),
        "configured": explicit_path or registry_path.exists(),
        "seeded_from_example": seeded_from_example,
        "raw_text": "",
        "topics": [],
        "defaults": {},
        "version": None,
    }

    if registry_path.exists():
        payload["raw_text"] = registry_path.read_text(encoding="utf-8")

    try:
        registry = load_archivist_topic_registry(
            config,
            project_root=project_root,
            required=explicit_path,
        )
    except Exception as exc:
        payload["error"] = str(exc)
        return payload

    payload["version"] = registry.version
    payload["defaults"] = asdict(registry.defaults)

    metadata_db = _build_metadata_db(config, project_root=project_root)
    payload["topics"] = [
        _serialize_topic(topic, db=metadata_db)
        for topic in registry.topics
    ]
    return payload


def save_archivist_registry_text(
    config_data: dict[str, Any],
    *,
    project_root: Path,
    content: str,
) -> dict[str, Any]:
    """Validate and atomically persist the archivist registry file."""

    config = _as_config(config_data)
    registry_path = resolve_archivist_topics_path(config, project_root=project_root)
    registry_path.parent.mkdir(parents=True, exist_ok=True)

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=str(registry_path.parent),
            prefix=f".{registry_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(content)
            temp_path = Path(handle.name)

        validation_config = _build_registry_override_config(
            config_data,
            registry_path=temp_path,
        )
        load_archivist_topic_registry(
            validation_config,
            project_root=project_root,
            required=True,
        )
        os.replace(temp_path, registry_path)
    except ArchivistTopicConfigError:
        raise
    except Exception as exc:
        raise ArchivistAdminError(
            f"Failed to save archivist registry {registry_path}: {exc}"
        ) from exc
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()

    return build_archivist_admin_payload(config_data, project_root=project_root)


def queue_archivist_topic_force(
    config_data: dict[str, Any],
    *,
    project_root: Path,
    topic_id: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Queue a manual force request for the next archivist topic run."""

    registry, topic = _require_topic(
        config_data,
        project_root=project_root,
        topic_id=topic_id,
    )
    metadata_db = _build_metadata_db(_as_config(config_data), project_root=project_root)
    state = request_archivist_topic_force(topic, db=metadata_db, reason=reason)
    return {
        "status": "ok",
        "topic_id": topic.id,
        "state": _serialize_state(topic, state),
        "topic_count": len(registry.topics),
    }


def clear_archivist_topic_force_request(
    config_data: dict[str, Any],
    *,
    project_root: Path,
    topic_id: str,
) -> dict[str, Any]:
    """Clear a queued manual force request for an archivist topic."""

    registry, topic = _require_topic(
        config_data,
        project_root=project_root,
        topic_id=topic_id,
    )
    metadata_db = _build_metadata_db(_as_config(config_data), project_root=project_root)
    state = clear_archivist_topic_force(topic.id, db=metadata_db)
    return {
        "status": "ok",
        "topic_id": topic.id,
        "state": _serialize_state(topic, state),
        "topic_count": len(registry.topics),
    }


def _require_topic(
    config_data: dict[str, Any],
    *,
    project_root: Path,
    topic_id: str,
) -> tuple[Any, ArchivistTopicDefinition]:
    config = _as_config(config_data)
    registry = load_archivist_topic_registry(
        config,
        project_root=project_root,
        required=True,
    )
    topic = registry.get_topic(topic_id)
    if topic is None:
        raise ArchivistAdminError(f"Unknown archivist topic: {topic_id}")
    return registry, topic


def _serialize_topic(
    topic: ArchivistTopicDefinition,
    *,
    db: MetadataDB,
) -> dict[str, Any]:
    payload = {
        "id": topic.id,
        "title": topic.title,
        "output_path": topic.output_path,
        "description": topic.description,
        "include_roots": list(topic.include_roots),
        "exclude_roots": list(topic.exclude_roots),
        "source_types": list(topic.source_types),
        "include_tags": list(topic.include_tags),
        "exclude_tags": list(topic.exclude_tags),
        "include_terms": list(topic.include_terms),
        "exclude_terms": list(topic.exclude_terms),
        "cadence_hours": topic.cadence_hours,
        "max_sources": topic.max_sources,
        "allow_manual_force": topic.allow_manual_force,
    }

    try:
        state = load_archivist_topic_state(topic.id, db=db)
    except Exception as exc:
        payload["state_error"] = str(exc)
        return payload

    payload["state"] = _serialize_state(topic, state)
    return payload


def _serialize_state(topic: ArchivistTopicDefinition, state: Any) -> dict[str, Any]:
    return {
        "last_run_at": state.last_run_at,
        "last_success_at": state.last_success_at,
        "last_candidate_count": state.last_candidate_count,
        "last_model_provider": state.last_model_provider,
        "last_model": state.last_model,
        "force_requested_at": state.force_requested_at,
        "force_reason": state.force_reason,
        "next_due_at": _compute_next_due_at(
            state.last_success_at,
            topic.cadence_hours,
        ),
    }


def _compute_next_due_at(last_success_at: str | None, cadence_hours: float) -> str | None:
    if not last_success_at:
        return None
    try:
        base = datetime.fromisoformat(last_success_at)
    except ValueError as exc:
        raise ArchivistAdminError(
            f"Invalid archivist timestamp in topic state: {last_success_at}"
        ) from exc
    return (base + timedelta(hours=float(cadence_hours))).isoformat()


def _build_registry_override_config(
    config_data: dict[str, Any],
    *,
    registry_path: Path,
) -> Config:
    updated = deepcopy(config_data)
    paths = updated.setdefault("paths", {})
    if not isinstance(paths, dict):
        raise ArchivistAdminError("paths config must be an object")
    paths["archivist_topics_file"] = str(registry_path)

    config = Config()
    config.data = updated
    return config


def _build_metadata_db(config: Config, *, project_root: Path) -> MetadataDB:
    layout = build_path_layout(config, project_root=project_root)
    layout.ensure_directories()
    return MetadataDB(str(layout.database_path))


def _as_config(config_data: dict[str, Any]) -> Config:
    config = Config()
    config.data = deepcopy(config_data)
    return config
