"""Build non-secret runtime summaries for the settings UI."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from .archivist_topics import load_archivist_topic_registry, resolve_archivist_topics_path
from .config import Config
from .metadata_db import MetadataDB
from .path_layout import build_path_layout


class ConfigLike(Protocol):
    def get(self, key: str, default: Any = None) -> Any:
        ...


def _as_config(config_data: dict[str, Any]) -> Config:
    config = Config()
    config.data = config_data
    return config


def _summarize_layout(config: ConfigLike, *, project_root: Path) -> dict[str, Any]:
    try:
        layout = build_path_layout(config, project_root=project_root)
    except Exception as exc:
        return {"error": str(exc)}

    return {
        "vault_root": str(layout.vault_root),
        "wiki_root": str(layout.wiki_root),
        "system_root": str(layout.system_root),
        "cache_root": str(layout.cache_root),
        "digests_root": str(layout.digests_root),
        "auth_root": str(layout.auth_root),
        "database_path": str(layout.database_path),
    }


def _summarize_archivist(config: ConfigLike, *, project_root: Path) -> dict[str, Any]:
    registry_path = resolve_archivist_topics_path(config, project_root=project_root)
    explicit_path = bool(str(config.get("paths.archivist_topics_file", "") or "").strip())
    summary: dict[str, Any] = {
        "configured": explicit_path or registry_path.exists(),
        "registry_path": str(registry_path),
        "exists": registry_path.exists(),
        "topic_count": 0,
        "topics": [],
        "corpus": {
            "document_count": 0,
            "embedding_count": 0,
            "last_indexed_at": None,
            "last_embedding_at": None,
            "by_source_type": {},
        },
    }

    if not summary["configured"]:
        return summary

    try:
        registry = load_archivist_topic_registry(
            config,
            project_root=project_root,
            required=explicit_path,
        )
    except Exception as exc:
        summary["error"] = str(exc)
        return summary

    summary["topic_count"] = len(registry.topics)
    summary["topics"] = sorted(topic.id for topic in registry.topics)
    try:
        layout = build_path_layout(config, project_root=project_root)
        if layout.database_path.exists():
            summary["corpus"] = MetadataDB(str(layout.database_path)).get_archivist_corpus_stats()
    except Exception as exc:
        summary["corpus_error"] = str(exc)
    return summary


def _summarize_web_clipper(config: ConfigLike, *, project_root: Path) -> dict[str, Any]:
    source_config = config.get("sources.web_clipper", {}) or {}
    enabled = bool(source_config.get("enabled", False))
    configured = bool(source_config.get("note_dirs")) or bool(
        source_config.get("attachment_dirs")
    )
    summary: dict[str, Any] = {
        "enabled": enabled,
        "configured": configured,
        "note_dirs": [],
        "attachment_dirs": [],
        "watch_dirs": [],
        "note_extensions": list(source_config.get("note_extensions", []) or []),
        "attachment_extensions": list(
            source_config.get("attachment_extensions", []) or []
        ),
    }

    if not configured:
        return summary

    try:
        from collectors.web_clipper_layout import build_web_clipper_contract

        contract = build_web_clipper_contract(
            config,
            layout=build_path_layout(config, project_root=project_root),
        )
    except Exception as exc:
        summary["error"] = str(exc)
        return summary

    summary["note_dirs"] = [str(path) for path in contract.note_dirs]
    summary["attachment_dirs"] = [str(path) for path in contract.attachment_dirs]
    summary["watch_dirs"] = [str(path) for path in contract.watch_dirs]
    if not summary["note_extensions"]:
        summary["note_extensions"] = list(contract.note_extensions)
    if not summary["attachment_extensions"]:
        summary["attachment_extensions"] = list(contract.attachment_extensions)
    return summary


def build_settings_runtime_summary(
    config_data: dict[str, Any],
    *,
    project_root: Path,
) -> dict[str, Any]:
    """Return resolved, non-secret runtime state for the settings page."""
    config = _as_config(config_data)
    return {
        "layout": _summarize_layout(config, project_root=project_root),
        "archivist": _summarize_archivist(config, project_root=project_root),
        "web_clipper": _summarize_web_clipper(config, project_root=project_root),
    }
