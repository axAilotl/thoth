"""Build non-secret runtime summaries for the settings UI."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Protocol

from .archivist_topics import load_archivist_topic_registry, resolve_archivist_topics_path
from .config import Config
from .connector_registry import load_connector_registry
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
        "raw_root": str(layout.raw_root),
        "library_root": str(layout.library_root),
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


def _summarize_connectors(config: ConfigLike, *, project_root: Path) -> dict[str, Any]:
    try:
        registry = load_connector_registry(config, project_root=project_root)
    except Exception as exc:
        return {"error": str(exc), "connectors": [], "total": 0}
    summary = registry.to_dict(config=config)
    for connector in summary["connectors"]:
        config_keys = list(connector.get("config_keys") or [])
        auth_keys = list(connector.get("auth") or [])
        connector["configured_keys"] = [
            key for key in config_keys if _config_or_env_present(config, key)
        ]
        connector["auth_status"] = {
            "keys": auth_keys,
            "configured": [
                key for key in auth_keys if _config_or_env_present(config, key)
            ],
            "missing": [
                key for key in auth_keys if not _config_or_env_present(config, key)
            ],
            "has_any": not auth_keys
            or any(_config_or_env_present(config, key) for key in auth_keys),
        }
    return summary


def _summarize_providers(config: ConfigLike) -> dict[str, Any]:
    providers = config.get("llm.providers", {}) or {}
    tasks = config.get("llm.tasks", {}) or {}
    if not isinstance(providers, dict):
        providers = {}
    if not isinstance(tasks, dict):
        tasks = {}

    provider_items = []
    for name, provider_config in sorted(providers.items()):
        if not isinstance(provider_config, dict):
            provider_config = {}
        models = provider_config.get("models")
        provider_items.append(
            {
                "name": name,
                "enabled": bool(provider_config.get("enabled", False)),
                "has_vision": bool(provider_config.get("has_vision", False)),
                "model_aliases": sorted(models.keys()) if isinstance(models, dict) else [],
                "has_base_url": bool(str(provider_config.get("base_url") or "").strip()),
                "has_api_key_env": bool(
                    str(provider_config.get("api_key_env") or "").strip()
                ),
            }
        )

    task_items = {}
    for task_name, task_config in sorted(tasks.items()):
        if not isinstance(task_config, dict):
            continue
        fallback = task_config.get("fallback")
        fallback_providers = []
        if isinstance(fallback, list):
            fallback_providers = [
                str(item.get("provider"))
                for item in fallback
                if isinstance(item, dict) and item.get("provider")
            ]
        task_items[task_name] = {
            "enabled": bool(task_config.get("enabled", False)),
            "fallback_providers": fallback_providers,
        }

    return {
        "total": len(provider_items),
        "enabled": [item["name"] for item in provider_items if item["enabled"]],
        "providers": provider_items,
        "tasks": task_items,
    }


def _summarize_storage(layout_summary: dict[str, Any]) -> dict[str, Any]:
    if "error" in layout_summary:
        return {"error": layout_summary["error"]}
    return {
        "vault_root": layout_summary.get("vault_root"),
        "system_root": layout_summary.get("system_root"),
        "raw_root": layout_summary.get("raw_root"),
        "cache_root": layout_summary.get("cache_root"),
        "digests_root": layout_summary.get("digests_root"),
        "database_path": layout_summary.get("database_path"),
    }


def _summarize_wiki_group(
    layout_summary: dict[str, Any],
    archivist_summary: dict[str, Any],
) -> dict[str, Any]:
    if "error" in layout_summary:
        return {"error": layout_summary["error"]}
    return {
        "wiki_root": layout_summary.get("wiki_root"),
        "okf_target": "v0.1",
        "archivist_registry_path": archivist_summary.get("registry_path"),
        "archivist_topic_count": archivist_summary.get("topic_count", 0),
        "archivist_topics": list(archivist_summary.get("topics") or []),
    }


def _summarize_automation(config: ConfigLike) -> dict[str, Any]:
    jobs = {}
    for key, default_interval in (
        ("archivist", 12),
        ("social_sync", 8),
        ("x_api_sync", 8),
    ):
        value = config.get(f"automation.{key}", {}) or {}
        if not isinstance(value, dict):
            jobs[key] = {"error": f"automation.{key} must be an object"}
            continue
        jobs[key] = {
            "enabled": bool(value.get("enabled", False)),
            "run_on_startup": bool(value.get("run_on_startup", False)),
            "interval_hours": value.get("interval_hours", default_interval),
        }
        if key == "x_api_sync":
            jobs[key]["max_results"] = value.get("max_results", 100)
            jobs[key]["max_pages"] = value.get("max_pages")
            jobs[key]["resume_from_checkpoint"] = bool(
                value.get("resume_from_checkpoint", True)
            )
    return {
        "jobs": jobs,
        "enabled": [key for key, value in jobs.items() if value.get("enabled")],
    }


def _summarize_grouped_config(
    config: ConfigLike,
    *,
    layout_summary: dict[str, Any],
    archivist_summary: dict[str, Any],
    connectors_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "providers": _summarize_providers(config),
        "connectors": {
            "total": connectors_summary.get("total", 0),
            "enabled": [
                item["name"]
                for item in connectors_summary.get("connectors", [])
                if item.get("enabled")
            ],
            "items": connectors_summary.get("connectors", []),
            "error": connectors_summary.get("error"),
        },
        "storage": _summarize_storage(layout_summary),
        "wiki": _summarize_wiki_group(layout_summary, archivist_summary),
        "automation": _summarize_automation(config),
    }


def _config_or_env_present(config: ConfigLike, key: str) -> bool:
    if not key:
        return False
    if key.isupper() and "." not in key:
        return bool(os.getenv(key, "").strip())
    value = config.get(key)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def build_settings_runtime_summary(
    config_data: dict[str, Any],
    *,
    project_root: Path,
) -> dict[str, Any]:
    """Return resolved, non-secret runtime state for the settings page."""
    config = _as_config(config_data)
    layout_summary = _summarize_layout(config, project_root=project_root)
    archivist_summary = _summarize_archivist(config, project_root=project_root)
    web_clipper_summary = _summarize_web_clipper(config, project_root=project_root)
    connectors_summary = _summarize_connectors(config, project_root=project_root)
    return {
        "layout": layout_summary,
        "archivist": archivist_summary,
        "web_clipper": web_clipper_summary,
        "connectors": connectors_summary,
        "groups": _summarize_grouped_config(
            config,
            layout_summary=layout_summary,
            archivist_summary=archivist_summary,
            connectors_summary=connectors_summary,
        ),
    }
