"""Build non-secret runtime summaries for the settings UI."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Protocol

from .archivist_topics import load_archivist_topic_registry, resolve_archivist_topics_path
from .config import Config
from .connector_registry import (
    ConnectorManifestError,
    load_connector_registry,
    validate_allowed_side_effects,
    validate_manifest_outputs,
)
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


def _summarize_pi_skills(config: ConfigLike, *, project_root: Path) -> dict[str, Any]:
    source_config = config.get("sources.pi_skills", {}) or {}
    if not isinstance(source_config, dict):
        return {"error": "sources.pi_skills must be an object", "enabled": False}

    raw_skills = source_config.get("skills") or []
    if isinstance(raw_skills, dict):
        skill_items = [
            {"id": key, **(value if isinstance(value, dict) else {})}
            for key, value in raw_skills.items()
        ]
    elif isinstance(raw_skills, list):
        skill_items = [item for item in raw_skills if isinstance(item, dict)]
    else:
        skill_items = []

    output_dir = source_config.get("output_dir") or ".thoth_system/skill_outputs/pi"
    output_path = Path(str(output_dir)).expanduser()
    if not output_path.is_absolute():
        output_path = project_root / output_path

    fallback = source_config.get("fallback")
    routes = []
    if isinstance(fallback, list):
        for item in fallback:
            if not isinstance(item, dict) or not item.get("provider"):
                continue
            routes.append(
                {
                    "provider": str(item.get("provider")),
                    "model": str(item.get("model") or ""),
                }
            )
    if not routes:
        routes.append(
            {
                "provider": str(source_config.get("default_provider") or "pi"),
                "model": str(source_config.get("default_model") or "archivist_agent"),
            }
        )
    route_identities = [
        _summarize_pi_route_identity(config, source_config, route) for route in routes
    ]

    try:
        skills = [
            _summarize_pi_skill_manifest(item, source_config=source_config)
            for item in skill_items
            if item.get("id")
        ]
    except (ConnectorManifestError, ValueError) as exc:
        return {
            "enabled": bool(source_config.get("enabled", True)),
            "output_dir": str(output_path),
            "safety_mode": "no_tools_json",
            "default_provider": str(source_config.get("default_provider") or "pi"),
            "default_model": str(source_config.get("default_model") or "archivist_agent"),
            "routes": routes,
            "route_identities": route_identities,
            "total": 0,
            "skills": [],
            "error": str(exc),
        }

    return {
        "enabled": bool(source_config.get("enabled", True)),
        "output_dir": str(output_path),
        "safety_mode": "no_tools_json",
        "default_provider": str(source_config.get("default_provider") or "pi"),
        "default_model": str(source_config.get("default_model") or "archivist_agent"),
        "routes": routes,
        "route_identities": route_identities,
        "total": len(skills),
        "skills": skills,
    }


def _summarize_pi_skill_manifest(
    item: dict[str, Any],
    *,
    source_config: dict[str, Any],
) -> dict[str, Any]:
    skill_id = str(item.get("id") or "").strip()
    if not skill_id:
        raise ValueError("Pi skill definition missing id")
    artifact_types = _required_manifest_list(item, "artifact_types", skill_id=skill_id)
    inputs = _required_manifest_list(item, "inputs", skill_id=skill_id)
    outputs = _required_manifest_list(item, "outputs", skill_id=skill_id)
    try:
        validate_manifest_outputs(outputs, origin=f"Pi skill {skill_id!r}")
    except ConnectorManifestError as exc:
        raise ValueError(str(exc)) from exc
    auth = _required_manifest_list(item, "auth", skill_id=skill_id, allow_empty=True)
    safety_mode = _required_manifest_string(item, "safety_mode", skill_id=skill_id)
    queue_behavior = _required_manifest_string(item, "queue_behavior", skill_id=skill_id)
    allowed_side_effects = _required_manifest_list(
        item,
        "allowed_side_effects",
        skill_id=skill_id,
        allow_empty=True,
    )
    try:
        validate_allowed_side_effects(
            allowed_side_effects,
            origin=f"Pi skill {skill_id!r}",
        )
    except ConnectorManifestError as exc:
        raise ValueError(str(exc)) from exc
    return {
        "id": skill_id,
        "description": str(item.get("description") or ""),
        "artifact_types": list(artifact_types),
        "inputs": list(inputs),
        "outputs": list(outputs),
        "auth": list(auth),
        "safety_mode": safety_mode,
        "queue_behavior": queue_behavior,
        "allowed_side_effects": list(allowed_side_effects),
        "source_name": str(item.get("source_name") or ""),
        "allowlist": _pi_skill_allowlist_status(skill_id, source_config),
    }


def _pi_skill_allowlist_status(
    skill_id: str,
    source_config: dict[str, Any],
) -> dict[str, Any]:
    allowlist = _optional_string_set(source_config.get("allowlist"))
    if allowlist is None:
        return {
            "configured": False,
            "allowed": True,
            "matched": [],
        }
    matched = [skill_id] if skill_id in allowlist else []
    return {
        "configured": True,
        "allowed": bool(matched),
        "matched": matched,
    }


def _summarize_pi_route_identity(
    config: ConfigLike,
    source_config: dict[str, Any],
    route: dict[str, str],
) -> dict[str, Any]:
    provider = str(route.get("provider") or "")
    provider_cfg = config.get(f"llm.providers.{provider}", {}) or {}
    if not isinstance(provider_cfg, dict):
        provider_cfg = {}
    command = str(provider_cfg.get("command") or "pi")
    model_alias = str(route.get("model") or "")
    model = _pi_model_id(provider_cfg, model_alias) or model_alias or None
    identity = {
        "provider": provider,
        "configured_command": command,
        "resolved_command": _resolve_command_path(command),
        "pi_provider": str(provider_cfg.get("pi_provider") or "") or None,
        "model": model,
        "install_if_missing": bool(provider_cfg.get("install_if_missing", False)),
        "install_command_configured": bool(provider_cfg.get("install_command")),
    }
    pin = _pi_command_pin(source_config, provider_cfg, provider)
    drift = []
    if pin:
        field_map = {
            "command": "configured_command",
            "configured_command": "configured_command",
            "resolved_command": "resolved_command",
            "pi_provider": "pi_provider",
            "model": "model",
        }
        for pin_field, identity_field in field_map.items():
            if pin_field not in pin:
                continue
            expected = pin.get(pin_field)
            actual = identity.get(identity_field)
            if expected != actual:
                drift.append(
                    {
                        "field": pin_field,
                        "expected": expected,
                        "actual": actual,
                    }
                )
    identity["pin"] = dict(pin) if pin else {}
    identity["pinned"] = bool(pin)
    identity["drift"] = drift
    return identity


def _pi_command_pin(
    source_config: dict[str, Any],
    provider_cfg: dict[str, Any],
    provider: str,
) -> dict[str, Any]:
    pins = source_config.get("command_pins") or {}
    if isinstance(pins, dict) and isinstance(pins.get(provider), dict):
        return pins[provider]
    provider_pin = provider_cfg.get("command_pin")
    if isinstance(provider_pin, dict):
        return provider_pin
    return {}


def _pi_model_id(provider_cfg: dict[str, Any], model: str | None) -> str | None:
    models = provider_cfg.get("models")
    if not isinstance(models, dict):
        return model
    if model and isinstance(models.get(model), dict):
        return str(models[model].get("id") or model)
    if model:
        return model
    default_model = models.get("default")
    if isinstance(default_model, dict):
        return str(default_model.get("id") or "") or None
    return None


def _resolve_command_path(command: str) -> str | None:
    resolved = shutil.which(command)
    if resolved:
        return str(Path(resolved).resolve())
    command_path = Path(command).expanduser()
    if command_path.exists():
        return str(command_path.resolve())
    return None


def _optional_string_set(value: Any) -> set[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return {item.strip() for item in value.split(",") if item.strip()}
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    raise ValueError("allowlist must be an array or string")


def _required_manifest_string(
    item: dict[str, Any],
    field_name: str,
    *,
    skill_id: str,
) -> str:
    text = str(item.get(field_name) or "").strip()
    if not text:
        raise ValueError(f"Pi skill {skill_id!r} requires {field_name}")
    return text


def _required_manifest_list(
    item: dict[str, Any],
    field_name: str,
    *,
    skill_id: str,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if field_name not in item:
        raise ValueError(f"Pi skill {skill_id!r} requires {field_name}")
    value = item.get(field_name)
    if isinstance(value, (list, tuple)):
        values = [str(part).strip() for part in value if str(part).strip()]
    else:
        raise ValueError(f"Pi skill {skill_id!r} {field_name} must be an array")
    if not values and not allow_empty:
        raise ValueError(f"Pi skill {skill_id!r} requires {field_name}")
    return tuple(values)


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


def _connector_group(connectors_summary: dict[str, Any]) -> dict[str, Any]:
    connectors = connectors_summary.get("connectors", [])
    return {
        "total": connectors_summary.get("total", 0),
        "enabled": [
            item["name"]
            for item in connectors
            if item.get("enabled")
        ],
        "items": connectors,
        "error": connectors_summary.get("error"),
    }


def _skills_group(pi_skills_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "enabled": bool(pi_skills_summary.get("enabled", False)),
        "total": pi_skills_summary.get("total", 0),
        "items": pi_skills_summary.get("skills", []),
        "safety_mode": pi_skills_summary.get("safety_mode"),
        "output_dir": pi_skills_summary.get("output_dir"),
        "routes": pi_skills_summary.get("routes", []),
        "route_identities": pi_skills_summary.get("route_identities", []),
        "error": pi_skills_summary.get("error"),
    }


def _summarize_sources_and_skills(
    connectors_summary: dict[str, Any],
    pi_skills_summary: dict[str, Any],
    web_clipper_summary: dict[str, Any],
) -> dict[str, Any]:
    connectors = _connector_group(connectors_summary)
    skills = _skills_group(pi_skills_summary)
    return {
        "connectors": connectors,
        "skills": skills,
        "web_clipper": {
            "enabled": bool(web_clipper_summary.get("enabled", False)),
            "configured": bool(web_clipper_summary.get("configured", False)),
            "watch_dirs": list(web_clipper_summary.get("watch_dirs") or []),
            "error": web_clipper_summary.get("error"),
        },
    }


def _summarize_wiki_and_archivist(
    layout_summary: dict[str, Any],
    archivist_summary: dict[str, Any],
    automation_summary: dict[str, Any],
) -> dict[str, Any]:
    wiki = _summarize_wiki_group(layout_summary, archivist_summary)
    corpus = archivist_summary.get("corpus") or {}
    return {
        **wiki,
        "archivist_configured": bool(archivist_summary.get("configured", False)),
        "archivist_exists": bool(archivist_summary.get("exists", False)),
        "archivist_error": archivist_summary.get("error"),
        "corpus_error": archivist_summary.get("corpus_error"),
        "corpus": corpus,
        "automation": (automation_summary.get("jobs") or {}).get("archivist", {}),
    }


def _summarize_security(
    config: ConfigLike,
    *,
    providers_summary: dict[str, Any],
    connectors_summary: dict[str, Any],
    pi_skills_summary: dict[str, Any],
) -> dict[str, Any]:
    connector_auth = []
    for connector in connectors_summary.get("connectors", []):
        auth_status = connector.get("auth_status") or {}
        auth_keys = list(auth_status.get("keys") or [])
        if not auth_keys:
            continue
        connector_auth.append(
            {
                "name": connector.get("name"),
                "enabled": bool(connector.get("enabled", False)),
                "configured": list(auth_status.get("configured") or []),
                "missing": list(auth_status.get("missing") or []),
                "total": len(auth_keys),
            }
        )

    provider_auth = [
        {
            "name": provider["name"],
            "enabled": provider["enabled"],
            "has_api_key_env": provider["has_api_key_env"],
        }
        for provider in providers_summary.get("providers", [])
    ]

    side_effects = set()
    safety_modes = set()
    for connector in connectors_summary.get("connectors", []):
        side_effects.update(connector.get("allowed_side_effects") or [])
        if connector.get("safety_mode"):
            safety_modes.add(str(connector["safety_mode"]))
    for skill in pi_skills_summary.get("skills", []):
        side_effects.update(skill.get("allowed_side_effects") or [])
        if skill.get("safety_mode"):
            safety_modes.add(str(skill["safety_mode"]))

    prompts = config.get("llm.prompts", {}) or {}
    prompt_groups = sorted(prompts.keys()) if isinstance(prompts, dict) else []
    return {
        "connector_auth": connector_auth,
        "provider_auth": provider_auth,
        "prompt_security": {
            "threat_scanner": "available",
            "sensitive_redaction": "available",
            "configured_prompt_groups": prompt_groups,
        },
        "safety_modes": sorted(safety_modes),
        "allowed_side_effects": sorted(side_effects),
    }


def _summarize_overview(
    *,
    providers_summary: dict[str, Any],
    sources_and_skills: dict[str, Any],
    wiki_and_archivist: dict[str, Any],
    automation_summary: dict[str, Any],
    layout_summary: dict[str, Any],
) -> dict[str, Any]:
    connectors = sources_and_skills["connectors"]
    skills = sources_and_skills["skills"]
    what_happened = [
        (
            f"{len(providers_summary.get('enabled') or [])}/"
            f"{providers_summary.get('total', 0)} providers enabled"
        ),
        f"{len(connectors.get('enabled') or [])}/{connectors.get('total', 0)} sources enabled",
        f"{skills.get('total', 0)} Pi skills configured",
        f"{wiki_and_archivist.get('archivist_topic_count', 0)} archivist topics loaded",
    ]

    stuck = []
    for summary in (layout_summary, connectors, skills, sources_and_skills["web_clipper"]):
        if summary.get("error"):
            stuck.append(str(summary["error"]))
    if wiki_and_archivist.get("archivist_error"):
        stuck.append(str(wiki_and_archivist["archivist_error"]))
    if wiki_and_archivist.get("corpus_error"):
        stuck.append(str(wiki_and_archivist["corpus_error"]))

    for connector in connectors.get("items", []):
        auth_status = connector.get("auth_status") or {}
        if connector.get("enabled") and auth_status.get("missing"):
            missing = ", ".join(auth_status["missing"])
            stuck.append(f"{connector['name']} missing auth: {missing}")

    run_next = []
    jobs = automation_summary.get("jobs") or {}
    for key in automation_summary.get("enabled") or []:
        interval = jobs.get(key, {}).get("interval_hours")
        suffix = f" every {interval}h" if interval else ""
        run_next.append(f"{key}{suffix}")
    if not run_next:
        run_next.append("No background jobs enabled")

    return {
        "what_happened": what_happened,
        "what_is_stuck": stuck,
        "what_should_run_next": run_next,
        "counts": {
            "providers_enabled": len(providers_summary.get("enabled") or []),
            "providers_total": providers_summary.get("total", 0),
            "sources_enabled": len(connectors.get("enabled") or []),
            "sources_total": connectors.get("total", 0),
            "skills_total": skills.get("total", 0),
            "archivist_topics": wiki_and_archivist.get("archivist_topic_count", 0),
        },
    }


def _summarize_grouped_config(
    config: ConfigLike,
    *,
    layout_summary: dict[str, Any],
    archivist_summary: dict[str, Any],
    connectors_summary: dict[str, Any],
    web_clipper_summary: dict[str, Any],
    pi_skills_summary: dict[str, Any],
) -> dict[str, Any]:
    providers_summary = _summarize_providers(config)
    automation_summary = _summarize_automation(config)
    sources_and_skills = _summarize_sources_and_skills(
        connectors_summary,
        pi_skills_summary,
        web_clipper_summary,
    )
    wiki_and_archivist = _summarize_wiki_and_archivist(
        layout_summary,
        archivist_summary,
        automation_summary,
    )
    security = _summarize_security(
        config,
        providers_summary=providers_summary,
        connectors_summary=connectors_summary,
        pi_skills_summary=pi_skills_summary,
    )
    return {
        "overview": _summarize_overview(
            providers_summary=providers_summary,
            sources_and_skills=sources_and_skills,
            wiki_and_archivist=wiki_and_archivist,
            automation_summary=automation_summary,
            layout_summary=layout_summary,
        ),
        "sources_and_skills": sources_and_skills,
        "wiki_and_archivist": wiki_and_archivist,
        "security": security,
        "advanced": {
            "providers": providers_summary,
            "task_routing": providers_summary.get("tasks", {}),
            "storage": _summarize_storage(layout_summary),
            "automation": automation_summary,
        },
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
    pi_skills_summary = _summarize_pi_skills(config, project_root=project_root)
    return {
        "layout": layout_summary,
        "archivist": archivist_summary,
        "web_clipper": web_clipper_summary,
        "connectors": connectors_summary,
        "pi_skills": pi_skills_summary,
        "groups": _summarize_grouped_config(
            config,
            layout_summary=layout_summary,
            archivist_summary=archivist_summary,
            connectors_summary=connectors_summary,
            web_clipper_summary=web_clipper_summary,
            pi_skills_summary=pi_skills_summary,
        ),
    }
