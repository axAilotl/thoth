"""Connector entrypoint resolution and invocation for agent-driven runs.

Manifests declare an ``entrypoint`` import string (``module:attr`` or
``module:Class.method``). Entrypoints are resolved lazily at run time and every
resolution failure raises :class:`ConnectorRunnerError` before any run history
or connector side effects happen.

The generic invocation contract (no core edits required) is a class that
accepts ``(config, *, layout, db)`` and exposes ``collect(**options)``; the
result may be a coroutine and may provide ``to_dict()``. Entrypoints whose
calling convention predates or differs from that contract are dispatched
through the explicit adapter table below.
"""

from __future__ import annotations

import asyncio
import importlib
import os
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .config import Config
from .connector_registry import ConnectorManifest, ConnectorManifestError
from .metadata_db import MetadataDB
from .path_layout import PathLayout


class ConnectorRunnerError(ConnectorManifestError):
    """Raised when a connector entrypoint cannot be resolved or invoked safely."""


@dataclass(frozen=True)
class ConnectorRunContext:
    """Runtime dependencies handed to connector invocation adapters."""

    config: Config
    layout: PathLayout
    db: MetadataDB
    actor: str | None = None


@dataclass(frozen=True)
class ResolvedEntrypoint:
    """A manifest entrypoint resolved through importlib."""

    target: Any
    root: Any
    module: str
    attrs: tuple[str, ...]


RunAdapter = Callable[
    [ConnectorRunContext, ResolvedEntrypoint, Mapping[str, Any]],
    dict[str, Any],
]


def resolve_connector_entrypoint(manifest: ConnectorManifest) -> ResolvedEntrypoint:
    """Import the manifest entrypoint, failing closed on any resolution error."""
    module_name, separator, attr_path = manifest.entrypoint.partition(":")
    attrs = tuple(part for part in attr_path.split(".") if part)
    if not separator or not module_name.strip() or not attrs:
        raise ConnectorRunnerError(
            f"{manifest.origin}: connector {manifest.name!r} entrypoint must be "
            f"'module:attribute', got {manifest.entrypoint!r}"
        )
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise ConnectorRunnerError(
            f"{manifest.origin}: connector {manifest.name!r} entrypoint module "
            f"{module_name!r} failed to import: {exc}"
        ) from exc
    target: Any = module
    root: Any = module
    for index, attr in enumerate(attrs):
        try:
            target = getattr(target, attr)
        except AttributeError as exc:
            raise ConnectorRunnerError(
                f"{manifest.origin}: connector {manifest.name!r} entrypoint "
                f"{manifest.entrypoint!r} has no attribute {attr!r}"
            ) from exc
        if index == 0:
            root = target
    return ResolvedEntrypoint(
        target=target,
        root=root,
        module=module_name,
        attrs=attrs,
    )


def connector_run_handler(
    manifest: ConnectorManifest,
    context: ConnectorRunContext,
) -> Callable[[Mapping[str, Any]], dict[str, Any]]:
    """Resolve the manifest entrypoint and return an executable run handler."""
    resolved = resolve_connector_entrypoint(manifest)
    adapter = _RUN_ADAPTERS.get(manifest.entrypoint)
    if adapter is not None:
        shape, run = adapter
        _validate_entrypoint_shape(manifest, resolved, shape)
        return lambda options: run(context, resolved, options)
    _validate_generic_entrypoint(manifest, resolved)
    return lambda options: _run_generic_connector(context, resolved, options)


def connector_run_planner(
    manifest: ConnectorManifest,
    context: ConnectorRunContext,
) -> Callable[[Mapping[str, Any]], dict[str, Any]] | None:
    """Return a dry-run planner for manifests that declare one, else None."""
    adapter = _PLAN_ADAPTERS.get(manifest.entrypoint)
    if adapter is None:
        return None
    resolved = resolve_connector_entrypoint(manifest)
    _validate_entrypoint_shape(manifest, resolved, "class")
    return lambda options: adapter(context, resolved, options)


def _validate_entrypoint_shape(
    manifest: ConnectorManifest,
    resolved: ResolvedEntrypoint,
    shape: str,
) -> None:
    valid = {
        "class": lambda: isinstance(resolved.target, type),
        "class_method": lambda: (
            len(resolved.attrs) >= 2
            and isinstance(resolved.root, type)
            and callable(resolved.target)
        ),
        "function": lambda: (
            callable(resolved.target) and not isinstance(resolved.target, type)
        ),
    }[shape]()
    if not valid:
        raise ConnectorRunnerError(
            f"{manifest.origin}: connector {manifest.name!r} entrypoint "
            f"{manifest.entrypoint!r} does not resolve to the expected {shape}"
        )


def _validate_generic_entrypoint(
    manifest: ConnectorManifest,
    resolved: ResolvedEntrypoint,
) -> None:
    if isinstance(resolved.target, type) and callable(
        getattr(resolved.target, "collect", None)
    ):
        return
    raise ConnectorRunnerError(
        f"{manifest.origin}: connector {manifest.name!r} entrypoint "
        f"{manifest.entrypoint!r} has no invocation adapter and does not follow "
        "the generic contract (a class with a collect method)"
    )


def _run_generic_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    connector = entrypoint.target(context.config, layout=context.layout, db=context.db)
    result = connector.collect(**dict(options))
    if asyncio.iscoroutine(result):
        result = _run_async(result)
    if hasattr(result, "to_dict"):
        result = result.to_dict()
    if not isinstance(result, Mapping):
        raise ConnectorRunnerError(
            f"connector entrypoint {entrypoint.module}:{'.'.join(entrypoint.attrs)} "
            f"returned unsupported result type {type(result).__name__}"
        )
    return dict(result)


def _run_arxiv_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    collector = entrypoint.target(db=context.db)
    source = str(
        options.get("source") or context.config.get("sources.arxiv.source", "api")
    )
    if source not in {"api", "rss"}:
        raise ConnectorRunnerError("arxiv connector source must be 'api' or 'rss'")
    limit = _positive_int(
        options.get("limit"),
        default=int(context.config.get("sources.arxiv.limit", 50) or 50),
    )

    if source == "rss":
        categories = _string_list(options.get("categories")) or _string_list(
            context.config.get("sources.arxiv.categories", [])
        )
        if not categories:
            raise ConnectorRunnerError("arxiv RSS execution requires categories")
        feed_format = str(
            options.get("feed_format")
            or context.config.get("sources.arxiv.feed_format", "rss")
        )
        artifacts = collector.scan_rss_feeds(
            categories,
            max_results=limit,
            feed_format=feed_format,
        )
        return {
            "source": source,
            "categories": categories,
            "feed_format": feed_format,
            "queued": _artifact_summaries(artifacts),
            "queued_count": len(artifacts),
        }

    topics = _string_list(options.get("topics")) or _string_list(
        context.config.get("sources.arxiv.topics", [])
    )
    if not topics:
        raise ConnectorRunnerError("arxiv API execution requires topics")
    artifacts = collector.discover_papers(topics, max_results=limit)
    return {
        "source": source,
        "topics": topics,
        "queued": _artifact_summaries(artifacts),
        "queued_count": len(artifacts),
    }


def _run_github_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    token = (
        context.config.get("sources.github.token")
        or os.getenv("GITHUB_API")
        or os.getenv("GITHUB_TOKEN")
    )
    username = _optional_text(options.get("github_user") or options.get("username"))
    if not token and not username:
        raise ConnectorRunnerError(
            "github connector requires a username for public stars, or sources.github.token, GITHUB_API, or GITHUB_TOKEN"
        )

    collector = entrypoint.root(db=context.db)
    limit = _positive_int(
        options.get("limit"),
        default=int(context.config.get("sources.github.limit", 50) or 50),
    )
    discover = getattr(collector, entrypoint.attrs[-1])
    artifacts = discover(
        username=username,
        limit=limit,
        token=token,
    )
    return {
        "username": username or "authenticated account",
        "queued": _artifact_summaries(artifacts),
        "queued_count": len(artifacts),
    }


def _run_huggingface_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    username = _optional_text(
        options.get("hf_user")
        or options.get("username")
        or context.config.get("sources.huggingface.username")
        or os.getenv("HF_USER")
    )
    if not username:
        raise ConnectorRunnerError(
            "huggingface connector requires sources.huggingface.username, HF_USER, or username option"
        )

    collector = entrypoint.root(db=context.db)
    limit = _positive_int(
        options.get("limit"),
        default=int(context.config.get("sources.huggingface.limit", 50) or 50),
    )
    discover = getattr(collector, entrypoint.attrs[-1])
    artifacts = discover(username=username, limit=limit)
    return {
        "username": username,
        "queued": _artifact_summaries(artifacts),
        "queued_count": len(artifacts),
    }


def _run_web_clipper_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    if context.config.get("sources.web_clipper.enabled", True) is False:
        raise ConnectorRunnerError("web_clipper connector is disabled")

    unknown = set(options) - {"limit"}
    if unknown:
        raise ConnectorRunnerError(f"Unsupported web_clipper options: {sorted(unknown)}")
    limit = options.get("limit")
    if limit is not None and (
        isinstance(limit, bool) or not isinstance(limit, int) or limit < 1
    ):
        raise ConnectorRunnerError("web_clipper limit must be a positive integer")
    collector = entrypoint.target(context.config, layout=context.layout, db=context.db)
    records = collector.collect(limit=limit) if limit is not None else collector.collect()
    changed = [record for record in records if record.is_new_or_changed]
    queued = [
        record
        for record in records
        if record.would_queue and record.artifact is not None
    ]
    staged = [record for record in records if record.would_stage]
    return {
        "scanned_count": len(records),
        "changed_count": len(changed),
        "queued_count": len(queued),
        "staged_count": len(staged),
        "deferred_count": len(collector.last_deferred_sources),
        "deferred_sources": collector.last_deferred_sources,
        "queued": [
            {
                "artifact_id": record.artifact.id if record.artifact else None,
                "path": record.path,
                "source_id": record.source_id,
            }
            for record in queued
        ],
        "budget": collector.last_budget_usage,
    }


def _run_x_api_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    no_resume = _optional_bool(options.get("no_resume"))
    return _run_async(
        entrypoint.target(
            context.config,
            layout=context.layout,
            max_results=_optional_int(options.get("max_results")),
            max_pages=_optional_int(options.get("max_pages")),
            resume_from_checkpoint=False if no_resume else None,
        )
    )


def _run_youtube_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    connector = entrypoint.target(context.config, layout=context.layout, db=context.db)
    configured = context.config.get("sources.youtube", {}) or {}
    urls = _string_list(options.get("urls") or options.get("url")) or _string_list(
        configured.get("urls")
    )
    playlist_urls = _string_list(
        options.get("playlist_urls") or options.get("playlist_url")
    ) or _string_list(configured.get("playlist_urls"))
    export_paths = _string_list(
        options.get("export_paths") or options.get("export_path")
    ) or _string_list(configured.get("export_paths"))
    if not urls and not playlist_urls and not export_paths:
        raise ConnectorRunnerError(
            "youtube connector requires urls, playlist_urls, or export_paths"
        )
    archive_video = (
        _optional_bool(options.get("archive_video"))
        if "archive_video" in options
        else None
    )
    no_resume = _optional_bool(options.get("no_resume"))
    result = _run_async(
        connector.collect(
            urls=urls,
            playlist_urls=playlist_urls,
            export_paths=export_paths,
            limit=_optional_int(options.get("limit")),
            archive_video=archive_video,
            archive_max_duration_seconds=_optional_float(
                options.get("archive_max_duration_seconds")
            ),
            archive_max_file_size_bytes=_optional_int(
                options.get("archive_max_file_size_bytes")
            ),
            archive_format=_optional_text(options.get("archive_format")),
            archive_timeout_seconds=_optional_float(
                options.get("archive_timeout_seconds")
            ),
            resume=not bool(no_resume),
        )
    )
    return result.to_dict()


def _run_omi_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    connector = entrypoint.target(context.config, layout=context.layout, db=context.db)
    configured = context.config.get("sources.omi", {}) or {}
    export_paths = _string_list(
        options.get("export_paths") or options.get("export_path")
    ) or _string_list(configured.get("export_paths") or configured.get("export_path"))
    export_dirs = _string_list(
        options.get("export_dirs") or options.get("export_dir")
    ) or _string_list(configured.get("export_dirs") or configured.get("export_dir"))
    api_key_env = options.get("api_key_env") or configured.get("api_key_env")
    api_key_available = bool(
        options.get("api_key")
        or configured.get("api_key")
        or os.getenv(str(api_key_env or "OMI_API_KEY"))
    )
    if not export_paths and not export_dirs and not api_key_available:
        raise ConnectorRunnerError(
            "omi connector requires export_paths, export_dirs, or an Omi API key"
        )
    result = _run_async(
        connector.collect(
            export_paths=export_paths,
            export_dirs=export_dirs,
            file_patterns=_string_list(
                options.get("file_patterns") or options.get("file_pattern")
            )
            or _string_list(configured.get("file_patterns")),
            source_name=options.get("source_name") or configured.get("source_name"),
            device_id=options.get("device_id") or configured.get("device_id"),
            speaker=options.get("speaker") or configured.get("speaker"),
            session_id=options.get("session_id") or configured.get("session_id"),
            language=options.get("language") or configured.get("language"),
            limit=_optional_int(options.get("limit")),
            api_key=options.get("api_key"),
            api_key_env=api_key_env,
            api_base_url=options.get("api_base_url") or configured.get("base_url"),
            api_limit=_optional_int(options.get("api_limit")),
            api_page_size=_optional_int(options.get("api_page_size")),
            include_transcript=_optional_bool(options.get("include_transcript")),
            start_date=options.get("start_date") or configured.get("start_date"),
            end_date=options.get("end_date") or configured.get("end_date"),
            categories=options.get("categories") or configured.get("categories"),
            folder_id=options.get("folder_id") or configured.get("folder_id"),
            starred=_optional_bool(options.get("starred")),
            timeout_seconds=options.get("timeout_seconds")
            or configured.get("timeout_seconds"),
        )
    )
    return result.to_dict()


def _run_skill_outputs_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    connector = entrypoint.target(context.config, layout=context.layout, db=context.db)
    configured = context.config.get("sources.skill_outputs", {}) or {}
    output_paths = _string_list(
        options.get("output_paths")
        or options.get("output_path")
        or options.get("export_paths")
        or options.get("export_path")
    ) or _string_list(configured.get("output_paths") or configured.get("output_path"))
    output_dirs = _string_list(
        options.get("output_dirs")
        or options.get("output_dir")
        or options.get("export_dirs")
        or options.get("export_dir")
    ) or _string_list(configured.get("output_dirs") or configured.get("output_dir"))
    if not output_paths and not output_dirs:
        raise ConnectorRunnerError(
            "skill_outputs connector requires output_paths or output_dirs"
        )
    result = _run_async(
        connector.collect(
            output_paths=output_paths,
            output_dirs=output_dirs,
            file_patterns=_string_list(
                options.get("file_patterns") or options.get("file_pattern")
            )
            or _string_list(configured.get("file_patterns")),
            source_name=options.get("source_name") or configured.get("source_name"),
            limit=_optional_int(options.get("limit")),
        )
    )
    return result.to_dict()


def _run_imported_markdown_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    connector = entrypoint.target(context.config, layout=context.layout, db=context.db)
    configured = context.config.get("sources.imported_markdown", {}) or {}
    import_paths = _string_list(
        options.get("import_paths")
        or options.get("import_path")
        or options.get("input_paths")
        or options.get("input_path")
        or options.get("export_paths")
        or options.get("export_path")
    ) or _string_list(
        configured.get("import_paths")
        or configured.get("import_path")
        or configured.get("paths")
    )
    import_dirs = _string_list(
        options.get("import_dirs")
        or options.get("import_dir")
        or options.get("input_dirs")
        or options.get("input_dir")
        or options.get("export_dirs")
        or options.get("export_dir")
    ) or _string_list(
        configured.get("import_dirs")
        or configured.get("import_dir")
        or configured.get("dirs")
    )
    if not import_paths and not import_dirs:
        raise ConnectorRunnerError(
            "imported_markdown connector requires import_paths or import_dirs"
        )
    result = _run_async(
        connector.collect(
            import_paths=import_paths,
            import_dirs=import_dirs,
            file_patterns=_string_list(
                options.get("file_patterns") or options.get("file_pattern")
            )
            or _string_list(configured.get("file_patterns")),
            source_name=options.get("source_name") or configured.get("source_name"),
            limit=_optional_int(options.get("limit")),
        )
    )
    return result.to_dict()


def _plan_pi_skills_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    connector = entrypoint.target(context.config, layout=context.layout, db=context.db)
    return connector.plan(
        skill_id=options.get("skill") or options.get("skill_id"),
        prompt=options.get("prompt"),
        input_paths=(
            options.get("input_paths")
            or options.get("input_path")
            or options.get("export_paths")
            or options.get("export_path")
        ),
        output_dir=_first_string(options.get("output_dir") or options.get("output_dirs")),
        provider=options.get("provider"),
        model=options.get("model"),
        limit=_optional_int(options.get("limit")),
    )


def _run_pi_skills_connector(
    context: ConnectorRunContext,
    entrypoint: ResolvedEntrypoint,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    connector = entrypoint.target(context.config, layout=context.layout, db=context.db)
    result = _run_async(
        connector.collect(
            skill_id=options.get("skill") or options.get("skill_id"),
            prompt=options.get("prompt"),
            input_paths=(
                options.get("input_paths")
                or options.get("input_path")
                or options.get("export_paths")
                or options.get("export_path")
            ),
            output_dir=_first_string(
                options.get("output_dir") or options.get("output_dirs")
            ),
            provider=options.get("provider"),
            model=options.get("model"),
            limit=_optional_int(options.get("limit")),
            actor=context.actor,
        )
    )
    return result.to_dict()


# Explicit invocation adapters for builtin entrypoints whose calling convention
# does not match the generic `Class(config, layout=..., db=...).collect(**options)`
# contract. Each entry documents why the convention cannot be generic:
_RUN_ADAPTERS: dict[str, tuple[str, RunAdapter]] = {
    # Async module-level function, not a collector class.
    "core.x_api_bookmark_sync:run_x_api_bookmark_backfill": (
        "function",
        _run_x_api_connector,
    ),
    # db-only constructor; dispatches discover_papers/scan_rss_feeds by source mode.
    "collectors.arxiv_collector:ArXivCollector": ("class", _run_arxiv_connector),
    # Class.method entrypoints on a db-only constructor with token/username auth.
    "collectors.social_collector:SocialCollector.discover_github_stars": (
        "class_method",
        _run_github_connector,
    ),
    "collectors.social_collector:SocialCollector.discover_hf_likes": (
        "class_method",
        _run_huggingface_connector,
    ),
    # Sync collect() returning per-file records that need change/budget shaping.
    "collectors.web_clipper_collector:WebClipperCollector": (
        "class",
        _run_web_clipper_connector,
    ),
    # collect() kwargs merge config defaults with option aliases and validation.
    "collectors.youtube_connector:YouTubeConnector": ("class", _run_youtube_connector),
    "collectors.personal_transcript_connector:PersonalTranscriptConnector": (
        "class",
        _run_omi_connector,
    ),
    "collectors.skill_output_connector:SkillOutputConnector": (
        "class",
        _run_skill_outputs_connector,
    ),
    "collectors.imported_markdown_connector:ImportedMarkdownConnector": (
        "class",
        _run_imported_markdown_connector,
    ),
    # collect() takes skill routing kwargs plus the resolved run actor.
    "collectors.pi_skill_connector:PiSkillConnector": ("class", _run_pi_skills_connector),
}

# Dry-run planners keyed by entrypoint; only pi_skills exposes a plan surface.
_PLAN_ADAPTERS: dict[str, RunAdapter] = {
    "collectors.pi_skill_connector:PiSkillConnector": _plan_pi_skills_connector,
}


def _run_async(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    if hasattr(coro, "close"):
        coro.close()
    raise ConnectorRunnerError(
        "Connector execution is not available inside an active event loop"
    )


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _first_string(value: Any) -> str | None:
    values = _string_list(value)
    return values[0] if values else None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ConnectorRunnerError(
            f"Numeric connector option must be a number, got {value!r}"
        ) from exc


def _optional_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    raise ConnectorRunnerError("Boolean connector options must be true or false")


def _positive_int(value: Any, *, default: int) -> int:
    resolved = default if value is None or value == "" else int(value)
    return max(1, resolved)


def _artifact_summaries(artifacts: list[Any]) -> list[dict[str, Any]]:
    summaries = []
    for artifact in artifacts:
        summaries.append(
            {
                "artifact_id": getattr(artifact, "id", None),
                "title": (
                    getattr(artifact, "title", None)
                    or getattr(artifact, "repo_name", None)
                    or getattr(artifact, "source_uri", None)
                ),
                "source_type": getattr(artifact, "source_type", None),
            }
        )
    return summaries


__all__ = [
    "ConnectorRunContext",
    "ConnectorRunnerError",
    "ResolvedEntrypoint",
    "connector_run_handler",
    "connector_run_planner",
    "resolve_connector_entrypoint",
]
