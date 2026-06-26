"""Shared service layer for CLI and MCP agent-facing surfaces."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from .config import Config, config
from .connector_registry import connector_policy_status, load_connector_registry
from .ingestion_runtime import KnowledgeArtifactRuntime
from .metadata_db import IngestionQueueEntry, MetadataDB, get_metadata_db
from .path_layout import PathLayout, build_path_layout
from .prompt_security import (
    THOTH_REDACTION_METADATA_KEY,
    THOTH_SECURITY_AUDIT_KEY,
    THOTH_SECURITY_FINDINGS_KEY,
    THOTH_SECURITY_FINDING_COUNT_KEY,
    THOTH_SECURITY_PATTERN_IDS_KEY,
    THOTH_SECURITY_POLICY_KEY,
    prompt_security_requires_review,
)
from .research_graph import ResearchGraphService
from .wiki_io import read_document
from .wiki_query import WikiQueryRunner


class AgentSurfaceError(RuntimeError):
    """Raised when an agent-facing request cannot be fulfilled safely."""


class AgentSurfaceService:
    """Stable service API for agent-facing CLI and MCP tools."""

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
    ):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.db = db or get_metadata_db()

    def query_wiki(self, query: str, *, limit: int = 10) -> dict[str, Any]:
        """Search the compiled wiki and include source provenance for each hit."""
        runner = WikiQueryRunner(self.config, layout=self.layout)
        result = runner.search(query, limit=limit)
        hits = []
        for hit in result.hits:
            document = read_document(hit.page_path)
            frontmatter = document.frontmatter
            hits.append(
                {
                    "slug": hit.slug,
                    "title": hit.title,
                    "summary": hit.summary,
                    "score": hit.score,
                    "matched_fields": list(hit.matched_fields),
                    "page_path": str(hit.page_path),
                    "kind": hit.kind,
                    "record_type": hit.record_type,
                    "provenance": {
                        "artifact_id": frontmatter.get("thoth_artifact_id")
                        or frontmatter.get("artifact_id"),
                        "source_type": frontmatter.get("thoth_source_type")
                        or frontmatter.get("source_type"),
                        "source_paths": list(hit.source_paths),
                        "resource": frontmatter.get("resource"),
                        "related_slugs": list(hit.related_slugs),
                    },
                }
            )
        return {
            "query": result.query,
            "queried_at": result.queried_at,
            "hits": hits,
        }

    def list_artifacts(
        self,
        *,
        artifact_type: str | None = None,
        status: str | None = None,
        source: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """List queued/processed artifacts with queue provenance."""
        entries = self.db.list_ingestion_entries(
            artifact_type=artifact_type,
            status=status,
            source=source,
            limit=limit,
        )
        return {
            "artifacts": [self._serialize_ingestion_entry(entry) for entry in entries],
            "total": len(entries),
        }

    def get_artifact(
        self,
        artifact_id: str,
        *,
        include_quarantined: bool = False,
    ) -> dict[str, Any]:
        """Return canonical artifact data and queue provenance for one artifact."""
        entry = self.db.get_ingestion_entry(artifact_id)
        if not entry:
            raise AgentSurfaceError(f"Artifact not found: {artifact_id}")
        if not include_quarantined and _entry_requires_security_review(entry):
            raise AgentSurfaceError(
                f"Artifact requires security review: {artifact_id}"
            )
        runtime = KnowledgeArtifactRuntime(self.config, layout=self.layout, db=self.db)
        artifact = runtime.materialize_artifact(entry)
        return {
            "queue": self._serialize_ingestion_entry(entry),
            "canonical_record": artifact.canonical_record(),
            "provenance": artifact.provenance.to_dict() if artifact.provenance else {},
        }

    def get_artifact_provenance(
        self,
        artifact_id: str,
        *,
        include_quarantined: bool = False,
    ) -> dict[str, Any]:
        """Return provenance only for a queued artifact."""
        artifact = self.get_artifact(
            artifact_id,
            include_quarantined=include_quarantined,
        )
        return {
            "artifact_id": artifact_id,
            "queue": artifact["queue"],
            "provenance": artifact["provenance"],
        }

    def approve_artifact_security_override(
        self,
        artifact_id: str,
        *,
        actor: str,
        reason: str,
    ) -> dict[str, Any]:
        """Approve a quarantined artifact for processing with audit metadata."""
        entry = self.db.approve_ingestion_security_override(
            artifact_id,
            actor=actor,
            reason=reason,
        )
        if not entry:
            raise AgentSurfaceError(f"Artifact not found: {artifact_id}")
        return {"queue": self._serialize_ingestion_entry(entry)}

    def list_connectors(self) -> dict[str, Any]:
        """Return connector registry metadata."""
        return load_connector_registry(self.config).to_dict(config=self.config)

    def connector_run_plan(
        self,
        connector_name: str,
        *,
        execute: bool = False,
        options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Backward-compatible wrapper for connector execution planning."""
        return self.run_connector(
            connector_name,
            execute=execute,
            options=options,
        )

    def run_connector(
        self,
        connector_name: str,
        *,
        execute: bool = False,
        options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Plan or execute a connector through the shared agent service layer."""
        registry = load_connector_registry(self.config)
        manifest = registry.get(connector_name)
        policy = connector_policy_status(manifest, self.config)
        sanitized_options = {
            str(key): value
            for key, value in dict(options or {}).items()
            if value is not None
        }
        plan = {
            "status": "planned",
            "execute": False,
            "connector": manifest.to_dict(config=self.config),
            "policy": policy,
            "options": sanitized_options,
        }
        if not execute:
            if manifest.name == "pi_skills":
                plan["run_plan"] = self._plan_pi_skills_connector(sanitized_options)
            return plan
        if not manifest.is_enabled(self.config):
            raise AgentSurfaceError(f"Connector is disabled: {connector_name}")
        if not policy["allowlist"]["allowed"]:
            raise AgentSurfaceError(
                f"Connector is not allowlisted: {connector_name}"
            )
        if policy["pins"]["drift"]:
            drift_fields = ", ".join(
                str(item["field"]) for item in policy["pins"]["drift"]
            )
            raise AgentSurfaceError(
                f"Connector pin drift detected for {connector_name}: {drift_fields}"
            )

        handlers = {
            "arxiv": self._run_arxiv_connector,
            "github": self._run_github_connector,
            "huggingface": self._run_huggingface_connector,
            "web_clipper": self._run_web_clipper_connector,
            "x_api": self._run_x_api_connector,
            "youtube": self._run_youtube_connector,
            "omi": self._run_omi_connector,
            "personal_transcripts": self._run_omi_connector,
            "skill_outputs": self._run_skill_outputs_connector,
            "external_skill": self._run_skill_outputs_connector,
            "last30days-skill": self._run_skill_outputs_connector,
            "pi_skills": self._run_pi_skills_connector,
            "pi_skill": self._run_pi_skills_connector,
        }
        handler = handlers.get(manifest.name) or handlers.get(connector_name)
        if handler is None:
            raise AgentSurfaceError(
                f"Connector {connector_name!r} has no executable adapter registered"
            )

        result = handler(sanitized_options)
        return {
            **plan,
            "status": "completed",
            "execute": True,
            "result": serialize_agent_payload(result),
        }

    def missing_papers(
        self,
        *,
        min_references: int = 2,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Return the research graph missing-paper report."""
        return ResearchGraphService(self.db).missing_papers_report(
            min_references=min_references,
            limit=limit,
        )

    def _serialize_ingestion_entry(self, entry: IngestionQueueEntry) -> dict[str, Any]:
        return {
            "artifact_id": entry.artifact_id,
            "artifact_type": entry.artifact_type,
            "source": entry.source,
            "priority": entry.priority,
            "status": entry.status,
            "attempts": entry.attempts,
            "last_error": entry.last_error,
            "next_attempt_at": entry.next_attempt_at,
            "created_at": entry.created_at,
            "processed_at": entry.processed_at,
            "capabilities": _json_list(entry.capabilities_json),
            "security_metadata": _security_metadata_from_payload(entry.payload_json),
        }

    def _run_arxiv_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        from collectors.arxiv_collector import ArXivCollector

        collector = ArXivCollector(db=self.db)
        source = str(options.get("source") or self.config.get("sources.arxiv.source", "api"))
        if source not in {"api", "rss"}:
            raise AgentSurfaceError("arxiv connector source must be 'api' or 'rss'")
        limit = _positive_int(
            options.get("limit"),
            default=int(self.config.get("sources.arxiv.limit", 50) or 50),
        )

        if source == "rss":
            categories = _string_list(options.get("categories")) or _string_list(
                self.config.get("sources.arxiv.categories", [])
            )
            if not categories:
                raise AgentSurfaceError("arxiv RSS execution requires categories")
            feed_format = str(
                options.get("feed_format")
                or self.config.get("sources.arxiv.feed_format", "rss")
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
            self.config.get("sources.arxiv.topics", [])
        )
        if not topics:
            raise AgentSurfaceError("arxiv API execution requires topics")
        artifacts = collector.discover_papers(topics, max_results=limit)
        return {
            "source": source,
            "topics": topics,
            "queued": _artifact_summaries(artifacts),
            "queued_count": len(artifacts),
        }

    def _run_github_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        token = (
            self.config.get("sources.github.token")
            or os.getenv("GITHUB_API")
            or os.getenv("GITHUB_TOKEN")
        )
        username = _optional_text(options.get("github_user") or options.get("username"))
        if not token and not username:
            raise AgentSurfaceError(
                "github connector requires a username for public stars, or sources.github.token, GITHUB_API, or GITHUB_TOKEN"
            )

        from collectors.social_collector import SocialCollector

        collector = SocialCollector(db=self.db)
        limit = _positive_int(
            options.get("limit"),
            default=int(self.config.get("sources.github.limit", 50) or 50),
        )
        artifacts = collector.discover_github_stars(
            username=username,
            limit=limit,
            token=token,
        )
        return {
            "username": username or "authenticated account",
            "queued": _artifact_summaries(artifacts),
            "queued_count": len(artifacts),
        }

    def _run_huggingface_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        username = _optional_text(
            options.get("hf_user")
            or options.get("username")
            or self.config.get("sources.huggingface.username")
            or os.getenv("HF_USER")
        )
        if not username:
            raise AgentSurfaceError(
                "huggingface connector requires sources.huggingface.username, HF_USER, or username option"
            )

        from collectors.social_collector import SocialCollector

        collector = SocialCollector(db=self.db)
        limit = _positive_int(
            options.get("limit"),
            default=int(self.config.get("sources.huggingface.limit", 50) or 50),
        )
        artifacts = collector.discover_hf_likes(username=username, limit=limit)
        return {
            "username": username,
            "queued": _artifact_summaries(artifacts),
            "queued_count": len(artifacts),
        }

    def _run_web_clipper_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        if self.config.get("sources.web_clipper.enabled", True) is False:
            raise AgentSurfaceError("web_clipper connector is disabled")

        from collectors.web_clipper_collector import WebClipperCollector

        collector = WebClipperCollector(self.config, layout=self.layout, db=self.db)
        records = collector.collect()
        changed = [record for record in records if record.is_new_or_changed]
        queued = [
            record
            for record in changed
            if record.file_type == "note" and record.artifact is not None
        ]
        staged = [record for record in changed if record.file_type == "attachment"]
        return {
            "scanned_count": len(records),
            "changed_count": len(changed),
            "queued_count": len(queued),
            "staged_count": len(staged),
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

    def _run_x_api_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        from .x_api_bookmark_sync import run_x_api_bookmark_backfill

        return _run_async(
            run_x_api_bookmark_backfill(
                self.config,
                layout=self.layout,
                max_results=_optional_int(options.get("max_results")),
                max_pages=_optional_int(options.get("max_pages")),
                resume_from_checkpoint=(
                    False if bool(options.get("no_resume", False)) else None
                ),
            )
        )

    def _run_youtube_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        from collectors.youtube_connector import YouTubeConnector

        connector = YouTubeConnector(self.config, layout=self.layout, db=self.db)
        configured = self.config.get("sources.youtube", {}) or {}
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
            raise AgentSurfaceError(
                "youtube connector requires urls, playlist_urls, or export_paths"
            )
        result = _run_async(
            connector.collect(
                urls=urls,
                playlist_urls=playlist_urls,
                export_paths=export_paths,
                limit=_optional_int(options.get("limit")),
                archive_video=(
                    bool(options["archive_video"])
                    if "archive_video" in options
                    else None
                ),
                resume=not bool(options.get("no_resume", False)),
            )
        )
        return result.to_dict()

    def _run_omi_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        from collectors.personal_transcript_connector import PersonalTranscriptConnector

        connector = PersonalTranscriptConnector(self.config, layout=self.layout, db=self.db)
        configured = self.config.get("sources.omi", {}) or {}
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
            raise AgentSurfaceError(
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

    def _run_skill_outputs_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        from collectors.skill_output_connector import SkillOutputConnector

        connector = SkillOutputConnector(self.config, layout=self.layout, db=self.db)
        configured = self.config.get("sources.skill_outputs", {}) or {}
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
            raise AgentSurfaceError(
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

    def _plan_pi_skills_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        from collectors.pi_skill_connector import PiSkillConnector

        connector = PiSkillConnector(self.config, layout=self.layout, db=self.db)
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

    def _run_pi_skills_connector(self, options: Mapping[str, Any]) -> dict[str, Any]:
        from collectors.pi_skill_connector import PiSkillConnector

        connector = PiSkillConnector(self.config, layout=self.layout, db=self.db)
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
            )
        )
        return result.to_dict()


def serialize_agent_payload(value: Any) -> Any:
    """Convert service objects into JSON-friendly payloads."""
    if is_dataclass(value):
        return serialize_agent_payload(asdict(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): serialize_agent_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [serialize_agent_payload(item) for item in value]
    return value


def _json_list(value: str | None) -> list[str]:
    if not value:
        return []

    payload = json.loads(value)
    if not isinstance(payload, list):
        raise AgentSurfaceError("Queue capabilities_json must decode to a list")
    return [str(item) for item in payload]


def _security_metadata_from_payload(payload_json: str | None) -> dict[str, Any]:
    if not payload_json:
        return {}
    try:
        payload = json.loads(payload_json)
    except Exception:
        return {}
    if not isinstance(payload, Mapping):
        return {}
    normalized_metadata = payload.get("normalized_metadata")
    if not isinstance(normalized_metadata, Mapping):
        return {}
    return {
        key: normalized_metadata[key]
        for key in (
            THOTH_SECURITY_FINDINGS_KEY,
            THOTH_SECURITY_FINDING_COUNT_KEY,
            THOTH_SECURITY_PATTERN_IDS_KEY,
            THOTH_SECURITY_POLICY_KEY,
            THOTH_SECURITY_AUDIT_KEY,
            THOTH_REDACTION_METADATA_KEY,
        )
        if normalized_metadata.get(key)
    }


def _entry_requires_security_review(entry: IngestionQueueEntry) -> bool:
    if entry.status in {"needs_review", "blocked"}:
        return True
    metadata = _security_metadata_from_payload(entry.payload_json)
    return prompt_security_requires_review(metadata)


def _run_async(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    if hasattr(coro, "close"):
        coro.close()
    raise AgentSurfaceError("Connector execution is not available inside an active event loop")


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
    raise AgentSurfaceError("Boolean connector options must be true or false")


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
