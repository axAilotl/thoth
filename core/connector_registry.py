"""Connector manifest discovery for source ingestion backends."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol


class ConfigLike(Protocol):
    def get(self, key: str, default: Any = None) -> Any:
        ...


class ConnectorManifestError(ValueError):
    """Raised when a connector manifest is missing required contract fields."""


@dataclass(frozen=True)
class ConnectorManifest:
    """Declarative metadata for a connector that can produce artifacts."""

    name: str
    source_name: str
    display_name: str
    artifact_types: tuple[str, ...]
    entrypoint: str
    queue_capability: bool
    capabilities: tuple[str, ...] = field(default_factory=tuple)
    config_keys: tuple[str, ...] = field(default_factory=tuple)
    auth: tuple[str, ...] = field(default_factory=tuple)
    source_aliases: tuple[str, ...] = field(default_factory=tuple)
    cli_command: str | None = None
    config_namespace: str | None = None
    default_enabled: bool = True
    description: str = ""
    origin: str = "builtin"

    @property
    def source_names(self) -> tuple[str, ...]:
        names = [self.source_name, *self.source_aliases]
        return tuple(dict.fromkeys(name for name in names if name))

    def is_enabled(self, config: ConfigLike | None = None) -> bool:
        if config is None or not self.config_namespace:
            return self.default_enabled
        source_config = config.get(self.config_namespace, None)
        if not isinstance(source_config, Mapping):
            return self.default_enabled
        return bool(source_config.get("enabled", self.default_enabled))

    def to_dict(self, *, config: ConfigLike | None = None) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_name": self.source_name,
            "source_aliases": list(self.source_aliases),
            "source_names": list(self.source_names),
            "display_name": self.display_name,
            "description": self.description,
            "artifact_types": list(self.artifact_types),
            "entrypoint": self.entrypoint,
            "queue_capability": self.queue_capability,
            "capabilities": list(self.capabilities),
            "config_keys": list(self.config_keys),
            "auth": list(self.auth),
            "cli_command": self.cli_command,
            "config_namespace": self.config_namespace,
            "enabled": self.is_enabled(config),
            "origin": self.origin,
        }

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        origin: str,
    ) -> "ConnectorManifest":
        name = _required_string(value, "name", origin=origin)
        source_name = _required_string(value, "source_name", origin=origin)
        display_name = _optional_string(value.get("display_name")) or name
        entrypoint = _required_string(value, "entrypoint", origin=origin)
        artifact_types = _required_string_tuple(
            value.get("artifact_types"),
            field_name="artifact_types",
            origin=origin,
        )
        capabilities = _string_tuple(value.get("capabilities"))
        config_keys = _string_tuple(value.get("config_keys"))
        auth = _string_tuple(value.get("auth"))
        source_aliases = _string_tuple(value.get("source_aliases"))
        cli_command = _optional_string(value.get("cli_command"))
        config_namespace = _optional_string(value.get("config_namespace"))
        description = _optional_string(value.get("description")) or ""
        default_enabled = bool(value.get("default_enabled", True))

        queue_capability = value.get("queue_capability")
        if not isinstance(queue_capability, bool):
            raise ConnectorManifestError(
                f"{origin}: connector {name!r} requires boolean queue_capability"
            )

        return cls(
            name=name,
            source_name=source_name,
            display_name=display_name,
            artifact_types=artifact_types,
            entrypoint=entrypoint,
            queue_capability=queue_capability,
            capabilities=capabilities,
            config_keys=config_keys,
            auth=auth,
            source_aliases=source_aliases,
            cli_command=cli_command,
            config_namespace=config_namespace,
            default_enabled=default_enabled,
            description=description,
            origin=origin,
        )


class ConnectorRegistry:
    """Resolved connector manifests in discovery order."""

    def __init__(self, manifests: Iterable[ConnectorManifest]):
        self._manifests = tuple(manifests)
        self._by_name = {manifest.name: manifest for manifest in self._manifests}
        self._by_source_name: dict[str, ConnectorManifest] = {}
        for manifest in self._manifests:
            for source_name in manifest.source_names:
                self._by_source_name.setdefault(source_name, manifest)

    def list(self) -> list[ConnectorManifest]:
        return list(self._manifests)

    def get(self, name: str) -> ConnectorManifest:
        manifest = self._by_name.get(name) or self._by_source_name.get(name)
        if manifest is None:
            raise KeyError(f"Unknown connector: {name}")
        return manifest

    def to_dict(self, *, config: ConfigLike | None = None) -> dict[str, Any]:
        return {
            "connectors": [
                manifest.to_dict(config=config) for manifest in self._manifests
            ],
            "total": len(self._manifests),
        }


BUILTIN_CONNECTOR_MANIFESTS: tuple[dict[str, Any], ...] = (
    {
        "name": "x_api",
        "source_name": "x_api_backfill",
        "source_aliases": ["browser_extension", "x_api"],
        "display_name": "X API Bookmarks",
        "description": "Backfill X/Twitter bookmarks into the bookmark queue.",
        "artifact_types": ["tweet"],
        "capabilities": ["bookmarks", "oauth", "queue"],
        "config_keys": [
            "sources.x_api.client_id",
            "sources.x_api.redirect_uri",
            "sources.x_api.scopes",
            "automation.x_api_sync",
        ],
        "auth": [
            "sources.x_api.client_id",
            "sources.x_api.redirect_uri",
            "x_api_token_bundle",
        ],
        "queue_capability": True,
        "entrypoint": "core.x_api_bookmark_sync:run_x_api_bookmark_backfill",
        "cli_command": "x-api-sync",
        "config_namespace": "sources.x_api",
        "default_enabled": False,
    },
    {
        "name": "arxiv",
        "source_name": "arxiv",
        "source_aliases": ["arxiv_rss"],
        "display_name": "arXiv",
        "description": "Discover papers from arXiv search and category feeds.",
        "artifact_types": ["paper"],
        "capabilities": ["papers", "metadata", "queue"],
        "config_keys": [
            "sources.arxiv.source",
            "sources.arxiv.feed_format",
            "sources.arxiv.topics",
            "sources.arxiv.categories",
            "sources.arxiv.limit",
        ],
        "auth": [],
        "queue_capability": True,
        "entrypoint": "collectors.arxiv_collector:ArXivCollector",
        "cli_command": "arxiv",
        "config_namespace": "sources.arxiv",
        "default_enabled": True,
    },
    {
        "name": "github",
        "source_name": "github",
        "display_name": "GitHub Stars",
        "description": "Collect starred GitHub repositories.",
        "artifact_types": ["repository"],
        "capabilities": ["repositories", "stars", "queue"],
        "config_keys": [
            "sources.github.enabled",
            "sources.github.username",
            "sources.github.limit",
            "sources.github.token",
        ],
        "auth": ["sources.github.token", "GITHUB_API", "GITHUB_TOKEN"],
        "queue_capability": True,
        "entrypoint": "collectors.social_collector:SocialCollector.discover_github_stars",
        "cli_command": "social",
        "config_namespace": "sources.github",
        "default_enabled": True,
    },
    {
        "name": "huggingface",
        "source_name": "huggingface",
        "display_name": "Hugging Face Likes",
        "description": "Collect liked Hugging Face models, datasets, and spaces.",
        "artifact_types": ["repository"],
        "capabilities": ["repositories", "likes", "queue"],
        "config_keys": [
            "sources.huggingface.enabled",
            "sources.huggingface.username",
            "sources.huggingface.limit",
            "sources.huggingface.token",
            "sources.huggingface.include_models",
            "sources.huggingface.include_datasets",
            "sources.huggingface.include_spaces",
        ],
        "auth": [
            "sources.huggingface.token",
            "HF_USER",
            "HF_TOKEN",
            "HUGGINGFACEHUB_API_TOKEN",
            "HUGGINGFACE_API_TOKEN",
        ],
        "queue_capability": True,
        "entrypoint": "collectors.social_collector:SocialCollector.discover_hf_likes",
        "cli_command": "social",
        "config_namespace": "sources.huggingface",
        "default_enabled": True,
    },
    {
        "name": "web_clipper",
        "source_name": "web_clipper",
        "display_name": "Web Clipper",
        "description": "Index configured Web Clipper notes and staged attachments.",
        "artifact_types": ["web_clipper"],
        "capabilities": ["markdown", "frontmatter", "attachments", "queue"],
        "config_keys": [
            "sources.web_clipper.enabled",
            "sources.web_clipper.note_dirs",
            "sources.web_clipper.attachment_dirs",
            "sources.web_clipper.note_extensions",
            "sources.web_clipper.attachment_extensions",
        ],
        "auth": [],
        "queue_capability": True,
        "entrypoint": "collectors.web_clipper_collector:WebClipperCollector",
        "cli_command": "web-clipper",
        "config_namespace": "sources.web_clipper",
        "default_enabled": True,
    },
    {
        "name": "youtube",
        "source_name": "youtube",
        "display_name": "YouTube",
        "description": "Collect YouTube video metadata and transcripts from URLs, playlists, and local exports.",
        "artifact_types": ["video", "transcript"],
        "capabilities": ["video", "transcripts", "playlists", "exports", "queue", "archive"],
        "config_keys": [
            "sources.youtube.enabled",
            "sources.youtube.urls",
            "sources.youtube.playlist_urls",
            "sources.youtube.export_paths",
            "sources.youtube.archive_video",
            "sources.youtube.api_key",
            "youtube.enable_transcripts",
            "youtube.enable_llm_transcript_processing",
        ],
        "auth": ["sources.youtube.api_key", "YOUTUBE_API_KEY"],
        "queue_capability": True,
        "entrypoint": "collectors.youtube_connector:YouTubeConnector",
        "cli_command": "connectors run youtube",
        "config_namespace": "sources.youtube",
        "default_enabled": True,
    },
    {
        "name": "omi",
        "source_name": "omi",
        "source_aliases": ["personal_transcripts", "personal_transcript"],
        "display_name": "Omi / Personal Transcripts",
        "description": "Collect Omi-style transcript exports while preserving raw exports and speaker/session/device metadata.",
        "artifact_types": ["transcript"],
        "capabilities": [
            "transcripts",
            "personal_data",
            "speaker_metadata",
            "session_metadata",
            "queue",
        ],
        "config_keys": [
            "sources.omi.enabled",
            "sources.omi.export_paths",
            "sources.omi.export_dirs",
            "sources.omi.file_patterns",
            "sources.omi.source_name",
            "sources.omi.device_id",
            "sources.omi.speaker",
            "sources.omi.language",
        ],
        "auth": [],
        "queue_capability": True,
        "entrypoint": "collectors.personal_transcript_connector:PersonalTranscriptConnector",
        "cli_command": "connectors run omi",
        "config_namespace": "sources.omi",
        "default_enabled": True,
    },
    {
        "name": "skill_outputs",
        "source_name": "skill_outputs",
        "source_aliases": ["external_skill", "last30days-skill"],
        "display_name": "External Skill Outputs",
        "description": "Ingest JSON/JSONL output envelopes from external skills through the artifact queue.",
        "artifact_types": [
            "paper",
            "repository",
            "transcript",
            "tweet",
            "video",
            "web_clipper",
        ],
        "capabilities": ["skills", "envelopes", "queue", "raw_preservation"],
        "config_keys": [
            "sources.skill_outputs.enabled",
            "sources.skill_outputs.output_paths",
            "sources.skill_outputs.output_dirs",
            "sources.skill_outputs.file_patterns",
            "sources.skill_outputs.source_name",
        ],
        "auth": [],
        "queue_capability": True,
        "entrypoint": "collectors.skill_output_connector:SkillOutputConnector",
        "cli_command": "connectors run skill_outputs",
        "config_namespace": "sources.skill_outputs",
        "default_enabled": True,
    },
)


def load_connector_registry(
    config: ConfigLike | None = None,
    *,
    project_root: Path | None = None,
) -> ConnectorRegistry:
    """Discover built-in connectors first, then optional plugin manifests."""
    manifests: list[ConnectorManifest] = []
    names: set[str] = set()

    for payload in BUILTIN_CONNECTOR_MANIFESTS:
        manifest = ConnectorManifest.from_mapping(payload, origin="builtin")
        manifests.append(manifest)
        names.add(manifest.name)

    for manifest_path in _iter_plugin_manifest_paths(config, project_root=project_root):
        manifest = _load_manifest_file(manifest_path)
        if manifest.name in names:
            raise ConnectorManifestError(
                f"{manifest_path}: duplicate connector name {manifest.name!r}"
            )
        manifests.append(manifest)
        names.add(manifest.name)

    return ConnectorRegistry(manifests)


def _load_manifest_file(path: Path) -> ConnectorManifest:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConnectorManifestError(f"{path}: invalid connector JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ConnectorManifestError(f"{path}: connector manifest must be an object")
    return ConnectorManifest.from_mapping(payload, origin=str(path))


def _iter_plugin_manifest_paths(
    config: ConfigLike | None,
    *,
    project_root: Path | None,
) -> Iterable[Path]:
    root = project_root or Path.cwd()
    raw_dirs: list[str] = []
    if config is not None:
        raw_dirs.extend(_configured_paths(config.get("connectors.plugin_dirs")))
        raw_dirs.extend(_configured_paths(config.get("connectors.skill_dirs")))
        raw_dirs.extend(_configured_paths(config.get("skills.connector_dirs")))
    env_path = os.getenv("THOTH_CONNECTOR_PATH")
    if env_path:
        raw_dirs.extend(item for item in env_path.split(os.pathsep) if item.strip())

    seen: set[Path] = set()
    for raw_dir in raw_dirs:
        candidate = Path(raw_dir).expanduser()
        if not candidate.is_absolute():
            candidate = root / candidate
        candidate = candidate.resolve()
        for manifest_path in _manifest_paths_under(candidate):
            if manifest_path in seen:
                continue
            seen.add(manifest_path)
            yield manifest_path


def _manifest_paths_under(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    if not path.exists():
        return
    direct_names = ("connector.json", "manifest.json")
    for name in direct_names:
        candidate = path / name
        if candidate.is_file():
            yield candidate
    for candidate in sorted(path.glob("*.connector.json")):
        if candidate.is_file():
            yield candidate
    for child in sorted(path.iterdir(), key=lambda item: item.name):
        if not child.is_dir():
            continue
        for name in direct_names:
            candidate = child / name
            if candidate.is_file():
                yield candidate


def _configured_paths(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(os.pathsep) if item.strip()]
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ConnectorManifestError("connector directory configuration must be a string or list")


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_string(
    value: Mapping[str, Any],
    field_name: str,
    *,
    origin: str,
) -> str:
    text = _optional_string(value.get(field_name))
    if not text:
        raise ConnectorManifestError(f"{origin}: connector manifest requires {field_name}")
    return text


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ConnectorManifestError("connector manifest list fields must be arrays")
    return tuple(str(item).strip() for item in value if str(item).strip())


def _required_string_tuple(
    value: Any,
    *,
    field_name: str,
    origin: str,
) -> tuple[str, ...]:
    items = _string_tuple(value)
    if not items:
        raise ConnectorManifestError(f"{origin}: connector manifest requires {field_name}")
    return items
