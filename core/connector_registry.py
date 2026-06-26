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


FORBIDDEN_ALLOWED_SIDE_EFFECTS = {
    "direct_wiki_write",
    "wiki_write",
    "wiki_file_write",
    "write_wiki",
    "wiki:write",
}
FORBIDDEN_DIRECT_WIKI_OUTPUTS = {
    "compiled_wiki",
    "compiled_wiki_page",
    "compiled_wiki_path",
    "direct_wiki",
    "page_path",
    "wiki",
    "wiki_file",
    "wiki_output",
    "wiki_output_path",
    "wiki_page",
    "wiki_path",
}


@dataclass(frozen=True)
class ConnectorManifest:
    """Declarative metadata for a connector that can produce artifacts."""

    name: str
    source_name: str
    display_name: str
    artifact_types: tuple[str, ...]
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    entrypoint: str
    queue_capability: bool
    safety_mode: str
    queue_behavior: str
    allowed_side_effects: tuple[str, ...]
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
            "inputs": list(self.inputs),
            "outputs": list(self.outputs),
            "entrypoint": self.entrypoint,
            "queue_capability": self.queue_capability,
            "queue_behavior": self.queue_behavior,
            "safety_mode": self.safety_mode,
            "allowed_side_effects": list(self.allowed_side_effects),
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
        inputs = _required_string_tuple(
            value.get("inputs"),
            field_name="inputs",
            origin=origin,
        )
        outputs = _required_string_tuple(
            value.get("outputs"),
            field_name="outputs",
            origin=origin,
        )
        validate_manifest_outputs(outputs, origin=origin)
        capabilities = _string_tuple(value.get("capabilities"))
        config_keys = _string_tuple(value.get("config_keys"))
        auth = _required_string_tuple(
            value.get("auth"),
            field_name="auth",
            origin=origin,
            allow_empty=True,
        )
        safety_mode = _required_string(value, "safety_mode", origin=origin)
        queue_behavior = _required_string(value, "queue_behavior", origin=origin)
        allowed_side_effects = _required_string_tuple(
            value.get("allowed_side_effects"),
            field_name="allowed_side_effects",
            origin=origin,
            allow_empty=True,
        )
        validate_allowed_side_effects(allowed_side_effects, origin=origin)
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
            inputs=inputs,
            outputs=outputs,
            entrypoint=entrypoint,
            queue_capability=queue_capability,
            safety_mode=safety_mode,
            queue_behavior=queue_behavior,
            allowed_side_effects=allowed_side_effects,
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
        "inputs": ["remote_api:x_bookmarks"],
        "outputs": ["artifact_queue:tweet"],
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
        "queue_behavior": "queues_artifacts",
        "safety_mode": "network_ingest_queue",
        "allowed_side_effects": [
            "network_read",
            "auth_token_read",
            "raw_file_write",
            "artifact_queue_write",
        ],
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
        "inputs": ["remote_api:arxiv"],
        "outputs": ["artifact_queue:paper"],
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
        "queue_behavior": "queues_artifacts",
        "safety_mode": "network_ingest_queue",
        "allowed_side_effects": [
            "network_read",
            "raw_file_write",
            "artifact_queue_write",
        ],
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
        "inputs": ["remote_api:github_stars"],
        "outputs": ["artifact_queue:repository"],
        "capabilities": ["repositories", "stars", "queue"],
        "config_keys": [
            "sources.github.enabled",
            "sources.github.username",
            "sources.github.limit",
            "sources.github.token",
        ],
        "auth": ["sources.github.token", "GITHUB_API", "GITHUB_TOKEN"],
        "queue_capability": True,
        "queue_behavior": "queues_artifacts",
        "safety_mode": "network_ingest_queue",
        "allowed_side_effects": [
            "network_read",
            "auth_token_read",
            "raw_file_write",
            "artifact_queue_write",
        ],
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
        "inputs": ["remote_api:huggingface_likes"],
        "outputs": ["artifact_queue:repository"],
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
        "queue_behavior": "queues_artifacts",
        "safety_mode": "network_ingest_queue",
        "allowed_side_effects": [
            "network_read",
            "auth_token_read",
            "raw_file_write",
            "artifact_queue_write",
        ],
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
        "inputs": [
            "local_files:web_clipper_notes",
            "local_files:web_clipper_attachments",
        ],
        "outputs": ["artifact_queue:web_clipper"],
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
        "queue_behavior": "queues_artifacts",
        "safety_mode": "local_ingest_queue",
        "allowed_side_effects": [
            "local_file_read",
            "raw_file_write",
            "artifact_queue_write",
        ],
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
        "inputs": [
            "remote_api:youtube",
            "remote_media:youtube",
            "local_files:youtube_exports",
        ],
        "outputs": ["artifact_queue:video", "artifact_queue:transcript"],
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
        "queue_behavior": "queues_artifacts",
        "safety_mode": "network_ingest_queue",
        "allowed_side_effects": [
            "network_read",
            "auth_token_read",
            "local_file_read",
            "raw_file_write",
            "artifact_queue_write",
        ],
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
        "description": "Collect Omi conversations or local transcript exports while preserving raw sources and speaker/session/device metadata.",
        "artifact_types": ["transcript"],
        "inputs": ["remote_api:omi", "local_files:omi_exports"],
        "outputs": ["artifact_queue:transcript"],
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
            "sources.omi.api_enabled",
            "sources.omi.api_key_env",
            "sources.omi.base_url",
            "sources.omi.api_limit",
            "sources.omi.api_page_size",
            "sources.omi.include_transcript",
            "sources.omi.source_name",
            "sources.omi.device_id",
            "sources.omi.speaker",
            "sources.omi.language",
        ],
        "auth": ["sources.omi.api_key", "OMI_API_KEY"],
        "queue_capability": True,
        "queue_behavior": "queues_artifacts",
        "safety_mode": "network_ingest_queue",
        "allowed_side_effects": [
            "network_read",
            "auth_token_read",
            "local_file_read",
            "raw_file_write",
            "artifact_queue_write",
        ],
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
        "inputs": ["local_files:skill_output_envelopes"],
        "outputs": ["artifact_queue:*"],
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
        "queue_behavior": "queues_artifacts",
        "safety_mode": "queue_only",
        "allowed_side_effects": [
            "local_file_read",
            "raw_file_write",
            "artifact_queue_write",
        ],
        "entrypoint": "collectors.skill_output_connector:SkillOutputConnector",
        "cli_command": "connectors run skill_outputs",
        "config_namespace": "sources.skill_outputs",
        "default_enabled": True,
    },
    {
        "name": "pi_skills",
        "source_name": "pi_skills",
        "source_aliases": ["pi_skill"],
        "display_name": "Pi Skills",
        "description": "Run configured local Pi skills and ingest their JSON/JSONL artifact envelopes.",
        "artifact_types": [
            "paper",
            "repository",
            "transcript",
            "tweet",
            "video",
            "web_clipper",
        ],
        "inputs": ["operator_prompt", "local_files:allowed_input_roots"],
        "outputs": ["skill_output_envelopes", "artifact_queue:*"],
        "capabilities": ["skills", "pi", "envelopes", "queue", "raw_preservation"],
        "config_keys": [
            "sources.pi_skills.enabled",
            "sources.pi_skills.skills",
            "sources.pi_skills.output_dir",
            "sources.pi_skills.default_provider",
            "sources.pi_skills.default_model",
            "sources.pi_skills.fallback",
        ],
        "auth": ["llm.providers.pi", "llm.providers.pi_openrouter"],
        "queue_capability": True,
        "queue_behavior": "queues_artifacts",
        "safety_mode": "no_tools_json",
        "allowed_side_effects": [
            "llm_api_call",
            "subprocess_exec",
            "local_file_read",
            "local_file_write",
            "artifact_queue_write",
        ],
        "entrypoint": "collectors.pi_skill_connector:PiSkillConnector",
        "cli_command": "connectors run pi_skills",
        "config_namespace": "sources.pi_skills",
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
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if value is None:
        raise ConnectorManifestError(f"{origin}: connector manifest requires {field_name}")
    items = _string_tuple(value)
    if not items and not allow_empty:
        raise ConnectorManifestError(f"{origin}: connector manifest requires {field_name}")
    return items


def validate_allowed_side_effects(
    side_effects: Iterable[str],
    *,
    origin: str,
) -> None:
    for side_effect in side_effects:
        normalized = str(side_effect).strip().lower().replace("-", "_")
        if normalized in FORBIDDEN_ALLOWED_SIDE_EFFECTS:
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot allow direct wiki writes"
            )
        if "wiki" in normalized and "write" in normalized:
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot allow direct wiki writes"
            )


def validate_manifest_outputs(
    outputs: Iterable[str],
    *,
    origin: str,
) -> None:
    """Reject connector output contracts that target compiled wiki files directly."""
    for output in outputs:
        normalized = str(output).strip().lower().replace("\\", "/").replace("-", "_")
        if not normalized:
            continue
        output_parts = [
            part for part in normalized.replace(":", "/").split("/") if part
        ]
        if normalized in FORBIDDEN_DIRECT_WIKI_OUTPUTS:
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot declare direct wiki outputs"
            )
        if any(part in FORBIDDEN_DIRECT_WIKI_OUTPUTS for part in output_parts):
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot declare direct wiki outputs"
            )
        if normalized.startswith(("wiki/", "./wiki/", "../wiki/")):
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot declare direct wiki outputs"
            )
