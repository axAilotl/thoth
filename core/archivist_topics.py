"""Validated archivist topic registry loading and normalization."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import re
from typing import Any

import yaml

from .config import Config

ARCHIVIST_TOPICS_FILENAME = "archivist_topics.yaml"
ARCHIVIST_TOPICS_EXAMPLE_FILENAME = "archivist_topics.example.yaml"
ARCHIVIST_TOPIC_REGISTRY_VERSION = 1

_TOPIC_ID_RE = re.compile(r"^[a-z0-9]+(?:[a-z0-9_-]*[a-z0-9])?$")
_WHITESPACE_RE = re.compile(r"\s+")


class ArchivistTopicConfigError(ValueError):
    """Raised when the archivist topic registry is missing or invalid."""


@dataclass(frozen=True)
class ArchivistTopicDefaults:
    """Registry-level defaults applied to topic definitions."""

    cadence_hours: float = 12.0
    max_sources: int | None = 120
    allow_manual_force: bool = True


@dataclass(frozen=True)
class ArchivistTopicDefinition:
    """Single archivist topic definition loaded from YAML."""

    id: str
    title: str
    output_path: str
    include_roots: tuple[str, ...]
    exclude_roots: tuple[str, ...] = ()
    source_types: tuple[str, ...] = ()
    include_tags: tuple[str, ...] = ()
    exclude_tags: tuple[str, ...] = ()
    include_terms: tuple[str, ...] = ()
    exclude_terms: tuple[str, ...] = ()
    description: str | None = None
    cadence_hours: float = 12.0
    max_sources: int | None = 120
    allow_manual_force: bool = True

    def output_path_for_root(self, wiki_root: Path) -> Path:
        """Resolve the topic output path inside the compiled wiki root."""
        return wiki_root / PurePosixPath(self.output_path)


@dataclass(frozen=True)
class ArchivistTopicRegistry:
    """Normalized archivist topic registry."""

    topics: tuple[ArchivistTopicDefinition, ...] = ()
    source_path: Path | None = None
    version: int = ARCHIVIST_TOPIC_REGISTRY_VERSION
    defaults: ArchivistTopicDefaults = ArchivistTopicDefaults()

    def get_topic(self, topic_id: str) -> ArchivistTopicDefinition | None:
        normalized_id = _normalize_topic_id(topic_id, field_name="topic_id")
        for topic in self.topics:
            if topic.id == normalized_id:
                return topic
        return None


def resolve_archivist_topics_path(
    config: Config,
    *,
    project_root: Path | None = None,
) -> Path:
    """Resolve the archivist topic registry path relative to the project root."""
    raw_value = config.get("paths.archivist_topics_file")
    candidate = Path(str(raw_value).strip()) if raw_value else Path(ARCHIVIST_TOPICS_FILENAME)
    if candidate.is_absolute():
        return candidate
    return (project_root or Path.cwd()) / candidate


def resolve_archivist_topics_example_path(
    *,
    project_root: Path | None = None,
) -> Path:
    """Resolve the shipped archivist topic example path relative to the project root."""

    return (project_root or Path.cwd()) / ARCHIVIST_TOPICS_EXAMPLE_FILENAME


def seed_archivist_topic_registry_from_example(
    config: Config,
    *,
    project_root: Path | None = None,
) -> Path | None:
    """Create the live archivist registry from the tracked example when missing."""

    registry_path = resolve_archivist_topics_path(config, project_root=project_root)
    if registry_path.exists():
        return registry_path

    example_path = resolve_archivist_topics_example_path(project_root=project_root)
    if not example_path.exists():
        return None

    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")
    return registry_path


def load_archivist_topic_registry(
    config: Config,
    *,
    project_root: Path | None = None,
    required: bool = False,
) -> ArchivistTopicRegistry:
    """Load and validate the archivist topic registry from YAML."""
    registry_path = resolve_archivist_topics_path(config, project_root=project_root)
    explicit_path = bool(str(config.get("paths.archivist_topics_file", "") or "").strip())

    if not registry_path.exists():
        if required or explicit_path:
            raise ArchivistTopicConfigError(
                f"Archivist topic registry file not found: {registry_path}"
            )
        return ArchivistTopicRegistry(source_path=registry_path)

    try:
        payload = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ArchivistTopicConfigError(
            f"Failed to parse archivist topic registry {registry_path}: {exc}"
        ) from exc

    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise ArchivistTopicConfigError(
            f"Archivist topic registry must be a YAML object: {registry_path}"
        )

    version = payload.get("version", ARCHIVIST_TOPIC_REGISTRY_VERSION)
    if version != ARCHIVIST_TOPIC_REGISTRY_VERSION:
        raise ArchivistTopicConfigError(
            f"Unsupported archivist topic registry version: {version}"
        )

    defaults = _parse_defaults(payload.get("defaults"))
    topics_payload = payload.get("topics", [])
    if topics_payload is None:
        topics_payload = []
    if not isinstance(topics_payload, list):
        raise ArchivistTopicConfigError("archivist topics must be a list")

    topics: list[ArchivistTopicDefinition] = []
    seen_ids: set[str] = set()
    seen_output_paths: set[str] = set()
    for index, raw_topic in enumerate(topics_payload, start=1):
        topic = _parse_topic(raw_topic, defaults, index=index)
        if topic.id in seen_ids:
            raise ArchivistTopicConfigError(
                f"Duplicate archivist topic id: {topic.id}"
            )
        if topic.output_path in seen_output_paths:
            raise ArchivistTopicConfigError(
                f"Duplicate archivist topic output_path: {topic.output_path}"
            )
        seen_ids.add(topic.id)
        seen_output_paths.add(topic.output_path)
        topics.append(topic)

    return ArchivistTopicRegistry(
        topics=tuple(topics),
        source_path=registry_path,
        version=version,
        defaults=defaults,
    )


def _parse_defaults(raw_defaults: Any) -> ArchivistTopicDefaults:
    if raw_defaults in (None, {}):
        return ArchivistTopicDefaults()
    if not isinstance(raw_defaults, dict):
        raise ArchivistTopicConfigError("archivist defaults must be an object")

    return ArchivistTopicDefaults(
        cadence_hours=_parse_positive_float(
            raw_defaults.get("cadence_hours", 12),
            field_name="defaults.cadence_hours",
        ),
        max_sources=_parse_positive_int(
            raw_defaults.get("max_sources", 120),
            field_name="defaults.max_sources",
            allow_none=True,
        ),
        allow_manual_force=bool(raw_defaults.get("allow_manual_force", True)),
    )


def _parse_topic(
    raw_topic: Any,
    defaults: ArchivistTopicDefaults,
    *,
    index: int,
) -> ArchivistTopicDefinition:
    if not isinstance(raw_topic, dict):
        raise ArchivistTopicConfigError(f"archivist topic #{index} must be an object")

    field_prefix = f"topics[{index}]"
    include_roots = _normalize_string_list(
        raw_topic.get("include_roots"),
        field_name=f"{field_prefix}.include_roots",
        required=True,
        normalizer=_normalize_topic_root,
    )

    return ArchivistTopicDefinition(
        id=_normalize_topic_id(
            raw_topic.get("id"),
            field_name=f"{field_prefix}.id",
        ),
        title=_normalize_required_text(
            raw_topic.get("title"),
            field_name=f"{field_prefix}.title",
        ),
        output_path=_normalize_output_path(
            raw_topic.get("output_path"),
            field_name=f"{field_prefix}.output_path",
        ),
        description=_normalize_optional_text(raw_topic.get("description")),
        include_roots=include_roots,
        exclude_roots=_normalize_string_list(
            raw_topic.get("exclude_roots"),
            field_name=f"{field_prefix}.exclude_roots",
            normalizer=_normalize_topic_root,
        ),
        source_types=_normalize_string_list(
            raw_topic.get("source_types"),
            field_name=f"{field_prefix}.source_types",
            normalizer=_normalize_generic_token,
        ),
        include_tags=_normalize_string_list(
            raw_topic.get("include_tags"),
            field_name=f"{field_prefix}.include_tags",
            normalizer=_normalize_tag,
        ),
        exclude_tags=_normalize_string_list(
            raw_topic.get("exclude_tags"),
            field_name=f"{field_prefix}.exclude_tags",
            normalizer=_normalize_tag,
        ),
        include_terms=_normalize_string_list(
            raw_topic.get("include_terms"),
            field_name=f"{field_prefix}.include_terms",
            normalizer=_normalize_term,
        ),
        exclude_terms=_normalize_string_list(
            raw_topic.get("exclude_terms"),
            field_name=f"{field_prefix}.exclude_terms",
            normalizer=_normalize_term,
        ),
        cadence_hours=_parse_positive_float(
            raw_topic.get("cadence_hours", defaults.cadence_hours),
            field_name=f"{field_prefix}.cadence_hours",
        ),
        max_sources=_parse_positive_int(
            raw_topic.get("max_sources", defaults.max_sources),
            field_name=f"{field_prefix}.max_sources",
            allow_none=True,
        ),
        allow_manual_force=bool(
            raw_topic.get("allow_manual_force", defaults.allow_manual_force)
        ),
    )


def _normalize_required_text(value: Any, *, field_name: str) -> str:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        raise ArchivistTopicConfigError(f"{field_name} is required")
    return normalized


def _normalize_optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_topic_id(value: Any, *, field_name: str) -> str:
    normalized = _normalize_required_text(value, field_name=field_name).lower()
    if not _TOPIC_ID_RE.fullmatch(normalized):
        raise ArchivistTopicConfigError(
            f"{field_name} must match {_TOPIC_ID_RE.pattern}"
        )
    return normalized


def _normalize_topic_root(value: Any) -> str:
    normalized = _normalize_safe_relative_path(value)
    if normalized.endswith(".md"):
        raise ArchivistTopicConfigError(
            "topic roots must be directory prefixes, not individual markdown files"
        )
    return normalized


def _normalize_output_path(value: Any, *, field_name: str) -> str:
    normalized = _normalize_safe_relative_path(value)
    if not normalized.endswith(".md"):
        raise ArchivistTopicConfigError(f"{field_name} must point to a markdown file")
    pure_path = PurePosixPath(normalized)
    if not pure_path.parts or pure_path.parts[0] != "pages":
        raise ArchivistTopicConfigError(
            f"{field_name} must live under wiki/pages"
        )
    return normalized


def _normalize_safe_relative_path(value: Any) -> str:
    text = _normalize_optional_text(value)
    if text is None:
        raise ArchivistTopicConfigError("relative path value is required")
    if Path(text).is_absolute():
        raise ArchivistTopicConfigError("paths must be relative")

    pure_path = PurePosixPath(text.replace("\\", "/"))
    if pure_path.is_absolute():
        raise ArchivistTopicConfigError("paths must be relative")
    parts = tuple(part for part in pure_path.parts if part not in ("", "."))
    if not parts:
        raise ArchivistTopicConfigError("paths cannot be empty")
    if any(part == ".." for part in parts):
        raise ArchivistTopicConfigError("paths cannot escape their root")
    return PurePosixPath(*parts).as_posix()


def _normalize_tag(value: Any) -> str:
    text = _normalize_optional_text(value)
    if text is None:
        raise ArchivistTopicConfigError("tags cannot be empty")
    normalized = text.strip().lower().lstrip("#")
    normalized = _WHITESPACE_RE.sub("_", normalized)
    normalized = normalized.replace("-", "_")
    normalized = normalized.strip("_")
    if not normalized:
        raise ArchivistTopicConfigError("tags cannot be empty")
    return normalized


def _normalize_term(value: Any) -> str:
    text = _normalize_optional_text(value)
    if text is None:
        raise ArchivistTopicConfigError("terms cannot be empty")
    normalized = _WHITESPACE_RE.sub(" ", text.strip().lower())
    if not normalized:
        raise ArchivistTopicConfigError("terms cannot be empty")
    return normalized


def _normalize_generic_token(value: Any) -> str:
    text = _normalize_optional_text(value)
    if text is None:
        raise ArchivistTopicConfigError("tokens cannot be empty")
    normalized = _WHITESPACE_RE.sub("_", text.strip().lower())
    normalized = normalized.replace("-", "_")
    normalized = normalized.strip("_")
    if not normalized:
        raise ArchivistTopicConfigError("tokens cannot be empty")
    return normalized


def _normalize_string_list(
    value: Any,
    *,
    field_name: str,
    required: bool = False,
    normalizer,
) -> tuple[str, ...]:
    if value in (None, []):
        if required:
            raise ArchivistTopicConfigError(f"{field_name} must be a non-empty list")
        return ()
    if not isinstance(value, list):
        raise ArchivistTopicConfigError(f"{field_name} must be a list")

    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        normalized_item = normalizer(item)
        if normalized_item in seen:
            continue
        seen.add(normalized_item)
        normalized.append(normalized_item)

    if required and not normalized:
        raise ArchivistTopicConfigError(f"{field_name} must be a non-empty list")
    return tuple(normalized)


def _parse_positive_float(value: Any, *, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ArchivistTopicConfigError(f"{field_name} must be a number") from exc
    if parsed <= 0:
        raise ArchivistTopicConfigError(f"{field_name} must be positive")
    return parsed


def _parse_positive_int(
    value: Any,
    *,
    field_name: str,
    allow_none: bool = False,
) -> int | None:
    if value is None:
        if allow_none:
            return None
        raise ArchivistTopicConfigError(f"{field_name} is required")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ArchivistTopicConfigError(f"{field_name} must be an integer") from exc
    if parsed <= 0:
        raise ArchivistTopicConfigError(f"{field_name} must be positive")
    return parsed
