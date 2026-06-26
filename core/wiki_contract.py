"""
Wiki contract and compiled knowledge-base conventions for Thoth.

This module defines the on-disk wiki layout, page schema, and validation rules
for compiled knowledge pages. It does not create directories or write files.
That belongs in the wiki scaffold/maintenance layer.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Tuple

from .config import Config
from .path_layout import build_path_layout

OKF_VERSION = "0.1"
OKF_TYPE_BY_WIKI_KIND = {
    "topic": "Topic",
    "entity": "Entity",
    "concept": "Concept",
}
OKF_TYPE_BY_RECORD_TYPE = {
    "wiki_query": "Reference",
}
WIKI_INDEX_FILENAME = "index.md"
WIKI_LOG_FILENAME = "log.md"
WIKI_PAGES_DIRNAME = "pages"
WIKI_SUPPORTED_PAGE_KINDS = ("topic", "entity", "concept")
WIKI_SUPPORTED_RECORD_TYPES = ("wiki_page", "wiki_query")
WIKI_RESERVED_FILENAMES = (WIKI_INDEX_FILENAME, WIKI_LOG_FILENAME)
WIKI_SLUG_MAX_LENGTH = 80

_WIKI_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_WIKI_LEGACY_TWEET_SLUG_RE = re.compile(r"^tweet-[0-9]+$")


def normalize_wiki_slug(raw_value: str, max_length: int = WIKI_SLUG_MAX_LENGTH) -> str:
    """Normalize a free-form title or slug into a canonical wiki slug."""
    value = raw_value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")

    if not value:
        raise ValueError("Wiki slug cannot be empty after normalization")

    if len(value) > max_length:
        value = value[:max_length].rstrip("-")

    if not value:
        raise ValueError("Wiki slug cannot be empty after truncation")

    return value


def is_legacy_tweet_slug(raw_value: str) -> bool:
    """Return True when a page slug matches the legacy compiled tweet-page pattern."""
    value = str(raw_value or "").strip().lower()
    return bool(_WIKI_LEGACY_TWEET_SLUG_RE.fullmatch(value))


def okf_type_for_wiki_page(kind: str, record_type: str = "wiki_page") -> str:
    """Return the OKF concept type for a Thoth wiki page."""
    if record_type in OKF_TYPE_BY_RECORD_TYPE:
        return OKF_TYPE_BY_RECORD_TYPE[record_type]
    return OKF_TYPE_BY_WIKI_KIND.get(kind, "Reference")


def _stable_unique_strings(values: Tuple[str, ...]) -> Tuple[str, ...]:
    cleaned = (str(value).strip() for value in values)
    return tuple(sorted({value for value in cleaned if value}))


def _stable_metadata_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _stable_metadata_value(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple, set)):
        normalized = [_stable_metadata_value(item) for item in value]
        return sorted(normalized, key=_metadata_sort_key)
    return value


def _metadata_sort_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)


def _stable_security_findings(values: Tuple[Any, ...]) -> Tuple[Any, ...]:
    normalized = [
        _stable_metadata_value(value)
        for value in values
        if value not in (None, {}, [], ())
    ]
    deduped = {_metadata_sort_key(value): value for value in normalized}
    return tuple(deduped[key] for key in sorted(deduped))


def _stable_influence_sources(values: Tuple[Any, ...]) -> Tuple[Dict[str, Any], ...]:
    records: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, Mapping):
            continue
        record = {
            str(key): _stable_metadata_value(item)
            for key, item in value.items()
            if item not in (None, "", [], {})
        }
        if not record:
            continue
        fingerprint = _metadata_sort_key(record)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        records.append(record)
    return tuple(records)


@dataclass(frozen=True)
class WikiPageSpec:
    """Declarative schema for a compiled wiki page."""

    title: str
    slug: str
    kind: str = "topic"
    record_type: str = "wiki_page"
    okf_type: str | None = None
    summary: str = ""
    aliases: Tuple[str, ...] = field(default_factory=tuple)
    source_paths: Tuple[str, ...] = field(default_factory=tuple)
    influence_sources: Tuple[Any, ...] = field(default_factory=tuple)
    related_slugs: Tuple[str, ...] = field(default_factory=tuple)
    language: str = "en"
    translated_from: str | None = None
    query: str | None = None
    query_terms: Tuple[str, ...] = field(default_factory=tuple)
    curated: bool | None = None
    result_count: int | None = None
    created_at: str | None = None
    updated_at: str | None = None
    resource: str | None = None
    artifact_id: str | None = None
    source_type: str | None = None
    event_ids: Tuple[str, ...] = field(default_factory=tuple)
    source_ids: Tuple[str, ...] = field(default_factory=tuple)
    session_ids: Tuple[str, ...] = field(default_factory=tuple)
    capture_page_type: str | None = None
    capture_page_key: str | None = None
    capture_event_count: int | None = None
    capture_audit: Mapping[str, Any] | None = None
    security_findings: Tuple[Any, ...] = field(default_factory=tuple)
    security_policy: Mapping[str, Any] | None = None
    input_hash: str | None = None
    input_manifest: Tuple[Any, ...] = field(default_factory=tuple)
    change_provenance: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "aliases", _stable_unique_strings(self.aliases))
        object.__setattr__(self, "source_paths", _stable_unique_strings(self.source_paths))
        object.__setattr__(
            self,
            "influence_sources",
            _stable_influence_sources(self.influence_sources),
        )
        object.__setattr__(self, "related_slugs", _stable_unique_strings(self.related_slugs))
        object.__setattr__(self, "query_terms", _stable_unique_strings(self.query_terms))
        object.__setattr__(self, "event_ids", _stable_unique_strings(self.event_ids))
        object.__setattr__(self, "source_ids", _stable_unique_strings(self.source_ids))
        object.__setattr__(self, "session_ids", _stable_unique_strings(self.session_ids))
        object.__setattr__(
            self,
            "security_findings",
            _stable_security_findings(self.security_findings),
        )
        object.__setattr__(
            self,
            "input_manifest",
            _stable_security_findings(self.input_manifest),
        )

    def frontmatter(self) -> Dict[str, Any]:
        """Render a frontmatter dictionary for the compiled wiki page."""
        okf_type = self.okf_type or okf_type_for_wiki_page(self.kind, self.record_type)
        data = {
            "type": okf_type,
            "id": self.slug,
            "thoth_type": self.record_type,
            "thoth_id": self.slug,
            "title": self.title,
            "description": self.summary,
            "resource": self.resource,
            "timestamp": self.updated_at,
            "thoth_okf_version": OKF_VERSION,
            "thoth_slug": self.slug,
            "thoth_kind": self.kind,
            "thoth_summary": self.summary,
            "thoth_aliases": list(self.aliases),
            "thoth_source_paths": list(self.source_paths),
            "thoth_influence_sources": list(self.influence_sources),
            "thoth_related_slugs": list(self.related_slugs),
            "thoth_language": self.language,
            "thoth_translated_from": self.translated_from,
            "thoth_query": self.query,
            "thoth_query_terms": list(self.query_terms),
            "thoth_curated": self.curated,
            "thoth_result_count": self.result_count,
            "thoth_created_at": self.created_at,
            "thoth_updated_at": self.updated_at,
            "thoth_artifact_id": self.artifact_id,
            "thoth_source_type": self.source_type,
            "thoth_event_ids": list(self.event_ids) or None,
            "thoth_source_ids": list(self.source_ids) or None,
            "thoth_session_ids": list(self.session_ids) or None,
            "thoth_capture_page_type": self.capture_page_type,
            "thoth_capture_page_key": self.capture_page_key,
            "thoth_capture_event_count": self.capture_event_count,
            "thoth_capture_audit": _stable_metadata_value(self.capture_audit)
            if self.capture_audit
            else None,
            "thoth_security_findings": list(self.security_findings) or None,
            "thoth_security_policy": _stable_metadata_value(self.security_policy)
            if self.security_policy
            else None,
            "thoth_input_hash": self.input_hash,
            "thoth_input_manifest": list(self.input_manifest) or None,
            "thoth_change_provenance": _stable_metadata_value(self.change_provenance)
            if self.change_provenance
            else None,
            # Legacy aliases retained so existing local readers and hand-authored
            # pages keep working while new metadata has namespaced equivalents.
            "slug": self.slug,
            "kind": self.kind,
            "summary": self.summary,
            "aliases": list(self.aliases),
            "source_paths": list(self.source_paths),
            "influence_sources": list(self.influence_sources),
            "related_slugs": list(self.related_slugs),
            "language": self.language,
            "translated_from": self.translated_from,
            "query": self.query,
            "query_terms": list(self.query_terms),
            "curated": self.curated,
            "result_count": self.result_count,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        return {key: value for key, value in data.items() if value is not None}


@dataclass(frozen=True)
class WikiContract:
    """Canonical wiki layout and validation rules."""

    root: Path
    index_filename: str = WIKI_INDEX_FILENAME
    log_filename: str = WIKI_LOG_FILENAME
    pages_dirname: str = WIKI_PAGES_DIRNAME
    supported_kinds: Tuple[str, ...] = WIKI_SUPPORTED_PAGE_KINDS
    supported_record_types: Tuple[str, ...] = WIKI_SUPPORTED_RECORD_TYPES
    reserved_filenames: Tuple[str, ...] = WIKI_RESERVED_FILENAMES
    slug_max_length: int = WIKI_SLUG_MAX_LENGTH

    @property
    def index_path(self) -> Path:
        return self.root / self.index_filename

    @property
    def log_path(self) -> Path:
        return self.root / self.log_filename

    @property
    def pages_dir(self) -> Path:
        return self.root / self.pages_dirname

    def is_reserved_filename(self, filename: str) -> bool:
        return filename in self.reserved_filenames

    def validate_slug(self, slug: str) -> None:
        """Reject malformed or reserved wiki slugs."""
        if not slug or not slug.strip():
            raise ValueError("Wiki slug cannot be empty")
        if slug != normalize_wiki_slug(slug, self.slug_max_length):
            raise ValueError(f"Wiki slug must be normalized: {slug}")
        if slug in {name.removesuffix(".md") for name in self.reserved_filenames}:
            raise ValueError(f"Wiki slug is reserved: {slug}")
        if not _WIKI_SLUG_RE.fullmatch(slug):
            raise ValueError(f"Wiki slug contains invalid characters: {slug}")

    def validate_page_spec(self, spec: WikiPageSpec) -> None:
        """Validate a page specification before it is written to disk."""
        if not spec.title or not spec.title.strip():
            raise ValueError("Wiki page title cannot be empty")
        if spec.record_type not in self.supported_record_types:
            raise ValueError(f"Unsupported wiki record type: {spec.record_type}")
        if spec.kind not in self.supported_kinds:
            raise ValueError(f"Unsupported wiki page kind: {spec.kind}")
        okf_type = spec.okf_type or okf_type_for_wiki_page(spec.kind, spec.record_type)
        if not okf_type or not okf_type.strip():
            raise ValueError("Wiki page OKF type cannot be empty")
        self.validate_slug(spec.slug)
        if spec.language and not spec.language.strip():
            raise ValueError("Wiki page language cannot be empty")
        for source_path in spec.source_paths:
            if not source_path or not source_path.strip():
                raise ValueError("Wiki page source_paths cannot contain empty entries")
        for influence in spec.influence_sources:
            if not isinstance(influence, Mapping):
                raise ValueError("Wiki page influence_sources entries must be objects")
            forbidden = {
                key
                for key in influence
                if any(marker in str(key).lower() for marker in ("content", "excerpt", "secret"))
            }
            if forbidden:
                raise ValueError("Wiki page influence_sources cannot include source content")
        if spec.input_hash and not spec.input_hash.strip():
            raise ValueError("Wiki page input_hash cannot be empty")
        for input_record in spec.input_manifest:
            if not isinstance(input_record, Mapping):
                raise ValueError("Wiki page input_manifest entries must be objects")
            forbidden = {
                key
                for key in input_record
                if any(marker in str(key).lower() for marker in ("content", "excerpt", "secret"))
            }
            if forbidden:
                raise ValueError("Wiki page input_manifest cannot include source content")
        for related_slug in spec.related_slugs:
            self.validate_slug(related_slug)

    def page_path(self, slug: str) -> Path:
        """Return the canonical markdown path for a compiled wiki page."""
        self.validate_slug(slug)
        return self.pages_dir / f"{slug}.md"

    def page_path_for(self, spec: WikiPageSpec) -> Path:
        self.validate_page_spec(spec)
        return self.page_path(spec.slug)

    def frontmatter_for(self, spec: WikiPageSpec) -> Dict[str, Any]:
        self.validate_page_spec(spec)
        return spec.frontmatter()


def build_wiki_contract(config: Config, *, project_root: Path | None = None) -> WikiContract:
    """Build the wiki contract from runtime configuration."""
    layout = build_path_layout(config, project_root=project_root)
    return WikiContract(root=layout.wiki_root)
