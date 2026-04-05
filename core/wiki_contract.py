"""
Wiki contract and compiled knowledge-base conventions for Thoth.

This module defines the on-disk wiki layout, page schema, and validation rules
for compiled knowledge pages. It does not create directories or write files.
That belongs in the wiki scaffold/maintenance layer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Tuple

from .config import Config
from .path_layout import build_path_layout

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


@dataclass(frozen=True)
class WikiPageSpec:
    """Declarative schema for a compiled wiki page."""

    title: str
    slug: str
    kind: str = "topic"
    record_type: str = "wiki_page"
    summary: str = ""
    aliases: Tuple[str, ...] = field(default_factory=tuple)
    source_paths: Tuple[str, ...] = field(default_factory=tuple)
    related_slugs: Tuple[str, ...] = field(default_factory=tuple)
    language: str = "en"
    translated_from: str | None = None
    query: str | None = None
    query_terms: Tuple[str, ...] = field(default_factory=tuple)
    curated: bool | None = None
    result_count: int | None = None
    created_at: str | None = None
    updated_at: str | None = None

    def frontmatter(self) -> Dict[str, Any]:
        """Render a frontmatter dictionary for the compiled wiki page."""
        data = {
            "thoth_type": self.record_type,
            "title": self.title,
            "slug": self.slug,
            "kind": self.kind,
            "summary": self.summary,
            "aliases": list(self.aliases),
            "source_paths": list(self.source_paths),
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
        self.validate_slug(spec.slug)
        if spec.language and not spec.language.strip():
            raise ValueError("Wiki page language cannot be empty")
        for source_path in spec.source_paths:
            if not source_path or not source_path.strip():
                raise ValueError("Wiki page source_paths cannot contain empty entries")
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
