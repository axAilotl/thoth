"""
Strict parser for Web Clipper markdown notes.
"""

from __future__ import annotations

from datetime import date, datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml


class WebClipperParseError(ValueError):
    """Base error for strict Web Clipper parsing failures."""


class WebClipperFrontmatterError(WebClipperParseError):
    """Raised when frontmatter is missing or malformed."""


class WebClipperMarkdownError(WebClipperParseError):
    """Raised when the markdown note cannot be parsed."""


@dataclass(frozen=True)
class WebClipperParsedNote:
    """Parsed Web Clipper markdown note."""

    source_path: Path
    raw_content: str
    frontmatter: Dict[str, Any]
    body: str
    title: str
    source_url: Optional[str] = None
    source_language: Optional[str] = None


def _extract_title(frontmatter: Mapping[str, Any], body: str, source_path: Path) -> str:
    for key in ("title", "name", "display_title"):
        value = frontmatter.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            candidate = stripped[2:].strip()
            if candidate:
                return candidate
        if stripped:
            break

    return source_path.stem


def _extract_source_url(frontmatter: Mapping[str, Any]) -> Optional[str]:
    for key in ("url", "source_url", "source", "link", "canonical_url"):
        value = frontmatter.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_source_language(frontmatter: Mapping[str, Any]) -> Optional[str]:
    for key in ("lang", "language", "locale"):
        value = frontmatter.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _normalize_yaml_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize_yaml_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_yaml_value(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def parse_web_clipper_markdown(
    source_text: str,
    *,
    source_path: Path,
) -> WebClipperParsedNote:
    """Parse a Web Clipper markdown note with strict frontmatter handling."""
    if not source_text or not source_text.strip():
        raise WebClipperMarkdownError(f"Empty Web Clipper note: {source_path}")

    if not source_text.startswith("---"):
        raise WebClipperFrontmatterError(
            f"Missing frontmatter delimiter at start of note: {source_path}"
        )

    lines = source_text.splitlines(keepends=True)
    if not lines or lines[0].strip() != "---":
        raise WebClipperFrontmatterError(
            f"Malformed frontmatter opening delimiter in note: {source_path}"
        )

    closing_index = None
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            closing_index = index
            break

    if closing_index is None:
        raise WebClipperFrontmatterError(
            f"Missing closing frontmatter delimiter in note: {source_path}"
        )

    frontmatter_text = "".join(lines[1:closing_index])
    body_lines = lines[closing_index + 1 :]
    if body_lines and body_lines[0].strip() == "":
        body_lines = body_lines[1:]
    body = "".join(body_lines)

    try:
        frontmatter = yaml.safe_load(frontmatter_text) if frontmatter_text.strip() else {}
    except yaml.YAMLError as exc:
        raise WebClipperFrontmatterError(
            f"Invalid Web Clipper frontmatter in {source_path}: {exc}"
        ) from exc

    if frontmatter is None:
        frontmatter = {}
    if not isinstance(frontmatter, dict):
        raise WebClipperFrontmatterError(
            f"Web Clipper frontmatter must be a mapping in {source_path}"
        )
    frontmatter = _normalize_yaml_value(frontmatter)

    title = _extract_title(frontmatter, body, source_path)
    return WebClipperParsedNote(
        source_path=source_path,
        raw_content=source_text,
        frontmatter=dict(frontmatter),
        body=body,
        title=title,
        source_url=_extract_source_url(frontmatter),
        source_language=_extract_source_language(frontmatter),
    )
