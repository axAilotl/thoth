"""Shared markdown I/O helpers for the wiki layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import os
import yaml


@dataclass(frozen=True)
class WikiDocument:
    """Parsed wiki markdown document."""

    path: Path
    frontmatter: dict[str, Any]
    body: str


def atomic_write_text(path: Path, content: str) -> None:
    """Write text atomically to avoid partially-written wiki pages."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    with open(temp_path, "w", encoding="utf-8") as handle:
        handle.write(content)
        if not content.endswith("\n"):
            handle.write("\n")
    os.replace(temp_path, path)


def render_frontmatter(data: dict[str, Any]) -> str:
    """Render frontmatter in a stable markdown-compatible format."""
    return "---\n" + yaml.safe_dump(
        data,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    ) + "---\n"


def read_frontmatter(path: Path) -> dict[str, Any]:
    """Read frontmatter from a markdown document, or return an empty dict."""
    document = read_document(path)
    return document.frontmatter


def read_document(path: Path) -> WikiDocument:
    """Read a wiki markdown document and split frontmatter from body."""
    content = path.read_text(encoding="utf-8")
    if not content.startswith("---\n"):
        return WikiDocument(path=path, frontmatter={}, body=content)

    end_marker = content.find("\n---\n", 4)
    if end_marker == -1:
        return WikiDocument(path=path, frontmatter={}, body=content)

    payload = yaml.safe_load(content[4:end_marker]) or {}
    frontmatter = payload if isinstance(payload, dict) else {}
    body = content[end_marker + len("\n---\n") :]
    return WikiDocument(path=path, frontmatter=frontmatter, body=body)


def truncate_summary(value: str, *, limit: int = 320) -> str:
    """Condense long text into a compact single-line summary."""
    normalized = " ".join((value or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "..."
