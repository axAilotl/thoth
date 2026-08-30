"""Render transcript derivative content to stable markdown and JSON."""

from __future__ import annotations

import json
from typing import Any

from .identity import ProcessorIdentity
from .storage import TranscriptStorageError


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _split_frontmatter(content: str, required: bool = True) -> tuple[str, str]:
    """Split content into frontmatter and body, validating delimiters."""
    if not content.startswith("---\n"):
        raise TranscriptStorageError("missing opening frontmatter delimiter")
    remainder = content[4:]
    close_index = remainder.find("\n---\n")
    if close_index == -1:
        raise TranscriptStorageError("missing closing frontmatter delimiter")
    frontmatter = remainder[:close_index]
    body = remainder[close_index + 5 :]
    if required and not frontmatter.strip():
        raise TranscriptStorageError("empty frontmatter")
    return frontmatter, body


def render_transcript_markdown(
    *,
    title: str,
    normalized_text: str,
    source_path: str | None,
    version: str,
    cache_key: str,
) -> str:
    lines = [
        "---",
        f"title: {json.dumps(title, ensure_ascii=False)}",
        "source_type: transcript",
        f"version: {version}",
        f"cache_key: {cache_key}",
    ]
    if source_path:
        lines.append(f"source_path: {json.dumps(source_path, ensure_ascii=False)}")
    lines.extend(
        [
            "---",
            "",
            f"# {title}",
            "",
            "## Normalized Transcript",
            "",
            normalized_text,
            "",
        ]
    )
    return "\n".join(lines)


def render_summary_markdown(
    *,
    title: str,
    summary: str,
    tags: list[str],
    source_path: str | None,
    version: str,
    cache_key: str,
) -> str:
    lines = [
        "---",
        f"title: {json.dumps(f'Summary: {title}', ensure_ascii=False)}",
        "source_type: transcript",
        f"version: {version}",
        f"cache_key: {cache_key}",
    ]
    if source_path:
        lines.append(f"source_path: {json.dumps(source_path, ensure_ascii=False)}")
    if tags:
        lines.append(f"tags: {json.dumps(tags, ensure_ascii=False)}")
    lines.extend(
        [
            "---",
            "",
            f"# Summary: {title}",
            "",
            summary,
            "",
        ]
    )
    return "\n".join(lines)


def render_classification_json(
    *,
    title: str,
    tags: list[str],
    summary: str,
    normalized_length: int,
    source_hash: str,
    cache_key: str,
    version: str,
    processor_identity: ProcessorIdentity,
) -> str:
    return _canonical_json(
        {
            "title": title,
            "tags": tags,
            "summary": summary,
            "normalized_length": normalized_length,
            "source_hash": source_hash,
            "cache_key": cache_key,
            "version": version,
            "processor_identity": processor_identity.to_dict(),
        }
    )


def extract_normalized_transcript_text(content: str) -> str:
    """Return the normalized transcript body from a rendered Markdown file.

    Validates the exact renderer structure: opening/closing frontmatter,
    title heading, and normalized transcript marker.
    """
    _frontmatter, body = _split_frontmatter(content)
    body = body.lstrip("\n")
    # Body should start with "# <title>\n\n## Normalized Transcript\n\n<body>\n".
    if not body.startswith("# "):
        raise TranscriptStorageError("transcript markdown missing title heading")
    marker = "\n\n## Normalized Transcript\n\n"
    marker_index = body.find(marker)
    if marker_index == -1:
        raise TranscriptStorageError("transcript markdown missing normalized marker")
    text = body[marker_index + len(marker) :]
    # The renderer appends a trailing newline.
    return text.rstrip("\n")


def extract_summary_text(content: str) -> str:
    """Return the plain summary body from a rendered summary Markdown file.

    Preserves multi-paragraph summaries; only the renderer-generated heading
    and trailing newline are stripped.
    """
    _frontmatter, body = _split_frontmatter(content)
    body = body.lstrip("\n")
    if not body.startswith("# Summary:"):
        raise TranscriptStorageError("summary markdown missing summary heading")
    # Body is "# Summary: <title>\n\n<summary>\n". Split off the heading.
    parts = body.split("\n\n", 1)
    if len(parts) < 2:
        raise TranscriptStorageError("summary markdown missing body")
    return parts[1].rstrip("\n")


def classification_tags_from_content(content: str) -> list[str]:
    """Extract the tag list from a classification derivative's stable content."""
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        raise TranscriptStorageError(
            f"classification content is not valid JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise TranscriptStorageError(
            f"classification content must be a JSON object, got {type(parsed).__name__}"
        )
    tags = parsed.get("tags")
    if not isinstance(tags, list):
        raise TranscriptStorageError(
            "classification content missing 'tags' list"
        )
    for tag in tags:
        if not isinstance(tag, str) or not tag.strip():
            raise TranscriptStorageError(
                "classification tags must be non-blank strings"
            )
    return [tag for tag in tags if tag]
