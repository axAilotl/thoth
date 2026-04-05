"""Query building helpers for archivist retrieval."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from .models import ArchivistRetrievalQuery

if TYPE_CHECKING:
    from ..archivist_topics import ArchivistTopicDefinition

_TOKEN_RE = re.compile(r"[a-z0-9_]{2,}")


def build_archivist_retrieval_query(
    topic: ArchivistTopicDefinition,
) -> ArchivistRetrievalQuery:
    """Build a normalized retrieval query from a topic definition."""

    text_parts = [topic.title]
    if topic.description:
        text_parts.append(topic.description)
    if topic.retrieval.query_text:
        text_parts.append(topic.retrieval.query_text)
    if topic.retrieval.term_mode != "off":
        text_parts.extend(topic.include_terms)
    if topic.retrieval.tag_mode != "off":
        text_parts.extend(tag.replace("_", " ") for tag in topic.include_tags)

    normalized_text = " ".join(part.strip() for part in text_parts if str(part).strip()).strip()
    return ArchivistRetrievalQuery(
        topic_id=topic.id,
        text=normalized_text,
        include_tags=topic.include_tags,
        exclude_tags=topic.exclude_tags,
        include_terms=topic.include_terms,
        exclude_terms=topic.exclude_terms,
        source_types=topic.source_types,
    )


def build_full_text_match_expression(query: ArchivistRetrievalQuery) -> str:
    """Convert a normalized archivist query into a safe FTS5 MATCH string."""

    clauses: list[str] = []
    seen: set[str] = set()

    for phrase in query.include_terms:
        cleaned = _clean_phrase(phrase)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        clauses.append(f"\"{cleaned}\"")

    for tag in query.include_tags:
        cleaned = _clean_phrase(tag.replace("_", " "))
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        clauses.append(f"\"{cleaned}\"")

    for token in _TOKEN_RE.findall(query.text.lower()):
        if token in seen:
            continue
        seen.add(token)
        clauses.append(token)

    return " OR ".join(clauses)


def _clean_phrase(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace('"', " ").split())
