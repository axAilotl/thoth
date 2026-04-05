"""SQLite FTS-backed archivist retrieval."""

from __future__ import annotations

from .models import ArchivistCorpusDocument, ResolvedArchivistRoot
from .query import build_full_text_match_expression


def retrieve_full_text_documents(
    *,
    db,
    query,
    include_roots: tuple[ResolvedArchivistRoot, ...],
    source_types: tuple[str, ...],
    limit: int,
) -> list[tuple[ArchivistCorpusDocument, float]]:
    """Return FTS-ranked documents for an archivist retrieval query."""

    expression = build_full_text_match_expression(query)
    if not expression:
        return []

    return db.search_archivist_corpus_full_text(
        query=expression,
        root_filters=tuple((root.scope, root.relative_prefix) for root in include_roots),
        source_types=source_types,
        limit=limit,
    )
