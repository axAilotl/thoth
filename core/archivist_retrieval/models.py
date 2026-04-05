"""Shared retrieval models for archivist indexing and ranking."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

RETRIEVAL_MODES = {"literal", "full_text", "semantic", "hybrid"}
FILTER_MODES = {"required", "query", "off"}


@dataclass(frozen=True)
class ArchivistRetrievalPolicy:
    """Topic-level retrieval policy for large corpus selection."""

    mode: str = "full_text"
    tag_mode: str = "required"
    term_mode: str = "required"
    query_text: str | None = None
    full_text_limit: int = 150
    semantic_limit: int = 80
    rerank_limit: int = 120
    max_new_embeddings_per_run: int = 24
    semantic_weight: float = 0.55
    full_text_weight: float = 0.35
    recency_weight: float = 0.10
    source_type_weights: tuple[tuple[str, float], ...] = ()

    def weight_for_source_type(self, source_type: str) -> float:
        for candidate_type, weight in self.source_type_weights:
            if candidate_type == source_type:
                return weight
        return 1.0

    def requires_full_text(self) -> bool:
        return self.mode in {"full_text", "hybrid"}

    def requires_semantic(self) -> bool:
        return self.mode in {"semantic", "hybrid"}


@dataclass(frozen=True)
class ResolvedArchivistRoot:
    """Concrete root resolved for an archivist topic gate."""

    spec: str
    scope: str
    relative_prefix: str
    path: Path


@dataclass(frozen=True)
class ArchivistCorpusDocument:
    """Stored archivist corpus document used by retrieval backends."""

    candidate_key: str
    path: Path
    scope: str
    scope_relative_path: str
    source_type: str
    file_type: str
    title: str
    tags: tuple[str, ...]
    content_text: str
    source_hash: str
    size_bytes: int
    updated_at: str
    source_id: str | None = None

    def search_corpus(self) -> str:
        parts = [
            self.title,
            self.content_text,
            " ".join(self.tags),
            self.scope_relative_path.replace("/", " "),
            self.source_type,
            self.file_type,
            self.source_id or "",
        ]
        return " ".join(part for part in parts if part).lower()


@dataclass(frozen=True)
class ArchivistCandidate(ArchivistCorpusDocument):
    """Single archivist source candidate after code-side gating and retrieval."""

    root_spec: str = ""
    retrieval_score: float = 0.0
    retrieval_sources: tuple[str, ...] = field(default_factory=tuple)
    full_text_score: float | None = None
    semantic_score: float | None = None


@dataclass(frozen=True)
class ArchivistSelectionResult:
    """Deterministic candidate selection result for a single topic."""

    topic_id: str
    candidates: tuple[ArchivistCandidate, ...]
    scanned_roots: tuple[str, ...]
    missing_roots: tuple[str, ...]
    indexed_count: int = 0
    retrieval_mode: str = "literal"


@dataclass(frozen=True)
class ArchivistRetrievalQuery:
    """Normalized retrieval query built from a topic definition."""

    topic_id: str
    text: str
    include_tags: tuple[str, ...] = ()
    exclude_tags: tuple[str, ...] = ()
    include_terms: tuple[str, ...] = ()
    exclude_terms: tuple[str, ...] = ()
    source_types: tuple[str, ...] = ()
