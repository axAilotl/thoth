"""Shared models for staged archivist compilation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..archivist_selection import ArchivistCandidate

DEFAULT_SOURCE_TYPE_LIMITS: dict[str, int] = {
    "paper": 14,
    "note": 14,
    "web_clipper": 14,
    "thread": 18,
    "tweet": 18,
    "repository": 8,
    "transcript": 8,
    "translation": 10,
    "journal": 10,
    "pdf": 8,
}

DEFAULT_SOURCE_TYPE_ORDER: dict[str, int] = {
    "paper": 10,
    "note": 20,
    "web_clipper": 25,
    "thread": 30,
    "tweet": 40,
    "repository": 50,
    "transcript": 60,
    "translation": 70,
    "journal": 80,
    "pdf": 90,
}

SOURCE_TYPE_LABELS: dict[str, str] = {
    "paper": "Papers",
    "note": "Notes",
    "web_clipper": "Web Clips",
    "thread": "Threads",
    "tweet": "Tweets",
    "repository": "Repositories",
    "transcript": "Transcripts",
    "translation": "Translations",
    "journal": "Journals",
    "pdf": "PDFs",
}


@dataclass(frozen=True)
class ArchivistTopicSourceUsage:
    """Durable topic/source usage row used to control archivist spend."""

    topic_id: str
    candidate_key: str
    source_type: str
    source_hash: str
    retrieval_score: float
    last_polled_at: str
    last_selected_at: str | None = None
    last_read_at: str | None = None
    last_source_used_at: str | None = None
    last_final_used_at: str | None = None
    selected_count: int = 0
    read_count: int = 0
    source_used_count: int = 0
    final_used_count: int = 0
    last_decision: str = "polled_only"
    last_reason: str | None = None
    updated_at: str | None = None

    def is_unchanged(self, candidate: ArchivistCandidate) -> bool:
        return self.source_hash == candidate.source_hash

    def was_never_used(self) -> bool:
        return self.read_count > 0 and self.source_used_count == 0 and self.final_used_count == 0

    def should_skip_unchanged_candidate(self, candidate: ArchivistCandidate) -> bool:
        return self.is_unchanged(candidate) and self.was_never_used()

    def should_carry_forward(self, candidate: ArchivistCandidate) -> bool:
        return self.is_unchanged(candidate) and (
            self.source_used_count > 0 or self.final_used_count > 0
        )


@dataclass(frozen=True)
class ArchivistStagePlan:
    """Selection plan for a single source-type brief."""

    source_type: str
    source_label: str
    selected_candidates: tuple[ArchivistCandidate, ...]
    new_candidate_keys: tuple[str, ...]
    carryover_candidate_keys: tuple[str, ...]
    skipped_unchanged_candidate_keys: tuple[str, ...]
    skipped_limited_candidate_keys: tuple[str, ...]


@dataclass(frozen=True)
class ArchivistStagePlanningResult:
    """Aggregate selection plan across all source types for a topic run."""

    stage_plans: tuple[ArchivistStagePlan, ...]
    any_source_delta: bool
    selected_candidate_keys: tuple[str, ...]
    skipped_unchanged_candidate_keys: tuple[str, ...]
    skipped_limited_candidate_keys: tuple[str, ...]


@dataclass(frozen=True)
class ArchivistSourceBrief:
    """Single source-type intermediate brief produced by the archivist."""

    source_type: str
    source_label: str
    body: str
    selected_candidate_keys: tuple[str, ...]
    promoted_candidate_keys: tuple[str, ...]
    skipped_unchanged_candidate_keys: tuple[str, ...]
    skipped_limited_candidate_keys: tuple[str, ...]


def source_type_label(source_type: str) -> str:
    return SOURCE_TYPE_LABELS.get(source_type, source_type.replace("_", " ").title())


def source_type_limit(source_type: str, configured_limits: tuple[tuple[str, int], ...]) -> int:
    for candidate_type, limit in configured_limits:
        if candidate_type == source_type:
            return int(limit)
    return DEFAULT_SOURCE_TYPE_LIMITS.get(source_type, 10)


def source_type_sort_key(source_type: str) -> tuple[int, str]:
    return (DEFAULT_SOURCE_TYPE_ORDER.get(source_type, 999), source_type)
