"""Stage planning and citation extraction for archivist compilation."""

from __future__ import annotations

from collections import defaultdict
import re
from typing import Iterable, Mapping

from ..archivist_selection import ArchivistCandidate
from ..archivist_topics import ArchivistTopicDefinition
from .models import (
    ArchivistStagePlan,
    ArchivistStagePlanningResult,
    ArchivistTopicSourceUsage,
    source_type_label,
    source_type_limit,
    source_type_sort_key,
)

_SOURCE_CITATION_RE = re.compile(r"\[S(\d+)\]")


def build_stage_planning_result(
    topic: ArchivistTopicDefinition,
    candidates: tuple[ArchivistCandidate, ...] | list[ArchivistCandidate],
    *,
    usage_by_key: Mapping[str, ArchivistTopicSourceUsage],
    force: bool,
) -> ArchivistStagePlanningResult:
    """Select staged source-type packets with durable usage-aware spend guards."""

    ordered_candidates = tuple(
        sorted(
            candidates,
            key=lambda candidate: (
                source_type_sort_key(candidate.source_type),
                candidate.retrieval_score,
                candidate.updated_at,
                candidate.candidate_key,
            ),
            reverse=False,
        )
    )
    grouped: dict[str, list[ArchivistCandidate]] = defaultdict(list)
    any_source_delta = False
    for candidate in ordered_candidates:
        grouped[candidate.source_type].append(candidate)
        usage = usage_by_key.get(candidate.candidate_key)
        if force or usage is None or not usage.is_unchanged(candidate):
            any_source_delta = True

    stage_plans: list[ArchivistStagePlan] = []
    selected_keys: list[str] = []
    skipped_unchanged_keys: list[str] = []
    skipped_limited_keys: list[str] = []

    for source_type in sorted(grouped, key=source_type_sort_key):
        candidates_for_type = tuple(
            sorted(
                grouped[source_type],
                key=lambda candidate: (
                    candidate.retrieval_score,
                    candidate.updated_at,
                    candidate.candidate_key,
                ),
                reverse=True,
            )
        )
        configured_limit = source_type_limit(
            source_type,
            topic.retrieval.source_type_limits,
        )
        carryover_limit = max(0, int(topic.retrieval.carryover_limit_per_type))
        new_candidates: list[ArchivistCandidate] = []
        carryover_candidates: list[ArchivistCandidate] = []
        unchanged_unused_keys: list[str] = []

        for candidate in candidates_for_type:
            usage = usage_by_key.get(candidate.candidate_key)
            if force or usage is None or not usage.is_unchanged(candidate):
                new_candidates.append(candidate)
                continue
            if usage.should_skip_unchanged_candidate(candidate):
                unchanged_unused_keys.append(candidate.candidate_key)
                continue
            if usage.should_carry_forward(candidate):
                carryover_candidates.append(candidate)
                continue
            unchanged_unused_keys.append(candidate.candidate_key)

        selected_candidates: list[ArchivistCandidate] = []
        skipped_limited_for_type: list[str] = []

        if force:
            selected_candidates = list(candidates_for_type[:configured_limit])
            skipped_limited_for_type = [
                candidate.candidate_key for candidate in candidates_for_type[configured_limit:]
            ]
        elif any_source_delta:
            selected_candidates.extend(new_candidates[:configured_limit])
            skipped_limited_for_type.extend(
                candidate.candidate_key for candidate in new_candidates[configured_limit:]
            )
            remaining = max(0, configured_limit - len(selected_candidates))
            if remaining > 0 and carryover_limit > 0:
                carryover_slice = carryover_candidates[: min(remaining, carryover_limit)]
                selected_candidates.extend(carryover_slice)
                skipped_limited_for_type.extend(
                    candidate.candidate_key
                    for candidate in carryover_candidates[min(remaining, carryover_limit):]
                )
            else:
                skipped_limited_for_type.extend(
                    candidate.candidate_key for candidate in carryover_candidates
                )

        if not selected_candidates:
            skipped_unchanged_keys.extend(unchanged_unused_keys)
            skipped_limited_keys.extend(skipped_limited_for_type)
            continue

        selected_key_set = {candidate.candidate_key for candidate in selected_candidates}
        new_key_set = {candidate.candidate_key for candidate in new_candidates}
        carryover_key_set = {candidate.candidate_key for candidate in carryover_candidates}

        stage_plans.append(
            ArchivistStagePlan(
                source_type=source_type,
                source_label=source_type_label(source_type),
                selected_candidates=tuple(selected_candidates),
                new_candidate_keys=tuple(
                    candidate.candidate_key
                    for candidate in selected_candidates
                    if candidate.candidate_key in new_key_set
                ),
                carryover_candidate_keys=tuple(
                    candidate.candidate_key
                    for candidate in selected_candidates
                    if candidate.candidate_key in carryover_key_set
                ),
                skipped_unchanged_candidate_keys=tuple(unchanged_unused_keys),
                skipped_limited_candidate_keys=tuple(skipped_limited_for_type),
            )
        )
        selected_keys.extend(candidate.candidate_key for candidate in selected_candidates)
        skipped_unchanged_keys.extend(unchanged_unused_keys)
        skipped_limited_keys.extend(skipped_limited_for_type)

    return ArchivistStagePlanningResult(
        stage_plans=tuple(stage_plans),
        any_source_delta=any_source_delta,
        selected_candidate_keys=tuple(dict.fromkeys(selected_keys)),
        skipped_unchanged_candidate_keys=tuple(dict.fromkeys(skipped_unchanged_keys)),
        skipped_limited_candidate_keys=tuple(dict.fromkeys(skipped_limited_keys)),
    )


def extract_cited_candidate_keys(
    content: str,
    candidates: tuple[ArchivistCandidate, ...] | list[ArchivistCandidate],
) -> tuple[str, ...]:
    """Return the candidate keys cited as [S1], [S2], ... in first-use order."""

    ordered_candidates = tuple(candidates)
    cited_keys: list[str] = []
    seen: set[str] = set()
    for match in _SOURCE_CITATION_RE.finditer(content or ""):
        index = int(match.group(1)) - 1
        if index < 0 or index >= len(ordered_candidates):
            continue
        candidate_key = ordered_candidates[index].candidate_key
        if candidate_key in seen:
            continue
        seen.add(candidate_key)
        cited_keys.append(candidate_key)
    return tuple(cited_keys)


def source_paths_for_candidates(candidates: Iterable[ArchivistCandidate]) -> tuple[str, ...]:
    normalized = [candidate.scope_relative_path for candidate in candidates]
    return tuple(dict.fromkeys(normalized))
