"""Archivist retrieval benchmarking helpers."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .archivist_retrieval.service import select_archivist_candidates_async
from .archivist_topics import ArchivistTopicDefinition, load_archivist_topic_registry
from .config import Config
from .llm_interface import LLMInterface
from .metadata_db import MetadataDB, get_metadata_db
from .path_layout import PathLayout, build_path_layout


@dataclass(frozen=True)
class ArchivistBenchmarkResult:
    """Summary of a retrieval-only archivist topic evaluation."""

    topic_id: str
    retrieval_mode: str
    candidate_count: int
    indexed_count: int
    scanned_roots: tuple[str, ...]
    missing_roots: tuple[str, ...]
    source_type_counts: dict[str, int]
    top_candidate_paths: tuple[str, ...]


async def benchmark_archivist_topics(
    config: Config,
    *,
    project_root: Path | None = None,
    topic_ids: Sequence[str] | None = None,
    limit: int | None = None,
    db: MetadataDB | None = None,
    llm_interface: LLMInterface | None = None,
) -> list[ArchivistBenchmarkResult]:
    """Run retrieval-only benchmarking for configured archivist topics."""

    resolved_root = project_root or Path.cwd()
    layout = build_path_layout(config, project_root=resolved_root)
    metadata_db = db or get_metadata_db()
    llm = llm_interface or LLMInterface(config.get("llm", {}))
    registry = load_archivist_topic_registry(
        config,
        project_root=resolved_root,
        required=True,
    )
    topics = _select_topics(registry.topics, topic_ids=topic_ids, limit=limit)
    results: list[ArchivistBenchmarkResult] = []
    for topic in topics:
        selection = await select_archivist_candidates_async(
            topic,
            config=config,
            layout=layout,
            db=metadata_db,
            llm_interface=llm,
        )
        counts = Counter(candidate.source_type for candidate in selection.candidates)
        results.append(
            ArchivistBenchmarkResult(
                topic_id=topic.id,
                retrieval_mode=selection.retrieval_mode,
                candidate_count=len(selection.candidates),
                indexed_count=selection.indexed_count,
                scanned_roots=selection.scanned_roots,
                missing_roots=selection.missing_roots,
                source_type_counts=dict(sorted(counts.items())),
                top_candidate_paths=tuple(
                    candidate.scope_relative_path
                    for candidate in selection.candidates[:10]
                ),
            )
        )
    return results


def _select_topics(
    topics: Sequence[ArchivistTopicDefinition],
    *,
    topic_ids: Sequence[str] | None,
    limit: int | None,
) -> tuple[ArchivistTopicDefinition, ...]:
    selected = tuple(topics)
    if topic_ids:
        wanted = {str(topic_id).strip().lower() for topic_id in topic_ids if str(topic_id).strip()}
        selected = tuple(topic for topic in selected if topic.id in wanted)
    if limit is not None:
        selected = selected[: max(0, int(limit))]
    return selected
