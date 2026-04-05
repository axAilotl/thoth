"""High-level archivist retrieval orchestration."""

from __future__ import annotations

from datetime import datetime, timezone

from ..archivist_topics import ArchivistTopicDefinition
from ..config import Config
from ..llm_interface import LLMInterface
from ..metadata_db import MetadataDB, get_metadata_db
from ..path_layout import PathLayout, build_path_layout
from .full_text import retrieve_full_text_documents
from .inventory import (
    ArchivistInventoryResult,
    document_matches_root,
    materialize_candidate,
    resolve_archivist_root_spec,
    sync_archivist_inventory,
)
from .models import (
    ArchivistCandidate,
    ArchivistCorpusDocument,
    ArchivistSelectionResult,
)
from .query import build_archivist_retrieval_query
from .semantic import retrieve_semantic_documents


async def select_archivist_candidates_async(
    topic: ArchivistTopicDefinition,
    *,
    config: Config,
    layout: PathLayout | None = None,
    db: MetadataDB | None = None,
    llm_interface: LLMInterface | None = None,
) -> ArchivistSelectionResult:
    """Return archivist candidates using modular retrieval backends."""

    resolved_layout = layout or build_path_layout(config)
    metadata_db = db or get_metadata_db()
    inventory = sync_archivist_inventory(
        topic.include_roots,
        exclude_root_specs=topic.exclude_roots,
        config=config,
        layout=resolved_layout,
        db=metadata_db,
    )
    include_roots = tuple(
        resolve_archivist_root_spec(root_spec, layout=resolved_layout)
        for root_spec in topic.include_roots
    )
    query = build_archivist_retrieval_query(topic)
    eligible_documents = tuple(
        document
        for document in inventory.documents
        if _passes_required_filters(document, topic)
        and _passes_exclusion_filters(document, topic)
    )

    if topic.retrieval.mode == "literal":
        candidates = _rank_literal_documents(
            eligible_documents,
            include_roots=include_roots,
            topic=topic,
        )
    elif topic.retrieval.mode == "full_text":
        candidates = _rank_full_text_documents(
            eligible_documents,
            include_roots=include_roots,
            topic=topic,
            query=query,
            db=metadata_db,
        )
    elif topic.retrieval.mode == "semantic":
        if llm_interface is None:
            raise ValueError("Semantic archivist retrieval requires an LLM interface")
        candidates = await _rank_semantic_documents(
            eligible_documents,
            include_roots=include_roots,
            topic=topic,
            query=query,
            db=metadata_db,
            llm_interface=llm_interface,
        )
    else:
        if llm_interface is None:
            raise ValueError("Hybrid archivist retrieval requires an LLM interface")
        candidates = await _rank_hybrid_documents(
            eligible_documents,
            include_roots=include_roots,
            topic=topic,
            query=query,
            db=metadata_db,
            llm_interface=llm_interface,
        )

    if topic.max_sources is not None:
        candidates = candidates[: topic.max_sources]

    return ArchivistSelectionResult(
        topic_id=topic.id,
        candidates=tuple(candidates),
        scanned_roots=inventory.scanned_roots,
        missing_roots=inventory.missing_roots,
        indexed_count=inventory.indexed_count,
        retrieval_mode=topic.retrieval.mode,
    )


def _rank_literal_documents(
    documents: tuple[ArchivistCorpusDocument, ...],
    *,
    include_roots,
    topic: ArchivistTopicDefinition,
) -> list[ArchivistCandidate]:
    scored: list[tuple[ArchivistCorpusDocument, float]] = []
    for document in documents:
        score = _literal_query_score(document, topic)
        scored.append((document, score))
    return _materialize_ranked_candidates(scored, include_roots=include_roots, topic=topic)


def _rank_full_text_documents(
    documents: tuple[ArchivistCorpusDocument, ...],
    *,
    include_roots,
    topic: ArchivistTopicDefinition,
    query,
    db: MetadataDB,
) -> list[ArchivistCandidate]:
    allowed_keys = {document.candidate_key for document in documents}
    scored = retrieve_full_text_documents(
        db=db,
        query=query,
        include_roots=include_roots,
        source_types=topic.source_types,
        limit=topic.retrieval.full_text_limit,
    )
    scored = [
        (document, score)
        for document, score in scored
        if document.candidate_key in allowed_keys
    ]
    if not scored:
        return _rank_literal_documents(documents, include_roots=include_roots, topic=topic)

    return _materialize_ranked_candidates(
        _normalize_full_text_scores(scored),
        include_roots=include_roots,
        topic=topic,
        score_name="full_text",
    )


async def _rank_semantic_documents(
    documents: tuple[ArchivistCorpusDocument, ...],
    *,
    include_roots,
    topic: ArchivistTopicDefinition,
    query,
    db: MetadataDB,
    llm_interface: LLMInterface,
) -> list[ArchivistCandidate]:
    scored = await retrieve_semantic_documents(
        db=db,
        llm_interface=llm_interface,
        query=query,
        documents=documents,
        max_results=topic.retrieval.semantic_limit,
        max_new_embeddings_per_run=topic.retrieval.max_new_embeddings_per_run,
    )
    return _materialize_ranked_candidates(
        _normalize_semantic_scores(scored),
        include_roots=include_roots,
        topic=topic,
        score_name="semantic",
    )


async def _rank_hybrid_documents(
    documents: tuple[ArchivistCorpusDocument, ...],
    *,
    include_roots,
    topic: ArchivistTopicDefinition,
    query,
    db: MetadataDB,
    llm_interface: LLMInterface,
) -> list[ArchivistCandidate]:
    allowed_keys = {document.candidate_key for document in documents}
    full_text_results = retrieve_full_text_documents(
        db=db,
        query=query,
        include_roots=include_roots,
        source_types=topic.source_types,
        limit=topic.retrieval.full_text_limit,
    )
    full_text_results = [
        (document, score)
        for document, score in full_text_results
        if document.candidate_key in allowed_keys
    ]
    semantic_results = await retrieve_semantic_documents(
        db=db,
        llm_interface=llm_interface,
        query=query,
        documents=documents,
        max_results=topic.retrieval.semantic_limit,
        max_new_embeddings_per_run=topic.retrieval.max_new_embeddings_per_run,
    )
    if not full_text_results and not semantic_results:
        return _rank_literal_documents(documents, include_roots=include_roots, topic=topic)

    full_text_scores = dict(_normalize_full_text_scores(full_text_results))
    semantic_scores = dict(_normalize_semantic_scores(semantic_results))
    merged_documents = {
        document.candidate_key: document for document in documents
        if document.candidate_key in full_text_scores or document.candidate_key in semantic_scores
    }
    scored: list[tuple[ArchivistCorpusDocument, float, float, float]] = []
    for candidate_key, document in merged_documents.items():
        ft_score = full_text_scores.get(candidate_key, 0.0)
        sem_score = semantic_scores.get(candidate_key, 0.0)
        base_score = (
            topic.retrieval.full_text_weight * ft_score
            + topic.retrieval.semantic_weight * sem_score
        )
        scored.append((document, base_score, ft_score, sem_score))

    scored.sort(
        key=lambda item: (
            _apply_topic_weight(item[1], item[0], topic),
            item[0].updated_at,
            item[0].candidate_key,
        ),
        reverse=True,
    )
    candidates: list[ArchivistCandidate] = []
    for document, base_score, ft_score, sem_score in scored[: topic.retrieval.rerank_limit]:
        root_spec = _root_spec_for_document(document, include_roots)
        candidates.append(
            materialize_candidate(
                document,
                root_spec=root_spec,
                retrieval_score=_apply_topic_weight(base_score, document, topic),
                retrieval_sources=("full_text", "semantic"),
                full_text_score=ft_score,
                semantic_score=sem_score,
            )
        )
    return candidates


def _materialize_ranked_candidates(
    scored_documents,
    *,
    include_roots,
    topic: ArchivistTopicDefinition,
    score_name: str = "literal",
) -> list[ArchivistCandidate]:
    candidates: list[ArchivistCandidate] = []
    for item in scored_documents[: topic.retrieval.rerank_limit]:
        if len(item) == 2:
            document, score = item
            full_text_score = score if score_name == "full_text" else None
            semantic_score = score if score_name == "semantic" else None
        else:
            raise ValueError("Unsupported ranked document payload")
        weighted = _apply_topic_weight(score, document, topic)
        candidates.append(
            materialize_candidate(
                document,
                root_spec=_root_spec_for_document(document, include_roots),
                retrieval_score=weighted,
                retrieval_sources=(score_name,),
                full_text_score=full_text_score,
                semantic_score=semantic_score,
            )
        )
    candidates.sort(
        key=lambda item: (item.retrieval_score, item.updated_at, item.candidate_key),
        reverse=True,
    )
    return candidates


def _passes_required_filters(
    document: ArchivistCorpusDocument,
    topic: ArchivistTopicDefinition,
) -> bool:
    if topic.source_types and document.source_type not in topic.source_types:
        return False

    document_tags = set(document.tags)
    if topic.retrieval.tag_mode == "required":
        if topic.include_tags and document_tags.isdisjoint(topic.include_tags):
            return False

    if topic.retrieval.term_mode == "required":
        search_corpus = document.search_corpus()
        if topic.include_terms and not any(term in search_corpus for term in topic.include_terms):
            return False

    return True


def _passes_exclusion_filters(
    document: ArchivistCorpusDocument,
    topic: ArchivistTopicDefinition,
) -> bool:
    document_tags = set(document.tags)
    if topic.exclude_tags and not document_tags.isdisjoint(topic.exclude_tags):
        return False
    search_corpus = document.search_corpus()
    if topic.exclude_terms and any(term in search_corpus for term in topic.exclude_terms):
        return False
    return True


def _literal_query_score(
    document: ArchivistCorpusDocument,
    topic: ArchivistTopicDefinition,
) -> float:
    search_corpus = document.search_corpus()
    score = 0.0
    title_lower = document.title.lower()
    for term in topic.include_terms:
        if term in search_corpus:
            score += 1.5
        if term in title_lower:
            score += 1.0
    document_tags = set(document.tags)
    for tag in topic.include_tags:
        if tag in document_tags:
            score += 1.25
    if topic.title.lower() in search_corpus:
        score += 0.5
    return score + _recency_score(document.updated_at, recency_weight=topic.retrieval.recency_weight)


def _normalize_full_text_scores(scored_documents):
    if not scored_documents:
        return []
    normalized: list[tuple[ArchivistCorpusDocument, float]] = []
    for document, raw_rank in scored_documents:
        normalized.append((document, 1.0 / (1.0 + abs(float(raw_rank)))))
    return normalized


def _normalize_semantic_scores(scored_documents):
    return [
        (document, max(0.0, min(1.0, (float(score) + 1.0) / 2.0)))
        for document, score in scored_documents
    ]


def _apply_topic_weight(
    score: float,
    document: ArchivistCorpusDocument,
    topic: ArchivistTopicDefinition,
) -> float:
    weighted = float(score) * topic.retrieval.weight_for_source_type(document.source_type)
    return weighted + _recency_score(
        document.updated_at,
        recency_weight=topic.retrieval.recency_weight,
    )


def _recency_score(updated_at: str, *, recency_weight: float) -> float:
    if recency_weight <= 0:
        return 0.0
    try:
        timestamp = datetime.fromisoformat(updated_at)
    except ValueError:
        return 0.0
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    age_days = max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds() / 86400.0)
    return recency_weight * (1.0 / (1.0 + age_days / 30.0))


def _root_spec_for_document(document: ArchivistCorpusDocument, include_roots) -> str:
    for root in include_roots:
        if document_matches_root(document, root):
            return root.spec
    return include_roots[0].spec if include_roots else document.scope
