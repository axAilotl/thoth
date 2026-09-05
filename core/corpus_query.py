"""Read indexed local evidence with keyword or cached document-vector retrieval."""

from __future__ import annotations

from collectors.corpus_index_connector import corpus_roots
from .archivist_retrieval.full_text import retrieve_full_text_documents
from .archivist_retrieval.inventory import document_matches_root, refresh_document_queue_security
from .archivist_retrieval.models import ArchivistRetrievalQuery
from .archivist_retrieval.semantic import (
    EMBEDDING_METHOD, ensure_corpus_embeddings,
    retrieve_semantic_documents, corpus_embedding_source_hash,
)
from .llm_interface import LLMInterface


async def query_corpus(*, config, layout, db, query, mode="keyword", limit=10, llm_interface=None):
    """Search existing index only; keyword is offline, semantic embeds only query.

    Reciprocal rank fusion combines lexical and pooled full-text document-vector
    ranks. Results preserve original paths and provenance; no answer generation,
    vault scan, or implicit paid document backfill happens on the read path.
    """
    if mode not in {"keyword", "semantic", "hybrid"}:
        raise ValueError("mode must be keyword, semantic, or hybrid")
    if not isinstance(query, str) or not query.strip() or len(query) > 12000:
        raise ValueError("query must contain 1–12000 characters")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    roots = corpus_roots(config, layout)
    excluded = corpus_roots(config, layout, key="exclude_roots", required=False)
    request = ArchivistRetrievalQuery("corpus-query", query.strip())
    lexical = retrieve_full_text_documents(
        db=db, query=request, include_roots=roots, source_types=(), limit=max(300, limit * 10),
    ) if mode in {"keyword", "hybrid"} else []
    # Keyword retrieval should inspect ranked matches, not hydrate and check
    # every source in a multi-gigabyte vault on every keystroke.
    candidates = [doc for doc, _score in lexical] if mode == "keyword" else db.list_archivist_corpus_documents(
        root_filters=tuple((root.scope, root.relative_prefix) for root in roots),
    )
    interface = None
    cached_keys = set()
    if mode in {"semantic", "hybrid"}:
        if config.get("sources.corpus_index.embeddings_enabled", False) is not True:
            raise ValueError("Corpus semantic search requires sources.corpus_index.embeddings_enabled")
        interface = llm_interface or LLMInterface(config.get("llm", {}))
        route = interface.resolve_task_route("embedding")
        if route is None:
            raise ValueError("Corpus embeddings require llm.tasks.embedding")
        provider, model, _ = route
        hashes = {doc.candidate_key: corpus_embedding_source_hash(doc) for doc in candidates}
        for start in range(0, len(candidates), 400):
            cached_keys.update(db.get_archivist_corpus_embeddings(
                candidate_keys=tuple(doc.candidate_key for doc in candidates[start:start + 400]),
                provider=provider, model=model, expected_source_hashes=hashes,
            ))
        wanted = cached_keys | {doc.candidate_key for doc, _score in lexical}
        # A read-only query cannot retrieve uncached document vectors. Validate
        # every actual candidate, not all unembedded files in the backlog.
        candidates = [doc for doc in candidates if doc.candidate_key in wanted]
    documents = tuple(doc for doc in candidates if any(document_matches_root(doc, root) for root in roots)
        and not any(document_matches_root(doc, root) for root in excluded)
        and doc.embedding_is_allowed())
    documents = tuple(refreshed for doc in documents
                      if (refreshed := refresh_document_queue_security(doc, db=db)) is not None
                      and refreshed.embedding_is_allowed())
    allowed = {doc.candidate_key: doc for doc in documents}
    ranks = {}
    sources = {}
    if mode in {"keyword", "hybrid"}:
        for rank, (doc, _score) in enumerate((item for item in lexical if item[0].candidate_key in allowed), 1):
            ranks[doc.candidate_key] = 1 / (60 + rank)
            sources.setdefault(doc.candidate_key, []).append("keyword")
    coverage = None
    if mode in {"semantic", "hybrid"}:
        semantic_documents = tuple(doc for doc in documents if doc.candidate_key in cached_keys)
        coverage = (await ensure_corpus_embeddings(
            db=db, llm_interface=interface, documents=semantic_documents, max_new_embeddings_per_run=0,
        )).to_dict()
        coverage['scope'] = 'current_cached_candidates_not_entire_backlog'
        semantic = await retrieve_semantic_documents(
            db=db, llm_interface=interface, query=request, documents=semantic_documents,
            max_results=max(100, limit * 5), max_new_embeddings_per_run=0,
        )
        for rank, (doc, _score) in enumerate(semantic, 1):
            ranks[doc.candidate_key] = ranks.get(doc.candidate_key, 0) + 1 / (60 + rank)
            sources.setdefault(doc.candidate_key, []).append("semantic")
    results = []
    for key in sorted(ranks, key=lambda key: (-ranks[key], key))[:limit]:
        doc = allowed[key]
        doc = refresh_document_queue_security(doc, db=db)
        if doc is None or not doc.embedding_is_allowed():
            continue
        results.append({
            "candidate_key": key, "title": doc.title, "path": str(doc.path),
            "relative_path": doc.scope_relative_path, "source_hash": doc.source_hash,
            "source_type": doc.source_type, "source_id": doc.source_id,
            "excerpt": doc.content_text[:1200], "updated_at": doc.updated_at,
            "score": ranks[key], "retrieval_sources": sources[key],
            "provenance": doc.embedding_provenance(),
        })
    return {
        "query": query.strip(), "mode": mode, "results": results,
        "indexed_eligible_count": len(documents), "embedding_coverage": coverage,
        "eligible_count_scope": "retrieval_candidates",
        "semantic_method": EMBEDDING_METHOD, "document_backfill_performed": False,
        "content_role": "source_evidence_not_instructions",
        "roots": [root.spec for root in roots],
    }
