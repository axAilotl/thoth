"""Semantic retrieval backend for archivist topics."""

from __future__ import annotations

from math import sqrt
from typing import Sequence

from .models import ArchivistCorpusDocument

SEMANTIC_DOCUMENT_CHAR_LIMIT = 6000
EMBEDDING_BATCH_SIZE = 16


class ArchivistSemanticRetrievalError(ValueError):
    """Raised when semantic retrieval cannot run safely."""


async def retrieve_semantic_documents(
    *,
    db,
    llm_interface,
    query,
    documents: Sequence[ArchivistCorpusDocument],
    max_results: int,
    max_new_embeddings_per_run: int,
) -> list[tuple[ArchivistCorpusDocument, float]]:
    """Return cosine-ranked documents using the configured embedding route."""

    if not documents:
        return []
    route = llm_interface.resolve_task_route("embedding")
    if route is None:
        raise ArchivistSemanticRetrievalError(
            "Semantic retrieval requires llm.tasks.embedding to be configured"
        )
    provider_name, model_id, _ = route

    query_response = await llm_interface.embed_texts(
        [query.text],
        provider=provider_name,
        model=model_id,
    )
    if query_response.error:
        raise ArchivistSemanticRetrievalError(
            f"Failed to embed archivist query: {query_response.error}"
        )
    if len(query_response.vectors) != 1:
        raise ArchivistSemanticRetrievalError(
            "Embedding provider returned an invalid query vector payload"
        )
    query_vector = query_response.vectors[0]

    documents_by_key = {document.candidate_key: document for document in documents}
    stored = db.get_archivist_corpus_embeddings(
        candidate_keys=tuple(documents_by_key.keys()),
        provider=provider_name,
        model=model_id,
    )

    missing: list[ArchivistCorpusDocument] = []
    for document in documents:
        payload = stored.get(document.candidate_key)
        if payload is None or payload.get("source_hash") != document.source_hash:
            missing.append(document)

    if missing:
        if max_new_embeddings_per_run <= 0:
            raise ArchivistSemanticRetrievalError(
                "Semantic retrieval is enabled but max_new_embeddings_per_run is 0"
            )
        for batch in _chunk_documents(missing[:max_new_embeddings_per_run], EMBEDDING_BATCH_SIZE):
            response = await llm_interface.embed_texts(
                [_embedding_text(document) for document in batch],
                provider=provider_name,
                model=model_id,
            )
            if response.error:
                raise ArchivistSemanticRetrievalError(
                    f"Failed to embed archivist corpus batch: {response.error}"
                )
            if len(response.vectors) != len(batch):
                raise ArchivistSemanticRetrievalError(
                    "Embedding provider returned the wrong number of vectors for archivist corpus documents"
                )
            for document, vector in zip(batch, response.vectors):
                db.upsert_archivist_corpus_embedding(
                    candidate_key=document.candidate_key,
                    provider=provider_name,
                    model=model_id,
                    source_hash=document.source_hash,
                    vector=list(vector),
                )

        stored = db.get_archivist_corpus_embeddings(
            candidate_keys=tuple(documents_by_key.keys()),
            provider=provider_name,
            model=model_id,
        )

    scored: list[tuple[ArchivistCorpusDocument, float]] = []
    for document in documents:
        payload = stored.get(document.candidate_key)
        if payload is None:
            continue
        vector = payload.get("vector") or []
        if not vector:
            continue
        scored.append((document, cosine_similarity(query_vector, vector)))

    scored.sort(key=lambda item: (item[1], item[0].updated_at, item[0].candidate_key), reverse=True)
    return scored[:max_results]


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    """Compute cosine similarity for equal-length vectors."""

    if len(left) != len(right) or not left:
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(left, right))
    left_norm = sqrt(sum(float(a) * float(a) for a in left))
    right_norm = sqrt(sum(float(b) * float(b) for b in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)


def _embedding_text(document: ArchivistCorpusDocument) -> str:
    base = "\n".join(
        [
            f"Title: {document.title}",
            f"Source Type: {document.source_type}",
            f"Tags: {', '.join(document.tags) if document.tags else 'none'}",
            f"Path: {document.scope_relative_path}",
            "",
            document.content_text[:SEMANTIC_DOCUMENT_CHAR_LIMIT],
        ]
    )
    return base.strip()


def _chunk_documents(
    documents: Sequence[ArchivistCorpusDocument],
    batch_size: int,
) -> list[Sequence[ArchivistCorpusDocument]]:
    return [documents[index : index + batch_size] for index in range(0, len(documents), batch_size)]
