"""Semantic retrieval backend for archivist topics."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from math import isfinite, sqrt
from typing import Sequence

from ..chunking import chunk_text
from ..sensitive_redaction import SensitiveRedactionError, redact_sensitive_text
from .inventory import refresh_document_queue_security
from .models import ArchivistCorpusDocument

SEMANTIC_DOCUMENT_CHAR_LIMIT = 6000
EMBEDDING_BATCH_SIZE = 16
EMBEDDING_METHOD = "full-content-chunk-mean-v1"


class ArchivistSemanticRetrievalError(ValueError):
    """Raised when semantic retrieval cannot run safely."""


@dataclass(frozen=True)
class CorpusEmbeddingResult:
    stored: dict
    eligible_count: int
    blocked_count: int
    empty_count: int
    embedded_count: int
    reused_count: int
    pending_count: int
    oversized_count: int
    chunk_inputs: int

    def to_dict(self) -> dict:
        return {
            "method": EMBEDDING_METHOD,
            "representation": "document_vector_mean_of_all_text_chunks",
            "chunk_chars": SEMANTIC_DOCUMENT_CHAR_LIMIT,
            **{key: value for key, value in vars(self).items() if key != "stored"},
            "covered_count": len(self.stored),
        }


def corpus_embedding_source_hash(document: ArchivistCorpusDocument) -> str:
    payload = f"{EMBEDDING_METHOD}:{SEMANTIC_DOCUMENT_CHAR_LIMIT}:{document.embedding_source_hash()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def ensure_corpus_embeddings(
    *, db, llm_interface, documents: Sequence[ArchivistCorpusDocument],
    max_new_embeddings_per_run: int, budget=None,
) -> CorpusEmbeddingResult:
    """Backfill whole-document vectors within an actual chunk-input budget.

    Completed documents persist immediately. Never save a partial document or
    reuse the old prefix-only cache. Oversized documents remain explicitly
    pending until the operator increases the budget. No summaries are generated.
    """
    if (isinstance(max_new_embeddings_per_run, bool)
            or not isinstance(max_new_embeddings_per_run, int)
            or max_new_embeddings_per_run < 0):
        raise ValueError("max_new_embeddings_per_run must be a nonnegative integer")
    refreshed = [refresh_document_queue_security(doc, db=db) for doc in documents]
    eligible = [doc for doc in refreshed if doc is not None and doc.content_text.strip() and _document_embedding_is_allowed(doc)]
    empty_count = sum(not doc.content_text.strip() for doc in documents)
    blocked_count = len(documents) - len(eligible) - empty_count
    route = llm_interface.resolve_task_route("embedding")
    if route is None:
        raise ArchivistSemanticRetrievalError("Corpus embeddings require llm.tasks.embedding")
    provider, model, _ = route
    hashes = {doc.candidate_key: corpus_embedding_source_hash(doc) for doc in eligible}
    stored = {}
    # Keep SQLite parameter counts bounded for very large vaults.
    for batch in _chunk_documents(eligible, 400):
        stored.update(db.get_archivist_corpus_embeddings(
            candidate_keys=tuple(doc.candidate_key for doc in batch),
            provider=provider, model=model, expected_source_hashes=hashes,
        ))
    reused_count = len(stored)
    used = embedded = oversized = reserved = 0
    selected = []
    for document in eligible:
        if document.candidate_key in stored:
            continue
        if max_new_embeddings_per_run == 0:
            continue
        texts = [chunk.text for chunk in chunk_text(
            _embedding_text(document), chunk_size=SEMANTIC_DOCUMENT_CHAR_LIMIT,
            namespace="corpus_embedding",
        )]
        if len(texts) > max_new_embeddings_per_run:
            oversized += 1
            continue
        if reserved + len(texts) > max_new_embeddings_per_run:
            continue
        if budget is not None:
            for text in texts:
                budget.add_input_text(text, label=document.candidate_key)
        reserved += len(texts)
        selected.append((document, texts))
    # Batch across document boundaries, not one network request per short note.
    inputs = [(doc, text, index == len(texts) - 1)
              for doc, texts in selected for index, text in enumerate(texts)]
    partial = {}
    rejected_keys = set()
    for start in range(0, len(inputs), EMBEDDING_BATCH_SIZE):
        batch = []
        for document, text, last in inputs[start:start + EMBEDDING_BATCH_SIZE]:
            current = refresh_document_queue_security(document, db=db)
            if (current is None or not _document_embedding_is_allowed(current)
                    or corpus_embedding_source_hash(current) != hashes[document.candidate_key]):
                rejected_keys.add(document.candidate_key)
            if document.candidate_key not in rejected_keys:
                batch.append((document, text, last))
        if not batch:
            continue
        response = await llm_interface.embed_texts(
            [text for _doc, text, _last in batch], provider=provider, model=model,
        )
        if response.error:
            raise ArchivistSemanticRetrievalError(f"Corpus embedding failed: {response.error}")
        if len(response.vectors) != len(batch):
            raise ArchivistSemanticRetrievalError("Embedding provider returned wrong vector count")
        used += len(batch)
        for (document, _text, last), chunk_vector in zip(batch, response.vectors):
            vectors = partial.setdefault(document.candidate_key, [])
            vectors.append(chunk_vector)
            if not last:
                continue
            vector = _mean_vector(vectors)
            db.upsert_archivist_corpus_embedding(
                candidate_key=document.candidate_key, provider=provider, model=model,
                source_hash=hashes[document.candidate_key],
                provenance=document.embedding_provenance(), vector=vector,
            )
            stored[document.candidate_key] = {"vector": vector}
            partial.pop(document.candidate_key)
            embedded += 1
    return CorpusEmbeddingResult(
        stored, len(eligible), blocked_count, empty_count, embedded,
        reused_count, len(eligible) - len(stored), oversized, used,
    )


def _mean_vector(vectors: Sequence[Sequence[float]]) -> list[float]:
    if not vectors or not vectors[0]:
        raise ArchivistSemanticRetrievalError("Embedding provider returned an empty vector")
    dimension = len(vectors[0])
    if any(len(vector) != dimension or any(not isfinite(float(value)) for value in vector) for vector in vectors):
        raise ArchivistSemanticRetrievalError("Embedding provider returned invalid vector dimensions or values")
    # Normalize each chunk first so provider-specific magnitudes cannot dominate.
    normalized = []
    for vector in vectors:
        norm = sqrt(sum(float(value) ** 2 for value in vector))
        normalized.append([float(value) / norm if norm else 0.0 for value in vector])
    return [sum(vector[index] for vector in normalized) / len(normalized) for index in range(dimension)]


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
    embedding_documents = tuple(
        document for document in documents if _document_embedding_is_allowed(document)
    )
    if not embedding_documents:
        return []
    if _text_has_sensitive_findings(query.text):
        raise ArchivistSemanticRetrievalError(
            "Semantic retrieval query contains sensitive content and cannot be embedded"
        )

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

    coverage = await ensure_corpus_embeddings(
        db=db, llm_interface=llm_interface, documents=embedding_documents,
        max_new_embeddings_per_run=max_new_embeddings_per_run,
    )
    stored = coverage.stored

    scored: list[tuple[ArchivistCorpusDocument, float]] = []
    for document in embedding_documents:
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
            f"Source ID: {document.source_id or 'n/a'}",
            f"Source Key: {document.source_key or 'n/a'}",
            f"Source Trust: {document.source_trust_score:.3f} ({document.source_trust_reason})",
            f"Tags: {', '.join(document.tags) if document.tags else 'none'}",
            f"Path: {document.scope_relative_path}",
            "",
            document.content_text,
        ]
    )
    return base.strip()


def _document_embedding_is_allowed(document: ArchivistCorpusDocument) -> bool:
    return document.embedding_is_allowed() and not _text_has_sensitive_findings(
        _embedding_text(document)
    )


def _text_has_sensitive_findings(value: str) -> bool:
    try:
        return redact_sensitive_text(value).has_findings
    except SensitiveRedactionError:
        return True


def _chunk_documents(
    documents: Sequence[ArchivistCorpusDocument],
    batch_size: int,
) -> list[Sequence[ArchivistCorpusDocument]]:
    return [documents[index : index + batch_size] for index in range(0, len(documents), batch_size)]
