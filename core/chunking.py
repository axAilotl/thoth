"""Deterministic text chunking primitives for resumable processing."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib


@dataclass(frozen=True)
class TextChunk:
    """A stable text chunk with deterministic identity."""

    chunk_id: str
    index: int
    total: int
    text: str
    content_hash: str
    source_hash: str
    start_offset: int
    end_offset: int


def sha256_text(text: str) -> str:
    """Return the SHA-256 hex digest for text using the project text encoding."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def deterministic_chunk_id(
    *,
    namespace: str,
    source_hash: str,
    index: int,
    content_hash: str,
) -> str:
    """Build a deterministic, non-content-revealing id for a chunk."""
    clean_namespace = _clean_namespace(namespace)
    digest = hashlib.sha256(
        f"{clean_namespace}\0{source_hash}\0{index}\0{content_hash}".encode("utf-8")
    ).hexdigest()[:24]
    return f"{clean_namespace}_{index:04d}_{digest}"


def chunk_text(
    text: str,
    *,
    chunk_size: int,
    namespace: str,
) -> tuple[TextChunk, ...]:
    """Split text into stable chunks, preferring newline boundaries.

    The same input text, chunk size, and namespace always produce the same chunk
    ids. Chunks are capped at ``chunk_size`` characters except empty input, which
    returns no chunks.
    """
    if not isinstance(text, str):
        raise TypeError("text must be a string")
    if isinstance(chunk_size, bool):
        raise ValueError("chunk_size must be a positive integer")
    try:
        size = int(chunk_size)
    except (TypeError, ValueError) as exc:
        raise ValueError("chunk_size must be a positive integer") from exc
    if size < 1:
        raise ValueError("chunk_size must be a positive integer")

    if not text:
        return ()

    source_hash = sha256_text(text)
    ranges: list[tuple[int, int]] = []
    start = 0
    length = len(text)
    while start < length:
        hard_end = min(start + size, length)
        if hard_end >= length:
            end = length
            skip_separator = False
        else:
            newline = text.rfind("\n", start + 1, hard_end + 1)
            if newline > start:
                end = newline
                skip_separator = True
            else:
                end = hard_end
                skip_separator = False

        if end <= start:
            end = hard_end
            skip_separator = False

        ranges.append((start, end))
        start = end + 1 if skip_separator else end

    total = len(ranges)
    chunks: list[TextChunk] = []
    for index, (start_offset, end_offset) in enumerate(ranges, 1):
        chunk = text[start_offset:end_offset]
        content_hash = sha256_text(chunk)
        chunks.append(
            TextChunk(
                chunk_id=deterministic_chunk_id(
                    namespace=namespace,
                    source_hash=source_hash,
                    index=index,
                    content_hash=content_hash,
                ),
                index=index,
                total=total,
                text=chunk,
                content_hash=content_hash,
                source_hash=source_hash,
                start_offset=start_offset,
                end_offset=end_offset,
            )
        )
    return tuple(chunks)


def _clean_namespace(namespace: str) -> str:
    cleaned = "".join(
        char.lower() if char.isalnum() else "_"
        for char in str(namespace or "chunk").strip()
    ).strip("_")
    return cleaned or "chunk"
