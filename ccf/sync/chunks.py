"""Chunked transfer integrity for resumable packs (spec 11.4).

A pack file is split into fixed-size chunks; a sidecar document records
the whole-file digest, the chunk size, the total length, and one SHA-256
digest per chunk. Receivers verify each chunk as it arrives, so an
interrupted transfer resumes from the first unverified chunk instead of
restarting — over HTTP byte ranges or a plain file/USB copy with
identical semantics.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from ccf.hashing import digest_string, parse_digest
from ccf.sync.packio import PackError

DEFAULT_CHUNK_SIZE = 65536
SIDECAR_SUFFIX = ".chunks.json"
SIDECAR_FORMAT = "ccf.delta-pack-chunks/0.1.2-rc1"


class ChunkVerificationError(PackError):
    """Raised when a chunk or pack file fails its recorded digest."""


def chunk_count(total_length: int, chunk_size: int) -> int:
    return (total_length + chunk_size - 1) // chunk_size


def build_sidecar(pack_path: str | Path, *, chunk_size: int = DEFAULT_CHUNK_SIZE) -> dict:
    """Compute the chunk sidecar for a pack file (streamed, constant memory)."""
    if chunk_size <= 0:
        raise PackError(f"chunk size must be positive: {chunk_size}")
    pack_path = Path(pack_path)
    chunks: list[str] = []
    hasher = hashlib.sha256()
    total_length = 0
    with pack_path.open("rb") as fh:
        while True:
            block = fh.read(chunk_size)
            if not block:
                break
            chunks.append(digest_string(block))
            hasher.update(block)
            total_length += len(block)
    return {
        "format": SIDECAR_FORMAT,
        "pack_digest": "sha256:" + hasher.hexdigest(),
        "chunk_size": chunk_size,
        "total_length": total_length,
        "chunks": chunks,
    }


def write_sidecar(pack_path: str | Path, *, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Path:
    """Write ``<pack>.chunks.json`` next to the pack file; return its path."""
    pack_path = Path(pack_path)
    sidecar = build_sidecar(pack_path, chunk_size=chunk_size)
    sidecar_path = pack_path.with_name(pack_path.name + SIDECAR_SUFFIX)
    sidecar_path.write_text(json.dumps(sidecar, indent=2) + "\n", encoding="utf-8")
    return sidecar_path


def load_sidecar(sidecar_path: str | Path) -> dict:
    """Load and validate a chunk sidecar."""
    try:
        sidecar = json.loads(Path(sidecar_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PackError(f"cannot read chunk sidecar {sidecar_path}: {exc}") from exc
    for field in ("format", "pack_digest", "chunk_size", "total_length", "chunks"):
        if field not in sidecar:
            raise PackError(f"chunk sidecar missing {field!r}")
    if sidecar["format"] != SIDECAR_FORMAT:
        raise PackError(f"not a chunk sidecar: {sidecar['format']!r}")
    parse_digest(sidecar["pack_digest"])
    for digest in sidecar["chunks"]:
        parse_digest(digest)
    expected = chunk_count(int(sidecar["total_length"]), int(sidecar["chunk_size"]))
    if len(sidecar["chunks"]) != expected:
        raise PackError(
            f"sidecar declares {len(sidecar['chunks'])} chunks, expected {expected}"
        )
    return sidecar


def verify_chunk(sidecar: dict, index: int, data: bytes) -> None:
    """Verify one chunk's bytes against its sidecar digest (fail closed)."""
    chunk_size = int(sidecar["chunk_size"])
    total = int(sidecar["total_length"])
    chunks = sidecar["chunks"]
    if not 0 <= index < len(chunks):
        raise ChunkVerificationError(f"chunk index out of range: {index}")
    offset = index * chunk_size
    expected_length = min(chunk_size, total - offset)
    if len(data) != expected_length:
        raise ChunkVerificationError(
            f"chunk {index} length {len(data)} != expected {expected_length}"
        )
    if digest_string(data) != chunks[index]:
        raise ChunkVerificationError(f"chunk {index} digest mismatch (tampered)")


def verify_file(pack_path: str | Path, sidecar: dict) -> None:
    """Verify a complete pack file against its sidecar (fail closed).

    Streamed chunk by chunk: memory use stays bounded by the chunk size.
    """
    pack_path = Path(pack_path)
    total = int(sidecar["total_length"])
    if pack_path.stat().st_size != total:
        raise ChunkVerificationError(
            f"pack length {pack_path.stat().st_size} != sidecar {total}"
        )
    chunk_size = int(sidecar["chunk_size"])
    hasher = hashlib.sha256()
    with pack_path.open("rb") as fh:
        for index in range(len(sidecar["chunks"])):
            block = fh.read(chunk_size)
            verify_chunk(sidecar, index, block)
            hasher.update(block)
    if "sha256:" + hasher.hexdigest() != sidecar["pack_digest"]:
        raise ChunkVerificationError("pack digest mismatch (tampered)")
