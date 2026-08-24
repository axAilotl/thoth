"""Resumable pack transport (spec 6.7, 11.4).

One chunked-copy engine drives every transport: a ``ChunkSource`` reads
byte ranges, the engine verifies each chunk against the sidecar digests
before appending it to a ``.part`` file, existing verified prefixes are
kept across interruptions, and the completed file is renamed into place
only after the whole-pack digest matches.

- :class:`FileChunkSource` — plain file/USB copy (conformance baseline);
- :class:`HttpChunkSource` — HTTP byte-range GETs via httpx;
- :func:`make_pack_app` — a small FastAPI app serving a pack with Range
  support, so HTTP and file copies exercise identical pack semantics.
"""

from __future__ import annotations

import os
from pathlib import Path

from ccf.sync.chunks import (
    ChunkVerificationError,
    SIDECAR_SUFFIX,
    chunk_count,
    load_sidecar,
    verify_chunk,
    verify_file,
)
from ccf.sync.packio import PackError


class TransportError(PackError):
    """Raised when a transport cannot deliver verified bytes."""


class FileChunkSource:
    """Byte-range reads from a local pack file (file/USB transport)."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if not self.path.is_file():
            raise TransportError(f"pack file not found: {self.path}")

    def length(self) -> int:
        return self.path.stat().st_size

    def read(self, offset: int, length: int) -> bytes:
        with self.path.open("rb") as handle:
            handle.seek(offset)
            return handle.read(length)


class HttpChunkSource:
    """Byte-range reads over HTTP Range GETs (httpx)."""

    def __init__(self, url: str, *, client=None, timeout: float = 30.0) -> None:
        self._url = url
        self._timeout = timeout
        if client is None:
            import httpx

            client = httpx.Client(timeout=timeout)
            self._owns_client = True
        else:
            self._owns_client = False
        self._client = client

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _range_get(self, offset: int, length: int):
        end = offset + length - 1
        response = self._client.get(
            self._url, headers={"Range": f"bytes={offset}-{end}"}
        )
        if response.status_code != 206:
            raise TransportError(
                f"range GET {self._url} returned {response.status_code}, "
                "expected 206 Partial Content"
            )
        return response

    def length(self) -> int:
        response = self._range_get(0, 1)
        # Content-Range: bytes 0-0/<total>
        return int(response.headers["Content-Range"].rsplit("/", 1)[1])

    def read(self, offset: int, length: int) -> bytes:
        return self._range_get(offset, length).content


def fetch_sidecar_http(url: str, *, client=None) -> dict:
    """Fetch and validate ``<pack>.chunks.json`` over HTTP."""
    owns = client is None
    if owns:
        import httpx

        client = httpx.Client(timeout=30.0)
    try:
        response = client.get(url + SIDECAR_SUFFIX)
        if response.status_code != 200:
            raise TransportError(
                f"sidecar GET {url}{SIDECAR_SUFFIX} returned {response.status_code}"
            )
        from ccf.jcs import loads as jcs_loads

        sidecar = jcs_loads(response.content)
        # Reuse the file-sidecar validation by round-tripping the document.
        from ccf.sync.chunks import SIDECAR_FORMAT
        from ccf.hashing import parse_digest

        if sidecar.get("format") != SIDECAR_FORMAT:
            raise TransportError("remote sidecar has unexpected format")
        parse_digest(sidecar["pack_digest"])
        return sidecar
    finally:
        if owns:
            client.close()


def resumable_copy(source, sidecar: dict, dest_path: str | Path) -> dict:
    """Copy a pack from ``source`` to ``dest_path``, resuming verified prefixes.

    Existing ``<dest>.part`` bytes are re-verified chunk by chunk; the
    first unverified or absent chunk is where transfer resumes. The final
    file is verified against the whole-pack digest and atomically renamed.
    """
    dest_path = Path(dest_path)
    part_path = dest_path.with_name(dest_path.name + ".part")
    chunk_size = int(sidecar["chunk_size"])
    total = int(sidecar["total_length"])
    chunks = sidecar["chunks"]
    if chunk_count(total, chunk_size) != len(chunks):
        raise TransportError("sidecar chunk list does not match total length")
    if hasattr(source, "length") and source.length() != total:
        raise TransportError(
            f"source length {source.length()} != sidecar total {total}"
        )

    verified = 0
    if part_path.exists():
        existing = part_path.read_bytes()
        while verified < len(chunks):
            offset = verified * chunk_size
            data = existing[offset : offset + chunk_size]
            if len(data) != min(chunk_size, total - offset):
                break
            try:
                verify_chunk(sidecar, verified, data)
            except ChunkVerificationError:
                break
            verified += 1
        # Drop any unverified tail.
        with part_path.open("r+b") as handle:
            handle.truncate(verified * chunk_size)

    transferred = 0
    with part_path.open("ab") as handle:
        for index in range(verified, len(chunks)):
            offset = index * chunk_size
            length = min(chunk_size, total - offset)
            data = source.read(offset, length)
            verify_chunk(sidecar, index, data)
            handle.write(data)
            transferred += 1

    verify_file(part_path, sidecar)
    os.replace(part_path, dest_path)
    return {
        "dest": str(dest_path),
        "total_length": total,
        "chunks_total": len(chunks),
        "chunks_resumed": verified,
        "chunks_transferred": transferred,
    }


def copy_pack_file(
    source_path: str | Path, dest_path: str | Path, *, sidecar_path: str | Path | None = None
) -> dict:
    """File/USB transport: resumable copy with verified chunk digests."""
    source_path = Path(source_path)
    sidecar = load_sidecar(
        sidecar_path or source_path.with_name(source_path.name + SIDECAR_SUFFIX)
    )
    return resumable_copy(FileChunkSource(source_path), sidecar, dest_path)


def fetch_pack_http(url: str, dest_path: str | Path, *, client=None) -> dict:
    """HTTP transport: fetch a pack with Range requests and resume support."""
    sidecar = fetch_sidecar_http(url, client=client)
    source = HttpChunkSource(url, client=client)
    try:
        return resumable_copy(source, sidecar, dest_path)
    finally:
        source.close()


def make_pack_app(pack_path: str | Path):
    """FastAPI app serving one pack file with HTTP Range support.

    Routes: ``GET /pack`` (Range-aware), ``GET /pack.chunks.json``.
    """
    import fastapi
    from fastapi.responses import Response

    pack_path = Path(pack_path)
    sidecar_path = pack_path.with_name(pack_path.name + SIDECAR_SUFFIX)
    if not pack_path.is_file() or not sidecar_path.is_file():
        raise TransportError(
            f"pack and sidecar must exist before serving: {pack_path}"
        )

    app = fastapi.FastAPI()

    @app.get("/pack.chunks.json")
    def sidecar():
        return Response(
            sidecar_path.read_bytes(), media_type="application/json"
        )

    def pack(request: fastapi.Request):
        data = pack_path.read_bytes()
        range_header = request.headers.get("range")
        if range_header is None:
            return Response(data, media_type="application/octet-stream")
        try:
            unit, _, bounds = range_header.partition("=")
            if unit.strip() != "bytes":
                raise ValueError
            start_text, _, end_text = bounds.partition("-")
            start = int(start_text)
            end = int(end_text) if end_text else len(data) - 1
            if start < 0 or end < start or end >= len(data):
                raise ValueError
        except ValueError:
            raise fastapi.HTTPException(
                status_code=416, detail="invalid Range"
            ) from None
        return Response(
            data[start : end + 1],
            status_code=206,
            media_type="application/octet-stream",
            headers={
                "Content-Range": f"bytes {start}-{end}/{len(data)}",
                "Accept-Ranges": "bytes",
            },
        )

    # ``from __future__ import annotations`` defers evaluation; bind the
    # request annotation to the real class before FastAPI reads the hints.
    pack.__annotations__["request"] = fastapi.Request
    app.add_api_route("/pack", pack, methods=["GET"])
    return app
