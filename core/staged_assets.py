"""Atomic staging helpers for vault-bound binary assets."""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import Config
from .path_layout import PathLayout, build_path_layout


class StagedAssetError(RuntimeError):
    """Base class for staged asset failures."""


class StagedAssetValidationError(StagedAssetError, ValueError):
    """Raised when staged bytes do not match the expected asset type."""


@dataclass(frozen=True)
class PublishedAsset:
    """Published asset metadata."""

    path: Path
    size_bytes: int
    sha256: str


def _looks_like_pdf(header: bytes) -> bool:
    return header.startswith(b"%PDF")


def _looks_like_image(header: bytes) -> bool:
    return (
        header.startswith(b"\xff\xd8\xff")
        or header.startswith(b"\x89PNG\r\n\x1a\n")
        or header.startswith((b"GIF87a", b"GIF89a"))
        or (header.startswith(b"RIFF") and b"WEBP" in header[:16])
    )


def _looks_like_mp4(header: bytes) -> bool:
    return len(header) >= 12 and b"ftyp" in header[4:12]


def _normalize_asset_type(asset_type: str) -> str:
    normalized = asset_type.strip().lower()
    if normalized in {"photo", "thumbnail", "image"}:
        return "image"
    if normalized in {"video", "animated_gif", "mp4"}:
        return "video"
    if normalized in {"pdf", "document_pdf"}:
        return "pdf"
    if normalized in {"binary", "document", "attachment"}:
        return "binary"
    raise StagedAssetValidationError(f"Unsupported staged asset type: {asset_type}")


def validate_existing_asset(path: Path, *, asset_type: str) -> bool:
    """Return True when an existing asset matches the expected signature."""
    if not path.exists() or not path.is_file():
        return False
    if path.stat().st_size <= 0:
        return False
    with open(path, "rb") as handle:
        header = handle.read(64)
    normalized = _normalize_asset_type(asset_type)
    if normalized == "pdf":
        return _looks_like_pdf(header)
    if normalized == "image":
        return _looks_like_image(header)
    if normalized == "video":
        return _looks_like_mp4(header)
    return True


class StagedAssetPublisher:
    """Stage binary data under .thoth_system/tmp before atomic publish."""

    def __init__(self, config: Config, *, layout: PathLayout | None = None):
        self.layout = layout or build_path_layout(config)
        self.layout.ensure_directories()
        self.staging_root = self.layout.temp_root / "downloads"
        self.staging_root.mkdir(parents=True, exist_ok=True)

    def publish_bytes(
        self,
        destination: Path,
        content: bytes,
        *,
        asset_type: str,
    ) -> PublishedAsset:
        return self.publish_chunks(destination, [content], asset_type=asset_type)

    def publish_file(
        self,
        source: Path,
        destination: Path,
        *,
        asset_type: str,
        chunk_size: int = 1024 * 1024,
    ) -> PublishedAsset:
        if not source.exists() or not source.is_file():
            raise StagedAssetValidationError(
                f"Source asset does not exist or is not a file: {source}"
            )

        def read_chunks() -> Iterable[bytes]:
            with source.open("rb") as handle:
                while True:
                    chunk = handle.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

        return self.publish_chunks(
            destination,
            read_chunks(),
            asset_type=asset_type,
        )

    def publish_chunks(
        self,
        destination: Path,
        chunks: Iterable[bytes],
        *,
        asset_type: str,
    ) -> PublishedAsset:
        normalized = _normalize_asset_type(asset_type)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._temp_path_for(destination)
        size_bytes = 0
        sha256 = hashlib.sha256()
        header = bytearray()

        try:
            with open(temp_path, "wb") as handle:
                for chunk in chunks:
                    if not isinstance(chunk, (bytes, bytearray)):
                        raise StagedAssetValidationError(
                            "Staged asset chunks must be bytes"
                        )
                    if not chunk:
                        continue
                    handle.write(chunk)
                    size_bytes += len(chunk)
                    sha256.update(chunk)
                    if len(header) < 64:
                        header.extend(chunk[: 64 - len(header)])

            self._validate_header(bytes(header), size_bytes=size_bytes, asset_type=normalized)
            self._publish_staged_file(temp_path, destination)
            return PublishedAsset(
                path=destination,
                size_bytes=size_bytes,
                sha256=sha256.hexdigest(),
            )
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def _temp_path_for(self, destination: Path) -> Path:
        safe_name = destination.name or "asset"
        return self.staging_root / f"{uuid.uuid4().hex}.{safe_name}.part"

    def _publish_temp_path_for(self, destination: Path) -> Path:
        safe_name = destination.name or "asset"
        return destination.parent / f".thoth-publish-{uuid.uuid4().hex}.{safe_name}.part"

    def _publish_staged_file(self, staged_path: Path, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if self._same_filesystem(staged_path, destination.parent):
            os.replace(staged_path, destination)
            self._fsync_directory(destination.parent)
            return

        publish_temp_path = self._publish_temp_path_for(destination)
        try:
            with open(staged_path, "rb") as source_handle, open(publish_temp_path, "wb") as dest_handle:
                shutil.copyfileobj(source_handle, dest_handle, length=1024 * 1024)
                dest_handle.flush()
                os.fsync(dest_handle.fileno())
            os.replace(publish_temp_path, destination)
            self._fsync_directory(destination.parent)
        finally:
            if publish_temp_path.exists():
                publish_temp_path.unlink()

    def _same_filesystem(self, source_path: Path, target_dir: Path) -> bool:
        return os.stat(source_path).st_dev == os.stat(target_dir).st_dev

    def _fsync_directory(self, directory: Path) -> None:
        if not hasattr(os, "O_DIRECTORY"):
            return
        dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _validate_header(self, header: bytes, *, size_bytes: int, asset_type: str) -> None:
        if size_bytes <= 0:
            raise StagedAssetValidationError("Refusing to publish an empty asset")

        if asset_type == "pdf" and not _looks_like_pdf(header):
            raise StagedAssetValidationError("Refusing to publish a non-PDF as a PDF")
        if asset_type == "image" and not _looks_like_image(header):
            raise StagedAssetValidationError("Refusing to publish a non-image as image media")
        if asset_type == "video" and not _looks_like_mp4(header):
            raise StagedAssetValidationError("Refusing to publish a non-MP4 as video media")
