"""Low-level derivative file storage: atomic writes and path helpers."""

from __future__ import annotations

import hashlib
import os
import re
import secrets
from pathlib import Path

from .models import TranscriptDerivative


class TranscriptStorageError(RuntimeError):
    """Raised when derivative storage fails."""


def _safe_slug(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")
    return text or "transcript"


def _atomic_write_text(path: Path, content: str) -> None:
    """Write ``content`` to ``path`` atomically using a unique temp file."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise TranscriptStorageError(
            f"failed to create directory for {path}: {exc}"
        ) from exc
    temp_path = path.with_name(
        f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    )
    write_error: OSError | None = None
    try:
        temp_path.write_text(content, encoding="utf-8")
        temp_path.replace(path)
    except OSError as exc:
        write_error = exc
        try:
            temp_path.unlink(missing_ok=True)
        except OSError as cleanup_exc:
            raise TranscriptStorageError(
                f"failed to write {path}: {write_error}; cleanup also failed"
            ) from cleanup_exc
        raise TranscriptStorageError(
            f"failed to write {path}: {write_error}"
        ) from write_error


def derivative_paths_for_artifact(
    artifact_id: str,
    cache_key: str,
    version: str,
    vault_root: Path,
) -> dict[str, Path]:
    """Return filesystem paths grouped under the full cache identity.

    The cache key (64-hex SHA-256) is part of the relative directory so that
    two output profiles for the same artifact never collide, while a single
    cache generation remains grouped under one identity.
    """
    slug = _safe_slug(artifact_id)
    return {
        "transcript": vault_root
        / "transcripts"
        / "processed"
        / cache_key
        / f"{slug}_{version}.md",
        "summary": vault_root
        / "transcripts"
        / "summaries"
        / cache_key
        / f"{slug}_{version}.md",
        "classification": vault_root
        / "transcripts"
        / "classifications"
        / cache_key
        / f"{slug}_{version}.json",
    }


def resolve_derivative_path(relative_path: str, vault_root: Path) -> Path:
    """Resolve a canonical relative derivative path under the vault.

    Rejects absolute paths, backslashes, dot/dot-dot segments, and paths that
    escape the vault via symlinks or traversal.
    """
    if not isinstance(relative_path, str) or not relative_path.strip():
        raise TranscriptStorageError("derivative path must be a non-blank string")
    if relative_path.startswith("/"):
        raise TranscriptStorageError(f"derivative path must be relative: {relative_path}")
    if "\\" in relative_path:
        raise TranscriptStorageError(
            f"derivative path must use POSIX separators: {relative_path}"
        )
    for segment in relative_path.split("/"):
        if segment in ("", ".", ".."):
            raise TranscriptStorageError(
                f"derivative path contains invalid segment: {relative_path}"
            )

    candidate = vault_root / relative_path
    if candidate.is_symlink():
        raise TranscriptStorageError(
            f"derivative path is a symlink: {relative_path}"
        )
    try:
        resolved = candidate.resolve()
        resolved.relative_to(vault_root.resolve())
    except ValueError as exc:
        raise TranscriptStorageError(
            f"derivative path escapes vault: {relative_path}"
        ) from exc
    return resolved


def verify_derivative_files_valid(
    derivatives: tuple[TranscriptDerivative, ...],
    vault_root: Path,
) -> None:
    """Validate that each derivative file is a regular file with exact bytes.

    Raises ``TranscriptStorageError`` if a file is missing, a symlink, not a
    regular file, not valid UTF-8, or its SHA-256 does not match the cached
    commitment.
    """
    for derivative in derivatives:
        path = resolve_derivative_path(derivative.path, vault_root)
        if path.is_symlink():
            raise TranscriptStorageError(
                f"derivative path is a symlink: {derivative.path}"
            )
        if not path.is_file():
            raise TranscriptStorageError(
                f"derivative file is missing or not a regular file: {derivative.path}"
            )
        try:
            raw_bytes = path.read_bytes()
        except OSError as exc:
            raise TranscriptStorageError(
                f"failed to read derivative file {derivative.path}: {exc}"
            ) from exc
        actual_hash = hashlib.sha256(raw_bytes).hexdigest()
        if actual_hash != derivative.content_sha256:
            raise TranscriptStorageError(
                f"derivative file hash mismatch for {derivative.path}: "
                f"expected {derivative.content_sha256}, got {actual_hash}"
            )
        try:
            decoded = raw_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise TranscriptStorageError(
                f"derivative file is not valid UTF-8: {derivative.path}"
            ) from exc
        if decoded != derivative.content:
            raise TranscriptStorageError(
                f"derivative file content mismatch for {derivative.path}"
            )
