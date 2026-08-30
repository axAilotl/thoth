"""Content carriers for local filesystem and Obsidian vault layouts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping

from .content_roots import (
    ArtifactHandle,
    ContentCarrier,
    ContentRoot,
    ContentRootError,
    ContentRootMode,
)


class ContentAdapterError(ContentRootError, RuntimeError):
    """Raised when a carrier operation is refused or unsafe."""


_WRITE_MODES = {ContentRootMode.MANAGED_INBOX, ContentRootMode.PROJECTION_OUTPUT}
_DELETE_MODES = {ContentRootMode.MANAGED_INBOX, ContentRootMode.PROJECTION_OUTPUT}


def _safe_relpath(value: str) -> str:
    """Normalize a relative POSIX path and reject traversal attempts."""
    text = str(value or "").strip()
    if not text:
        raise ContentAdapterError("artifact relpath cannot be empty")
    if text.startswith("/"):
        raise ContentAdapterError(f"artifact relpath must be relative: {value!r}")
    path = Path(text)
    if any(part == ".." for part in path.parts):
        raise ContentAdapterError(
            f"artifact relpath cannot traverse upward: {value!r}"
        )
    return path.as_posix()


def _hash_file(path: Path, *, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _hash_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class FilesystemContentAdapter:
    """Local-filesystem carrier that enforces content-root policy and safety."""

    name = "filesystem"

    def supports_mode(self, mode: ContentRootMode) -> bool:
        return mode in {
            ContentRootMode.WATCH_ONLY,
            ContentRootMode.MANAGED_INBOX,
            ContentRootMode.PROJECTION_OUTPUT,
            ContentRootMode.PROTECTED,
            ContentRootMode.EXTERNAL,
        }

    def _root_path(self, root: ContentRoot) -> Path:
        return root.base_path.resolve()

    def _resolve_locator(self, root: ContentRoot, relpath: str) -> Path:
        root_path = self._root_path(root)
        safe = _safe_relpath(relpath)
        target = (root_path / safe).resolve()
        try:
            target.relative_to(root_path)
        except ValueError as exc:
            raise ContentAdapterError(
                f"artifact path escapes content root {root.root_id!r}: {relpath!r}"
            ) from exc
        return target

    def _check_symlink(self, path: Path) -> None:
        if path.is_symlink() or any(parent.is_symlink() for parent in path.parents):
            raise ContentAdapterError(
                f"refusing to follow symlink inside content root: {path}"
            )

    def list_artifacts(
        self,
        root: ContentRoot,
        *,
        prefix: str | None = None,
    ) -> Iterator[ArtifactHandle]:
        root_path = self._root_path(root)
        if not root_path.is_dir():
            return

        search_path = root_path
        if prefix:
            safe_prefix = _safe_relpath(prefix)
            search_path = (root_path / safe_prefix).resolve()
            try:
                search_path.relative_to(root_path)
            except ValueError as exc:
                raise ContentAdapterError(
                    f"prefix escapes content root {root.root_id!r}: {prefix!r}"
                ) from exc
            if not search_path.is_dir():
                return

        for path in sorted(search_path.rglob("*"), key=lambda p: str(p)):
            if not path.is_file():
                continue
            self._check_symlink(path)
            try:
                relpath = path.relative_to(root_path).as_posix()
            except ValueError:
                continue
            stat = path.stat()
            digest = _hash_file(path)
            yield ArtifactHandle(
                root_id=root.root_id,
                relpath=relpath,
                locator=path,
                digest=digest,
                size_bytes=stat.st_size,
            )

    def read_artifact(self, handle: ArtifactHandle) -> bytes:
        path = self._locator_to_path(handle)
        return path.read_bytes()

    def write_artifact(
        self,
        root: ContentRoot,
        relpath: str,
        content: bytes,
    ) -> ArtifactHandle:
        if root.mode not in _WRITE_MODES:
            raise ContentAdapterError(
                f"refusing to write to {root.mode.value} content root "
                f"{root.root_id!r}"
            )
        target = self._resolve_locator(root, relpath)
        self._check_symlink(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_path = target.with_name(f".{target.name}.tmp")
        try:
            temp_path.write_bytes(content)
            temp_path.replace(target)
        finally:
            if temp_path.exists():
                temp_path.unlink()

        stat = target.stat()
        digest = _hash_bytes(content)
        return ArtifactHandle(
            root_id=root.root_id,
            relpath=_safe_relpath(relpath),
            locator=target,
            digest=digest,
            size_bytes=stat.st_size,
        )

    def remove_artifact(
        self,
        root: ContentRoot,
        handle: ArtifactHandle,
    ) -> None:
        if root.mode not in _DELETE_MODES:
            raise ContentAdapterError(
                f"refusing to delete from {root.mode.value} content root "
                f"{root.root_id!r}"
            )
        if handle.root_id != root.root_id:
            raise ContentAdapterError(
                f"artifact handle root_id {handle.root_id!r} does not match "
                f"root {root.root_id!r}"
            )
        path = self._locator_to_path(handle)
        self._check_symlink(path)
        path.unlink(missing_ok=True)

    def _locator_to_path(self, handle: ArtifactHandle) -> Path:
        if isinstance(handle.locator, Path):
            return handle.locator
        raise ContentAdapterError(
            f"filesystem adapter received non-path locator: {handle.locator!r}"
        )


class ObsidianContentAdapter:
    """Obsidian vault carrier: filesystem carrier with vault conventions.

    Hidden Obsidian internals (``.obsidian``, ``.trash``, ``.smart-env``,
    ``.claude``) and git directories are skipped. Symlinks are refused.
    """

    name = "obsidian"
    _SKIP_DIRS = {".obsidian", ".trash", ".smart-env", ".claude", ".git"}

    def __init__(self) -> None:
        self._filesystem = FilesystemContentAdapter()

    def supports_mode(self, mode: ContentRootMode) -> bool:
        return self._filesystem.supports_mode(mode)

    def list_artifacts(
        self,
        root: ContentRoot,
        *,
        prefix: str | None = None,
    ) -> Iterator[ArtifactHandle]:
        for handle in self._filesystem.list_artifacts(root, prefix=prefix):
            if self._skip_relpath(handle.relpath):
                continue
            yield ArtifactHandle(
                root_id=handle.root_id,
                relpath=handle.relpath,
                locator=handle.locator,
                digest=handle.digest,
                size_bytes=handle.size_bytes,
            )

    def read_artifact(self, handle: ArtifactHandle) -> bytes:
        self._guard_vault_path(handle.relpath)
        return self._filesystem.read_artifact(handle)

    def write_artifact(
        self,
        root: ContentRoot,
        relpath: str,
        content: bytes,
    ) -> ArtifactHandle:
        self._guard_vault_path(relpath)
        return self._filesystem.write_artifact(root, relpath, content)

    def remove_artifact(
        self,
        root: ContentRoot,
        handle: ArtifactHandle,
    ) -> None:
        self._guard_vault_path(handle.relpath)
        self._filesystem.remove_artifact(root, handle)

    def _guard_vault_path(self, relpath: str) -> None:
        if self._skip_relpath(relpath):
            raise ContentAdapterError(
                f"refusing to operate on Obsidian internal path: {relpath!r}"
            )

    def _skip_relpath(self, relpath: str) -> bool:
        parts = Path(relpath).parts
        return any(part in self._SKIP_DIRS for part in parts)


@dataclass
class InMemoryContentAdapter:
    """Test-only carrier backed by a dictionary.

    Deterministic, fast, and does not touch the filesystem. Useful for unit
    tests that must prove policy behavior without I/O.
    """

    name = "memory"
    _storage: dict[str, bytes] = field(default_factory=dict)

    def supports_mode(self, mode: ContentRootMode) -> bool:
        return mode in {
            ContentRootMode.WATCH_ONLY,
            ContentRootMode.MANAGED_INBOX,
            ContentRootMode.PROJECTION_OUTPUT,
            ContentRootMode.PROTECTED,
            ContentRootMode.EXTERNAL,
        }

    def list_artifacts(
        self,
        root: ContentRoot,
        *,
        prefix: str | None = None,
    ) -> Iterator[ArtifactHandle]:
        prefix_key = f"{root.root_id}:/"
        for key in sorted(self._storage):
            if not key.startswith(prefix_key):
                continue
            relpath = key[len(prefix_key) :]
            if prefix and not relpath.startswith(_safe_relpath(prefix)):
                continue
            content = self._storage[key]
            yield ArtifactHandle(
                root_id=root.root_id,
                relpath=relpath,
                locator=key,
                digest=_hash_bytes(content),
                size_bytes=len(content),
            )

    def read_artifact(self, handle: ArtifactHandle) -> bytes:
        key = self._locator_to_key(handle)
        if key not in self._storage:
            raise ContentAdapterError(f"artifact not found: {key!r}")
        return self._storage[key]

    def write_artifact(
        self,
        root: ContentRoot,
        relpath: str,
        content: bytes,
    ) -> ArtifactHandle:
        if root.mode not in _WRITE_MODES:
            raise ContentAdapterError(
                f"refusing to write to {root.mode.value} content root "
                f"{root.root_id!r}"
            )
        safe = _safe_relpath(relpath)
        key = f"{root.root_id}:/{safe}"
        self._storage[key] = content
        return ArtifactHandle(
            root_id=root.root_id,
            relpath=safe,
            locator=key,
            digest=_hash_bytes(content),
            size_bytes=len(content),
        )

    def remove_artifact(
        self,
        root: ContentRoot,
        handle: ArtifactHandle,
    ) -> None:
        if root.mode not in _DELETE_MODES:
            raise ContentAdapterError(
                f"refusing to delete from {root.mode.value} content root "
                f"{root.root_id!r}"
            )
        if handle.root_id != root.root_id:
            raise ContentAdapterError(
                f"artifact handle root_id {handle.root_id!r} does not match "
                f"root {root.root_id!r}"
            )
        key = self._locator_to_key(handle)
        self._storage.pop(key, None)

    def _locator_to_key(self, handle: ArtifactHandle) -> str:
        if isinstance(handle.locator, str):
            return handle.locator
        raise ContentAdapterError(
            f"memory adapter received non-string locator: {handle.locator!r}"
        )


def default_content_carriers() -> Mapping[str, ContentCarrier]:
    """Return the built-in carrier registry.

    Test-only adapters are intentionally excluded from production registration;
    they must be supplied directly by tests when needed.
    """
    return {
        FilesystemContentAdapter.name: FilesystemContentAdapter(),
        ObsidianContentAdapter.name: ObsidianContentAdapter(),
    }


__all__ = [
    "ContentAdapterError",
    "FilesystemContentAdapter",
    "ObsidianContentAdapter",
    "InMemoryContentAdapter",
    "default_content_carriers",
]
