"""Obsidian vault filesystem scanning and classification.

Walks a vault segment directory into a deterministic layout: markdown
notes, binary attachment files, and git repository directories. Hidden
Obsidian/VCS internals (``.obsidian/``, ``.git/``, ``.trash/``) are never
descended into. A directory containing a ``.git`` entry is a repository:
it becomes a source record at import time and its working tree is not
blob-dumped.

Paths in every result are vault-relative POSIX strings — they double as
origin native IDs, so they must be stable across runs of the importer.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path

_SKIP_DIRS = {".git", ".obsidian", ".trash", ".smart-env", ".claude"}


class VaultScanError(RuntimeError):
    """Raised when the vault cannot be scanned safely."""


@dataclass(frozen=True)
class VaultFile:
    """One file inside the vault segment."""

    relpath: str  # vault-relative POSIX path
    abspath: Path
    size_bytes: int
    sha256: str

    @property
    def is_markdown(self) -> bool:
        return self.relpath.lower().endswith(".md")

    @property
    def stem(self) -> str:
        return Path(self.relpath).stem


@dataclass
class VaultLayout:
    """Scan result for one vault tree."""

    root: Path
    notes: list[VaultFile] = field(default_factory=list)
    binaries: list[VaultFile] = field(default_factory=list)
    repos: list[str] = field(default_factory=list)  # relpaths of git repo dirs


def hash_file(path: Path, *, chunk_size: int = 1 << 20) -> str:
    """Streaming SHA-256 of one file (never loads it whole)."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def scan_vault(root: str | Path) -> VaultLayout:
    """Scan one vault tree into notes / binaries / git repos.

    Results are sorted by relative path so imports are deterministic.
    Symlinks are not followed (fail closed against escaping the vault).
    """
    root = Path(root)
    if not root.is_dir():
        raise VaultScanError(f"vault root is not a directory: {root}")
    layout = VaultLayout(root=root)

    def walk(directory: Path) -> None:
        for entry in sorted(directory.iterdir(), key=lambda e: e.name):
            if entry.is_symlink():
                raise VaultScanError(f"refusing to follow symlink in vault: {entry}")
            if entry.is_dir():
                if entry.name in _SKIP_DIRS:
                    continue
                if (entry / ".git").exists():
                    layout.repos.append(entry.relative_to(root).as_posix())
                    continue
                walk(entry)
                continue
            if entry.name.startswith(".") or not entry.is_file():
                continue
            relpath = entry.relative_to(root).as_posix()
            record = VaultFile(
                relpath=relpath,
                abspath=entry,
                size_bytes=entry.stat().st_size,
                sha256=hash_file(entry),
            )
            if record.is_markdown:
                layout.notes.append(record)
            else:
                layout.binaries.append(record)

    walk(root)
    layout.notes.sort(key=lambda f: f.relpath)
    layout.binaries.sort(key=lambda f: f.relpath)
    layout.repos.sort()
    return layout
