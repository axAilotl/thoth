"""
Wiki scaffold and maintenance primitives for Thoth.

This layer owns the concrete on-disk wiki structure: directory creation,
baseline file seeding, and append-only maintenance logging.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import os

from .config import Config
from .wiki_contract import WikiContract, build_wiki_contract

WIKI_INDEX_TITLE = "Thoth Wiki"
WIKI_LOG_TITLE = "Wiki Maintenance Log"


class WikiScaffoldError(RuntimeError):
    """Raised when the wiki scaffold cannot be created or updated."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write(content)
        if not content.endswith("\n"):
            handle.write("\n")
    os.replace(tmp_path, path)


def _render_index_content(contract: WikiContract, created_at: str, updated_at: str) -> str:
    return (
        "---\n"
        "thoth_type: wiki_index\n"
        f"title: {WIKI_INDEX_TITLE}\n"
        f"root: {contract.root}\n"
        f"created_at: {created_at}\n"
        f"updated_at: {updated_at}\n"
        "---\n\n"
        f"# {WIKI_INDEX_TITLE}\n\n"
        "This directory stores the compiled wiki layer.\n\n"
        "- `index.md` is the navigation root.\n"
        "- `log.md` is the append-only maintenance log.\n"
        "- `pages/` contains compiled wiki pages.\n"
    )


def _render_log_content(contract: WikiContract, created_at: str, updated_at: str) -> str:
    return (
        "---\n"
        "thoth_type: wiki_log\n"
        f"title: {WIKI_LOG_TITLE}\n"
        f"root: {contract.root}\n"
        "append_only: true\n"
        f"created_at: {created_at}\n"
        f"updated_at: {updated_at}\n"
        "---\n\n"
        f"# {WIKI_LOG_TITLE}\n"
    )


@dataclass(frozen=True)
class WikiScaffold:
    """Resolved wiki scaffold for a configured Thoth runtime."""

    contract: WikiContract
    seeded_paths: tuple[Path, ...] = ()

    @property
    def root(self) -> Path:
        return self.contract.root

    @property
    def index_path(self) -> Path:
        return self.contract.index_path

    @property
    def log_path(self) -> Path:
        return self.contract.log_path

    @property
    def pages_dir(self) -> Path:
        return self.contract.pages_dir


def build_wiki_scaffold(config: Config, *, project_root: Path | None = None) -> WikiScaffold:
    """Resolve the wiki scaffold without mutating the filesystem."""
    contract = build_wiki_contract(config, project_root=project_root)
    return WikiScaffold(contract=contract)


def ensure_wiki_scaffold(
    config: Config,
    *,
    project_root: Path | None = None,
) -> WikiScaffold:
    """Create the wiki directory and baseline files if they do not exist."""
    scaffold = build_wiki_scaffold(config, project_root=project_root)
    contract = scaffold.contract
    created_paths: list[Path] = []
    created_at = _now_iso()

    try:
        contract.root.mkdir(parents=True, exist_ok=True)
        contract.pages_dir.mkdir(parents=True, exist_ok=True)

        if not contract.index_path.exists():
            _atomic_write_text(
                contract.index_path,
                _render_index_content(contract, created_at, created_at),
            )
            created_paths.append(contract.index_path)

        if not contract.log_path.exists():
            _atomic_write_text(
                contract.log_path,
                _render_log_content(contract, created_at, created_at),
            )
            created_paths.append(contract.log_path)

        return WikiScaffold(contract=contract, seeded_paths=tuple(created_paths))
    except Exception as exc:
        raise WikiScaffoldError(f"Failed to ensure wiki scaffold: {exc}") from exc


def append_wiki_log_entry(
    scaffold: WikiScaffold | WikiContract,
    message: str,
    *,
    timestamp: str | None = None,
) -> Path:
    """Append a maintenance event to the wiki log."""
    content = message.strip()
    if not content:
        raise ValueError("Wiki log message cannot be empty")

    contract = scaffold.contract if isinstance(scaffold, WikiScaffold) else scaffold
    if not contract.root.exists():
        raise WikiScaffoldError(f"Wiki root does not exist: {contract.root}")

    contract.pages_dir.mkdir(parents=True, exist_ok=True)
    if not contract.log_path.exists():
        _atomic_write_text(
            contract.log_path,
            _render_log_content(contract, _now_iso(), _now_iso()),
        )

    entry_timestamp = timestamp or _now_iso()
    entry = f"\n## {entry_timestamp}\n\n- {content}\n"

    try:
        with open(contract.log_path, "a", encoding="utf-8") as handle:
            handle.write(entry)
    except Exception as exc:
        raise WikiScaffoldError(f"Failed to append wiki log entry: {exc}") from exc

    return contract.log_path
