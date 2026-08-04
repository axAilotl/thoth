"""
Wiki scaffold and maintenance primitives for Thoth.

This layer owns the concrete on-disk wiki structure: directory creation,
baseline file seeding, and append-only maintenance logging.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import os
import re

from .config import Config
from .time_utils import utc_now, utc_now_iso
from .wiki_contract import WikiContract, build_wiki_contract

WIKI_INDEX_TITLE = "Thoth Wiki"
WIKI_LOG_TITLE = "Wiki Maintenance Log"
_FRONTMATTER_END_MARKER = "\n---\n"
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class WikiScaffoldError(RuntimeError):
    """Raised when the wiki scaffold cannot be created or updated."""


def _now_iso() -> str:
    return utc_now_iso()


def _date_from_timestamp(value: str) -> str:
    normalized = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date().isoformat()
    except ValueError:
        if _ISO_DATE_RE.fullmatch(value.strip()):
            return value.strip()
        return utc_now().date().isoformat()


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write(content)
        if not content.endswith("\n"):
            handle.write("\n")
    os.replace(tmp_path, path)


def _strip_frontmatter(content: str) -> str:
    if not content.startswith("---\n"):
        return content
    end_marker = content.find(_FRONTMATTER_END_MARKER, 4)
    if end_marker == -1:
        return content
    return content[end_marker + len(_FRONTMATTER_END_MARKER) :]


def _render_index_content(contract: WikiContract, created_at: str, updated_at: str) -> str:
    return (
        f"# {WIKI_INDEX_TITLE}\n\n"
        "This directory stores the compiled wiki layer.\n\n"
        "## Structure\n\n"
        f"* [`index.md`]({contract.index_filename}) - Navigation root.\n"
        f"* [`log.md`]({contract.log_filename}) - Append-only maintenance log.\n"
        f"* [`pages/`]({contract.pages_dirname}/) - Compiled wiki pages.\n"
    )


def _render_log_content(contract: WikiContract, created_at: str, updated_at: str) -> str:
    return f"# {WIKI_LOG_TITLE}\n\n## {_date_from_timestamp(created_at)}\n\n* **Initialization**: Created wiki scaffold.\n"


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
    entry_date = _date_from_timestamp(entry_timestamp)
    entry = f"\n## {entry_date}\n\n* **Update**: {content}\n"

    try:
        if contract.log_path.exists():
            current = contract.log_path.read_text(encoding="utf-8")
            stripped = _strip_frontmatter(current)
            if stripped != current:
                _atomic_write_text(contract.log_path, stripped)
        with open(contract.log_path, "a", encoding="utf-8") as handle:
            handle.write(entry)
    except Exception as exc:
        raise WikiScaffoldError(f"Failed to append wiki log entry: {exc}") from exc

    return contract.log_path
