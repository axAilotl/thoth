"""
Web Clipper source contract and watched layout.

This module defines where Web Clipper content is expected to live inside the
vault and how the collector should classify note files versus attachments.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Sequence

from core.path_layout import PathLayout, build_path_layout

DEFAULT_NOTE_EXTENSIONS = (".md", ".markdown")
DEFAULT_ATTACHMENT_EXTENSIONS = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".pdf",
    ".mp4",
    ".mov",
    ".m4a",
    ".mp3",
    ".wav",
)


class ConfigLike(Protocol):
    def get(self, key: str, default: Any = None) -> Any:
        ...


def _resolve_relative_dir(raw_value: str, *, base_dir: Path, config_key: str) -> Path:
    if not raw_value or not str(raw_value).strip():
        raise ValueError(f"Required path not configured: {config_key}")

    path = Path(raw_value)
    if path.is_absolute():
        return path
    return base_dir / path


def _resolve_dir_list(
    raw_values: Sequence[str] | None,
    *,
    base_dir: Path,
    config_key: str,
) -> tuple[Path, ...]:
    if not raw_values:
        raise ValueError(f"Required list not configured: {config_key}")

    resolved: list[Path] = []
    seen: set[Path] = set()
    for raw_value in raw_values:
        resolved_path = _resolve_relative_dir(
            raw_value,
            base_dir=base_dir,
            config_key=config_key,
        )
        if resolved_path in seen:
            continue
        seen.add(resolved_path)
        resolved.append(resolved_path)
    return tuple(resolved)


def _ensure_within_source_root(path: Path, *, layout: PathLayout, config_key: str) -> None:
    try:
        path.relative_to(layout.raw_root)
    except ValueError as exc:
        raise ValueError(
            f"{config_key} must stay inside the raw source root: {path}"
        ) from exc


@dataclass(frozen=True)
class WebClipperSourceContract:
    """Canonical source contract for Obsidian Web Clipper ingest."""

    note_dirs: tuple[Path, ...]
    attachment_dirs: tuple[Path, ...]
    note_extensions: tuple[str, ...] = DEFAULT_NOTE_EXTENSIONS
    attachment_extensions: tuple[str, ...] = DEFAULT_ATTACHMENT_EXTENSIONS

    def __post_init__(self):
        if not self.note_dirs:
            raise ValueError("Web Clipper note_dirs must not be empty")
        if not self.attachment_dirs:
            raise ValueError("Web Clipper attachment_dirs must not be empty")
        if set(self.note_dirs).intersection(self.attachment_dirs):
            raise ValueError("Web Clipper note_dirs and attachment_dirs must not overlap")

    @property
    def watch_dirs(self) -> tuple[Path, ...]:
        return self.note_dirs + self.attachment_dirs

    def is_note_path(self, path: Path) -> bool:
        return path.suffix.lower() in self.note_extensions

    def is_attachment_path(self, path: Path) -> bool:
        return path.suffix.lower() in self.attachment_extensions

    def classify_path(self, path: Path) -> str:
        if self.is_note_path(path):
            return "note"
        if self.is_attachment_path(path):
            return "attachment"
        return "ignored"


def build_web_clipper_contract(
    config: ConfigLike,
    *,
    layout: PathLayout | None = None,
) -> WebClipperSourceContract:
    """Build the explicit Web Clipper source contract from config."""

    path_layout = layout or build_path_layout(config)
    web_clipper_config = config.get("sources.web_clipper", {}) or {}

    note_dirs = _resolve_dir_list(
        web_clipper_config.get("note_dirs"),
        base_dir=path_layout.raw_root,
        config_key="sources.web_clipper.note_dirs",
    )
    attachment_dirs = _resolve_dir_list(
        web_clipper_config.get("attachment_dirs"),
        base_dir=path_layout.raw_root,
        config_key="sources.web_clipper.attachment_dirs",
    )

    for dir_path in note_dirs + attachment_dirs:
        _ensure_within_source_root(
            dir_path,
            layout=path_layout,
            config_key="sources.web_clipper",
        )

    note_extensions = tuple(
        str(item).lower()
        for item in web_clipper_config.get("note_extensions", DEFAULT_NOTE_EXTENSIONS)
    )
    attachment_extensions = tuple(
        str(item).lower()
        for item in web_clipper_config.get(
            "attachment_extensions", DEFAULT_ATTACHMENT_EXTENSIONS
        )
    )

    return WebClipperSourceContract(
        note_dirs=note_dirs,
        attachment_dirs=attachment_dirs,
        note_extensions=note_extensions,
        attachment_extensions=attachment_extensions,
    )
