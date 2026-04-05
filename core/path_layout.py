"""Canonical vault and system path layout for Thoth."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


class ConfigLike(Protocol):
    def get(self, key: str, default: Any = None) -> Any:
        ...


def _resolve_required_path(
    raw_value: str | None,
    *,
    config_key: str,
    relative_to: Path | None = None,
) -> Path:
    if not raw_value or not str(raw_value).strip():
        raise ValueError(f"Required path not configured: {config_key}")

    path = Path(raw_value)
    if path.is_absolute() or relative_to is None:
        return path
    return relative_to / path


def _resolve_optional_path(
    raw_value: str | None,
    *,
    default_value: str,
    relative_to: Path,
) -> Path:
    value = raw_value or default_value
    path = Path(value)
    if path.is_absolute():
        return path
    return relative_to / path


@dataclass(frozen=True)
class PathLayout:
    vault_root: Path
    system_root: Path
    temp_root: Path
    auth_root: Path
    raw_root: Path
    library_root: Path
    wiki_root: Path
    digests_root: Path
    cache_root: Path
    llm_cache_root: Path
    database_path: Path
    download_tracking_file: Path
    realtime_bookmarks_file: Path
    log_file: Path

    def ensure_directories(self) -> None:
        for path in (
            self.vault_root,
            self.system_root,
            self.temp_root,
            self.auth_root,
            self.raw_root,
            self.library_root,
            self.wiki_root,
            self.digests_root,
            self.cache_root,
            self.llm_cache_root,
            self.database_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


def build_path_layout(config: ConfigLike, *, project_root: Path | None = None) -> PathLayout:
    base_root = project_root or Path.cwd()
    vault_root = _resolve_required_path(
        config.get("paths.vault_dir"),
        config_key="paths.vault_dir",
        relative_to=base_root,
    )
    system_root = _resolve_required_path(
        config.get("paths.system_dir"),
        config_key="paths.system_dir",
        relative_to=base_root,
    )
    temp_root = system_root / "tmp"
    auth_root = system_root / "auth"
    raw_root = _resolve_optional_path(
        config.get("paths.raw_dir"),
        default_value="raw",
        relative_to=vault_root,
    )
    library_root = _resolve_optional_path(
        config.get("paths.library_dir"),
        default_value="library",
        relative_to=vault_root,
    )
    wiki_root = _resolve_optional_path(
        config.get("paths.wiki_dir"),
        default_value="wiki",
        relative_to=vault_root.parent,
    )
    digests_root = _resolve_optional_path(
        config.get("paths.digests_dir"),
        default_value="_digests",
        relative_to=vault_root,
    )
    cache_root = _resolve_required_path(
        config.get("paths.cache_dir"),
        config_key="paths.cache_dir",
        relative_to=system_root,
    )
    llm_cache_root = system_root / "llm_cache"
    database_path = _resolve_required_path(
        config.get("database.path"),
        config_key="database.path",
        relative_to=system_root,
    )
    download_tracking_file = system_root / "download_tracking.json"
    realtime_bookmarks_file = system_root / "realtime_bookmarks.json"
    log_file = system_root / "thoth.log"

    return PathLayout(
        vault_root=vault_root,
        system_root=system_root,
        temp_root=temp_root,
        auth_root=auth_root,
        raw_root=raw_root,
        library_root=library_root,
        wiki_root=wiki_root,
        digests_root=digests_root,
        cache_root=cache_root,
        llm_cache_root=llm_cache_root,
        database_path=database_path,
        download_tracking_file=download_tracking_file,
        realtime_bookmarks_file=realtime_bookmarks_file,
        log_file=log_file,
    )
