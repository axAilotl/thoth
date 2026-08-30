"""Canonical vault and system path layout for Thoth."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol

from .content_roots import (
    ContentRoot,
    ContentRootError,
    ContentRootMode,
    ContentRootPolicy,
    build_content_root_policy,
    content_roots_from_config,
)
from .content_root_adapters import default_content_carriers


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


def _operational_paths_for_layout(
    system_root: Path,
    cache_root: Path,
    database_path: Path,
    download_tracking_file: Path,
    realtime_bookmarks_file: Path,
    log_file: Path,
) -> tuple[Path, ...]:
    return (
        system_root,
        cache_root,
        database_path.parent,
        download_tracking_file.parent,
        realtime_bookmarks_file.parent,
        log_file.parent,
    )


def _default_content_roots(
    vault_root: Path,
    wiki_root: Path,
) -> tuple[ContentRoot, ...]:
    """Construct explicit default behavior-owned roots from legacy path keys.

    When the operator has not configured ``content_roots``, the legacy layout
    is interpreted as two roots: the vault is the managed inbox and the wiki
    directory is projection output. This is explicit default construction, not
    a silent fallback, and is still validated by the policy.
    """
    return (
        ContentRoot(
            root_id="vault",
            mode=ContentRootMode.MANAGED_INBOX,
            adapter_kind="filesystem",
            base_path=vault_root,
        ),
        ContentRoot(
            root_id="wiki",
            mode=ContentRootMode.PROJECTION_OUTPUT,
            adapter_kind="filesystem",
            base_path=wiki_root,
        ),
    )


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
    content_roots: tuple[ContentRoot, ...] = ()
    content_root_policy: ContentRootPolicy | None = None

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

    def content_roots_by_mode(self, *modes: ContentRootMode) -> tuple[ContentRoot, ...]:
        if self.content_root_policy is None:
            return ()
        return self.content_root_policy.roots_by_mode(*modes)


def _validate_scope_paths_in_content_roots(
    policy: ContentRootPolicy,
    *,
    vault_root: Path,
    raw_root: Path,
    library_root: Path,
) -> None:
    """Fail closed when legacy scope paths lack the required ownership mode.

    Vault and library are readable source scopes, so they may be watch-only or
    managed. Raw connector capture is mutable and must be managed-inbox.
    """
    readable_modes = (ContentRootMode.WATCH_ONLY, ContentRootMode.MANAGED_INBOX)
    for scope_name, scope_path, modes in (
        ("vault", vault_root, readable_modes),
        ("raw", raw_root, (ContentRootMode.MANAGED_INBOX,)),
        ("library", library_root, readable_modes),
    ):
        if policy.root_containing(scope_path, *modes) is None:
            mode_names = ", ".join(mode.value for mode in modes)
            raise ContentRootError(
                f"{scope_name} scope path {scope_path} is not inside a "
                f"content root with required mode ({mode_names})"
            )


def _build_content_roots(
    config: ConfigLike,
    *,
    project_root: Path,
    vault_root: Path,
    wiki_root: Path,
    system_root: Path,
    cache_root: Path,
    database_path: Path,
    download_tracking_file: Path,
    realtime_bookmarks_file: Path,
    log_file: Path,
) -> tuple[ContentRoot, ...]:
    raw_roots = config.get("content_roots")
    if raw_roots:
        roots = content_roots_from_config(
            raw_roots,
            relative_to=project_root,
        )
    else:
        roots = _default_content_roots(vault_root, wiki_root)

    operational_paths = _operational_paths_for_layout(
        system_root=system_root,
        cache_root=cache_root,
        database_path=database_path,
        download_tracking_file=download_tracking_file,
        realtime_bookmarks_file=realtime_bookmarks_file,
        log_file=log_file,
    )
    policy = build_content_root_policy(
        roots,
        default_content_carriers(),
        operational_paths=operational_paths,
    )
    return policy.roots, policy


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

    content_roots, content_root_policy = _build_content_roots(
        config,
        project_root=base_root,
        vault_root=vault_root,
        wiki_root=wiki_root,
        system_root=system_root,
        cache_root=cache_root,
        database_path=database_path,
        download_tracking_file=download_tracking_file,
        realtime_bookmarks_file=realtime_bookmarks_file,
        log_file=log_file,
    )

    if content_root_policy is not None:
        _validate_scope_paths_in_content_roots(
            content_root_policy,
            vault_root=vault_root,
            raw_root=raw_root,
            library_root=library_root,
        )

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
        content_roots=content_roots,
        content_root_policy=content_root_policy,
    )
