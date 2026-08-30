"""Behavior-owned content roots and filesystem safety policy for Thoth.

Content roots represent locations by behavior (mode) and adapter rather than
product name. They replace scattered absolute-path assumptions in core logic
with an artifact/carrier interface where paths are locators, never identity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol, runtime_checkable


class ContentRootMode(str, Enum):
    """Ownership mode governing what Thoth may do inside a content root."""

    WATCH_ONLY = "watch_only"
    MANAGED_INBOX = "managed_inbox"
    PROJECTION_OUTPUT = "projection_output"
    PROTECTED = "protected"
    EXTERNAL = "external"


class ContentRootError(ValueError):
    """Raised when a content root or policy is invalid."""


@dataclass(frozen=True)
class ContentRoot:
    """A single validated content root.

    ``base_path`` is the adapter-specific root directory. ``mode`` determines
    which operations are permitted. ``adapter_kind`` selects the carrier.
    """

    root_id: str
    mode: ContentRootMode
    adapter_kind: str
    base_path: Path
    tags: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.root_id or not str(self.root_id).strip():
            raise ContentRootError("content root id cannot be empty")
        if not isinstance(self.mode, ContentRootMode):
            raise ContentRootError(
                f"invalid content root mode for {self.root_id!r}: {self.mode!r}"
            )
        if not self.adapter_kind or not str(self.adapter_kind).strip():
            raise ContentRootError(
                f"content root adapter cannot be empty for {self.root_id!r}"
            )


@dataclass(frozen=True)
class ArtifactHandle:
    """Adapter-neutral reference to one artifact inside a content root.

    ``relpath`` is the POSIX locator used as the artifact path in downstream
    records. ``locator`` is adapter-specific opaque data (e.g., an absolute
    filesystem path) and must not be used as identity.
    """

    root_id: str
    relpath: str
    locator: Any
    digest: str | None = None
    size_bytes: int | None = None


@runtime_checkable
class ContentCarrier(Protocol):
    """Carrier interface for listing, reading, and writing artifacts."""

    def supports_mode(self, mode: ContentRootMode) -> bool:
        """Return True when this adapter can operate under ``mode``."""
        ...

    def list_artifacts(
        self,
        root: ContentRoot,
        *,
        prefix: str | None = None,
    ) -> Iterator[ArtifactHandle]:
        """Yield artifacts under ``root`` matching the optional prefix locator."""
        ...

    def read_artifact(self, handle: ArtifactHandle) -> bytes:
        """Return the artifact bytes referenced by ``handle``."""
        ...

    def write_artifact(
        self,
        root: ContentRoot,
        relpath: str,
        content: bytes,
    ) -> ArtifactHandle:
        """Write ``content`` to ``relpath`` under ``root`` and return a handle."""
        ...

    def remove_artifact(
        self,
        root: ContentRoot,
        handle: ArtifactHandle,
    ) -> None:
        """Remove the artifact referenced by ``handle``."""
        ...


@dataclass(frozen=True)
class ContentRootPolicy:
    """Validated collection of content roots and their carriers."""

    roots: tuple[ContentRoot, ...]
    carriers: Mapping[str, ContentCarrier]

    def root_by_id(self, root_id: str) -> ContentRoot:
        for root in self.roots:
            if root.root_id == root_id:
                return root
        raise ContentRootError(f"content root not found: {root_id!r}")

    def roots_by_mode(self, *modes: ContentRootMode) -> tuple[ContentRoot, ...]:
        return tuple(root for root in self.roots if root.mode in modes)

    def carrier_for(self, root: ContentRoot) -> ContentCarrier:
        carrier = self.carriers.get(root.adapter_kind)
        if carrier is None:
            raise ContentRootError(
                f"no carrier registered for adapter {root.adapter_kind!r}"
            )
        return carrier


def _is_subpath(candidate: Path, parent: Path) -> bool:
    """Return True when ``candidate`` is equal to or inside ``parent``.

    Both paths are resolved before comparison. Symlinks are not followed by
    design; callers resolve them only when the adapter has already vetted them.
    """
    try:
        candidate.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _paths_overlap(a: Path, b: Path) -> bool:
    """Return True when two roots share any filesystem space."""
    a_resolved = a.resolve()
    b_resolved = b.resolve()
    return _is_subpath(a_resolved, b_resolved) or _is_subpath(b_resolved, a_resolved)


def validate_content_roots(
    roots: Iterable[ContentRoot],
    *,
    operational_paths: Iterable[Path] | None = None,
) -> None:
    """Validate a set of content roots; raise ContentRootError on any violation.

    Checks:
    - root ids are unique
    - base paths exist or can be created (as directories)
    - no two roots overlap
    - operational paths (state, credentials, queues, caches) are not inside any root
    """
    seen_ids: set[str] = set()
    resolved_roots: list[tuple[ContentRoot, Path]] = []

    for root in roots:
        root_id = root.root_id.strip()
        if root_id in seen_ids:
            raise ContentRootError(f"duplicate content root id: {root_id!r}")
        seen_ids.add(root_id)

        base = root.base_path
        if not base.is_absolute():
            raise ContentRootError(
                f"content root {root_id!r} base path must be absolute: {base}"
            )

        try:
            base.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ContentRootError(
                f"content root {root_id!r} cannot be created: {base}"
            ) from exc

        if not base.is_dir():
            raise ContentRootError(
                f"content root {root_id!r} is not a directory: {base}"
            )

        resolved = base.resolve()
        for previous_root, previous_resolved in resolved_roots:
            if _paths_overlap(resolved, previous_resolved):
                raise ContentRootError(
                    f"content roots {previous_root.root_id!r} and {root_id!r} overlap"
                )
        resolved_roots.append((root, resolved))

    if operational_paths:
        for op_path in operational_paths:
            if not op_path.is_absolute():
                continue
            op_resolved = op_path.resolve()
            for root, root_resolved in resolved_roots:
                if _paths_overlap(op_resolved, root_resolved):
                    raise ContentRootError(
                        f"operational path {op_path} overlaps content root "
                        f"{root.root_id!r} ({root_resolved})"
                    )


def build_content_root_policy(
    roots: Iterable[ContentRoot],
    carriers: Mapping[str, ContentCarrier],
    *,
    operational_paths: Iterable[Path] | None = None,
) -> ContentRootPolicy:
    """Validate roots and pair them with carriers."""
    root_tuple = tuple(roots)
    validate_content_roots(root_tuple, operational_paths=operational_paths)
    for root in root_tuple:
        carrier = carriers.get(root.adapter_kind)
        if carrier is None:
            raise ContentRootError(
                f"no carrier registered for adapter {root.adapter_kind!r}"
            )
        if not carrier.supports_mode(root.mode):
            raise ContentRootError(
                f"adapter {root.adapter_kind!r} does not support mode "
                f"{root.mode.value!r} for root {root.root_id!r}"
            )
    return ContentRootPolicy(roots=root_tuple, carriers=dict(carriers))


def _parse_content_root_mode(value: str) -> ContentRootMode:
    try:
        return ContentRootMode(value)
    except ValueError as exc:
        raise ContentRootError(f"unknown content root mode: {value!r}") from exc


def content_root_from_config(
    item: Mapping[str, Any],
    *,
    relative_to: Path | None = None,
) -> ContentRoot:
    """Build a ContentRoot from a config item (e.g., JSON/YAML)."""
    root_id = str(item.get("id") or "").strip()
    mode_value = item.get("mode")
    adapter_kind = str(item.get("adapter") or "").strip()
    raw_path = item.get("path")

    mode = (
        _parse_content_root_mode(mode_value)
        if isinstance(mode_value, ContentRootMode)
        else _parse_content_root_mode(str(mode_value))
    )

    if not raw_path or not str(raw_path).strip():
        raise ContentRootError(f"content root {root_id!r} missing path")

    path = Path(str(raw_path))
    if not path.is_absolute():
        if relative_to is None:
            raise ContentRootError(
                f"content root {root_id!r} path must be absolute or relative_to "
                f"must be provided: {path}"
            )
        path = relative_to / path

    tags = tuple(
        str(tag).strip()
        for tag in item.get("tags", [])
        if str(tag).strip()
    )
    metadata = item.get("metadata") or {}
    if not isinstance(metadata, Mapping):
        metadata = {}

    return ContentRoot(
        root_id=root_id,
        mode=mode,
        adapter_kind=adapter_kind,
        base_path=path,
        tags=tags,
        metadata=dict(metadata),
    )


def content_roots_from_config(
    items: Iterable[Mapping[str, Any]],
    *,
    relative_to: Path | None = None,
) -> tuple[ContentRoot, ...]:
    """Build validated ContentRoots from a config list."""
    return tuple(
        content_root_from_config(item, relative_to=relative_to) for item in items
    )


__all__ = [
    "ArtifactHandle",
    "ContentCarrier",
    "ContentRoot",
    "ContentRootError",
    "ContentRootMode",
    "ContentRootPolicy",
    "build_content_root_policy",
    "content_root_from_config",
    "content_roots_from_config",
    "validate_content_roots",
]
