"""Archivist topic candidate selection with hard source gates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Optional

from collectors.web_clipper_parser import parse_web_clipper_markdown

from .archivist_topics import ArchivistTopicDefinition
from .config import Config
from .metadata_db import MetadataDB, get_metadata_db
from .path_layout import PathLayout, build_path_layout
from .wiki_io import read_document

SUPPORTED_TEXT_EXTENSIONS = {".md", ".markdown", ".txt"}
SUPPORTED_BINARY_EXTENSIONS = {".pdf"}
KNOWN_VAULT_ROOTS = {
    "tweets",
    "threads",
    "papers",
    "stars",
    "repos",
    "transcripts",
    "images",
    "videos",
    "media",
    "_digests",
}
KNOWN_LIBRARY_ROOTS = {"translations"}
EXPLICIT_SCOPES = {"vault", "raw", "library"}


class ArchivistSelectionError(ValueError):
    """Raised when archivist source selection cannot be completed safely."""


@dataclass(frozen=True)
class ResolvedArchivistRoot:
    """Concrete root resolved for an archivist topic gate."""

    spec: str
    scope: str
    relative_prefix: str
    path: Path


@dataclass(frozen=True)
class ArchivistCandidate:
    """Single archivist source candidate after all code-side gates."""

    candidate_key: str
    path: Path
    scope: str
    scope_relative_path: str
    root_spec: str
    source_type: str
    file_type: str
    title: str
    tags: tuple[str, ...]
    content_text: str
    source_hash: str
    size_bytes: int
    updated_at: str
    source_id: str | None = None


@dataclass(frozen=True)
class ArchivistSelectionResult:
    """Deterministic candidate selection result for a single topic."""

    topic_id: str
    candidates: tuple[ArchivistCandidate, ...]
    scanned_roots: tuple[str, ...]
    missing_roots: tuple[str, ...]


def select_archivist_candidates(
    topic: ArchivistTopicDefinition,
    *,
    config: Config,
    layout: PathLayout | None = None,
    db: MetadataDB | None = None,
) -> ArchivistSelectionResult:
    """Return archivist candidates using only configured code-enforced gates."""

    resolved_layout = layout or build_path_layout(config)
    metadata_db = db or get_metadata_db()

    include_roots = tuple(
        _resolve_root_spec(root_spec, layout=resolved_layout)
        for root_spec in topic.include_roots
    )
    exclude_roots = tuple(
        _resolve_root_spec(root_spec, layout=resolved_layout)
        for root_spec in topic.exclude_roots
    )

    candidates_by_key: dict[str, ArchivistCandidate] = {}
    scanned_roots: list[str] = []
    missing_roots: list[str] = []

    for root in include_roots:
        if not root.path.exists():
            missing_roots.append(root.spec)
            continue
        if not root.path.is_dir():
            raise ArchivistSelectionError(
                f"Archivist root is not a directory: {root.path}"
            )

        scanned_roots.append(root.spec)
        for path in sorted(root.path.rglob("*"), key=lambda item: str(item)):
            if not path.is_file():
                continue

            candidate = _build_candidate(
                path,
                root=root,
                layout=resolved_layout,
                db=metadata_db,
            )
            if candidate is None:
                continue
            if _is_excluded(candidate, exclude_roots):
                continue
            if not _matches_filters(candidate, topic):
                continue

            existing = candidates_by_key.get(candidate.candidate_key)
            if existing is None or candidate.updated_at > existing.updated_at:
                candidates_by_key[candidate.candidate_key] = candidate

    candidates = sorted(
        candidates_by_key.values(),
        key=lambda item: (item.updated_at, item.candidate_key),
        reverse=True,
    )
    if topic.max_sources is not None:
        candidates = candidates[: topic.max_sources]

    return ArchivistSelectionResult(
        topic_id=topic.id,
        candidates=tuple(candidates),
        scanned_roots=tuple(scanned_roots),
        missing_roots=tuple(missing_roots),
    )


def _resolve_root_spec(spec: str, *, layout: PathLayout) -> ResolvedArchivistRoot:
    pure_spec = PurePosixPath(spec)
    parts = pure_spec.parts
    if not parts:
        raise ArchivistSelectionError("Archivist root spec cannot be empty")

    scope = ""
    relative_parts: tuple[str, ...]
    if parts[0] in EXPLICIT_SCOPES:
        scope = parts[0]
        relative_parts = parts[1:]
    elif parts[0] in KNOWN_LIBRARY_ROOTS:
        scope = "library"
        relative_parts = parts
    elif parts[0] in KNOWN_VAULT_ROOTS:
        scope = "vault"
        relative_parts = parts
    else:
        scope = "raw"
        relative_parts = parts

    base = _scope_base(scope, layout)
    relative_prefix = PurePosixPath(*relative_parts).as_posix() if relative_parts else ""
    path = base / PurePosixPath(*relative_parts) if relative_parts else base

    return ResolvedArchivistRoot(
        spec=spec,
        scope=scope,
        relative_prefix=relative_prefix,
        path=path,
    )


def _scope_base(scope: str, layout: PathLayout) -> Path:
    if scope == "vault":
        return layout.vault_root
    if scope == "raw":
        return layout.raw_root
    if scope == "library":
        return layout.library_root
    raise ArchivistSelectionError(f"Unsupported archivist root scope: {scope}")


def _build_candidate(
    path: Path,
    *,
    root: ResolvedArchivistRoot,
    layout: PathLayout,
    db: MetadataDB,
) -> ArchivistCandidate | None:
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_TEXT_EXTENSIONS and suffix not in SUPPORTED_BINARY_EXTENSIONS:
        return None

    base = _scope_base(root.scope, layout)
    try:
        scope_relative_path = path.relative_to(base).as_posix()
    except ValueError as exc:
        raise ArchivistSelectionError(
            f"Archivist candidate escaped its scope root: {path}"
        ) from exc

    if root.relative_prefix and not _path_has_prefix(scope_relative_path, root.relative_prefix):
        raise ArchivistSelectionError(
            f"Archivist candidate escaped its include root: {path}"
        )

    stat = path.stat()
    updated_at = datetime.fromtimestamp(stat.st_mtime).isoformat()
    source_id = _lookup_source_id(path, layout=layout, db=db)
    candidate_key = f"{root.scope}:{scope_relative_path}"

    if suffix in SUPPORTED_TEXT_EXTENSIONS:
        title, content_text, tags, source_type, file_type, source_hash = _read_text_candidate(
            path,
            scope=root.scope,
            scope_relative_path=scope_relative_path,
        )
    else:
        title = _prettify_name(path.stem)
        content_text = ""
        tags = ()
        source_type = _detect_source_type(
            scope=root.scope,
            scope_relative_path=scope_relative_path,
            default_type="pdf",
        )
        file_type = "pdf"
        source_hash = _sha256_file(path)

    return ArchivistCandidate(
        candidate_key=candidate_key,
        path=path,
        scope=root.scope,
        scope_relative_path=scope_relative_path,
        root_spec=root.spec,
        source_type=source_type,
        file_type=file_type,
        title=title,
        tags=tags,
        content_text=content_text,
        source_hash=source_hash,
        size_bytes=stat.st_size,
        updated_at=updated_at,
        source_id=source_id,
    )


def _lookup_source_id(path: Path, *, layout: PathLayout, db: MetadataDB) -> str | None:
    entry = db.get_file_entry(str(path))
    if entry and entry.source_id:
        return entry.source_id

    for base in (layout.vault_root, layout.raw_root):
        try:
            rel_path = str(path.relative_to(base))
        except ValueError:
            continue
        entry = db.get_file_entry(rel_path)
        if entry and entry.source_id:
            return entry.source_id

    return None


def _read_text_candidate(
    path: Path,
    *,
    scope: str,
    scope_relative_path: str,
) -> tuple[str, str, tuple[str, ...], str, str, str]:
    suffix = path.suffix.lower()
    if scope == "raw" and _path_has_prefix(scope_relative_path, "web-clipper"):
        parsed = parse_web_clipper_markdown(path.read_text(encoding="utf-8"), source_path=path)
        tags = _normalize_tags(parsed.frontmatter.get("tags") or parsed.frontmatter.get("tag"))
        content_text = parsed.body.strip()
        source_hash = _sha256_text(parsed.raw_content)
        return (
            parsed.title,
            content_text,
            tags,
            "web_clipper",
            "note",
            source_hash,
        )

    if suffix == ".txt":
        text = path.read_text(encoding="utf-8")
        title = _prettify_name(path.stem)
        source_type = _detect_source_type(
            scope=scope,
            scope_relative_path=scope_relative_path,
            default_type="note",
        )
        return (
            title,
            text.strip(),
            (),
            source_type,
            "text",
            _sha256_text(text),
        )

    document = read_document(path)
    frontmatter = document.frontmatter if isinstance(document.frontmatter, dict) else {}
    title = str(frontmatter.get("title") or _extract_heading(document.body) or _prettify_name(path.stem))
    tags = _normalize_tags(frontmatter.get("tags") or frontmatter.get("tag"))
    thoth_type = str(frontmatter.get("thoth_type") or "").strip().lower()
    file_type = "translation" if thoth_type == "translation_companion" else "markdown"
    source_type = _detect_source_type(
        scope=scope,
        scope_relative_path=scope_relative_path,
        frontmatter=frontmatter,
        default_type="note",
    )
    content_text = document.body.strip()
    source_hash = _sha256_text(path.read_text(encoding="utf-8"))
    return title, content_text, tags, source_type, file_type, source_hash


def _detect_source_type(
    *,
    scope: str,
    scope_relative_path: str,
    frontmatter: Optional[dict[str, Any]] = None,
    default_type: str,
) -> str:
    frontmatter = frontmatter or {}
    fm_type = str(frontmatter.get("type") or "").strip().lower().replace("-", "_")
    if fm_type:
        if fm_type == "tweet":
            return "tweet"
        if fm_type == "thread":
            return "thread"

    thoth_type = str(frontmatter.get("thoth_type") or "").strip().lower()
    if thoth_type == "translation_companion":
        return "translation"

    first_part = PurePosixPath(scope_relative_path).parts[0] if PurePosixPath(scope_relative_path).parts else ""
    normalized_first = first_part.replace("-", "_")
    if normalized_first == "tweets":
        return "tweet"
    if normalized_first == "threads":
        return "thread"
    if normalized_first == "papers":
        return "paper"
    if normalized_first in {"stars", "repos"}:
        return "repository"
    if normalized_first == "transcripts":
        return "transcript"
    if normalized_first == "translations" or scope == "library":
        return "translation"
    if normalized_first == "journals":
        return "journal"
    if normalized_first == "web_clipper":
        return "web_clipper"
    return default_type


def _normalize_tags(raw_tags: Any) -> tuple[str, ...]:
    if raw_tags is None:
        return ()
    items: Iterable[Any]
    if isinstance(raw_tags, str):
        items = raw_tags.split(",")
    elif isinstance(raw_tags, (list, tuple, set)):
        items = raw_tags
    else:
        return ()

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_tag in items:
        text = str(raw_tag).strip().lower().lstrip("#")
        text = " ".join(text.split()).replace("-", "_").replace(" ", "_").strip("_")
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(normalized)


def _is_excluded(candidate: ArchivistCandidate, exclude_roots: tuple[ResolvedArchivistRoot, ...]) -> bool:
    return any(
        root.scope == candidate.scope
        and (
            not root.relative_prefix
            or _path_has_prefix(candidate.scope_relative_path, root.relative_prefix)
        )
        for root in exclude_roots
    )


def _matches_filters(candidate: ArchivistCandidate, topic: ArchivistTopicDefinition) -> bool:
    if topic.source_types and candidate.source_type not in topic.source_types:
        return False

    candidate_tags = set(candidate.tags)
    if topic.include_tags and candidate_tags.isdisjoint(topic.include_tags):
        return False
    if topic.exclude_tags and not candidate_tags.isdisjoint(topic.exclude_tags):
        return False

    search_corpus = _search_corpus(candidate)
    if topic.include_terms and not any(term in search_corpus for term in topic.include_terms):
        return False
    if topic.exclude_terms and any(term in search_corpus for term in topic.exclude_terms):
        return False

    return True


def _search_corpus(candidate: ArchivistCandidate) -> str:
    parts = [
        candidate.title,
        candidate.content_text,
        " ".join(candidate.tags),
        candidate.scope_relative_path.replace("/", " "),
        candidate.source_type,
        candidate.file_type,
        candidate.source_id or "",
    ]
    return " ".join(part for part in parts if part).lower()


def _extract_heading(body: str) -> str | None:
    for line in (body or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip() or None
        return stripped
    return None


def _prettify_name(value: str) -> str:
    return " ".join(value.replace("_", " ").replace("-", " ").split())


def _path_has_prefix(path_value: str, prefix: str) -> bool:
    if not prefix:
        return True
    return path_value == prefix or path_value.startswith(prefix + "/")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
