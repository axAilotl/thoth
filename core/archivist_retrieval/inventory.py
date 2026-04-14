"""Incremental archivist corpus inventory and source parsing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import logging
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Optional

from collectors.web_clipper_layout import (
    WebClipperSourceContract,
    build_web_clipper_contract,
)
from collectors.web_clipper_parser import parse_web_clipper_markdown

from ..config import Config
from ..metadata_db import MetadataDB, get_metadata_db
from ..path_layout import PathLayout, build_path_layout
from ..pdf_text import PDFTextExtractionError, extract_pdf_text, extract_pdf_title
from ..wiki_io import read_document
from .models import (
    ArchivistCandidate,
    ArchivistCorpusDocument,
    ResolvedArchivistRoot,
)

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

logger = logging.getLogger(__name__)


class ArchivistInventoryError(ValueError):
    """Raised when archivist corpus inventory cannot complete safely."""


@dataclass(frozen=True)
class ArchivistInventoryResult:
    """Resolved corpus inventory for a selection run."""

    documents: tuple[ArchivistCorpusDocument, ...]
    scanned_roots: tuple[str, ...]
    missing_roots: tuple[str, ...]
    indexed_count: int
    reused_count: int


def sync_archivist_inventory(
    include_root_specs: tuple[str, ...],
    *,
    exclude_root_specs: tuple[str, ...],
    config: Config,
    layout: PathLayout | None = None,
    db: MetadataDB | None = None,
) -> ArchivistInventoryResult:
    """Incrementally sync allowed roots into the corpus index and return documents."""

    resolved_layout = layout or build_path_layout(config)
    metadata_db = db or get_metadata_db()
    metadata_db.ensure_archivist_corpus_tables()

    include_roots = tuple(
        resolve_archivist_root_spec(root_spec, layout=resolved_layout)
        for root_spec in include_root_specs
    )
    exclude_roots = tuple(
        resolve_archivist_root_spec(root_spec, layout=resolved_layout)
        for root_spec in exclude_root_specs
    )
    web_clipper_contract = _load_web_clipper_contract(config, layout=resolved_layout)

    documents_by_key: dict[str, ArchivistCorpusDocument] = {}
    scanned_roots: list[str] = []
    missing_roots: list[str] = []
    indexed_count = 0
    reused_count = 0

    for root in include_roots:
        if not root.path.exists():
            missing_roots.append(root.spec)
            continue
        if not root.path.is_dir():
            raise ArchivistInventoryError(f"Archivist root is not a directory: {root.path}")

        scanned_roots.append(root.spec)
        keep_candidate_keys: list[str] = []
        for path in sorted(root.path.rglob("*"), key=lambda item: str(item)):
            if not path.is_file():
                continue

            document, was_reused = _load_or_parse_document(
                path,
                root=root,
                layout=resolved_layout,
                db=metadata_db,
                web_clipper_contract=web_clipper_contract,
            )
            if document is None:
                continue
            if _is_excluded_document(document, exclude_roots):
                continue

            keep_candidate_keys.append(document.candidate_key)
            existing = documents_by_key.get(document.candidate_key)
            if existing is None or document.updated_at > existing.updated_at:
                documents_by_key[document.candidate_key] = document

            if was_reused:
                reused_count += 1
            else:
                indexed_count += 1

        metadata_db.prune_archivist_corpus_documents(
            scope=root.scope,
            relative_prefix=root.relative_prefix,
            keep_candidate_keys=tuple(keep_candidate_keys),
        )

    return ArchivistInventoryResult(
        documents=tuple(
            sorted(
                documents_by_key.values(),
                key=lambda item: (item.updated_at, item.candidate_key),
                reverse=True,
            )
        ),
        scanned_roots=tuple(scanned_roots),
        missing_roots=tuple(missing_roots),
        indexed_count=indexed_count,
        reused_count=reused_count,
    )


def resolve_archivist_root_spec(spec: str, *, layout: PathLayout) -> ResolvedArchivistRoot:
    pure_spec = PurePosixPath(spec)
    parts = pure_spec.parts
    if not parts:
        raise ArchivistInventoryError("Archivist root spec cannot be empty")

    if parts[0] in EXPLICIT_SCOPES:
        scope = parts[0]
        relative_parts = parts[1:]
    elif parts[0] in KNOWN_LIBRARY_ROOTS:
        scope = "library"
        relative_parts = parts
    else:
        scope = "vault"
        relative_parts = parts

    base = scope_base(scope, layout)
    relative_prefix = PurePosixPath(*relative_parts).as_posix() if relative_parts else ""
    path = base / PurePosixPath(*relative_parts) if relative_parts else base
    return ResolvedArchivistRoot(
        spec=spec,
        scope=scope,
        relative_prefix=relative_prefix,
        path=path,
    )


def scope_base(scope: str, layout: PathLayout) -> Path:
    if scope == "vault":
        return layout.vault_root
    if scope == "raw":
        return layout.raw_root
    if scope == "library":
        return layout.library_root
    raise ArchivistInventoryError(f"Unsupported archivist root scope: {scope}")


def document_matches_root(
    document: ArchivistCorpusDocument,
    root: ResolvedArchivistRoot,
) -> bool:
    return document.scope == root.scope and (
        not root.relative_prefix
        or _path_has_prefix(document.scope_relative_path, root.relative_prefix)
    )


def materialize_candidate(
    document: ArchivistCorpusDocument,
    *,
    root_spec: str,
    retrieval_score: float = 0.0,
    retrieval_sources: tuple[str, ...] = (),
    full_text_score: float | None = None,
    semantic_score: float | None = None,
) -> ArchivistCandidate:
    return ArchivistCandidate(
        candidate_key=document.candidate_key,
        path=document.path,
        scope=document.scope,
        scope_relative_path=document.scope_relative_path,
        root_spec=root_spec,
        source_type=document.source_type,
        file_type=document.file_type,
        title=document.title,
        tags=document.tags,
        content_text=document.content_text,
        source_hash=document.source_hash,
        size_bytes=document.size_bytes,
        updated_at=document.updated_at,
        source_id=document.source_id,
        retrieval_score=retrieval_score,
        retrieval_sources=retrieval_sources,
        full_text_score=full_text_score,
        semantic_score=semantic_score,
    )


def _load_or_parse_document(
    path: Path,
    *,
    root: ResolvedArchivistRoot,
    layout: PathLayout,
    db: MetadataDB,
    web_clipper_contract: WebClipperSourceContract | None,
) -> tuple[ArchivistCorpusDocument | None, bool]:
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_TEXT_EXTENSIONS and suffix not in SUPPORTED_BINARY_EXTENSIONS:
        return None, False

    base = scope_base(root.scope, layout)
    try:
        scope_relative_path = path.relative_to(base).as_posix()
    except ValueError as exc:
        raise ArchivistInventoryError(
            f"Archivist candidate escaped its scope root: {path}"
        ) from exc

    if root.relative_prefix and not _path_has_prefix(scope_relative_path, root.relative_prefix):
        raise ArchivistInventoryError(f"Archivist candidate escaped its include root: {path}")

    candidate_key = f"{root.scope}:{scope_relative_path}"
    stat = path.stat()
    updated_at = datetime.fromtimestamp(stat.st_mtime).isoformat()
    existing = db.get_archivist_corpus_document(candidate_key)
    if (
        existing is not None
        and existing.updated_at == updated_at
        and existing.size_bytes == stat.st_size
        and existing.path == path
    ):
        return existing, True

    source_id = _lookup_source_id(path, layout=layout, db=db)
    if suffix in SUPPORTED_TEXT_EXTENSIONS:
        title, content_text, tags, source_type, file_type, source_hash = _read_text_document(
            path,
            scope=root.scope,
            scope_relative_path=scope_relative_path,
            web_clipper_contract=web_clipper_contract,
        )
    else:
        title, content_text, tags, source_type, file_type, source_hash = _read_pdf_document(
            path,
            scope=root.scope,
            scope_relative_path=scope_relative_path,
        )

    document = ArchivistCorpusDocument(
        candidate_key=candidate_key,
        path=path,
        scope=root.scope,
        scope_relative_path=scope_relative_path,
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
    db.upsert_archivist_corpus_document(document)
    return document, False


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


def _read_text_document(
    path: Path,
    *,
    scope: str,
    scope_relative_path: str,
    web_clipper_contract: WebClipperSourceContract | None,
) -> tuple[str, str, tuple[str, ...], str, str, str]:
    suffix = path.suffix.lower()
    if _is_web_clipper_note_path(path, web_clipper_contract):
        parsed = parse_web_clipper_markdown(path.read_text(encoding="utf-8"), source_path=path)
        tags = _normalize_tags(parsed.frontmatter.get("tags") or parsed.frontmatter.get("tag"))
        content_text = parsed.body.strip()
        source_hash = _sha256_text(parsed.raw_content)
        return parsed.title, content_text, tags, "web_clipper", "note", source_hash

    if suffix == ".txt":
        text = path.read_text(encoding="utf-8")
        title = _prettify_name(path.stem)
        source_type = _detect_source_type(
            scope=scope,
            scope_relative_path=scope_relative_path,
            default_type="note",
        )
        return title, text.strip(), (), source_type, "text", _sha256_text(text)

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
    raw_text = path.read_text(encoding="utf-8")
    return title, document.body.strip(), tags, source_type, file_type, _sha256_text(raw_text)


def _read_pdf_document(
    path: Path,
    *,
    scope: str,
    scope_relative_path: str,
) -> tuple[str, str, tuple[str, ...], str, str, str]:
    title = _prettify_name(path.stem)
    content_text = ""
    try:
        extracted_title = extract_pdf_title(path)
        if extracted_title:
            title = extracted_title
    except PDFTextExtractionError as exc:
        logger.warning("Failed to extract PDF title for %s: %s", path, exc)

    try:
        content_text = extract_pdf_text(path)
    except PDFTextExtractionError as exc:
        logger.warning("Failed to extract PDF text for %s: %s", path, exc)

    source_type = _detect_source_type(
        scope=scope,
        scope_relative_path=scope_relative_path,
        default_type="pdf",
    )
    return (
        title,
        content_text,
        _infer_pdf_tags(path=path, title=title, content_text=content_text),
        source_type,
        "pdf",
        _sha256_file(path),
    )


def _infer_pdf_tags(
    *,
    path: Path,
    title: str,
    content_text: str,
) -> tuple[str, ...]:
    corpus = " ".join(
        part for part in (path.stem, title, content_text[:1000]) if part
    ).lower()
    tags: list[str] = []
    if "whitepaper" in corpus or "white paper" in corpus:
        tags.append("whitepaper")
    return tuple(tags)


def _detect_source_type(
    *,
    scope: str,
    scope_relative_path: str,
    frontmatter: Optional[dict[str, Any]] = None,
    default_type: str,
) -> str:
    frontmatter = frontmatter or {}
    fm_type = str(frontmatter.get("type") or "").strip().lower().replace("-", "_")
    if fm_type in {"tweet", "thread"}:
        return fm_type

    thoth_type = str(frontmatter.get("thoth_type") or "").strip().lower()
    if thoth_type == "translation_companion":
        return "translation"

    first_part = PurePosixPath(scope_relative_path).parts[0] if PurePosixPath(scope_relative_path).parts else ""
    normalized_first = first_part.replace("-", "_")
    if normalized_first == "tweets":
        return "tweet"
    if normalized_first == "threads":
        return "thread"
    if normalized_first in {"papers", "pdfs"}:
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


def _load_web_clipper_contract(
    config: Config,
    *,
    layout: PathLayout,
) -> WebClipperSourceContract | None:
    source_config = config.get("sources.web_clipper", {}) or {}
    if not isinstance(source_config, dict):
        return None
    if not source_config.get("note_dirs") and not source_config.get("attachment_dirs"):
        return None
    return build_web_clipper_contract(config, layout=layout)


def _is_web_clipper_note_path(
    path: Path,
    contract: WebClipperSourceContract | None,
) -> bool:
    if contract is None:
        return False
    if not contract.is_note_path(path):
        return False
    return any(path == root or root in path.parents for root in contract.note_dirs)


def _normalize_tags(raw_tags: Any) -> tuple[str, ...]:
    if raw_tags is None:
        return ()
    if isinstance(raw_tags, str):
        items: Iterable[Any] = raw_tags.split(",")
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


def _is_excluded_document(
    document: ArchivistCorpusDocument,
    exclude_roots: tuple[ResolvedArchivistRoot, ...],
) -> bool:
    return any(document_matches_root(document, root) for root in exclude_roots)


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
