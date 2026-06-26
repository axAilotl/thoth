"""Connector for imported markdown files outside Web Clipper roots."""

from __future__ import annotations

import asyncio
import hashlib
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from core.artifacts import MarkdownArtifact
from core.capture_event_store import CaptureEventStore
from core.capture_lifecycle import CaptureLifecycleResult, CaptureLifecycleService
from core.config import Config, config
from core.connector_budgets import start_connector_budget_run
from core.connector_capture import ConnectorCaptureQueue
from core.metadata_db import MetadataDB, get_metadata_db
from core.path_layout import PathLayout, build_path_layout

from .web_clipper_parser import WebClipperParsedNote, parse_web_clipper_markdown


DEFAULT_FILE_PATTERNS = ("*.md", "*.markdown")


@dataclass(frozen=True)
class ImportedMarkdownRecord:
    """Queued artifact produced from one imported markdown file."""

    artifact_id: str
    source_name: str
    source_path: Path
    raw_markdown_path: Path
    queued: bool = True
    capture_event_id: str | None = None
    capture_source_id: str | None = None
    capture_session_id: str | None = None
    raw_ref_id: str | None = None
    artifact_link_id: str | None = None


@dataclass(frozen=True)
class ImportedMarkdownResult:
    """Summary of one imported markdown connector run."""

    records: tuple[ImportedMarkdownRecord, ...] = field(default_factory=tuple)
    import_paths: tuple[str, ...] = field(default_factory=tuple)
    import_dirs: tuple[str, ...] = field(default_factory=tuple)
    budget: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [
                {
                    "artifact_id": record.artifact_id,
                    "source_name": record.source_name,
                    "source_path": str(record.source_path),
                    "raw_markdown_path": str(record.raw_markdown_path),
                    "queued": record.queued,
                    "capture_event_id": record.capture_event_id,
                    "capture_source_id": record.capture_source_id,
                    "capture_session_id": record.capture_session_id,
                    "raw_ref_id": record.raw_ref_id,
                    "artifact_link_id": record.artifact_link_id,
                }
                for record in self.records
            ],
            "queued_count": sum(1 for record in self.records if record.queued),
            "import_paths": list(self.import_paths),
            "import_dirs": list(self.import_dirs),
            "budget": self.budget,
        }


class ImportedMarkdownConnector:
    """Collect markdown imports through the artifact queue and capture lifecycle."""

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
        capture_event_store: CaptureEventStore | None = None,
    ) -> None:
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.layout.ensure_directories()
        self.db = db or get_metadata_db()
        self.capture_queue = ConnectorCaptureQueue(
            self.config,
            layout=self.layout,
            db=self.db,
            capture_event_store=capture_event_store,
        )
        self.last_budget_usage: dict[str, Any] = {}

    async def collect(
        self,
        *,
        import_paths: Iterable[str | Path] | None = None,
        import_dirs: Iterable[str | Path] | None = None,
        file_patterns: Iterable[str] | None = None,
        source_name: str | None = None,
        limit: int | None = None,
    ) -> ImportedMarkdownResult:
        """Collect markdown files and queue capture-only markdown artifacts."""
        configured = self.config.get("sources.imported_markdown", {}) or {}
        if not isinstance(configured, Mapping):
            configured = {}

        resolved_source = _clean_string(
            source_name or configured.get("source_name")
        ) or "imported_markdown"
        resolved_paths = _path_list(import_paths) or _path_list(
            configured.get("import_paths")
            or configured.get("import_path")
            or configured.get("paths")
        )
        resolved_dirs = _path_list(import_dirs) or _path_list(
            configured.get("import_dirs")
            or configured.get("import_dir")
            or configured.get("dirs")
        )
        resolved_patterns = tuple(
            _string_list(file_patterns)
            or _string_list(configured.get("file_patterns"))
            or DEFAULT_FILE_PATTERNS
        )
        if not resolved_paths and not resolved_dirs:
            raise ValueError(
                "Imported markdown ingestion requires import_paths or import_dirs"
            )

        input_files = self._resolve_input_files(
            import_paths=resolved_paths,
            import_dirs=resolved_dirs,
            file_patterns=resolved_patterns,
        )
        if limit is not None:
            input_files = input_files[: max(0, int(limit))]

        budget = start_connector_budget_run(self.config, "imported_markdown")
        budget.add_files(input_files)
        self.last_budget_usage = budget.summary()

        records: list[ImportedMarkdownRecord] = []
        run_id = datetime.now().isoformat()
        with self.capture_queue.lifecycle() as lifecycle:
            for import_path in input_files:
                source_text = await asyncio.to_thread(
                    import_path.read_text,
                    encoding="utf-8",
                )
                parsed_note = parse_web_clipper_markdown(
                    source_text,
                    source_path=import_path,
                )
                note_source_name = _source_name_from_note(
                    parsed_note,
                    default_source_name=resolved_source,
                )
                raw_markdown_path = await asyncio.to_thread(
                    self._preserve_raw_markdown,
                    import_path,
                    note_source_name,
                )
                records.append(
                    self._queue_note(
                        lifecycle,
                        parsed_note,
                        source_name=note_source_name,
                        raw_markdown_path=raw_markdown_path,
                        run_id=run_id,
                    )
                )

        return ImportedMarkdownResult(
            records=tuple(records),
            import_paths=tuple(str(path) for path in resolved_paths),
            import_dirs=tuple(str(path) for path in resolved_dirs),
            budget=self.last_budget_usage,
        )

    def _resolve_input_files(
        self,
        *,
        import_paths: Iterable[Path],
        import_dirs: Iterable[Path],
        file_patterns: Iterable[str],
    ) -> list[Path]:
        candidates: list[Path] = []
        for path in import_paths:
            resolved = path.expanduser()
            if not resolved.exists():
                raise FileNotFoundError(
                    f"Imported markdown path does not exist: {resolved}"
                )
            if resolved.is_dir():
                candidates.extend(_files_in_dir(resolved, file_patterns))
            elif resolved.is_file():
                candidates.append(resolved)
            else:
                raise ValueError(f"Imported markdown path is not a file: {resolved}")

        for directory in import_dirs:
            resolved = directory.expanduser()
            if not resolved.exists():
                raise FileNotFoundError(
                    f"Imported markdown directory does not exist: {resolved}"
                )
            if not resolved.is_dir():
                raise ValueError(
                    f"Imported markdown directory is not a directory: {resolved}"
                )
            candidates.extend(_files_in_dir(resolved, file_patterns))

        return _dedupe_paths(candidates)

    def _preserve_raw_markdown(self, import_path: Path, source_name: str) -> Path:
        raw_root = self.layout.raw_root / "imported_markdown" / _safe_slug(source_name)
        raw_root.mkdir(parents=True, exist_ok=True)
        payload = import_path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()[:12]
        raw_name = f"{_safe_slug(import_path.stem)}-{digest}{import_path.suffix.lower()}"
        raw_path = raw_root / raw_name
        try:
            if import_path.resolve() == raw_path.resolve():
                return raw_path
        except FileNotFoundError:
            pass
        if not raw_path.exists():
            shutil.copy2(import_path, raw_path)
        return raw_path

    def _queue_note(
        self,
        lifecycle: CaptureLifecycleService,
        parsed_note: WebClipperParsedNote,
        *,
        source_name: str,
        raw_markdown_path: Path,
        run_id: str,
    ) -> ImportedMarkdownRecord:
        artifact = self._build_artifact(
            parsed_note,
            source_name=source_name,
            raw_markdown_path=raw_markdown_path,
        )
        source_id = _source_id_for_path(parsed_note.source_path, layout=self.layout)
        result = self.capture_queue.queue_artifact(
            lifecycle,
            artifact,
            artifact_type="markdown",
            source={
                "source_name": source_name,
                "source_type": "imported_markdown",
                "collector": "imported_markdown_connector",
                "native_source_id": source_id,
                "base_uri": str(parsed_note.source_path.parent),
                "metadata": {
                    "source_path": str(parsed_note.source_path),
                    "source_relative_path": source_id,
                    "frontmatter_source": parsed_note.frontmatter.get("source"),
                    "capture_run_id": run_id,
                    "security_policy": "prompt_security_scan_on_queue",
                },
            },
            session={
                "session_type": "imported_markdown_import",
                "native_session_id": f"imported_markdown:{run_id}",
                "started_at": run_id,
                "metadata": {
                    "source_name": source_name,
                    "capture_run_id": run_id,
                },
            },
            event={
                "event_type": "imported_markdown_note",
                "native_event_id": source_id,
                "occurred_at": artifact.created_at,
                "captured_at": artifact.ingested_at,
                "privacy": _privacy_metadata(parsed_note.frontmatter),
                "retention": _retention_metadata(parsed_note.frontmatter),
                "provenance": {
                    "collector": "imported_markdown_connector",
                    "capture_run_id": run_id,
                    "source_path": str(parsed_note.source_path),
                    "raw_preserved": True,
                    "security_policy": "prompt_security_scan_on_queue",
                    "source_trust_reason": "operator_imported_markdown",
                },
            },
            raw_path=raw_markdown_path,
        )
        if self.db.get_ingestion_entry(artifact.id) is None:
            raise RuntimeError(
                f"Failed to queue imported markdown artifact: {parsed_note.source_path}"
            )
        return _record_from_result(
            result,
            source_path=parsed_note.source_path,
            raw_markdown_path=raw_markdown_path,
        )

    def _build_artifact(
        self,
        parsed_note: WebClipperParsedNote,
        *,
        source_name: str,
        raw_markdown_path: Path,
    ) -> MarkdownArtifact:
        source_path = parsed_note.source_path
        source_id = _source_id_for_path(source_path, layout=self.layout)
        artifact_id = _artifact_id(parsed_note, source_name=source_name)
        raw_ref = self._relative_to_vault(raw_markdown_path)
        sha256 = _sha256_file(raw_markdown_path)
        size_bytes = raw_markdown_path.stat().st_size
        created_at = _frontmatter_string(
            parsed_note.frontmatter,
            "created",
            "created_at",
            "date",
            "timestamp",
        )
        return MarkdownArtifact(
            id=artifact_id,
            source_type="imported_markdown",
            raw_content=parsed_note.raw_content,
            created_at=created_at,
            ingested_at=datetime.now().isoformat(),
            source_path=str(source_path),
            source_relative_path=source_id,
            file_type="markdown",
            title=parsed_note.title,
            frontmatter=parsed_note.frontmatter,
            body=parsed_note.body,
            source_checksum=sha256,
            source_size_bytes=size_bytes,
            source_language=parsed_note.source_language,
            source_url=parsed_note.source_url,
            tags=_extract_tags(parsed_note.frontmatter),
            custom_metadata={
                "source_kind": "imported_markdown",
                "source_name": source_name,
                "source_path": str(source_path),
                "source_relative_path": source_id,
                "raw_payload_path": raw_ref,
                "frontmatter_keys": sorted(parsed_note.frontmatter.keys()),
                "security_policy": "prompt_security_scan_on_queue",
            },
            normalized_metadata={
                "artifact_id": artifact_id,
                "source_type": "imported_markdown",
                "source_name": source_name,
                "source_path": str(source_path),
                "source_relative_path": source_id,
            },
        )

    def _relative_to_vault(self, path: Path | None) -> str | None:
        if path is None:
            return None
        try:
            return path.relative_to(self.layout.vault_root).as_posix()
        except ValueError:
            return str(path)


def _record_from_result(
    result: CaptureLifecycleResult,
    *,
    source_path: Path,
    raw_markdown_path: Path,
) -> ImportedMarkdownRecord:
    return ImportedMarkdownRecord(
        artifact_id=result.queue_artifact_id,
        source_name=result.source_name,
        source_path=source_path,
        raw_markdown_path=raw_markdown_path,
        capture_event_id=result.event_id,
        capture_source_id=result.source_id,
        capture_session_id=result.session_id,
        raw_ref_id=result.raw_ref_id,
        artifact_link_id=result.artifact_link_id,
    )


def _source_name_from_note(
    parsed_note: WebClipperParsedNote,
    *,
    default_source_name: str,
) -> str:
    explicit = _frontmatter_string(parsed_note.frontmatter, "source_name")
    if explicit:
        return explicit
    source = _frontmatter_string(parsed_note.frontmatter, "source")
    if source and "://" not in source:
        return source
    return default_source_name


def _artifact_id(parsed_note: WebClipperParsedNote, *, source_name: str) -> str:
    explicit = _frontmatter_string(
        parsed_note.frontmatter,
        "artifact_id",
        "id",
        "thoth_artifact_id",
    )
    if explicit:
        return explicit
    return f"{_artifact_id_source_prefix(source_name)}-{_safe_slug(parsed_note.title)}"


def _artifact_id_source_prefix(source_name: str) -> str:
    slug = _safe_slug(source_name)
    if slug.endswith("-import"):
        return slug[: -len("-import")] or slug
    return slug


def _privacy_metadata(frontmatter: Mapping[str, Any]) -> dict[str, Any]:
    privacy = frontmatter.get("privacy")
    payload = dict(privacy) if isinstance(privacy, Mapping) else {}
    classification = (
        _frontmatter_string(payload, "classification", "privacy_class", "class")
        or _frontmatter_string(frontmatter, "privacy_class", "classification")
        or (_clean_string(privacy) if not isinstance(privacy, Mapping) else None)
        or "personal"
    )
    payload.setdefault("classification", classification)
    payload.setdefault("privacy_class", classification)
    payload.setdefault("source", "frontmatter" if privacy else "default")
    return payload


def _retention_metadata(frontmatter: Mapping[str, Any]) -> dict[str, Any]:
    retention = frontmatter.get("retention")
    payload = dict(retention) if isinstance(retention, Mapping) else {}
    retention_class = (
        _frontmatter_string(payload, "retention_class", "policy_name", "policy", "class")
        or _frontmatter_string(frontmatter, "retention_class", "policy_name")
        or (_clean_string(retention) if not isinstance(retention, Mapping) else None)
        or "imported_markdown"
    )
    payload.setdefault("retention_class", retention_class)
    payload.setdefault("policy", retention_class)
    payload.setdefault("action", "retain")
    payload.setdefault("scope", ["raw_capture", "artifact_queue"])
    return payload


def _source_id_for_path(path: Path, *, layout: PathLayout) -> str:
    try:
        return path.relative_to(layout.vault_root).as_posix()
    except ValueError:
        return str(path.resolve())


def _frontmatter_string(frontmatter: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = frontmatter.get(key)
        text = _clean_string(value)
        if text:
            return text
    return None


def _extract_tags(frontmatter: Mapping[str, Any]) -> list[str]:
    raw_tags = frontmatter.get("tags") or frontmatter.get("tag")
    if raw_tags is None:
        return []
    if isinstance(raw_tags, str):
        items = [part.strip() for part in raw_tags.split(",")]
    elif isinstance(raw_tags, (list, tuple)):
        items = [str(item).strip() for item in raw_tags]
    else:
        return []
    return [item for item in items if item]


def _files_in_dir(directory: Path, file_patterns: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in file_patterns:
        files.extend(path for path in directory.rglob(pattern) if path.is_file())
    return sorted(files)


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    deduped: dict[Path, Path] = {}
    for path in paths:
        deduped[path.resolve()] = path
    return list(deduped.values())


def _path_list(value: Any) -> list[Path]:
    return [Path(item).expanduser() for item in _string_list(value)]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    chars = [char if char.isalnum() else "-" for char in text]
    slug = "-".join(part for part in "".join(chars).split("-") if part)
    return slug or "markdown"


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
