"""
Web Clipper collector - indexes explicit source directories under the vault.

This collector only scans the configured allowlist from the Web Clipper source
contract. It does not expand beyond those roots, and it does not parse or mutate
source documents.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from core.capture_event_store import CaptureEventStore
from core.capture_lifecycle import CaptureLifecycleService
from core.config import Config
from core.connector_budgets import start_connector_budget_run
from core.connector_capture import ConnectorCaptureQueue
from core.artifacts.web_clipper import WebClipperArtifact
from core.metadata_db import (
    FileMetadata,
    MetadataDB,
    get_metadata_db,
)
from core.path_layout import PathLayout, build_path_layout
from core.staged_assets import (
    StagedAssetPublisher,
    StagedAssetValidationError,
    validate_existing_asset,
)

from .web_clipper_parser import (
    WebClipperFrontmatterError,
    WebClipperMarkdownError,
    WebClipperParsedNote,
    parse_web_clipper_markdown,
)
from .web_clipper_layout import WebClipperSourceContract, build_web_clipper_contract

logger = logging.getLogger(__name__)


class WebClipperSourceError(ValueError):
    """Raised when configured Web Clipper source roots are unusable."""


@dataclass(frozen=True)
class WebClipperFileRecord:
    """Discovery record for a single Web Clipper source file."""

    path: Path
    root: Path
    source_id: str
    file_type: str
    size_bytes: int
    sha256: str
    updated_at: str
    is_new_or_changed: bool
    artifact: WebClipperArtifact | None = None
    managed_path: Path | None = None
    would_queue: bool = False
    would_stage: bool = False


@dataclass(frozen=True)
class _ScannedFile:
    path: Path
    root: Path
    source_id: str
    file_type: str
    size_bytes: int
    sha256: str
    updated_at: str
    is_new_or_changed: bool


class WebClipperCollector:
    """Index files from the explicit Web Clipper allowlist."""

    def __init__(
        self,
        config: Config,
        *,
        layout: PathLayout | None = None,
        contract: WebClipperSourceContract | None = None,
        db: Optional[MetadataDB] = None,
        capture_event_store: CaptureEventStore | None = None,
    ):
        self.config = config
        self.layout = layout or build_path_layout(config)
        self.contract = contract or build_web_clipper_contract(config, layout=self.layout)
        self.db = db or get_metadata_db()
        self._asset_publisher: StagedAssetPublisher | None = None
        self.capture_queue = ConnectorCaptureQueue(
            config,
            layout=self.layout,
            db=self.db,
            capture_event_store=capture_event_store,
        )
        self.last_budget_usage: dict[str, object] = {}

        self._validate_roots()

    def collect(self) -> List[WebClipperFileRecord]:
        """Scan the configured allowlist and upsert file metadata."""
        discovered: List[WebClipperFileRecord] = []
        run_id = datetime.now().isoformat()
        targets = self._discover_targets()

        budget = start_connector_budget_run(self.config, "web_clipper")
        budget.add_files([path for path, _root, _file_type in targets])
        self.last_budget_usage = budget.summary()

        with self.capture_queue.lifecycle() as lifecycle:
            for path, root, file_type in targets:
                if file_type == "note":
                    discovered.append(
                        self._index_note_file(
                            path,
                            root=root,
                            lifecycle=lifecycle,
                            run_id=run_id,
                        )
                    )
                elif file_type == "attachment":
                    discovered.append(self._index_attachment_file(path, root=root))

        return discovered

    def plan(self) -> List[WebClipperFileRecord]:
        """Scan the configured allowlist without writing queue or file entries."""
        discovered: List[WebClipperFileRecord] = []
        for path, root, file_type in self._discover_targets():
            if file_type == "note":
                discovered.append(self._plan_note_file(path, root=root))
            elif file_type == "attachment":
                discovered.append(self._plan_attachment_file(path, root=root))

        return discovered

    def _validate_roots(self) -> None:
        missing = [root for root in self.contract.watch_dirs if not root.exists()]
        if missing:
            formatted = ", ".join(str(path) for path in missing)
            raise WebClipperSourceError(
                f"Web Clipper source directories do not exist: {formatted}"
            )
        for root in self.contract.watch_dirs:
            if not root.is_dir():
                raise WebClipperSourceError(
                    f"Web Clipper source directory is not a directory: {root}"
                )

    def _discover_targets(self) -> list[tuple[Path, Path, str]]:
        targets: list[tuple[Path, Path, str]] = []
        for root in self.contract.note_dirs:
            targets.extend(self._discover_root(root, expected_type="note"))
        for root in self.contract.attachment_dirs:
            targets.extend(self._discover_root(root, expected_type="attachment"))
        return targets

    def _discover_root(
        self,
        root: Path,
        *,
        expected_type: str,
    ) -> list[tuple[Path, Path, str]]:
        discovered: list[tuple[Path, Path, str]] = []
        for path in sorted(root.rglob("*"), key=lambda value: str(value)):
            if not path.is_file():
                continue

            file_type = self.contract.classify_path(path)
            if file_type != expected_type:
                logger.debug("Skipping unsupported Web Clipper file: %s", path)
                continue

            discovered.append((path, root, file_type))

        return discovered

    def _index_note_file(
        self,
        path: Path,
        *,
        root: Path,
        lifecycle: CaptureLifecycleService,
        run_id: str,
    ) -> WebClipperFileRecord:
        scanned = self._scan_file(path, root=root, file_type="note")
        parsed_note = self._read_note(path)
        self._upsert_scanned_file(scanned)

        if scanned.is_new_or_changed:
            self._queue_note_artifact(
                parsed_note,
                source_id=scanned.source_id,
                size_bytes=scanned.size_bytes,
                sha256=scanned.sha256,
                lifecycle=lifecycle,
                run_id=run_id,
            )

        return self._note_record(scanned, parsed_note)

    def _queue_note_artifact(
        self,
        parsed_note: WebClipperParsedNote,
        *,
        source_id: str,
        size_bytes: int,
        sha256: str,
        lifecycle: CaptureLifecycleService,
        run_id: str,
    ) -> None:
        artifact = self._build_artifact(
            parsed_note,
            source_path=parsed_note.source_path,
            source_id=source_id,
            size_bytes=size_bytes,
            sha256=sha256,
        )
        self.capture_queue.queue_artifact(
            lifecycle,
            artifact,
            artifact_type="web_clipper",
            source={
                "source_name": "web_clipper",
                "source_type": "web_clipper",
                "collector": "web_clipper_collector",
                "native_source_id": source_id,
                "base_uri": str(self.layout.vault_root),
                "metadata": {
                    "source_relative_path": source_id,
                    "file_type": "note",
                },
            },
            session={
                "session_type": "web_clipper_scan",
                "native_session_id": f"web_clipper:{run_id}",
                "started_at": run_id,
                "metadata": {"note_roots": [str(root) for root in self.contract.note_dirs]},
            },
            event={
                "event_type": "web_clipper_note",
                "native_event_id": source_id,
                "occurred_at": artifact.created_at,
                "captured_at": artifact.ingested_at,
                "provenance": {"collector": "web_clipper_collector"},
            },
            raw_path=parsed_note.source_path,
        )
        if self.db.get_ingestion_entry(artifact.id) is None:
            raise RuntimeError(
                f"Failed to queue Web Clipper note for ingestion: {parsed_note.source_path}"
            )

    def _index_attachment_file(
        self,
        path: Path,
        *,
        root: Path,
    ) -> WebClipperFileRecord:
        scanned = self._scan_file(path, root=root, file_type="attachment")
        self._upsert_scanned_file(scanned)
        managed_path, attachment_asset_type, should_stage = (
            self._attachment_stage_plan(scanned)
        )
        if should_stage:
            try:
                self._get_asset_publisher().publish_file(
                    path,
                    managed_path,
                    asset_type=attachment_asset_type,
                )
            except StagedAssetValidationError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                raise RuntimeError(
                    f"Failed to stage Web Clipper attachment {path}: {exc}"
                ) from exc

        return self._attachment_record(scanned, managed_path=managed_path)

    def _get_asset_publisher(self) -> StagedAssetPublisher:
        if self._asset_publisher is None:
            self._asset_publisher = StagedAssetPublisher(
                self.config,
                layout=self.layout,
            )
        return self._asset_publisher

    def _plan_note_file(
        self,
        path: Path,
        *,
        root: Path,
    ) -> WebClipperFileRecord:
        scanned = self._scan_file(path, root=root, file_type="note")
        return self._note_record(
            scanned,
            self._read_note(path),
            would_queue=scanned.is_new_or_changed,
        )

    def _plan_attachment_file(
        self,
        path: Path,
        *,
        root: Path,
    ) -> WebClipperFileRecord:
        scanned = self._scan_file(path, root=root, file_type="attachment")
        managed_path, _asset_type, would_stage = self._attachment_stage_plan(scanned)
        return self._attachment_record(
            scanned,
            managed_path=managed_path,
            would_stage=would_stage,
        )

    def _scan_file(
        self,
        path: Path,
        *,
        root: Path,
        file_type: str,
    ) -> _ScannedFile:
        self._ensure_safe_source_path(path)
        stat = path.stat()
        size_bytes = stat.st_size
        updated_at = datetime.fromtimestamp(stat.st_mtime).isoformat()
        sha256 = self._sha256_file(path)
        source_id = str(path.relative_to(self.layout.vault_root))
        existing = self.db.get_file_entry(str(path))
        is_new_or_changed = (
            existing is None
            or existing.file_type != file_type
            or existing.size_bytes != size_bytes
            or existing.hash != sha256
            or existing.source_id != source_id
        )
        return _ScannedFile(
            path=path,
            root=root,
            source_id=source_id,
            file_type=file_type,
            size_bytes=size_bytes,
            sha256=sha256,
            updated_at=updated_at,
            is_new_or_changed=is_new_or_changed,
        )

    def _read_note(self, path: Path) -> WebClipperParsedNote:
        try:
            source_text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            raise WebClipperMarkdownError(
                f"Failed to decode Web Clipper note {path}: {exc}"
            ) from exc
        return self._parse_note(path, source_text)

    def _upsert_scanned_file(self, scanned: _ScannedFile) -> None:
        file_meta = FileMetadata(
            path=str(scanned.path),
            file_type=scanned.file_type,
            size_bytes=scanned.size_bytes,
            hash=scanned.sha256,
            updated_at=scanned.updated_at,
            source_id=scanned.source_id,
        )
        if not self.db.upsert_file(file_meta):
            raise RuntimeError(f"Failed to index Web Clipper file: {scanned.path}")

    def _note_record(
        self,
        scanned: _ScannedFile,
        parsed_note: WebClipperParsedNote,
        *,
        would_queue: bool = False,
    ) -> WebClipperFileRecord:
        return WebClipperFileRecord(
            path=scanned.path,
            root=scanned.root,
            source_id=scanned.source_id,
            file_type=scanned.file_type,
            size_bytes=scanned.size_bytes,
            sha256=scanned.sha256,
            updated_at=scanned.updated_at,
            is_new_or_changed=scanned.is_new_or_changed,
            artifact=self._build_artifact(
                parsed_note,
                source_path=scanned.path,
                source_id=scanned.source_id,
                size_bytes=scanned.size_bytes,
                sha256=scanned.sha256,
            ),
            would_queue=would_queue,
        )

    def _attachment_stage_plan(
        self,
        scanned: _ScannedFile,
    ) -> tuple[Path, str, bool]:
        managed_path = self._managed_attachment_path(scanned.source_id)
        asset_type = self._attachment_asset_type(scanned.path)
        same_path = managed_path.resolve() == scanned.path.resolve()
        should_stage = (
            not same_path
            and (
                scanned.is_new_or_changed
                or not managed_path.exists()
                or not validate_existing_asset(managed_path, asset_type=asset_type)
            )
        )
        return managed_path, asset_type, should_stage

    def _attachment_record(
        self,
        scanned: _ScannedFile,
        *,
        managed_path: Path,
        would_stage: bool = False,
    ) -> WebClipperFileRecord:
        return WebClipperFileRecord(
            path=scanned.path,
            root=scanned.root,
            source_id=scanned.source_id,
            file_type=scanned.file_type,
            size_bytes=scanned.size_bytes,
            sha256=scanned.sha256,
            updated_at=scanned.updated_at,
            is_new_or_changed=scanned.is_new_or_changed,
            artifact=self._build_attachment_artifact(
                source_path=scanned.path,
                source_id=scanned.source_id,
                size_bytes=scanned.size_bytes,
                sha256=scanned.sha256,
                managed_path=managed_path,
            ),
            managed_path=managed_path,
            would_stage=would_stage,
        )

    def _parse_note(self, path: Path, source_text: str) -> WebClipperParsedNote:
        try:
            return parse_web_clipper_markdown(source_text, source_path=path)
        except (WebClipperFrontmatterError, WebClipperMarkdownError):
            raise
        except Exception as exc:  # pragma: no cover - defensive
            raise WebClipperMarkdownError(
                f"Failed to parse Web Clipper note {path}: {exc}"
            ) from exc

    def _build_artifact(
        self,
        parsed_note: WebClipperParsedNote,
        *,
        source_path: Path,
        source_id: str,
        size_bytes: int,
        sha256: str,
    ) -> WebClipperArtifact:
        return WebClipperArtifact(
            id=f"webclip:{source_id}",
            source_type="web_clipper",
            raw_content=parsed_note.raw_content,
            created_at=parsed_note.frontmatter.get("created")
            if isinstance(parsed_note.frontmatter.get("created"), str)
            else None,
            ingested_at=datetime.now().isoformat(),
            source_path=str(source_path),
            source_relative_path=source_id,
            file_type="note",
            title=parsed_note.title,
            frontmatter=parsed_note.frontmatter,
            body=parsed_note.body,
            source_checksum=sha256,
            source_size_bytes=size_bytes,
            source_language=parsed_note.source_language,
            source_url=parsed_note.source_url,
            tags=self._extract_tags(parsed_note.frontmatter),
            custom_metadata={
                "source_kind": "web_clipper",
                "source_path": str(source_path),
                "source_relative_path": source_id,
                "frontmatter_keys": sorted(parsed_note.frontmatter.keys()),
            },
        )

    def _build_attachment_artifact(
        self,
        *,
        source_path: Path,
        source_id: str,
        size_bytes: int,
        sha256: str,
        managed_path: Path,
    ) -> WebClipperArtifact:
        return WebClipperArtifact(
            id=f"webclip:{source_id}",
            source_type="web_clipper",
            raw_content="",
            ingested_at=datetime.now().isoformat(),
            source_path=str(source_path),
            source_relative_path=source_id,
            file_type="attachment",
            title=source_path.stem,
            frontmatter={},
            body="",
            source_checksum=sha256,
            source_size_bytes=size_bytes,
            capabilities=("binary_attachment",),
            output_paths={"vault": str(managed_path)},
            custom_metadata={
                "source_kind": "web_clipper",
                "source_path": str(source_path),
                "source_relative_path": source_id,
                "attachment_extension": source_path.suffix.lower(),
                "managed_path": str(managed_path),
            },
        )

    def _extract_tags(self, frontmatter: dict[str, object]) -> list[str]:
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

    def _ensure_safe_source_path(self, path: Path) -> None:
        vault_root = self.layout.vault_root.resolve()
        resolved_path = path.resolve()
        try:
            resolved_path.relative_to(vault_root)
        except ValueError as exc:
            raise ValueError(
                f"Web Clipper source path escapes the vault root: {path}"
            ) from exc

    def _managed_attachment_path(self, source_id: str) -> Path:
        return self.layout.vault_root / source_id

    def _attachment_asset_type(self, path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix == ".pdf":
            return "pdf"
        if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
            return "image"
        if suffix in {".mp4", ".mov"}:
            return "video"
        return "binary"

    def _sha256_file(self, path: Path, *, chunk_size: int = 1024 * 1024) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(chunk_size)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()
