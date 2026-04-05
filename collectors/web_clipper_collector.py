"""
Web Clipper collector - indexes explicit source directories under the raw vault.

This collector only scans the configured allowlist from the Web Clipper source
contract. It does not expand beyond those roots, and it does not parse or mutate
source documents.
"""

from __future__ import annotations

import json
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from core.config import Config
from core.artifacts.web_clipper import WebClipperArtifact
from core.metadata_db import (
    FileMetadata,
    IngestionQueueEntry,
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


class WebClipperCollector:
    """Index files from the explicit Web Clipper allowlist."""

    def __init__(
        self,
        config: Config,
        *,
        layout: PathLayout | None = None,
        contract: WebClipperSourceContract | None = None,
        db: Optional[MetadataDB] = None,
    ):
        self.config = config
        self.layout = layout or build_path_layout(config)
        self.contract = contract or build_web_clipper_contract(config, layout=self.layout)
        self.db = db or get_metadata_db()
        self.asset_publisher = StagedAssetPublisher(config, layout=self.layout)

        self._validate_roots()

    def collect(self) -> List[WebClipperFileRecord]:
        """Scan the configured allowlist and upsert file metadata."""
        discovered: List[WebClipperFileRecord] = []

        for root in self.contract.note_dirs:
            discovered.extend(self._scan_root(root, expected_type="note"))

        for root in self.contract.attachment_dirs:
            discovered.extend(self._scan_root(root, expected_type="attachment"))

        return discovered

    def _validate_roots(self) -> None:
        missing = [root for root in self.contract.watch_dirs if not root.exists()]
        if missing:
            formatted = ", ".join(str(path) for path in missing)
            raise ValueError(f"Web Clipper source directories do not exist: {formatted}")
        for root in self.contract.watch_dirs:
            if not root.is_dir():
                raise ValueError(f"Web Clipper source directory is not a directory: {root}")

    def _scan_root(self, root: Path, *, expected_type: str) -> List[WebClipperFileRecord]:
        discovered: List[WebClipperFileRecord] = []
        for path in sorted(root.rglob("*"), key=lambda value: str(value)):
            if not path.is_file():
                continue

            file_type = self.contract.classify_path(path)
            if file_type != expected_type:
                logger.debug("Skipping unsupported Web Clipper file: %s", path)
                continue

            if file_type == "note":
                discovered.append(self._index_note_file(path, root=root))
            elif file_type == "attachment":
                discovered.append(self._index_attachment_file(path, root=root))
            else:
                logger.debug("Skipping unsupported Web Clipper file: %s", path)

        return discovered

    def _index_note_file(self, path: Path, *, root: Path) -> WebClipperFileRecord:
        self._ensure_safe_source_path(path)
        try:
            source_text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            raise WebClipperMarkdownError(
                f"Failed to decode Web Clipper note {path}: {exc}"
            ) from exc
        parsed_note = self._parse_note(path, source_text)

        stat = path.stat()
        size_bytes = stat.st_size
        updated_at = datetime.fromtimestamp(stat.st_mtime).isoformat()
        sha256 = self._sha256_file(path)
        source_id = str(path.relative_to(self.layout.raw_root))
        file_type = "note"
        existing = self.db.get_file_entry(str(path))
        is_new_or_changed = (
            existing is None
            or existing.file_type != file_type
            or existing.size_bytes != size_bytes
            or existing.hash != sha256
            or existing.source_id != source_id
        )

        file_meta = FileMetadata(
            path=str(path),
            file_type=file_type,
            size_bytes=size_bytes,
            hash=sha256,
            updated_at=updated_at,
            source_id=source_id,
        )
        if not self.db.upsert_file(file_meta):
            raise RuntimeError(f"Failed to index Web Clipper file: {path}")

        if is_new_or_changed:
            self._queue_note_artifact(parsed_note, source_id=source_id, size_bytes=size_bytes, sha256=sha256)

        return WebClipperFileRecord(
            path=path,
            root=root,
            source_id=source_id,
            file_type=file_type,
            size_bytes=size_bytes,
            sha256=sha256,
            updated_at=updated_at,
            is_new_or_changed=is_new_or_changed,
            artifact=self._build_artifact(
                parsed_note,
                source_path=path,
                source_id=source_id,
                size_bytes=size_bytes,
                sha256=sha256,
            ),
        )

    def _queue_note_artifact(
        self,
        parsed_note: WebClipperParsedNote,
        *,
        source_id: str,
        size_bytes: int,
        sha256: str,
    ) -> None:
        artifact = self._build_artifact(
            parsed_note,
            source_path=parsed_note.source_path,
            source_id=source_id,
            size_bytes=size_bytes,
            sha256=sha256,
        )
        queue_entry = IngestionQueueEntry(
            artifact_id=artifact.id,
            artifact_type="web_clipper",
            source="web_clipper",
            payload_json=json.dumps(artifact.to_dict(), ensure_ascii=False),
            created_at=artifact.ingested_at,
            capabilities_json=json.dumps(list(artifact.capabilities)),
        )
        if not self.db.upsert_ingestion_entry(queue_entry):
            raise RuntimeError(
                f"Failed to queue Web Clipper note for ingestion: {parsed_note.source_path}"
            )

    def _index_attachment_file(
        self,
        path: Path,
        *,
        root: Path,
    ) -> WebClipperFileRecord:
        self._ensure_safe_source_path(path)
        stat = path.stat()
        size_bytes = stat.st_size
        updated_at = datetime.fromtimestamp(stat.st_mtime).isoformat()
        sha256 = self._sha256_file(path)
        source_id = str(path.relative_to(self.layout.raw_root))
        file_type = "attachment"
        existing = self.db.get_file_entry(str(path))
        is_new_or_changed = (
            existing is None
            or existing.file_type != file_type
            or existing.size_bytes != size_bytes
            or existing.hash != sha256
            or existing.source_id != source_id
        )

        file_meta = FileMetadata(
            path=str(path),
            file_type=file_type,
            size_bytes=size_bytes,
            hash=sha256,
            updated_at=updated_at,
            source_id=source_id,
        )
        if not self.db.upsert_file(file_meta):
            raise RuntimeError(f"Failed to index Web Clipper file: {path}")

        managed_path = self._managed_attachment_path(source_id)
        attachment_asset_type = self._attachment_asset_type(path)
        should_stage = (
            is_new_or_changed
            or not managed_path.exists()
            or not validate_existing_asset(
                managed_path, asset_type=attachment_asset_type
            )
        )
        if should_stage:
            try:
                self.asset_publisher.publish_file(
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

        return WebClipperFileRecord(
            path=path,
            root=root,
            source_id=source_id,
            file_type=file_type,
            size_bytes=size_bytes,
            sha256=sha256,
            updated_at=updated_at,
            is_new_or_changed=is_new_or_changed,
            artifact=self._build_attachment_artifact(
                source_path=path,
                source_id=source_id,
                size_bytes=size_bytes,
                sha256=sha256,
                managed_path=managed_path,
            ),
            managed_path=managed_path,
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
            output_paths={"library": str(managed_path)},
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
        raw_root = self.layout.raw_root.resolve()
        resolved_path = path.resolve()
        try:
            resolved_path.relative_to(raw_root)
        except ValueError as exc:
            raise ValueError(
                f"Web Clipper source path escapes the raw root: {path}"
            ) from exc

    def _managed_attachment_path(self, source_id: str) -> Path:
        return self.layout.library_root / source_id

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
