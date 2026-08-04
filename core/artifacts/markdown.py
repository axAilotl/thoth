"""MarkdownArtifact - Canonical artifact for imported markdown captures."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple

from .base import KnowledgeArtifact


@dataclass
class MarkdownArtifact(KnowledgeArtifact):
    """Canonical artifact for markdown imported as capture evidence."""

    source_type: str = "imported_markdown"
    capabilities: Tuple[str, ...] = (
        "markdown",
        "frontmatter",
        "text_extraction",
        "capture_only",
    )

    source_path: str = ""
    source_relative_path: str = ""
    file_type: str = "markdown"
    title: str = ""
    frontmatter: Dict[str, Any] = field(default_factory=dict)
    body: str = ""
    source_checksum: Optional[str] = None
    source_size_bytes: Optional[int] = None
    source_language: Optional[str] = None
    source_url: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        if self.frontmatter is None:
            self.frontmatter = {}

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "source_path": self.source_path,
                "source_relative_path": self.source_relative_path,
                "file_type": self.file_type,
                "title": self.title,
                "frontmatter": self.frontmatter,
                "body": self.body,
                "source_checksum": self.source_checksum,
                "source_size_bytes": self.source_size_bytes,
                "source_language": self.source_language,
                "source_url": self.source_url,
            }
        )
        return data

    @classmethod
    def from_queue_payload(cls, payload: Mapping[str, Any]) -> "MarkdownArtifact":
        """Build an imported markdown artifact from persisted queue payload data."""
        artifact_payload = dict(payload)
        raw_content = artifact_payload.get("raw_content")
        if raw_content is None:
            raw_content = json.dumps(artifact_payload, ensure_ascii=False)
        elif not isinstance(raw_content, str):
            raw_content = json.dumps(raw_content, ensure_ascii=False)

        artifact_id = str(
            artifact_payload.get("id")
            or artifact_payload.get("artifact_id")
            or artifact_payload.get("source_relative_path")
            or artifact_payload.get("source_path")
            or ""
        ).strip()
        if not artifact_id:
            raise ValueError("markdown queue payload missing id")

        frontmatter = artifact_payload.get("frontmatter")
        if not isinstance(frontmatter, dict):
            frontmatter = {}

        base_fields = cls.base_fields_from_payload(artifact_payload)
        return cls(
            id=artifact_id,
            source_type=str(
                artifact_payload.get("source_type")
                or artifact_payload.get("source")
                or "imported_markdown"
            ),
            raw_content=raw_content,
            created_at=artifact_payload.get("created_at"),
            ingested_at=artifact_payload.get("ingested_at"),
            processing_status=str(artifact_payload.get("processing_status") or "pending"),
            source_path=str(
                artifact_payload.get("source_path")
                or artifact_payload.get("source_file")
                or ""
            ),
            source_relative_path=str(
                artifact_payload.get("source_relative_path")
                or artifact_payload.get("source_id")
                or ""
            ),
            file_type=str(artifact_payload.get("file_type") or "markdown"),
            title=str(artifact_payload.get("title") or ""),
            frontmatter=frontmatter,
            body=str(artifact_payload.get("body") or ""),
            source_checksum=artifact_payload.get("source_checksum"),
            source_size_bytes=artifact_payload.get("source_size_bytes"),
            source_language=artifact_payload.get("source_language"),
            source_url=artifact_payload.get("source_url"),
            **base_fields,
        )
