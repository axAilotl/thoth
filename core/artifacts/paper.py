"""
PaperArtifact - Research paper knowledge entity.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Mapping

from .base import KnowledgeArtifact


def _coerce_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


@dataclass
class PaperArtifact(KnowledgeArtifact):
    """Research paper discovered by Hermes or manual import."""

    source_type: str = "arxiv"  # or 'semantic_scholar', 'openreview'
    capabilities: Tuple[str, ...] = (
        "pdf_download",
        "llm_summary",
        "embedding",
        "citation_graph",
    )

    # Paper-specific fields
    title: str = ""
    authors: List[str] = field(default_factory=list)
    abstract: str = ""
    doi: Optional[str] = None
    arxiv_id: Optional[str] = None
    pdf_url: Optional[str] = None
    venue: Optional[str] = None
    published_at: Optional[str] = None
    references: List[Any] = field(default_factory=list)
    citations: List[Any] = field(default_factory=list)
    source_provider: Optional[str] = None
    citations_count: Optional[int] = None
    relevance_score: Optional[float] = None

    def __post_init__(self):
        super().__post_init__()
        if self.authors is None:
            self.authors = []
        if self.references is None:
            self.references = []
        if self.citations is None:
            self.citations = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = super().to_dict()
        data.update(
            {
                "title": self.title,
                "authors": self.authors,
                "abstract": self.abstract,
                "doi": self.doi,
                "arxiv_id": self.arxiv_id,
                "pdf_url": self.pdf_url,
                "venue": self.venue,
                "published_at": self.published_at,
                "references": self.references,
                "citations": self.citations,
                "source_provider": self.source_provider,
                "citations_count": self.citations_count,
                "relevance_score": self.relevance_score,
            }
        )
        return data

    @classmethod
    def from_queue_payload(cls, payload: Mapping[str, Any]) -> "PaperArtifact":
        """Build a paper artifact from persisted queue payload data."""
        artifact_payload = dict(payload)
        raw_content = artifact_payload.get("raw_content")
        if raw_content is None:
            raw_content = json.dumps(artifact_payload, ensure_ascii=False)
        elif not isinstance(raw_content, str):
            raw_content = json.dumps(raw_content, ensure_ascii=False)

        paper_id = str(
            artifact_payload.get("id")
            or artifact_payload.get("artifact_id")
            or artifact_payload.get("arxiv_id")
            or ""
        ).strip()
        if not paper_id:
            raise ValueError("paper queue payload missing id")

        return cls(
            id=paper_id,
            source_type=str(artifact_payload.get("source_type") or artifact_payload.get("source") or "arxiv"),
            raw_content=raw_content,
            created_at=str(artifact_payload.get("created_at") or artifact_payload.get("published") or artifact_payload.get("updated_at") or ""),
            ingested_at=str(artifact_payload.get("ingested_at") or artifact_payload.get("created_at") or ""),
            processing_status=str(artifact_payload.get("processing_status") or "pending"),
            title=str(artifact_payload.get("title") or ""),
            authors=list(artifact_payload.get("authors") or []),
            abstract=str(artifact_payload.get("abstract") or ""),
            doi=artifact_payload.get("doi"),
            arxiv_id=artifact_payload.get("arxiv_id") or paper_id,
            pdf_url=artifact_payload.get("pdf_url"),
            venue=artifact_payload.get("venue"),
            published_at=artifact_payload.get("published_at")
            or artifact_payload.get("published")
            or artifact_payload.get("publication_date"),
            references=_coerce_list(artifact_payload.get("references")),
            citations=_coerce_list(artifact_payload.get("citations")),
            source_provider=artifact_payload.get("source_provider")
            or artifact_payload.get("source")
            or artifact_payload.get("source_type"),
            citations_count=artifact_payload.get("citations_count"),
            relevance_score=artifact_payload.get("relevance_score"),
            **cls.base_fields_from_payload(artifact_payload),
        )
