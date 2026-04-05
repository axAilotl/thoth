"""
RepositoryArtifact - GitHub/HuggingFace repository entity.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Mapping

from .base import KnowledgeArtifact


@dataclass
class RepositoryArtifact(KnowledgeArtifact):
    """GitHub/HuggingFace repository."""

    source_type: str = "github"  # or 'huggingface'
    capabilities: Tuple[str, ...] = (
        "readme_download",
        "llm_summary",
        "code_index",
        "embedding",
    )

    repo_name: str = ""
    description: str = ""
    stars: int = 0
    language: Optional[str] = None
    topics: List[str] = field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()
        if self.topics is None:
            self.topics = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = super().to_dict()
        data.update(
            {
                "repo_name": self.repo_name,
                "description": self.description,
                "stars": self.stars,
                "language": self.language,
                "topics": self.topics,
            }
        )
        return data

    @classmethod
    def from_queue_payload(cls, payload: Mapping[str, Any]) -> "RepositoryArtifact":
        """Build a repository artifact from persisted queue payload data."""
        artifact_payload = dict(payload)
        raw_content = artifact_payload.get("raw_content")
        if raw_content is None:
            raw_content = json.dumps(artifact_payload, ensure_ascii=False)
        elif not isinstance(raw_content, str):
            raw_content = json.dumps(raw_content, ensure_ascii=False)

        repo_id = str(
            artifact_payload.get("id")
            or artifact_payload.get("artifact_id")
            or artifact_payload.get("repo_name")
            or artifact_payload.get("full_name")
            or ""
        ).strip()
        if not repo_id:
            raise ValueError("repository queue payload missing id")

        return cls(
            id=repo_id,
            source_type=str(artifact_payload.get("source_type") or artifact_payload.get("source") or "github"),
            raw_content=raw_content,
            created_at=str(artifact_payload.get("created_at") or artifact_payload.get("updated_at") or ""),
            ingested_at=str(artifact_payload.get("ingested_at") or artifact_payload.get("created_at") or ""),
            processing_status=str(artifact_payload.get("processing_status") or "pending"),
            repo_name=str(artifact_payload.get("repo_name") or artifact_payload.get("full_name") or repo_id),
            description=str(artifact_payload.get("description") or ""),
            stars=int(artifact_payload.get("stars", 0) or 0),
            language=artifact_payload.get("language"),
            topics=list(artifact_payload.get("topics") or []),
        )
