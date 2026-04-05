"""
Base KnowledgeArtifact class for Thoth.
All ingestible entities inherit from this.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any


@dataclass
class KnowledgeArtifact:
    """Base class for all ingestible knowledge entities."""

    id: str = ""  # Unique identifier
    source_type: str = "generic"  # 'twitter', 'arxiv', 'github', 'hermes', 'financial', etc.
    raw_content: str = ""  # Original content (text, JSON, HTML)
    created_at: Optional[str] = None  # When artifact was created (source time)
    ingested_at: Optional[str] = None  # When artifact entered Thoth
    processing_status: str = "pending"  # 'pending', 'processing', 'processed', 'failed'

    # Capability flags - what can this artifact provide?
    capabilities: Tuple[str, ...] = field(default_factory=tuple)
    # Examples: ('media', 'urls', 'transcription', 'llm_summary', 'embedding')

    # Metadata
    tags: List[str] = field(default_factory=list)
    importance_score: Optional[float] = None
    custom_metadata: Dict[str, Any] = field(default_factory=dict)

    # Output tracking
    output_paths: Dict[str, str] = field(default_factory=dict)  # {'markdown': 'path/to/file.md'}

    def __post_init__(self):
        """Ensure default values are set for lists and dicts."""
        if self.tags is None:
            self.tags = []
        if self.custom_metadata is None:
            self.custom_metadata = {}
        if self.output_paths is None:
            self.output_paths = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "source_type": self.source_type,
            "raw_content": self.raw_content,
            "created_at": self.created_at,
            "ingested_at": self.ingested_at,
            "processing_status": self.processing_status,
            "capabilities": list(self.capabilities),
            "tags": self.tags,
            "importance_score": self.importance_score,
            "custom_metadata": self.custom_metadata,
            "output_paths": self.output_paths,
        }
