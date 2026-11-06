"""
ConversationArtifact - AI conversation entity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any

from .base import KnowledgeArtifact


@dataclass
class ConversationArtifact(KnowledgeArtifact):
    """AI conversation export (Claude, ChatGPT, etc.)."""

    source_type: str = "claude_conversation"
    capabilities: Tuple[str, ...] = ("embedding", "llm_summary", "code_extraction")

    messages: List[Dict[str, str]] = field(default_factory=list)
    model: Optional[str] = None
    tokens_used: Optional[int] = None
    title: str = ""
    body: str = ""
    source_url: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        if self.messages is None:
            self.messages = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = super().to_dict()
        data.update(
            {
                "messages": self.messages,
                "model": self.model,
                "tokens_used": self.tokens_used,
                "title": self.title,
                "body": self.body,
                "source_url": self.source_url,
            }
        )
        return data
