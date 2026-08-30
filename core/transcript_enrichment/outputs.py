"""Explicit output types that can be requested for transcript enrichment."""

from __future__ import annotations

from enum import Enum


class TranscriptOutput(str, Enum):
    """A durable output that can be produced from a transcript source.

    - ``TRANSCRIPT``: normalized, locally derived transcript text.
    - ``SUMMARY``: a concise summary (expensive; requires an injected summarizer
      unless the source already supplied one).
    - ``CLASSIFICATION``: structured tags (expensive; requires an injected
      classifier unless the source already supplied tags).
    """

    TRANSCRIPT = "transcript"
    SUMMARY = "summary"
    CLASSIFICATION = "classification"

    @classmethod
    def values(cls) -> set[str]:
        return {member.value for member in cls}
