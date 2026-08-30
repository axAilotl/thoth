"""Processor protocols and production local normalizer."""

from __future__ import annotations

import re
from typing import Protocol

from core.artifacts import TranscriptArtifact

from .identity import ProcessorIdentity


class TranscriptNormalizer(Protocol):
    """Production-capable local transcript normalizer."""

    def identity(self) -> ProcessorIdentity:
        ...

    def normalize(self, raw_text: str, artifact: TranscriptArtifact) -> str:
        """Return normalized transcript text without calling a model/provider."""
        ...


class TranscriptSummarizer(Protocol):
    """Injected processor that produces a summary from a normalized transcript."""

    def identity(self) -> ProcessorIdentity:
        ...

    def summarize(self, normalized_text: str, artifact: TranscriptArtifact) -> str:
        """Return a summary; may call a model/provider."""
        ...


class TranscriptClassifier(Protocol):
    """Injected processor that produces classification tags."""

    def identity(self) -> ProcessorIdentity:
        ...

    def classify(self, normalized_text: str, artifact: TranscriptArtifact) -> list[str]:
        """Return a list of tags; may call a model/provider."""
        ...


class LocalTranscriptNormalizer:
    """Deterministic local normalization: timestamps and speaker prefixes to paragraphs.

    This is intentionally a pure local transform. It performs no model work and
    makes no provider calls. Speaker-label stripping is narrowly scoped to the
    ``Speaker <id>:`` convention so ordinary prose such as ``Warning:`` is
    preserved.
    """

    def identity(self) -> ProcessorIdentity:
        return ProcessorIdentity(
            processor_name="thoth.local_transcript_normalizer",
            processor_version="1.0.0",
            prompt_version="local-timestamp-speaker-removal-1",
            config_version="transcript-normalizer-1",
            model="none",
            provider="none",
        )

    def normalize(self, raw_text: str, artifact: TranscriptArtifact) -> str:
        lines = raw_text.splitlines()
        paragraphs: list[str] = []
        current: list[str] = []

        timestamp_re = re.compile(
            r"^\s*(\[?\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?\]?\s*)+"
        )
        speaker_re = re.compile(r"^\s*Speaker\s+\w+:\s*")

        for line in lines:
            line = line.strip()
            if not line:
                if current:
                    paragraphs.append(" ".join(current))
                    current = []
                continue
            line = timestamp_re.sub("", line)
            line = speaker_re.sub("", line)
            if line:
                current.append(line)

        if current:
            paragraphs.append(" ".join(current))

        return "\n\n".join(paragraphs).strip()
