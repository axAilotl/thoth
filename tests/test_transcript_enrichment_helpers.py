"""Shared helpers for transcript enrichment tests."""

from __future__ import annotations

from pathlib import Path

from core.artifacts import TranscriptArtifact
from core.config import Config
from core.transcript_enrichment import (
    ProcessingMode,
    ProcessingRequest,
    ProcessorIdentity,
    TranscriptOutput,
)

from tests.fixtures.cissa_like_recording import make_cissa_like_recording


def request_with_outputs(
    *outputs: TranscriptOutput,
    mode: ProcessingMode = ProcessingMode.REUSE,
) -> ProcessingRequest:
    return ProcessingRequest(mode=mode, outputs=tuple(outputs))


def make_test_config(tmp_path: Path) -> Config:
    config = Config()
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", str(tmp_path / ".thoth_system"))
    config.set("paths.cache_dir", str(tmp_path / "cache"))
    config.set("paths.raw_dir", str(tmp_path / "vault" / "raw"))
    config.set("paths.library_dir", str(tmp_path / "vault" / "library"))
    config.set("paths.wiki_dir", str(tmp_path / "wiki"))
    config.set("paths.digests_dir", str(tmp_path / "vault" / "_digests"))
    config.set("database.path", str(tmp_path / ".thoth_system" / "meta.db"))
    return config


class CountingFakeSummarizer:
    """Test-only injected summarizer that never calls a provider."""

    def __init__(self) -> None:
        self.call_count = 0

    def identity(self) -> ProcessorIdentity:
        return ProcessorIdentity(
            processor_name="test.counting_fake_summarizer",
            processor_version="1.0.0",
            prompt_version="test-summary-prompt-1",
            config_version="test-summary-config-1",
            model="test-summary-model",
            provider="test-summary-provider",
        )

    def summarize(self, normalized_text: str, artifact: TranscriptArtifact) -> str:
        self.call_count += 1
        title = artifact.title or artifact.transcript_id or artifact.id or "transcript"
        first = (normalized_text.split("\n\n")[0] or "").strip()
        sentence = first if len(first) <= 80 else first[:79].rsplit(" ", 1)[0] + "..."
        return f"Summary of {title}: {sentence or 'no text'}"


class CountingFakeClassifier:
    """Test-only injected classifier that never calls a provider."""

    def __init__(self) -> None:
        self.call_count = 0

    def identity(self) -> ProcessorIdentity:
        return ProcessorIdentity(
            processor_name="test.counting_fake_classifier",
            processor_version="1.0.0",
            prompt_version="test-classification-prompt-1",
            config_version="test-classification-config-1",
            model="test-classification-model",
            provider="test-classification-provider",
        )

    def classify(self, normalized_text: str, artifact: TranscriptArtifact) -> list[str]:
        self.call_count += 1
        text = f"{artifact.title or ''} {normalized_text}".lower()
        tags = []
        for word, tag in [
            ("schema", "schema-design"),
            ("adoption", "adoption"),
            ("deployment", "deployment"),
            ("monitoring", "monitoring"),
            ("container", "containerization"),
            ("infrastructure", "infrastructure"),
            ("cissa", "cissa"),
            ("transcript", "transcript"),
        ]:
            if word in text and tag not in tags:
                tags.append(tag)
        return tags if tags else ["general"]


def make_cissa_artifact(**overrides) -> TranscriptArtifact:
    recording = make_cissa_like_recording()
    return TranscriptArtifact(
        id=overrides.pop("id", recording.artifact_id),
        source_type=recording.source_name,
        raw_transcript=recording.transcript_text,
        title=recording.title,
        **overrides,
    )
