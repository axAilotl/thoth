"""Tests for processor identity and local normalizer."""

from __future__ import annotations

import pytest

from core.artifacts import TranscriptArtifact
from core.transcript_enrichment import (
    LocalTranscriptNormalizer,
    ProcessorIdentity,
    TranscriptIdentityError,
)
from core.transcript_enrichment.identity import (
    is_source_provided_identity,
    source_provided_classification_identity,
    source_provided_summary_identity,
)


def test_local_normalizer_preserves_colon_led_prose():
    raw = "Warning: do not strip this line.\nNote: this should stay too."
    normalizer = LocalTranscriptNormalizer()
    normalized = normalizer.normalize(raw, TranscriptArtifact())
    assert "Warning:" in normalized
    assert "Note:" in normalized


def test_local_normalizer_strips_speaker_prefixes_and_timestamps():
    raw = """[00:00:00] Speaker 0: First sentence.
[00:00:05] Speaker 0: Second sentence.

[00:00:10] Speaker 1: New paragraph."""
    normalizer = LocalTranscriptNormalizer()
    normalized = normalizer.normalize(raw, TranscriptArtifact())
    assert "[00:00:00]" not in normalized
    assert "Speaker 0:" not in normalized
    assert "First sentence. Second sentence." in normalized
    assert "New paragraph." in normalized


def test_processor_identity_rejects_blank_name():
    with pytest.raises(TranscriptIdentityError, match="processor_name"):
        ProcessorIdentity("", "1", "1", "1", "m", "p")


def test_processor_identity_rejects_blank_version():
    with pytest.raises(TranscriptIdentityError, match="processor_version"):
        ProcessorIdentity("name", "", "1", "1", "m", "p")


def test_processor_identity_rejects_blank_prompt_version():
    with pytest.raises(TranscriptIdentityError, match="prompt_version"):
        ProcessorIdentity("name", "1", "   ", "1", "m", "p")


def test_processor_identity_rejects_blank_config_version():
    with pytest.raises(TranscriptIdentityError, match="config_version"):
        ProcessorIdentity("name", "1", "1", None, "m", "p")  # type: ignore[arg-type]


def test_processor_identity_rejects_blank_model():
    with pytest.raises(TranscriptIdentityError, match="model"):
        ProcessorIdentity("name", "1", "1", "1", "", "p")


def test_processor_identity_rejects_blank_provider():
    with pytest.raises(TranscriptIdentityError, match="provider"):
        ProcessorIdentity("name", "1", "1", "1", "m", "")


def test_processor_identity_allows_none_sentinel_for_local_processor():
    identity = ProcessorIdentity(
        processor_name="local",
        processor_version="1",
        prompt_version="1",
        config_version="1",
        model="none",
        provider="none",
    )
    assert identity.model == "none"


def test_source_provided_identities_are_valid_and_honest():
    summary_identity = source_provided_summary_identity()
    classification_identity = source_provided_classification_identity()
    assert is_source_provided_identity(summary_identity)
    assert is_source_provided_identity(classification_identity)
    assert summary_identity.model == "none"
    assert summary_identity.provider == "none"
    assert classification_identity.model == "none"
    assert classification_identity.provider == "none"


def test_processor_identity_from_mapping_rejects_unknown_keys():
    value = LocalTranscriptNormalizer().identity().to_dict()
    value["unexpected"] = "not-an-extension-point"

    with pytest.raises(TranscriptIdentityError, match="contain exactly"):
        ProcessorIdentity.from_mapping(value)


def test_processor_identity_from_mapping_rejects_non_string_values():
    value = LocalTranscriptNormalizer().identity().to_dict()
    value["processor_version"] = 1

    with pytest.raises(TranscriptIdentityError, match="must be a string"):
        ProcessorIdentity.from_mapping(value)
