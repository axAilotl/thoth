"""Tests for canonical source/origin commitments and cache key separation."""

from __future__ import annotations

import pytest

from core.artifacts import RawPayloadRef, TranscriptArtifact
from core.path_layout import build_path_layout
from core.transcript_enrichment import (
    ProcessorIdentity,
    TranscriptEnrichmentService,
    TranscriptOutput,
)
from core.transcript_enrichment.commitments import (
    cache_key,
    origin_commitment,
    source_commitment,
)
from core.metadata_db import MetadataDB

from tests.fixtures.cissa_like_recording import make_cissa_like_recording
from tests.test_transcript_enrichment_helpers import CountingFakeSummarizer, make_test_config


def test_source_commitment_includes_summary_tags_and_context():
    recording = make_cissa_like_recording()
    artifact = TranscriptArtifact(
        id=recording.artifact_id,
        source_type=recording.source_name,
        raw_transcript=recording.transcript_text,
        processed_transcript="processed",
        summary="source summary",
        tags=["source-tag"],
        language="en",
        speaker="Speaker 0",
        session_id="session-1",
        device_id="device-1",
    )
    commit_a = source_commitment(artifact)

    artifact.summary = "different summary"
    commit_b = source_commitment(artifact)
    assert commit_a != commit_b


def test_origin_commitment_includes_raw_payload_and_provenance():
    recording = make_cissa_like_recording()
    artifact = TranscriptArtifact(
        id=recording.artifact_id,
        source_type=recording.source_type,
        raw_transcript=recording.transcript_text,
        title=recording.title,
        raw_payload=RawPayloadRef(
            path=recording.audio_path,
            sha256=recording.audio_sha256,
            size_bytes=len(recording.audio_blob),
            media_type="audio/wav",
        ),
    )
    commit_a = origin_commitment(artifact)

    artifact.raw_payload = RawPayloadRef(
        path=recording.audio_path,
        sha256="different-sha256",
        size_bytes=len(recording.audio_blob),
        media_type="audio/wav",
    )
    commit_b = origin_commitment(artifact)
    assert commit_a != commit_b


def test_cache_key_separates_different_recordings_with_same_text(tmp_path):
    config = make_test_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    recording = make_cissa_like_recording()

    artifact_a = TranscriptArtifact(
        id="recording-a",
        source_type=recording.source_type,
        raw_transcript=recording.transcript_text,
        title=recording.title,
        session_id="session-a",
    )
    artifact_b = TranscriptArtifact(
        id="recording-b",
        source_type=recording.source_type,
        raw_transcript=recording.transcript_text,
        title=recording.title,
        session_id="session-b",
    )

    service = TranscriptEnrichmentService(
        config, layout=layout, db=MetadataDB(str(layout.database_path))
    )
    outputs = (TranscriptOutput.TRANSCRIPT,)
    key_a = cache_key(artifact_a, service.normalizer, outputs, None, None)
    key_b = cache_key(artifact_b, service.normalizer, outputs, None, None)
    assert key_a != key_b


def test_cache_key_changes_when_audio_sha256_changes(tmp_path):
    config = make_test_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    recording = make_cissa_like_recording()

    artifact = TranscriptArtifact(
        id=recording.artifact_id,
        source_type=recording.source_type,
        raw_transcript=recording.transcript_text,
        title=recording.title,
        raw_payload=RawPayloadRef(
            path=recording.audio_path,
            sha256=recording.audio_sha256,
            size_bytes=len(recording.audio_blob),
            media_type="audio/wav",
        ),
    )

    service = TranscriptEnrichmentService(
        config, layout=layout, db=MetadataDB(str(layout.database_path))
    )
    outputs = (TranscriptOutput.TRANSCRIPT,)
    key_original = cache_key(artifact, service.normalizer, outputs, None, None)
    artifact.raw_payload = RawPayloadRef(
        path=recording.audio_path,
        sha256="different-sha256",
        size_bytes=len(recording.audio_blob),
        media_type="audio/wav",
    )
    key_changed = cache_key(artifact, service.normalizer, outputs, None, None)
    assert key_changed != key_original


def test_cache_key_includes_processor_identity(tmp_path):
    recording = make_cissa_like_recording()
    artifact = TranscriptArtifact(
        id=recording.artifact_id,
        source_type=recording.source_name,
        raw_transcript=recording.transcript_text,
        title=recording.title,
    )

    summarizer_a = CountingFakeSummarizer()
    summarizer_b = _DifferentSummarizer()
    config = make_test_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))

    service_a = TranscriptEnrichmentService(
        config, layout=layout, db=db, summarizer=summarizer_a
    )
    service_b = TranscriptEnrichmentService(
        config, layout=layout, db=db, summarizer=summarizer_b
    )

    outputs = (TranscriptOutput.TRANSCRIPT, TranscriptOutput.SUMMARY)
    key_a = cache_key(artifact, service_a.normalizer, outputs, service_a.summarizer, None)
    key_b = cache_key(artifact, service_b.normalizer, outputs, service_b.summarizer, None)
    assert key_a != key_b


class _DifferentSummarizer:
    def identity(self) -> ProcessorIdentity:
        return ProcessorIdentity(
            processor_name="different",
            processor_version="1.0.0",
            prompt_version="different",
            config_version="different",
            model="different",
            provider="different",
        )

    def summarize(self, normalized_text: str, artifact: TranscriptArtifact) -> str:
        return "different"
