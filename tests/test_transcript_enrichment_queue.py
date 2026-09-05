"""Tests for queue-driven transcript enrichment modes and runtime plumbing."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from core.artifacts import RawPayloadRef, TranscriptArtifact
from core.config import Config
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.metadata_db import IngestionQueueEntry, MetadataDB
from core.path_layout import PathLayout, build_path_layout
from core.transcript_enrichment import (
    ProcessingMode,
    ProcessingRequest,
    TranscriptEnrichmentError,
    TranscriptOutput,
    TranscriptRequestError,
    TranscriptEnrichmentService,
)
from core.transcript_enrichment.request import current_processing_request
from core.wiki_contract import normalize_wiki_slug

from tests.fixtures.cissa_like_recording import make_cissa_like_recording
from tests.test_transcript_enrichment_helpers import (
    CountingFakeClassifier,
    CountingFakeSummarizer,
    make_cissa_artifact,
    make_test_config,
    request_with_outputs,
)


def _make_runtime_and_service(
    tmp_path: Path,
) -> tuple[KnowledgeArtifactRuntime, TranscriptEnrichmentService, Config, PathLayout, MetadataDB]:
    config = make_test_config(tmp_path)
    config.set("wiki.publish_source_pages", True)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    enrichment_service = TranscriptEnrichmentService(
        config,
        layout=layout,
        db=db,
        summarizer=summarizer,
        classifier=classifier,
    )
    runtime = KnowledgeArtifactRuntime(
        config,
        layout=layout,
        db=db,
        transcript_enrichment_service=enrichment_service,
    )
    return runtime, enrichment_service, config, layout, db


def _enqueue_artifact(
    db: MetadataDB,
    artifact: TranscriptArtifact,
    source: str,
    *,
    processing_request: ProcessingRequest | None = None,
) -> IngestionQueueEntry:
    payload = artifact.to_dict()
    if processing_request is not None:
        payload["processing_request"] = processing_request.to_dict()
    entry = IngestionQueueEntry(
        artifact_id=artifact.id,
        artifact_type="transcript",
        source=source,
        payload_json=json.dumps(payload),
        created_at=artifact.created_at or "2026-08-29T21:42:18Z",
    )
    assert db.upsert_ingestion_entry(entry)
    return entry


def test_malformed_processing_request_fails_closed():
    with pytest.raises(TranscriptRequestError):
        ProcessingRequest.from_payload({"mode": 123})
    with pytest.raises(TranscriptRequestError):
        ProcessingRequest.from_payload(["reuse"])
    with pytest.raises(TranscriptRequestError):
        ProcessingRequest.from_payload("unknown_mode")


def test_queue_reuse_mode_reuses_cache_without_processor_calls(tmp_path: Path):
    runtime, enrichment_service, _config, layout, db = _make_runtime_and_service(tmp_path)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    _enqueue_artifact(db, artifact, "cissa", processing_request=request)
    first = asyncio.run(runtime.process_pending_ingestions_once())
    assert first[0].details["mode"] == "reuse"
    assert first[0].details["cache_hit"] is False
    assert first[0].details["rerun_requested"] is False
    assert first[0].details["version"] == "v1"
    assert enrichment_service.summarizer.call_count == 1

    _enqueue_artifact(db, artifact, "cissa", processing_request=request)
    second = asyncio.run(runtime.process_pending_ingestions_once())
    assert second[0].details["mode"] == "reuse"
    assert second[0].details["cache_hit"] is True
    assert second[0].details["rerun_requested"] is False
    assert second[0].details["version"] == "v1"
    assert enrichment_service.summarizer.call_count == 1


def test_queue_recompute_mode_calls_processor_and_bumps_version(tmp_path: Path):
    runtime, enrichment_service, _config, layout, db = _make_runtime_and_service(tmp_path)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    _enqueue_artifact(db, artifact, "cissa", processing_request=request)
    first = asyncio.run(runtime.process_pending_ingestions_once())
    assert first[0].details["version"] == "v1"
    assert first[0].details["rerun_requested"] is False

    _enqueue_artifact(
        db,
        artifact,
        "cissa",
        processing_request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
            mode=ProcessingMode.RECOMPUTE,
        ),
    )
    recompute = asyncio.run(runtime.process_pending_ingestions_once())
    assert recompute[0].details["mode"] == "recompute"
    assert recompute[0].details["version"] == "v2"
    assert recompute[0].details["rerun_requested"] is True
    assert enrichment_service.summarizer.call_count == 2


def test_queue_rebuild_mode_recreates_same_v1_path_without_processor_calls(
    tmp_path: Path,
):
    runtime, enrichment_service, _config, layout, db = _make_runtime_and_service(tmp_path)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    _enqueue_artifact(db, artifact, "cissa", processing_request=request)
    first = asyncio.run(runtime.process_pending_ingestions_once())
    transcript_path = layout.vault_root / first[0].details["derivatives"]["transcript"]
    assert transcript_path.name.endswith("_v1.md")
    transcript_path.unlink()

    _enqueue_artifact(
        db,
        artifact,
        "cissa",
        processing_request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
            mode=ProcessingMode.REBUILD_PROJECTION,
        ),
    )
    rebuild = asyncio.run(runtime.process_pending_ingestions_once())
    assert rebuild[0].details["mode"] == "rebuild_projection"
    assert rebuild[0].details["version"] == "v1"
    assert rebuild[0].details["rerun_requested"] is False
    assert enrichment_service.summarizer.call_count == 1
    rebuilt_path = layout.vault_root / rebuild[0].details["derivatives"]["transcript"]
    assert rebuilt_path == transcript_path
    assert rebuilt_path.exists()
    assert rebuilt_path.name.endswith("_v1.md")


def test_queue_malformed_processing_request_routes_to_review(tmp_path: Path):
    runtime, _enrichment_service, _config, layout, db = _make_runtime_and_service(tmp_path)
    artifact = make_cissa_artifact()

    payload = artifact.to_dict()
    payload["processing_request"] = {"mode": 123}
    entry = IngestionQueueEntry(
        artifact_id=artifact.id,
        artifact_type="transcript",
        source="cissa",
        payload_json=json.dumps(payload),
        created_at="2026-08-29T21:42:18Z",
    )
    assert db.upsert_ingestion_entry(entry)
    results = asyncio.run(runtime.process_pending_ingestions_once())
    assert results[0].status == "needs_review"
    assert "processing_request" in results[0].details.get("error", "")


def test_queue_dispatch_artifact_signature_unchanged_for_monkeypatches(
    tmp_path: Path,
):
    config = make_test_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)

    async def fake_dispatch(artifact):
        return type(
            "Result", (), {"artifact_id": artifact.id, "status": "processed"}
        )()

    runtime.dispatch_artifact = fake_dispatch
    result = asyncio.run(runtime.dispatch_artifact(artifact=TranscriptArtifact(id="x")))
    assert result.status == "processed"


def test_context_var_no_cross_talk_between_concurrent_entries(tmp_path: Path):
    """Two concurrent transcript entries with different outputs must not interfere."""
    runtime, enrichment_service, _config, layout, db = _make_runtime_and_service(tmp_path)

    artifact_a = make_cissa_artifact(id="artifact-a")
    artifact_b = make_cissa_artifact(id="artifact-b")

    _enqueue_artifact(
        db,
        artifact_a,
        "cissa",
        processing_request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT, TranscriptOutput.SUMMARY
        ),
    )
    _enqueue_artifact(
        db,
        artifact_b,
        "cissa",
        processing_request=request_with_outputs(TranscriptOutput.TRANSCRIPT),
    )

    results = asyncio.run(runtime.process_pending_ingestions_once(concurrency=2))
    statuses = {r.artifact_id: r for r in results}
    assert statuses["artifact-a"].status == "processed"
    assert statuses["artifact-b"].status == "processed"
    assert enrichment_service.summarizer.call_count == 1


def test_context_var_request_reset_after_failure(tmp_path: Path):
    """A failed entry must not leave its processing request in the current task."""
    runtime, enrichment_service, _config, layout, db = _make_runtime_and_service(tmp_path)
    # Remove summarizer so the summary request fails closed inside dispatch.
    enrichment_service.summarizer = None

    artifact = make_cissa_artifact()
    entry = _enqueue_artifact(
        db,
        artifact,
        "cissa",
        processing_request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT, TranscriptOutput.SUMMARY
        ),
    )

    with pytest.raises(TranscriptEnrichmentError, match="no summarizer injected"):
        asyncio.run(runtime.process_ingestion_entry(entry))
    assert current_processing_request() is None


def test_ingestion_runtime_processes_cissa_fixture_to_wiki_and_search(tmp_path: Path):
    runtime, enrichment_service, _config, layout, db = _make_runtime_and_service(tmp_path)
    recording = make_cissa_like_recording()

    audio_path = layout.vault_root / recording.audio_path
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(recording.audio_blob)

    source_path = layout.vault_root / recording.transcript_path
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(recording.transcript_text, encoding="utf-8")

    artifact = TranscriptArtifact(
        id=recording.artifact_id,
        source_type=recording.source_type,
        raw_content=json.dumps({"transcript_text": recording.transcript_text}),
        created_at=recording.started_at,
        ingested_at="2026-08-29T21:42:18Z",
        transcript_id=recording.artifact_id,
        title=recording.title,
        transcript_path=recording.transcript_path,
        raw_transcript=recording.transcript_text,
        raw_payload=RawPayloadRef(
            path=recording.audio_path,
            sha256=recording.audio_sha256,
            size_bytes=len(recording.audio_blob),
            media_type="audio/wav",
        ),
        session_id=recording.session_id,
        device_id=recording.device_id,
        language=recording.language,
    )

    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )
    _enqueue_artifact(db, artifact, recording.source_name, processing_request=request)
    results = asyncio.run(runtime.process_pending_ingestions_once())

    assert len(results) == 1
    assert results[0].status == "processed"
    assert results[0].details["cache_hit"] is False
    assert results[0].details["version"] == "v1"
    assert results[0].details["rerun_requested"] is False

    expected_slug = f"transcript-{normalize_wiki_slug(recording.artifact_id)}"
    wiki_page = layout.wiki_root / "pages" / f"{expected_slug}.md"
    assert wiki_page.exists()
    wiki_text = wiki_page.read_text(encoding="utf-8")
    assert recording.title in wiki_text
    assert "schema open" in wiki_text

    docs = db.search_archivist_corpus_full_text(query="schema open adoption")
    assert any(recording.artifact_id in str(row[0].candidate_key) for row in docs)
