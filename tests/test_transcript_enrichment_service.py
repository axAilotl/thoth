"""Tests for TranscriptEnrichmentService behavior."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from core.artifacts import TranscriptArtifact
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.transcript_enrichment import (
    LocalTranscriptNormalizer,
    ProcessingMode,
    ProcessingRequest,
    TranscriptCacheError,
    TranscriptEnrichmentError,
    TranscriptEnrichmentService,
    TranscriptOutput,
    TranscriptRequestError,
    apply_derivatives_to_artifact,
)
from core.transcript_enrichment.identity import source_provided_summary_identity
from core.transcript_enrichment.cache_state import load_cache_state, persist_cache_state
from core.transcript_enrichment.commitments import cache_key
from core.transcript_enrichment.models import TranscriptDerivative, TranscriptDerivativeError
from core.transcript_enrichment.rendering import extract_summary_text
from core.transcript_enrichment.storage import (
    TranscriptStorageError,
    _atomic_write_text,
    derivative_paths_for_artifact,
    resolve_derivative_path,
)

from tests.test_transcript_enrichment_helpers import (
    CountingFakeClassifier,
    CountingFakeSummarizer,
    make_cissa_artifact,
    make_test_config,
    request_with_outputs,
)


def _service(
    tmp_path: Path,
    *,
    summarizer=None,
    classifier=None,
) -> tuple[TranscriptEnrichmentService, Path]:
    config = make_test_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    return (
        TranscriptEnrichmentService(
            config,
            layout=layout,
            db=db,
            summarizer=summarizer,
            classifier=classifier,
        ),
        tmp_path,
    )


def test_service_creates_versioned_derivatives(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()

    result = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
        ),
    )

    assert result.cache_hit is False
    assert result.version == "v1"
    assert result.indexed is True
    assert result.mode == ProcessingMode.REUSE
    assert result.rerun_requested is False
    assert summarizer.call_count == 1
    assert classifier.call_count == 1

    paths = result.derivative_paths()
    assert {"transcript", "summary", "classification"} == set(paths.keys())

    transcript_path = service.layout.vault_root / paths["transcript"]
    summary_path = service.layout.vault_root / paths["summary"]
    classification_path = service.layout.vault_root / paths["classification"]

    assert transcript_path.exists()
    assert summary_path.exists()
    assert classification_path.exists()
    assert "schema open" in transcript_path.read_text(encoding="utf-8")
    assert "Summary of" in summary_path.read_text(encoding="utf-8")

    classification = json.loads(classification_path.read_text(encoding="utf-8"))
    assert "schema-design" in classification["tags"]
    assert classification["version"] == "v1"


def test_service_fails_explicitly_without_injected_summarizer(tmp_path: Path):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact()
    with pytest.raises(
        TranscriptEnrichmentError, match="no summarizer injected"
    ):
        service.enrich(
            artifact,
            request=request_with_outputs(TranscriptOutput.SUMMARY),
        )


def test_service_fails_explicitly_without_injected_classifier(tmp_path: Path):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact()
    with pytest.raises(
        TranscriptEnrichmentError, match="no classifier injected"
    ):
        service.enrich(
            artifact,
            request=request_with_outputs(TranscriptOutput.CLASSIFICATION),
        )


def test_service_does_not_call_injected_processor_unless_requested(
    tmp_path: Path,
):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()

    result = service.enrich(artifact)

    assert result.derivatives
    assert all(d.output_type == "transcript" for d in result.derivatives)
    assert summarizer.call_count == 0
    assert classifier.call_count == 0


def test_service_reuses_cache_without_calling_processors(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    service.enrich(artifact, request=request)
    assert summarizer.call_count == 1
    assert classifier.call_count == 1

    second = service.enrich(artifact, request=request)
    assert second.cache_hit is True
    assert second.version == "v1"
    assert second.rerun_requested is False
    assert summarizer.call_count == 1
    assert classifier.call_count == 1


def test_service_recomputes_on_request(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    first = service.enrich(artifact, request=request)
    assert first.version == "v1"
    assert first.rerun_requested is False

    recompute = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
            mode=ProcessingMode.RECOMPUTE,
        ),
    )
    assert recompute.cache_hit is False
    assert recompute.version == "v2"
    assert recompute.mode == ProcessingMode.RECOMPUTE
    assert recompute.rerun_requested is True
    assert summarizer.call_count == 2
    assert classifier.call_count == 2


def test_service_rebuilds_projection_at_same_path_without_processor_calls(
    tmp_path: Path,
):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    first = service.enrich(artifact, request=request)
    first_path = service.layout.vault_root / first.derivative_paths()["transcript"]
    assert first_path.name.endswith("_v1.md")
    first_path.unlink()
    assert not first_path.exists()

    rebuild = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
            mode=ProcessingMode.REBUILD_PROJECTION,
        ),
    )
    assert rebuild.cache_hit is True
    assert rebuild.mode == ProcessingMode.REBUILD_PROJECTION
    assert rebuild.version == "v1"
    assert rebuild.rerun_requested is False
    assert summarizer.call_count == 1
    assert classifier.call_count == 1

    rebuilt_path = service.layout.vault_root / rebuild.derivative_paths()["transcript"]
    assert rebuilt_path == first_path
    assert rebuilt_path.exists()
    assert rebuilt_path.name.endswith("_v1.md")


def test_rebuild_projection_rebuilds_missing_files_without_bumping_version(
    tmp_path: Path,
):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    first = service.enrich(artifact, request=request)
    first_path = service.layout.vault_root / first.derivative_paths()["transcript"]
    first_path.unlink()

    rebuild = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
            mode=ProcessingMode.REBUILD_PROJECTION,
        ),
    )
    assert rebuild.cache_hit is True
    assert rebuild.version == "v1"
    assert rebuild.rerun_requested is False
    assert summarizer.call_count == 1
    assert classifier.call_count == 1
    assert (service.layout.vault_root / rebuild.derivative_paths()["transcript"]).exists()


def test_reuse_fails_closed_when_derivative_file_is_missing(
    tmp_path: Path,
):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    service.enrich(artifact, request=request)
    first_path = service.layout.vault_root / service.enrich(
        artifact, request=request
    ).derivative_paths()["transcript"]
    first_path.unlink()

    with pytest.raises(TranscriptStorageError, match="missing"):
        service.enrich(artifact, request=request)


def test_rebuild_projection_fails_closed_without_cache(tmp_path: Path):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact()
    with pytest.raises(TranscriptEnrichmentError, match="cannot rebuild projection"):
        service.enrich(
            artifact,
            request=request_with_outputs(
                TranscriptOutput.TRANSCRIPT,
                mode=ProcessingMode.REBUILD_PROJECTION,
            ),
        )


def test_cache_hit_verifies_outer_commitments(tmp_path: Path):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact()

    service.enrich(artifact)
    key = cache_key(artifact, service.normalizer, (TranscriptOutput.TRANSCRIPT,), None, None)
    from core.transcript_enrichment.cache_state import load_cache_state, persist_cache_state

    state = load_cache_state(service.db, key)
    assert state is not None

    state["source_hash"] = "a" * 64
    persist_cache_state(service.db, key, state)

    with pytest.raises(TranscriptCacheError, match="cached source commitment"):
        service.enrich(artifact)


def test_cache_hit_fails_when_derivative_content_is_tampered(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    service.enrich(artifact, request=request)
    key = cache_key(artifact, service.normalizer, request.outputs, summarizer, classifier)
    from core.transcript_enrichment.cache_state import load_cache_state, persist_cache_state

    state = load_cache_state(service.db, key)
    assert state is not None
    # Tamper the cached transcript content while leaving outer commitments intact.
    state["derivatives"][0]["content"] = "tampered content"
    persist_cache_state(service.db, key, state)

    with pytest.raises(TranscriptCacheError, match="content_sha256 mismatch"):
        service.enrich(artifact, request=request)


def test_atomic_writes_are_concurrency_safe(tmp_path: Path):
    path = tmp_path / "file.md"
    _atomic_write_text(path, "content")
    assert path.read_text(encoding="utf-8") == "content"
    assert not list(tmp_path.glob(".file.md.*.tmp"))


def test_concurrent_atomic_writes_to_same_path_are_safe(tmp_path: Path):
    from concurrent.futures import ThreadPoolExecutor

    path = tmp_path / "concurrent.md"

    def writer(label: str):
        _atomic_write_text(path, label)
        return label

    labels = ["a", "b", "c"]
    with ThreadPoolExecutor(max_workers=len(labels)) as executor:
        results = list(executor.map(writer, labels))
    final = path.read_text(encoding="utf-8")
    assert final in labels
    assert not list(tmp_path.glob(".concurrent.md.*.tmp"))
    assert len(results) == len(labels)


def test_apply_derivatives_preserves_source_summary_and_tags(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact(
        processed_transcript="Source-provided processed transcript.",
        summary="Source-provided summary.",
        tags=["source-tag"],
    )
    original_raw = artifact.raw_transcript

    result = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
        ),
    )
    apply_derivatives_to_artifact(artifact, result)

    assert artifact.raw_transcript == original_raw
    assert artifact.processed_transcript == "Source-provided processed transcript."
    assert artifact.summary == "Source-provided summary."
    # Source-provided tags are reused as the classification derivative, so no
    # processor-generated tags are merged in.
    assert artifact.tags == ["source-tag"]


def test_apply_derivatives_is_idempotent(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    result = service.enrich(artifact, request=request)
    apply_derivatives_to_artifact(artifact, result)
    first_outputs = len(artifact.derived_outputs)
    first_relationships = len(artifact.relationships)

    apply_derivatives_to_artifact(artifact, result)
    assert len(artifact.derived_outputs) == first_outputs
    assert len(artifact.relationships) == first_relationships


def test_source_provided_summary_materialized_without_summarizer(tmp_path: Path):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact(summary="Source summary text.")

    result = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT, TranscriptOutput.SUMMARY
        ),
    )

    summary_derivative = next(
        d for d in result.derivatives if d.output_type == "summary"
    )
    assert extract_summary_text(summary_derivative.content) == "Source summary text."
    assert summary_derivative.processor_identity.processor_name == "thoth.source_provided"


def test_source_provided_classification_materialized_without_classifier(
    tmp_path: Path,
):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact(tags=["source-tag", "adoption"])

    result = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT, TranscriptOutput.CLASSIFICATION
        ),
    )

    classification_derivative = next(
        d for d in result.derivatives if d.output_type == "classification"
    )
    parsed = json.loads(classification_derivative.content)
    assert parsed["tags"] == ["source-tag", "adoption"]
    assert (
        classification_derivative.processor_identity.processor_name
        == "thoth.source_provided"
    )


def test_classification_representation_is_stable_on_rebuild(tmp_path: Path):
    service, _ = _service(tmp_path, classifier=CountingFakeClassifier())
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.CLASSIFICATION,
    )

    first = service.enrich(artifact, request=request)
    first_path = service.layout.vault_root / first.derivative_paths()["classification"]
    first_content = first_path.read_text(encoding="utf-8")
    parsed_first = json.loads(first_content)

    rebuild = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.CLASSIFICATION,
            mode=ProcessingMode.REBUILD_PROJECTION,
        ),
    )
    rebuild_path = service.layout.vault_root / rebuild.derivative_paths()["classification"]
    rebuild_content = rebuild_path.read_text(encoding="utf-8")
    parsed_rebuild = json.loads(rebuild_content)

    assert isinstance(parsed_first, dict)
    assert isinstance(parsed_rebuild, dict)
    assert parsed_rebuild["tags"] == parsed_first["tags"]
    assert parsed_rebuild["processor_identity"] == parsed_first["processor_identity"]


def test_request_rejects_strings_unknown_keys_and_duplicate_outputs():
    with pytest.raises(TranscriptRequestError, match="JSON object"):
        ProcessingRequest.from_payload("reuse")
    with pytest.raises(TranscriptRequestError, match="unknown keys"):
        ProcessingRequest.from_payload({"mode": "reuse", "extra": True})
    with pytest.raises(TranscriptRequestError, match="duplicate output"):
        ProcessingRequest.from_payload(
            {"outputs": ["transcript", "transcript"]}
        )
    with pytest.raises(TranscriptRequestError, match="unknown"):
        ProcessingRequest.from_payload({"outputs": ["transcript", "summary", "invalid"]})


def test_search_uses_stable_candidate_key_and_replaces_on_recompute(tmp_path: Path):
    config = make_test_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    service = TranscriptEnrichmentService(
        config,
        layout=layout,
        db=db,
        summarizer=CountingFakeSummarizer(),
        classifier=CountingFakeClassifier(),
    )
    service.enrich(artifact, request=request)
    docs = db.search_archivist_corpus_full_text(query="schema open adoption")
    keys = [row[0].candidate_key for row in docs]
    assert keys.count(f"transcript:{artifact.id}") == 1

    service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
            mode=ProcessingMode.RECOMPUTE,
        ),
    )
    docs = db.search_archivist_corpus_full_text(query="schema open adoption")
    keys = [row[0].candidate_key for row in docs]
    assert keys.count(f"transcript:{artifact.id}") == 1


def test_rebuild_preserves_exact_file_bytes_and_cache_state(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    first = service.enrich(artifact, request=request)
    key = first.cache_key
    state_before = load_cache_state(service.db, key)

    original_bytes: dict[str, bytes] = {}
    for output_type, rel_path in first.derivative_paths().items():
        original_bytes[output_type] = (
            service.layout.vault_root / rel_path
        ).read_bytes()
        (service.layout.vault_root / rel_path).unlink()

    rebuild = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
            mode=ProcessingMode.REBUILD_PROJECTION,
        ),
    )

    assert rebuild.version == "v1"
    assert rebuild.rerun_requested is False
    assert rebuild.cache_hit is True
    assert summarizer.call_count == 1
    assert classifier.call_count == 1

    state_after = load_cache_state(service.db, key)
    assert state_after == state_before

    first_created_at = {
        d.output_type: d.created_at for d in first.derivatives
    }
    for derivative in rebuild.derivatives:
        assert derivative.version == "v1"
        assert derivative.created_at == first_created_at[derivative.output_type]
        current_bytes = (
            service.layout.vault_root / derivative.path
        ).read_bytes()
        assert current_bytes == original_bytes[derivative.output_type]


def test_cache_key_ignores_unused_injected_processors_for_source_provided_values(
    tmp_path: Path,
):
    from tests.fixtures.cissa_like_recording import make_cissa_like_recording

    recording = make_cissa_like_recording()
    artifact = TranscriptArtifact(
        id=recording.artifact_id,
        source_type=recording.source_name,
        raw_transcript=recording.transcript_text,
        title=recording.title,
        summary="Source summary.",
        tags=["source-tag"],
    )
    outputs = (
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )
    config = make_test_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))

    service_without = TranscriptEnrichmentService(config, layout=layout, db=db)
    service_with = TranscriptEnrichmentService(
        config,
        layout=layout,
        db=db,
        summarizer=CountingFakeSummarizer(),
        classifier=CountingFakeClassifier(),
    )

    key_without = cache_key(
        artifact, service_without.normalizer, outputs, None, None
    )
    key_with = cache_key(
        artifact, service_with.normalizer, outputs, service_with.summarizer, service_with.classifier
    )
    assert key_without == key_with


def test_derivative_path_validation_rejects_unsafe_paths(tmp_path: Path):
    vault_root = tmp_path / "vault"
    vault_root.mkdir()

    with pytest.raises(TranscriptStorageError, match="relative"):
        resolve_derivative_path("/etc/passwd", vault_root)
    with pytest.raises(TranscriptStorageError, match="POSIX"):
        resolve_derivative_path("transcripts\\processed\\x.md", vault_root)
    with pytest.raises(TranscriptStorageError, match="invalid segment"):
        resolve_derivative_path("transcripts/../other.md", vault_root)
    with pytest.raises(TranscriptStorageError, match="invalid segment"):
        resolve_derivative_path("transcripts/./x.md", vault_root)


def test_atomic_write_reports_cleanup_failure(tmp_path: Path, monkeypatch):
    path = tmp_path / "readonly.md"

    def failing_write_text(*args, **kwargs):
        raise OSError("write failed")

    def failing_unlink(*args, **kwargs):
        raise OSError("cleanup failed")

    monkeypatch.setattr(Path, "write_text", failing_write_text)
    monkeypatch.setattr(Path, "unlink", failing_unlink)

    with pytest.raises(TranscriptStorageError, match="cleanup also failed"):
        _atomic_write_text(path, "content")


def test_derivative_from_mapping_validates_required_fields():
    import hashlib

    body = "body"
    body_sha256 = hashlib.sha256(body.encode("utf-8")).hexdigest()
    base = {
        "output_type": "transcript",
        "path": "transcripts/processed/x_v1.md",
        "content": body,
        "media_type": "text/markdown",
        "version": "v1",
        "cache_key": "a" * 64,
        "source_hash": "b" * 64,
        "content_sha256": body_sha256,
        "processor_identity": {
            "processor_name": "p",
            "processor_version": "1",
            "prompt_version": "1",
            "config_version": "1",
            "model": "m",
            "provider": "p",
        },
        "created_at": "2026-08-29T21:42:18Z",
    }
    # Valid base constructs.
    TranscriptDerivative.from_mapping(base)

    with pytest.raises(TranscriptDerivativeError, match="media_type"):
        TranscriptDerivative.from_mapping({**base, "media_type": "text/plain"})
    with pytest.raises(TranscriptDerivativeError, match="version"):
        TranscriptDerivative.from_mapping({**base, "version": "1"})
    with pytest.raises(TranscriptDerivativeError, match="cache_key"):
        TranscriptDerivative.from_mapping({**base, "cache_key": "short"})
    with pytest.raises(TranscriptDerivativeError, match="content_sha256 mismatch"):
        TranscriptDerivative.from_mapping({**base, "content_sha256": "a" * 64})


def test_cache_state_next_version_rejects_malformed_version():
    from core.transcript_enrichment.cache_state import next_version
    from core.transcript_enrichment.cache_state import TranscriptCacheError

    with pytest.raises(TranscriptCacheError, match="malformed"):
        next_version({"version": "not-a-version"})
    with pytest.raises(TranscriptCacheError, match="malformed"):
        next_version({"version": 123})


def _build_first_state(service, artifact, request):
    service.enrich(artifact, request=request)
    key = cache_key(
        artifact, service.normalizer, request.outputs, service.summarizer, service.classifier
    )
    return load_cache_state(service.db, key)


def test_malformed_transcript_content_with_valid_hash_fails_closed(tmp_path: Path):
    import hashlib

    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    state = _build_first_state(service, artifact, request)
    transcript_index = next(
        i for i, d in enumerate(state["derivatives"]) if d["output_type"] == "transcript"
    )
    bad_content = "not valid markdown"
    bad_hash = hashlib.sha256(bad_content.encode("utf-8")).hexdigest()
    state["derivatives"][transcript_index]["content"] = bad_content
    state["derivatives"][transcript_index]["content_sha256"] = bad_hash
    persist_cache_state(service.db, state["cache_key"], state)
    rel_path = state["derivatives"][transcript_index]["path"]
    (service.layout.vault_root / rel_path).write_text(bad_content, encoding="utf-8")

    with pytest.raises(TranscriptStorageError, match="frontmatter"):
        service.enrich(artifact, request=request)


def test_malformed_summary_content_with_valid_hash_fails_closed(tmp_path: Path):
    import hashlib

    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    state = _build_first_state(service, artifact, request)
    summary_index = next(
        i for i, d in enumerate(state["derivatives"]) if d["output_type"] == "summary"
    )
    bad_content = "plain text without frontmatter"
    bad_hash = hashlib.sha256(bad_content.encode("utf-8")).hexdigest()
    state["derivatives"][summary_index]["content"] = bad_content
    state["derivatives"][summary_index]["content_sha256"] = bad_hash
    persist_cache_state(service.db, state["cache_key"], state)
    rel_path = state["derivatives"][summary_index]["path"]
    (service.layout.vault_root / rel_path).write_text(bad_content, encoding="utf-8")

    result = service.enrich(artifact, request=request)
    with pytest.raises(TranscriptStorageError, match="frontmatter"):
        apply_derivatives_to_artifact(artifact, result)


def test_classification_tags_reject_non_string_or_blank_tags(tmp_path: Path):
    import hashlib

    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    state = _build_first_state(service, artifact, request)
    classification_index = next(
        i for i, d in enumerate(state["derivatives"]) if d["output_type"] == "classification"
    )
    parsed = json.loads(state["derivatives"][classification_index]["content"])
    parsed["tags"] = ["valid", 123, "   "]
    bad_content = json.dumps(parsed, sort_keys=True)
    bad_hash = hashlib.sha256(bad_content.encode("utf-8")).hexdigest()
    state["derivatives"][classification_index]["content"] = bad_content
    state["derivatives"][classification_index]["content_sha256"] = bad_hash
    persist_cache_state(service.db, state["cache_key"], state)
    rel_path = state["derivatives"][classification_index]["path"]
    (service.layout.vault_root / rel_path).write_text(bad_content, encoding="utf-8")

    with pytest.raises(TranscriptStorageError, match="non-blank strings"):
        service.enrich(artifact, request=request)


def test_cache_state_rejects_wrong_artifact_id(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    state = _build_first_state(service, artifact, request)
    state["artifact_id"] = "someone-else"
    persist_cache_state(service.db, state["cache_key"], state)

    with pytest.raises(TranscriptCacheError, match="artifact_id"):
        service.enrich(artifact, request=request)


def test_cache_state_rejects_missing_or_extra_outputs(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    full_request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    state = _build_first_state(service, artifact, full_request)
    # Drop summary derivative and expect failure when requesting summary.
    state["derivatives"] = [
        d for d in state["derivatives"] if d["output_type"] != "summary"
    ]
    persist_cache_state(service.db, state["cache_key"], state)

    with pytest.raises(TranscriptCacheError, match="output types"):
        service.enrich(artifact, request=full_request)

    # Extra derivative should also fail for a transcript-only request.
    import hashlib

    artifact2 = make_cissa_artifact(id="artifact-extra-output")
    transcript_only = request_with_outputs(TranscriptOutput.TRANSCRIPT)
    state2 = _build_first_state(service, artifact2, transcript_only)
    # Inject a spurious summary derivative with a valid content hash.
    bad_summary = "not a real summary"
    state2["derivatives"].append(
        {
            "output_type": "summary",
            "path": "transcripts/summaries/spurious_v1.md",
            "content": bad_summary,
            "media_type": "text/markdown",
            "version": "v1",
            "cache_key": state2["cache_key"],
            "source_hash": state2["source_hash"],
            "content_sha256": hashlib.sha256(bad_summary.encode("utf-8")).hexdigest(),
            "processor_identity": source_provided_summary_identity().to_dict(),
            "created_at": "2026-08-29T21:42:18Z",
        }
    )
    persist_cache_state(service.db, state2["cache_key"], state2)
    with pytest.raises(TranscriptCacheError, match="output types"):
        service.enrich(
            artifact2,
            request=transcript_only,
        )


def test_cache_state_rejects_malformed_processor_identities(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    state = _build_first_state(service, artifact, request)
    state["processor_identities"]["normalizer"]["provider"] = ""
    persist_cache_state(service.db, state["cache_key"], state)

    with pytest.raises(TranscriptCacheError, match="normalizer identity"):
        service.enrich(artifact, request=request)

    artifact2 = make_cissa_artifact(id="artifact-bad-processor-identities")
    state2 = _build_first_state(service, artifact2, request)
    state2["processor_identities"]["unexpected"] = {}
    persist_cache_state(service.db, state2["cache_key"], state2)

    with pytest.raises(TranscriptCacheError, match="processor_identities"):
        service.enrich(artifact2, request=request)


def test_version_v0_is_rejected():
    from core.transcript_enrichment.cache_state import (
        TranscriptCacheError,
        next_version,
    )

    with pytest.raises(TranscriptCacheError, match="malformed"):
        next_version({"version": "v0"})

    with pytest.raises(TranscriptDerivativeError, match="v<N>"):
        TranscriptDerivative(
            output_type="transcript",
            path="transcripts/processed/x_v0.md",
            content="content",
            media_type="text/markdown",
            version="v0",
            cache_key="a" * 64,
            source_hash="b" * 64,
            content_sha256="0" * 64,
            processor_identity=LocalTranscriptNormalizer().identity(),
        )


def test_cross_cache_paths_are_unique_and_stable(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    transcript_only = request_with_outputs(TranscriptOutput.TRANSCRIPT)
    full = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    first = service.enrich(artifact, request=transcript_only)
    second = service.enrich(artifact, request=full)

    first_paths = set(first.derivative_paths().values())
    second_paths = set(second.derivative_paths().values())
    assert not first_paths & second_paths, "paths collide across cache identities"

    # Both sets of files exist with exact committed bytes.
    for derivative in (*first.derivatives, *second.derivatives):
        file_path = service.layout.vault_root / derivative.path
        assert file_path.read_text(encoding="utf-8") == derivative.content

    # Alternating operations on one profile must not affect the other.
    transcript_path = service.layout.vault_root / first.derivative_paths()["transcript"]
    original_text = transcript_path.read_text(encoding="utf-8")
    transcript_path.unlink()

    rebuild = service.enrich(
        artifact,
        request=request_with_outputs(TranscriptOutput.TRANSCRIPT, mode=ProcessingMode.REBUILD_PROJECTION),
    )
    assert rebuild.derivative_paths()["transcript"] == first.derivative_paths()["transcript"]
    assert transcript_path.read_text(encoding="utf-8") == original_text

    full_path = service.layout.vault_root / second.derivative_paths()["transcript"]
    assert full_path.exists()


def test_reuse_fails_closed_on_disk_byte_tamper(tmp_path: Path):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact()
    service.enrich(artifact)

    state = load_cache_state(
        service.db,
        cache_key(artifact, service.normalizer, (TranscriptOutput.TRANSCRIPT,), None, None),
    )
    rel_path = state["derivatives"][0]["path"]
    (service.layout.vault_root / rel_path).write_text("tampered bytes", encoding="utf-8")

    with pytest.raises(TranscriptStorageError, match="hash mismatch"):
        service.enrich(artifact)


def test_reuse_fails_closed_on_invalid_utf8_file(tmp_path: Path):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact()
    service.enrich(artifact)

    state = load_cache_state(
        service.db,
        cache_key(artifact, service.normalizer, (TranscriptOutput.TRANSCRIPT,), None, None),
    )
    rel_path = state["derivatives"][0]["path"]
    (service.layout.vault_root / rel_path).write_bytes(b"\xff\xfe")

    # Invalid UTF-8 bytes cannot match the cached SHA-256 of valid content.
    with pytest.raises(TranscriptStorageError, match="hash mismatch"):
        service.enrich(artifact)


def test_reuse_fails_closed_on_symlink_replacement(tmp_path: Path):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact()
    service.enrich(artifact)

    state = load_cache_state(
        service.db,
        cache_key(artifact, service.normalizer, (TranscriptOutput.TRANSCRIPT,), None, None),
    )
    rel_path = state["derivatives"][0]["path"]
    target = service.layout.vault_root / rel_path
    target.unlink()
    target.symlink_to(service.layout.vault_root / "evil.md")

    with pytest.raises(TranscriptStorageError, match="symlink"):
        service.enrich(artifact)


def test_cache_state_rejects_unknown_top_level_and_derivative_keys(tmp_path: Path):
    service, _ = _service(tmp_path)
    artifact = make_cissa_artifact()
    service.enrich(artifact)

    key = cache_key(
        artifact, service.normalizer, (TranscriptOutput.TRANSCRIPT,), None, None
    )
    state = load_cache_state(service.db, key)
    state["unknown_top_level"] = True
    persist_cache_state(service.db, key, state)

    with pytest.raises(TranscriptCacheError, match="unknown top-level keys"):
        service.enrich(artifact)

    state = load_cache_state(service.db, key)
    state.pop("unknown_top_level", None)
    state["derivatives"][0]["extra"] = "value"
    persist_cache_state(service.db, key, state)

    with pytest.raises(TranscriptCacheError, match="unknown keys"):
        service.enrich(artifact)


def test_cache_state_cross_checks_processor_identities(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service, _ = _service(tmp_path, summarizer=summarizer, classifier=classifier)
    artifact = make_cissa_artifact()
    request = request_with_outputs(
        TranscriptOutput.TRANSCRIPT,
        TranscriptOutput.SUMMARY,
        TranscriptOutput.CLASSIFICATION,
    )

    state = _build_first_state(service, artifact, request)
    state["processor_identities"]["summarizer"]["provider"] = "tampered-provider"
    persist_cache_state(service.db, state["cache_key"], state)

    with pytest.raises(TranscriptCacheError, match="summarizer identity"):
        service.enrich(artifact, request=request)

    artifact2 = make_cissa_artifact(id="artifact-processor-cross-check")
    state2 = _build_first_state(service, artifact2, request)
    state2["processor_identities"]["summarizer"] = None
    persist_cache_state(service.db, state2["cache_key"], state2)

    with pytest.raises(TranscriptCacheError, match="summary derivative present"):
        service.enrich(artifact2, request=request)


def test_extract_summary_preserves_multi_paragraph_summary(tmp_path: Path):
    summarizer = CountingFakeSummarizer()
    service, _ = _service(tmp_path, summarizer=summarizer)
    artifact = make_cissa_artifact()
    request = request_with_outputs(TranscriptOutput.TRANSCRIPT, TranscriptOutput.SUMMARY)

    result = service.enrich(artifact, request=request)

    summary_derivative = next(d for d in result.derivatives if d.output_type == "summary")
    paragraphs = ["First paragraph.", "Second paragraph.", "Third paragraph."]
    multi = "\n\n".join(paragraphs)
    patched = summary_derivative.content.replace(
        extract_summary_text(summary_derivative.content),
        multi,
    )
    patched_hash = hashlib.sha256(patched.encode("utf-8")).hexdigest()

    # Update the cached derivative to the multi-paragraph content so a rebuild
    # writes exactly those bytes.
    state = load_cache_state(service.db, result.cache_key)
    for d in state["derivatives"]:
        if d["output_type"] == "summary":
            d["content"] = patched
            d["content_sha256"] = patched_hash
    persist_cache_state(service.db, result.cache_key, state)
    summary_path = service.layout.vault_root / summary_derivative.path
    summary_path.write_text(patched, encoding="utf-8")

    # Rebuild from the patched cache state and apply to a fresh artifact;
    # artifact.summary must keep all paragraphs.
    rebuild = service.enrich(
        artifact,
        request=request_with_outputs(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            mode=ProcessingMode.REBUILD_PROJECTION,
        ),
    )
    fresh_artifact = make_cissa_artifact(id=artifact.id)
    apply_derivatives_to_artifact(fresh_artifact, rebuild)
    assert fresh_artifact.summary == multi
