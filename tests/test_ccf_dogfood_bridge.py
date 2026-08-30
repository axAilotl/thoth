"""Deterministic local Cissa→Thoth→Keeper P0 bridge proof (thoth-u7v.1).

This test exercises the reconciled Thoth Capsule identity, CCF admission,
transcript enrichment, and the read-only Keeper surface using only local,
synthetic fixtures. It does not write to the Cissa repository, call paid or
external AI providers, or rely on live Thoth/Cissa services.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

import pytest

from ccf.capsule import write_capsule
from ccf.hashing import encode_b64url
from ccf.ids import derive_id
from ccf.interop import load_capsule_integrity
from ccf.schemas import SchemaSet
from core.artifacts import ArtifactSourceIdentity, TranscriptArtifact
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.transcript_enrichment import (
    ProcessingMode,
    ProcessingRequest,
    TranscriptEnrichmentService,
    TranscriptOutput,
)
from keeper_profile import KeeperProfile, KeeperProfileConfig
from tests.ccf_helpers import make_rig
from tests.fixtures.cissa_like_recording import make_cissa_like_recording
from tests.test_transcript_enrichment_helpers import (
    CountingFakeClassifier,
    CountingFakeSummarizer,
    make_test_config,
)
from thoth_keeper import KeeperProfileMCPServer


def _deterministic_salt_fn():
    """Return a deterministic salt generator for repeatable object commitments."""
    counter = 0

    def _salt() -> str:
        nonlocal counter
        counter += 1
        payload = f"thoth-u7v-salt-{counter}".encode()
        return encode_b64url(hashlib.sha256(payload).digest())

    return _salt


def _derive_id(kind: str, *material: str) -> str:
    """Derive a stable, spec-legal CCF URN from the fixture session."""
    return derive_id(uuid.NAMESPACE_DNS, kind, list(material))


@pytest.fixture
def rig(ccf_settings, tmp_path, ccf_package_root):
    """Archive + producer rig with deterministic admission salts."""
    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    rig.archive._salt_fn = _deterministic_salt_fn()
    return rig


def _build_submissions(rig, recording):
    """Map the recording to a source, audio blob, transcript record, and links."""
    source_id = _derive_id("record", recording.session_id, "source")
    audio_blob_id = _derive_id("blob", recording.session_id, "audio")
    transcript_id = _derive_id("record", recording.session_id, "transcript")
    derived_link_id = _derive_id("link", recording.session_id, "derived")
    transcript_part_id = _derive_id("link", recording.session_id, "transcript-part")
    audio_part_id = _derive_id("link", recording.session_id, "audio-part")

    origin = {
        "source_id": source_id,
        "native_id": recording.session_id,
        "revision": "1",
    }

    source = rig.producer.new_record(
        object_id=source_id,
        type="core.source",
        claims=rig.claims(),
        payload={
            "kind": recording.source_type,
            "name": recording.source_name,
            "connector": "fixture",
            "native_identity": recording.session_id,
            "trust_class": "trusted",
            "extensions": {},
        },
    )

    audio_blob, audio_bytes = rig.producer.new_blob(
        blob_id=audio_blob_id,
        data=recording.audio_blob,
        media_type="audio/wav",
        claims=rig.claims(),
        origin=origin,
    )

    transcript = rig.producer.new_record(
        object_id=transcript_id,
        type="experience.utterance",
        claims=rig.claims(),
        origin=origin,
        payload={
            "text": recording.transcript_text,
            "language": recording.language,
            "speaker_id": None,
            "sequence": "1",
            "transcription": {
                "engine": "fixture",
                "engine_version": "1.0.0",
            },
            "extensions": {
                "thoth_session_id": recording.session_id,
                "thoth_device_id": recording.device_id,
            },
        },
    )

    derived_link = rig.producer.new_link(
        link_id=derived_link_id,
        type="ccf.derived_from",
        from_id=transcript_id,
        to_id=audio_blob_id,
        claims=rig.claims(),
    )
    transcript_part = rig.producer.new_link(
        link_id=transcript_part_id,
        type="ccf.part_of",
        from_id=transcript_id,
        to_id=source_id,
        claims=rig.claims(),
    )
    audio_part = rig.producer.new_link(
        link_id=audio_part_id,
        type="ccf.part_of",
        from_id=audio_blob_id,
        to_id=source_id,
        claims=rig.claims(),
    )

    return {
        "source": source,
        "source_id": source_id,
        "audio_blob": audio_blob,
        "audio_bytes": audio_bytes,
        "transcript": transcript,
        "derived_link": derived_link,
        "transcript_part": transcript_part,
        "audio_part": audio_part,
        "audio_blob_id": audio_blob_id,
        "transcript_id": transcript_id,
    }


def _build_capsule(rig, recording, tmp_path: Path, submissions: dict, schemas):
    """Write a current-root Capsule carrying the fixture and its audio bytes."""
    capsule_dir = tmp_path / "capsule"
    pack_id = _derive_id("pack", recording.session_id)

    manifest = {
        "format": "ccf.capsule/0.2.0",
        "pack_id": pack_id,
        "created_at": rig.clock(),
        "level": "ccf-exchange-v1",
        "capabilities": [],
        "root_record_id": submissions["source_id"],
        "membership_link_types": ["ccf.part_of"],
        "custody": {
            "completeness": "complete",
            "losslessness": "lossless",
            "omissions": [],
        },
        "catalog_dependencies": [
            {
                "kind": "semantic_catalog",
                "identifier": "ccf.semantic-catalog/0.1.2",
                "digest": rig.archive.semantic_catalog_root,
                "required": True,
            }
        ],
        "streams": [
            {
                "path": "submissions/objects.ndjson",
                "media_type": "application/x-ndjson",
                "content_role": "submissions",
                "handling": "activate",
                "activation_requirements": {
                    "minimum_level": "ccf-exchange-v1",
                    "capabilities": [],
                },
                "digest": (
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                ),
                "byte_length": "0",
                "required": True,
            },
            {
                "path": "opaque/audio.wav",
                "media_type": "audio/wav",
                "content_role": "opaque",
                "handling": "preserve_opaque",
                "activation_requirements": {
                    "minimum_level": "ccf-exchange-v1",
                    "capabilities": [],
                },
                "digest": (
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                ),
                "byte_length": "0",
                "required": True,
            },
        ],
        "dependencies": [],
        "proofs": [],
        "extensions": {},
    }

    submission_values = [
        submissions["source"],
        submissions["transcript"],
        submissions["audio_blob"],
        submissions["derived_link"],
        submissions["transcript_part"],
        submissions["audio_part"],
    ]

    return write_capsule(
        capsule_dir,
        manifest=manifest,
        submission_streams={"submissions/objects.ndjson": submission_values},
        opaque_streams={"opaque/audio.wav": submissions["audio_bytes"]},
        schemas=schemas,
    )


def _transcript_artifact(recording) -> TranscriptArtifact:
    """Build the Thoth artifact that drives transcript enrichment."""
    return TranscriptArtifact(
        id=recording.artifact_id,
        source_type=recording.source_name,
        raw_transcript=recording.transcript_text,
        title=recording.title,
        language=recording.language,
        session_id=recording.session_id,
        device_id=recording.device_id,
        source_identity=ArtifactSourceIdentity(
            source_name=recording.source_name,
            source_type=recording.source_type,
            native_id=recording.session_id,
        ),
    )


def _enrichment_service(tmp_path: Path):
    """Return a local transcript enrichment service with counting fake processors."""
    config = make_test_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    summarizer = CountingFakeSummarizer()
    classifier = CountingFakeClassifier()
    service = TranscriptEnrichmentService(
        config,
        layout=layout,
        db=db,
        summarizer=summarizer,
        classifier=classifier,
    )
    return service, summarizer, classifier


def test_u7v_dogfood_bridge_proof(
    rig, tmp_path, ccf_package_root, ccf_draft_root, monkeypatch
):
    """End-to-end P0 bridge proof with receive, verify, admit, and process evidence."""
    recording = make_cissa_like_recording()
    schemas = SchemaSet.load_layered(ccf_package_root, ccf_draft_root)
    submissions = _build_submissions(rig, recording)
    capsule = _build_capsule(rig, recording, tmp_path, submissions, schemas)

    # Receive: integrity-verified Capsule load.
    received = load_capsule_integrity(capsule.root)
    assert received.manifest["pack_id"] == capsule.manifest["pack_id"]
    streams = {stream.path: stream for stream in received.streams}
    assert streams["submissions/objects.ndjson"].spec["digest"].startswith("sha256:")
    assert streams["opaque/audio.wav"].spec["digest"].startswith("sha256:")
    assert streams["opaque/audio.wav"].data == submissions["audio_bytes"]

    # Verify: current-root preview produces a pending, identity-bound uplift receipt.
    inspection = rig.archive.inspect_capsule(capsule.root)
    assert inspection["status"] == "preview"
    assert inspection["disposition"] == "current"
    assert inspection["identity"]["root"] == "current"
    assert inspection["identity"]["catalog_root"] == rig.archive.semantic_catalog_root

    preview = rig.archive.preview_capsule(capsule.root)
    receipt = preview["uplift"]
    assert receipt["status"] == "pending"
    assert receipt["source_pack_id"] == capsule.manifest["pack_id"]
    assert receipt["destination_archive_id"] == rig.archive.archive_id
    assert (
        receipt["destination_level"]
        == rig.archive.interop_context().declaration["level"]
    )

    receipt_objects = {obj["source_id"]: obj for obj in receipt["objects"]}
    assert receipt_objects[submissions["source_id"]]["object_kind"] == "record"
    assert receipt_objects[submissions["transcript_id"]]["object_kind"] == "record"
    assert receipt_objects[submissions["audio_blob_id"]]["object_kind"] == "blob"
    assert all(
        obj["source_id"] == obj["canonical_id"]
        and obj["producer_authentication"] == "absent"
        for obj in receipt["objects"]
    )

    before_head = rig.archive.head()

    # Admit: sign and commit the same submissions as a producer batch.
    batch = rig.producer.create_batch(
        records=[submissions["source"], submissions["transcript"]],
        links=[
            submissions["derived_link"],
            submissions["transcript_part"],
            submissions["audio_part"],
        ],
        blobs=[submissions["audio_blob"]],
        blob_data={submissions["audio_blob_id"]: submissions["audio_bytes"]},
    )
    assert batch["producer_id"] == rig.producer.producer_id
    assert batch["credential_id"] == rig.producer.credential.credential_id
    assert batch["semantic_catalog_root"] == rig.archive.semantic_catalog_root

    result = rig.archive.admit_batch(
        batch,
        blob_bytes={submissions["audio_blob_id"]: submissions["audio_bytes"]},
    )
    assert result["status"] == "accepted"
    assert result["archive_id"] == rig.archive.archive_id
    assert len(result["admissions"]) == 6

    admitted = {a["object_id"]: a for a in result["admissions"]}
    assert admitted[submissions["source_id"]]["status"] == "admitted"
    assert admitted[submissions["transcript_id"]]["status"] == "admitted"
    assert admitted[submissions["audio_blob_id"]]["status"] == "admitted"
    assert all(a["object_hash"] for a in result["admissions"])

    after_first_admit = rig.archive.head()
    assert after_first_admit["sequence"] > before_head["sequence"]

    # Idempotent duplicate delivery.
    replay = rig.archive.admit_batch(
        batch,
        blob_bytes={submissions["audio_blob_id"]: submissions["audio_bytes"]},
    )
    assert replay["status"] == "accepted"
    assert rig.archive.head() == after_first_admit
    assert replay == result

    # Process: transcript enrichment creates searchable projections and derivatives.
    service, summarizer, classifier = _enrichment_service(tmp_path)
    request = ProcessingRequest(
        mode=ProcessingMode.REUSE,
        outputs=(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
        ),
    )
    artifact = _transcript_artifact(recording)
    enrich_result = service.enrich(artifact, request=request)
    assert not enrich_result.cache_hit
    assert enrich_result.version == "v1"
    assert enrich_result.indexed
    assert len(enrich_result.cache_key) == 64
    assert all(
        derivative.cache_key == enrich_result.cache_key
        for derivative in enrich_result.derivatives
    )
    assert summarizer.call_count == 1
    assert classifier.call_count == 1

    derivative_paths = enrich_result.derivative_paths()
    assert {"transcript", "summary", "classification"} == set(derivative_paths.keys())
    for rel_path in derivative_paths.values():
        assert (service.layout.vault_root / rel_path).is_file()

    # Searchable transcript projection via Keeper.
    keeper = KeeperProfile(
        KeeperProfileConfig(
            db_path=str(service.db.db_path),
            allowed_roots=["vault/transcripts"],
            stale_index_seconds=86400 * 365,
        )
    )
    keeper_server = KeeperProfileMCPServer(keeper)
    keeper_response = keeper_server.call_tool(
        "keeper_query", {"query": "containerizing", "limit": 5}
    )
    keeper_payload = json.loads(keeper_response["content"][0]["text"])
    assert keeper_payload["status"] == "ok"
    assert keeper_payload["total"] == 1

    passage = keeper_payload["passages"][0]
    assert passage["artifact_id"] == recording.artifact_id
    assert passage["source_key"] == recording.session_id
    assert "containerizing" in passage["snippet"].lower()
    assert passage["provenance"]["artifact_id"] == recording.artifact_id
    assert passage["provenance"]["source_key"] == recording.session_id

    # Explicit Keeper-unavailable failure.
    missing_db = KeeperProfile(
        KeeperProfileConfig(
            db_path=str(tmp_path / "missing.db"),
            allowed_roots=["vault/transcripts"],
        )
    )
    unavailable_response = KeeperProfileMCPServer(missing_db).call_tool(
        "keeper_query", {"query": "containerizing", "limit": 5}
    )
    unavailable = json.loads(unavailable_response["content"][0]["text"])
    assert unavailable["status"] == "unavailable_storage"

    # Projection deletion + rebuild without another summarizer/classifier call.
    for rel_path in derivative_paths.values():
        (service.layout.vault_root / rel_path).unlink()

    rebuild_request = ProcessingRequest(
        mode=ProcessingMode.REBUILD_PROJECTION,
        outputs=(
            TranscriptOutput.TRANSCRIPT,
            TranscriptOutput.SUMMARY,
            TranscriptOutput.CLASSIFICATION,
        ),
    )
    rebuild = service.enrich(artifact, request=rebuild_request)
    assert rebuild.cache_hit
    assert rebuild.version == "v1"
    assert rebuild.cache_key == enrich_result.cache_key
    assert rebuild.derivative_paths() == derivative_paths
    assert summarizer.call_count == 1
    assert classifier.call_count == 1
    for rel_path in rebuild.derivative_paths().values():
        assert (service.layout.vault_root / rel_path).is_file()

    # Simulated interruption during admission fails without partial canonical mutation.
    import ccf.admission as admission_module

    def _failing_commit(*args, **kwargs):
        raise admission_module.AdmissionError("simulated interruption")

    interrupted_record = rig.producer.new_record(
        object_id=_derive_id("record", recording.session_id, "interrupted"),
        type="experience.utterance",
        claims=rig.claims(),
        payload={
            "text": "This record must not survive the interrupted transaction.",
            "language": "en",
            "speaker_id": None,
            "sequence": "2",
            "transcription": {"engine": "fixture", "engine_version": "1.0.0"},
            "extensions": {},
        },
    )
    interrupted_batch = rig.producer.create_batch(records=[interrupted_record])
    with monkeypatch.context() as patch:
        patch.setattr(admission_module, "commit_objects", _failing_commit)
        with pytest.raises(
            admission_module.AdmissionError, match="simulated interruption"
        ):
            rig.archive.admit_batch(interrupted_batch)
    assert rig.archive.head() == after_first_admit

    # A distinct corrupted delivery fails safely after the interrupted
    # transaction rolled back, without moving the canonical head.
    corrupted = dict(interrupted_batch)
    corrupted["batch_hash"] = corrupted["batch_hash"][:-1] + (
        "0" if corrupted["batch_hash"][-1] != "0" else "1"
    )
    corrupt_result = rig.archive.admit_batch(corrupted)
    assert corrupt_result["status"] == "quarantined"
    assert rig.archive.head() == after_first_admit
