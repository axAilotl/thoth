"""thothmap media mapping tests (checklist 4: files -> Blob + experience.artifact)."""

from __future__ import annotations

import hashlib

import pytest

from ccf.thothmap import MapContext
from ccf.thothmap.artifacts import media_submissions
from ccf.thothmap.context import ThothMapError, data_subject
from ccf.thothmap.sessions import session_submission
from ccf.thothmap.sources import source_submission

from ccf_helpers import admit_mapped, compartment, make_rig, outcome_for

WAV_BYTES = b"RIFF\x24\x00\x00\x00WAVEfmt fake-audio-bytes-for-thothmap-tests"


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def ctx(rig):
    return MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)


@pytest.fixture()
def source_id(rig, ctx):
    mapped = source_submission(
        rig.producer,
        ctx,
        {"source_name": "omi", "source_type": "wearable_audio", "collector": "thoth.capture"},
    )
    admit_mapped(rig, mapped)
    return mapped.records[0]["id"]


@pytest.fixture()
def session_id(rig, ctx, source_id):
    mapped = session_submission(
        rig.producer,
        ctx,
        {
            "session_id": "session-1842",
            "session_type": "ambient",
            "status": "closed",
            "started_at": "2026-08-11T21:41:48Z",
            "ended_at": "2026-08-11T21:42:18Z",
            "metadata": {},
        },
        source_ccf_id=source_id,
        participants=[rig.person_id],
    )
    admit_mapped(rig, mapped)
    return mapped.records[0]["id"]


def raw_ref(**overrides):
    base = {
        "raw_ref_id": "raw-ref:6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "path": "knowledge_vault/raw/omi/segment-1842.wav",
        "sha256": hashlib.sha256(WAV_BYTES).hexdigest(),
        "size_bytes": len(WAV_BYTES),
        "mime_type": "audio/wav",
        "created_at": "2026-08-11T21:42:18.331Z",
    }
    base.update(overrides)
    return base


def test_media_maps_to_blob_artifact_and_links(rig, ctx, source_id, session_id):
    subjects = [data_subject(rig.person_id, "speaker", identity_state="verified")]
    mapped = media_submissions(
        rig.producer,
        ctx,
        raw_ref(),
        data=WAV_BYTES,
        source_ccf_id=source_id,
        session_ccf_id=session_id,
        source_subjects=subjects,
    )
    artifact, blob = mapped.records[0], mapped.blobs[0]
    link_types = {link["type"] for link in mapped.links}
    assert link_types == {"ccf.has_blob", "ccf.captured_in"}
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.2:payload.experience.artifact",
        artifact["payload"],
        what="experience.artifact",
    )

    result = admit_mapped(rig, mapped)
    assert result["status"] == "accepted"
    for object_id in (artifact["id"], blob["id"]):
        assert outcome_for(result, object_id)["status"] == "admitted"
    # Blob payload availability is reported once bytes verify against the
    # declared salted content commitment.
    assert outcome_for(result, blob["id"])["payload_available"] is True

    semantic = compartment(rig, artifact["id"], "semantic")
    assert semantic["origin"]["native_id"] == raw_ref()["raw_ref_id"]
    # Multi-subject media: conservative subject inheritance (spec 3.9).
    assert semantic["privacy"]["data_subjects"] == subjects
    assert semantic["privacy"]["data_classes"] == ["voice_recording"]

    has_blob = next(l for l in mapped.links if l["type"] == "ccf.has_blob")
    structural = compartment(rig, has_blob["id"], "structural")
    assert structural["from_id"] == artifact["id"]
    assert structural["to_id"] == blob["id"]
    captured_in = next(l for l in mapped.links if l["type"] == "ccf.captured_in")
    structural = compartment(rig, captured_in["id"], "structural")
    assert structural["to_id"] == session_id


def test_media_fails_closed_on_content_mismatch(rig, ctx, source_id):
    with pytest.raises(ThothMapError, match="sha256 mismatch"):
        media_submissions(
            rig.producer, ctx, raw_ref(), data=b"tampered", source_ccf_id=source_id
        )
    with pytest.raises(ThothMapError, match="size mismatch"):
        media_submissions(
            rig.producer,
            ctx,
            raw_ref(sha256=None, size_bytes=len(WAV_BYTES) + 1),
            data=WAV_BYTES,
            source_ccf_id=source_id,
        )


def test_media_reimport_idempotent_and_blob_revision_conflict(rig, ctx, source_id):
    mapped = media_submissions(rig.producer, ctx, raw_ref(), data=WAV_BYTES, source_ccf_id=source_id)
    admit_mapped(rig, mapped)
    replay = admit_mapped(rig, mapped)
    for object_id in (mapped.records[0]["id"], mapped.blobs[0]["id"]):
        assert outcome_for(replay, object_id)["status"] == "existing"

    # Changed bytes at the same origin revision conflict instead of replacing.
    # (The blob is submitted on its own: a whole changed-media batch would be
    # atomically rejected at reference completeness when every object in it
    # conflicts, which is correct but hides the per-object outcome.)
    other = b"RIFF different-bytes"
    remapped = media_submissions(
        rig.producer,
        ctx,
        raw_ref(sha256=None, size_bytes=None),
        data=other,
        source_ccf_id=source_id,
    )
    batch = rig.producer.create_batch(blobs=remapped.blobs)
    conflict = rig.archive.admit_batch(batch, blob_bytes=remapped.blob_data)
    assert outcome_for(conflict, remapped.blobs[0]["id"])["status"] == "origin_revision_conflict"
