"""thothmap transcript mapping tests (checklist 4: transcripts -> experience.utterance)."""

from __future__ import annotations

import hashlib

import pytest

from ccf.thothmap import MapContext
from ccf.thothmap.artifacts import media_submissions
from ccf.thothmap.context import ThothMapError, data_subject
from ccf.thothmap.sessions import run_submission, session_submission
from ccf.thothmap.sources import source_submission
from ccf.thothmap.transcripts import utterance_submissions

from ccf_helpers import admit_mapped, compartment, make_rig, outcome_for

WAV_BYTES = b"RIFF transcript-source-audio"


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def ctx(rig):
    return MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)


@pytest.fixture()
def capture(rig, ctx):
    """Admitted source + session + media artifact/blob + transcription run."""
    source = source_submission(
        rig.producer,
        ctx,
        {"source_name": "omi", "source_type": "wearable_audio", "collector": "thoth.capture"},
    )
    admit_mapped(rig, source)
    source_id = source.records[0]["id"]

    session = session_submission(
        rig.producer,
        ctx,
        {
            "session_id": "boot-8891/session-1",
            "session_type": "ambient",
            "status": "closed",
            "started_at": "2026-08-11T21:41:48Z",
            "ended_at": "2026-08-11T21:42:18Z",
            "metadata": {"capture_mode": "manual-test"},
        },
        source_ccf_id=source_id,
        participants=[rig.person_id],
    )
    media = media_submissions(
        rig.producer,
        ctx,
        {
            "raw_ref_id": "raw-ref:segment-1842",
            "path": "knowledge_vault/raw/omi/segment-1842.wav",
            "sha256": hashlib.sha256(WAV_BYTES).hexdigest(),
            "size_bytes": len(WAV_BYTES),
            "mime_type": "audio/wav",
            "created_at": "2026-08-11T21:42:18.331Z",
        },
        data=WAV_BYTES,
        source_ccf_id=source_id,
        session_ccf_id=session.records[0]["id"],
    )
    run = run_submission(
        rig.producer,
        ctx,
        {
            "run_id": "connector_run_transcription1",
            "connector_name": "deepgram",
            "status": "completed",
            "started_at": "2026-08-11T21:42:18.400Z",
            "finished_at": "2026-08-11T21:42:19.000Z",
        },
        source_ccf_id=source_id,
        run_kind="transcription",
        task="Transcribe captured audio",
    )
    admit_mapped(rig, session.extend(media).extend(run))
    return {
        "source_id": source_id,
        "session_id": session.records[0]["id"],
        "artifact_id": media.records[0]["id"],
        "run_id": run.records[0]["id"],
    }


TRANSCRIPT = {
    "transcript_id": "omi_transcript_boot-8891",
    "raw_transcript": "Speaker 0: I want the schema open because adoption is still a win.",
    "language": "en",
    "speaker": "Speaker 0",
    "session_id": "boot-8891/session-1",
    "started_at": "2026-08-11T21:41:48Z",
    "ended_at": "2026-08-11T21:42:18Z",
}


def test_whole_transcript_single_utterance(rig, ctx, capture):
    subjects = [data_subject(rig.person_id, "speaker", identity_state="verified")]
    mapped = utterance_submissions(
        rig.producer,
        ctx,
        TRANSCRIPT,
        source_ccf_id=capture["source_id"],
        media_artifact_ccf_id=capture["artifact_id"],
        run_ccf_id=capture["run_id"],
        session_ccf_id=capture["session_id"],
        engine="deepgram",
        engine_version="nova-2",
        speaker_ccf_id=rig.person_id,
        source_subjects=subjects,
    )
    assert len(mapped.records) == 1
    utterance = mapped.records[0]
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.2-rc1:payload.experience.utterance",
        utterance["payload"],
        what="experience.utterance",
    )
    link_types = [link["type"] for link in mapped.links]
    assert link_types.count("ccf.derived_from") == 1
    assert link_types.count("ccf.generated_by") == 1
    assert link_types.count("ccf.captured_in") == 1
    assert link_types.count("ccf.has_transcript") == 1

    result = admit_mapped(rig, mapped)
    assert result["status"] == "accepted"
    semantic = compartment(rig, utterance["id"], "semantic")
    assert semantic["origin"]["native_id"] == "omi_transcript_boot-8891"
    # Conservative subject propagation from the source media (spec 3.9).
    assert semantic["privacy"]["data_subjects"] == subjects
    assert semantic["privacy"]["data_classes"] == ["speech_content"]
    assert semantic["authority"]["basis"] == "quoted_statement"
    assert semantic["payload"]["transcription"]["engine"] == "deepgram"


def test_segmented_transcript_utterances(rig, ctx, capture):
    snapshot = dict(
        TRANSCRIPT,
        segments=[
            {"text": "I want the schema open", "speaker": "Ada", "start_ms": 0, "end_ms": 1500,
             "confidence": 0.96},
            {"text": "because adoption is still a win", "speaker": "Ada", "start_ms": 1500,
             "end_ms": 3000, "confidence": 0.91},
        ],
    )
    mapped = utterance_submissions(
        rig.producer,
        ctx,
        snapshot,
        source_ccf_id=capture["source_id"],
        media_artifact_ccf_id=capture["artifact_id"],
        run_ccf_id=capture["run_id"],
        engine="deepgram",
        engine_version="nova-2",
    )
    assert [r["payload"]["sequence"] for r in mapped.records] == ["1", "2"]
    native_ids = [r["origin"]["native_id"] for r in mapped.records]
    assert native_ids == [
        "omi_transcript_boot-8891/utterance-1",
        "omi_transcript_boot-8891/utterance-2",
    ]
    derived = [l for l in mapped.links if l["type"] == "ccf.derived_from"]
    assert derived[0]["selector"] == {"kind": "media_time", "start_ms": 0, "end_ms": 1500}
    assert all(l["to_id"] == capture["artifact_id"] for l in derived)

    result = admit_mapped(rig, mapped)
    assert result["status"] == "accepted"
    semantic = compartment(rig, mapped.records[0]["id"], "semantic")
    assert semantic["payload"]["transcription"]["mean_confidence"] == 0.96
    # Unknown speaker: machine-inference authority asserted by the runtime.
    assert semantic["authority"]["basis"] == "machine_inference"
    assert semantic["authority"]["asserted_by"] == rig.runtime_id


def test_transcript_requires_provenance(rig, ctx, capture):
    with pytest.raises(ThothMapError):
        utterance_submissions(
            rig.producer,
            ctx,
            TRANSCRIPT,
            source_ccf_id=capture["source_id"],
            media_artifact_ccf_id="not-a-urn",
            run_ccf_id=capture["run_id"],
            engine="deepgram",
            engine_version="nova-2",
        )


def test_transcript_requires_text(rig, ctx, capture):
    empty = {"transcript_id": "t-empty", "language": "en"}
    with pytest.raises(ThothMapError, match="text"):
        utterance_submissions(
            rig.producer,
            ctx,
            empty,
            source_ccf_id=capture["source_id"],
            media_artifact_ccf_id=capture["artifact_id"],
            run_ccf_id=capture["run_id"],
            engine="deepgram",
            engine_version="nova-2",
        )
