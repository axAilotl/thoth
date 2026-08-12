"""Transcripts -> ``experience.utterance`` Records (checklist 4, row 4).

Thoth persists transcripts as flat markdown/text (``TranscriptArtifact`` in
``core/artifacts/media.py``; personal exports are rendered to plain lines by
``collectors/personal_transcript_connector.py``) — there is no durable
segment-level record with timestamps or confidences. This module therefore
maps:

- a whole transcript (``raw_transcript``/``transcript_text``) to one
  ``experience.utterance``; or
- an optional caller-parsed ``segments`` list (re-derived from raw exports
  or STT responses) to one utterance per segment, each with its own
  ``media_time`` selector on the ``derived_from`` Link.

Every utterance carries ``derived_from`` -> source media artifact and
``generated_by`` -> transcription run Links, plus ``has_transcript`` from
the parent artifact/session (structural retention keeps the lineage edge
even after payload erasure). Subjects propagate conservatively from the
source media (spec section 3.9).

Snapshot keys (TranscriptArtifact fields): ``transcript_id``,
``raw_transcript`` (or ``transcript_text``), ``language``, ``speaker``,
``session_id``, ``started_at``, ``ended_at``. Optional ``segments``:
``[{text, speaker?, start_ms?, end_ms?, confidence?}]``.
"""

from __future__ import annotations

from ccf.producer import Producer

from ccf.thothmap.context import (
    MapContext,
    MappedSubmissions,
    ThothMapError,
    claims,
    inherit_subjects,
    occurred_at,
    optional_str,
    origin,
    require_str,
    require_urn,
)


def utterance_submissions(
    producer: Producer,
    ctx: MapContext,
    snapshot: dict,
    *,
    source_ccf_id: str,
    media_artifact_ccf_id: str,
    run_ccf_id: str,
    session_ccf_id: str | None = None,
    revision: str | int | None = "1",
    engine: str,
    engine_version: str,
    speaker_ccf_id: str | None = None,
    source_subjects: list[dict] | None = None,
    subjects: list[dict] | None = None,
    language: str | None = None,
) -> MappedSubmissions:
    """Convert one transcript snapshot to utterance Records + Links.

    ``media_artifact_ccf_id`` is the mapped source-media artifact the
    transcript was derived from; ``run_ccf_id`` is the mapped transcription
    ``process.run``. Both are required — a transcript without exact
    provenance is not admittable evidence.
    """
    require_urn(source_ccf_id, "record", field="source_ccf_id")
    require_urn(media_artifact_ccf_id, "record", field="media_artifact_ccf_id")
    require_urn(run_ccf_id, "record", field="run_ccf_id")
    if session_ccf_id is not None:
        require_urn(session_ccf_id, "record", field="session_ccf_id")
    if speaker_ccf_id is not None:
        require_urn(speaker_ccf_id, "record", field="speaker_ccf_id")

    transcript_id = require_str(snapshot, "transcript_id", what="transcript")
    text_language = language or optional_str(snapshot, "language") or "und"
    resolved_subjects, coverage = inherit_subjects(subjects, source_subjects)

    segments = snapshot.get("segments")
    if segments is None:
        text = (
            optional_str(snapshot, "raw_transcript")
            or optional_str(snapshot, "transcript_text")
        )
        if not text:
            raise ThothMapError("transcript snapshot requires transcript text")
        segments = [{"text": text, "speaker": snapshot.get("speaker")}]
    if not isinstance(segments, list) or not segments:
        raise ThothMapError("transcript 'segments' must be a non-empty list")

    started = snapshot.get("started_at")
    ended = snapshot.get("ended_at")

    result = MappedSubmissions()
    multiple = len(segments) > 1
    for index, segment in enumerate(segments, start=1):
        if not isinstance(segment, dict):
            raise ThothMapError("transcript segments must be objects")
        seg_text = segment.get("text")
        if not isinstance(seg_text, str) or not seg_text:
            raise ThothMapError(f"transcript segment {index} requires non-empty 'text'")
        native_id = (
            f"{transcript_id}/utterance-{index}" if multiple else transcript_id
        )

        transcription = {
            "engine": engine,
            "engine_version": str(engine_version),
        }
        confidence = segment.get("confidence")
        if confidence is not None:
            confidence = float(confidence)
            if not 0.0 <= confidence <= 1.0:
                raise ThothMapError(
                    f"transcript segment {index} confidence out of range: {confidence}"
                )
            transcription["mean_confidence"] = confidence
        transcription["language_detected"] = text_language

        utterance_claims = claims(
            ctx,
            basis="quoted_statement" if speaker_ccf_id else "machine_inference",
            asserted_by=speaker_ccf_id or producer.producer_id,
            subjects=resolved_subjects,
            subject_coverage=coverage,
            data_classes=["speech_content"],
        )
        utterance = producer.new_record(
            type="experience.utterance",
            claims=utterance_claims,
            occurred_at=occurred_at(started, ended) if started is not None else None,
            origin=origin(source_ccf_id, native_id, revision),
            payload={
                "text": seg_text,
                "language": text_language,
                "speaker_id": speaker_ccf_id,
                "sequence": str(index),
                "transcription": transcription,
                "extensions": {
                    "thoth_transcript_id": transcript_id,
                    "thoth_speaker_label": segment.get("speaker"),
                    "thoth_session_id": snapshot.get("session_id"),
                },
            },
        )
        result.records.append(utterance)

        selector: dict = {}
        start_ms = segment.get("start_ms")
        end_ms = segment.get("end_ms")
        if start_ms is not None or end_ms is not None:
            selector = {
                "kind": "media_time",
                "start_ms": int(start_ms or 0),
                "end_ms": int(end_ms if end_ms is not None else start_ms or 0),
            }
        result.links.append(
            producer.new_link(
                type="ccf.derived_from",
                from_id=utterance["id"],
                to_id=media_artifact_ccf_id,
                claims=claims(ctx),
                selector=selector,
            )
        )
        result.links.append(
            producer.new_link(
                type="ccf.generated_by",
                from_id=utterance["id"],
                to_id=run_ccf_id,
                claims=claims(ctx),
                selector={},
            )
        )
        if session_ccf_id is not None:
            result.links.append(
                producer.new_link(
                    type="ccf.captured_in",
                    from_id=utterance["id"],
                    to_id=session_ccf_id,
                    claims=claims(ctx),
                    selector={},
                )
            )
        parent = media_artifact_ccf_id if session_ccf_id is None else session_ccf_id
        result.links.append(
            producer.new_link(
                type="ccf.has_transcript",
                from_id=parent,
                to_id=utterance["id"],
                claims=claims(ctx),
                selector={},
            )
        )
    return result
