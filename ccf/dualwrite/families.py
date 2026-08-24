"""Phase-2 converter families wired into the live dual-write mirror.

``CcfDualWriteService.mirror_capture`` mirrors the capture envelope —
source, session/run, media (Blob + artifact), and security findings — as
one signed batch. This module mirrors the remaining ``ccf.thothmap``
converter families, each as its own batch so one family's failure never
blocks another:

- transcripts (``experience.utterance`` per transcript/segment),
- semantic entities (``semantic.entity`` from canonical identity),
- semantic candidate assertions (``semantic.assertion``, materialized as
  review targets — Thoth has no production candidate-creation path
  outside the review flows),
- review decisions (``governance.review_decision`` for both the artifact
  review queue and the semantic memory review service),
- wiki projections (``experience.artifact`` with
  ``artifact_role="wiki_projection"`` and mandatory evidence Links).

Every function is idempotent via the archive origin index (or via
admission returning ``existing`` for an identical origin tuple), so
re-mirroring never duplicates or conflicts. Callers wrap each family in
its own try/except and ledger failures through
``service.record_error(...)`` — the mirror stays fail-open per family.

Reviewer identity: Thoth records review actors as free text and the
dual-write archive bootstraps exactly one Person (the operator), so
review decisions are mirrored with ``service.ctx.person_id`` as the
reviewer; the free-text actor is preserved verbatim in the decision
payload extensions (``thoth_actor``) by the converter.
"""

from __future__ import annotations

import logging
from typing import Mapping

from ccf.db import open_ccf_connection
from ccf.thothmap import review as thothmap_review
from ccf.thothmap import semantic as thothmap_semantic
from ccf.thothmap import transcripts as thothmap_transcripts
from ccf.thothmap import wiki as thothmap_wiki
from ccf.thothmap.context import MappedSubmissions

from ccf.dualwrite.conventions import (
    ASSERTION_REVISION,
    ENTITY_REVISION,
    REVIEW_REVISION,
    SESSION_REVISION,
    TRANSCRIPT_REVISION,
    WIKI_REVISION,
    raw_ref_id_for,
    run_native_id,
    source_record_id,
)
from ccf.dualwrite.service import CcfDualWriteService, DualWriteError

logger = logging.getLogger(__name__)

_OK_ADMISSION_STATUSES = {"admitted", "existing"}


def _required(value, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise DualWriteError(f"dual-write mirror requires non-empty {field}")
    return value


def _source_ccf_id(service: CcfDualWriteService, source: Mapping) -> str:
    return source_record_id(
        service.archive.archive_id,
        _required(source.get("source_id"), "source.source_id"),
    )


def _admit(
    service: CcfDualWriteService, parts: MappedSubmissions, *, label: str
) -> dict:
    """Sign and admit one family batch; raise DualWriteError on failure."""
    batch = service.producer.create_batch(
        records=parts.records,
        links=parts.links,
        blobs=parts.blobs,
        blob_data=parts.blob_data or None,
    )
    result = service.archive.admit_batch(batch, blob_bytes=parts.blob_data or None)
    bad = [
        admission
        for admission in result.get("admissions", [])
        if admission.get("status") not in _OK_ADMISSION_STATUSES
    ]
    if result.get("status") != "accepted" or bad:
        raise DualWriteError(
            f"dual-write admission failed for {label}: "
            f"batch status {result.get('status')!r}, rejections: {bad[:3]}"
        )
    return {
        "status": "accepted",
        "archive_id": service.archive.archive_id,
        "batch_id": batch["batch_id"],
        "commit_sequence": result.get("commit_sequence"),
        "admissions": result.get("admissions", []),
    }


# ---------------------------------------------------------------------------
# Origin lookups shared by the family call sites
# ---------------------------------------------------------------------------


def find_mirrored_object(
    service: CcfDualWriteService,
    *,
    native_id: str,
    revision: str,
    object_kind: str = "record",
) -> tuple[str, str] | None:
    """Return ``(source_ccf_id, object_id)`` for one origin tuple, or None.

    Cross-source counterpart of ``service._origin_index``: used to resolve
    mirrored objects (entities, assertions) from Thoth-native IDs at call
    sites that do not know which capture source produced them.
    """
    with open_ccf_connection(service.settings.store) as conn:
        row = conn.execute(
            """
            SELECT source_id, object_id FROM origin_index
            WHERE archive_id = %s AND native_id = %s
              AND revision = %s AND object_kind = %s
            ORDER BY source_id
            LIMIT 1
            """,
            (service.archive.archive_id, native_id, str(revision), object_kind),
        ).fetchone()
    if row is None:
        return None
    return row[0], row[1]


def mirrored_media_artifact(
    service: CcfDualWriteService, *, source_id: str, sha256: str, path: str
) -> str | None:
    """CCF URN of the mirrored media artifact for one legacy raw payload.

    ``path`` must be the absolute resolved path exactly as the capture
    lifecycle recorded it (the raw_ref_id derivation is path-sensitive).
    """
    source_ccf_id = source_record_id(service.archive.archive_id, source_id)
    raw_ref_id = raw_ref_id_for(source_id, sha256, path)
    origins = service._origin_index(source_ccf_id)
    return origins.get((raw_ref_id, sha256, "record"))


# ---------------------------------------------------------------------------
# Transcripts
# ---------------------------------------------------------------------------


def mirror_transcript(
    service: CcfDualWriteService,
    *,
    source: Mapping,
    transcript: Mapping,
    media_artifact_ccf_id: str,
    run_ccf_id: str | None = None,
    session_ccf_id: str | None = None,
    session_id: str | None = None,
    engine: str,
    engine_version: str = "unknown",
) -> dict:
    """Mirror one captured transcript's utterances with their Links.

    ``media_artifact_ccf_id`` is the mirrored artifact of the capture's raw
    payload; ``run_ccf_id`` the mirrored run that produced the transcript
    (falls back to the mirrored capture run for ``session_id``). A
    transcript without a mirrored run has no admittable provenance and
    fails closed.
    """
    source_ccf_id = _source_ccf_id(service, source)
    transcript_id = _required(
        transcript.get("transcript_id"), "transcript.transcript_id"
    )
    origins = service._origin_index(source_ccf_id)
    if (transcript_id, TRANSCRIPT_REVISION, "record") in origins or (
        f"{transcript_id}/utterance-1",
        TRANSCRIPT_REVISION,
        "record",
    ) in origins:
        return {"status": "existing", "archive_id": service.archive.archive_id}

    if run_ccf_id is None and session_id:
        run_ccf_id = origins.get(
            (run_native_id(session_id), SESSION_REVISION, "record")
        )
    if run_ccf_id is None:
        raise DualWriteError(
            f"no mirrored transcription run for transcript {transcript_id}; "
            "mirror the capture (session/run) before its utterances"
        )

    mapped = thothmap_transcripts.utterance_submissions(
        service.producer,
        service.ctx,
        dict(transcript),
        source_ccf_id=source_ccf_id,
        media_artifact_ccf_id=media_artifact_ccf_id,
        run_ccf_id=run_ccf_id,
        session_ccf_id=session_ccf_id,
        revision=TRANSCRIPT_REVISION,
        engine=engine,
        engine_version=engine_version,
    )
    receipt = _admit(service, mapped, label=f"transcript {transcript_id}")
    receipt["utterance_ids"] = [record["id"] for record in mapped.records]
    return receipt


# ---------------------------------------------------------------------------
# Semantic entities and candidate assertions
# ---------------------------------------------------------------------------


def mirror_entity(
    service: CcfDualWriteService, *, source: Mapping, entity: Mapping
) -> dict:
    """Mirror one canonical entity as a ``semantic.entity`` Record."""
    source_ccf_id = _source_ccf_id(service, source)
    canonical_id = _required(entity.get("canonical_id"), "entity.canonical_id")
    origins = service._origin_index(source_ccf_id)
    existing = origins.get((canonical_id, ENTITY_REVISION, "record"))
    if existing is not None:
        return {
            "status": "existing",
            "archive_id": service.archive.archive_id,
            "entity_id": existing,
        }
    mapped = thothmap_semantic.entity_submission(
        service.producer,
        service.ctx,
        dict(entity),
        source_ccf_id=source_ccf_id,
        revision=ENTITY_REVISION,
    )
    receipt = _admit(service, mapped, label=f"entity {canonical_id}")
    receipt["entity_id"] = mapped.records[0]["id"]
    return receipt


def mirror_candidate_assertion(
    service: CcfDualWriteService,
    *,
    source_ccf_id: str,
    candidate: Mapping,
    subject_ccf_id: str | None = None,
    evidence_ccf_ids: list[str] | None = None,
) -> str:
    """Mirror one semantic memory candidate as a candidate assertion.

    Returns the assertion URN (existing or newly admitted) so review
    decisions can target it.
    """
    candidate_id = _required(
        candidate.get("candidate_id"), "candidate.candidate_id"
    )
    found = find_mirrored_object(
        service, native_id=candidate_id, revision=ASSERTION_REVISION
    )
    if found is not None:
        return found[1]
    mapped = thothmap_semantic.assertion_submissions(
        service.producer,
        service.ctx,
        dict(candidate),
        source_ccf_id=source_ccf_id,
        subject_ccf_id=subject_ccf_id,
        evidence_ccf_ids=evidence_ccf_ids,
        revision=ASSERTION_REVISION,
    )
    _admit(service, mapped, label=f"candidate assertion {candidate_id}")
    return mapped.records[0]["id"]


# ---------------------------------------------------------------------------
# Review decisions
# ---------------------------------------------------------------------------


def mirror_review_decision(
    service: CcfDualWriteService,
    *,
    source_ccf_id: str,
    review: Mapping,
    target_ccf_ids: list[str],
    evidence_ccf_ids: list[str] | None = None,
    native_id: str | None = None,
) -> dict:
    """Mirror one Thoth review event as a ``governance.review_decision``.

    The reviewer is always the archive principal (see module docstring).
    Re-mirroring the same event maps to the same origin tuple and admits
    as ``existing``.
    """
    mapped = thothmap_review.review_submissions(
        service.producer,
        service.ctx,
        dict(review),
        source_ccf_id=source_ccf_id,
        target_ccf_ids=target_ccf_ids,
        reviewer_ccf_id=service.ctx.person_id,
        evidence_ccf_ids=evidence_ccf_ids,
        revision=REVIEW_REVISION,
        native_id=native_id,
    )
    # Idempotency: the converter's default native_id is deterministic per
    # event, so an already-mirrored decision is skipped before signing
    # (re-admitting a fresh URN under the same origin tuple would conflict).
    origin = mapped.records[0].get("origin") or {}
    found = find_mirrored_object(
        service,
        native_id=origin.get("native_id", ""),
        revision=origin.get("revision", REVIEW_REVISION),
    )
    if found is not None:
        return {
            "status": "existing",
            "archive_id": service.archive.archive_id,
            "decision_id": found[1],
        }
    receipt = _admit(
        service, mapped, label=f"review {review.get('action')!r}"
    )
    receipt["decision_id"] = mapped.records[0]["id"]
    return receipt


# ---------------------------------------------------------------------------
# Wiki projections
# ---------------------------------------------------------------------------


def mirror_wiki_projection(
    service: CcfDualWriteService,
    *,
    page: Mapping,
    evidence: list[tuple[str, str]],
) -> dict:
    """Mirror one compiled wiki page as a projection artifact.

    ``evidence`` is a non-empty list of ``(source_ccf_id, object_id)``
    pairs naming the exact mirrored objects the page was compiled from;
    the first entry's source anchors the projection's origin tuple. The
    page ``input_hash`` is the origin revision when present, so a
    recompiled page re-mirrors while an unchanged one is skipped. Pages
    whose inputs cannot be resolved to mirrored objects must NOT be
    passed here — the converter rightly refuses projections without
    evidence.
    """
    if not evidence:
        raise DualWriteError(
            "wiki projection mirror requires resolved evidence objects"
        )
    slug = _required(page.get("slug"), "wiki.slug")
    revision = str(page.get("input_hash") or WIKI_REVISION)
    source_ccf_id = evidence[0][0]
    origins = service._origin_index(source_ccf_id)
    existing = origins.get((f"wiki:{slug}", revision, "record"))
    if existing is not None:
        return {
            "status": "existing",
            "archive_id": service.archive.archive_id,
            "projection_id": existing,
        }
    mapped = thothmap_wiki.wiki_projection_submissions(
        service.producer,
        service.ctx,
        dict(page),
        source_ccf_id=source_ccf_id,
        evidence_ccf_ids=[object_id for _, object_id in evidence],
        revision=revision,
    )
    receipt = _admit(service, mapped, label=f"wiki projection {slug}")
    receipt["projection_id"] = mapped.records[0]["id"]
    return receipt
