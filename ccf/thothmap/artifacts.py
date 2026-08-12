"""Original files/media -> Blob + ``experience.artifact`` + Links (checklist 4, row 3).

Maps a Thoth ``RawArtifactRef`` (``core.capture_event_store.RawArtifactRef``
/ ``raw_artifact_refs`` table — the immutable, sha256-pinned reference to a
captured file under ``knowledge_vault/``) to:

- one Blob submission carrying the actual bytes (content commitment is
  verified against the declared ``sha256``/``size_bytes`` — a mismatch
  fails closed before anything is signed);
- one ``experience.artifact`` Record (``artifact_role`` default
  ``raw_capture``) whose origin carries the Thoth ``raw_ref_id``;
- a ``ccf.has_blob`` Link artifact -> Blob;
- a ``ccf.captured_in`` Link artifact -> session when the capture session
  has been mapped.

The caller reads the file bytes; converters never touch the filesystem.
"""

from __future__ import annotations

import hashlib

from ccf.producer import Producer

from ccf.thothmap.context import (
    MapContext,
    MappedSubmissions,
    ThothMapError,
    claims,
    data_classes_for_media_type,
    inherit_subjects,
    occurred_at,
    optional_str,
    origin,
    require_str,
    require_urn,
)


def media_submissions(
    producer: Producer,
    ctx: MapContext,
    snapshot: dict,
    *,
    data: bytes,
    source_ccf_id: str,
    session_ccf_id: str | None = None,
    revision: str | int | None = "1",
    artifact_role: str = "raw_capture",
    description: str | None = None,
    subjects: list[dict] | None = None,
    source_subjects: list[dict] | None = None,
    data_classes: list[str] | None = None,
    name: str | None = None,
) -> MappedSubmissions:
    """Convert one raw captured file to Blob + artifact + provenance Links.

    Snapshot keys (RawArtifactRef fields): ``raw_ref_id``, ``path``,
    ``sha256``, ``size_bytes``, ``mime_type``, ``created_at``. The blob's
    origin native ID is the ``raw_ref_id``; the artifact shares the same
    native ID (distinct object kinds never collide in the origin index).
    """
    require_urn(source_ccf_id, "record", field="source_ccf_id")
    if session_ccf_id is not None:
        require_urn(session_ccf_id, "record", field="session_ccf_id")
    raw_ref_id = require_str(snapshot, "raw_ref_id", what="raw artifact ref")
    path = require_str(snapshot, "path", what="raw artifact ref")
    media_type = optional_str(snapshot, "mime_type") or "application/octet-stream"
    if not isinstance(data, (bytes, bytearray)) or not data:
        raise ThothMapError("media mapping requires the captured bytes")

    declared_sha = optional_str(snapshot, "sha256")
    actual_sha = hashlib.sha256(bytes(data)).hexdigest()
    if declared_sha is not None and declared_sha.lower() != actual_sha:
        raise ThothMapError(
            f"sha256 mismatch for {raw_ref_id}: declared {declared_sha}, actual {actual_sha}"
        )
    declared_size = snapshot.get("size_bytes")
    if declared_size is not None and int(declared_size) != len(data):
        raise ThothMapError(
            f"size mismatch for {raw_ref_id}: declared {declared_size}, actual {len(data)}"
        )

    classes = (
        list(data_classes)
        if data_classes is not None
        else data_classes_for_media_type(media_type)
    )
    resolved_subjects, coverage = inherit_subjects(subjects, source_subjects)
    when = snapshot.get("created_at")

    blob_claims = claims(
        ctx,
        subjects=resolved_subjects,
        subject_coverage=coverage,
        data_classes=classes,
    )
    blob_sub, blob_bytes = producer.new_blob(
        data=bytes(data),
        media_type=media_type,
        claims=blob_claims,
        origin=origin(source_ccf_id, raw_ref_id, revision),
    )

    artifact_claims = claims(
        ctx,
        subjects=resolved_subjects,
        subject_coverage=coverage,
        data_classes=classes,
    )
    artifact = producer.new_record(
        type="experience.artifact",
        claims=artifact_claims,
        occurred_at=occurred_at(when) if when is not None else None,
        origin=origin(source_ccf_id, raw_ref_id, revision),
        payload={
            "name": name or path.rsplit("/", 1)[-1],
            "media_type": media_type,
            "description": description or f"Raw capture {raw_ref_id}",
            "external_uri": None,
            "artifact_role": artifact_role,
            "extensions": {
                "thoth_raw_ref_id": raw_ref_id,
                "thoth_path": path,
                "thoth_sha256": actual_sha,
            },
        },
    )

    has_blob = producer.new_link(
        type="ccf.has_blob",
        from_id=artifact["id"],
        to_id=blob_sub["id"],
        claims=claims(ctx),
        selector={},
    )
    links = [has_blob]
    if session_ccf_id is not None:
        links.append(
            producer.new_link(
                type="ccf.captured_in",
                from_id=artifact["id"],
                to_id=session_ccf_id,
                claims=claims(ctx),
                selector={},
            )
        )
    return MappedSubmissions(
        records=[artifact],
        links=links,
        blobs=[blob_sub],
        blob_data={blob_sub["id"]: blob_bytes},
    )
