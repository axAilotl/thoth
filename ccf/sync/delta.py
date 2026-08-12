"""Delta packs (spec sections 6.7, 11.4).

A delta pack carries a bounded commit range — commits, members, the
member objects' compartments, and Blob bytes — as a compressed
ZIP-compatible file plus a chunk sidecar for resumable byte-range
transfer. Apply is same-identity only: the pack must extend the local
head exactly (its first commit's parent is the local head hash), the
segment is verified offline, and a full in-database ``verify_chain``
replays the result. Divergent chains are forks, not deltas.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

from ccf.admission import load_archive
from ccf.hashing import canonical_digest
from ccf.ids import generate_id
from ccf.journal import verify_chain
from ccf.objects import now_timestamp
from ccf.sync.chunks import write_sidecar
from ccf.sync.completeness import classify_references
from ccf.sync.packio import (
    IncompletePackError,
    PackError,
    PackReader,
    StreamEntry,
    json_bytes,
    load_pack_objects,
    ndjson_bytes,
    verify_stream_digests,
)
from ccf.sync.restore import append_pack_commits, insert_pack_objects
from ccf.sync.verify import verify_commit_chain, verify_pack_object

DELTA_PACK_FORMAT = "ccf.delta-pack/0.1.1"
SCHEMA_DELTA_MANIFEST = "urn:ccf:schema:0.1.1:sync.delta-pack-manifest"


class DeltaPackError(PackError):
    """Raised when a delta pack cannot be built or applied safely."""


def _header_dict(row) -> dict:
    return {
        "spec": "ccf/0.1.1",
        "object_kind": row[1],
        "id": row[0],
        "hash_profile": "ccf-jcs-sha256-v2",
        "structural_commitment": row[2],
        "semantic_commitment": row[3],
        "object_hash": row[4],
    }


def build_delta_pack(
    conn,
    *,
    archive_id: str,
    from_sequence: int,
    through_sequence: int,
    out_file: str | Path,
    schemas,
    clock=now_timestamp,
    chunk_size: int | None = None,
) -> dict:
    """Build a compressed delta pack for commits (from, through]; returns manifest.

    Also writes ``<out_file>.chunks.json`` so the pack can move with
    verified resume over any transport.
    """
    archive = load_archive(conn, archive_id)
    head = conn.execute(
        "SELECT sequence FROM archive_head WHERE archive_id = %s", (archive_id,)
    ).fetchone()
    if head is None:
        raise DeltaPackError(f"archive {archive_id} has no head")
    head_sequence = int(head[0])
    if not (0 <= from_sequence < through_sequence <= head_sequence):
        raise DeltaPackError(
            f"invalid range ({from_sequence}, {through_sequence}] for head "
            f"{head_sequence}"
        )

    commits = [
        {
            "sequence": str(int(row[0])),
            "record_id": row[1],
            "parent_commit_hash": row[2],
            "commit_hash": row[3],
            "merkle_root": row[4],
        }
        for row in conn.execute(
            """
            SELECT sequence, commit_record_id, parent_commit_hash, commit_hash,
                   batch_merkle_root
            FROM commit_journal
            WHERE archive_id = %s AND sequence > %s AND sequence <= %s
            ORDER BY sequence ASC
            """,
            (archive_id, from_sequence, through_sequence),
        ).fetchall()
    ]
    members = [
        {
            "commit_sequence": str(int(row[0])),
            "commit_position": int(row[1]),
            "admitted_at": row[2],
            "object_kind": row[3],
            "object_id": row[4],
            "object_hash": row[5],
        }
        for row in conn.execute(
            """
            SELECT commit_sequence, commit_position, admitted_at, object_kind,
                   object_id, object_hash
            FROM commit_member
            WHERE archive_id = %s AND commit_sequence > %s AND commit_sequence <= %s
            ORDER BY commit_sequence ASC, commit_position ASC
            """,
            (archive_id, from_sequence, through_sequence),
        ).fetchall()
    ]
    object_ids = {commit["record_id"] for commit in commits}
    object_ids.update(member["object_id"] for member in members)

    # Assemble pack entries in memory order: streams -> zip.
    entries: list[tuple[str, bytes, bool]] = []  # (name, bytes, required)
    from ccf.hashing import encode_b64url

    headers_by_kind: dict[str, list[dict]] = {"record": [], "link": [], "blob": []}
    from ccf.sync.packio import PackObject

    objects: dict[str, PackObject] = {}
    blob_payloads: dict[str, bytes] = {}
    for object_id in sorted(object_ids):
        row = conn.execute(
            """
            SELECT id, object_kind, structural_commitment, semantic_commitment,
                   object_hash
            FROM object_header WHERE id = %s
            """,
            (object_id,),
        ).fetchone()
        if row is None:
            raise DeltaPackError(f"journal references missing object {object_id}")
        header = _header_dict(row)
        headers_by_kind[header["object_kind"]].append(header)
        obj = PackObject(header=header)
        for compartment, fmt, salt, content in conn.execute(
            """
            SELECT compartment, format, salt, plaintext_json FROM compartment
            WHERE object_id = %s AND state = 'plaintext'
            """,
            (object_id,),
        ).fetchall():
            setattr(
                obj,
                compartment,
                {"format": fmt, "salt": encode_b64url(bytes(salt)), "content": content},
            )
        objects[object_id] = obj
        if header["object_kind"] == "blob":
            blob_row = conn.execute(
                "SELECT plaintext_bytes FROM blob_content"
                " WHERE blob_id = %s AND state = 'plaintext'",
                (object_id,),
            ).fetchone()
            if blob_row is not None:
                blob_payloads[object_id] = bytes(blob_row[0])

    for kind in ("record", "link", "blob"):
        entries.append(
            (f"objects/{kind}s.ndjson", ndjson_bytes(headers_by_kind[kind]), True)
        )
    for object_id in sorted(objects):
        obj = objects[object_id]
        kind = obj.object_kind
        uuid_part = object_id.removeprefix(f"urn:ccf:{kind}:")
        for compartment in ("structural", "semantic"):
            envelope = getattr(obj, compartment)
            if envelope is not None:
                entries.append(
                    (
                        f"compartments/{kind}s/{uuid_part}.{compartment}.json",
                        json_bytes(envelope),
                        True,
                    )
                )
        if object_id in blob_payloads:
            obj.blob_data_name = f"blob-data/{uuid_part}.bin"
            entries.append((obj.blob_data_name, blob_payloads[object_id], False))
    entries.append(("integrity/commits.ndjson", ndjson_bytes(commits), True))
    entries.append(("integrity/members.ndjson", ndjson_bytes(members), True))

    streams = [
        StreamEntry(name, digest, len(data), required)
        for name, data, required in entries
        for digest in [_sha256(data)]
    ]
    report = classify_references(
        objects,
        known_ids=_store_object_ids(conn, archive_id) - object_ids,
    )
    manifest = {
        "format": DELTA_PACK_FORMAT,
        "pack_id": generate_id("pack"),
        "archive_id": archive_id,
        "from_sequence": str(from_sequence),
        "through_sequence": str(through_sequence),
        "created_at": clock(),
        "streams": [
            entry.to_dict() for entry in sorted(streams, key=lambda e: e.path)
        ],
        "pack_digest": canonical_digest(
            "ccf:delta-pack:v1",
            [entry.to_dict() for entry in sorted(streams, key=lambda e: e.path)],
        ),
        "extensions": {
            "genesis_commit_hash": archive["genesis_commit_hash"],
            "completeness": report.to_dict(),
        },
    }
    schemas.validate(SCHEMA_DELTA_MANIFEST, manifest, what="delta pack manifest")

    out_file = Path(out_file)
    with zipfile.ZipFile(out_file, "w", zipfile.ZIP_DEFLATED) as archive_zip:
        archive_zip.writestr("manifest.json", json_bytes(manifest))
        for name, data, _ in entries:
            archive_zip.writestr(name, data)
    write_sidecar(out_file, chunk_size=chunk_size or _default_chunk_size())
    return manifest


def _default_chunk_size() -> int:
    from ccf.sync.chunks import DEFAULT_CHUNK_SIZE

    return DEFAULT_CHUNK_SIZE


def _sha256(data: bytes) -> str:
    from ccf.hashing import digest_string

    return digest_string(data)


def _store_object_ids(conn, archive_id: str) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT id FROM object_header WHERE archive_id = %s", (archive_id,)
        ).fetchall()
    }


def apply_delta_pack(
    conn,
    *,
    archive_id: str,
    pack_path: str | Path,
    schemas,
    allow_partial: bool = False,
    clock=now_timestamp,
) -> dict:
    """Apply a verified delta pack to the local archive; returns a report.

    Fails closed unless the pack's archive identity matches and its first
    commit's parent is exactly the local head hash.
    """
    archive = load_archive(conn, archive_id)
    with PackReader(pack_path) as reader:
        if not reader.has("manifest.json"):
            raise DeltaPackError("delta pack has no manifest.json")
        manifest = reader.read_json("manifest.json")
        schemas.validate(SCHEMA_DELTA_MANIFEST, manifest, what="delta pack manifest")
        if manifest["format"] != DELTA_PACK_FORMAT:
            raise DeltaPackError(f"not a delta pack: {manifest['format']!r}")
        if manifest["archive_id"] != archive_id:
            raise DeltaPackError(
                f"delta pack archive {manifest['archive_id']} != local {archive_id}"
            )
        streams = [StreamEntry.from_dict(s) for s in manifest["streams"]]
        verify_stream_digests(reader, streams)
        if canonical_digest(
            "ccf:delta-pack:v1",
            [entry.to_dict() for entry in sorted(streams, key=lambda e: e.path)],
        ) != manifest["pack_digest"]:
            raise DeltaPackError("delta pack digest mismatch (tampered)")

        contents = load_pack_objects(reader)
        commits = reader.read_ndjson("integrity/commits.ndjson")
        members = reader.read_ndjson("integrity/members.ndjson")

    head = conn.execute(
        "SELECT sequence, commit_hash FROM archive_head WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()
    if head is None:
        raise DeltaPackError(f"archive {archive_id} has no head")
    local_sequence, local_hash = int(head[0]), head[1]
    if int(manifest["from_sequence"]) != local_sequence:
        raise DeltaPackError(
            f"delta pack starts at {manifest['from_sequence']}, local head is "
            f"{local_sequence} (not a clean extension)"
        )
    if not commits or commits[0]["parent_commit_hash"] != local_hash:
        raise DeltaPackError("delta pack does not extend the local head hash")
    if int(commits[-1]["sequence"]) != int(manifest["through_sequence"]):
        raise DeltaPackError("delta pack commits do not reach through_sequence")

    for object_id, obj in contents.objects.items():
        verify_pack_object(obj, blob_data=contents.blob_data.get(object_id))
    known = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT id, object_hash FROM object_header WHERE archive_id = %s",
            (archive_id,),
        ).fetchall()
    }
    chain = verify_commit_chain(
        commits,
        members,
        contents.objects,
        expected_first_sequence=local_sequence + 1,
        expected_parent_hash=local_hash,
        known_object_hashes=known,
    )
    report = classify_references(contents.objects, known_ids=set(known))
    if not report.complete and not allow_partial:
        raise IncompletePackError(
            f"delta pack has undeclared dangling references: {report.dangling}"
        )

    inserted, skipped = insert_pack_objects(
        conn, archive_id, contents, updated_at=clock(), skip_existing=True
    )
    append_pack_commits(
        conn, archive_id, commits, members, start_sequence=local_sequence + 1
    )
    verification = verify_chain(
        conn,
        archive_id=archive_id,
        expected_genesis_hash=archive["genesis_commit_hash"],
    )
    return {
        "status": "applied",
        "archive_id": archive_id,
        "from_sequence": manifest["from_sequence"],
        "through_sequence": manifest["through_sequence"],
        "head_commit_hash": chain["head_commit_hash"],
        "objects_inserted": len(inserted),
        "objects_skipped": len(skipped),
        "partial": not report.complete,
        "completeness": report.to_dict(),
        "verification": verification,
    }
