"""Mindpack export (spec sections 11.1, 11.5).

Serializes a live archive store into the boring-transport layout: object
NDJSON streams, per-compartment envelopes, Blob bytes, the commit journal
and members, producer batches/heads, the archive row, lineage heads, the
origin index, the pinned schemas/registries, and a completeness-reporting
manifest validated against ``objects.mindpack-manifest``.

Unavailable compartments are never fabricated: a withheld or erased
compartment simply has no file, and the object is declared in the
manifest's ``withheld``/``erased`` lists so reference completeness
(spec 2.5) stays machine-checkable.
"""

from __future__ import annotations

from pathlib import Path

from ccf.admission import load_archive
from ccf.hashing import blob_content_commitment, encode_b64url
from ccf.ids import generate_id
from ccf.objects import compartment_format, now_timestamp
from ccf.sync.completeness import classify_references
from ccf.sync.packio import PackObject, PackWriter

MINDPACK_FORMAT = "ccf.mindpack/0.1.2"
SCHEMA_MINDPACK_MANIFEST = "urn:ccf:schema:0.1.2:objects.mindpack-manifest"


class ExportError(RuntimeError):
    """Raised when the local store cannot be exported consistently."""


def _header_dict(row) -> dict:
    return {
        "spec": "ccf/0.1.2",
        "object_kind": row[1],
        "id": row[0],
        "hash_profile": "ccf-jcs-sha256-v2",
        "structural_commitment": row[2],
        "semantic_commitment": row[3],
        "object_hash": row[4],
    }


def _load_store_objects(conn, archive_id: str) -> list[PackObject]:
    """Every object of the archive, in journal order (members then commits)."""
    headers = {
        row[0]: _header_dict(row)
        for row in conn.execute(
            """
            SELECT id, object_kind, structural_commitment, semantic_commitment,
                   object_hash
            FROM object_header WHERE archive_id = %s
            """,
            (archive_id,),
        ).fetchall()
    }
    compartments: dict[str, dict[str, dict | None]] = {}
    states: dict[str, dict[str, str]] = {}
    for object_id, compartment, state, fmt, salt, content in conn.execute(
        """
        SELECT object_id, compartment, state, format, salt, plaintext_json
        FROM compartment
        """
    ).fetchall():
        envelope = None
        if state == "plaintext":
            envelope = {
                "format": fmt,
                "salt": encode_b64url(bytes(salt)),
                "content": content,
            }
        compartments.setdefault(object_id, {})[compartment] = envelope
        states.setdefault(object_id, {})[compartment] = state

    blob_states: dict[str, str] = {}
    blob_bytes: dict[str, bytes] = {}
    for blob_id, state, data in conn.execute(
        "SELECT blob_id, state, plaintext_bytes FROM blob_content"
    ).fetchall():
        blob_states[blob_id] = state
        if state == "plaintext":
            blob_bytes[blob_id] = bytes(data)

    # Journal order: for each commit, its members in position order, then
    # the commit Record itself.
    ordered_ids: list[str] = []
    for sequence, record_id in conn.execute(
        """
        SELECT sequence, commit_record_id FROM commit_journal
        WHERE archive_id = %s ORDER BY sequence ASC
        """,
        (archive_id,),
    ).fetchall():
        for (object_id,) in conn.execute(
            """
            SELECT object_id FROM commit_member
            WHERE archive_id = %s AND commit_sequence = %s
            ORDER BY commit_position ASC
            """,
            (archive_id, sequence),
        ).fetchall():
            ordered_ids.append(object_id)
        ordered_ids.append(record_id)

    objects: list[PackObject] = []
    seen: set[str] = set()
    for object_id in ordered_ids:
        if object_id in seen:
            continue
        seen.add(object_id)
        header = headers.get(object_id)
        if header is None:
            raise ExportError(f"journal references object without header: {object_id}")
        obj = PackObject(
            header=header,
            structural=compartments.get(object_id, {}).get("structural"),
            semantic=compartments.get(object_id, {}).get("semantic"),
        )
        if header["object_kind"] == "blob" and object_id in blob_bytes:
            obj.blob_data_name = f"blob-data/{object_id.removeprefix('urn:ccf:blob:')}.bin"
            obj._blob_bytes = blob_bytes[object_id]
        obj._states = states.get(object_id, {})  # export-local annotation
        obj._blob_state = blob_states.get(object_id)
        objects.append(obj)
    if len(seen) != len(headers):
        orphans = sorted(set(headers) - seen)
        raise ExportError(f"objects outside the journal: {orphans}")
    return objects


def export_mindpack(
    conn,
    *,
    archive_id: str,
    package_root: str | Path,
    out_dir: str | Path,
    schemas,
    clock=now_timestamp,
    mode: str = "restore",
    external_dependencies: list[dict] | None = None,
) -> dict:
    """Export the archive under ``out_dir``; returns the manifest document.

    ``external_dependencies`` declares references that intentionally resolve
    outside the pack (``object_id`` + ``reason``, optional ``locator``).
    Undeclared dangling references mark the pack incomplete in its manifest
    instead of being silently dropped (spec 2.5).
    """
    if mode not in ("restore", "replica"):
        raise ExportError(f"mindpack export mode must be restore/replica: {mode!r}")
    package_root = Path(package_root)
    archive = load_archive(conn, archive_id)
    head_row = conn.execute(
        "SELECT sequence, commit_hash FROM archive_head WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()
    if head_row is None:
        raise ExportError(f"archive {archive_id} has no head")
    head_sequence, head_commit_hash = int(head_row[0]), head_row[1]
    extra = conn.execute(
        "SELECT hash_profile, signature_profile, signer_key_id, erasure_domain_id,"
        "       created_at FROM archive WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()

    objects = _load_store_objects(conn, archive_id)
    withheld: set[str] = set()
    erased: set[str] = set()
    for obj in objects:
        obj_states = set(obj._states.values())
        if obj.object_kind == "blob" and obj._blob_state:
            obj_states.add(obj._blob_state)
        if "erased" in obj_states:
            erased.add(obj.object_id)
        elif obj_states & {"withheld", "encrypted"}:
            # Encrypted compartments are portable but not exportable as
            # plaintext; they are declared withheld in the pack.
            withheld.add(obj.object_id)

    external_dependencies = list(external_dependencies or [])
    external_ids = {dep["object_id"] for dep in external_dependencies}
    # Object IDs admitted into this archive under foreign custody (11.3).
    foreign_object_ids = _foreign_object_ids(conn) if _has_foreign_custody(conn) else set()

    writer = PackWriter(out_dir)

    # Object streams + compartments + blob bytes.
    counts = {"records": 0, "links": 0, "blobs": 0}
    streams_by_kind: dict[str, list[dict]] = {"record": [], "link": [], "blob": []}
    for obj in objects:
        kind = obj.object_kind
        streams_by_kind[kind].append(obj.header)
        counts[f"{kind}s"] += 1
        for compartment in ("structural", "semantic"):
            envelope = getattr(obj, compartment)
            if envelope is None:
                continue
            if envelope["format"] != compartment_format(kind, compartment):
                raise ExportError(
                    f"envelope format mismatch for {obj.object_id}/{compartment}"
                )
            writer.write_json(
                f"compartments/{kind}s/"
                f"{obj.object_id.removeprefix(f'urn:ccf:{kind}:')}.{compartment}.json",
                envelope,
            )
        if obj.blob_data_name is not None:
            data = obj._blob_bytes
            semantic = (obj.semantic or {}).get("content") or {}
            structural = (obj.structural or {}).get("content") or {}
            salt = semantic.get("content_salt")
            if not salt or blob_content_commitment(salt, data) != structural.get(
                "content_commitment"
            ):
                raise ExportError(
                    f"local blob {obj.object_id} fails its content commitment"
                )
            writer.write_bytes(obj.blob_data_name, data, required=False)
    for kind in ("record", "link", "blob"):
        writer.write_ndjson(f"objects/{kind}s.ndjson", streams_by_kind[kind])

    # Integrity streams.
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
            FROM commit_journal WHERE archive_id = %s ORDER BY sequence ASC
            """,
            (archive_id,),
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
            FROM commit_member WHERE archive_id = %s
            ORDER BY commit_sequence ASC, commit_position ASC
            """,
            (archive_id,),
        ).fetchall()
    ]
    writer.write_ndjson("integrity/commits.ndjson", commits)
    writer.write_ndjson("integrity/members.ndjson", members)

    # Operational restore streams.
    writer.write_json(
        "archive.json",
        {
            "format": "ccf.archive-row/0.1.2",
            "archive_id": archive_id,
            "epoch_id": archive["epoch_id"],
            "genesis_commit_hash": archive["genesis_commit_hash"],
            "hash_profile": extra[0],
            "signature_profile": extra[1],
            "semantic_catalog_root": archive["semantic_catalog_root"],
            "active_profiles": list(archive["active_profiles"]),
            "signer_key_id": extra[2],
            "erasure_domain_id": extra[3],
            "created_at": extra[4],
        },
    )
    writer.write_ndjson(
        "lineage-heads.ndjson",
        [
            {
                "lineage_id": row[0],
                "head_record_id": row[1],
                "head_record_hash": row[2],
                "head_commit_sequence": str(int(row[3])),
                "state": row[4],
                "valid_from": row[5],
                "expires_at": row[6],
            }
            for row in conn.execute(
                """
                SELECT lineage_id, head_record_id, head_record_hash,
                       head_commit_sequence, state, valid_from, expires_at
                FROM lineage_head WHERE archive_id = %s ORDER BY lineage_id
                """,
                (archive_id,),
            ).fetchall()
        ],
    )
    writer.write_ndjson(
        "origin-index.ndjson",
        [
            {
                "source_id": row[0],
                "native_id": row[1],
                "revision": row[2],
                "submission_hash": row[3],
                "object_kind": row[4],
                "object_id": row[5],
                "lifecycle": row[6],
            }
            for row in conn.execute(
                """
                SELECT source_id, native_id, revision, submission_hash,
                       object_kind, object_id, lifecycle
                FROM origin_index WHERE archive_id = %s
                ORDER BY source_id, native_id, revision, object_kind
                """,
                (archive_id,),
            ).fetchall()
        ],
    )
    writer.write_ndjson(
        "producer-heads.ndjson",
        [
            {
                "producer_id": row[0],
                "producer_sequence": str(int(row[1])),
                "batch_hash": row[2],
                "credential_id": row[3],
                "updated_at": row[4],
            }
            for row in conn.execute(
                """
                SELECT producer_id, producer_sequence, batch_hash, credential_id,
                       updated_at
                FROM producer_head ORDER BY producer_id
                """
            ).fetchall()
        ],
    )
    for batch_id, batch_json in conn.execute(
        "SELECT batch_id, batch_json FROM producer_batch ORDER BY producer_id,"
        " producer_sequence"
    ).fetchall():
        writer.write_json(
            f"producer-batches/{batch_id.removeprefix('urn:ccf:batch:')}.json",
            batch_json,
        )

    # Pinned semantics: schemas, registries, semantic catalog.
    for subdir in ("schemas", "registries"):
        source_dir = package_root / subdir
        for path in sorted(source_dir.rglob("*")):
            if path.is_file():
                writer.write_file(
                    f"{subdir}/{path.relative_to(source_dir)}", path
                )
    writer.write_file("semantic-catalog.json", package_root / "semantic-catalog.json")

    # Foreign custody proofs held by this archive (spec 11.3/11.5).
    custody_proofs = [
        f"{row[0]}:{row[1]}"
        for row in (
            conn.execute(
                "SELECT source_archive_id, head_commit_hash FROM foreign_custody"
                " WHERE archive_id = %s ORDER BY source_archive_id, head_commit_hash",
                (archive_id,),
            ).fetchall()
            if _has_foreign_custody(conn)
            else []
        )
    ]

    report = classify_references(
        {obj.object_id: obj for obj in objects},
        external_ids=external_ids,
        withheld_ids=withheld,
        erased_ids=erased,
        foreign_ids=foreign_object_ids,
    )

    compartment_availability = _compartment_availability(conn, archive_id, objects)

    manifest = {
        "format": MINDPACK_FORMAT,
        "mode": mode,
        "custody": {
            "completeness": (
                "complete" if not report.external and not report.dangling else "partial"
            ),
            "restore_capable": not report.external and not report.dangling,
        },
        "pack_id": generate_id("pack"),
        "archive_id": archive_id,
        "epoch_id": archive["epoch_id"],
        "created_at": clock(),
        "genesis_commit_hash": archive["genesis_commit_hash"],
        "head_commit_hash": head_commit_hash,
        "head_sequence": str(head_sequence),
        "semantic_catalog_root": archive["semantic_catalog_root"],
        "hash_profile": "ccf-jcs-sha256-v2",
        "profiles": list(archive["active_profiles"]),
        "counts": {
            "records": str(counts["records"]),
            "links": str(counts["links"]),
            "blobs": str(counts["blobs"]),
            "commits": str(len(commits)),
        },
        "streams": [entry.to_dict() for entry in sorted(writer.streams, key=lambda e: e.path)],
        "external_dependencies": external_dependencies,
        "withheld": sorted(withheld),
        "erased": sorted(erased),
        "foreign_custody_proofs": custody_proofs,
        "compartment_availability": compartment_availability,
        "extensions": {},
    }
    schemas.validate(SCHEMA_MINDPACK_MANIFEST, manifest, what="mindpack manifest")
    writer.write_json("manifest.json", manifest)
    return manifest


def _compartment_availability(conn, archive_id: str, objects: list[PackObject]) -> list[dict]:
    """Per-compartment availability declarations (0.1.2 manifest field).

    Every compartment of every packed object is declared with its
    availability state, commitment, and retention profile. Unavailable
    (withheld/erased) compartments carry a custody anchor — the object's
    own admission coordinates — and, for erasures, the erasure receipt
    Record as the unavailability lineage. A withheld compartment with no
    derivable lineage fails closed: the manifest must not fabricate one.
    """
    coordinates = {
        row[0]: (int(row[1]), int(row[2]))
        for row in conn.execute(
            """
            SELECT object_id, commit_sequence, commit_position
            FROM admission WHERE archive_id = %s
            """,
            (archive_id,),
        ).fetchall()
    }
    # Erasure receipts cover their targets through ccf.covers membership
    # Links (receipt -> target).
    receipt_of: dict[str, str] = {}
    for target_id, receipt_id in conn.execute(
        """
        SELECT l.plaintext_json ->> 'to_id', l.plaintext_json ->> 'from_id'
        FROM compartment l
        JOIN compartment r
          ON r.object_id = (l.plaintext_json ->> 'from_id')
         AND r.compartment = 'structural' AND r.state = 'plaintext'
         AND r.plaintext_json ->> 'type' = 'lineage.erasure_receipt'
        WHERE l.compartment = 'structural' AND l.state = 'plaintext'
          AND l.plaintext_json ->> 'type' = 'ccf.covers'
        ORDER BY l.plaintext_json ->> 'from_id'
        """
    ).fetchall():
        receipt_of.setdefault(target_id, receipt_id)

    availability_of = {
        "plaintext": "available",
        "encrypted": "withheld",
        "withheld": "withheld",
        "erased": "erased",
    }
    entries: list[dict] = []
    for obj in objects:
        states = dict(obj._states)
        if obj.object_kind == "blob" and obj._blob_state:
            states["blob_content"] = obj._blob_state
        structural_content = (obj.structural or {}).get("content") or {}
        retention_profile = structural_content.get("retention_profile")
        if retention_profile is None:
            if states.get("structural") == "erased":
                # Only the ``erasable`` profile permits structural erasure,
                # so the observed state entails the profile.
                retention_profile = "erasable"
            else:
                raise ExportError(
                    f"cannot determine retention profile for {obj.object_id}: "
                    "structural compartment is not plaintext"
                )
        for compartment in ("structural", "semantic", "blob_content"):
            if compartment == "blob_content":
                if obj.object_kind != "blob":
                    continue
                commitment = structural_content.get("content_commitment")
                if commitment is None:
                    raise ExportError(
                        f"blob {obj.object_id} has no content commitment"
                    )
            elif compartment == "semantic":
                if obj.header.get("semantic_commitment") is None:
                    continue  # object legitimately lacks a semantic compartment
                commitment = obj.header["semantic_commitment"]
            else:
                commitment = obj.header["structural_commitment"]
            state = states.get(compartment)
            if state is None:
                raise ExportError(
                    f"{obj.object_id}/{compartment} has no compartment row"
                )
            availability = availability_of[state]
            custody_proof = None
            lineage_id = None
            if availability != "available":
                position = coordinates.get(obj.object_id)
                if position is None:
                    raise ExportError(
                        f"{obj.object_id} is unavailable but has no admission "
                        "coordinates to anchor custody"
                    )
                custody_proof = f"commit:{position[0]}:{position[1]}"
                if availability == "erased":
                    lineage_id = receipt_of.get(obj.object_id)
                    if lineage_id is None:
                        raise ExportError(
                            f"{obj.object_id} is erased but no erasure receipt "
                            "lineage covers it"
                        )
                else:
                    raise ExportError(
                        f"{obj.object_id}/{compartment} is withheld; the "
                        "unavailability lineage is not derivable for a local "
                        "export (refusing to fabricate one)"
                    )
            entries.append(
                {
                    "object_kind": obj.object_kind,
                    "object_id": obj.object_id,
                    "compartment": compartment,
                    "availability": availability,
                    "commitment": commitment,
                    "retention_profile": retention_profile,
                    "source_custody_proof": custody_proof,
                    "unavailability_lineage_id": lineage_id,
                }
            )
    return entries


def _has_foreign_custody(conn) -> bool:
    row = conn.execute(
        "SELECT to_regclass('foreign_custody')"
    ).fetchone()
    return row is not None and row[0] is not None


def _foreign_object_ids(conn) -> set[str]:
    """Object IDs admitted into this archive under foreign custody."""
    rows = conn.execute(
        "SELECT commits_json FROM foreign_custody"
    ).fetchall()
    ids: set[str] = set()
    for (commits_json,) in rows:
        for commit in commits_json:
            for member in commit.get("members", []):
                ids.add(member["object_id"])
    return ids


def export_mindpack_zip(pack_dir: str | Path, out_file: str | Path) -> Path:
    """Zip an exported mindpack directory into a single container file."""
    from ccf.sync.packio import zip_pack_dir

    return zip_pack_dir(pack_dir, out_file)
