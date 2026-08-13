"""Restore/replica: reconstitute an archive from a mindpack (spec 11.2).

Restore preserves the archive identity, epoch, original admission
coordinates, journal, and head — it never re-admits objects into a new
history and never remaps portable IDs. The trusted genesis and head are
verified against the pack's own verified chain, and a full in-database
``verify_chain`` replays everything after insertion.

The ``insert_*`` helpers are shared with delta-pack apply and fork import:
they write portable rows exactly as exported, failing closed on any ID
that already exists with different content.
"""

from __future__ import annotations

from pathlib import Path

from ccf.db import CcfPostgresSettings, migrate_ccf_store, open_ccf_connection
from ccf.hashing import commit_leaf, decode_b64url, digest_string, submission_hash
from ccf.jcs import loads as jcs_loads
from ccf.journal import verify_chain
from ccf.objects import now_timestamp
from ccf.schemas import SchemaSet
from ccf.sync.completeness import CompletenessReport, classify_references
from ccf.sync.export import SCHEMA_MINDPACK_MANIFEST
from ccf.sync.manifest import (
    compare_manifest,
    derive_pack_inventory,
    unavailable_object_ids,
)
from ccf.sync.packio import (
    MANIFEST_MODE_MISMATCH,
    IncompletePackError,
    PackError,
    PackReader,
    StreamEntry,
    load_pack_objects,
    parse_ndjson,
    verify_stream_digests,
)
from ccf.sync.verify import verify_commit_chain, verify_pack_object


class RestoreError(PackError):
    """Raised when a pack cannot be restored safely."""


def _jsonb(value):
    from psycopg.types.json import Jsonb

    return Jsonb(value)


class VerifiedMindpack:
    """A fully verified mindpack, ready for restore or merge."""

    def __init__(self, reader: PackReader, schemas: SchemaSet, *, allow_partial: bool = False,
                 known_ids: set[str] | None = None,
                 allow_missing_member_objects: bool = False,
                 operation: str = "import") -> None:
        if not reader.has("manifest.json"):
            raise RestoreError("pack has no manifest.json")
        self.manifest = reader.read_json("manifest.json")
        # Format check first, so a pack from another CCF version fails
        # closed with a clear unsupported-version error rather than a bare
        # schema violation (no silent cross-version acceptance).
        if self.manifest.get("format") != "ccf.mindpack/0.1.2-rc1":
            raise RestoreError(
                f"unsupported mindpack format {self.manifest.get('format')!r}: "
                "this archive implements ccf.mindpack/0.1.2-rc1"
            )
        schemas.validate(SCHEMA_MINDPACK_MANIFEST, self.manifest, what="mindpack manifest")

        streams = [StreamEntry.from_dict(s) for s in self.manifest["streams"]]
        self.streams = streams
        self.stream_notes = verify_stream_digests(reader, streams)

        contents = load_pack_objects(reader)
        self.objects = contents.objects
        self.blob_data = contents.blob_data
        # Availability is derived from the pack contents (which objects
        # actually lack compartments), never from the manifest's own
        # withheld/erased declarations.
        unavailable = unavailable_object_ids(self.objects, self.blob_data)
        for object_id, obj in self.objects.items():
            verify_pack_object(
                obj, blob_data=self.blob_data.get(object_id), unavailable_ids=unavailable
            )

        if not reader.has("integrity/commits.ndjson"):
            raise RestoreError("pack has no integrity/commits.ndjson")
        self.commits = reader.read_ndjson("integrity/commits.ndjson")
        self.members = (
            reader.read_ndjson("integrity/members.ndjson")
            if reader.has("integrity/members.ndjson")
            else []
        )
        self.chain = verify_commit_chain(
            self.commits,
            self.members,
            self.objects,
            allow_missing_member_objects=allow_missing_member_objects,
        )

        # The manifest is an unsigned transport index: reconstruct the
        # actual inventory from the verified material and fail closed on
        # any disagreement, before any canonical state is touched.
        self.inventory = derive_pack_inventory(
            self.objects,
            self.blob_data,
            self.commits,
            self.members,
            known_ids=known_ids,
        )
        manifest = self.manifest
        self.completeness: CompletenessReport = classify_references(
            self.objects,
            external_ids={d["object_id"] for d in manifest["external_dependencies"]},
            withheld_ids=self.inventory.withheld,
            erased_ids=self.inventory.erased,
            known_ids=known_ids,
        )
        if not self.completeness.complete and not allow_partial:
            raise IncompletePackError(
                "pack has undeclared dangling references: "
                f"{self.completeness.dangling}"
            )
        compare_manifest(
            manifest,
            self.inventory,
            chain=self.chain,
            pack_names=reader.names(),
            completeness=self.completeness,
            allow_partial=allow_partial,
            operation=operation,
        )

    @property
    def partial(self) -> bool:
        return not self.completeness.complete


def verify_mindpack(
    pack_path: str | Path,
    *,
    package_root: str | Path,
    allow_partial: bool = False,
    known_ids: set[str] | None = None,
    allow_missing_member_objects: bool = False,
    operation: str = "import",
) -> VerifiedMindpack:
    """Open and fully verify a mindpack (directory or ZIP container)."""
    schemas = SchemaSet.load(package_root)
    with PackReader(pack_path) as reader:
        return VerifiedMindpack(
            reader,
            schemas,
            allow_partial=allow_partial,
            known_ids=known_ids,
            allow_missing_member_objects=allow_missing_member_objects,
            operation=operation,
        )


def insert_pack_objects(
    conn,
    archive_id: str,
    pack,
    *,
    updated_at: str,
    skip_existing: bool = False,
    unavailable_ids: set[str] | None = None,
) -> tuple[list[str], list[str]]:
    """Insert portable object rows (headers, compartments, blob bytes).

    With ``skip_existing`` an ID that already exists with the identical
    object hash is skipped; the same ID with a different hash is a hard
    collision and fails closed. A compartment absent from the pack yields
    no row at all (it never existed) unless the object is declared
    withheld/erased — in which case a ``withheld`` row records that
    operational state honestly.
    """
    if unavailable_ids is None:
        manifest = getattr(pack, "manifest", None) or {}
        unavailable_ids = set(manifest.get("withheld", [])) | set(
            manifest.get("erased", [])
        )
    inserted: list[str] = []
    skipped: list[str] = []
    for object_id in pack.objects:
        obj = pack.objects[object_id]
        header = obj.header
        existing = conn.execute(
            "SELECT object_hash FROM object_header WHERE id = %s", (object_id,)
        ).fetchone()
        if existing is not None:
            if existing[0] != header["object_hash"]:
                raise RestoreError(
                    f"hard ID collision at {object_id}: same ID, different content"
                )
            if skip_existing:
                skipped.append(object_id)
                continue
            raise RestoreError(f"object {object_id} already exists")
        conn.execute(
            """
            INSERT INTO object_header (
                id, archive_id, object_kind, spec, hash_profile,
                structural_commitment, semantic_commitment, object_hash,
                submission_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                object_id,
                archive_id,
                header["object_kind"],
                header["spec"],
                header["hash_profile"],
                header["structural_commitment"],
                header["semantic_commitment"],
                header["object_hash"],
                None,
            ),
        )
        for compartment in ("structural", "semantic"):
            envelope = getattr(obj, compartment)
            if envelope is None:
                if object_id in unavailable_ids:
                    conn.execute(
                        """
                        INSERT INTO compartment (
                            object_id, compartment, state, updated_at
                        ) VALUES (%s, %s, 'withheld', %s)
                        """,
                        (object_id, compartment, updated_at),
                    )
                continue
            conn.execute(
                """
                INSERT INTO compartment (
                    object_id, compartment, state, format, salt,
                    plaintext_json, updated_at
                ) VALUES (%s, %s, 'plaintext', %s, %s, %s, %s)
                """,
                (
                    object_id,
                    compartment,
                    envelope["format"],
                    decode_b64url(envelope["salt"]),
                    _jsonb(envelope["content"]),
                    updated_at,
                ),
            )
        if header["object_kind"] == "blob":
            data = pack.blob_data.get(object_id)
            semantic = (obj.semantic or {}).get("content") or {}
            salt = semantic.get("content_salt")
            structural = (obj.structural or {}).get("content") or {}
            if data is not None:
                conn.execute(
                    """
                    INSERT INTO blob_content (
                        blob_id, state, byte_length, plaintext_bytes,
                        content_salt, updated_at
                    ) VALUES (%s, 'plaintext', %s, %s, %s, %s)
                    """,
                    (
                        object_id,
                        len(data),
                        data,
                        decode_b64url(salt) if salt else None,
                        updated_at,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO blob_content (
                        blob_id, state, byte_length, content_salt, updated_at
                    ) VALUES (%s, 'withheld', %s, %s, %s)
                    """,
                    (
                        object_id,
                        int(structural.get("byte_length", "0")),
                        decode_b64url(salt) if salt else None,
                        updated_at,
                    ),
                )
        inserted.append(object_id)
    return inserted, skipped


def append_pack_commits(
    conn,
    archive_id: str,
    commits: list[dict],
    members: list[dict],
    *,
    start_sequence: int,
) -> None:
    """Append verified commit/member/admission rows at ``start_sequence``.

    The commit segment must already be verified (offline chain check);
    this writes the journal rows, recomputing member leaf hashes, then
    advances ``archive_head`` to the segment tip.
    """
    members_by_sequence: dict[int, list[dict]] = {}
    for member in members:
        members_by_sequence.setdefault(int(member["commit_sequence"]), []).append(member)

    head_hash: str | None = None
    head_record_id: str | None = None
    head_catalog_root: str | None = None
    head_signer: str | None = None
    head_committed_at: str | None = None
    for commit in commits:
        sequence = int(commit["sequence"])
        if sequence < start_sequence:
            continue
        record_id = commit["record_id"]
        payload = conn.execute(
            """
            SELECT plaintext_json -> 'structural_payload'
            FROM compartment
            WHERE object_id = %s AND compartment = 'structural'
            """,
            (record_id,),
        ).fetchone()
        if payload is None or payload[0] is None:
            raise RestoreError(f"commit Record {record_id} structural payload missing")
        payload = payload[0]
        member_count = len(members_by_sequence.get(sequence, []))
        conn.execute(
            """
            INSERT INTO commit_journal (
                archive_id, sequence, commit_record_id, parent_commit_hash,
                commit_hash, batch_merkle_root, member_count, signer_key_id,
                semantic_catalog_root, committed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                archive_id,
                sequence,
                record_id,
                commit["parent_commit_hash"],
                commit["commit_hash"],
                commit["merkle_root"],
                member_count,
                payload["signer_key_id"],
                payload["semantic_catalog_root"],
                payload["committed_at"],
            ),
        )
        for member in members_by_sequence.get(sequence, []):
            conn.execute(
                """
                INSERT INTO commit_member (
                    archive_id, commit_sequence, commit_position, object_kind,
                    object_id, object_hash, admitted_at, leaf_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    archive_id,
                    sequence,
                    int(member["commit_position"]),
                    member["object_kind"],
                    member["object_id"],
                    member["object_hash"],
                    member["admitted_at"],
                    "sha256:" + commit_leaf(member).hex(),
                ),
            )
            conn.execute(
                """
                INSERT INTO admission (
                    archive_id, commit_sequence, commit_position, object_kind,
                    object_id, object_hash, admitted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    archive_id,
                    sequence,
                    int(member["commit_position"]),
                    member["object_kind"],
                    member["object_id"],
                    member["object_hash"],
                    member["admitted_at"],
                ),
            )
        head_hash = commit["commit_hash"]
        head_record_id = record_id
        head_catalog_root = payload["semantic_catalog_root"]
        head_signer = payload["signer_key_id"]
        head_committed_at = payload["committed_at"]

    if head_hash is None:
        return
    conn.execute(
        """
        UPDATE archive_head
        SET sequence = %s, commit_record_id = %s, commit_hash = %s,
            semantic_catalog_root = %s, signer_key_id = %s, updated_at = %s
        WHERE archive_id = %s
        """,
        (
            int(commits[-1]["sequence"]),
            head_record_id,
            head_hash,
            head_catalog_root,
            head_signer,
            head_committed_at,
            archive_id,
        ),
    )


def _reread_operational_streams(
    pack_path: str | Path, pack: VerifiedMindpack
) -> tuple[dict, list[dict], list[dict], list[dict], list[dict]]:
    """Re-read the operational streams with digest re-verification.

    Full verification ran through a reader that is closed by the time the
    restore transaction consumes the operational streams. Every re-read is
    checked against the verified manifest's byte length and digest, so a
    pack swapped between the two opens fails closed instead of landing
    unverified epoch, origin, or producer state.
    """
    entries = {s.path: s for s in pack.streams}
    with PackReader(pack_path) as reader:

        def read_verified(name: str) -> bytes:
            entry = entries.get(name)
            if entry is None:
                raise RestoreError(
                    f"operational stream {name} is not in the verified manifest"
                )
            data = reader.read(name, max_bytes=entry.byte_length)
            if len(data) != entry.byte_length or digest_string(data) != entry.digest:
                raise RestoreError(
                    f"operational stream {name} changed since verification"
                )
            return data

        def read_json(name: str) -> dict:
            value = jcs_loads(read_verified(name))
            if not isinstance(value, dict):
                raise RestoreError(f"operational stream {name} is not a JSON object")
            return value

        archive_row = read_json("archive.json")
        lineage_heads = parse_ndjson(
            read_verified("lineage-heads.ndjson"), what="lineage-heads.ndjson"
        )
        origin_rows = parse_ndjson(
            read_verified("origin-index.ndjson"), what="origin-index.ndjson"
        )
        producer_heads = (
            parse_ndjson(
                read_verified("producer-heads.ndjson"), what="producer-heads.ndjson"
            )
            if reader.has("producer-heads.ndjson")
            else []
        )
        batches = [
            read_json(name)
            for name in sorted(reader.names())
            if name.startswith("producer-batches/") and name.endswith(".json")
        ]
    return archive_row, lineage_heads, origin_rows, producer_heads, batches


def restore_mindpack(
    settings: CcfPostgresSettings,
    *,
    package_root: str | Path,
    pack_path: str | Path,
    trusted_genesis_hash: str | None = None,
    trusted_head_hash: str | None = None,
    bootstrap_new_archive: bool = False,
    allow_partial: bool = False,
    clock=now_timestamp,
) -> dict:
    """Restore a mindpack into an empty CCF store; returns a report.

    Fails closed unless the store has no archive row: restore never
    re-admits into an existing history and never remaps portable IDs.

    The pack's chain is signed by a key carried inside the pack itself, so
    verification alone cannot distinguish a genuine archive from a forged,
    self-consistent one. Restore therefore refuses to run without an
    identity anchor: pass ``trusted_genesis_hash`` obtained out-of-band, or
    pass ``bootstrap_new_archive=True`` to pin a brand-new archive — the
    report's ``genesis_commit_hash`` must then be recorded out-of-band as
    the anchor for every future restore of this archive.
    """
    if trusted_genesis_hash is None and not bootstrap_new_archive:
        raise RestoreError(
            "restore requires a trusted genesis hash obtained out-of-band "
            "(or bootstrap_new_archive=True to pin a brand-new archive)"
        )
    pack = verify_mindpack(
        pack_path, package_root=package_root, allow_partial=allow_partial,
        operation="restore",
    )
    manifest = pack.manifest
    if manifest["mode"] not in ("restore", "replica"):
        raise RestoreError(
            f"pack mode {manifest['mode']!r} is not restorable",
            reason=MANIFEST_MODE_MISMATCH,
        )
    if trusted_genesis_hash is not None and (
        trusted_genesis_hash != pack.chain["genesis_commit_hash"]
    ):
        raise RestoreError("trusted genesis hash does not match pack")
    if trusted_head_hash is not None and trusted_head_hash != pack.chain["head_commit_hash"]:
        raise RestoreError("trusted head hash does not match pack")

    migrate_ccf_store(settings)
    with open_ccf_connection(settings) as conn:
        with conn.transaction():
            existing = conn.execute("SELECT COUNT(*) FROM archive").fetchone()[0]
            if existing:
                raise RestoreError(
                    "CCF store is not empty; restore requires an empty store "
                    "(use import_mindpack for merge/fork/delta)"
                )
            for required in ("archive.json", "lineage-heads.ndjson", "origin-index.ndjson"):
                if required not in {s.path for s in map(StreamEntry.from_dict, manifest["streams"])}:
                    raise RestoreError(
                        f"pack lacks the {required} stream; cannot restore "
                        "operational state (fail closed)"
                    )
            # Streams were digest-verified already; re-read them with
            # digest re-verification against the verified manifest.
            (
                archive_row,
                lineage_heads,
                origin_rows,
                producer_heads,
                batches,
            ) = _reread_operational_streams(pack_path, pack)

            received_at = clock()
            archive_id = manifest["archive_id"]
            conn.execute(
                """
                INSERT INTO archive (
                    archive_id, epoch_id, genesis_commit_hash, hash_profile,
                    signature_profile, semantic_catalog_root, active_profiles,
                    signer_key_id, erasure_domain_id, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    archive_id,
                    archive_row["epoch_id"],
                    archive_row["genesis_commit_hash"],
                    archive_row["hash_profile"],
                    archive_row["signature_profile"],
                    archive_row["semantic_catalog_root"],
                    _jsonb(archive_row["active_profiles"]),
                    archive_row["signer_key_id"],
                    archive_row["erasure_domain_id"],
                    archive_row["created_at"],
                ),
            )
            inserted, _ = insert_pack_objects(
                conn, archive_id, pack, updated_at=received_at,
                unavailable_ids=pack.inventory.unavailable_ids,
            )
            genesis = pack.commits[0]
            genesis_payload = pack.objects[genesis["record_id"]].structural[
                "content"
            ]["structural_payload"]
            conn.execute(
                """
                INSERT INTO archive_head (
                    archive_id, sequence, commit_record_id, commit_hash,
                    semantic_catalog_root, signer_key_id, updated_at
                ) VALUES (%s, 0, %s, %s, %s, %s, %s)
                """,
                (
                    archive_id,
                    genesis["record_id"],
                    genesis["commit_hash"],
                    genesis_payload["semantic_catalog_root"],
                    genesis_payload["signer_key_id"],
                    genesis_payload["committed_at"],
                ),
            )
            append_pack_commits(conn, archive_id, pack.commits, pack.members,
                                start_sequence=0)

            for row in lineage_heads:
                conn.execute(
                    """
                    INSERT INTO lineage_head (
                        archive_id, lineage_id, head_record_id, head_record_hash,
                        head_commit_sequence, state, valid_from, expires_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        archive_id,
                        row["lineage_id"],
                        row["head_record_id"],
                        row["head_record_hash"],
                        int(row["head_commit_sequence"]),
                        row["state"],
                        row["valid_from"],
                        row["expires_at"],
                    ),
                )
            for row in origin_rows:
                conn.execute(
                    """
                    INSERT INTO origin_index (
                        archive_id, source_id, native_id, revision,
                        submission_hash, object_kind, object_id, lifecycle
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        archive_id,
                        row["source_id"],
                        row["native_id"],
                        row["revision"],
                        row["submission_hash"],
                        row["object_kind"],
                        row["object_id"],
                        row["lifecycle"],
                    ),
                )
            skipped_batches = _restore_producer_state(
                conn, pack, batches, producer_heads, received_at
            )

            report = verify_chain(
                conn,
                archive_id=archive_id,
                expected_genesis_hash=manifest["genesis_commit_hash"],
            )
            if report["head_commit_hash"] != manifest["head_commit_hash"]:
                raise RestoreError("restored head does not match manifest head")

    return {
        "status": "restored",
        "archive_id": archive_id,
        "epoch_id": manifest["epoch_id"],
        "genesis_commit_hash": manifest["genesis_commit_hash"],
        "head_commit_hash": manifest["head_commit_hash"],
        "head_sequence": manifest["head_sequence"],
        "objects_restored": len(inserted),
        "partial": pack.partial,
        "completeness": pack.completeness.to_dict(),
        "skipped_batches": skipped_batches,
        "stream_notes": pack.stream_notes,
        "verification": report,
    }


def _restore_producer_state(
    conn, pack: VerifiedMindpack, batches: list[dict], producer_heads: list[dict],
    received_at: str,
) -> list[str]:
    """Restore producer batch rows (chain continuity) and producer heads."""
    commit_of: dict[str, int] = {}
    for member in pack.members:
        commit_of[member["object_id"]] = int(member["commit_sequence"])

    skipped: list[str] = []
    for batch in batches:
        object_ids = [
            sub["id"]
            for kind in ("records", "links", "blobs")
            for sub in batch.get(kind, [])
        ]
        sequences = [commit_of[oid] for oid in object_ids if oid in commit_of]
        if not sequences:
            skipped.append(batch["batch_id"])
            continue
        conn.execute(
            """
            INSERT INTO producer_batch (
                batch_id, producer_id, producer_sequence, previous_batch_hash,
                credential_id, created_at, semantic_catalog_root, batch_hash,
                signature, batch_json, status, spooled_at, committed_sequence
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'accepted', %s, %s)
            """,
            (
                batch["batch_id"],
                batch["producer_id"],
                int(batch["producer_sequence"]),
                batch["previous_batch_hash"],
                batch["credential_id"],
                batch["created_at"],
                batch["semantic_catalog_root"],
                batch["batch_hash"],
                decode_b64url(batch["signature"]),
                _jsonb(batch),
                received_at,
                min(sequences),
            ),
        )
        # Recover portable submission hashes for idempotent re-admission.
        for kind in ("records", "links", "blobs"):
            for sub in batch.get(kind, []):
                conn.execute(
                    "UPDATE object_header SET submission_hash = %s WHERE id = %s",
                    (submission_hash(sub), sub["id"]),
                )
    for head in producer_heads:
        conn.execute(
            """
            INSERT INTO producer_head (
                producer_id, producer_sequence, batch_hash, credential_id, updated_at
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                head["producer_id"],
                int(head["producer_sequence"]),
                head["batch_hash"],
                head["credential_id"],
                head["updated_at"],
            ),
        )
    return skipped
