"""Signed archive journal: commit construction and chain verification.

Implements spec sections 4.8-4.9 and 7.2-7.4. An ``integrity.commit``
Record is built in the exact spec order: unsigned structural content,
``ccf:commit-sig:v2`` signing digest, Ed25519 signature, compartment
envelope, portable header; ``commit_hash`` is the commit Record's object
hash. The commit Record is excluded from its own member Merkle root and the
chain links through ``parent_commit_hash``.

:func:`verify_chain` replays the journal from the genesis commit to the
archive head and re-derives everything an operator could tamper with:
member leaf hashes and Merkle roots, compartment commitments and object
hashes, parent linkage, sequence contiguity, and commit signatures.
"""

from __future__ import annotations

from dataclasses import dataclass

from ccf import CCF_HASH_PROFILE, CCF_SIGNATURE_PROFILE, CCF_SPEC
from ccf.catalog import SemanticCatalog
from ccf.hashing import (
    commit_leaf,
    commit_signing_digest,
    decode_b64url,
    encode_b64url,
    merkle_root,
    object_hash,
    sign_digest,
    verify_digest,
)
from ccf.keys import public_key_from_b64url, public_key_text
from ccf.objects import new_salt, validate_decimal_string, validate_timestamp
from ccf.registry import PinnedRegistries


class JournalError(RuntimeError):
    """Raised when journal construction or verification fails."""


def commit_type_digests(
    catalog: SemanticCatalog, registries: PinnedRegistries
) -> tuple[str, str]:
    """Pinned schema/registry-entry digests for ``integrity.commit``."""
    entry = registries.type_entry("integrity.commit")
    return (
        catalog.schema_digest(entry["semantic_schema_id"]),
        registries.entry_digest(entry),
    )


@dataclass(frozen=True)
class CommitRecord:
    """A fully built and signed ``integrity.commit`` Record."""

    record_id: str
    sequence: int
    header: dict
    structural_envelope: dict
    members: list[dict]
    merkle_root: str
    commit_hash: str


def build_commit_record(
    *,
    commit_record_id: str,
    archive_id: str,
    epoch_id: str,
    sequence: int,
    parent_commit_hash: str | None,
    members: list[dict],
    signer,
    signer_key_id: str,
    semantic_catalog_root: str,
    active_profiles: list[str],
    committed_at: str,
    catalog: SemanticCatalog,
    registries: PinnedRegistries,
    salt_fn=new_salt,
) -> CommitRecord:
    """Build and sign an ``integrity.commit`` Record (spec section 4.9).

    ``members`` are the commit's member dicts (numeric ``commit_position``
    contiguous from zero, canonical string ``commit_sequence``); the commit
    Record itself must not appear among them.
    """
    validate_decimal_string(str(sequence))
    validate_timestamp(committed_at)
    for member in members:
        if member["object_id"] == commit_record_id:
            raise JournalError("commit Record must be excluded from its own members")

    root = merkle_root(members)
    schema_digest, entry_digest = commit_type_digests(catalog, registries)
    structural_payload = {
        "archive_id": archive_id,
        "epoch_id": epoch_id,
        "sequence": str(sequence),
        "parent_commit_hash": parent_commit_hash,
        "batch_merkle_root": root,
        "member_count": str(len(members)),
        "hash_profile": CCF_HASH_PROFILE,
        "signature_profile": CCF_SIGNATURE_PROFILE,
        "signer_key_id": signer_key_id,
        "signer_public_key": public_key_text(signer),
        "semantic_catalog_root": semantic_catalog_root,
        "active_profiles": list(active_profiles),
        "committed_at": committed_at,
    }
    structural_content = {
        "type": "integrity.commit",
        "type_version": 1,
        "type_visibility": "clear",
        "schema_digest": schema_digest,
        "registry_entry_digest": entry_digest,
        "retention_profile": "epoch_lifetime_required",
        "structural_payload": structural_payload,
        "extensions": {},
    }
    signing_header = {
        "spec": CCF_SPEC,
        "object_kind": "record",
        "id": commit_record_id,
        "hash_profile": CCF_HASH_PROFILE,
        "semantic_commitment": None,
    }
    digest = commit_signing_digest(signing_header, structural_content)
    structural_payload["signature"] = encode_b64url(sign_digest(signer, digest))

    envelope = {
        "format": "ccf.record-structural/0.1.2-rc1",
        "salt": salt_fn(),
        "content": structural_content,
    }
    from ccf.hashing import compartment_commitment

    structural_commitment = compartment_commitment("record", "structural", envelope)
    header_fields = {
        "spec": CCF_SPEC,
        "object_kind": "record",
        "id": commit_record_id,
        "hash_profile": CCF_HASH_PROFILE,
        "structural_commitment": structural_commitment,
        "semantic_commitment": None,
    }
    header = dict(header_fields, object_hash=object_hash(header_fields))
    return CommitRecord(
        record_id=commit_record_id,
        sequence=sequence,
        header=header,
        structural_envelope=envelope,
        members=list(members),
        merkle_root=root,
        commit_hash=header["object_hash"],
    )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise JournalError(message)


def _fetch_envelope(conn, object_id: str, compartment: str) -> tuple[str | None, dict | None]:
    """(state, envelope) for a compartment; envelope only when plaintext."""
    row = conn.execute(
        """
        SELECT state, format, salt, plaintext_json FROM compartment
        WHERE object_id = %s AND compartment = %s
        """,
        (object_id, compartment),
    ).fetchone()
    if row is None:
        return None, None
    if row[0] != "plaintext":
        return row[0], None
    return row[0], {
        "format": row[1],
        "salt": encode_b64url(bytes(row[2])),
        "content": row[3],
    }


def _preload_members(conn, archive_id: str) -> dict[int, list[tuple]]:
    """All commit member rows for the archive, grouped by commit sequence.

    One set-based scan replaces the per-commit member query plus the
    per-member leaf-hash probe: on replica schemas built without indexes
    (``CREATE TABLE AS SELECT`` copies) those per-row probes degrade to
    full table scans, i.e. O(n^2) in archive size.
    """
    grouped: dict[int, list[tuple]] = {}
    for row in conn.execute(
        """
        SELECT commit_sequence, commit_position, object_kind, object_id,
               object_hash, admitted_at, leaf_hash
        FROM commit_member
        WHERE archive_id = %s
        ORDER BY commit_sequence ASC, commit_position ASC
        """,
        (archive_id,),
    ):
        sequence, position, kind, object_id, member_hash, admitted_at, leaf = row
        grouped.setdefault(int(sequence), []).append(
            (int(position), kind, object_id, member_hash, admitted_at, leaf)
        )
    return grouped


def _preload_objects(conn, object_ids: list[str]) -> tuple[dict, dict]:
    """Bulk-load header rows and compartment envelopes for member objects.

    Same lookups ``_verify_object`` performs, batched into two queries
    instead of three per object. Duplicate keys keep the first row,
    matching the previous ``fetchone`` behaviour.
    """
    headers: dict[str, tuple] = {}
    envelopes: dict[tuple[str, str], tuple] = {}
    if not object_ids:
        return headers, envelopes
    ids = sorted(set(object_ids))
    for row in conn.execute(
        """
        SELECT id, object_kind, spec, hash_profile, structural_commitment,
               semantic_commitment, object_hash
        FROM object_header WHERE id = ANY(%s)
        """,
        (ids,),
    ):
        headers.setdefault(row[0], row[1:])
    for row in conn.execute(
        """
        SELECT object_id, compartment, state, format, salt, plaintext_json
        FROM compartment WHERE object_id = ANY(%s)
        """,
        (ids,),
    ):
        object_id, compartment, state, format_, salt, plaintext = row
        envelope = None
        if state == "plaintext":
            envelope = {
                "format": format_,
                "salt": encode_b64url(bytes(salt)),
                "content": plaintext,
            }
        envelopes.setdefault((object_id, compartment), (state, envelope))
    return headers, envelopes


def _verify_object(headers, envelopes, object_id: str, expected_hash: str, context: str) -> None:
    """Recompute an admitted object's commitments and object hash.

    An erased or withheld compartment is no longer inspectable: its
    commitment in the header is historical (spec 3.10) and cannot be
    recomputed, so verification skips the recomputation for that
    compartment. The object hash — which binds the commitment values — is
    still always recomputed, so a forged post-erasure header is detected.
    """
    from ccf.hashing import compartment_commitment, parse_digest

    row = headers.get(object_id)
    _require(row is not None, f"{context}: object {object_id} missing")
    kind, spec, hash_profile, structural_c, semantic_c, stored_hash = row
    _require(stored_hash == expected_hash, f"{context}: object hash mismatch for {object_id}")
    structural_state, structural = envelopes.get((object_id, "structural"), (None, None))
    _require(
        structural_state is not None,
        f"{context}: structural compartment unavailable for {object_id}",
    )
    if structural_state == "plaintext":
        recomputed_structural = compartment_commitment(kind, "structural", structural)
        _require(
            recomputed_structural == structural_c,
            f"{context}: structural commitment mismatch for {object_id}",
        )
    else:
        _require(
            structural_state in ("withheld", "erased"),
            f"{context}: structural compartment state {structural_state!r} "
            f"is not verifiable for {object_id}",
        )
    semantic_state, semantic = envelopes.get((object_id, "semantic"), (None, None))
    if semantic_state == "plaintext":
        recomputed_semantic = compartment_commitment(kind, "semantic", semantic)
    elif semantic_state in ("withheld", "erased"):
        recomputed_semantic = semantic_c  # historical commitment, not inspectable
    else:
        recomputed_semantic = None
    _require(
        recomputed_semantic == semantic_c,
        f"{context}: semantic commitment mismatch for {object_id}",
    )
    header_fields = {
        "spec": spec,
        "object_kind": kind,
        "id": object_id,
        "hash_profile": hash_profile,
        "structural_commitment": structural_c,
        "semantic_commitment": semantic_c,
    }
    _require(
        object_hash(header_fields) == stored_hash,
        f"{context}: recomputed object hash mismatch for {object_id}",
    )
    parse_digest(stored_hash)


def verify_chain(
    conn,
    *,
    archive_id: str,
    expected_genesis_hash: str | None = None,
    verify_objects: bool = True,
) -> dict:
    """Verify the signed commit chain from genesis through the head.

    Provides prefix integrity (spec section 7.4): detects corruption, object
    mutation, omission inside the presented history, and writes by actors
    without the active signing key. Raises :class:`JournalError` on the
    first discrepancy; returns a verification report otherwise.
    """
    archive = conn.execute(
        "SELECT epoch_id, genesis_commit_hash FROM archive WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()
    _require(archive is not None, f"unknown archive: {archive_id}")
    epoch_id, genesis_commit_hash = archive

    commits = conn.execute(
        """
        SELECT sequence, commit_record_id, parent_commit_hash, commit_hash,
               batch_merkle_root, member_count, signer_key_id,
               semantic_catalog_root, committed_at
        FROM commit_journal WHERE archive_id = %s ORDER BY sequence ASC
        """,
        (archive_id,),
    ).fetchall()
    _require(bool(commits), f"archive {archive_id} has no commits")
    _require(
        int(commits[0][0]) == 0, f"archive {archive_id} journal does not start at genesis"
    )

    signer_public_key: str | None = None
    previous_hash: str | None = None
    members_verified = 0

    # Set-based preloads: one scan of commit_member plus (when object
    # verification is on) one scan each of object_header and compartment.
    # The per-commit/per-member queries they replace are full table scans
    # on index-less replica schemas, which made verification O(n^2).
    members_by_sequence = _preload_members(conn, archive_id)
    headers: dict = {}
    envelopes: dict = {}
    if verify_objects:
        headers, envelopes = _preload_objects(
            conn,
            [
                stored[2]
                for stored_rows in members_by_sequence.values()
                for stored in stored_rows
            ],
        )

    for index, row in enumerate(commits):
        (
            sequence,
            record_id,
            parent_hash,
            commit_hash,
            stored_root,
            member_count,
            signer_key_id,
            catalog_root,
            committed_at,
        ) = row
        context = f"commit sequence {index}"
        _require(int(sequence) == index, f"{context}: non-contiguous sequence {sequence}")
        _require(parent_hash == previous_hash, f"{context}: parent hash chain broken")

        header_row = conn.execute(
            "SELECT object_hash FROM object_header WHERE id = %s AND object_kind = 'record'",
            (record_id,),
        ).fetchone()
        _require(header_row is not None, f"{context}: commit Record {record_id} missing")
        _require(
            header_row[0] == commit_hash,
            f"{context}: commit_hash does not match commit Record object hash",
        )

        envelope_state, envelope = _fetch_envelope(conn, record_id, "structural")
        _require(
            envelope_state == "plaintext",
            f"{context}: commit structural compartment unavailable",
        )
        content = envelope["content"]
        payload = content.get("structural_payload", {})
        _require(content.get("type") == "integrity.commit", f"{context}: not an integrity.commit")
        _require(payload.get("archive_id") == archive_id, f"{context}: archive_id mismatch")
        _require(payload.get("epoch_id") == epoch_id, f"{context}: epoch_id mismatch")
        _require(payload.get("sequence") == str(index), f"{context}: payload sequence mismatch")
        _require(
            payload.get("parent_commit_hash") == previous_hash,
            f"{context}: payload parent hash mismatch",
        )
        _require(
            payload.get("hash_profile") == CCF_HASH_PROFILE
            and payload.get("signature_profile") == CCF_SIGNATURE_PROFILE,
            f"{context}: unexpected hash/signature profile",
        )
        _require(payload.get("signer_key_id") == signer_key_id, f"{context}: signer key mismatch")
        _require(
            payload.get("semantic_catalog_root") == catalog_root,
            f"{context}: catalog root mismatch",
        )
        _require(payload.get("committed_at") == committed_at, f"{context}: committed_at mismatch")
        if index == 0:
            signer_public_key = payload.get("signer_public_key")
            _require(bool(signer_public_key), "genesis: signer public key missing")
        _require(
            payload.get("signer_public_key") == signer_public_key,
            f"{context}: unannounced signer rotation",
        )

        # Members: rebuild canonical member dicts, recheck leaves and root.
        stored_rows = members_by_sequence.get(index, [])
        members = [
            {
                "commit_sequence": str(index),
                "commit_position": position,
                "admitted_at": admitted_at,
                "object_kind": kind,
                "object_id": object_id,
                "object_hash": member_hash,
            }
            for position, kind, object_id, member_hash, admitted_at, _leaf in stored_rows
        ]
        _require(
            len(members) == int(member_count) == int(payload.get("member_count", -1)),
            f"{context}: member count mismatch",
        )
        for member, stored_row in zip(members, stored_rows):
            stored_leaf = stored_row[5]
            recomputed_leaf = "sha256:" + commit_leaf(member).hex()
            _require(
                recomputed_leaf == stored_leaf,
                f"{context}: member leaf mismatch at position {member['commit_position']}",
            )
        recomputed_root = merkle_root(members)
        _require(
            recomputed_root == stored_root == payload.get("batch_merkle_root"),
            f"{context}: batch Merkle root mismatch",
        )
        _require(
            all(member["object_id"] != record_id for member in members),
            f"{context}: commit Record appears in its own member root",
        )

        # Commit signature over the unsigned structural content.
        unsigned_payload = {k: v for k, v in payload.items() if k != "signature"}
        unsigned_content = dict(content, structural_payload=unsigned_payload)
        signing_header = {
            "spec": CCF_SPEC,
            "object_kind": "record",
            "id": record_id,
            "hash_profile": CCF_HASH_PROFILE,
            "semantic_commitment": None,
        }
        digest = commit_signing_digest(signing_header, unsigned_content)
        try:
            verify_digest(
                public_key_from_b64url(signer_public_key),
                decode_b64url(payload.get("signature", "")),
                digest,
            )
        except Exception as exc:
            raise JournalError(f"{context}: commit signature invalid: {exc}") from exc

        if verify_objects:
            for member in members:
                _verify_object(
                    headers, envelopes, member["object_id"], member["object_hash"], context
                )

        members_verified += len(members)
        previous_hash = commit_hash

    _require(
        previous_hash == genesis_commit_hash if len(commits) == 1 else True,
        "genesis hash mismatch with archive row",
    )
    _require(
        commits[0][3] == genesis_commit_hash,
        "archive row genesis hash does not match journal",
    )
    if expected_genesis_hash is not None:
        _require(
            commits[0][3] == expected_genesis_hash,
            f"genesis hash {commits[0][3]} != trusted {expected_genesis_hash}",
        )

    head = conn.execute(
        "SELECT sequence, commit_hash FROM archive_head WHERE archive_id = %s",
        (archive_id,),
    ).fetchone()
    _require(head is not None, f"archive {archive_id} has no head row")
    _require(
        int(head[0]) == len(commits) - 1 and head[1] == previous_hash,
        "archive head does not match verified chain tip",
    )

    # The admission table is derived state that serves admission
    # coordinates to readers; it must mirror the verified journal exactly.
    # A tampered or partial row is detected here even though the journal
    # itself still verifies (spec 7.4 covers what readers are served).
    def _rows(table: str) -> list[tuple]:
        return [
            (int(sequence), int(position), kind, object_id, object_hash, admitted_at)
            for sequence, position, kind, object_id, object_hash, admitted_at in conn.execute(
                f"""
                SELECT commit_sequence, commit_position, object_kind,
                       object_id, object_hash, admitted_at
                FROM {table} WHERE archive_id = %s
                ORDER BY commit_sequence, commit_position
                """,
                (archive_id,),
            ).fetchall()
        ]

    _require(
        _rows("admission") == _rows("commit_member"),
        "admission rows do not match the verified commit members",
    )

    return {
        "archive_id": archive_id,
        "epoch_id": epoch_id,
        "genesis_commit_hash": commits[0][3],
        "head_sequence": str(len(commits) - 1),
        "head_commit_hash": previous_hash,
        "commits_verified": len(commits),
        "members_verified": members_verified,
        "signer_public_key": signer_public_key,
    }
