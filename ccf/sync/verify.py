"""Offline verification of pack contents (spec 4.8-4.9, 7.4, 11.5).

Verifies a pack without a database: stream digests (see
:mod:`ccf.sync.packio`), portable object headers and compartment
commitments, salted Blob content commitments, and the commit chain —
sequence contiguity, parent linkage, member Merkle roots, and Ed25519
commit signatures with the signer public key pinned at the first commit.

This is what lets the vendored ``examples/mindpack`` directory be checked
standalone, and what restore/merge/delta-apply run before trusting a pack.
"""

from __future__ import annotations

from ccf import CCF_HASH_PROFILE, CCF_SIGNATURE_PROFILE
from ccf.hashing import (
    blob_content_commitment,
    commit_leaf,
    commit_signing_digest,
    decode_b64url,
    merkle_root,
    verify_digest,
)
from ccf.keys import public_key_from_b64url
from ccf.objects import PortableHeader
from ccf.sync.packio import PackError, PackObject


class PackVerificationError(PackError):
    """Raised when pack objects or the commit chain fail verification."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PackVerificationError(message)


def verify_pack_object(
    obj: PackObject,
    *,
    blob_data: bytes | None = None,
    unavailable_ids: set[str] | None = None,
) -> None:
    """Recompute one object's commitments and object hash.

    Unavailable objects (``unavailable_ids``, derived from the pack
    contents — never from manifest declarations) may lack envelopes;
    their header commitments are authenticated by commit membership
    instead, but every envelope they do carry is still recomputed
    against the header. Content is never fabricated for them.
    """
    unavailable_ids = unavailable_ids or set()
    object_id = obj.object_id
    try:
        header = PortableHeader.from_dict(obj.header)
    except Exception as exc:
        raise PackVerificationError(f"header invalid for {object_id}: {exc}") from exc
    _require(
        header.object_hash == obj.header["object_hash"],
        f"object hash field mismatch for {object_id}",
    )

    envelopes = {"structural": obj.structural, "semantic": obj.semantic}
    if object_id in unavailable_ids:
        # A partially available object still proves every compartment it
        # carries: each present envelope must match its header commitment
        # (the header itself is authenticated by commit membership).
        # Content is never fabricated for the absent ones.
        if envelopes["structural"] is not None:
            _verify_envelope_commitment(header, envelopes["structural"], "structural")
        if envelopes["semantic"] is not None:
            _require(
                header.semantic_commitment is not None,
                f"semantic envelope without header commitment for {object_id}",
            )
            _verify_envelope_commitment(header, envelopes["semantic"], "semantic")
        if (
            obj.object_kind == "blob"
            and blob_data is not None
            and envelopes["structural"] is not None
            and envelopes["semantic"] is not None
        ):
            _verify_blob_bytes(obj, blob_data, envelopes)
        return
    _require(
        envelopes["structural"] is not None,
        f"structural compartment missing for available object {object_id}",
    )
    try:
        header.verify(
            _envelope(envelopes["structural"]),
            _envelope(envelopes["semantic"]) if envelopes["semantic"] else None,
        )
    except Exception as exc:
        raise PackVerificationError(f"object verification failed: {exc}") from exc

    if obj.object_kind == "blob" and blob_data is not None:
        _verify_blob_bytes(obj, blob_data, envelopes)


def _verify_envelope_commitment(
    header: PortableHeader, envelope: dict, compartment: str
) -> None:
    """Recompute one present envelope's commitment against the header."""
    try:
        commitment = _envelope(envelope).commitment(header.object_kind, compartment)
    except Exception as exc:
        raise PackVerificationError(
            f"{compartment} envelope invalid for {header.id}: {exc}"
        ) from exc
    expected = (
        header.structural_commitment
        if compartment == "structural"
        else header.semantic_commitment
    )
    _require(
        commitment == expected,
        f"{compartment} commitment mismatch for {header.id}",
    )


def _verify_blob_bytes(obj: PackObject, blob_data: bytes, envelopes: dict) -> None:
    """Verify included Blob bytes against the salted content commitment."""
    object_id = obj.object_id
    semantic = envelopes["semantic"] or {}
    content = semantic.get("content") or {}
    salt = content.get("content_salt")
    structural = envelopes["structural"]["content"]
    _require(bool(salt), f"blob {object_id} semantic content_salt missing")
    _require(
        str(len(blob_data)) == structural.get("byte_length"),
        f"blob {object_id} byte length mismatch",
    )
    _require(
        blob_content_commitment(salt, blob_data)
        == structural.get("content_commitment"),
        f"blob {object_id} content commitment mismatch",
    )


def _envelope(raw: dict):
    from ccf.objects import CompartmentEnvelope

    return CompartmentEnvelope.from_dict(raw)


def verify_commit_chain(
    commits: list[dict],
    members: list[dict],
    objects: dict[str, PackObject],
    *,
    expected_first_sequence: int = 0,
    expected_parent_hash: str | None = None,
    known_object_hashes: dict[str, str] | None = None,
    allow_missing_member_objects: bool = False,
) -> dict:
    """Verify a contiguous commit segment from a pack.

    ``commits`` are ``integrity/commits.ndjson`` rows (sequence,
    record_id, parent_commit_hash, commit_hash, merkle_root); ``members``
    are ``integrity/members.ndjson`` rows. For delta segments,
    ``expected_first_sequence``/``expected_parent_hash`` anchor the segment
    to the importer's verified local head and ``known_object_hashes``
    satisfies members whose objects predate the pack.

    Member Merkle roots authenticate (object_id, object_hash) pairs, so a
    member whose object is not in the pack does not break chain integrity:
    with ``allow_missing_member_objects`` such members are tolerated (the
    reference-completeness pass reports them) — used by foreign merge of
    partial packs. Restore and delta apply keep the strict default.

    Malformed member/commit fields (missing keys, non-numeric positions)
    are re-raised as :class:`PackVerificationError`, never raw
    ``KeyError``/``ValueError``.
    """
    try:
        return _verify_commit_chain(
            commits,
            members,
            objects,
            expected_first_sequence=expected_first_sequence,
            expected_parent_hash=expected_parent_hash,
            known_object_hashes=known_object_hashes,
            allow_missing_member_objects=allow_missing_member_objects,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PackVerificationError(
            f"malformed commit or member fields: {exc}"
        ) from exc


def _verify_commit_chain(
    commits: list[dict],
    members: list[dict],
    objects: dict[str, PackObject],
    *,
    expected_first_sequence: int = 0,
    expected_parent_hash: str | None = None,
    known_object_hashes: dict[str, str] | None = None,
    allow_missing_member_objects: bool = False,
) -> dict:
    """Implementation of :func:`verify_commit_chain`."""
    _require(bool(commits), "pack contains no commits")
    known_object_hashes = known_object_hashes or {}
    members_by_sequence: dict[int, list[dict]] = {}
    for member in members:
        members_by_sequence.setdefault(int(member["commit_sequence"]), []).append(member)

    signer_public_key: str | None = None
    previous_hash = expected_parent_hash
    verified_members = 0

    for offset, commit in enumerate(commits):
        sequence = int(commit["sequence"])
        context = f"pack commit sequence {sequence}"
        _require(
            sequence == expected_first_sequence + offset,
            f"{context}: non-contiguous segment",
        )
        _require(
            commit["parent_commit_hash"] == previous_hash,
            f"{context}: parent hash chain broken",
        )
        record_id = commit["record_id"]
        obj = objects.get(record_id)
        _require(obj is not None, f"{context}: commit Record {record_id} not in pack")
        _require(
            obj.header["object_hash"] == commit["commit_hash"],
            f"{context}: commit hash does not match commit Record object hash",
        )
        _require(obj.structural is not None, f"{context}: commit compartment missing")
        content = obj.structural["content"]
        payload = content.get("structural_payload", {})
        _require(content.get("type") == "integrity.commit", f"{context}: bad type")
        _require(payload.get("sequence") == str(sequence), f"{context}: sequence mismatch")
        _require(
            payload.get("parent_commit_hash") == previous_hash,
            f"{context}: payload parent mismatch",
        )
        _require(
            payload.get("hash_profile") == CCF_HASH_PROFILE
            and payload.get("signature_profile") == CCF_SIGNATURE_PROFILE,
            f"{context}: unexpected hash/signature profile",
        )
        if offset == 0 and expected_first_sequence == 0:
            signer_public_key = payload.get("signer_public_key")
            _require(bool(signer_public_key), "genesis: signer public key missing")
        elif signer_public_key is None:
            signer_public_key = payload.get("signer_public_key")
        _require(
            payload.get("signer_public_key") == signer_public_key,
            f"{context}: unannounced signer rotation",
        )

        commit_members = sorted(
            members_by_sequence.get(sequence, []),
            key=lambda m: int(m["commit_position"]),
        )
        _require(
            commit["merkle_root"] == merkle_root(commit_members),
            f"{context}: member Merkle root mismatch",
        )
        _require(
            payload.get("batch_merkle_root") == commit["merkle_root"],
            f"{context}: payload Merkle root mismatch",
        )
        _require(
            int(payload.get("member_count", -1)) == len(commit_members),
            f"{context}: member count mismatch",
        )
        for member in commit_members:
            _require(
                member["object_id"] != record_id,
                f"{context}: commit Record in its own member root",
            )
            known = objects.get(member["object_id"])
            if known is None:
                if allow_missing_member_objects and member["object_id"] not in (
                    known_object_hashes
                ):
                    continue  # partial pack: completeness pass reports this
                _require(
                    known_object_hashes.get(member["object_id"]) == member["object_hash"],
                    f"{context}: member {member['object_id']} neither in pack nor known",
                )
            else:
                _require(
                    known.header["object_hash"] == member["object_hash"],
                    f"{context}: member object hash mismatch for {member['object_id']}",
                )
            # Leaf integrity is implied by the recomputed root above; this
            # assertion keeps tampered member fields from hiding behind a
            # root computed over canonicalized members.
            _require(
                isinstance(commit_leaf(member), bytes) and len(commit_leaf(member)) == 32,
                f"{context}: member leaf not computable",
            )

        unsigned_payload = {k: v for k, v in payload.items() if k != "signature"}
        unsigned_content = dict(content, structural_payload=unsigned_payload)
        signing_header = {
            "spec": "ccf/0.1.2",
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
            raise PackVerificationError(
                f"{context}: commit signature invalid: {exc}"
            ) from exc

        verified_members += len(commit_members)
        previous_hash = commit["commit_hash"]

    return {
        "genesis_commit_hash": commits[0]["commit_hash"],
        "head_commit_hash": previous_hash,
        "head_sequence": str(int(commits[-1]["sequence"])),
        "commits_verified": len(commits),
        "members_verified": verified_members,
        "signer_public_key": signer_public_key,
    }
