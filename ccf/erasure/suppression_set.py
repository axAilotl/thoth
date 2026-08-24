"""Canonical suppression sets (spec 12.7, 0.1.2).

Suppression commitments are canonical, journal-covered erasure lineage: a
``lineage.suppression_set`` Record commits to the pinned profile
(``ccf-hmac-sha256-suppression-v1``), the governed token Blob, the entry
count, the entries Merkle root, the key/profile identifier, and a scope
commitment; the erasure receipt commits back to the set. Both objects are
admitted through the normal canonical path with
``authorized_erasure_worker`` authority inside the saga's block-stage
transaction, so they are covered by the signed admission journal.

The ``suppression_entry`` table is a rebuildable PROJECTION of that
canonical state (:func:`rebuild_projection`). Deleting or tampering with
lookup rows is detected — admission audits the projection against the
canonical sets before trusting it (:func:`audit_projection`) — and never
removes canonical suppression authority.

Blob layout (pinned by the profile and the suppression-canonical vector):
JCS ``{"entries": [...], "profile": "ccf-hmac-sha256-suppression-v1"}``
with entries in ascending Unicode code-point order; the Merkle tree uses
the registry-pinned leaf/node/empty domains and splits at the largest
power of two smaller than the node count.

The ``scope_commitment`` derivation is registry-pinned as
``ccf:suppression-scope:v1`` over the JCS array of sorted erased object IDs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from ccf import schema_urn
from ccf.admission import ResolvedObject, _make_envelope, _make_header
from ccf.erasure.errors import ErasureError, SuppressionProjectionError
from ccf.erasure.suppression import SUPPRESSION_PROFILE
from ccf.hashing import (
    blob_content_commitment,
    canonical_digest,
    domain_hash_bytes,
)
from ccf.jcs import canonical_bytes

SUPPRESSION_SET_TYPE = "lineage.suppression_set"
SUPPRESSION_BLOB_TYPE = "blob.suppression_set"
SUPPRESSION_BLOB_MEDIA_TYPE = "application/vnd.ccf.suppression-set+json"

SCHEMA_SET_PAYLOAD = schema_urn("payload.lineage.suppression_set")
SCHEMA_SET_STRUCTURAL = schema_urn("structural.lineage.suppression_set")
SCHEMA_BLOB_STRUCTURAL = schema_urn("objects.blob-structural-content")
SCHEMA_BLOB_SEMANTIC = schema_urn("objects.blob-semantic-content")

#: Deployment identifier for the governed suppression key (the key bytes
#: themselves never enter canonical state; spec 12.7).
KEY_PROFILE_ID = "thoth-ccf-suppression-key-v1"


# ---------------------------------------------------------------------------
# Merkle construction (registries/suppression-profiles)
# ---------------------------------------------------------------------------


def _leaf(token: str) -> bytes:
    return domain_hash_bytes("ccf:suppression-leaf:v1", token.encode("utf-8"))


def _node(left: bytes, right: bytes) -> bytes:
    return domain_hash_bytes("ccf:suppression-node:v1", left, right)


def _empty_root() -> bytes:
    return domain_hash_bytes("ccf:suppression-empty:v1")


def _largest_power_of_two_smaller_than(n: int) -> int:
    k = 1 << (n.bit_length() - 1)
    return k >> 1 if k == n else k


def entries_merkle_root(tokens: list[str]) -> str:
    """Root over ascending-sorted tokens; duplicates reject (profile)."""
    if len(set(tokens)) != len(tokens):
        raise ErasureError("suppression entries contain duplicates")
    ordered = sorted(tokens)
    if not ordered:
        return "sha256:" + _empty_root().hex()

    def _rec(hashes: list[bytes]) -> bytes:
        if len(hashes) == 1:
            return hashes[0]
        k = _largest_power_of_two_smaller_than(len(hashes))
        return _node(_rec(hashes[:k]), _rec(hashes[k:]))

    return "sha256:" + _rec([_leaf(token) for token in ordered]).hex()


def suppression_blob_bytes(tokens: list[str]) -> bytes:
    """The governed token Blob: JCS over sorted entries + profile name."""
    return canonical_bytes({"entries": sorted(tokens), "profile": SUPPRESSION_PROFILE})


def scope_commitment(plans: list[dict]) -> str:
    """Registry-pinned commitment over the sorted erased object IDs."""
    return canonical_digest(
        "ccf:suppression-scope:v1", sorted(plan["object_id"] for plan in plans)
    )


# ---------------------------------------------------------------------------
# Canonical object construction
# ---------------------------------------------------------------------------


def build_suppression_blob(
    tokens: list[str],
    *,
    blob_id: str,
    archive: dict,
    catalog,
    registries,
    schemas,
    salt_fn,
) -> ResolvedObject:
    """The governed token Blob as a resolved canonical object."""
    data = suppression_blob_bytes(tokens)
    entry = registries.blob_type_entry(SUPPRESSION_BLOB_TYPE)
    content_salt = salt_fn()
    structural_content = {
        "type": SUPPRESSION_BLOB_TYPE,
        "type_version": 1,
        "type_visibility": "clear",
        "schema_digest": catalog.schema_digest(entry["semantic_schema_id"]),
        "registry_entry_digest": registries.entry_digest(entry),
        "retention_profile": entry["retention_profile"],
        "media_type": SUPPRESSION_BLOB_MEDIA_TYPE,
        "byte_length": str(len(data)),
        "content_commitment": blob_content_commitment(content_salt, data),
        "content_profile": "ccf-blob-content-v2",
        "availability_class": "controlled",
        "erasure_domain_id": archive["erasure_domain_id"],
        "structural_payload": {"sensitivity": "governed_sensitive_metadata"},
        "extensions": {},
    }
    semantic_content = {
        "content_salt": content_salt,
        "filename": "suppression-set.json",
        "content_encryption_profile": "none",
        "content_key_ref": None,
        "extensions": {},
    }
    schemas.validate(
        SCHEMA_BLOB_STRUCTURAL, structural_content, what="suppression blob structural"
    )
    schemas.validate(
        SCHEMA_BLOB_SEMANTIC, semantic_content, what="suppression blob semantic"
    )
    structural = _make_envelope("blob", "structural", structural_content, salt_fn)
    semantic = _make_envelope("blob", "semantic", semantic_content, salt_fn)
    header = _make_header("blob", blob_id, structural, semantic)
    return ResolvedObject(
        object_kind="blob",
        object_id=blob_id,
        header=header,
        structural=structural,
        semantic=semantic,
        submission_hash=None,
        origin=None,
        lineage_update=None,
        blob_data=data,
    )


def build_suppression_set_spec(
    tokens: list[str],
    *,
    record_id: str,
    blob_id: str,
    plans: list[dict],
    worker_id: str,
    authority: dict,
    recorded_at: str,
    schemas,
) -> dict:
    """The ``admit_bootstrap``-shaped spec for the suppression-set Record.

    ``erasure_receipt_id`` is null at block-stage time (the receipt lands
    later in the saga and commits back to this set); the schema permits it.
    """
    root = entries_merkle_root(tokens)
    structural_payload = {
        "profile": SUPPRESSION_PROFILE,
        "suppression_blob_id": blob_id,
        "entry_count": str(len(tokens)),
        "entries_merkle_root": root,
        "key_profile_id": KEY_PROFILE_ID,
        "scope_commitment": scope_commitment(plans),
        "erasure_receipt_id": None,
    }
    payload = {
        "sensitivity": "governed_sensitive_metadata",
        "purpose": "erased_content_reintroduction_prevention",
        "extensions": {},
    }
    schemas.validate(SCHEMA_SET_PAYLOAD, payload, what="suppression set payload")
    schemas.validate(
        SCHEMA_SET_STRUCTURAL,
        structural_payload,
        what="suppression set structural payload",
    )
    return {
        "type": SUPPRESSION_SET_TYPE,
        "object_id": record_id,
        "recorded_by": worker_id,
        "recorded_at": recorded_at,
        "authority": authority,
        "payload": payload,
        "structural_payload": structural_payload,
    }


def set_descriptor(spec: dict, blob_id: str) -> dict:
    """The durable on-operation view the receipt stage commits back to."""
    structural = spec["structural_payload"]
    return {
        "profile": structural["profile"],
        "suppression_set_record_id": spec["object_id"],
        "suppression_blob_id": blob_id,
        "entry_count": structural["entry_count"],
        "entries_merkle_root": structural["entries_merkle_root"],
        "key_profile_id": structural["key_profile_id"],
        "scope_commitment": structural["scope_commitment"],
    }


# ---------------------------------------------------------------------------
# Canonical reads, audit, and projection rebuild
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CanonicalSuppressionSet:
    """One verified canonical suppression set."""

    record_id: str
    blob_id: str
    entry_count: int
    merkle_root: str
    key_profile_id: str
    scope_commitment: str
    tokens: tuple[str, ...]


def load_canonical_sets(conn, archive_id: str) -> list[CanonicalSuppressionSet]:
    """Load and verify every canonical suppression set; fail closed.

    Each set's governed Blob is parsed and checked against the Record's
    pinned entry count and Merkle root, and the Blob bytes must be the
    exact canonical serialization — a tampered canonical state raises
    rather than being laundered into the projection.
    """
    rows = conn.execute(
        """
        SELECT c.object_id, c.plaintext_json
        FROM compartment c
        JOIN object_header h ON h.id = c.object_id
        WHERE h.archive_id = %s
          AND c.compartment = 'structural' AND c.state = 'plaintext'
          AND c.plaintext_json ->> 'type' = 'lineage.suppression_set'
        ORDER BY c.object_id
        """,
        (archive_id,),
    ).fetchall()
    sets: list[CanonicalSuppressionSet] = []
    for record_id, content in rows:
        payload = content.get("structural_payload") or {}
        blob_id = payload.get("suppression_blob_id")
        blob_row = conn.execute(
            "SELECT state, plaintext_bytes FROM blob_content WHERE blob_id = %s",
            (blob_id,),
        ).fetchone()
        if blob_row is None or blob_row[0] != "plaintext":
            raise SuppressionProjectionError(
                f"suppression set {record_id} blob {blob_id} is unavailable "
                "(structural retention is required); cannot reconstruct tokens"
            )
        data = bytes(blob_row[1])
        try:
            document = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SuppressionProjectionError(
                f"suppression blob {blob_id} is not valid JSON: {exc}"
            ) from exc
        if document.get("profile") != SUPPRESSION_PROFILE:
            raise SuppressionProjectionError(
                f"suppression blob {blob_id} profile {document.get('profile')!r} "
                f"!= {SUPPRESSION_PROFILE!r}"
            )
        tokens = document.get("entries")
        if not isinstance(tokens, list) or not all(isinstance(t, str) for t in tokens):
            raise SuppressionProjectionError(
                f"suppression blob {blob_id} entries are not a string list"
            )
        if data != suppression_blob_bytes(tokens):
            raise SuppressionProjectionError(
                f"suppression blob {blob_id} bytes are not the canonical "
                "serialization (tampered)"
            )
        if len(tokens) != int(payload["entry_count"]):
            raise SuppressionProjectionError(
                f"suppression set {record_id} entry_count {payload['entry_count']} "
                f"!= blob entries {len(tokens)} (tampered)"
            )
        if entries_merkle_root(tokens) != payload["entries_merkle_root"]:
            raise SuppressionProjectionError(
                f"suppression set {record_id} Merkle root mismatch (tampered)"
            )
        sets.append(
            CanonicalSuppressionSet(
                record_id=record_id,
                blob_id=blob_id,
                entry_count=len(tokens),
                merkle_root=payload["entries_merkle_root"],
                key_profile_id=payload["key_profile_id"],
                scope_commitment=payload["scope_commitment"],
                tokens=tuple(tokens),
            )
        )
    return sets


def verify_projection(conn, archive_id: str) -> dict:
    """Compare the lookup projection against canonical suppression state.

    Returns a report with ``ok``, the expected/present commitment sets,
    and any missing/extra/tampered rows. Never raises for drift — callers
    that must fail closed use :func:`audit_projection`.
    """
    sets = load_canonical_sets(conn, archive_id)
    expected: dict[str, set[str]] = {}  # commitment -> set record ids
    for canonical in sets:
        for token in canonical.tokens:
            expected.setdefault(token, set()).add(canonical.record_id)
    rows = conn.execute(
        """
        SELECT suppression_set_record_id, commitment FROM suppression_entry
        WHERE archive_id = %s
        """,
        (archive_id,),
    ).fetchall()
    present: dict[str, set[str]] = {}
    for set_id, commitment in rows:
        present.setdefault(commitment, set()).add(set_id)
    expected_pairs = {(token, sid) for token, sids in expected.items() for sid in sids}
    present_pairs = {(token, sid) for token, sids in present.items() for sid in sids}
    missing = sorted(f"{sid}:{token}" for token, sid in expected_pairs - present_pairs)
    extra = sorted(f"{sid}:{token}" for token, sid in present_pairs - expected_pairs)
    return {
        "ok": not missing and not extra,
        "sets": len(sets),
        "expected_rows": len(expected_pairs),
        "present_rows": len(present_pairs),
        "missing": missing,
        "extra": extra,
    }


def audit_projection(conn, archive_id: str) -> None:
    """Fail closed when the lookup projection drifts from canonical state."""
    report = verify_projection(conn, archive_id)
    if not report["ok"]:
        raise SuppressionProjectionError(
            "suppression lookup projection drifted from canonical state "
            f"(missing={len(report['missing'])}, extra={len(report['extra'])}); "
            "rebuild it from canonical state before admitting"
        )


def _receipt_linkage(conn, archive_id: str, set_record_id: str) -> tuple[str | None, list[str]]:
    """``(receipt_id, authorized_producers)`` for a set, when its erasure
    receipt has landed (pre-receipt rebuilds fail closed to no authorized
    producers — everyone gets the generic response)."""
    row = conn.execute(
        """
        SELECT c.object_id FROM compartment c
        JOIN object_header h ON h.id = c.object_id
        WHERE h.archive_id = %s
          AND c.compartment = 'structural' AND c.state = 'plaintext'
          AND c.plaintext_json ->> 'type' = 'lineage.erasure_receipt'
          AND c.plaintext_json -> 'structural_payload'
              -> 'suppression_commitment' ->> 'suppression_set_record_id' = %s
        ORDER BY c.object_id
        LIMIT 1
        """,
        (archive_id, set_record_id),
    ).fetchone()
    if row is None:
        return None, []
    receipt_id = row[0]
    operation = conn.execute(
        "SELECT operation_id, authorized_producers FROM erasure_operation "
        "WHERE receipt_id = %s",
        (receipt_id,),
    ).fetchone()
    if operation is None:
        return receipt_id, []
    return receipt_id, list(operation[1])


def rebuild_projection(conn, archive_id: str, *, now: str) -> int:
    """Rebuild the suppression lookup projection from canonical state.

    Full-table rewrite: every row is reconstructed from the verified
    canonical ``lineage.suppression_set`` lineage, so a destroyed or
    tampered table is restored exactly (spec 12.7). Returns the row count.
    """
    from psycopg.types.json import Jsonb

    sets = load_canonical_sets(conn, archive_id)
    conn.execute(
        "DELETE FROM suppression_entry WHERE archive_id = %s", (archive_id,)
    )
    count = 0
    for canonical in sets:
        receipt_id, authorized = _receipt_linkage(conn, archive_id, canonical.record_id)
        operation_id = None
        if receipt_id is not None:
            op = conn.execute(
                "SELECT operation_id FROM erasure_operation WHERE receipt_id = %s",
                (receipt_id,),
            ).fetchone()
            operation_id = op[0] if op else None
        for token in canonical.tokens:
            conn.execute(
                """
                INSERT INTO suppression_entry (
                    archive_id, suppression_set_record_id, commitment, kind,
                    operation_id, suppression_blob_id, key_profile_id,
                    erasure_receipt_id, authorized_producers, created_at
                ) VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s)
                """,
                (
                    archive_id,
                    canonical.record_id,
                    token,
                    operation_id,
                    canonical.blob_id,
                    canonical.key_profile_id,
                    receipt_id,
                    Jsonb(authorized),
                    now,
                ),
            )
            count += 1
    return count


def mark_receipt(
    conn,
    *,
    archive_id: str,
    set_record_id: str,
    receipt_id: str,
    operation_id: str,
    authorized_producers: list[str],
) -> None:
    """Backfill receipt linkage on projection rows once the receipt lands."""
    from psycopg.types.json import Jsonb

    conn.execute(
        """
        UPDATE suppression_entry
        SET erasure_receipt_id = %s, operation_id = %s, authorized_producers = %s
        WHERE archive_id = %s AND suppression_set_record_id = %s
        """,
        (
            receipt_id,
            operation_id,
            Jsonb(list(authorized_producers)),
            archive_id,
            set_record_id,
        ),
    )
