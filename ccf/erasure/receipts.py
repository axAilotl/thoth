"""Canonical erasure receipts and membership Links (spec 1.6, 3.8).

A completed erasure saga appends a ``lineage.erasure_receipt`` Record —
the durable result of the saga — plus one ``ccf.covers`` membership Link
per target. Both go through the canonical commit path (signed commit,
journal, invalidation effects) inside the saga's receipt-stage
transaction, so the receipt and the operation's terminal stage are
atomic.

The receipt Record's ``required_authority`` is ``authorized_erasure_worker``;
the caller's claim must satisfy it (``explicit_authorization``). Payload
and structural payload are validated against the pinned schemas before
the Record is built.
"""

from __future__ import annotations

from ccf.admission import ResolvedObject, _make_envelope, _make_header
from ccf.catalog import SemanticCatalog
from ccf.registry import PinnedRegistries
from ccf.schemas import SchemaSet

RECEIPT_TYPE = "lineage.erasure_receipt"
MEMBERSHIP_LINK_TYPE = "ccf.covers"

SCHEMA_RECEIPT_PAYLOAD = "urn:ccf:schema:0.1.2:payload.lineage.erasure_receipt"
SCHEMA_RECEIPT_STRUCTURAL = "urn:ccf:schema:0.1.2:structural.lineage.erasure_receipt"
SCHEMA_LINK_STRUCTURAL = "urn:ccf:schema:0.1.2:objects.link-structural-content"
SCHEMA_LINK_SEMANTIC = "urn:ccf:schema:0.1.2:objects.link-semantic-content"


def build_receipt_record_spec(
    *,
    schemas: SchemaSet,
    operation_id: str,
    decision_id: str,
    profile: str,
    targets: list[dict],
    verification: dict,
    worker_id: str,
    authority: dict,
    completed_at: str,
    suppression_commitment: dict,
) -> dict:
    """The ``admit_bootstrap``-shaped spec for the erasure receipt Record.

    ``selectors_invalidated`` counts selector (Link semantic) erasures.
    ``keys_destroyed`` and ``ciphertexts_deleted`` are honestly zero: this
    is a plaintext-envelope logical erasure (spec 3.7).

    ``suppression_commitment`` commits the receipt to the canonical
    ``lineage.suppression_set`` lineage (profile, set Record and Blob IDs,
    entry count, Merkle root, key/profile id, scope commitment) — required
    by the 0.1.2 receipt schema (spec 12.7).
    """
    selectors = sum(
        1
        for plan in targets
        if plan["object_kind"] == "link" and plan["erase_semantic"]
    )
    payload = {
        "operation_id": operation_id,
        "decision_id": decision_id,
        "status": "verified",
        "keys_destroyed": "0",
        "ciphertexts_deleted": "0",
        "selectors_invalidated": str(selectors),
        "completed_at": completed_at,
        "verification": verification,
        "extensions": {},
    }
    structural_payload = {
        "decision_id": decision_id,
        "profile": profile,
        "verified_at": completed_at,
        "target_count": str(len(targets)),
        "destroyed_key_count": "0",
        "status": "verified",
        "membership_link_type": MEMBERSHIP_LINK_TYPE,
        "suppression_commitment": suppression_commitment,
    }
    schemas.validate(SCHEMA_RECEIPT_PAYLOAD, payload, what="erasure receipt payload")
    schemas.validate(
        SCHEMA_RECEIPT_STRUCTURAL,
        structural_payload,
        what="erasure receipt structural payload",
    )
    return {
        "type": RECEIPT_TYPE,
        "recorded_by": worker_id,
        "recorded_at": completed_at,
        "authority": authority,
        "payload": payload,
        "structural_payload": structural_payload,
    }


def resolve_membership_link(
    *,
    link_id: str,
    receipt_id: str,
    target_id: str,
    worker_id: str,
    authority: dict,
    recorded_at: str,
    catalog: SemanticCatalog,
    registries: PinnedRegistries,
    schemas: SchemaSet,
    salt_fn,
) -> ResolvedObject:
    """Resolve one ``ccf.covers`` membership Link (receipt -> target)."""
    entry = registries.link_entry(MEMBERSHIP_LINK_TYPE)
    structural_content = {
        "type": MEMBERSHIP_LINK_TYPE,
        "type_version": 1,
        "type_visibility": "clear",
        "schema_digest": catalog.schema_digest(SCHEMA_LINK_SEMANTIC),
        "registry_entry_digest": registries.entry_digest(entry),
        "retention_profile": entry["retention_profile"],
        "structural_payload": {},
        "extensions": {},
        "from_id": receipt_id,
        "to_id": target_id,
    }
    semantic_content = {
        "recorded_by": worker_id,
        "recorded_at": recorded_at,
        "authority": authority,
        "payload": {},
        "extensions": {},
    }
    schemas.validate(
        SCHEMA_LINK_STRUCTURAL, structural_content, what="membership link structural"
    )
    schemas.validate(
        SCHEMA_LINK_SEMANTIC, semantic_content, what="membership link semantic"
    )
    structural = _make_envelope("link", "structural", structural_content, salt_fn)
    semantic = _make_envelope("link", "semantic", semantic_content, salt_fn)
    header = _make_header("link", link_id, structural, semantic)
    return ResolvedObject(
        object_kind="link",
        object_id=link_id,
        header=header,
        structural=structural,
        semantic=semantic,
        submission_hash=None,
        origin=None,
        lineage_update=None,
        blob_data=None,
    )
