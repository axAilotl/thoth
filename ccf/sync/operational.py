"""Derive restore indexes from signed, verified mindpack material.

The operational streams in a mindpack are transport caches. They are useful
for diagnosis and reproducibility, but their manifest digests are unsigned.
Restore therefore reconstructs archive identity, lineage heads, origin rows,
and producer continuity from verified objects and journal membership. Claims
that can be checked are compared exactly; unavailable canonical compartments
may leave an unverifiable cache row, which is ignored rather than inserted.
"""

from __future__ import annotations

from collections import defaultdict

from ccf.hashing import (
    decode_b64url,
    producer_batch_hash,
    producer_batch_signing_digest,
    submission_hash,
    verify_digest,
)
from ccf.keys import public_key_from_b64url
from ccf.sync.packio import MANIFEST_HEAD_MISMATCH
from ccf.sync.verify import PackVerificationError

SCHEMA_PRODUCER_BATCH = "urn:ccf:schema:0.1.2:sync.producer-batch"


def _credential_versions(objects: dict, members: list[dict]) -> dict[str, list[dict]]:
    """Index credential versions by ID from their verified lineage history.

    A credential is an immutable Record, so rotation and revocation append
    successor Records.  The successor's effective time closes the preceding
    version even when the successor uses a different credential ID.
    """
    from ccf.governance.context import parse_timestamp

    coordinates = {
        member["object_id"]: (
            int(member["commit_sequence"]),
            int(member["commit_position"]),
        )
        for member in members
    }
    histories: dict[str, list[dict]] = defaultdict(list)
    for object_id, obj in objects.items():
        content = (obj.structural or {}).get("content") or {}
        if content.get("type") != "core.device_credential":
            continue
        payload = content.get("structural_payload") or {}
        lineage = content.get("lineage")
        coordinate = coordinates.get(object_id)
        if not payload.get("credential_id") or not isinstance(lineage, dict):
            raise PackVerificationError(
                f"device credential Record {object_id} lacks a canonical lineage"
            )
        if coordinate is None:
            raise PackVerificationError(
                f"device credential Record {object_id} has no admission coordinate"
            )
        histories[lineage["lineage_id"]].append(
            {
                "coordinate": coordinate,
                "object_id": object_id,
                "payload": payload,
                "lineage": lineage,
            }
        )

    versions: dict[str, list[dict]] = defaultdict(list)
    allowed = {None: {"issue"}, "issue": {"rotate", "revoke"}, "rotate": {"rotate", "revoke"}}
    for lineage_id, unordered in histories.items():
        history = sorted(unordered, key=lambda item: item["coordinate"])
        previous_id = None
        previous_state = None
        previous_time = None
        for index, version in enumerate(history):
            lineage = version["lineage"]
            if lineage.get("previous_head_id") != previous_id:
                raise PackVerificationError(
                    f"credential lineage {lineage_id} has a broken predecessor chain"
                )
            transition = lineage.get("transition")
            if transition not in allowed.get(previous_state, set()):
                raise PackVerificationError(
                    f"credential lineage {lineage_id} has invalid transition {transition!r}"
                )
            try:
                effective_at = parse_timestamp(lineage["valid_from"])
            except (KeyError, TypeError, ValueError) as exc:
                raise PackVerificationError(
                    f"credential lineage {lineage_id} has malformed validity: {exc}"
                ) from exc
            if previous_time is not None and effective_at <= previous_time:
                raise PackVerificationError(
                    f"credential lineage {lineage_id} validity is not increasing"
                )
            version["effective_at"] = effective_at
            version["successor_at"] = (
                parse_timestamp(history[index + 1]["lineage"]["valid_from"])
                if index + 1 < len(history)
                else None
            )
            versions[version["payload"]["credential_id"]].append(version)
            previous_id = version["object_id"]
            previous_state = transition
            previous_time = effective_at
    return versions


def _credential_for_batch(
    versions: dict[str, list[dict]], batch: dict
) -> dict:
    """Select the one credential version active at the signed batch time."""
    from ccf.governance.context import parse_timestamp

    batch_time = parse_timestamp(batch["created_at"])
    active = []
    for version in versions.get(batch["credential_id"], []):
        payload = version["payload"]
        lineage = version["lineage"]
        if lineage["transition"] == "revoke":
            continue
        try:
            payload_start = parse_timestamp(payload["valid_from"])
            payload_end = (
                parse_timestamp(payload["expires_at"])
                if payload.get("expires_at") is not None
                else None
            )
            lineage_end = (
                parse_timestamp(lineage["expires_at"])
                if lineage.get("expires_at") is not None
                else None
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"credential has malformed validity: {exc}") from exc
        ends = [
            candidate
            for candidate in (payload_end, lineage_end, version["successor_at"])
            if candidate is not None
        ]
        if (
            batch_time >= max(payload_start, version["effective_at"])
            and (not ends or batch_time < min(ends))
        ):
            active.append(payload)
    if len(active) != 1:
        raise ValueError("credential is not valid at batch creation time")
    return active[0]


def derive_archive_row(chain: dict, commits: list[dict], objects: dict) -> dict:
    """Reconstruct security-relevant archive fields from the signed chain."""
    genesis = commits[0]
    payload = objects[genesis["record_id"]].structural["content"][
        "structural_payload"
    ]
    return {
        "format": "ccf.archive-row/0.1.2",
        "archive_id": chain["archive_id"],
        "epoch_id": chain["epoch_id"],
        "genesis_commit_hash": chain["genesis_commit_hash"],
        "hash_profile": chain["hash_profile"],
        "signature_profile": chain["signature_profile"],
        "semantic_catalog_root": chain["semantic_catalog_root"],
        "active_profiles": chain["active_profiles"],
        "signer_key_id": chain["signer_key_id"],
        # This is an operational key-management namespace, not signed
        # canonical identity. Restore creates a fresh local domain.
        "erasure_domain_id": None,
        "created_at": payload["committed_at"],
    }


def compare_archive_row(claim: dict, derived: dict) -> None:
    """Reject any claimed archive value that can affect restored state."""
    for field, value in derived.items():
        if field == "erasure_domain_id" and value is None:
            continue  # no canonical content uses the old operational domain
        if claim.get(field) != value:
            raise PackVerificationError(
                f"archive.json {field} differs from verified canonical material",
                reason=MANIFEST_HEAD_MISMATCH,
            )


def derive_lineage_heads(objects: dict, members: list[dict]) -> list[dict]:
    """Replay portable compare-and-swap lineage blocks in admission order."""
    coordinates = {
        member["object_id"]: (
            int(member["commit_sequence"]),
            int(member["commit_position"]),
        )
        for member in members
    }
    transitions = []
    for object_id, obj in objects.items():
        content = (obj.structural or {}).get("content") or {}
        lineage = content.get("lineage")
        if lineage is not None:
            coordinate = coordinates.get(object_id)
            if coordinate is None:
                raise PackVerificationError(
                    f"lineage Record {object_id} has no verified admission coordinate"
                )
            transitions.append((coordinate, object_id, obj, lineage))
    heads: dict[str, dict] = {}
    for (sequence, _position), object_id, obj, lineage in sorted(transitions):
        lineage_id = lineage["lineage_id"]
        current = heads.get(lineage_id)
        previous = lineage.get("previous_head_id")
        if (current is None and previous is not None) or (
            current is not None and previous != current["head_record_id"]
        ):
            raise PackVerificationError(
                f"lineage {lineage_id} has a broken compare-and-swap chain"
            )
        heads[lineage_id] = {
            "lineage_id": lineage_id,
            "head_record_id": object_id,
            "head_record_hash": obj.header["object_hash"],
            "head_commit_sequence": str(sequence),
            "state": lineage["transition"],
            "valid_from": lineage["valid_from"],
            "expires_at": lineage["expires_at"],
        }
    return [heads[key] for key in sorted(heads)]


def derive_origin_rows(
    objects: dict, unavailable_ids: set[str], bound_object_ids: set[str]
) -> list[dict]:
    """Reconstruct origin idempotency rows only where plaintext proves them."""
    rows = []
    for object_id, obj in objects.items():
        if object_id not in bound_object_ids:
            continue
        semantic = (obj.semantic or {}).get("content") or {}
        origin = semantic.get("origin")
        if origin is None:
            continue
        rows.append(
            {
                "source_id": origin["source_id"],
                "native_id": origin["native_id"],
                "revision": origin["revision"],
                "submission_hash": origin["submission_hash"],
                "object_kind": obj.object_kind,
                "object_id": object_id,
                "lifecycle": "erased" if object_id in unavailable_ids else "active",
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            row["source_id"],
            row["native_id"],
            row["revision"],
            row["object_kind"],
        ),
    )


def compare_derived_rows(
    what: str,
    claims: list[dict],
    derived: list[dict],
    *,
    unavailable_ids: set[str],
) -> None:
    """Compare derivable rows; tolerate but discard caches for unavailable IDs."""
    derived_by_object = {row["head_record_id"]: row for row in derived} if (
        what == "lineage-heads.ndjson"
    ) else {row["object_id"]: row for row in derived}
    claimed_by_object: dict[str, dict] = {}
    object_field = "head_record_id" if what == "lineage-heads.ndjson" else "object_id"
    for row in claims:
        object_id = row.get(object_field)
        if not isinstance(object_id, str) or object_id in claimed_by_object:
            raise PackVerificationError(f"{what} has a duplicate or malformed row")
        claimed_by_object[object_id] = row
    for object_id, expected in derived_by_object.items():
        if claimed_by_object.get(object_id) != expected:
            raise PackVerificationError(
                f"{what} differs from canonical object {object_id}"
            )
    extras = set(claimed_by_object) - set(derived_by_object)
    if extras - unavailable_ids:
        raise PackVerificationError(
            f"{what} contains rows not derivable from verified objects: "
            f"{sorted(extras - unavailable_ids)}"
        )


def verify_producer_state(
    objects: dict,
    members: list[dict],
    batches: list[dict],
    producer_heads: list[dict],
    *,
    catalog_root: str,
    schemas,
) -> tuple[list[dict], list[dict], list[str], set[str]]:
    """Verify signed producer batches and rebuild heads from bound evidence."""
    credentials = _credential_versions(objects, members)

    signed: dict[str, list[tuple[dict, set[str]]]] = defaultdict(list)
    seen_batch_ids: set[str] = set()
    seen_coordinates: set[tuple[str, int]] = set()
    bound_object_ids: set[str] = set()
    for batch in batches:
        try:
            schemas.validate(SCHEMA_PRODUCER_BATCH, batch, what="producer batch")
            if producer_batch_hash(batch) != batch["batch_hash"]:
                raise ValueError("batch hash mismatch")
            if batch["semantic_catalog_root"] != catalog_root:
                raise ValueError("semantic catalog root mismatch")
            credential = _credential_for_batch(credentials, batch)
            if "capture" not in credential.get("scopes", []):
                raise ValueError("credential is missing, ambiguous, or lacks capture")
            if credential.get("subject_id") != batch["producer_id"]:
                raise ValueError("credential subject does not match producer")
            verify_digest(
                public_key_from_b64url(credential["signing_key"]["public_key"]),
                decode_b64url(batch["signature"]),
                producer_batch_signing_digest(batch["batch_hash"]),
            )
        except Exception as exc:
            raise PackVerificationError(
                f"producer batch {batch.get('batch_id')} is not independently valid: {exc}"
            ) from exc
        sequence = int(batch["producer_sequence"])
        coordinate = (batch["producer_id"], sequence)
        if batch["batch_id"] in seen_batch_ids or coordinate in seen_coordinates:
            raise PackVerificationError("producer batch identity/sequence is duplicated")
        seen_batch_ids.add(batch["batch_id"])
        seen_coordinates.add(coordinate)

        batch_bound_ids: set[str] = set()
        for kind in ("records", "links", "blobs"):
            for submission in batch[kind]:
                obj = objects.get(submission["id"])
                evidence = ((obj.semantic or {}).get("content") or {}).get(
                    "producer_evidence"
                ) if obj is not None else None
                expected_evidence = {
                    "batch_id": batch["batch_id"],
                    "credential_id": batch["credential_id"],
                    "producer_sequence": batch["producer_sequence"],
                    "submission_hash": submission_hash(submission),
                }
                if evidence == expected_evidence:
                    bound_object_ids.add(submission["id"])
                    batch_bound_ids.add(submission["id"])
        signed[batch["producer_id"]].append((batch, batch_bound_ids))

    verified_batches: list[dict] = []
    derived_heads: list[dict] = []
    signed_heads: list[dict] = []
    skipped: list[str] = []
    for producer_id, candidates in signed.items():
        ordered = sorted(candidates, key=lambda item: int(item[0]["producer_sequence"]))
        previous_hash = None
        expected_sequence = 1
        for batch, batch_bound_ids in ordered:
            sequence = int(batch["producer_sequence"])
            if sequence != expected_sequence or batch["previous_batch_hash"] != previous_hash:
                raise PackVerificationError(
                    f"producer {producer_id} batch chain is not contiguous"
                )
            expected_sequence += 1
            previous_hash = batch["batch_hash"]
            verified_batches.append(
                {
                    "batch": batch,
                    "bound_object_ids": sorted(batch_bound_ids),
                }
            )
        signed_latest = ordered[-1][0]
        signed_heads.append(
            {
                "producer_id": producer_id,
                "producer_sequence": signed_latest["producer_sequence"],
                "batch_hash": signed_latest["batch_hash"],
                "credential_id": signed_latest["credential_id"],
            }
        )
        derived_heads.append(
            {
                "producer_id": producer_id,
                "producer_sequence": signed_latest["producer_sequence"],
                "batch_hash": signed_latest["batch_hash"],
                "credential_id": signed_latest["credential_id"],
                "updated_at": signed_latest["created_at"],
            }
        )

    claimed_heads = {
        (
            row.get("producer_id"),
            row.get("producer_sequence"),
            row.get("batch_hash"),
            row.get("credential_id"),
        )
        for row in producer_heads
    }
    expected_heads = {
        (
            row["producer_id"],
            row["producer_sequence"],
            row["batch_hash"],
            row["credential_id"],
        )
        for row in signed_heads
    }
    if len(claimed_heads) != len(producer_heads) or claimed_heads != expected_heads:
        raise PackVerificationError(
            "producer-heads.ndjson differs from independently verified batches"
        )
    return (
        verified_batches,
        sorted(derived_heads, key=lambda row: row["producer_id"]),
        skipped,
        bound_object_ids,
    )
