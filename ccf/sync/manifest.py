"""Manifest cross-checks: derive ground truth from verified pack contents.

``manifest.json`` is an unsigned, non-authoritative transport index
(spec 11.5). It must never independently determine object availability,
custody, restoration eligibility, import behavior, or iteration bounds.
After the portable objects, Blob bytes, and commit chain have been
verified, the importer independently derives the pack inventory and
availability state from that verified material and compares every
manifest claim against it. Any disagreement fails closed here — before
any canonical state is created or modified.

The journal membership stream and the verified object streams define the
iteration set; manifest counts are compared, never iterated to, so a
maliciously reduced count cannot truncate verification.

With ``allow_partial`` the caller explicitly permits a truthfully declared
partial-custody import. It does not relax comparisons: every unsigned claim
must still match the inventory derived from verified contents exactly.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ccf.sync.completeness import object_references
from ccf.sync.packio import (
    MANIFEST_AVAILABILITY_MISMATCH,
    MANIFEST_COMPLETENESS_MISMATCH,
    MANIFEST_COUNT_MISMATCH,
    MANIFEST_EXTERNAL_DEPENDENCY_MISMATCH,
    MANIFEST_HEAD_MISMATCH,
    MANIFEST_MODE_MISMATCH,
    MANIFEST_STREAM_DIGEST_MISMATCH,
    MANIFEST_UNKNOWN_EXTENSION_MISMATCH,
)
from ccf.sync.verify import PackVerificationError

_RECEIPT_TYPE = "lineage.erasure_receipt"
_COVERS_LINK_TYPE = "ccf.covers"
EXTERNAL_DEPENDENCY_REASON = "unresolved_reference"

#: No final-0.1.2 manifest extensions are defined. Unknown claims fail closed.
_KNOWN_EXTENSIONS: frozenset[str] = frozenset()

#: Manifest modes consistent with each caller-requested operation. The
#: mode is non-authoritative exporter intent; it must be consistent with
#: the requested operation, never silently reinterpreted.
_OPERATION_MODES = {
    "restore": ("restore", "replica"),
    "merge": ("restore", "replica", "foreign_merge"),
    "import": ("restore", "replica"),
}


@dataclass
class DerivedInventory:
    """Pack inventory and availability derived from verified contents."""

    counts: dict[str, int]  # records/links/blobs/commits actually in the pack
    unavailable_ids: set[str]  # objects missing at least one expected compartment
    withheld: set[str]
    erased: set[str]
    availability: dict[tuple[str, str], dict] = field(default_factory=dict)
    unresolved_references: set[str] = field(default_factory=set)
    missing_member_ids: set[str] = field(default_factory=set)
    object_hashes: set[str] = field(default_factory=set)
    foreign_custody_proofs: set[str] = field(default_factory=set)

    @property
    def custody_complete(self) -> bool:
        """Whether the pack carries its full journal membership and references."""
        return (
            not self.missing_member_ids
            and not self.unresolved_references
            and not self.withheld
        )

    @property
    def restore_capable(self) -> bool:
        """A fully verified complete pack can reproduce canonical custody."""
        return self.custody_complete


def expected_compartments(obj) -> list[str]:
    """Compartments an object must carry when fully available."""
    compartments = ["structural"]
    if obj.header.get("semantic_commitment") is not None:
        compartments.append("semantic")
    if obj.object_kind == "blob":
        compartments.append("blob_content")
    return compartments


def unavailable_object_ids(objects: dict, blob_data: dict) -> set[str]:
    """Objects missing at least one expected compartment (presence only).

    This is derived from the verified pack contents alone — never from
    the manifest's withheld/erased declarations.
    """
    unavailable: set[str] = set()
    for object_id, obj in objects.items():
        for compartment in expected_compartments(obj):
            if compartment == "blob_content":
                if object_id not in blob_data:
                    unavailable.add(object_id)
            elif getattr(obj, compartment) is None:
                unavailable.add(object_id)
                break
    return unavailable


def _receipt_coverage(objects: dict) -> dict[str, str]:
    """Map erased object ID -> erasure receipt Record ID.

    Mirrors the exporter's derivation: a ``ccf.covers`` membership Link
    from a ``lineage.erasure_receipt`` Record covers its target. Only
    verified, present structural envelopes count; when several receipts
    cover one target the lexicographically first receipt wins.
    """
    receipts = {
        object_id
        for object_id, obj in objects.items()
        if obj.object_kind == "record"
        and ((obj.structural or {}).get("content") or {}).get("type") == _RECEIPT_TYPE
    }
    covers = [
        (obj.structural or {}).get("content") or {}
        for obj in objects.values()
        if obj.object_kind == "link"
        and ((obj.structural or {}).get("content") or {}).get("type")
        == _COVERS_LINK_TYPE
    ]
    covered: dict[str, str] = {}
    for content in sorted(covers, key=lambda c: c.get("from_id") or ""):
        receipt_id = content.get("from_id")
        target_id = content.get("to_id")
        if receipt_id in receipts and target_id:
            covered.setdefault(target_id, receipt_id)
    return covered


def derive_pack_inventory(
    objects: dict,
    blob_data: dict,
    commits: list[dict],
    members: list[dict],
    *,
    archive_id: str,
    known_ids: set[str] | None = None,
) -> DerivedInventory:
    """Reconstruct the actual pack inventory from verified material.

    ``objects``/``blob_data``/``commits``/``members`` must already be
    digest- and chain-verified; the derivation trusts no manifest field.
    """
    known_ids = known_ids or set()
    coordinates = {
        member["object_id"]: (
            int(member["commit_sequence"]),
            int(member["commit_position"]),
        )
        for member in members
    }
    covered = _receipt_coverage(objects)

    counts = {"records": 0, "links": 0, "blobs": 0, "commits": len(commits)}
    availability: dict[tuple[str, str], dict] = {}
    withheld: set[str] = set()
    erased: set[str] = set()
    for object_id, obj in objects.items():
        counts[f"{obj.object_kind}s"] += 1
        structural_content = (obj.structural or {}).get("content") or {}
        retention_profile = structural_content.get("retention_profile")
        states: dict[str, str] = {}
        for compartment in expected_compartments(obj):
            if compartment == "blob_content":
                present = object_id in blob_data
                commitment = structural_content.get("content_commitment")
            else:
                present = getattr(obj, compartment) is not None
                commitment = obj.header[f"{compartment}_commitment"]
            if present:
                state = "available"
            elif object_id in covered:
                state = "erased"
            else:
                state = "withheld"
            states[compartment] = state

            if state == "available":
                custody_proof = None
                lineage_id = None
            else:
                position = coordinates.get(object_id)
                # An unavailable object without admission coordinates is
                # not exportable; the derived None cannot match any valid
                # manifest claim, so the comparison fails closed.
                custody_proof = (
                    f"commit:{position[0]}:{position[1]}" if position else None
                )
                lineage_id = covered.get(object_id) if state == "erased" else None
            if retention_profile is None and states["structural"] == "erased":
                # Only the ``erasable`` profile permits structural erasure.
                retention = "erasable"
            else:
                retention = retention_profile
            availability[(object_id, compartment)] = {
                "object_kind": obj.object_kind,
                "object_id": object_id,
                "compartment": compartment,
                "availability": state,
                "commitment": commitment,
                "retention_profile": retention,
                "source_custody_proof": custody_proof,
                "unavailability_lineage_id": lineage_id,
            }
        if "erased" in states.values():
            erased.add(object_id)
        elif "withheld" in states.values():
            withheld.add(object_id)

    present_or_satisfied = set(objects) | known_ids | withheld | erased
    unresolved = set()
    for obj in objects.values():
        unresolved |= object_references(obj) - present_or_satisfied

    foreign_custody_proofs = set()
    for obj in objects.values():
        content = (obj.structural or {}).get("content") or {}
        payload = content.get("structural_payload") or {}
        source_archive_id = payload.get("archive_id")
        if (
            content.get("type") == "integrity.commit"
            and source_archive_id
            and source_archive_id != archive_id
        ):
            foreign_custody_proofs.add(
                f"{source_archive_id}:{obj.header['object_hash']}"
            )

    return DerivedInventory(
        counts=counts,
        unavailable_ids=withheld | erased,
        withheld=withheld,
        erased=erased,
        availability=availability,
        unresolved_references=unresolved,
        missing_member_ids={member["object_id"] for member in members} - set(objects),
        object_hashes={obj.header["object_hash"] for obj in objects.values()},
        foreign_custody_proofs=foreign_custody_proofs,
    )


def check_manifest_mode(manifest: dict, *, operation: str) -> None:
    """Fail closed when the manifest mode contradicts the requested operation."""
    allowed = _OPERATION_MODES[operation]
    if manifest["mode"] not in allowed:
        raise PackVerificationError(
            f"manifest mode {manifest['mode']!r} is inconsistent with a "
            f"requested {operation}",
            reason=MANIFEST_MODE_MISMATCH,
        )


def compare_manifest(
    manifest: dict,
    inventory: DerivedInventory,
    *,
    chain: dict,
    pack_names: set[str],
    allow_partial: bool,
    operation: str,
) -> None:
    """Compare every manifest claim against the derived ground truth.

    Runs after full content verification and before any database
    mutation; any disagreement raises :class:`PackVerificationError`
    with a stable ``MANIFEST_*`` reason.
    """
    check_manifest_mode(manifest, operation=operation)
    _compare_chain_identity(manifest, chain)
    _compare_counts(manifest, inventory)
    _compare_streams(manifest, pack_names)
    _compare_head(manifest, chain)
    _compare_availability(manifest, inventory)
    _compare_external_dependencies(manifest, inventory)
    _compare_custody_proofs(manifest, inventory)
    _compare_custody(manifest, inventory, allow_partial=allow_partial, operation=operation)
    _compare_extensions(manifest)


def _compare_counts(manifest: dict, inventory: DerivedInventory) -> None:
    claimed = manifest["counts"]
    derived = inventory.counts
    for key in ("records", "links", "blobs", "commits"):
        claim, actual = int(claimed[key]), derived[key]
        if claim != actual:
            raise PackVerificationError(
                f"manifest counts.{key} {claim} != derived {actual}",
                reason=MANIFEST_COUNT_MISMATCH,
            )


def _compare_streams(manifest: dict, pack_names: set[str]) -> None:
    listed = [entry["path"] for entry in manifest["streams"]]
    if len(set(listed)) != len(listed):
        raise PackVerificationError(
            "manifest lists a stream path more than once",
            reason=MANIFEST_STREAM_DIGEST_MISMATCH,
        )
    actual = set(pack_names) - {"manifest.json"}
    claimed = set(listed)
    if actual != claimed:
        raise PackVerificationError(
            "manifest stream membership differs from actual container members "
            f"(missing claims {sorted(actual - claimed)}, "
            f"absent members {sorted(claimed - actual)})",
            reason=MANIFEST_STREAM_DIGEST_MISMATCH,
        )
    for entry in manifest["streams"]:
        # The transport profile independently defines only Blob plaintext
        # bytes as optional. All object, compartment, journal, catalog, and
        # operational streams are required; the unsigned flag cannot weaken
        # that contract.
        expected_required = not entry["path"].startswith("blob-data/")
        if entry["required"] is not expected_required:
            raise PackVerificationError(
                f"manifest stream {entry['path']} has required="
                f"{entry['required']}, derived {expected_required}",
                reason=MANIFEST_STREAM_DIGEST_MISMATCH,
            )


def _compare_chain_identity(manifest: dict, chain: dict) -> None:
    """Compare unsigned identity/profile claims with signed commit payloads."""
    for manifest_field, chain_field in (
        ("archive_id", "archive_id"),
        ("epoch_id", "epoch_id"),
        ("semantic_catalog_root", "semantic_catalog_root"),
        ("hash_profile", "hash_profile"),
        ("profiles", "active_profiles"),
    ):
        if manifest[manifest_field] != chain[chain_field]:
            raise PackVerificationError(
                f"manifest {manifest_field} does not match the signed chain",
                reason=MANIFEST_HEAD_MISMATCH,
            )


def _compare_head(manifest: dict, chain: dict) -> None:
    if chain["genesis_commit_hash"] != manifest["genesis_commit_hash"]:
        raise PackVerificationError(
            "manifest genesis hash does not match verified chain",
            reason=MANIFEST_HEAD_MISMATCH,
        )
    if chain["head_commit_hash"] != manifest["head_commit_hash"] or (
        chain["head_sequence"] != manifest["head_sequence"]
    ):
        raise PackVerificationError(
            "manifest head does not match verified chain",
            reason=MANIFEST_HEAD_MISMATCH,
        )


def _compare_availability(manifest: dict, inventory: DerivedInventory) -> None:
    for field_name, derived_set in (
        ("withheld", inventory.withheld),
        ("erased", inventory.erased),
    ):
        claimed_set = set(manifest[field_name])
        if len(claimed_set) != len(manifest[field_name]):
            raise PackVerificationError(
                f"manifest {field_name} list contains duplicates",
                reason=MANIFEST_AVAILABILITY_MISMATCH,
            )
        if claimed_set != derived_set:
            raise PackVerificationError(
                f"manifest {field_name} list disagrees with derived pack "
                f"contents (claimed {sorted(claimed_set)}, "
                f"derived {sorted(derived_set)})",
                reason=MANIFEST_AVAILABILITY_MISMATCH,
            )

    claimed_entries: dict[tuple[str, str], dict] = {}
    for entry in manifest["compartment_availability"]:
        key = (entry["object_id"], entry["compartment"])
        if key in claimed_entries:
            raise PackVerificationError(
                f"duplicate compartment availability entry for {key}",
                reason=MANIFEST_AVAILABILITY_MISMATCH,
            )
        claimed_entries[key] = entry
    for key, derived_entry in inventory.availability.items():
        claimed_entry = claimed_entries.get(key)
        if claimed_entry is None:
            raise PackVerificationError(
                f"manifest lacks an availability entry for {key}",
                reason=MANIFEST_AVAILABILITY_MISMATCH,
            )
        _compare_availability_entry(key, claimed_entry, derived_entry)
    for key in claimed_entries:
        if key in inventory.availability:
            continue
        raise PackVerificationError(
            f"manifest availability entry {key} has no counterpart in the "
            "verified pack contents",
            reason=MANIFEST_AVAILABILITY_MISMATCH,
        )


def _compare_availability_entry(
    key: tuple[str, str], claimed: dict, derived: dict
) -> None:
    if claimed["availability"] != derived["availability"]:
        raise PackVerificationError(
            f"manifest availability for {key} is {claimed['availability']!r}, "
            f"derived {derived['availability']!r}",
            reason=MANIFEST_AVAILABILITY_MISMATCH,
        )
    if claimed != derived:
        raise PackVerificationError(
            f"manifest availability entry for {key} disagrees with the "
            f"derived entry (claimed {claimed}, derived {derived})",
            reason=MANIFEST_AVAILABILITY_MISMATCH,
        )


def _compare_external_dependencies(
    manifest: dict, inventory: DerivedInventory
) -> None:
    """Every claimed dependency must be a real unresolved reference.

    A dependency existing only as a manifest entry — pointing at nothing,
    or at material the pack verifiably carries — is invalid.
    """
    claimed = {
        dep["object_id"]: dep for dep in manifest["external_dependencies"]
    }
    if len(claimed) != len(manifest["external_dependencies"]):
        raise PackVerificationError(
            "manifest external_dependencies contains duplicates",
            reason=MANIFEST_EXTERNAL_DEPENDENCY_MISMATCH,
        )
    phantom = set(claimed) - inventory.unresolved_references
    if phantom:
        raise PackVerificationError(
            f"manifest external dependencies not backed by any unresolved "
            f"reference in the verified pack contents: {sorted(phantom)}",
            reason=MANIFEST_EXTERNAL_DEPENDENCY_MISMATCH,
        )
    missing = inventory.unresolved_references - set(claimed)
    if missing:
        raise PackVerificationError(
            "manifest omits external dependencies derived from unresolved "
            f"references in the verified pack contents: {sorted(missing)}",
            reason=MANIFEST_EXTERNAL_DEPENDENCY_MISMATCH,
        )
    expected = {
        object_id: {
            "object_id": object_id,
            "reason": EXTERNAL_DEPENDENCY_REASON,
        }
        for object_id in inventory.unresolved_references
    }
    if claimed != expected:
        raise PackVerificationError(
            "manifest dependency metadata is not independently derivable "
            f"(claimed {claimed}, derived {expected})",
            reason=MANIFEST_EXTERNAL_DEPENDENCY_MISMATCH,
        )


def _compare_custody_proofs(manifest: dict, inventory: DerivedInventory) -> None:
    """Compare proofs with foreign commit Records in verified contents."""
    claimed = set(manifest["foreign_custody_proofs"])
    if len(claimed) != len(manifest["foreign_custody_proofs"]):
        raise PackVerificationError(
            "manifest foreign_custody_proofs contains duplicates",
            reason=MANIFEST_HEAD_MISMATCH,
        )
    if claimed != inventory.foreign_custody_proofs:
        raise PackVerificationError(
            "manifest foreign custody proofs differ from verified foreign "
            f"commit Records (claimed {sorted(claimed)}, "
            f"derived {sorted(inventory.foreign_custody_proofs)})",
            reason=MANIFEST_HEAD_MISMATCH,
        )


def _compare_custody(
    manifest: dict,
    inventory: DerivedInventory,
    *,
    allow_partial: bool,
    operation: str,
) -> None:
    """Compare first-class custody claims with verified pack capability."""
    derived = {
        "completeness": "complete" if inventory.custody_complete else "partial",
        "restore_capable": inventory.restore_capable,
    }
    if manifest["custody"] != derived:
        raise PackVerificationError(
            f"manifest custody {manifest['custody']} != derived {derived}",
            reason=MANIFEST_COMPLETENESS_MISMATCH,
        )
    if not inventory.custody_complete and not allow_partial:
        raise PackVerificationError(
            "verified pack has partial custody but the caller did not permit it",
            reason=MANIFEST_COMPLETENESS_MISMATCH,
        )
    if operation == "restore" and not inventory.restore_capable:
        raise PackVerificationError(
            "verified pack is not restore capable",
            reason=MANIFEST_COMPLETENESS_MISMATCH,
        )


def _compare_extensions(manifest: dict) -> None:
    extensions = manifest["extensions"]
    unknown = set(extensions) - _KNOWN_EXTENSIONS
    if unknown:
        raise PackVerificationError(
            f"manifest declares unknown extensions: {sorted(unknown)}",
            reason=MANIFEST_UNKNOWN_EXTENSION_MISMATCH,
        )
