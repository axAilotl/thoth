"""CCF 0.2.0 uplift and downgrade receipts.

An uplift receipt maps each source submission ID to the same canonical ID
and never silently upgrades unsigned material to verified producer evidence.
A downgrade receipt enumerates the exact source-minus-export inventory.
"""

from __future__ import annotations

import json
from pathlib import Path

from ccf.capsule import Capsule, CapsuleError, submission_hashes_for
from ccf.hashing import submission_hash
from ccf.ids import generate_id, parse_id
from ccf.layered import LayeredRegistries, raw_digest
from ccf.objects import now_timestamp
from ccf.schemas import SchemaSet

SCHEMA_UPLIFT = "urn:ccf:schema:0.2.0:exchange.uplift-receipt"
SCHEMA_DOWNGRADE = "urn:ccf:schema:0.2.0:exchange.downgrade-receipt"
SCHEMA_INVENTORY = "urn:ccf:schema:0.2.0:exchange.downgrade-inventory"

INVENTORY_CATEGORIES = frozenset(
    {
        "submission",
        "journal_proof",
        "policy_state",
        "lineage_state",
        "compartment",
        "blob_content",
        "unknown_extension",
        "registry",
        "schema",
        "other",
    }
)


class ExchangeError(ValueError):
    """Raised when an uplift or downgrade receipt is dishonest."""


def _kind_of(object_id: str) -> str:
    return parse_id(object_id).kind


def build_pending_uplift(
    capsule: Capsule,
    *,
    destination_level: str,
    destination_archive_id: str,
    created_at: str | None = None,
    archive_resolution: dict[str, dict] | None = None,
    receipt_id: str | None = None,
) -> dict:
    """Build a pending L1-to-higher uplift that preserves every supplied ID."""
    hashes = submission_hashes_for(capsule)
    objects = []
    for submission in capsule.submissions:
        object_id = submission["id"]
        resolution = (archive_resolution or {}).get(object_id, {})
        objects.append(
            {
                "object_kind": submission["submission_kind"],
                "source_id": object_id,
                "source_submission_hash": hashes[object_id],
                "canonical_id": object_id,
                "object_hash": None,
                "disposition": "pending",
                "producer_authentication": "absent",
                "producer_proof": None,
                "archive_resolution": resolution,
            }
        )
    return {
        "format": "ccf.uplift-receipt/0.2.0",
        "receipt_id": receipt_id or generate_id("receipt"),
        "source_pack_id": capsule.manifest["pack_id"],
        "source_level": capsule.manifest["level"],
        "destination_level": destination_level,
        "destination_archive_id": destination_archive_id,
        "created_at": created_at or now_timestamp(),
        "status": "pending",
        "objects": objects,
        "conflicts": [],
        "extensions": {},
    }


def verify_uplift_receipt(
    receipt: dict,
    *,
    capsule: Capsule,
    layered: LayeredRegistries,
    schemas: SchemaSet,
    allow_verified_producer: bool = False,
) -> None:
    """Validate an uplift receipt against its source Capsule."""
    schemas.validate(SCHEMA_UPLIFT, receipt, what="uplift receipt")
    if receipt["source_pack_id"] != capsule.manifest["pack_id"]:
        raise ExchangeError("uplift receipt is not bound to this Capsule")
    if receipt["source_level"] != capsule.manifest["level"]:
        raise ExchangeError("uplift source_level does not match the Capsule")
    if layered.level_rank(receipt["destination_level"]) < layered.level_rank(
        receipt["source_level"]
    ):
        raise ExchangeError("uplift receipt moves to a weaker level")

    expected_ids = [submission["id"] for submission in capsule.submissions]
    got_ids = [entry["source_id"] for entry in receipt["objects"]]
    if sorted(got_ids) != sorted(expected_ids) or len(got_ids) != len(set(got_ids)):
        raise ExchangeError("uplift receipt does not cover every capsule object exactly once")

    hashes = submission_hashes_for(capsule)
    pending = receipt["status"] == "pending"
    for entry in receipt["objects"]:
        if entry["source_id"] != entry["canonical_id"]:
            raise ExchangeError("uplift changed a supplied portable ID")
        if entry["source_submission_hash"] != hashes[entry["source_id"]]:
            raise ExchangeError(
                f"uplift submission hash mismatch for {entry['source_id']}"
            )
        if entry["object_kind"] != _kind_of(entry["source_id"]):
            raise ExchangeError(f"uplift object_kind mismatch for {entry['source_id']}")
        if pending:
            if entry["disposition"] != "pending" or entry["object_hash"] is not None:
                raise ExchangeError("pending uplift claims a completed admission")
        elif entry["disposition"] in {"admitted", "existing"} and not entry.get(
            "object_hash"
        ):
            raise ExchangeError("completed uplift admission is missing an object hash")
        auth = entry.get("producer_authentication")
        if auth == "verified":
            if not allow_verified_producer:
                raise ExchangeError(
                    "verified producer authentication requires the signed-producer-sync suite"
                )
            if not entry.get("producer_proof"):
                raise ExchangeError("verified producer authentication has no retained proof")
        if auth in {None, "verified"} and entry.get("producer_proof") is None and auth == "verified":
            raise ExchangeError("verified producer authentication has no retained proof")


def load_inventory(path: Path, *, schemas: SchemaSet, what: str) -> list[dict]:
    entries = json.loads(path.read_text(encoding="utf-8"))
    schemas.validate(SCHEMA_INVENTORY, entries, what=what)
    keys = {(entry["category"], entry["subject"]) for entry in entries}
    if len(keys) != len(entries):
        raise ExchangeError(f"{what} contains duplicate entries")
    for entry in entries:
        if entry["category"] not in INVENTORY_CATEGORIES:
            raise ExchangeError(f"{what} has unknown category {entry['category']!r}")
    return entries


def verify_downgrade_receipt(
    receipt: dict,
    *,
    capsule_root: str | Path,
    layered: LayeredRegistries,
    schemas: SchemaSet,
) -> None:
    """Validate a downgrade receipt's inventories, omissions, and opaque bytes."""
    root = Path(capsule_root)
    schemas.validate(SCHEMA_DOWNGRADE, receipt, what="downgrade receipt")
    if layered.level_rank(receipt["target_level"]) >= layered.level_rank(
        receipt["source_level"]
    ):
        raise ExchangeError("downgrade receipt does not move to a weaker level")
    if receipt["losslessness"] == "lossy" and not receipt["omissions"]:
        raise ExchangeError("lossy downgrade did not enumerate omissions")
    if receipt["losslessness"] == "lossless" and receipt["omissions"]:
        raise ExchangeError("lossless downgrade enumerated omissions")

    inventories: dict[str, dict[tuple[str, str], str]] = {}
    for name in ("source_inventory", "export_inventory"):
        ref = receipt[name]
        inventory_path = root / ref["path"]
        content = inventory_path.read_bytes()
        if raw_digest(content) != ref["digest"]:
            raise ExchangeError(f"downgrade {name} digest mismatch")
        entries = load_inventory(inventory_path, schemas=schemas, what=f"downgrade {name}")
        for entry in entries:
            if entry["category"] == "submission" and entry["subject"].startswith(
                "submission:urn:ccf:"
            ):
                continue
            artifact = (root / entry["subject"]).read_bytes()
            if raw_digest(artifact) != entry["digest"]:
                raise ExchangeError(
                    f"downgrade {name} artifact digest mismatch: {entry['subject']}"
                )
        inventories[name] = {
            (entry["category"], entry["subject"]): entry["digest"] for entry in entries
        }

    source_keys = set(inventories["source_inventory"])
    export_keys = set(inventories["export_inventory"])
    if not export_keys <= source_keys:
        raise ExchangeError("downgrade export inventory adds undeclared source material")
    for key in export_keys:
        if inventories["source_inventory"][key] != inventories["export_inventory"][key]:
            raise ExchangeError("downgrade changed an exported logical item digest")

    omission_keys = {(entry["category"], entry["subject"]) for entry in receipt["omissions"]}
    if len(omission_keys) != len(receipt["omissions"]):
        raise ExchangeError("downgrade receipt contains duplicate omissions")
    if omission_keys != source_keys - export_keys:
        raise ExchangeError(
            "downgrade omissions are not the exact source/export inventory difference"
        )
    for item in receipt["preserved_opaque"]:
        content = (root / item["path"]).read_bytes()
        if raw_digest(content) != item["digest"]:
            raise ExchangeError(
                f"downgrade opaque preservation digest mismatch: {item['path']}"
            )


def logical_submission_entry(submission: dict) -> dict:
    """Inventory row for an assertion that moved between containers."""
    return {
        "category": "submission",
        "subject": f"submission:{submission['id']}",
        "digest": submission_hash(submission),
    }


def write_inventory(path: Path, entries: list[dict], *, schemas: SchemaSet) -> dict:
    """Write a downgrade inventory and return its path/digest ref."""
    schemas.validate(SCHEMA_INVENTORY, entries, what="downgrade inventory")
    payload = json.dumps(entries, indent=2, ensure_ascii=False) + "\n"
    encoded = payload.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded)
    return {"path": path.name, "digest": raw_digest(encoded)}
