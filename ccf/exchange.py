"""CCF 0.2.0 uplift and downgrade receipts.

An uplift receipt maps each source submission ID to the same canonical ID
and never silently upgrades unsigned material to verified producer evidence.
A downgrade receipt enumerates the exact source-minus-export inventory.
"""

from __future__ import annotations

import json
from pathlib import Path

from ccf.capsule import (
    Capsule,
    CapsuleError,
    SCHEMA_CAPSULE,
    _enumerate_package_files,
    load_capsule,
    submission_hashes_for,
    _resolve_package_path,
)
from ccf.downgrade_source import (
    SourcePackageError,
    load_verified_source_package,
    source_compartment_subject,
)
from ccf.hashing import submission_hash
from ccf.ids import generate_id, parse_id
from ccf.layered import LayeredError, LayeredRegistries, raw_digest
from ccf.sync.verify import PackVerificationError, verify_pack_object
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


def _secure_path(
    root: Path, rel: str, *, must_exist: bool = True, must_be_file: bool = True
) -> Path:
    """Resolve ``rel`` under ``root`` and re-raise containment errors as ExchangeError."""
    try:
        return _resolve_package_path(
            root, rel, must_exist=must_exist, must_be_file=must_be_file
        )
    except CapsuleError as exc:
        raise ExchangeError(str(exc)) from exc


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


def _load_source_dir(root: Path) -> Path:
    """Resolve and verify the contained downgrade-source directory."""
    source_dir = _secure_path(
        root, "downgrade-source", must_exist=False, must_be_file=False
    )
    if not source_dir.exists():
        raise ExchangeError(
            "downgrade source directory missing: downgrade-source"
        )
    if not source_dir.is_dir():
        raise ExchangeError(
            "downgrade source path is not a directory: downgrade-source"
        )
    return source_dir


def _load_export_dir(root: Path) -> Path:
    """Resolve and verify the contained downgrade-export directory."""
    export_dir = _secure_path(
        root, "downgrade-export", must_exist=False, must_be_file=False
    )
    if not export_dir.exists():
        raise ExchangeError(
            "downgrade export capsule directory missing: downgrade-export"
        )
    if not export_dir.is_dir():
        raise ExchangeError(
            "downgrade export capsule path is not a directory: downgrade-export"
        )
    return export_dir


def _load_export_capsule(
    root: Path,
    receipt: dict,
    export_dir: Path,
    actual_export_files: set[str],
    *,
    layered: LayeredRegistries,
    schemas: SchemaSet,
) -> Capsule:
    """Verify the receipt-bound export Capsule manifest and physical files.

    ``export_dir`` and ``actual_export_files`` must come from the secure
    directory resolution and symlink-free tree preflight performed before any
    direct read beneath the export tree.
    """
    manifest_path = _secure_path(
        export_dir, "manifest.json", must_exist=False, must_be_file=False
    )
    if not manifest_path.is_file():
        raise ExchangeError(
            "downgrade export capsule manifest missing: downgrade-export/manifest.json"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    schemas.validate(SCHEMA_CAPSULE, manifest, what="downgrade export capsule manifest")
    layered.level(manifest["level"])
    if manifest["pack_id"] != receipt["export_pack_id"]:
        raise ExchangeError("downgrade export pack_id does not bind the receipt")
    if manifest["level"] != receipt["target_level"]:
        raise ExchangeError("downgrade export level does not match receipt")
    if manifest["custody"]["losslessness"] != receipt["losslessness"]:
        raise ExchangeError("downgrade export losslessness does not match receipt")
    if manifest["custody"]["omissions"] != receipt["omissions"]:
        raise ExchangeError("downgrade export omissions do not match receipt")

    stream_paths = [stream["path"] for stream in manifest["streams"]]
    if len(stream_paths) != len(set(stream_paths)):
        raise ExchangeError("downgrade export capsule has duplicate stream paths")

    expected_files = {"manifest.json", *stream_paths}
    if expected_files != actual_export_files:
        raise ExchangeError(
            "downgrade export capsule has unmanifested or missing files"
        )

    for stream in manifest["streams"]:
        rel = stream["path"]
        stream_path = _secure_path(export_dir, rel)
        content = stream_path.read_bytes()
        if raw_digest(content) != stream["digest"]:
            raise ExchangeError(
                f"downgrade export stream digest mismatch: {rel}"
            )
        if str(len(content)) != stream["byte_length"]:
            raise ExchangeError(
                f"downgrade export stream byte_length mismatch: {rel}"
            )

    return load_capsule(export_dir, schemas=schemas)


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

    # Resolve the contained downgrade directories and preflight their trees
    # before any direct read beneath them. This rejects symlinked
    # integrity/, producer-batches/, manifest.json, etc. before bytes are
    # trusted, even when the inventory does not directly reference the link.
    source_dir = _load_source_dir(root)
    export_dir = _load_export_dir(root)
    try:
        actual_source_files = _enumerate_package_files(source_dir)
    except CapsuleError as exc:
        raise ExchangeError(f"downgrade source package tree invalid: {exc}") from exc
    try:
        actual_export_files = _enumerate_package_files(export_dir)
    except CapsuleError as exc:
        raise ExchangeError(f"downgrade export package tree invalid: {exc}") from exc

    inventories: dict[str, dict[tuple[str, str], str]] = {}
    for name in ("source_inventory", "export_inventory"):
        ref = receipt[name]
        inventory_path = _secure_path(root, ref["path"])
        content = inventory_path.read_bytes()
        if raw_digest(content) != ref["digest"]:
            raise ExchangeError(f"downgrade {name} digest mismatch")
        entries = load_inventory(inventory_path, schemas=schemas, what=f"downgrade {name}")
        for entry in entries:
            subject = entry["subject"]
            if entry["category"] == "submission" and subject.startswith(
                "submission:urn:ccf:"
            ):
                continue
            if name == "source_inventory" and not subject.startswith("downgrade-source/"):
                raise ExchangeError(
                    f"downgrade source inventory references outside source package: {subject}"
                )
            if name == "export_inventory" and not subject.startswith("downgrade-export/"):
                raise ExchangeError(
                    f"downgrade export inventory references outside export package: {subject}"
                )
            artifact_path = _secure_path(root, subject)
            artifact = artifact_path.read_bytes()
            if raw_digest(artifact) != entry["digest"]:
                raise ExchangeError(
                    f"downgrade {name} artifact digest mismatch: {subject}"
                )
        inventories[name] = {
            (entry["category"], entry["subject"]): entry["digest"] for entry in entries
        }

    # Prove the source inventory completely covers a valid downgrade source
    # package. The source directory is required (not optional), must contain a
    # real Verified archive identity/commit chain/members/producer batch, and a
    # physical proof cannot be hidden from both inventory and omissions.
    try:
        source_package = load_verified_source_package(source_dir, schemas=schemas)
    except SourcePackageError as exc:
        raise ExchangeError(str(exc)) from exc

    physical_source_inventory = {
        subject.removeprefix("downgrade-source/")
        for (category, subject) in inventories["source_inventory"]
        if category != "submission" and subject.startswith("downgrade-source/")
    }
    if physical_source_inventory != actual_source_files:
        raise ExchangeError(
            "downgrade source inventory does not exactly cover physical source package"
        )

    for commit in source_package["commits"]:
        subject = source_compartment_subject(commit["record_id"], "structural")
        if ("compartment", subject) not in inventories["source_inventory"]:
            raise ExchangeError(
                f"downgrade source inventory omits commit compartment for {commit['record_id']}"
            )

    # Every logical source submission entry must be an exact selected
    # producer-batch submission (a subset is allowed because the fixture only
    # inventories selected/exported assertions).
    batch_by_id = {
        submission["id"]: submission
        for submission in source_package["batch_submissions"]
    }
    for (category, subject), digest in inventories["source_inventory"].items():
        if category == "submission" and subject.startswith("submission:"):
            submission_id = subject.removeprefix("submission:")
            batch_submission = batch_by_id.get(submission_id)
            if batch_submission is None:
                raise ExchangeError(
                    f"downgrade source inventory submission is not a selected batch submission: {submission_id}"
                )
            if submission_hash(batch_submission) != digest:
                raise ExchangeError(
                    f"downgrade source inventory submission hash mismatch: {submission_id}"
                )

    # Bind the export inventory to the already verified export Capsule before
    # any subtraction/omission arithmetic: logical submission entries must
    # exactly cover the exported submissions (ID + JCS hash), the export must
    # contain exactly one submission stream, and every exported assertion must
    # be an exact source producer-batch submission bound to its canonical
    # journal-authenticated object.
    export_capsule = _load_export_capsule(
        root,
        receipt,
        export_dir,
        actual_export_files,
        layered=layered,
        schemas=schemas,
    )
    submission_streams = [
        stream for stream in export_capsule.streams if stream.content_role == "submissions"
    ]
    if len(submission_streams) != 1:
        raise ExchangeError(
            "downgrade export Capsule must contain exactly one submission stream"
        )
    exported_submissions = submission_streams[0].values
    if not exported_submissions:
        raise ExchangeError("downgrade export submission stream is empty")
    if export_capsule.manifest["root_record_id"] != exported_submissions[0]["id"]:
        raise ExchangeError("downgrade export root is not its selected source assertion")

    expected_logical_export = {
        ("submission", f"submission:{submission['id']}"): submission_hash(submission)
        for submission in exported_submissions
    }
    actual_logical_export = {
        key: digest
        for key, digest in inventories["export_inventory"].items()
        if key[0] == "submission"
    }
    if actual_logical_export != expected_logical_export:
        raise ExchangeError(
            "downgrade export inventory does not exactly cover exported submissions"
        )

    for submission in exported_submissions:
        batch_submission = batch_by_id.get(submission["id"])
        if batch_submission is None or batch_submission != submission:
            raise ExchangeError(
                "downgrade Exchange assertions are not exact source batch submissions"
            )
        try:
            requirement = layered.requirement_for_submission(submission)
        except LayeredError as exc:
            raise ExchangeError(str(exc)) from exc
        if requirement["minimum_level"] != "ccf-exchange-v1":
            raise ExchangeError(
                f"downgrade export activates {submission['id']} above Exchange level"
            )

        object_id = submission["id"]
        if object_id in source_package["unavailable_ids"]:
            raise ExchangeError(
                f"downgrade export submission is not fully available in source package: {object_id}"
            )
        source_obj = source_package["objects"][object_id]
        try:
            verify_pack_object(
                source_obj,
                blob_data=source_package["blob_data"].get(object_id),
            )
        except PackVerificationError as exc:
            raise ExchangeError(
                f"downgrade export submission source object verification failed: {exc}"
            ) from exc

        semantic_content = (source_obj.semantic or {}).get("content") or {}
        producer_evidence = semantic_content.get("producer_evidence")
        expected_evidence = {
            "batch_id": source_package["batch"]["batch_id"],
            "credential_id": source_package["batch"]["credential_id"],
            "producer_sequence": source_package["batch"]["producer_sequence"],
            "submission_hash": submission_hash(batch_submission),
        }
        if producer_evidence != expected_evidence:
            raise ExchangeError(
                f"downgrade export submission source object producer evidence mismatch: {object_id}"
            )

        header = source_package["headers"][submission["id"]]
        structural_subject = source_compartment_subject(submission["id"], "structural")
        if ("compartment", structural_subject) not in inventories["source_inventory"]:
            raise ExchangeError(
                f"downgrade source inventory omits structural compartment for {submission['id']}"
            )
        if header.get("semantic_commitment") is not None:
            semantic_subject = source_compartment_subject(submission["id"], "semantic")
            if ("compartment", semantic_subject) not in inventories["source_inventory"]:
                raise ExchangeError(
                    f"downgrade source inventory omits semantic compartment for {submission['id']}"
                )

        origin = submission.get("origin")
        if origin is not None:
            row = source_package["origin_index"].get(submission["id"])
            if row is None or any(
                row[field] != origin[field]
                for field in ("source_id", "native_id", "revision")
            ):
                raise ExchangeError(
                    f"downgrade source origin tuple mismatch for {submission['id']}"
                )

    for (category, subject), digest in inventories["export_inventory"].items():
        if category == "submission":
            continue
        artifact_path = _secure_path(root, subject)
        artifact = artifact_path.read_bytes()
        if raw_digest(artifact) != digest:
            raise ExchangeError(
                f"downgrade export inventory artifact digest mismatch: {subject}"
            )

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
        opaque_path = _secure_path(root, item["path"])
        content = opaque_path.read_bytes()
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
