"""Downgrade source package verification for CCF 0.2.0 exchange receipts.

A downgrade receipt binds to a real 0.1.2 Verified source package. This module
loads and verifies the source identity, commit chain, members, and selected
producer batch so that a lossless downgrade cannot be claimed from an empty or
unrelated source tree.
"""

from __future__ import annotations

import json
from pathlib import Path

from ccf.capsule import CapsuleError, _enumerate_package_files
from ccf.hashing import producer_batch_hash
from ccf.ids import parse_id
from ccf.schemas import CcfSchemaError, SchemaSet
from ccf.sync.packio import PackReader, load_pack_objects
from ccf.sync.verify import PackVerificationError, verify_commit_chain, verify_pack_object

SCHEMA_PRODUCER_BATCH = "urn:ccf:schema:0.1.2:sync.producer-batch"

# Required physical files in a downgrade source package, mirroring the
# vendored 0.2.0 oracle in spec/ccf/0.2.0/tools/validate-exchange.py.
_REQUIRED_SOURCE_FILES = frozenset(
    {
        "source-identity.json",
        "objects/records.ndjson",
        "objects/links.ndjson",
        "objects/blobs.ndjson",
        "integrity/commits.ndjson",
        "integrity/members.ndjson",
        "origin-index.ndjson",
    }
)

_SOURCE_IDENTITY_FIELDS = {
    "format",
    "archive_id",
    "epoch_id",
    "genesis_commit_hash",
    "head_commit_hash",
    "head_sequence",
    "semantic_catalog_root",
    "trusted_genesis_signer_key_id",
    "trusted_genesis_signer_public_key",
}


class SourcePackageError(ValueError):
    """Raised when a downgrade source package is missing or dishonest."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SourcePackageError(message)


def _read_ndjson(path: Path, *, what: str) -> list[dict]:
    if not path.is_file():
        raise SourcePackageError(f"{what} missing: {path.relative_to(path.parents[1]).as_posix()}")
    records: list[dict] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise SourcePackageError(f"{what} line {lineno} is not valid JSON") from exc
    return records


def _load_identity(source_dir: Path) -> dict:
    path = source_dir / "source-identity.json"
    if not path.is_file():
        raise SourcePackageError("downgrade source identity missing: source-identity.json")
    identity = json.loads(path.read_text(encoding="utf-8"))
    _require(isinstance(identity, dict), "downgrade source identity is not a JSON object")
    _require(
        identity.get("format") == "ccf.verified-source-identity/0.2.0",
        "downgrade source identity format mismatch",
    )
    _require(
        set(identity) == _SOURCE_IDENTITY_FIELDS,
        "downgrade source identity has missing or unknown fields",
    )
    parse_id(identity["archive_id"])
    parse_id(identity["epoch_id"])
    return identity


def _load_producer_batch(source_dir: Path, schemas: SchemaSet) -> dict:
    batch_dir = source_dir / "producer-batches"
    if not batch_dir.is_dir():
        raise SourcePackageError("downgrade source producer-batches directory missing")
    batches = list(batch_dir.glob("*.json"))
    _require(
        len(batches) == 1,
        "downgrade source must contain exactly one selected producer batch",
    )
    batch = json.loads(batches[0].read_text(encoding="utf-8"))
    try:
        schemas.validate(
            SCHEMA_PRODUCER_BATCH, batch, what="downgrade source producer batch"
        )
    except CcfSchemaError as exc:
        raise SourcePackageError(
            f"downgrade source producer batch is not a valid producer batch: {exc}"
        ) from exc
    _require(
        producer_batch_hash(batch) == batch["batch_hash"],
        "downgrade source producer batch_hash does not match recomputed batch hash",
    )
    return batch


def load_verified_source_package(
    source_dir: Path,
    *,
    schemas: SchemaSet,
) -> dict:
    """Load and verify a downgrade source package.

    Returns a dict with keys:
      - ``identity``: the verified source-identity document
      - ``headers``: object_id -> object header dict
      - ``commits``: list of commit summary dicts
      - ``members``: list of member summary dicts
      - ``objects``: dict of PackObject
      - ``origin_index``: object_id -> origin row dict
      - ``batch``: the selected producer batch document
      - ``batch_submissions``: flattened list of batch submissions
    """
    try:
        _enumerate_package_files(source_dir)
    except CapsuleError as exc:
        raise SourcePackageError(f"downgrade source package tree invalid: {exc}") from exc

    identity = _load_identity(source_dir)

    for rel in _REQUIRED_SOURCE_FILES:
        path = source_dir / rel
        if not path.is_file():
            raise SourcePackageError(f"downgrade source required file missing: {rel}")

    try:
        with PackReader(source_dir) as reader:
            contents = load_pack_objects(reader)
    except Exception as exc:
        raise SourcePackageError(
            f"downgrade source package cannot be loaded: {exc}"
        ) from exc

    headers = {obj.object_id: obj.header for obj in contents.objects.values()}

    commits = _read_ndjson(source_dir / "integrity" / "commits.ndjson", what="source commits")
    members = _read_ndjson(source_dir / "integrity" / "members.ndjson", what="source members")

    try:
        chain = verify_commit_chain(commits, members, contents.objects)
    except PackVerificationError as exc:
        raise SourcePackageError(f"downgrade source commit chain invalid: {exc}") from exc

    _require(
        chain["archive_id"] == identity["archive_id"],
        "downgrade source chain archive_id does not match source identity",
    )
    _require(
        chain["epoch_id"] == identity["epoch_id"],
        "downgrade source chain epoch_id does not match source identity",
    )
    _require(
        chain["semantic_catalog_root"] == identity["semantic_catalog_root"],
        "downgrade source chain semantic_catalog_root does not match source identity",
    )
    _require(
        chain["genesis_commit_hash"] == identity["genesis_commit_hash"],
        "downgrade source genesis commit hash does not match source identity",
    )
    _require(
        chain["head_commit_hash"] == identity["head_commit_hash"],
        "downgrade source head commit hash does not match source identity",
    )
    _require(
        chain["head_sequence"] == identity["head_sequence"],
        "downgrade source head sequence does not match source identity",
    )
    _require(
        chain["signer_public_key"] == identity["trusted_genesis_signer_public_key"],
        "downgrade source chain signer does not match trusted genesis signer",
    )
    _require(
        chain["signer_key_id"] == identity["trusted_genesis_signer_key_id"],
        "downgrade source chain signer_key_id does not match trusted genesis signer key id",
    )

    # Compartment availability is derived from the pack contents, never from
    # manifest declarations. Objects missing a committed compartment are
    # "unavailable" for the exchange seam; every present compartment is still
    # commitment-checked against the journal-authenticated header.
    unavailable_ids: set[str] = set()
    for obj in contents.objects.values():
        header = obj.header
        if obj.structural is None:
            unavailable_ids.add(obj.object_id)
        elif header.get("semantic_commitment") is not None and obj.semantic is None:
            unavailable_ids.add(obj.object_id)

    for obj in contents.objects.values():
        try:
            verify_pack_object(
                obj,
                blob_data=contents.blob_data.get(obj.object_id),
                unavailable_ids=unavailable_ids,
            )
        except PackVerificationError as exc:
            raise SourcePackageError(
                f"downgrade source object {obj.object_id} failed verification: {exc}"
            ) from exc

    origin_index = {}
    origin_path = source_dir / "origin-index.ndjson"
    if origin_path.is_file():
        for row in _read_ndjson(origin_path, what="source origin index"):
            object_id = row.get("object_id")
            if object_id in origin_index:
                raise SourcePackageError(f"duplicate origin index row for {object_id}")
            origin_index[object_id] = row

    batch = _load_producer_batch(source_dir, schemas)
    batch_submissions = [
        item
        for field in ("records", "links", "blobs")
        for item in batch.get(field, [])
    ]
    _require(bool(batch_submissions), "downgrade source producer batch has no submissions")
    batch_ids = {submission["id"] for submission in batch_submissions}
    missing_from_headers = batch_ids - set(headers)
    _require(
        not missing_from_headers,
        "downgrade producer batch references objects absent from source headers: "
        f"{sorted(missing_from_headers)}",
    )

    return {
        "identity": identity,
        "headers": headers,
        "commits": commits,
        "members": members,
        "objects": contents.objects,
        "origin_index": origin_index,
        "batch": batch,
        "batch_submissions": batch_submissions,
        "unavailable_ids": unavailable_ids,
        "blob_data": contents.blob_data,
    }


def source_compartment_subject(object_id: str, compartment: str) -> str:
    """Inventory subject for a selected source compartment."""
    kind = parse_id(object_id).kind
    plural = f"{kind}s"
    uuid = object_id.rsplit(":", 1)[1]
    return f"downgrade-source/compartments/{plural}/{uuid}.{compartment}.json"
