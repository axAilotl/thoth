"""CCF 0.2.0 layered-conformance tests.

Portable objects stay ``ccf/0.1.2``. These tests cover the additive
declaration, Capsule, receipt, and catalog-pin surfaces.
"""

from __future__ import annotations

import json

import pytest

from ccf import CCF_LAYER, CCF_LEVEL, CCF_SPEC, CCF_VERSION
from ccf.archive import DEFAULT_ACTIVE_PROFILES
from ccf.catalog import LayeredCatalog, SemanticCatalog
from ccf.capsule import CapsuleError, load_capsule, verify_capsule, write_capsule
from ccf.declaration import (
    THOTH_IMPLEMENTATION,
    THOTH_ROLES,
    build_thoth_declaration,
    validate_declaration,
)
from ccf.exchange import (
    ExchangeError,
    build_pending_uplift,
    verify_downgrade_receipt,
    verify_uplift_receipt,
)
from ccf.hashing import submission_hash
from ccf.layered import LayeredError, LayeredRegistries
from ccf.schemas import SchemaSet, is_ccf_uint64


@pytest.fixture(scope="module")
def layered(ccf_draft_root):
    return LayeredRegistries.load(ccf_draft_root)


@pytest.fixture(scope="module")
def layered_schemas(ccf_package_root, ccf_draft_root):
    return SchemaSet.load_layered(ccf_package_root, ccf_draft_root)


@pytest.fixture(scope="module")
def layered_catalog(ccf_package_root, ccf_draft_root):
    return LayeredCatalog.load(ccf_draft_root, ccf_package_root)


def test_portable_format_remains_012():
    assert CCF_SPEC == "ccf/0.1.2"
    assert CCF_VERSION == "0.1.2"
    assert CCF_LAYER == "0.2.0"
    assert CCF_LEVEL == "ccf-governed-archive-v1"


def test_draft_catalog_pins_exact_012(layered_catalog):
    assert layered_catalog.draft.format == "ccf.semantic-catalog/0.2.0"
    assert layered_catalog.draft.version == "0.2.0"
    assert layered_catalog.draft.entries_verified is True
    assert layered_catalog.base.format == "ccf.semantic-catalog/0.1.2"
    assert (
        layered_catalog.base_root
        == "sha256:992490912d2faffc1084d43041bd228d64265671ffa36840b30f62c0d1fea9e8"
    )
    assert (
        layered_catalog.root
        == "sha256:3fe9e54b016be93307be17479177eb666372e60d46b51f1b1d7cf9ba351abdc5"
    )


def test_draft_catalog_rejects_wrong_base_pin(ccf_draft_root, ccf_package_root, tmp_path):
    document = json.loads((ccf_draft_root / "semantic-catalog.json").read_text())
    document["base_catalogs"][0]["root"] = "sha256:" + "0" * 64
    (tmp_path / "semantic-catalog.json").write_text(json.dumps(document))
    # Copy schemas/registries so artifact verify can run if we pointed at tmp.
    # LayeredCatalog.load verifies both catalogs independently first.
    with pytest.raises(Exception):
        SemanticCatalog.from_document(document, package_root=ccf_draft_root)


def test_legacy_profiles_map_to_governed_archive(layered):
    mapped = layered.map_legacy_profiles(DEFAULT_ACTIVE_PROFILES)
    assert mapped.level == "ccf-governed-archive-v1"
    assert mapped.capabilities == ("ccf-signed-producer-sync-v1",)
    assert mapped.semantic_packs == ("ccf-continuity-pack-v1",)
    assert "ccf-archive-encryption-derived-v1" not in mapped.declared_features
    assert "ccf-object-erasure-v1" not in mapped.declared_features


def test_unmapped_profile_fails_closed(layered):
    with pytest.raises(LayeredError, match="unmapped"):
        layered.map_legacy_profiles(["ccf-not-a-profile"])


def test_thoth_declaration_is_honest(layered, layered_schemas, layered_catalog):
    declaration = build_thoth_declaration(
        layered=layered,
        catalog_roots=(layered_catalog.base_root, layered_catalog.root),
        schemas=layered_schemas,
    )
    assert declaration["implementation"] == THOTH_IMPLEMENTATION
    assert declaration["level"] == "ccf-governed-archive-v1"
    assert declaration["roles"] == list(THOTH_ROLES)
    assert declaration["portable_formats"] == ["ccf/0.1.2"]
    assert layered_catalog.base_root in declaration["semantic_catalog_roots"]
    assert "ccf-signed-producer-sync-v1" in declaration["capabilities"]
    assert "ccf-continuity-pack-v1" in declaration["capabilities"]
    assert "ccf-archive-encryption-derived-v1" not in declaration["capabilities"]
    assert "ccf-witnessed-integrity-v1" not in declaration["capabilities"]
    validate_declaration(declaration, layered=layered, schemas=layered_schemas)


def test_declaration_rejects_feature_below_level(layered, layered_schemas, layered_catalog):
    declaration = build_thoth_declaration(
        layered=layered,
        catalog_roots=(layered_catalog.base_root,),
        schemas=layered_schemas,
    )
    declaration["level"] = "ccf-exchange-v1"
    declaration["capabilities"] = ["ccf-object-erasure-v1"]
    with pytest.raises(LayeredError, match="requires"):
        validate_declaration(declaration, layered=layered, schemas=layered_schemas)


def test_ccf_uint64_format_boundaries():
    assert is_ccf_uint64("0") is True
    assert is_ccf_uint64("1") is True
    assert is_ccf_uint64(str(2**64 - 1)) is True
    assert is_ccf_uint64(str(2**64)) is False
    assert is_ccf_uint64("00") is False
    assert is_ccf_uint64("01") is False
    assert is_ccf_uint64("9" * 4301) is False
    assert is_ccf_uint64(1) is True  # type check owns non-strings


def test_uint64_enforced_on_producer_batch(ccf_package_root, layered_schemas):
    batch = json.loads(
        (ccf_package_root / "vectors" / "producer-batch.json").read_text()
    )["batch"]
    schema_id = "urn:ccf:schema:0.1.2:sync.producer-batch"
    layered_schemas.validate(schema_id, batch, what="producer batch")
    overflow = dict(batch, producer_sequence=str(2**64))
    with pytest.raises(Exception, match="producer_sequence|uint64|format"):
        layered_schemas.validate(schema_id, overflow, what="overflow batch")


def test_example_capsule_round_trip(
    ccf_capsule_example, layered, layered_schemas
):
    capsule = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    verify_capsule(
        capsule,
        layered=layered,
        schemas=layered_schemas,
        recipient_level="ccf-exchange-v1",
        recipient_capabilities=(),
    )
    assert capsule.manifest["root_record_id"] in {
        item["id"] for item in capsule.submissions
    }
    opaque_types = {value["type"] for value in capsule.opaque_values}
    assert "lineage.erasure_receipt" in opaque_types
    assert "org.example.future-governance" in opaque_types


def test_unregistered_type_cannot_activate(layered):
    with pytest.raises(LayeredError, match="unregistered"):
        layered.requirement_for_submission(
            {"submission_kind": "record", "type": "org.example.future-active"}
        )


def test_example_pending_uplift(ccf_capsule_example, layered, layered_schemas):
    capsule = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    receipt = json.loads((ccf_capsule_example / "uplift-receipt.json").read_text())
    verify_uplift_receipt(
        receipt, capsule=capsule, layered=layered, schemas=layered_schemas
    )
    built = build_pending_uplift(
        capsule,
        destination_level="ccf-verified-archive-v1",
        destination_archive_id=receipt["destination_archive_id"],
        created_at=receipt["created_at"],
        archive_resolution={
            entry["source_id"]: entry["archive_resolution"]
            for entry in receipt["objects"]
        },
        receipt_id=receipt["receipt_id"],
    )
    assert built["status"] == "pending"
    assert [entry["source_id"] for entry in built["objects"]] == [
        entry["source_id"] for entry in receipt["objects"]
    ]
    for left, right in zip(built["objects"], receipt["objects"]):
        assert left["source_submission_hash"] == right["source_submission_hash"]
        assert left["canonical_id"] == left["source_id"]
        assert left["object_hash"] is None
        assert left["producer_authentication"] == "absent"


def test_example_completed_uplift_does_not_invent_producer_auth(
    ccf_capsule_example, layered, layered_schemas
):
    capsule = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    receipt = json.loads(
        (ccf_capsule_example / "completed-uplift-receipt.json").read_text()
    )
    verify_uplift_receipt(
        receipt, capsule=capsule, layered=layered, schemas=layered_schemas
    )
    assert all(
        entry["producer_authentication"] != "verified" for entry in receipt["objects"]
    )


def test_verified_producer_claim_refused_without_capability(
    ccf_capsule_example, layered, layered_schemas
):
    capsule = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    receipt = json.loads((ccf_capsule_example / "uplift-receipt.json").read_text())
    receipt["objects"][0]["producer_authentication"] = "verified"
    receipt["objects"][0]["producer_proof"] = {
        "profile": "ccf-signed-producer-sync-v1",
        "credential_id": "urn:ccf:credential:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "batch_id": "urn:ccf:batch:bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "proof_digest": "sha256:" + "ab" * 32,
    }
    with pytest.raises(ExchangeError, match="signed-producer-sync"):
        verify_uplift_receipt(
            receipt, capsule=capsule, layered=layered, schemas=layered_schemas
        )


def test_uplift_cannot_rewrite_ids(ccf_capsule_example, layered, layered_schemas):
    capsule = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    receipt = json.loads((ccf_capsule_example / "uplift-receipt.json").read_text())
    receipt["objects"][0]["canonical_id"] = "urn:ccf:record:00000000-0000-4000-8000-000000000000"
    with pytest.raises(ExchangeError, match="portable ID"):
        verify_uplift_receipt(
            receipt, capsule=capsule, layered=layered, schemas=layered_schemas
        )


def test_example_downgrade_receipt(ccf_capsule_example, layered, layered_schemas):
    receipt = json.loads((ccf_capsule_example / "downgrade-receipt.json").read_text())
    verify_downgrade_receipt(
        receipt,
        capsule_root=ccf_capsule_example,
        layered=layered,
        schemas=layered_schemas,
    )


def test_activate_stream_above_recipient_refused(
    ccf_capsule_example, layered, layered_schemas
):
    capsule = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    # The opaque governed stream is preserve_opaque; flip it to activate.
    for stream in capsule.streams:
        if stream.content_role == "opaque":
            stream.spec["handling"] = "activate"
    with pytest.raises(CapsuleError, match="exceeds recipient"):
        verify_capsule(
            capsule,
            layered=layered,
            schemas=layered_schemas,
            recipient_level="ccf-exchange-v1",
            recipient_capabilities=(),
        )


def test_write_capsule_preserves_unknown_extension(
    tmp_path, ccf_capsule_example, layered, layered_schemas
):
    source = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    records = [
        value
        for stream in source.streams
        if stream.path == "submissions/records.ndjson"
        for value in stream.values
    ]
    links = [
        value
        for stream in source.streams
        if stream.path == "submissions/links.ndjson"
        for value in stream.values
    ]
    opaque = {
        "opaque/governance-material.ndjson": next(
            stream.data for stream in source.streams if stream.content_role == "opaque"
        )
    }
    written = write_capsule(
        tmp_path / "reexport",
        manifest=source.manifest,
        submission_streams={
            "submissions/records.ndjson": records,
            "submissions/links.ndjson": links,
        },
        opaque_streams=opaque,
        schemas=layered_schemas,
    )
    verify_capsule(
        written,
        layered=layered,
        schemas=layered_schemas,
        recipient_level="ccf-exchange-v1",
    )
    root = next(
        item for item in written.submissions if item["id"] == source.manifest["root_record_id"]
    )
    original = next(
        item for item in source.submissions if item["id"] == source.manifest["root_record_id"]
    )
    assert root["extensions"] == original["extensions"]
    assert submission_hash(root) == submission_hash(original)


def test_archive_declaration_and_capsule_preview(
    ccf_settings, tmp_path, ccf_package_root, ccf_capsule_example
):
    from tests.ccf_helpers import make_rig

    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    declaration = rig.archive.implementation_declaration()
    assert declaration["level"] == "ccf-governed-archive-v1"
    preview = rig.archive.preview_capsule(ccf_capsule_example)
    assert preview["uplift"]["status"] == "pending"
    assert preview["uplift"]["destination_archive_id"] == rig.archive.archive_id
    assert preview["uplift"]["destination_level"] == "ccf-governed-archive-v1"
    assert all(
        entry["source_id"] == entry["canonical_id"]
        for entry in preview["uplift"]["objects"]
    )
    assert all(
        entry["producer_authentication"] == "absent"
        for entry in preview["uplift"]["objects"]
    )
