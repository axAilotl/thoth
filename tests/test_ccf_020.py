"""CCF 0.2.0 layered-conformance tests.

Portable objects stay ``ccf/0.1.2``. These tests cover the additive
declaration, Capsule, receipt, and catalog-pin surfaces.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from ccf import CCF_LAYER, CCF_LEVEL, CCF_SPEC, CCF_VERSION
from ccf.archive import Archive, ArchiveError, DEFAULT_ACTIVE_PROFILES
from ccf.catalog import LayeredCatalog, SemanticCatalog
from ccf.capsule import (
    CapsuleError,
    _enumerate_package_files,
    load_capsule,
    verify_capsule,
    write_capsule,
)
from ccf.downgrade_source import SourcePackageError, load_verified_source_package
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
from ccf.hashing import producer_batch_hash, submission_hash
from ccf.layered import LayeredError, LayeredRegistries, raw_digest
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


# ---------------------------------------------------------------------------
# Regression tests for thoth-h0f.1 review blockers
# ---------------------------------------------------------------------------


def _copy_capsule_example(source: Path, dest: Path) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(source, dest)


def test_archive_open_rejects_mismatched_catalog_root(
    ccf_settings, tmp_path, ccf_package_root, monkeypatch
):
    """An archive opened against a current package whose root differs from the
    genesis-pinned root must fail closed instead of silently adopting the new
    semantics.
    """
    from tests.ccf_helpers import make_rig

    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    real_catalog = SemanticCatalog.load(ccf_package_root)
    fake_catalog = SemanticCatalog(dict(real_catalog.document), entries_verified=True)
    fake_catalog.root = "sha256:" + "0" * 64
    monkeypatch.setattr(SemanticCatalog, "load", lambda package_root: fake_catalog)
    with pytest.raises(ArchiveError, match="semantic catalog root mismatch"):
        Archive.open(
            ccf_settings,
            package_root=ccf_package_root,
            archive_key_path=rig.archive_key_path,
        )


def test_archive_declaration_uses_pinned_catalog_root(
    ccf_settings, tmp_path, ccf_package_root
):
    """The implementation declaration must publish the archive's pinned
    semantic-catalog root, not merely whatever root is on disk.
    """
    from tests.ccf_helpers import make_rig

    rig = make_rig(ccf_settings, tmp_path, ccf_package_root)
    declaration = rig.archive.implementation_declaration()
    assert rig.archive.semantic_catalog_root in declaration["semantic_catalog_roots"]


def _mutated_downgrade_example(source: Path, tmp_path: Path, mutate):
    root = tmp_path / "mutated-downgrade"
    _copy_capsule_example(source, root)
    receipt = json.loads((root / "downgrade-receipt.json").read_text())
    mutate(receipt, root)
    (root / "downgrade-receipt.json").write_text(
        json.dumps(receipt, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return root, receipt


def test_downgrade_receipt_rejects_inventory_path_escape(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    def mutate(receipt, root):
        receipt["source_inventory"]["path"] = "../evil-inventory.json"

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="path is not canonical relative POSIX"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_receipt_rejects_artifact_subject_escape(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    def mutate(receipt, root):
        bad_subject = "../downgrade-source/source-identity.json"
        for entry in receipt["omissions"]:
            if entry["subject"].endswith("source-identity.json"):
                entry["subject"] = bad_subject
                break
        entries = json.loads((root / "downgrade-source-inventory.json").read_text())
        for entry in entries:
            if entry["subject"].endswith("source-identity.json"):
                entry["subject"] = bad_subject
                break
        modified = json.dumps(entries, indent=2, ensure_ascii=False) + "\n"
        (root / "downgrade-source-inventory.json").write_text(
            modified, encoding="utf-8"
        )
        receipt["source_inventory"]["digest"] = raw_digest(
            modified.encode("utf-8")
        )

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="downgrade source inventory references outside source package"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_receipt_rejects_symlink_escape(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    def mutate(receipt, root):
        target = tmp_path / "outside-secret.json"
        target.write_text("{}\n", encoding="utf-8")
        link = root / "symlink-inventory.json"
        link.symlink_to(target)
        receipt["source_inventory"]["path"] = "symlink-inventory.json"
        receipt["source_inventory"]["digest"] = raw_digest(target.read_bytes())

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="path escapes root"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_receipt_rejects_directory_where_file_expected(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    def mutate(receipt, root):
        inventory_path = root / "downgrade-source-inventory.json"
        inventory_path.unlink()
        inventory_path.mkdir()

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="path is not a file"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_receipt_rejects_hidden_source_file(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A physical proof hidden under downgrade-source/ must be caught by the
    source-inventory coverage check; it cannot be omitted from both inventory
    and receipt omissions.
    """
    def mutate(receipt, root):
        (root / "downgrade-source" / "hidden-proof.json").write_text(
            "{}", encoding="utf-8"
        )

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="source inventory does not exactly cover"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_receipt_rejects_hidden_export_file(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A hidden file in the export Capsule must be caught by manifest/physical
    coverage, not silently ignored.
    """
    def mutate(receipt, root):
        (root / "downgrade-export" / "hidden-proof.json").write_text(
            "{}", encoding="utf-8"
        )

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="unmanifested or missing files"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_write_capsule_rejects_escape_before_write(
    tmp_path, ccf_capsule_example, layered_schemas
):
    """write_capsule must validate stream paths before any filesystem write so
    a ``../`` path cannot create a partial out-of-root file or the output
    directory itself.
    """
    source = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    manifest = dict(source.manifest)
    manifest["streams"] = [
        {
            "path": "../escaped.ndjson",
            "content_role": "submissions",
            "handling": "activate",
            "media_type": "application/x-ndjson",
            "activation_requirements": {
                "minimum_level": "ccf-exchange-v1",
                "capabilities": [],
            },
            "digest": "sha256:" + "0" * 64,
            "byte_length": "0",
            "required": True,
        }
    ]
    out = tmp_path / "escape-attempt"
    escaped = tmp_path / "escaped.ndjson"
    with pytest.raises(CapsuleError, match="path is not canonical relative POSIX"):
        write_capsule(
            out,
            manifest=manifest,
            submission_streams={"../escaped.ndjson": []},
        )
    assert not escaped.exists()
    assert not out.exists()


@pytest.mark.parametrize(
    "bad_path",
    [
        "../escaped.ndjson",
        "a/./b.ndjson",
        "a//b.ndjson",
        "./a.ndjson",
        "a/b/",
        "/absolute.ndjson",
    ],
)
def test_resolve_package_path_rejects_non_canonical_paths(bad_path, tmp_path):
    from ccf.capsule import _resolve_package_path

    root = tmp_path / "capsule"
    root.mkdir()
    with pytest.raises(CapsuleError, match="path is not canonical relative POSIX"):
        _resolve_package_path(root, bad_path, must_exist=False, must_be_file=False)


@pytest.mark.parametrize(
    "good_path",
    ["a.ndjson", "a/b.ndjson", "submissions/records.ndjson"],
)
def test_resolve_package_path_accepts_canonical_paths(good_path, tmp_path):
    from ccf.capsule import _resolve_package_path

    root = tmp_path / "capsule"
    root.mkdir()
    resolved = _resolve_package_path(
        root, good_path, must_exist=False, must_be_file=False
    )
    assert resolved == root / good_path


def test_downgrade_receipt_rejects_missing_export_capsule(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A downgrade receipt that declares export_pack_id must bind a physical
    export Capsule; absence of downgrade-export/ must fail closed.
    """
    def mutate(receipt, root):
        shutil.rmtree(root / "downgrade-export")

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade export capsule directory missing"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_receipt_rejects_missing_export_manifest(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    def mutate(receipt, root):
        (root / "downgrade-export" / "manifest.json").unlink()

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade export capsule manifest missing"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_invented_logical_submission(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """An invented submission entry added to both inventories must be rejected
    because the bound export Capsule does not contain it.
    """
    invented_id = "urn:ccf:record:00000000-0000-4000-8000-000000000000"
    invented_entry = {
        "category": "submission",
        "subject": f"submission:{invented_id}",
        "digest": "sha256:" + "0" * 64,
    }

    def mutate(receipt, root):
        for name, ref in (
            ("downgrade-source-inventory.json", "source_inventory"),
            ("downgrade-export-inventory.json", "export_inventory"),
        ):
            entries = json.loads((root / name).read_text())
            entries.append(invented_entry)
            encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
            (root / name).write_bytes(encoded)
            receipt[ref]["digest"] = raw_digest(encoded)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade source inventory submission is not a selected batch submission"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_missing_source_directory(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A receipt claiming a lossless downgrade with no downgrade-source/
    directory and empty omissions must fail closed with zero source evidence.
    """
    def mutate(receipt, root):
        single_entry = next(
            entry
            for entry in json.loads((root / "downgrade-export-inventory.json").read_text())
            if entry["category"] == "submission"
        )
        for name, ref in (
            ("downgrade-source-inventory.json", "source_inventory"),
            ("downgrade-export-inventory.json", "export_inventory"),
        ):
            encoded = json.dumps([single_entry], indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
            (root / name).write_bytes(encoded)
            receipt[ref]["digest"] = raw_digest(encoded)
        shutil.rmtree(root / "downgrade-source")
        receipt["losslessness"] = "lossless"
        receipt["omissions"] = []

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade source directory missing"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_source_proof_copied_into_export_inventory(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A physical downgrade-source proof must not be copied into the export
    inventory to hide an omission from both inventories.
    """
    def mutate(receipt, root):
        source_proof = next(
            entry
            for entry in json.loads((root / "downgrade-source-inventory.json").read_text())
            if entry["category"] != "submission" and entry["subject"].startswith("downgrade-source/")
        )
        entries = json.loads((root / "downgrade-export-inventory.json").read_text())
        entries.append(source_proof)
        encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        (root / "downgrade-export-inventory.json").write_bytes(encoded)
        receipt["export_inventory"]["digest"] = raw_digest(encoded)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade export inventory references outside export package"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_symlink_source_directory(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A symlinked downgrade-source/ pointing outside the capsule root must be
    rejected by the containment primitive.
    """
    def mutate(receipt, root):
        external = tmp_path / "external-source"
        shutil.copytree(root / "downgrade-source", external)
        shutil.rmtree(root / "downgrade-source")
        (root / "downgrade-source").symlink_to(external)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="path escapes root"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_mismatched_logical_submission_hash(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A logical submission entry in the export inventory with a mismatched
    JCS submission hash must fail closed.
    """
    def mutate(receipt, root):
        entries = json.loads((root / "downgrade-export-inventory.json").read_text())
        for entry in entries:
            if entry["category"] == "submission":
                entry["digest"] = "sha256:" + "0" * 64
                break
        encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        (root / "downgrade-export-inventory.json").write_bytes(encoded)
        receipt["export_inventory"]["digest"] = raw_digest(encoded)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade export inventory does not exactly cover exported submissions"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_empty_source_with_logical_only_inventory(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """An empty recreated downgrade-source directory plus a logical-only
    source inventory must not support a lossless downgrade claim.
    """
    def mutate(receipt, root):
        entries = [
            entry
            for entry in json.loads((root / "downgrade-source-inventory.json").read_text())
            if entry["category"] == "submission"
        ]
        encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        (root / "downgrade-source-inventory.json").write_bytes(encoded)
        receipt["source_inventory"]["digest"] = raw_digest(encoded)
        shutil.rmtree(root / "downgrade-source")
        (root / "downgrade-source").mkdir()
        receipt["losslessness"] = "lossless"
        receipt["omissions"] = []

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="downgrade source identity missing"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_source_inventory_subject_outside_package(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A non-submission source-inventory subject outside downgrade-source/
    must be rejected before any file outside the source package is trusted.
    """
    def mutate(receipt, root):
        entries = json.loads((root / "downgrade-source-inventory.json").read_text())
        for entry in entries:
            if entry["category"] == "journal_proof" and entry["subject"].endswith("source-identity.json"):
                entry["subject"] = "standalone-identity.json"
                break
        shutil.copy(root / "downgrade-source" / "source-identity.json", root / "standalone-identity.json")
        encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        (root / "downgrade-source-inventory.json").write_bytes(encoded)
        receipt["source_inventory"]["digest"] = raw_digest(encoded)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade source inventory references outside source package"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_modified_export_submission_not_source_batch(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """An export submission modified in the export stream, with its inventory
    hash updated to match, must still be rejected because it is not an exact
    source producer-batch submission.
    """
    def mutate(receipt, root):
        stream_path = root / "downgrade-export" / "submissions" / "records.ndjson"
        lines = stream_path.read_text().splitlines()
        submission = json.loads(lines[0])
        submission["payload"]["name"] = "tampered"
        modified_line = json.dumps(submission, separators=(",", ":"), ensure_ascii=False)
        modified_data = (modified_line + "\n").encode("utf-8")
        stream_path.write_bytes(modified_data)

        manifest = json.loads((root / "downgrade-export" / "manifest.json").read_text())
        for spec in manifest["streams"]:
            if spec["path"] == "submissions/records.ndjson":
                spec["digest"] = raw_digest(modified_data)
                spec["byte_length"] = str(len(modified_data))
        (root / "downgrade-export" / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

        entries = json.loads((root / "downgrade-export-inventory.json").read_text())
        for entry in entries:
            if entry["category"] == "submission":
                entry["digest"] = submission_hash(submission)
                break
        encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        (root / "downgrade-export-inventory.json").write_bytes(encoded)
        receipt["export_inventory"]["digest"] = raw_digest(encoded)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade Exchange assertions are not exact source batch submissions"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_invalid_source_identity(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A corrupted source-identity document must break the source package binding.
    """
    def mutate(receipt, root):
        identity_path = root / "downgrade-source" / "source-identity.json"
        identity = json.loads(identity_path.read_text())
        identity["format"] = "ccf.verified-source-identity/evil"
        identity_path.write_text(
            json.dumps(identity, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        entries = json.loads((root / "downgrade-source-inventory.json").read_text())
        for entry in entries:
            if entry["subject"].endswith("source-identity.json"):
                entry["digest"] = raw_digest(identity_path.read_bytes())
                break
        encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        (root / "downgrade-source-inventory.json").write_bytes(encoded)
        receipt["source_inventory"]["digest"] = raw_digest(encoded)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="downgrade source identity format mismatch"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_invalid_source_commit_chain(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A corrupted source commit chain must break the source package binding.
    """
    def mutate(receipt, root):
        commits_path = root / "downgrade-source" / "integrity" / "commits.ndjson"
        lines = commits_path.read_text().splitlines()
        first = json.loads(lines[0])
        first["sequence"] = "2"
        new_lines = [json.dumps(first)] + lines[1:]
        modified_data = "\n".join(new_lines).encode("utf-8") + b"\n"
        commits_path.write_bytes(modified_data)
        entries = json.loads((root / "downgrade-source-inventory.json").read_text())
        for entry in entries:
            if entry["subject"].endswith("commits.ndjson"):
                entry["digest"] = raw_digest(modified_data)
                break
        encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        (root / "downgrade-source-inventory.json").write_bytes(encoded)
        receipt["source_inventory"]["digest"] = raw_digest(encoded)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="downgrade source commit chain invalid"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_invalid_source_producer_batch(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A corrupted selected producer batch must break the source package binding.
    """
    def mutate(receipt, root):
        batch_path = next((root / "downgrade-source" / "producer-batches").glob("*.json"))
        batch = json.loads(batch_path.read_text())
        batch["format"] = "ccf.producer-batch/evil"
        batch_path.write_text(
            json.dumps(batch, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        entries = json.loads((root / "downgrade-source-inventory.json").read_text())
        for entry in entries:
            if entry["subject"].startswith("downgrade-source/producer-batches/"):
                entry["digest"] = raw_digest(batch_path.read_bytes())
                break
        encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        (root / "downgrade-source-inventory.json").write_bytes(encoded)
        receipt["source_inventory"]["digest"] = raw_digest(encoded)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="downgrade source producer batch"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_missing_exported_compartment(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A required structural compartment for an exported source submission must
    be present in the source inventory; removing it from disk and inventory
    must fail closed.
    """
    def mutate(receipt, root):
        comp_subject = "downgrade-source/compartments/records/b7972dfb-99c3-4376-897f-3c9f2848138b.structural.json"
        comp_path = root / comp_subject
        comp_path.unlink()
        entries = [
            entry
            for entry in json.loads((root / "downgrade-source-inventory.json").read_text())
            if entry["subject"] != comp_subject
        ]
        encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        (root / "downgrade-source-inventory.json").write_bytes(encoded)
        receipt["source_inventory"]["digest"] = raw_digest(encoded)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade export submission is not fully available in source package"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_archive_declaration_uses_pinned_active_profiles(
    ccf_settings, tmp_path, ccf_package_root
):
    """Declaration and re-opened archive must use the archive row's pinned
    active profiles, not DEFAULT_ACTIVE_PROFILES.
    """
    from tests.ccf_helpers import make_keypair

    archive_key_path = tmp_path / "archive-ed25519.pem"
    make_keypair(archive_key_path)
    custom_profiles = list(DEFAULT_ACTIVE_PROFILES) + ["ccf-work-pack-0.1.2"]
    archive = Archive.create(
        ccf_settings,
        package_root=ccf_package_root,
        archive_key_path=archive_key_path,
        active_profiles=custom_profiles,
    )
    assert archive.active_profiles == custom_profiles
    declaration = archive.implementation_declaration()
    assert "ccf-work-pack-v1" in declaration["capabilities"]
    assert declaration["extensions"]["thoth.legacy_profiles"] == custom_profiles

    opened = Archive.open(
        ccf_settings,
        package_root=ccf_package_root,
        archive_key_path=archive_key_path,
    )
    assert opened.active_profiles == custom_profiles


def test_verify_capsule_rejects_submissions_not_activated(
    ccf_capsule_example, layered, layered_schemas
):
    """A submissions stream must use handling=activate; any other handling is
    a schema/semantic violation.
    """
    capsule = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    for stream in capsule.streams:
        if stream.content_role == "submissions":
            stream.spec["handling"] = "preserve_inert"
    with pytest.raises(CapsuleError, match="submissions stream must use handling=activate"):
        verify_capsule(
            capsule,
            layered=layered,
            schemas=layered_schemas,
            recipient_level="ccf-exchange-v1",
            recipient_capabilities=(),
        )


def test_write_capsule_rejects_schema_invalid_input_before_mkdir(
    tmp_path, ccf_capsule_example, layered_schemas
):
    """write_capsule must schema-validate the input manifest before creating
    the output directory.
    """
    source = load_capsule(ccf_capsule_example, schemas=layered_schemas)
    manifest = dict(source.manifest)
    del manifest["pack_id"]
    out = tmp_path / "schema-invalid-out"
    with pytest.raises(CapsuleError, match="input capsule manifest"):
        write_capsule(
            out,
            manifest=manifest,
            submission_streams={},
            schemas=layered_schemas,
        )
    assert not out.exists()


def test_downgrade_receipt_rejects_symlink_export_capsule(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A symlinked downgrade-export/ that points outside the capsule root must
    be rejected by the containment primitive before any file is read outside.
    """
    def mutate(receipt, root):
        external = tmp_path / "external-export"
        shutil.copytree(root / "downgrade-export", external)
        shutil.rmtree(root / "downgrade-export")
        (root / "downgrade-export").symlink_to(external)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="path escapes root"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


# ---------------------------------------------------------------------------
# Downgrade source package binding and containment regressions
# ---------------------------------------------------------------------------


def _rewrite_inventory(root: Path, name: str, entries: list[dict], receipt: dict, ref_name: str) -> None:
    encoded = json.dumps(entries, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
    (root / name).write_bytes(encoded)
    receipt[ref_name]["digest"] = raw_digest(encoded)


def test_downgrade_rejects_recomputed_batch_with_source_object_evidence_binding(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """An attacker modifies the batch submission, export stream, manifest, and
    both inventories together, recomputing batch_hash. The source object's
    producer_evidence still authenticates the original submission, so the
    coherent tamper must still fail.
    """
    exported_id = "urn:ccf:record:b7972dfb-99c3-4376-897f-3c9f2848138b"

    def mutate(receipt, root):
        # Modify the exported submission identically in batch and export stream.
        batch_path = next((root / "downgrade-source" / "producer-batches").glob("*.json"))
        batch = json.loads(batch_path.read_text())
        for record in batch["records"]:
            if record["id"] == exported_id:
                record["payload"]["name"] = "coherently-tampered"
        batch["batch_hash"] = producer_batch_hash(batch)
        batch_path.write_text(
            json.dumps(batch, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

        stream_path = root / "downgrade-export" / "submissions" / "records.ndjson"
        lines = stream_path.read_text().splitlines()
        submission = json.loads(lines[0])
        submission["payload"]["name"] = "coherently-tampered"
        modified_line = json.dumps(submission, separators=(",", ":"), ensure_ascii=False)
        modified_data = (modified_line + "\n").encode("utf-8")
        stream_path.write_bytes(modified_data)

        manifest = json.loads((root / "downgrade-export" / "manifest.json").read_text())
        for spec in manifest["streams"]:
            if spec["path"] == "submissions/records.ndjson":
                spec["digest"] = raw_digest(modified_data)
                spec["byte_length"] = str(len(modified_data))
        (root / "downgrade-export" / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

        # Update inventories to match the coherently modified artifacts.
        for name, ref in (
            ("downgrade-source-inventory.json", "source_inventory"),
            ("downgrade-export-inventory.json", "export_inventory"),
        ):
            entries = json.loads((root / name).read_text())
            for entry in entries:
                if entry["subject"].startswith("downgrade-source/producer-batches/"):
                    entry["digest"] = raw_digest(batch_path.read_bytes())
                if entry["category"] == "submission" and entry["subject"] == f"submission:{exported_id}":
                    entry["digest"] = submission_hash(submission)
            _rewrite_inventory(root, name, entries, receipt, ref)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade export submission source object producer evidence mismatch"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_tampered_semantic_compartment_with_updated_inventory(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A selected source semantic compartment is tampered and the source
    inventory digest is updated. Header commitment verification must reject.
    """
    exported_id = "urn:ccf:record:b7972dfb-99c3-4376-897f-3c9f2848138b"
    uuid = exported_id.rsplit(":", 1)[1]
    comp_subject = f"downgrade-source/compartments/records/{uuid}.semantic.json"

    def mutate(receipt, root):
        comp_path = root / comp_subject
        envelope = json.loads(comp_path.read_text())
        envelope["content"]["some_tampered_field"] = "evil"
        comp_path.write_text(
            json.dumps(envelope, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        entries = json.loads((root / "downgrade-source-inventory.json").read_text())
        for entry in entries:
            if entry["subject"] == comp_subject:
                entry["digest"] = raw_digest(comp_path.read_bytes())
                break
        _rewrite_inventory(
            root, "downgrade-source-inventory.json", entries, receipt, "source_inventory"
        )

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade source object .* failed verification|semantic commitment mismatch"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_rehashed_source_identity(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """The source identity's trusted_genesis_signer_key_id is changed and the
    inventory digest updated. The chain signer_key_id binding must reject.
    """
    def mutate(receipt, root):
        identity_path = root / "downgrade-source" / "source-identity.json"
        identity = json.loads(identity_path.read_text())
        identity["trusted_genesis_signer_key_id"] = "urn:ccf:key:00000000-0000-4000-8000-000000000000"
        identity_path.write_text(
            json.dumps(identity, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        entries = json.loads((root / "downgrade-source-inventory.json").read_text())
        for entry in entries:
            if entry["subject"].endswith("source-identity.json"):
                entry["digest"] = raw_digest(identity_path.read_bytes())
                break
        _rewrite_inventory(
            root, "downgrade-source-inventory.json", entries, receipt, "source_inventory"
        )

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade source chain signer_key_id does not match"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_invented_source_only_logical_submission(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A logical submission entry invented only in the source inventory must be
    rejected because it is not an exact selected producer-batch submission.
    """
    invented_id = "urn:ccf:record:00000000-0000-4000-8000-000000000001"

    def mutate(receipt, root):
        entries = json.loads((root / "downgrade-source-inventory.json").read_text())
        entries.append({
            "category": "submission",
            "subject": f"submission:{invented_id}",
            "digest": "sha256:" + "0" * 64,
        })
        _rewrite_inventory(
            root, "downgrade-source-inventory.json", entries, receipt, "source_inventory"
        )

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade source inventory submission is not a selected batch submission"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_missing_required_source_file(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A required physical source file is removed and the inventory updated;
    the required-layout check must fail closed before coverage arithmetic.
    """
    def mutate(receipt, root):
        target = root / "downgrade-source" / "objects" / "blobs.ndjson"
        target.unlink()
        entries = [
            entry
            for entry in json.loads((root / "downgrade-source-inventory.json").read_text())
            if entry["subject"] != "downgrade-source/objects/blobs.ndjson"
        ]
        _rewrite_inventory(
            root, "downgrade-source-inventory.json", entries, receipt, "source_inventory"
        )

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade source required file missing"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_missing_selected_producer_batch(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """The selected producer batch file is removed and the inventory updated;
    verification must fail closed because the source package is incomplete.
    """
    def mutate(receipt, root):
        batch_path = next((root / "downgrade-source" / "producer-batches").glob("*.json"))
        batch_subject = f"downgrade-source/producer-batches/{batch_path.name}"
        shutil.rmtree(root / "downgrade-source" / "producer-batches")
        entries = [
            entry
            for entry in json.loads((root / "downgrade-source-inventory.json").read_text())
            if entry["subject"] != batch_subject
        ]
        _rewrite_inventory(
            root, "downgrade-source-inventory.json", entries, receipt, "source_inventory"
        )

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade source producer-batches directory missing"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_tampered_member_chain(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A source member row is tampered and the inventory digest updated; commit
    chain verification must reject the corrupted membership proof.
    """
    def mutate(receipt, root):
        members_path = root / "downgrade-source" / "integrity" / "members.ndjson"
        lines = members_path.read_text().splitlines()
        first = json.loads(lines[0])
        first["object_hash"] = "sha256:" + "0" * 64
        new_lines = [json.dumps(first)] + lines[1:]
        modified_data = "\n".join(new_lines).encode("utf-8") + b"\n"
        members_path.write_bytes(modified_data)
        entries = json.loads((root / "downgrade-source-inventory.json").read_text())
        for entry in entries:
            if entry["subject"].endswith("members.ndjson"):
                entry["digest"] = raw_digest(modified_data)
                break
        _rewrite_inventory(
            root, "downgrade-source-inventory.json", entries, receipt, "source_inventory"
        )

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(
        ExchangeError, match="downgrade source commit chain invalid"
    ):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


# ---------------------------------------------------------------------------
# Symlink containment regressions
# ---------------------------------------------------------------------------


def test_downgrade_rejects_nested_source_directory_symlink(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A nested directory symlink inside downgrade-source/ pointing outside the
    Capsule must be rejected before any direct read follows it.
    """
    def mutate(receipt, root):
        external = tmp_path / "outside-source"
        external.mkdir()
        (external / "hidden-proof.json").write_text("{}", encoding="utf-8")
        link = root / "downgrade-source" / "hidden-dir"
        link.symlink_to(external)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="downgrade source package tree invalid"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_nested_export_directory_symlink(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """A nested directory symlink inside downgrade-export/ pointing outside the
    Capsule must be rejected by exact physical coverage, not ignored.
    """
    def mutate(receipt, root):
        external = tmp_path / "outside-export"
        external.mkdir()
        (external / "stash.json").write_text("{}", encoding="utf-8")
        link = root / "downgrade-export" / "stash"
        link.symlink_to(external)

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="downgrade export package tree invalid"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_downgrade_rejects_symlinked_source_integrity_and_batches(
    ccf_capsule_example, layered, layered_schemas, tmp_path
):
    """Moving integrity/ and producer-batches/ outside and replacing them with
    symlinks defeats coverage if symlinks are followed. Coherently prune
    inventories and omissions; verification must still reject the symlink tree
    before PackReader reads outside bytes.
    """
    def mutate(receipt, root):
        source_dir = root / "downgrade-source"
        external = tmp_path / "outside-source-innards"
        external.mkdir()

        # Move the two directories outside and replace with symlinks.
        for name in ("integrity", "producer-batches"):
            src = source_dir / name
            dst = external / name
            src.rename(dst)
            (source_dir / name).symlink_to(dst)

        # Prune those entries from the source inventory and update the digest.
        pruned_subjects = {
            "downgrade-source/integrity/commits.ndjson",
            "downgrade-source/integrity/members.ndjson",
            "downgrade-source/producer-batches/98d352bf-7abb-4fdf-824c-3c93c4e55901.json",
        }
        entries = [
            entry
            for entry in json.loads((root / "downgrade-source-inventory.json").read_text())
            if entry["subject"] not in pruned_subjects
        ]
        _rewrite_inventory(
            root, "downgrade-source-inventory.json", entries, receipt, "source_inventory"
        )

        # Prune matching omissions so the receipt remains internally consistent.
        receipt["omissions"] = [
            entry
            for entry in receipt["omissions"]
            if entry["subject"] not in pruned_subjects
        ]

    root, receipt = _mutated_downgrade_example(
        ccf_capsule_example, tmp_path, mutate
    )
    with pytest.raises(ExchangeError, match="downgrade source package tree invalid"):
        verify_downgrade_receipt(
            receipt, capsule_root=root, layered=layered, schemas=layered_schemas
        )


def test_load_capsule_rejects_manifest_symlink_to_outside(
    tmp_path, ccf_capsule_example, layered_schemas
):
    """A Capsule manifest.json that is a symlink to a file outside the Capsule
    root must be rejected by containment before any bytes are trusted.
    """
    root = tmp_path / "symlink-manifest-capsule"
    shutil.copytree(ccf_capsule_example, root)
    external_manifest = tmp_path / "outside-manifest.json"
    external_manifest.write_bytes((root / "manifest.json").read_bytes())
    (root / "manifest.json").unlink()
    (root / "manifest.json").symlink_to(external_manifest)
    with pytest.raises(CapsuleError, match="path escapes root"):
        load_capsule(root, schemas=layered_schemas)


def test_load_verified_source_package_rejects_identity_symlink(
    ccf_capsule_example, layered_schemas, tmp_path
):
    """A symlinked source-identity.json must be rejected before any direct read,
    even when the symlink target is a valid file inside the same package.
    """
    source_dir = tmp_path / "symlink-identity-source"
    shutil.copytree(ccf_capsule_example / "downgrade-source", source_dir)
    identity_path = source_dir / "source-identity.json"
    in_root_copy = source_dir / "identity-copy.json"
    in_root_copy.write_bytes(identity_path.read_bytes())
    identity_path.unlink()
    identity_path.symlink_to(in_root_copy)
    with pytest.raises(SourcePackageError, match="downgrade source package tree invalid"):
        load_verified_source_package(source_dir, schemas=layered_schemas)


def test_enumerate_package_files_fails_closed_on_walk_error(tmp_path, monkeypatch):
    """A filesystem traversal error must surface as a CapsuleError, not be
    swallowed by os.walk's default silent behavior.
    """
    root = tmp_path / "walk-error-root"
    root.mkdir()
    (root / "file.txt").write_text("ok", encoding="utf-8")

    def broken_walk(*args, **kwargs):
        if kwargs.get("onerror"):
            kwargs["onerror"](OSError("simulated scandir failure"))
        return []

    monkeypatch.setattr("os.walk", broken_walk)
    with pytest.raises(CapsuleError, match="package tree traversal failed"):
        _enumerate_package_files(root)


def test_load_capsule_rejects_manifest_symlink_to_inside_target(
    tmp_path, ccf_capsule_example, layered_schemas
):
    """A Capsule manifest.json symlink pointing to a file inside the same root
    must still be rejected: the package policy is no symlinks.
    """
    root = tmp_path / "symlink-manifest-inside-capsule"
    shutil.copytree(ccf_capsule_example, root)
    manifest_copy = root / "manifest-copy.json"
    manifest_copy.write_bytes((root / "manifest.json").read_bytes())
    (root / "manifest.json").unlink()
    (root / "manifest.json").symlink_to(manifest_copy)
    with pytest.raises(CapsuleError, match="package contains symlink"):
        load_capsule(root, schemas=layered_schemas)


def test_load_capsule_rejects_manifest_symlink_to_outside(
    tmp_path, ccf_capsule_example, layered_schemas
):
    """A Capsule manifest.json that is a symlink to a file outside the Capsule
    root must be rejected before any bytes are trusted.
    """
    root = tmp_path / "symlink-manifest-capsule"
    shutil.copytree(ccf_capsule_example, root)
    external_manifest = tmp_path / "outside-manifest.json"
    external_manifest.write_bytes((root / "manifest.json").read_bytes())
    (root / "manifest.json").unlink()
    (root / "manifest.json").symlink_to(external_manifest)
    with pytest.raises(CapsuleError, match="package contains symlink"):
        load_capsule(root, schemas=layered_schemas)
