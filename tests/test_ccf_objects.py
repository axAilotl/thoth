"""Portable object envelope tests (spec sections 3 and 5).

Builds headers and compartments from the published vectors and the
``examples/thoth-capture`` package, verifies them against the vendored
JSON Schemas, and checks availability-state and admission-metadata rules.
"""

from __future__ import annotations

import copy
import json

import jsonschema
import pytest
from referencing import Registry, Resource

from ccf import CCF_HASH_PROFILE, CCF_SPEC
from ccf.objects import (
    AdmissionMetadata,
    AvailabilityState,
    CcfObjectError,
    CompartmentEnvelope,
    CompartmentStorage,
    PortableHeader,
    admission_order_key,
    compartment_format,
    new_salt,
    validate_decimal_string,
    validate_timestamp,
)

SCHEMA_IDS = {
    ("record", "header"): "urn:ccf:schema:0.1.2:objects.record-header",
    ("link", "header"): "urn:ccf:schema:0.1.2:objects.link-header",
    ("blob", "header"): "urn:ccf:schema:0.1.2:objects.blob-header",
    ("record", "structural"): "urn:ccf:schema:0.1.2:objects.record-structural",
    ("record", "semantic"): "urn:ccf:schema:0.1.2:objects.record-semantic",
    ("link", "structural"): "urn:ccf:schema:0.1.2:objects.link-structural",
    ("link", "semantic"): "urn:ccf:schema:0.1.2:objects.link-semantic",
    ("blob", "structural"): "urn:ccf:schema:0.1.2:objects.blob-structural",
    ("blob", "semantic"): "urn:ccf:schema:0.1.2:objects.blob-semantic",
}


@pytest.fixture(scope="module")
def schema_registry(ccf_package_root):
    resources = []
    for path in (ccf_package_root / "schemas").rglob("*.json"):
        document = json.loads(path.read_text(encoding="utf-8"))
        schema_id = document.get("$id")
        if schema_id:
            resources.append((schema_id, Resource.from_contents(document)))
    return Registry().with_resources(resources)


@pytest.fixture(scope="module")
def validate(schema_registry, ccf_package_root, load_ccf_json):
    def _validate(instance: dict, schema_id: str) -> None:
        schema_resource = schema_registry.get(schema_id)
        assert schema_resource is not None, schema_id
        validator = jsonschema.Draft202012Validator(
            schema_resource.contents, registry=schema_registry
        )
        validator.validate(instance)

    return _validate


@pytest.fixture(scope="module")
def object_vectors(ccf_vectors_dir, load_ccf_json):
    return load_ccf_json(ccf_vectors_dir / "object-hashes.json")


# --- Header construction from compartments (spec 3.1/4.5) ---------------------


@pytest.mark.parametrize("kind", ["record", "link", "blob"])
def test_build_header_reproduces_vectors(object_vectors, kind):
    vector = object_vectors[kind]
    structural = CompartmentEnvelope.from_dict(vector["structural"])
    semantic = CompartmentEnvelope.from_dict(vector["semantic"])
    header = PortableHeader.build(kind, vector["header"]["id"], structural, semantic)
    assert header.to_dict() == vector["header"]
    assert header.object_hash == vector["expected_object_hash"]


@pytest.mark.parametrize("kind", ["record", "link", "blob"])
def test_header_verify_roundtrip(object_vectors, kind):
    vector = object_vectors[kind]
    header = PortableHeader.from_dict(vector["header"])
    structural = CompartmentEnvelope.from_dict(vector["structural"])
    semantic = CompartmentEnvelope.from_dict(vector["semantic"])
    header.verify(structural, semantic)


def test_header_verify_fails_closed_on_tamper(object_vectors):
    vector = object_vectors["record"]
    header = PortableHeader.from_dict(vector["header"])
    structural = CompartmentEnvelope.from_dict(vector["structural"])
    semantic_data = copy.deepcopy(vector["semantic"])
    semantic_data["content"]["payload"]["text"] = "forged"
    with pytest.raises(CcfObjectError, match="verification failed"):
        header.verify(structural, CompartmentEnvelope.from_dict(semantic_data))


def test_absent_semantic_compartment_is_null_commitment(ccf_vectors_dir, load_ccf_json):
    # The genesis commit Record has no semantic compartment (section 4.3).
    genesis = load_ccf_json(ccf_vectors_dir / "commit-signing.json")["genesis"]
    structural = CompartmentEnvelope.from_dict(genesis["structural"])
    header = PortableHeader.build(
        "record", genesis["header"]["id"], structural, semantic=None
    )
    assert header.semantic_commitment is None
    assert header.to_dict() == genesis["header"]


def test_header_fields_exclude_admission_metadata(object_vectors):
    header = PortableHeader.from_dict(object_vectors["record"]["header"])
    assert set(header.to_dict()) == {
        "spec",
        "object_kind",
        "id",
        "hash_profile",
        "structural_commitment",
        "semantic_commitment",
        "object_hash",
    }
    assert header.spec == CCF_SPEC
    assert header.hash_profile == CCF_HASH_PROFILE


def test_header_rejects_mismatched_id_kind(object_vectors):
    data = copy.deepcopy(object_vectors["record"]["header"])
    data["id"] = data["id"].replace("urn:ccf:record:", "urn:ccf:link:")
    with pytest.raises(CcfObjectError):
        PortableHeader.from_dict(data)


def test_header_rejects_wrong_spec_or_profile(object_vectors):
    data = copy.deepcopy(object_vectors["record"]["header"])
    data["hash_profile"] = "sha256-raw"
    with pytest.raises(CcfObjectError):
        PortableHeader.from_dict(data)


# --- Compartment envelopes and availability states (spec 3.2/3.6) -------------


def test_envelope_create_generates_valid_salt():
    envelope = CompartmentEnvelope.create("record", "structural", {"type": "x"})
    assert envelope.format == compartment_format("record", "structural")
    assert len(envelope.salt) == 43  # 32 bytes, unpadded base64url
    assert new_salt() != new_salt()


def test_envelope_rejects_short_salt():
    with pytest.raises(CcfObjectError):
        CompartmentEnvelope(compartment_format("record", "structural"), "c2hvcnQ", {})


def test_envelope_commitment_rejects_wrong_format(object_vectors):
    envelope = CompartmentEnvelope.from_dict(object_vectors["record"]["structural"])
    with pytest.raises(CcfObjectError):
        envelope.commitment("record", "semantic")


def test_availability_state_rules(object_vectors):
    envelope = CompartmentEnvelope.from_dict(object_vectors["record"]["structural"])
    CompartmentStorage(AvailabilityState.PLAINTEXT, envelope)
    CompartmentStorage(AvailabilityState.ENCRYPTED, envelope)
    CompartmentStorage(AvailabilityState.WITHHELD)
    CompartmentStorage(AvailabilityState.ERASED)
    with pytest.raises(CcfObjectError):
        CompartmentStorage(AvailabilityState.PLAINTEXT)
    with pytest.raises(CcfObjectError):
        CompartmentStorage(AvailabilityState.ERASED, envelope)
    with pytest.raises(CcfObjectError):
        CompartmentStorage(AvailabilityState.WITHHELD, envelope)


# --- Admission metadata (spec 1.2 layer 4, 4.2) --------------------------------


def test_admission_metadata_member_matches_merkle_vector(
    ccf_vectors_dir, load_ccf_json, object_vectors
):
    merkle = load_ccf_json(ccf_vectors_dir / "merkle.json")
    expected = next(
        m
        for m in merkle["commit2"]["members"]
        if m["object_id"] == object_vectors["record"]["header"]["id"]
    )
    header = PortableHeader.from_dict(object_vectors["record"]["header"])
    admission = AdmissionMetadata(
        commit_sequence=expected["commit_sequence"],
        commit_position=expected["commit_position"],
        admitted_at=expected["admitted_at"],
    )
    assert admission.to_member(header) == expected


def test_admission_order_key_is_numeric():
    assert admission_order_key("9", 99) < admission_order_key("10", 0)
    with pytest.raises(CcfObjectError):
        admission_order_key("09", 0)
    with pytest.raises(CcfObjectError):
        AdmissionMetadata("1", -1, "2026-08-11T21:40:01.000Z")
    with pytest.raises(CcfObjectError):
        AdmissionMetadata("1", 0, "2026-08-11 21:40:01")


def test_canonical_string_validators():
    assert validate_timestamp("2026-08-11T21:42:18.331Z")
    assert validate_decimal_string("0")
    with pytest.raises(CcfObjectError):
        validate_timestamp("2026-08-11T21:42:18Z")
    with pytest.raises(CcfObjectError):
        validate_decimal_string("1.0")


# --- Schema validation against the vendored schemas ----------------------------


@pytest.mark.parametrize("kind", ["record", "link", "blob"])
def test_vector_headers_and_envelopes_validate(object_vectors, validate, kind):
    vector = object_vectors[kind]
    validate(vector["header"], SCHEMA_IDS[(kind, "header")])
    validate(vector["structural"], SCHEMA_IDS[(kind, "structural")])
    validate(vector["semantic"], SCHEMA_IDS[(kind, "semantic")])


def test_commit_members_validate(ccf_vectors_dir, load_ccf_json, validate):
    merkle = load_ccf_json(ccf_vectors_dir / "merkle.json")
    for name in ("commit1", "commit2"):
        for member in merkle[name]["members"]:
            validate(member, "urn:ccf:schema:0.1.2:objects.commit-member")


def test_producer_batch_validates(ccf_vectors_dir, load_ccf_json, validate):
    batch = load_ccf_json(ccf_vectors_dir / "producer-batch.json")["batch"]
    validate(batch, "urn:ccf:schema:0.1.2:sync.producer-batch")


def test_schema_validation_fails_closed_on_tamper(object_vectors, validate):
    header = copy.deepcopy(object_vectors["record"]["header"])
    header["admission"] = {"commit_sequence": "2"}  # archive-local field
    with pytest.raises(jsonschema.ValidationError):
        validate(header, SCHEMA_IDS[("record", "header")])


# --- End-to-end against the thoth-capture example ------------------------------


def test_thoth_capture_example_objects_verify(ccf_examples_dir, load_ccf_json):
    checked = 0
    for header_path in sorted(ccf_examples_dir.glob("*.header.json")):
        stem = header_path.name[: -len(".header.json")]
        header = PortableHeader.from_dict(load_ccf_json(header_path))
        structural = CompartmentEnvelope.from_dict(
            load_ccf_json(ccf_examples_dir / f"{stem}.structural.json")
        )
        semantic_path = ccf_examples_dir / f"{stem}.semantic.json"
        semantic = (
            CompartmentEnvelope.from_dict(load_ccf_json(semantic_path))
            if semantic_path.exists()
            else None
        )
        if semantic is None:
            assert header.semantic_commitment is None, stem
        header.verify(structural, semantic)
        checked += 1
    # 13 records, 7 links, 1 blob, plus 2 commit Records without semantics.
    assert checked == 23
