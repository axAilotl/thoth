"""Exercise the published CCF 0.3.0 vectors through Thoth's implementation."""

from __future__ import annotations

import copy
import json
import uuid

import pytest

from ccf import PACKAGE_ROOT, schema_id
from ccf.ids import generate_id, is_valid_id, parse_id
from ccf.jcs import canonical_bytes, canonicalize, loads
from ccf.objects import hash_bytes, hash_data, merge, now_timestamp, validate_object, validate_timestamp
from ccf.schemas import SchemaSet, validate_manifest

HASHES = json.loads((PACKAGE_ROOT / "vectors/hashes.json").read_text(encoding="utf-8"))
MERGES = json.loads((PACKAGE_ROOT / "vectors/merge.json").read_text(encoding="utf-8"))["cases"]


def record(**fields):
    obj = copy.deepcopy(MERGES[0]["a"])
    obj.update(fields)
    return obj


@pytest.mark.parametrize("case", HASHES["canonical"], ids=lambda case: case["name"])
def test_canonical_hash_vectors(case):
    value = loads(case["input"])
    assert canonicalize(value) == case["canonical"]
    assert canonical_bytes(value) == case["canonical"].encode("utf-8")
    assert hash_data(value) == case["hash"]


@pytest.mark.parametrize("case", HASHES["invalid"], ids=lambda case: case["name"])
def test_invalid_json_vectors(case):
    with pytest.raises(ValueError):
        loads(case["input"])


@pytest.mark.parametrize("case", HASHES["bytes"], ids=lambda case: case["name"])
def test_raw_byte_vectors(case):
    assert hash_bytes(bytes.fromhex(case["hex"])) == case["hash"]


@pytest.mark.parametrize("case", MERGES, ids=lambda case: case["name"])
def test_merge_vectors_in_both_directions(case):
    assert merge(case["a"], case["b"]) is case[case["winner"]]
    assert merge(case["b"], case["a"]) is case[case["winner"]]


@pytest.mark.parametrize("value", [1.0, 0.25, float("nan"), float("inf"), (1, 2), {1: "a"}, "\ud800", 2**53])
def test_nonportable_python_values_fail(value):
    with pytest.raises(ValueError):
        canonicalize({"unknown": {"nested": [value]}})


@pytest.mark.parametrize("text", [b"\xff", b"\xef\xbb\xbf{}", '{"x":Infinity}', '{"x":{"y":1,"y":2}}'])
def test_malformed_json_input_fails(text):
    with pytest.raises(ValueError):
        loads(text)


def test_escaped_surrogate_pair_decodes_to_valid_unicode():
    assert loads('"\\ud83d\\ude00"') == "😀"


def test_uuid_v7_has_timestamp_and_random_identity(monkeypatch):
    monkeypatch.setattr("ccf.ids.time.time_ns", lambda: 1_777_777_777_123_000_000)
    identifiers = {generate_id() for _ in range(100)}
    assert len(identifiers) == 100
    for identifier in identifiers:
        value = parse_id(identifier)
        assert value.version == 7
        assert value.variant == uuid.RFC_4122
        assert value.int >> 80 == 1_777_777_777_123


@pytest.mark.parametrize("value", [None, "", str(uuid.uuid4()), "01926F3A-0C1E-7B2A-8F10-5A1D2C3E4F50", "01926f3a-0c1e-7b2a-8f10-5a1d2c3e4f50\n"])
def test_uuid_requires_exact_v7_spelling(value):
    assert not is_valid_id(value)
    with pytest.raises(ValueError):
        parse_id(value)


@pytest.mark.parametrize("timestamp", ["2026-02-30T00:00:00.000Z", "2026-09-24T24:00:00.000Z", "2026-09-24T01:01:01Z", "2026-09-24T01:01:01.000+00:00", "2026-09-24T01:01:01.000Z\n"])
def test_invalid_timestamp(timestamp):
    with pytest.raises(ValueError):
        validate_timestamp(timestamp)


def test_current_time_is_canonical():
    assert validate_timestamp(now_timestamp())


@pytest.mark.parametrize("fields", [{"rev": True}, {"rev": 1.0}, {"rev": 0}, {"data": {"confidence": 0.8}}, {"ext": {"large": 2**53}}, {"source": {"app": "test", "extra": [1e2]}}, {"unknown": 0.5}, {"kind": []}, {"type": "note\n"}, {"hash": "sha256:" + "0" * 64 + "\n"}])
def test_invalid_objects_fail_through_schema_and_integer_rules(fields):
    with pytest.raises(ValueError):
        validate_object(record(**fields))


def test_unknown_fields_and_types_and_unresolved_locations_are_preserved():
    obj = record(type="other.future", loc="unrecognized:opaque", data={"a": [True, 1]}, ext={"future": "x"}, source={"app": "test", "foreign": [1, "x"]}, foreign={"field": None})
    before = copy.deepcopy(obj)
    assert validate_object(obj) == []
    assert obj == before


def test_links_allow_missing_endpoints_and_unknown_relationships():
    obj = record(kind="link", **{"from": generate_id(), "to": generate_id(), "rel": "future.relation", "start": "unknown field"})
    del obj["type"]
    assert validate_object(obj) == []


def test_hash_mismatch_is_only_a_warning():
    obj = record(data={"text": "new"}, hash=hash_data({"text": "old"}))
    before = copy.deepcopy(obj)
    assert "hash does not match" in validate_object(obj)[0]
    assert obj == before


def test_different_ids_cannot_merge():
    with pytest.raises(ValueError, match="different IDs"):
        merge(record(), record(id=generate_id()))


def test_merge_compares_device_utf8_and_full_object_bytes():
    a = record(source={"app": "test", "device": "\ue000"}, title="Z")
    b = record(source={"app": "test", "device": "😀"}, title="A")
    assert merge(a, b) is b
    assert merge(b, a) is b


def test_all_upstream_export_objects_and_manifest_validate_locally():
    directory = PACKAGE_ROOT / "examples/export"
    validate_manifest(loads((directory / "manifest.json").read_text(encoding="utf-8")))
    for name in ("records.jsonl", "links.jsonl", "blobs.jsonl"):
        for line in (directory / name).read_text(encoding="utf-8").splitlines():
            assert validate_object(loads(line)) == []


def test_schema_set_includes_all_five_resources_and_rejects_unknown_schema():
    schemas = SchemaSet.load()
    for name in ("common", "record", "link", "blob", "manifest"):
        assert schemas.has_schema(schema_id(name))
    with pytest.raises(ValueError, match="unknown CCF schema"):
        schemas.validate("https://example.com/never-fetch", {})


def test_manifest_calendar_and_nested_number_rules():
    manifest = loads((PACKAGE_ROOT / "examples/export/manifest.json").read_text(encoding="utf-8"))
    manifest["exported_at"] = "2026-02-30T00:00:00.000Z"
    with pytest.raises(ValueError, match="date/time"):
        validate_manifest(manifest)
    manifest["exported_at"] = now_timestamp()
    manifest["counts"]["files"] = 1.0
    with pytest.raises(ValueError):
        validate_manifest(manifest)
