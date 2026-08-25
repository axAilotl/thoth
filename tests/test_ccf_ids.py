"""CCF portable identifier tests (spec section 2)."""

from __future__ import annotations

import uuid

import pytest

from ccf.ids import ID_KINDS, CcfIdError, generate_id, is_valid_id, parse_id


def test_all_kinds_supported():
    assert ID_KINDS == frozenset(
        {
            "record",
            "link",
            "blob",
            "archive",
            "lineage",
            "key",
            "credential",
            "batch",
            "pack",
            "receipt",
        }
    )


@pytest.mark.parametrize("kind", sorted(ID_KINDS))
def test_generate_parse_roundtrip(kind):
    urn = generate_id(kind)
    parsed = parse_id(urn)
    assert parsed.kind == kind
    assert parsed.uuid.version == 4
    assert parsed.uuid.variant == uuid.RFC_4122
    assert str(parsed) == urn
    assert is_valid_id(urn)


def test_generated_ids_are_unique():
    assert len({generate_id("record") for _ in range(100)}) == 100


def test_spec_example_urn_valid():
    assert is_valid_id("urn:ccf:record:550e8400-e29b-41d4-a716-446655440000")


@pytest.mark.parametrize(
    "bad",
    [
        # UUIDv7 (version nibble 7) is not the portable profile.
        "urn:ccf:record:018f3c2a-1234-7abc-8def-0123456789ab",
        # Nil UUID (version nibble 0).
        "urn:ccf:record:00000000-0000-0000-0000-000000000000",
        # Wrong variant nibble.
        "urn:ccf:record:550e8400-e29b-41d4-c716-446655440000",
        # Uppercase anywhere.
        "urn:ccf:record:550E8400-E29B-41D4-A716-446655440000",
        "URN:ccf:record:550e8400-e29b-41d4-a716-446655440000",
        # Unknown or mistyped kind.
        "urn:ccf:widget:550e8400-e29b-41d4-a716-446655440000",
        "urn:ccf:Record:550e8400-e29b-41d4-a716-446655440000",
        # Malformed shapes.
        "urn:ccf:record:",
        "urn:ccf:record:not-a-uuid",
        "urn:ccf:record:550e8400e29b41d4a716446655440000",
        "urn:ccf:record:550e8400-e29b-41d4-a716-446655440000-extra",
        " urn:ccf:record:550e8400-e29b-41d4-a716-446655440000",
        "urn:ccf:record:550e8400-e29b-41d4-a716-446655440000 ",
        "record:550e8400-e29b-41d4-a716-446655440000",
        "",
    ],
)
def test_invalid_urns_rejected(bad):
    assert not is_valid_id(bad)
    with pytest.raises(CcfIdError):
        parse_id(bad)


def test_generate_rejects_unknown_kind():
    with pytest.raises(CcfIdError):
        generate_id("widget")


def test_parse_rejects_non_string():
    with pytest.raises(CcfIdError):
        parse_id(None)
