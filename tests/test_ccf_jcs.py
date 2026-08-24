"""JCS (RFC 8785) canonicalization conformance tests.

Reproduces every case in ``vectors/canonicalization.json`` and covers the
spec's rejection list with negative tests.
"""

from __future__ import annotations

import pytest

from ccf import jcs
from ccf.hashing import canonical_digest

VECTOR_DIGEST_DOMAIN = "ccf:canonicalization-vector:v1"


@pytest.fixture(scope="module")
def canon_vectors(ccf_vectors_dir, load_ccf_json):
    return load_ccf_json(ccf_vectors_dir / "canonicalization.json")


def test_every_vector_case_is_covered(canon_vectors):
    assert len(canon_vectors["cases"]) == 7
    assert len(canon_vectors["rejections"]) == 7


@pytest.mark.parametrize("case_index", range(7))
def test_canonical_serialization_matches_vector(canon_vectors, case_index):
    case = canon_vectors["cases"][case_index]
    assert jcs.canonicalize(case["value"]) == case["expected"], case["name"]


@pytest.mark.parametrize("case_index", range(7))
def test_canonical_digest_matches_vector(canon_vectors, case_index):
    case = canon_vectors["cases"][case_index]
    assert canonical_digest(VECTOR_DIGEST_DOMAIN, case["value"]) == case["digest"], case[
        "name"
    ]


# --- Number formatting: ECMAScript Number::toString semantics ----------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, "0"),
        (100, "100"),
        (-100, "-100"),
        (2**53 - 1, "9007199254740991"),
        (-(2**53 - 1), "-9007199254740991"),
        (100.0, "100"),
        (0.96, "0.96"),
        (-1.5, "-1.5"),
        (0.000001, "0.000001"),
        (1e-7, "1e-7"),
        (1.5e-7, "1.5e-7"),
        (1e20, "100000000000000000000"),
        (1e21, "1e+21"),
        (-1e21, "-1e+21"),
        (1.7976931348623157e308, "1.7976931348623157e+308"),
        (5e-324, "5e-324"),
        (0.1 + 0.2, "0.30000000000000004"),
    ],
)
def test_number_formatting_matches_ecmascript(value, expected):
    assert jcs.canonicalize(value) == expected


# --- Rejections (spec section 4.1 and the vector rejection list) -------------


def test_reject_negative_zero():
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize(-0.0)


def test_reject_nan():
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize(float("nan"))


def test_reject_infinity():
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize(float("inf"))
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize(float("-inf"))


def test_reject_unpaired_surrogates():
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize("\ud800")
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize("\udc00")
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize({"x\ud800": 1})


def test_reject_duplicate_keys_at_parse():
    with pytest.raises(jcs.JcsError):
        jcs.loads('{"a":1,"a":2}')


def test_reject_non_json_constants_at_parse():
    for text in ("NaN", "Infinity", "-Infinity", '{"x": NaN}'):
        with pytest.raises(jcs.JcsError):
            jcs.loads(text)


def test_reject_unsafe_integer():
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize(2**53)
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize(-(2**53) - 1)


def test_reject_bom_at_parse():
    with pytest.raises((UnicodeDecodeError, ValueError)):
        jcs.loads(b'\xef\xbb\xbf{"a":1}')


# --- Structural behavior ------------------------------------------------------


def test_object_keys_sorted_by_utf16_code_units():
    # U+10000 is a surrogate pair (D800 DC00) in UTF-16 and sorts before
    # U+FFFF there, even though its code point is larger.
    value = {"\U00010000": 1, "\uffff": 2}
    assert jcs.canonicalize(value) == '{"\U00010000":1,"\uffff":2}'


def test_control_character_escaping():
    assert jcs.canonicalize("a\x00b") == '"a\\u0000b"'
    assert jcs.canonicalize('q"\\\b\f\n\r\t') == '"q\\"\\\\\\b\\f\\n\\r\\t"'
    assert jcs.canonicalize("\x1f") == '"\\u001f"'


def test_no_unicode_normalization():
    # Combining and precomposed forms stay byte-distinct (vector pair).
    assert jcs.canonicalize("é") != jcs.canonicalize("é")


def test_strict_roundtrip_parse():
    value = {"z": [1, "a", None, True], "a": {"b": 0.5}}
    assert jcs.loads(jcs.canonicalize(value)) == value


def test_reject_unknown_types():
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize(object())
    with pytest.raises(jcs.JcsError):
        jcs.canonicalize({1: "non-string key"})
