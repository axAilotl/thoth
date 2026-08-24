"""Hash-profile conformance tests for ``ccf-jcs-sha256-v2``.

Reproduces every published vector: object hashes (all three kinds plus the
Blob content commitment), submission hashes, Merkle roots (empty, 4-member,
16-member mixed-kind), numeric admission ordering, the signed producer
batch, and commit signing with completed ``commit_hash`` values.
"""

from __future__ import annotations

import copy

import pytest
from cryptography.exceptions import InvalidSignature

from ccf.hashing import (
    CcfHashError,
    blob_content_commitment,
    commit_signing_digest,
    compartment_commitment,
    decode_b64url,
    encode_b64url,
    load_private_key,
    load_public_key,
    merkle_root,
    object_hash,
    parse_digest,
    producer_batch_hash,
    producer_batch_signing_digest,
    public_key_b64url,
    sign_digest,
    submission_hash,
    verify_digest,
)
from ccf.objects import admission_order_key


@pytest.fixture(scope="module")
def object_vectors(ccf_vectors_dir, load_ccf_json):
    return load_ccf_json(ccf_vectors_dir / "object-hashes.json")


@pytest.fixture(scope="module")
def batch_vector(ccf_vectors_dir, load_ccf_json):
    return load_ccf_json(ccf_vectors_dir / "producer-batch.json")


@pytest.fixture(scope="module")
def commit_vectors(ccf_vectors_dir, load_ccf_json):
    return load_ccf_json(ccf_vectors_dir / "commit-signing.json")


# --- Section 4.3/4.4/4.5: compartment commitments and object hashes ----------


@pytest.mark.parametrize("kind", ["record", "link", "blob"])
def test_compartment_commitments(object_vectors, kind):
    vector = object_vectors[kind]
    assert (
        compartment_commitment(kind, "structural", vector["structural"])
        == vector["expected_structural_commitment"]
    )
    assert (
        compartment_commitment(kind, "semantic", vector["semantic"])
        == vector["expected_semantic_commitment"]
    )


@pytest.mark.parametrize("kind", ["record", "link", "blob"])
def test_object_hash(object_vectors, kind):
    vector = object_vectors[kind]
    assert object_hash(vector["header"]) == vector["expected_object_hash"]


def test_blob_content_commitment(object_vectors, ccf_examples_dir):
    blob = object_vectors["blob"]
    content_salt = blob["semantic"]["content"]["content_salt"]
    data = (ccf_examples_dir / "segment-1842.wav").read_bytes()
    assert (
        blob_content_commitment(content_salt, data)
        == blob["expected_content_commitment"]
    )


def test_object_hash_excludes_object_hash_field_only(object_vectors):
    header = object_vectors["record"]["header"]
    hashed = object_hash(header)
    without = {k: v for k, v in header.items() if k != "object_hash"}
    assert object_hash(without) == hashed


def test_wrong_salt_length_rejected(object_vectors):
    envelope = copy.deepcopy(object_vectors["record"]["structural"])
    envelope["salt"] = encode_b64url(b"too short")
    with pytest.raises(CcfHashError):
        compartment_commitment("record", "structural", envelope)


def test_unknown_kind_or_compartment_rejected(object_vectors):
    envelope = object_vectors["record"]["structural"]
    with pytest.raises(CcfHashError):
        compartment_commitment("widget", "structural", envelope)
    with pytest.raises(CcfHashError):
        compartment_commitment("record", "middle", envelope)


def test_tampered_content_changes_commitment(object_vectors):
    envelope = copy.deepcopy(object_vectors["record"]["semantic"])
    envelope["content"]["payload"]["text"] = "tampered"
    assert (
        compartment_commitment("record", "semantic", envelope)
        != object_vectors["record"]["expected_semantic_commitment"]
    )


# --- Section 4.6: submission hashes -------------------------------------------


def test_submission_hashes(ccf_vectors_dir, load_ccf_json, batch_vector):
    vectors = load_ccf_json(ccf_vectors_dir / "submission-hashes.json")
    batch = batch_vector["batch"]
    submissions = {s["id"]: s for s in (*batch["records"], *batch["links"], *batch["blobs"])}
    entries = [*vectors["records"], *vectors["links"], *vectors["blobs"]]
    assert len(entries) == 16
    for entry in entries:
        submission = submissions.get(entry["id"])
        assert submission is not None, entry["id"]
        assert submission_hash(submission) == entry["expected_submission_hash"], entry["id"]


# --- Section 4.8: Merkle roots -------------------------------------------------


def test_merkle_roots(ccf_vectors_dir, load_ccf_json):
    vectors = load_ccf_json(ccf_vectors_dir / "merkle.json")
    assert merkle_root([]) == vectors["empty_expected"]
    assert merkle_root(vectors["commit1"]["members"]) == vectors["commit1"]["expected_root"]
    assert merkle_root(vectors["commit2"]["members"]) == vectors["commit2"]["expected_root"]


def test_merkle_root_is_order_independent_by_position(ccf_vectors_dir, load_ccf_json):
    vectors = load_ccf_json(ccf_vectors_dir / "merkle.json")
    members = list(reversed(vectors["commit2"]["members"]))
    assert merkle_root(members) == vectors["commit2"]["expected_root"]


def test_merkle_duplicate_position_rejected(ccf_vectors_dir, load_ccf_json):
    vectors = load_ccf_json(ccf_vectors_dir / "merkle.json")
    members = copy.deepcopy(vectors["commit1"]["members"])
    members[1]["commit_position"] = 0
    with pytest.raises(CcfHashError):
        merkle_root(members)


def test_merkle_position_gap_rejected(ccf_vectors_dir, load_ccf_json):
    vectors = load_ccf_json(ccf_vectors_dir / "merkle.json")
    members = copy.deepcopy(vectors["commit1"]["members"])
    members[3]["commit_position"] = 7
    with pytest.raises(CcfHashError):
        merkle_root(members)


def test_merkle_tampered_member_changes_root(ccf_vectors_dir, load_ccf_json):
    vectors = load_ccf_json(ccf_vectors_dir / "merkle.json")
    members = copy.deepcopy(vectors["commit1"]["members"])
    members[0]["object_hash"] = (
        "sha256:" + "0" * 64
    )
    assert merkle_root(members) != vectors["commit1"]["expected_root"]


# --- Section 4.2: numeric admission ordering -----------------------------------


def test_numeric_commit_sequence_ordering(ccf_vectors_dir, load_ccf_json):
    vector = load_ccf_json(ccf_vectors_dir / "ordering.json")
    ordered = vector["ordered"]
    shuffled = [ordered[i] for i in (1, 3, 0, 2)]
    result = sorted(
        shuffled,
        key=lambda m: admission_order_key(m["commit_sequence"], m["commit_position"]),
    )
    assert result == ordered


def test_lexicographic_ordering_is_invalid(ccf_vectors_dir, load_ccf_json):
    vector = load_ccf_json(ccf_vectors_dir / "ordering.json")
    assert vector["lexicographic_is_invalid"] is True
    lexicographic = sorted(
        vector["ordered"], key=lambda m: (m["commit_sequence"], m["commit_position"])
    )
    assert lexicographic != vector["ordered"]


# --- Section 4.7: producer batch hash and Ed25519 signature --------------------


def test_producer_batch_hash(batch_vector):
    assert producer_batch_hash(batch_vector["batch"]) == batch_vector["expected_batch_hash"]


def test_producer_batch_signature_verifies(ccf_vectors_dir, batch_vector):
    batch = batch_vector["batch"]
    public_key = load_public_key(ccf_vectors_dir / "device-ed25519-public.pem")
    digest = producer_batch_signing_digest(batch["batch_hash"])
    verify_digest(public_key, decode_b64url(batch["signature"]), digest)
    assert batch_vector["expected_signature_valid"] is True


def test_producer_batch_signature_reproduced_with_test_key(
    ccf_test_only_keys_dir, batch_vector
):
    batch = batch_vector["batch"]
    # The 0.1.2 package pins the same TEST-ONLY key material as 0.1.1 (the
    # private PEM is vendored in the final vectors tree under a gitignore
    # exception and pinned in SHA256SUMS).
    private_key = load_private_key(
        ccf_test_only_keys_dir / "TEST-ONLY-device-ed25519-private.pem"
    )
    digest = producer_batch_signing_digest(producer_batch_hash(batch))
    signature = sign_digest(private_key, digest)
    # Ed25519 is deterministic: signing reproduces the vector signature.
    assert encode_b64url(signature) == batch["signature"]


def test_producer_batch_tampering_breaks_hash_and_signature(ccf_vectors_dir, batch_vector):
    tampered = copy.deepcopy(batch_vector["batch"])
    tampered["producer_sequence"] = "2"
    tampered_hash = producer_batch_hash(tampered)
    assert tampered_hash != batch_vector["expected_batch_hash"]
    public_key = load_public_key(ccf_vectors_dir / "device-ed25519-public.pem")
    with pytest.raises(InvalidSignature):
        verify_digest(
            public_key,
            decode_b64url(batch_vector["batch"]["signature"]),
            producer_batch_signing_digest(tampered_hash),
        )


# --- Section 4.9: commit signing and commit_hash --------------------------------


@pytest.mark.parametrize("commit_name", ["genesis", "commit1", "commit2"])
def test_commit_signing_digest(commit_vectors, commit_name):
    vector = commit_vectors[commit_name]
    digest = commit_signing_digest(
        vector["signing_header"], vector["structural_content_without_signature"]
    )
    assert "sha256:" + digest.hex() == vector["expected_signing_digest"]


@pytest.mark.parametrize("commit_name", ["genesis", "commit1", "commit2"])
def test_commit_signature_verifies(ccf_vectors_dir, commit_vectors, commit_name):
    vector = commit_vectors[commit_name]
    public_key = load_public_key(ccf_vectors_dir / "archive-ed25519-public.pem")
    digest = commit_signing_digest(
        vector["signing_header"], vector["structural_content_without_signature"]
    )
    verify_digest(public_key, decode_b64url(vector["signature"]), digest)
    assert vector["expected_signature_valid"] is True


@pytest.mark.parametrize("commit_name", ["genesis", "commit1", "commit2"])
def test_commit_signature_reproduced_with_test_key(
    ccf_test_only_keys_dir, commit_vectors, commit_name
):
    vector = commit_vectors[commit_name]
    private_key = load_private_key(
        ccf_test_only_keys_dir / "TEST-ONLY-archive-ed25519-private.pem"
    )
    digest = commit_signing_digest(
        vector["signing_header"], vector["structural_content_without_signature"]
    )
    assert encode_b64url(sign_digest(private_key, digest)) == vector["signature"]


@pytest.mark.parametrize("commit_name", ["genesis", "commit1", "commit2"])
def test_commit_structural_commitment_and_hash(commit_vectors, commit_name):
    vector = commit_vectors[commit_name]
    assert (
        compartment_commitment("record", "structural", vector["structural"])
        == vector["header"]["structural_commitment"]
    )
    assert object_hash(vector["header"]) == vector["expected_commit_hash"]


def test_commit_chain_linkage(commit_vectors):
    genesis_hash = commit_vectors["genesis"]["expected_commit_hash"]
    commit1_hash = commit_vectors["commit1"]["expected_commit_hash"]
    payload1 = commit_vectors["commit1"]["structural_content_without_signature"][
        "structural_payload"
    ]
    payload2 = commit_vectors["commit2"]["structural_content_without_signature"][
        "structural_payload"
    ]
    assert payload1["parent_commit_hash"] == genesis_hash
    assert payload2["parent_commit_hash"] == commit1_hash
    assert payload1["sequence"] == "1"
    assert payload2["sequence"] == "2"


def test_commit_member_roots_match_merkle_vectors(
    ccf_vectors_dir, load_ccf_json, commit_vectors
):
    merkle_vectors = load_ccf_json(ccf_vectors_dir / "merkle.json")
    payload1 = commit_vectors["commit1"]["structural_content_without_signature"][
        "structural_payload"
    ]
    payload2 = commit_vectors["commit2"]["structural_content_without_signature"][
        "structural_payload"
    ]
    assert payload1["batch_merkle_root"] == merkle_vectors["commit1"]["expected_root"]
    assert payload2["batch_merkle_root"] == merkle_vectors["commit2"]["expected_root"]
    assert int(payload1["member_count"]) == len(merkle_vectors["commit1"]["members"])
    assert int(payload2["member_count"]) == len(merkle_vectors["commit2"]["members"])
    # Genesis commits over the empty-batch root.
    payload0 = commit_vectors["genesis"]["structural_content_without_signature"][
        "structural_payload"
    ]
    assert payload0["batch_merkle_root"] == merkle_vectors["empty_expected"]
    assert payload0["member_count"] == "0"


def test_commit_signer_public_key_matches_header(ccf_vectors_dir, commit_vectors):
    public_key = load_public_key(ccf_vectors_dir / "archive-ed25519-public.pem")
    signer = commit_vectors["genesis"]["structural_content_without_signature"][
        "structural_payload"
    ]["signer_public_key"]
    assert public_key_b64url(public_key) == signer


def test_commit_tampering_breaks_signature(ccf_vectors_dir, commit_vectors):
    vector = commit_vectors["commit1"]
    tampered = copy.deepcopy(vector["structural_content_without_signature"])
    tampered["structural_payload"]["committed_at"] = "2026-08-11T21:40:02.000Z"
    digest = commit_signing_digest(vector["signing_header"], tampered)
    public_key = load_public_key(ccf_vectors_dir / "archive-ed25519-public.pem")
    with pytest.raises(InvalidSignature):
        verify_digest(public_key, decode_b64url(vector["signature"]), digest)


# --- Digest/base64url helpers ---------------------------------------------------


def test_parse_digest_strict():
    raw = parse_digest("sha256:" + "ab" * 32)
    assert len(raw) == 32
    for bad in ("sha256:" + "AB" * 32, "sha256:" + "ab" * 31, "md5:" + "ab" * 32):
        with pytest.raises(CcfHashError):
            parse_digest(bad)


def test_b64url_roundtrip_and_padding_rejection():
    data = bytes(range(32))
    assert decode_b64url(encode_b64url(data)) == data
    with pytest.raises(CcfHashError):
        decode_b64url("not valid!")
