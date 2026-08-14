"""Independent reproduction coverage for every published CCF vector."""

from __future__ import annotations

import ast
from pathlib import Path

# ---------------------------------------------------------------------------
# Gate 1: independent vector reproduction
# ---------------------------------------------------------------------------

#: Every file the spec package publishes under vectors/ must be consumed
#: (reproduced or loaded) by at least one pure-Python test module. The
#: package's own JS tooling is never invoked anywhere in this suite.
PUBLISHED_VECTOR_FILES = {
    "canonicalization.json",
    "commit-signing.json",
    "merkle.json",
    "object-hashes.json",
    "ordering.json",
    "producer-batch.json",
    "submission-hashes.json",
    "admission-authority-classes.json",
    "conformance-0.1.2.json",
    "foreign-unavailability.json",
    "mindpack-manifest-tamper.json",
    "suppression-canonical.json",
    "archive-ed25519-public.pem",
    "device-ed25519-public.pem",
    "TEST-ONLY-archive-ed25519-private.pem",
    "TEST-ONLY-device-ed25519-private.pem",
}

# Explicit vector-to-independent-test contract. The gate parses the named
# modules and requires every listed test function to exist, so listing a vector
# filename inside this gate cannot satisfy its own coverage proof.
VECTOR_REPRODUCTION_CASES = {
    "canonicalization.json": {
        "test_ccf_jcs.py": {
            "test_every_vector_case_is_covered",
            "test_canonical_serialization_matches_vector",
            "test_canonical_digest_matches_vector",
        },
    },
    "commit-signing.json": {
        "test_ccf_hashing.py": {
            "test_commit_signing_digest",
            "test_commit_signature_verifies",
            "test_commit_signature_reproduced_with_test_key",
        },
    },
    "merkle.json": {
        "test_ccf_hashing.py": {
            "test_merkle_roots",
            "test_merkle_root_is_order_independent_by_position",
            "test_merkle_duplicate_position_rejected",
        },
    },
    "object-hashes.json": {
        "test_ccf_hashing.py": {
            "test_compartment_commitments",
            "test_object_hash",
            "test_blob_content_commitment",
        },
    },
    "ordering.json": {
        "test_ccf_hashing.py": {
            "test_numeric_commit_sequence_ordering",
            "test_lexicographic_ordering_is_invalid",
        },
    },
    "producer-batch.json": {
        "test_ccf_hashing.py": {
            "test_producer_batch_hash",
            "test_producer_batch_signature_verifies",
            "test_producer_batch_signature_reproduced_with_test_key",
        },
    },
    "submission-hashes.json": {
        "test_ccf_hashing.py": {"test_submission_hashes"},
    },
    "admission-authority-classes.json": {
        "test_ccf_012.py": {"test_authority_class_vector_matrix"},
    },
    "conformance-0.1.2.json": {
        "test_ccf_012.py": {"test_conformance_case_coverage"},
    },
    "foreign-unavailability.json": {
        "test_ccf_012.py": {"test_foreign_unavailability_vector"},
    },
    "mindpack-manifest-tamper.json": {
        "test_ccf_manifest_verification.py": {
            "test_manifest_tamper_vector_coverage"
        },
    },
    "suppression-canonical.json": {
        "test_ccf_012.py": {
            "test_suppression_canonical_vector",
            "test_suppression_preimage_rejections",
        },
    },
    "archive-ed25519-public.pem": {
        "test_ccf_hashing.py": {"test_commit_signature_verifies"},
    },
    "device-ed25519-public.pem": {
        "test_ccf_hashing.py": {"test_producer_batch_signature_verifies"},
    },
    "TEST-ONLY-archive-ed25519-private.pem": {
        "test_ccf_hashing.py": {"test_commit_signature_reproduced_with_test_key"},
    },
    "TEST-ONLY-device-ed25519-private.pem": {
        "test_ccf_hashing.py": {"test_producer_batch_signature_reproduced_with_test_key"},
    },
}


def test_gate1_every_published_vector_is_reproduced_from_tests(ccf_vectors_dir):
    published = {p.name for p in ccf_vectors_dir.iterdir() if p.is_file()}
    published.discard("README.md")
    assert published == PUBLISHED_VECTOR_FILES, (
        f"spec vectors changed; update the gate: {sorted(published)}"
    )
    assert set(VECTOR_REPRODUCTION_CASES) == published
    test_root = Path(__file__).parent
    for vector_name, modules in VECTOR_REPRODUCTION_CASES.items():
        for module_name, expected_cases in modules.items():
            module_path = test_root / module_name
            source = module_path.read_text(encoding="utf-8")
            assert vector_name in source, f"{module_name} does not load {vector_name}"
            tree = ast.parse(source, filename=str(module_path))
            actual_cases = {
                node.name
                for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            missing_cases = expected_cases - actual_cases
            assert not missing_cases, (
                f"{vector_name} reproduction cases missing from {module_name}: "
                f"{sorted(missing_cases)}"
            )


def test_gate1_vector_counts_are_stable(ccf_vectors_dir, load_ccf_json):
    """Pin the published vector counts the suite reproduces (§10 gate 1)."""
    canon = load_ccf_json(ccf_vectors_dir / "canonicalization.json")
    assert len(canon["cases"]) == 7
    assert len(canon["rejections"]) == 7
    merkle = load_ccf_json(ccf_vectors_dir / "merkle.json")
    assert len(merkle["commit1"]["members"]) >= 1
    assert len(merkle["commit2"]["members"]) == 19
    hashes = load_ccf_json(ccf_vectors_dir / "object-hashes.json")
    assert set(hashes) >= {"record", "link", "blob"}
