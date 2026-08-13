"""Semantic catalog pinning tests (spec section 4.10)."""

from __future__ import annotations

import copy

import pytest

from ccf.catalog import CatalogError, SemanticCatalog, compute_catalog_root

EXPECTED_ROOT = "sha256:8e82e040fcf84b9ce5e2dca8371e1d227d7fd7ea2d14a56bbe2f4d56fc6082ad"


@pytest.fixture(scope="module")
def catalog(ccf_package_root):
    return SemanticCatalog.load(ccf_package_root)


def test_catalog_root_matches_published_value(catalog):
    assert catalog.root == EXPECTED_ROOT
    assert catalog.format == "ccf.semantic-catalog/0.1.2-rc1"
    assert catalog.version == "0.1.2-rc1"
    assert catalog.entries_verified is True


def test_catalog_entry_counts(catalog):
    assert len(catalog.schemas) == 112
    assert len(catalog.registries) == 14


def test_pinned_digest_lookups(catalog):
    assert (
        catalog.schema_digest("urn:ccf:schema:0.1.2-rc1:objects.record-header")
        == "sha256:a3594d1d334f008c853c763984b343bd7034dc47b6e73ea5f1f17c821084a0b0"
    )
    assert (
        catalog.schema_digest("urn:ccf:schema:0.1.2-rc1:payload.experience.utterance")
        == "sha256:10b7d9b29a72a63d5f296a8480ba521954b3c48f27bbedf509b5016f9b20a9a1"
    )
    assert (
        catalog.registry_digest("ccf.types/0.1.2-rc1")
        == "sha256:6d16ff342751ee9055a283c7c9e92b479296afe2654f269a60caf6e5866e58d7"
    )


def test_unknown_lookup_raises(catalog):
    with pytest.raises(KeyError):
        catalog.schema_digest("urn:ccf:schema:0.1.2-rc1:nonexistent")
    with pytest.raises(KeyError):
        catalog.registry_digest("ccf.nonexistent/0.1.2-rc1")


def test_tampered_root_rejected(ccf_package_root, load_ccf_json):
    document = load_ccf_json(ccf_package_root / "semantic-catalog.json")
    tampered = copy.deepcopy(document)
    tampered["root"] = "sha256:" + "0" * 64
    with pytest.raises(CatalogError, match="root mismatch"):
        SemanticCatalog.from_document(tampered)


def test_tampered_entry_rejected_by_root(ccf_package_root, load_ccf_json):
    # Any entry mutation invalidates the root even before artifact checks.
    document = load_ccf_json(ccf_package_root / "semantic-catalog.json")
    tampered = copy.deepcopy(document)
    tampered["schemas"][0]["digest"] = "sha256:" + "1" * 64
    with pytest.raises(CatalogError, match="root mismatch"):
        SemanticCatalog.from_document(tampered)


def test_tampered_artifact_rejected(ccf_package_root, load_ccf_json):
    # A swapped local artifact must fail closed even when the attacker
    # recomputes a self-consistent root over the tampered entry list.
    document = load_ccf_json(ccf_package_root / "semantic-catalog.json")
    tampered = copy.deepcopy(document)
    tampered["schemas"][0]["digest"] = "sha256:" + "1" * 64
    tampered["root"] = compute_catalog_root(tampered)
    with pytest.raises(CatalogError, match="digest mismatch"):
        SemanticCatalog.from_document(tampered, package_root=ccf_package_root)


def test_missing_artifact_rejected(ccf_package_root, load_ccf_json, tmp_path):
    document = load_ccf_json(ccf_package_root / "semantic-catalog.json")
    with pytest.raises(CatalogError, match="missing"):
        SemanticCatalog.from_document(document, package_root=tmp_path)


def test_missing_fields_rejected():
    with pytest.raises(CatalogError, match="missing field"):
        SemanticCatalog.from_document({"format": "ccf.semantic-catalog/0.1.2-rc1"})
