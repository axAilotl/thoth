"""Semantic catalog pinning tests (spec section 4.10)."""

from __future__ import annotations

import copy

import pytest

from ccf.catalog import CatalogError, SemanticCatalog, compute_catalog_root

EXPECTED_ROOT = "sha256:1d4986414a6c0e76ee979e654487df739a96604593d7a18f8765643f3c32cc9a"


@pytest.fixture(scope="module")
def catalog(ccf_package_root):
    return SemanticCatalog.load(ccf_package_root)


def test_catalog_root_matches_published_value(catalog):
    assert catalog.root == EXPECTED_ROOT
    assert catalog.format == "ccf.semantic-catalog/0.1.1"
    assert catalog.version == "0.1.1"
    assert catalog.entries_verified is True


def test_catalog_entry_counts(catalog):
    assert len(catalog.schemas) == 107
    assert len(catalog.registries) == 12


def test_pinned_digest_lookups(catalog):
    assert (
        catalog.schema_digest("urn:ccf:schema:0.1.1:objects.record-header")
        == "sha256:28963a2c532dc87be0595a850b80e98158ec4d5f4d566da7158f67012bf9416b"
    )
    assert (
        catalog.schema_digest("urn:ccf:schema:0.1.1:payload.experience.utterance")
        == "sha256:b3aaed4d84dce259d6c2c57ba75e624b2dd28e88567a3917f2787ccef1fb0d4f"
    )
    assert (
        catalog.registry_digest("ccf.types/0.1.1")
        == "sha256:a7af3c2bb467b5d1ca0dce86bacc6b594784e854310183c5c81185bb4853ea89"
    )


def test_unknown_lookup_raises(catalog):
    with pytest.raises(KeyError):
        catalog.schema_digest("urn:ccf:schema:0.1.1:nonexistent")
    with pytest.raises(KeyError):
        catalog.registry_digest("ccf.nonexistent/0.1.1")


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
        SemanticCatalog.from_document({"format": "ccf.semantic-catalog/0.1.1"})
