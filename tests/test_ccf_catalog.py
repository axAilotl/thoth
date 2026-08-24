"""Semantic catalog pinning tests (spec section 4.10)."""

from __future__ import annotations

import copy

import pytest

from ccf.catalog import CatalogError, SemanticCatalog, compute_catalog_root

EXPECTED_ROOT = "sha256:447aa218156d0b33861090c5931bee78bc4a59300e94feacbcf89eb9d35dbc10"


@pytest.fixture(scope="module")
def catalog(ccf_package_root):
    return SemanticCatalog.load(ccf_package_root)


def test_catalog_root_matches_published_value(catalog):
    assert catalog.root == EXPECTED_ROOT
    assert catalog.format == "ccf.semantic-catalog/0.1.2"
    assert catalog.version == "0.1.2"
    assert catalog.entries_verified is True


def test_catalog_entry_counts(catalog):
    assert len(catalog.schemas) == 112
    assert len(catalog.registries) == 14


def test_pinned_digest_lookups(catalog):
    assert (
        catalog.schema_digest("urn:ccf:schema:0.1.2:objects.record-header")
        == "sha256:d818b2c98b406b098fee3298ae26e6b45234fbb682fb18abb060522abd0e0b7c"
    )
    assert (
        catalog.schema_digest("urn:ccf:schema:0.1.2:payload.experience.utterance")
        == "sha256:8ad65b218975e7ea06236523cdb2622bbadcf871586a44db91185c9ef626180a"
    )
    assert (
        catalog.registry_digest("ccf.types/0.1.2")
        == "sha256:6b1296d6c9f98fe5ff67c47569af7223de532e5a70b1aafc04176b93ee8d51a3"
    )


def test_unknown_lookup_raises(catalog):
    with pytest.raises(KeyError):
        catalog.schema_digest("urn:ccf:schema:0.1.2:nonexistent")
    with pytest.raises(KeyError):
        catalog.registry_digest("ccf.nonexistent/0.1.2")


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
        SemanticCatalog.from_document({"format": "ccf.semantic-catalog/0.1.2"})
