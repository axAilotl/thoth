"""CCF 0.2.0 Thoth↔Cissa interoperability boundary tests."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from ccf.archive import ArchiveError
from ccf.interop import (
    CISSA_LEGACY_ROOT,
    THOTH_TRANSIENT_ROOT,
)

CISSA_LEGACY = CISSA_LEGACY_ROOT
THOTH_TRANSIENT = THOTH_TRANSIENT_ROOT
UNKNOWN = "sha256:" + "0" * 64


def _copy_capsule(source: Path, dest: Path) -> Path:
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(source, dest)
    return dest


def _set_catalog_root(capsule_dir: Path, root: str) -> Path:
    manifest_path = capsule_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for dep in manifest["catalog_dependencies"]:
        if dep["kind"] == "semantic_catalog":
            dep["digest"] = root
            break
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return capsule_dir


def _add_external_dependency(capsule_dir: Path) -> Path:
    manifest_path = capsule_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("dependencies", []).append(
        {
            "object_id": "urn:ccf:blob:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "availability": "external",
            "reason": "test external carrier",
            "locator": "https://example.invalid/blob",
            "source_custody_proof": None,
            "unavailability_lineage_id": None,
        }
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return capsule_dir


@pytest.fixture
def rig(ccf_settings, tmp_path, ccf_package_root):
    from tests.ccf_helpers import make_rig

    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture
def current_capsule(ccf_capsule_example, tmp_path):
    return _copy_capsule(ccf_capsule_example, tmp_path / "current")


@pytest.fixture
def cissa_capsule(current_capsule):
    return _set_catalog_root(current_capsule, CISSA_LEGACY)


@pytest.fixture
def thoth_capsule(current_capsule):
    return _set_catalog_root(current_capsule, THOTH_TRANSIENT)


@pytest.fixture
def unknown_capsule(current_capsule):
    return _set_catalog_root(current_capsule, UNKNOWN)


class TestNegotiateIdentity:
    def test_current_root(self, rig, current_capsule):
        identity = rig.archive.negotiate_capsule(current_capsule)
        assert identity["root"] == "current"
        assert identity["catalog_root"] == rig.archive.semantic_catalog_root
        assert not identity["known_legacy"]

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_known_legacy_root(self, rig, current_capsule, root):
        capsule = _set_catalog_root(_copy_capsule(current_capsule, current_capsule.parent / root[-12:]), root)
        identity = rig.archive.negotiate_capsule(capsule)
        assert identity["root"] == "legacy"
        assert identity["known_legacy"] is True
        assert identity["catalog_root"] == root
        assert identity["legacy_identity"]["name"]

    def test_unknown_root(self, rig, unknown_capsule):
        identity = rig.archive.negotiate_capsule(unknown_capsule)
        assert identity["root"] == "unknown"
        assert not identity["known_legacy"]


class TestInspectCapsule:
    def test_current_is_preview_not_admission(self, rig, current_capsule):
        result = rig.archive.inspect_capsule(current_capsule)
        assert result["status"] == "preview"
        assert result["disposition"] == "current"
        assert result["identity"]["root"] == "current"
        assert all(
            entry["producer_authentication"] == "absent"
            for entry in result["uplift"]["objects"]
        )
        assert "admitted" not in result

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_legacy_refuse(self, rig, current_capsule, root):
        capsule = _set_catalog_root(_copy_capsule(current_capsule, current_capsule.parent / f"refuse-{root[-12:]}"), root)
        with pytest.raises(ArchiveError, match="policy is refuse"):
            rig.archive.inspect_capsule(capsule)

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_legacy_read_inert_preview(self, rig, current_capsule, root):
        capsule = _set_catalog_root(_copy_capsule(current_capsule, current_capsule.parent / f"read-{root[-12:]}"), root)
        before = rig.archive.head()
        result = rig.archive.inspect_capsule(capsule, policy="read")
        assert result["disposition"] == "legacy_read"
        assert result["status"] == "preview"
        assert result["identity"]["root"] == "legacy"
        assert result["identity"]["catalog_root"] == root
        assert all(
            entry["producer_authentication"] == "absent"
            for entry in result["uplift"]["objects"]
        )
        assert all(
            entry["source_id"] == entry["canonical_id"]
            for entry in result["uplift"]["objects"]
        )
        assert rig.archive.head() == before
        assert "admitted" not in result

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_legacy_uplift_pending_only(self, rig, current_capsule, root):
        capsule = _set_catalog_root(_copy_capsule(current_capsule, current_capsule.parent / f"uplift-{root[-12:]}"), root)
        result = rig.archive.inspect_capsule(capsule, policy="uplift")
        assert result["disposition"] == "legacy_uplift"
        assert result["status"] == "pending_uplift"
        assert result["uplift"]["status"] == "pending"
        assert all(
            entry["producer_authentication"] == "absent"
            for entry in result["uplift"]["objects"]
        )

    def test_unknown_root_refused(self, rig, unknown_capsule):
        with pytest.raises(ArchiveError, match="not recognized"):
            rig.archive.inspect_capsule(unknown_capsule)

    def test_unknown_policy_rejected(self, rig, current_capsule):
        with pytest.raises(ArchiveError, match="unknown Capsule inspect policy"):
            rig.archive.inspect_capsule(current_capsule, policy="admit")


class TestCompatibility:
    def test_current_root_passes_without_external_deps(self, rig, current_capsule):
        manifest_path = current_capsule / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["dependencies"] = []
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is True
        assert report["blockers"] == []
        assert report["identity"]["root"] == "current"

    def test_current_root_fails_with_external_carrier(self, rig, current_capsule):
        _add_external_dependency(current_capsule)
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is False
        assert any(b["type"] == "missing-carrier-contract" for b in report["blockers"])

    def test_cissa_root_reports_mismatch(self, rig, cissa_capsule):
        report = rig.archive.evaluate_capsule_compatibility(cissa_capsule)
        assert report["pass"] is False
        assert any(b["type"] == "cissa-root-mismatch" for b in report["blockers"])
        assert any(b["type"] == "root-not-current" for b in report["blockers"])

    def test_thoth_transient_root_reports_root_not_current(self, rig, thoth_capsule):
        report = rig.archive.evaluate_capsule_compatibility(thoth_capsule)
        assert report["pass"] is False
        assert any(b["type"] == "root-not-current" for b in report["blockers"])
        assert not any(b["type"] == "cissa-root-mismatch" for b in report["blockers"])

    def test_unknown_root_honest_refusal(self, rig, unknown_capsule):
        report = rig.archive.evaluate_capsule_compatibility(unknown_capsule)
        assert report["pass"] is False
        assert any(b["type"] == "unknown-root" for b in report["blockers"])
        assert not any(b["type"] == "cissa-root-mismatch" for b in report["blockers"])

    def test_corruption_fail_closed(self, rig, current_capsule):
        stream_path = current_capsule / "submissions" / "records.ndjson"
        stream_path.write_text(stream_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        with pytest.raises(ArchiveError, match="digest mismatch"):
            rig.archive.inspect_capsule(current_capsule)

    def test_capability_fail_closed(self, rig, current_capsule):
        manifest_path = current_capsule / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["capabilities"] = ["ccf-archive-encryption-derived-v1"]
        for stream in manifest["streams"]:
            stream["activation_requirements"]["capabilities"] = [
                "ccf-archive-encryption-derived-v1"
            ]
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is False
        assert any(
            b["type"] == "capsule-verification-failed" for b in report["blockers"]
        )


class TestReplayAndCrossRepo:
    def test_preview_idempotent(self, rig, current_capsule):
        first = rig.archive.preview_capsule(current_capsule)
        second = rig.archive.preview_capsule(current_capsule)
        assert first["uplift"]["source_pack_id"] == second["uplift"]["source_pack_id"]
        assert first["uplift"]["status"] == second["uplift"]["status"] == "pending"
        assert sorted(
            e["source_id"] for e in first["uplift"]["objects"]
        ) == sorted(e["source_id"] for e in second["uplift"]["objects"])

    def test_cross_repo_status_reports_external_gaps(self, rig):
        status = rig.archive.cross_repo_conformance_status()
        assert status["pass"] is False
        types = {b["type"] for b in status["blockers"]}
        assert "cissa-root-mismatch" in types
        assert "missing-authoritative-fixture" in types
        assert "missing-carrier-contract" in types

    def test_no_implied_admission(self, rig, current_capsule):
        from ccf.db import open_ccf_connection

        before = rig.archive.head()
        result = rig.archive.inspect_capsule(current_capsule)
        assert result["disposition"] in {"current", "legacy_read", "legacy_uplift"}
        assert rig.archive.head() == before
