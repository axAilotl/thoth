"""CCF 0.2.0 Thoth↔Cissa interoperability boundary tests.

These tests cover the honest Thoth-owned portion of the delivery contract:
protocol identity negotiation, explicit current/read/refuse/uplift disposition,
fail-closed compatibility reports, corruption, downgrade/capability failure,
and replay/preview behavior. They do not perform canonical admission.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from ccf.archive import ArchiveError
from ccf.capsule import CapsuleError, load_capsule, write_capsule
from ccf.interop import (
    KNOWN_LEGACY_ROOTS,
    InteropCompatibilityError,
    InteropError,
    evaluate_compatibility,
    negotiate_identity,
)


CISSA_LEGACY_ROOT = (
    "sha256:447aa218156d0b33861090c5931bee78bc4a59300e94feacbcf89eb9d35dbc10"
)
THOTH_TRANSIENT_ROOT = (
    "sha256:34a285bb6e0c3713e89ca6c4c59df5abdd4b1bb3498abd1391d44674f035a5f7"
)
UNKNOWN_ROOT = "sha256:" + "0" * 64


@pytest.fixture
def rig(ccf_settings, tmp_path, ccf_package_root):
    from tests.ccf_helpers import make_rig

    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture
def capsule_with_current_root(ccf_capsule_example, tmp_path):
    """Copy of the example Capsule with the reconciled current root."""
    dest = tmp_path / "current-root-capsule"
    shutil.copytree(ccf_capsule_example, dest)
    return dest


@pytest.fixture
def capsule_with_cissa_legacy_root(capsule_with_current_root):
    """Copy of the example Capsule with Cissa's legacy semantic-catalog root."""
    manifest_path = capsule_with_current_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for dep in manifest["catalog_dependencies"]:
        if dep["kind"] == "semantic_catalog":
            dep["digest"] = CISSA_LEGACY_ROOT
            break
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return capsule_with_current_root


@pytest.fixture
def capsule_with_thoth_transient_root(capsule_with_current_root):
    """Copy of the example Capsule with Thoth's transient legacy root."""
    manifest_path = capsule_with_current_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for dep in manifest["catalog_dependencies"]:
        if dep["kind"] == "semantic_catalog":
            dep["digest"] = THOTH_TRANSIENT_ROOT
            break
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return capsule_with_current_root


@pytest.fixture
def capsule_with_unknown_root(capsule_with_current_root):
    """Copy of the example Capsule with an unrecognized semantic-catalog root."""
    manifest_path = capsule_with_current_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for dep in manifest["catalog_dependencies"]:
        if dep["kind"] == "semantic_catalog":
            dep["digest"] = UNKNOWN_ROOT
            break
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return capsule_with_current_root


class TestNegotiateIdentity:
    def test_current_root_disposition(self, rig, capsule_with_current_root):
        identity = rig.archive.negotiate_capsule(capsule_with_current_root)
        assert identity["root_disposition"] == "current"
        assert identity["catalog_root"] == rig.archive.semantic_catalog_root
        assert identity["portable_format"] == "ccf/0.1.2"
        assert identity["envelope_version"] == "0.2.0"

    def test_cissa_legacy_root_disposition(
        self, rig, capsule_with_cissa_legacy_root
    ):
        identity = rig.archive.negotiate_capsule(capsule_with_cissa_legacy_root)
        assert identity["root_disposition"] == "legacy_refuse"
        assert identity["catalog_root"] == CISSA_LEGACY_ROOT

    def test_thoth_transient_root_disposition(
        self, rig, capsule_with_thoth_transient_root
    ):
        identity = rig.archive.negotiate_capsule(capsule_with_thoth_transient_root)
        assert identity["root_disposition"] == "legacy_read"
        assert identity["catalog_root"] == THOTH_TRANSIENT_ROOT

    def test_unknown_root_disposition(self, rig, capsule_with_unknown_root):
        identity = rig.archive.negotiate_capsule(capsule_with_unknown_root)
        assert identity["root_disposition"] == "legacy_refuse"
        assert identity["catalog_root"] == UNKNOWN_ROOT

    def test_unsupported_capsule_format(self, rig):
        with pytest.raises(InteropError, match="unsupported capsule format"):
            negotiate_identity(
                {"format": "ccf.capsule/0.9.9"},
                {"portable_formats": ["ccf/0.1.2"], "semantic_catalog_roots": []},
            )

    def test_missing_portable_format(self, rig):
        with pytest.raises(InteropError, match="portable format"):
            negotiate_identity(
                {"format": "ccf.capsule/0.2.0"},
                {"portable_formats": ["ccf/0.9.9"]},
            )

    def test_missing_semantic_catalog_dependency(self, rig):
        with pytest.raises(InteropError, match="semantic_catalog"):
            negotiate_identity(
                {
                    "format": "ccf.capsule/0.2.0",
                    "level": "ccf-exchange-v1",
                    "catalog_dependencies": [],
                },
                {"portable_formats": ["ccf/0.1.2"], "semantic_catalog_roots": []},
            )


class TestImportDisposition:
    def test_current_root_returns_preview_not_admission(
        self, rig, capsule_with_current_root
    ):
        result = rig.archive.import_capsule(capsule_with_current_root)
        assert result["status"] == "preview"
        assert result["disposition"] == "current"
        assert result["admitted"] == []
        assert result["archive_id"] == rig.archive.archive_id
        assert "admission" not in result["note"].lower() or "not performed" in result["note"]
        assert all(
            entry["producer_authentication"] == "absent"
            for entry in result["uplift"]["objects"]
        )

    def test_legacy_refuse_default(self, rig, capsule_with_cissa_legacy_root):
        with pytest.raises(InteropCompatibilityError):
            rig.archive.import_capsule(capsule_with_cissa_legacy_root)

    def test_legacy_read_allowed_for_thoth_transient(
        self, rig, capsule_with_thoth_transient_root
    ):
        result = rig.archive.import_capsule(
            capsule_with_thoth_transient_root,
            legacy_root_policy="read",
        )
        assert result["status"] == "preview"
        assert result["disposition"] == "legacy_read"
        assert result["admitted"] == []

    def test_legacy_read_refused_when_policy_is_refuse(
        self, rig, capsule_with_thoth_transient_root
    ):
        with pytest.raises(InteropError, match="legacy_root_policy is refuse"):
            rig.archive.import_capsule(
                capsule_with_thoth_transient_root,
                legacy_root_policy="refuse",
            )

    def test_legacy_uplift_rejected(self, rig, capsule_with_thoth_transient_root):
        with pytest.raises(InteropError, match="catalog-transition Record"):
            rig.archive.import_capsule(
                capsule_with_thoth_transient_root,
                legacy_root_policy="uplift",
            )

    def test_unknown_root_reports_blockers(
        self, rig, capsule_with_unknown_root
    ):
        with pytest.raises(InteropCompatibilityError) as exc_info:
            rig.archive.import_capsule(capsule_with_unknown_root)
        blockers = exc_info.value.blockers
        types = {b.get("type") for b in blockers}
        assert "unknown-catalog-root" in types
        assert any(b.get("external_contract") == "cissa-99d.2" for b in blockers)

    def test_import_requires_package_root(self, rig):
        rig.archive.package_root = None
        with pytest.raises(ArchiveError, match="package_root"):
            rig.archive.import_capsule(Path("/nonexistent"))


class TestCompatibilityHarness:
    def test_reports_missing_authoritative_fixture(
        self, rig, capsule_with_current_root
    ):
        report = rig.archive.evaluate_capsule_compatibility(capsule_with_current_root)
        assert not report["pass"]
        assert any(
            b["type"] == "missing-authoritative-fixture"
            for b in report["blockers"]
        )

    def test_reports_cissa_root_mismatch(
        self, rig, capsule_with_cissa_legacy_root
    ):
        report = rig.archive.evaluate_capsule_compatibility(
            capsule_with_cissa_legacy_root
        )
        assert not report["pass"]
        assert any(
            b["type"] == "cissa-root-mismatch" for b in report["blockers"]
        )
        assert any(
            b["type"] == "root-not-current" for b in report["blockers"]
        )

    def test_reports_missing_carrier_contract_for_external_blobs(
        self, rig, capsule_with_current_root
    ):
        manifest = json.loads(
            (capsule_with_current_root / "manifest.json").read_text(encoding="utf-8")
        )
        manifest["extensions"]["blob_transfers"] = [
            {
                "blob_id": "urn:ccf:blob:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "carrier_url": "https://example.com/blob",
                "range": {"start": 0, "end": 1024},
            }
        ]
        (capsule_with_current_root / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        report = rig.archive.evaluate_capsule_compatibility(capsule_with_current_root)
        assert any(
            b["type"] == "missing-carrier-contract" for b in report["blockers"]
        )


class TestCorruptionAndDowngrade:
    def test_corrupted_stream_digest_fails_closed(
        self, rig, capsule_with_current_root
    ):
        stream_path = capsule_with_current_root / "submissions" / "records.ndjson"
        stream_path.write_text(stream_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        with pytest.raises(CapsuleError, match="digest mismatch"):
            rig.archive.import_capsule(capsule_with_current_root)

    def test_corruption_reported_by_compatibility_harness(
        self, rig, capsule_with_current_root
    ):
        stream_path = capsule_with_current_root / "submissions" / "records.ndjson"
        stream_path.write_text(stream_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        report = rig.archive.evaluate_capsule_compatibility(capsule_with_current_root)
        assert any(
            b["type"] in {"capsule-load-failed", "capsule-verification-failed"}
            for b in report["blockers"]
        )

    def test_capsule_capability_above_recipient(
        self, rig, capsule_with_current_root
    ):
        manifest = json.loads(
            (capsule_with_current_root / "manifest.json").read_text(encoding="utf-8")
        )
        manifest["capabilities"] = ["ccf-archive-encryption-derived-v1"]
        for stream in manifest["streams"]:
            stream["activation_requirements"]["capabilities"] = [
                "ccf-archive-encryption-derived-v1"
            ]
        (capsule_with_current_root / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        with pytest.raises(Exception, match="requires|exceeds recipient|cannot activate|unknown features"):
            rig.archive.import_capsule(capsule_with_current_root)


class TestReplayAndPreview:
    def test_preview_is_idempotent(self, rig, capsule_with_current_root):
        first = rig.archive.preview_capsule(capsule_with_current_root)
        second = rig.archive.preview_capsule(capsule_with_current_root)
        assert first["uplift"]["receipt_id"] != second["uplift"]["receipt_id"]
        assert first["uplift"]["status"] == second["uplift"]["status"] == "pending"
        assert (
            first["uplift"]["source_pack_id"]
            == second["uplift"]["source_pack_id"]
        )
        assert sorted(
            entry["source_id"] for entry in first["uplift"]["objects"]
        ) == sorted(
            entry["source_id"] for entry in second["uplift"]["objects"]
        )

    def test_import_capsule_does_not_mutate_archive(
        self, rig, capsule_with_current_root
    ):
        from ccf.db import open_ccf_connection

        before = rig.archive.head()
        rig.archive.import_capsule(capsule_with_current_root)
        after = rig.archive.head()
        assert after == before

    def test_import_capsule_uses_existing_verified_primitives(
        self, rig, capsule_with_current_root
    ):
        result = rig.archive.import_capsule(capsule_with_current_root)
        assert "uplift" in result
        assert "capsule" in result
        assert result["uplift"]["format"] == "ccf.uplift-receipt/0.2.0"
