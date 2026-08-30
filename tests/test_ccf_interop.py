"""CCF 0.2.0 Thoth↔Cissa interoperability boundary tests."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from ccf.archive import ArchiveError
from ccf.db import open_ccf_connection
from ccf.interop import (
    CISSA_LEGACY_ROOT,
    THOTH_TRANSIENT_ROOT,
)
from ccf.layered import raw_digest

CISSA_LEGACY = CISSA_LEGACY_ROOT
THOTH_TRANSIENT = THOTH_TRANSIENT_ROOT
UNKNOWN = "sha256:" + "0" * 64
FROZEN_CLOCK = "2026-08-30T12:00:00.000Z"


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


def _strip_external_dependencies(capsule_dir: Path) -> Path:
    manifest_path = capsule_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["dependencies"] = []
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return capsule_dir


def _integrity_consistent_missing_submission_id(
    capsule_dir: Path, dest: Path
) -> Path:
    """Return a copy of ``capsule_dir`` with one submission missing its ``id``.

    The submissions stream digest and ``byte_length`` in the manifest are
    recomputed so the capsule passes ``load_capsule`` integrity checks, then
    fails ``verify_capsule`` with a shape error rather than a schema error.
    """
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(capsule_dir, dest)
    stream_path = dest / "submissions" / "records.ndjson"
    values = [json.loads(line) for line in stream_path.read_text(encoding="utf-8").splitlines()]
    # Remove ``id`` from a non-root record so the root remains present.
    values[1].pop("id")
    encoded = "".join(
        json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n"
        for value in values
    ).encode("utf-8")
    stream_path.write_bytes(encoded)

    manifest_path = dest / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for spec in manifest["streams"]:
        if spec["path"] == "submissions/records.ndjson":
            spec["digest"] = raw_digest(encoded)
            spec["byte_length"] = str(len(encoded))
            break
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return dest


@pytest.fixture
def rig(ccf_settings, tmp_path, ccf_package_root):
    from tests.ccf_helpers import make_rig

    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture
def current_capsule(ccf_capsule_example, tmp_path):
    return _strip_external_dependencies(
        _copy_capsule(ccf_capsule_example, tmp_path / "current")
    )


@pytest.fixture
def cissa_capsule(current_capsule):
    return _set_catalog_root(
        _copy_capsule(current_capsule, current_capsule.parent / "cissa"),
        CISSA_LEGACY,
    )


@pytest.fixture
def thoth_capsule(current_capsule):
    return _set_catalog_root(
        _copy_capsule(current_capsule, current_capsule.parent / "thoth"),
        THOTH_TRANSIENT,
    )


@pytest.fixture
def unknown_capsule(current_capsule):
    return _set_catalog_root(
        _copy_capsule(current_capsule, current_capsule.parent / "unknown"),
        UNKNOWN,
    )


def _frozen_rig(rig, timestamp: str = FROZEN_CLOCK):
    """Replace the archive clock with a frozen timestamp."""
    rig.archive.clock = lambda: timestamp
    return rig


def _archive_state(rig):
    """Snapshot head, object count, and journal count for one archive."""
    with open_ccf_connection(rig.archive.settings) as conn:
        head = conn.execute(
            """
            SELECT sequence, commit_record_id, commit_hash FROM archive_head
            WHERE archive_id = %s
            """,
            (rig.archive.archive_id,),
        ).fetchone()
        object_count = conn.execute(
            "SELECT COUNT(*) FROM object_header WHERE archive_id = %s",
            (rig.archive.archive_id,),
        ).fetchone()[0]
        journal_count = conn.execute(
            "SELECT COUNT(*) FROM commit_journal WHERE archive_id = %s",
            (rig.archive.archive_id,),
        ).fetchone()[0]
    return {
        "head": {
            "sequence": str(int(head[0])),
            "commit_record_id": head[1],
            "commit_hash": head[2],
        },
        "object_count": object_count,
        "journal_count": journal_count,
    }


def _add_required_catalog_dependency(capsule_dir: Path, root: str) -> Path:
    """Append an additional required semantic_catalog dependency."""
    manifest_path = capsule_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["catalog_dependencies"].append(
        {
            "kind": "semantic_catalog",
            "required": True,
            "digest": root,
        }
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return capsule_dir


class TestNegotiateIdentity:
    def test_current_root(self, rig, current_capsule):
        identity = rig.archive.negotiate_capsule(current_capsule)
        assert identity["root"] == "current"
        assert identity["catalog_root"] == rig.archive.semantic_catalog_root
        assert identity["roles"]
        assert not identity["known_legacy"]

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_known_legacy_root(self, rig, current_capsule, root):
        capsule = _set_catalog_root(
            _copy_capsule(current_capsule, current_capsule.parent / root[-12:]),
            root,
        )
        identity = rig.archive.negotiate_capsule(capsule)
        assert identity["root"] == "legacy"
        assert identity["known_legacy"] is True
        assert identity["catalog_root"] == root
        assert identity["legacy_identity"]["name"]
        assert identity["roles"]

        identity["legacy_identity"]["name"] = "caller-mutated"
        replay = rig.archive.negotiate_capsule(capsule)
        assert replay["legacy_identity"]["name"] != "caller-mutated"

    def test_unknown_root(self, rig, unknown_capsule):
        identity = rig.archive.negotiate_capsule(unknown_capsule)
        assert identity["root"] == "unknown"
        assert not identity["known_legacy"]

    @pytest.mark.parametrize("extra", [CISSA_LEGACY, THOTH_TRANSIENT, UNKNOWN])
    def test_multiple_required_catalog_roots_rejected(
        self, rig, current_capsule, extra
    ):
        capsule = _add_required_catalog_dependency(
            _copy_capsule(current_capsule, current_capsule.parent / f"ambig-{extra[-12:]}"),
            extra,
        )
        with pytest.raises(
            ArchiveError, match="exactly one required semantic_catalog dependency"
        ):
            rig.archive.negotiate_capsule(capsule)

    def test_multiple_required_catalog_roots_rejected_reversed(
        self, rig, current_capsule
    ):
        """Ambiguity must fail closed regardless of dependency order."""
        first = _copy_capsule(
            current_capsule, current_capsule.parent / "ambig-reversed"
        )
        manifest_path = first / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["catalog_dependencies"] = [
            {"kind": "semantic_catalog", "required": True, "digest": CISSA_LEGACY},
            {"kind": "semantic_catalog", "required": True, "digest": UNKNOWN},
        ]
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        with pytest.raises(
            ArchiveError, match="exactly one required semantic_catalog dependency"
        ):
            rig.archive.negotiate_capsule(first)

    @pytest.mark.parametrize("digest", ["", "   "])
    def test_required_catalog_root_blank_rejected(
        self, rig, current_capsule, digest
    ):
        capsule = _copy_capsule(
            current_capsule, current_capsule.parent / "blank-root"
        )
        manifest_path = capsule / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for dep in manifest["catalog_dependencies"]:
            if dep["kind"] == "semantic_catalog" and dep.get("required") is True:
                dep["digest"] = digest
                break
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        with pytest.raises(
            ArchiveError, match="nonblank string digest"
        ):
            rig.archive.negotiate_capsule(capsule)

    def test_required_catalog_root_non_string_rejected(self, rig, current_capsule):
        capsule = _copy_capsule(
            current_capsule, current_capsule.parent / "non-string-root"
        )
        manifest_path = capsule / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for dep in manifest["catalog_dependencies"]:
            if dep["kind"] == "semantic_catalog" and dep.get("required") is True:
                dep["digest"] = ["sha256:not-a-string"]
                break
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        with pytest.raises(
            ArchiveError, match="nonblank string digest"
        ):
            rig.archive.negotiate_capsule(capsule)


class TestInspectCapsule:
    def test_current_is_preview_not_admission(self, rig, current_capsule):
        result = rig.archive.inspect_capsule(current_capsule)
        assert result["status"] == "preview"
        assert result["disposition"] == "current"
        assert result["identity"]["root"] == "current"
        assert "uplift" in result
        assert all(
            entry["producer_authentication"] == "absent"
            for entry in result["uplift"]["objects"]
        )
        assert "admitted" not in result
        assert "importer_tag" not in result

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_legacy_refuse(self, rig, current_capsule, root):
        capsule = _set_catalog_root(
            _copy_capsule(current_capsule, current_capsule.parent / f"refuse-{root[-12:]}"),
            root,
        )
        with pytest.raises(ArchiveError, match="policy is refuse"):
            rig.archive.inspect_capsule(capsule)

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_legacy_read_inert_no_receipt(self, rig, current_capsule, root):
        capsule = _set_catalog_root(
            _copy_capsule(current_capsule, current_capsule.parent / f"read-{root[-12:]}"),
            root,
        )
        before = rig.archive.head()
        result = rig.archive.inspect_capsule(capsule, policy="read")
        assert result["disposition"] == "legacy_read"
        assert result["status"] == "preview"
        assert result["identity"]["root"] == "legacy"
        assert result["identity"]["catalog_root"] == root
        assert "uplift" not in result
        assert rig.archive.head() == before

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_legacy_uplift_verified_pending(self, rig, current_capsule, root):
        rig = _frozen_rig(rig)
        capsule = _set_catalog_root(
            _copy_capsule(current_capsule, current_capsule.parent / f"uplift-{root[-12:]}"),
            root,
        )
        result = rig.archive.inspect_capsule(capsule, policy="uplift")
        assert result["disposition"] == "legacy_uplift"
        assert result["status"] == "pending_uplift"
        receipt = result["uplift"]
        assert receipt["status"] == "pending"
        assert receipt["created_at"] == FROZEN_CLOCK
        assert receipt["destination_archive_id"] == rig.archive.archive_id
        assert all(
            entry["producer_authentication"] == "absent"
            for entry in receipt["objects"]
        )
        assert all(
            entry["source_id"] == entry["canonical_id"]
            for entry in receipt["objects"]
        )

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_legacy_uplift_replay_deterministic(self, rig, current_capsule, root):
        rig = _frozen_rig(rig)
        capsule = _set_catalog_root(
            _copy_capsule(current_capsule, current_capsule.parent / f"replay-{root[-12:]}"),
            root,
        )
        first = rig.archive.inspect_capsule(capsule, policy="uplift")
        second = rig.archive.inspect_capsule(capsule, policy="uplift")
        assert first["uplift"]["objects"] == second["uplift"]["objects"]
        assert first["uplift"]["created_at"] == second["uplift"]["created_at"]

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_legacy_read_replay_deterministic(self, rig, current_capsule, root):
        rig = _frozen_rig(rig)
        capsule = _set_catalog_root(
            _copy_capsule(current_capsule, current_capsule.parent / f"readreplay-{root[-12:]}"),
            root,
        )
        first = rig.archive.inspect_capsule(capsule, policy="read")
        second = rig.archive.inspect_capsule(capsule, policy="read")
        assert first["capsule"] == second["capsule"]
        assert first["identity"] == second["identity"]

    def test_unknown_root_refused(self, rig, unknown_capsule):
        with pytest.raises(ArchiveError, match="not recognized"):
            rig.archive.inspect_capsule(unknown_capsule)

    def test_unknown_policy_rejected(self, rig, current_capsule):
        with pytest.raises(ArchiveError, match="unknown Capsule inspect policy"):
            rig.archive.inspect_capsule(current_capsule, policy="admit")

    @pytest.mark.parametrize("policy", ["refuse", "read", "uplift"])
    def test_non_mutation_for_every_disposition(self, rig, thoth_capsule, policy):
        before = rig.archive.head()
        if policy == "refuse":
            with pytest.raises(ArchiveError):
                rig.archive.inspect_capsule(thoth_capsule, policy=policy)
        else:
            rig.archive.inspect_capsule(thoth_capsule, policy=policy)
        assert rig.archive.head() == before


class TestCompatibility:
    def test_current_root_passes_without_external_deps(self, rig, current_capsule):
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is True
        assert report["blockers"] == []
        assert report["identity"]["root"] == "current"

    def test_current_root_fails_with_external_carrier(self, rig, current_capsule):
        manifest_path = current_capsule / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest.setdefault("dependencies", []).append(
            {
                "object_id": "urn:ccf:blob:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "availability": "external",
                "reason": "test",
                "locator": "https://example.invalid/blob",
                "source_custody_proof": None,
                "unavailability_lineage_id": None,
            }
        )
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
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

    def test_corruption_reported_not_raised(self, rig, current_capsule):
        stream_path = current_capsule / "submissions" / "records.ndjson"
        stream_path.write_text(
            stream_path.read_text(encoding="utf-8") + "\n", encoding="utf-8"
        )
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is False
        assert any(
            b["type"] in {"capsule-load-failed", "capsule-verification-failed"}
            for b in report["blockers"]
        )

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
        before = rig.archive.head()
        result = rig.archive.inspect_capsule(current_capsule)
        assert result["disposition"] in {"current", "legacy_read", "legacy_uplift"}
        assert rig.archive.head() == before


class TestPreviewCapsule:
    def test_preview_current_verified_pending(self, rig, current_capsule):
        rig = _frozen_rig(rig)
        before = _archive_state(rig)
        preview = rig.archive.preview_capsule(current_capsule)
        after = _archive_state(rig)
        receipt = preview["uplift"]
        assert receipt["status"] == "pending"
        assert receipt["destination_archive_id"] == rig.archive.archive_id
        assert receipt["destination_level"] == "ccf-governed-archive-v1"
        assert receipt["created_at"] == FROZEN_CLOCK
        assert all(
            entry["source_id"] == entry["canonical_id"]
            for entry in receipt["objects"]
        )
        assert all(
            entry["producer_authentication"] == "absent"
            for entry in receipt["objects"]
        )
        assert after == before

    def test_preview_current_non_mutating(self, rig, current_capsule):
        before = _archive_state(rig)
        rig.archive.preview_capsule(current_capsule)
        assert _archive_state(rig) == before

    @pytest.mark.parametrize("root", [CISSA_LEGACY, THOTH_TRANSIENT])
    def test_preview_legacy_refuses_closed(self, rig, current_capsule, root):
        capsule = _set_catalog_root(
            _copy_capsule(current_capsule, current_capsule.parent / f"preview-legacy-{root[-12:]}"),
            root,
        )
        before = _archive_state(rig)
        with pytest.raises(ArchiveError, match="policy is refuse"):
            rig.archive.preview_capsule(capsule)
        assert _archive_state(rig) == before

    def test_preview_unknown_refuses_closed(self, rig, unknown_capsule):
        before = _archive_state(rig)
        with pytest.raises(ArchiveError, match="not recognized"):
            rig.archive.preview_capsule(unknown_capsule)
        assert _archive_state(rig) == before

    def test_preview_preserves_malformed_stream_boundary(
        self, rig, current_capsule, tmp_path
    ):
        """A hash-consistent current-root capsule whose submissions contain an
        object without ``id`` must fail closed at the preview API and leave the
        archive unchanged.
        """
        bad = _integrity_consistent_missing_submission_id(
            current_capsule, tmp_path / "missing-id-preview"
        )
        before = _archive_state(rig)
        with pytest.raises(ArchiveError):
            rig.archive.preview_capsule(bad)
        assert _archive_state(rig) == before


class TestFailClosedBoundary:
    """Compatibility harness must never raise for malformed Capsule input."""

    def test_invalid_manifest_json(self, rig, current_capsule):
        (current_capsule / "manifest.json").write_text("not json", encoding="utf-8")
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is False
        assert report["identity"] is None
        assert any(
            b["type"] == "capsule-load-failed" for b in report["blockers"]
        )

    def test_missing_catalog_dependencies(self, rig, current_capsule):
        manifest_path = current_capsule / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["catalog_dependencies"] = []
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is False
        assert report["identity"] is None
        assert any(
            b["type"] == "capsule-identity-failed" for b in report["blockers"]
        )

    def test_malformed_catalog_dependencies(self, rig, current_capsule):
        manifest_path = current_capsule / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["catalog_dependencies"] = "not-a-list"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is False
        assert report["identity"] is None
        assert any(
            b["type"] in {"capsule-load-failed", "capsule-identity-failed"}
            for b in report["blockers"]
        )

    def test_malformed_dependency_entries(self, rig, current_capsule):
        manifest_path = current_capsule / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["dependencies"] = ["not-an-object"]
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is False
        assert any(
            b["type"] in {"carrier-detection-failed", "capsule-load-failed"}
            for b in report["blockers"]
        )

    def test_corrupted_stream_report_only(self, rig, current_capsule):
        stream_path = current_capsule / "submissions" / "records.ndjson"
        stream_path.write_text(
            stream_path.read_text(encoding="utf-8") + "\n", encoding="utf-8"
        )
        report = rig.archive.evaluate_capsule_compatibility(current_capsule)
        assert report["pass"] is False
        assert report["identity"] is None
        assert any(
            b["type"] == "capsule-load-failed" for b in report["blockers"]
        )

    def test_archive_boundary_converts_context_failures(self, rig, current_capsule, monkeypatch):
        from ccf.catalog import CatalogError

        def _bad_load(*args, **kwargs):
            raise CatalogError("simulated catalog failure")

        monkeypatch.setattr(
            rig.archive, "_load_declaration_resources", _bad_load
        )
        with pytest.raises(ArchiveError, match="simulated catalog failure"):
            rig.archive.interop_context()
        with pytest.raises(ArchiveError, match="simulated catalog failure"):
            rig.archive.preview_capsule(current_capsule)
        with pytest.raises(ArchiveError, match="simulated catalog failure"):
            rig.archive.inspect_capsule(current_capsule)
        with pytest.raises(ArchiveError, match="simulated catalog failure"):
            rig.archive.evaluate_capsule_compatibility(current_capsule)

    def test_inspect_boundary_converts_capsule_errors(self, rig, current_capsule):
        (current_capsule / "manifest.json").write_text("not json", encoding="utf-8")
        with pytest.raises(ArchiveError):
            rig.archive.inspect_capsule(current_capsule)

    def test_inspect_boundary_converts_exchange_errors(
        self, rig, current_capsule, monkeypatch
    ):
        import ccf.interop as interop
        from ccf.exchange import ExchangeError

        def _bad_receipt(*args, **kwargs):
            raise ExchangeError("simulated receipt failure")

        monkeypatch.setattr(interop, "verify_uplift_receipt", _bad_receipt)
        with pytest.raises(ArchiveError, match="simulated receipt failure"):
            rig.archive.inspect_capsule(current_capsule)

    def test_negotiate_boundary_converts_identity_errors(self, rig, current_capsule):
        manifest_path = current_capsule / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["catalog_dependencies"] = []
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        with pytest.raises(ArchiveError):
            rig.archive.negotiate_capsule(current_capsule)

    def test_inspect_current_root_converts_shape_failures(
        self, rig, current_capsule, tmp_path
    ):
        """A hash-consistent current-root capsule whose submissions contain an
        object without ``id`` passes load integrity and manifest schema, then
        raises ``KeyError`` inside ``verify_capsule``. The public API must
        surface ``ArchiveError`` and leave the archive unchanged.
        """
        bad = _integrity_consistent_missing_submission_id(
            current_capsule, tmp_path / "missing-id"
        )
        before = rig.archive.head()
        with pytest.raises(ArchiveError):
            rig.archive.inspect_capsule(bad)
        assert rig.archive.head() == before

    def test_inspect_module_boundary_converts_keyerror(
        self, rig, current_capsule, tmp_path
    ):
        """The ``inspect_capsule`` module boundary must translate raw shape
        failures into ``InteropError`` on the current-root verification path.
        """
        from ccf.interop import InteropError, inspect_capsule, load_capsule_integrity

        bad = _integrity_consistent_missing_submission_id(
            current_capsule, tmp_path / "missing-id-module"
        )
        capsule = load_capsule_integrity(bad)
        with pytest.raises(InteropError):
            inspect_capsule(
                rig.archive.interop_context(), capsule, policy="refuse"
            )
