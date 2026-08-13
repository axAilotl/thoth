"""Lane-aware thothmap + dual-write mirror tests (manifest ccf block).

Covers: ``media_submissions`` carrying the connector manifest's declared
CCF lane, artifact role, namespaced manifest extensions, and
connector-native provenance onto the mirrored ``experience.artifact``;
the legacy generic mapping when no lane is threaded; fail-closed lane
validation at the converter; and an end-to-end ``mirror_capture`` proving
a capture from a lane-declared connector lands with its lane while a
capture from an unknown/manifest-less connector keeps legacy behavior.

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker.
"""

from __future__ import annotations

import hashlib

import pytest

from core.config import Config
from core.connector_registry import load_connector_registry

from ccf.dualwrite import CcfDualWriteService, resolve_dual_write_settings
from ccf.thothmap import MapContext
from ccf.thothmap.artifacts import media_submissions
from ccf.thothmap.context import ThothMapError
from ccf.thothmap.sources import source_submission

from ccf_helpers import admit_mapped, compartment, make_rig, outcome_for

PDF_BYTES = b"%PDF-1.4 fake-paper-bytes-for-thothmap-lane-tests"

LANE_SOURCE = {
    "source_name": "arxiv",
    "collector": "arxiv",
    "native_source_id": "arxiv:cs.CL",
}


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def ctx(rig):
    return MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)


@pytest.fixture()
def source_id(rig, ctx):
    mapped = source_submission(
        rig.producer,
        ctx,
        {"source_name": "arxiv", "source_type": "papers", "collector": "arxiv"},
    )
    admit_mapped(rig, mapped)
    return mapped.records[0]["id"]


def raw_ref(**overrides):
    base = {
        "raw_ref_id": "raw-ref:0f3b6d2e-9dad-11d1-80b4-00c04fd430c8",
        "path": "knowledge_vault/raw/arxiv/2401.00001.pdf",
        "sha256": hashlib.sha256(PDF_BYTES).hexdigest(),
        "size_bytes": len(PDF_BYTES),
        "mime_type": "application/pdf",
        "created_at": "2026-08-13T12:00:00.000Z",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Converter level
# ---------------------------------------------------------------------------


def test_media_carries_lane_role_extensions_and_provenance(rig, ctx, source_id):
    mapped = media_submissions(
        rig.producer,
        ctx,
        raw_ref(),
        data=PDF_BYTES,
        source_ccf_id=source_id,
        artifact_role="raw_capture",
        lane="paper",
        source_snapshot=dict(LANE_SOURCE),
        manifest_extensions={"thoth.lane": "paper", "acme.channel": "papers"},
    )
    artifact = mapped.records[0]
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.2-rc1:payload.experience.artifact",
        artifact["payload"],
        what="experience.artifact",
    )

    extensions = artifact["payload"]["extensions"]
    assert artifact["payload"]["artifact_role"] == "raw_capture"
    assert extensions["thoth_lane"] == "paper"
    assert extensions["thoth_source_name"] == "arxiv"
    assert extensions["thoth_collector"] == "arxiv"
    assert extensions["thoth_native_source_id"] == "arxiv:cs.CL"
    assert extensions["thoth.lane"] == "paper"
    assert extensions["acme.channel"] == "papers"
    # Legacy provenance keys are still present.
    assert extensions["thoth_raw_ref_id"] == raw_ref()["raw_ref_id"]

    result = admit_mapped(rig, mapped)
    assert outcome_for(result, artifact["id"])["status"] == "admitted"
    admitted = compartment(rig, artifact["id"], "semantic")
    assert admitted["payload"]["extensions"]["thoth_lane"] == "paper"


def test_media_without_lane_keeps_legacy_generic_shape(rig, ctx, source_id):
    mapped = media_submissions(
        rig.producer, ctx, raw_ref(), data=PDF_BYTES, source_ccf_id=source_id
    )
    artifact = mapped.records[0]

    assert artifact["payload"]["artifact_role"] == "raw_capture"
    assert set(artifact["payload"]["extensions"]) == {
        "thoth_raw_ref_id",
        "thoth_path",
        "thoth_sha256",
    }


def test_media_rejects_empty_lane(rig, ctx, source_id):
    with pytest.raises(ThothMapError, match="non-empty"):
        media_submissions(
            rig.producer,
            ctx,
            raw_ref(),
            data=PDF_BYTES,
            source_ccf_id=source_id,
            lane="  ",
        )


# ---------------------------------------------------------------------------
# End-to-end mirror
# ---------------------------------------------------------------------------


def _dualwrite_config(tmp_path, schema: str) -> Config:
    cfg = Config()
    cfg.data = {
        "database": {
            "ccf_archive": {
                "enabled": True,
                "dual_write": True,
                "backend": "postgres",
                "dsn_env": "THOTH_CCF_POSTGRES_DSN",
                "schema": schema,
                "device_key_path": str(tmp_path / "ccf" / "device.pem"),
                "archive_key_path": str(tmp_path / "ccf" / "archive.pem"),
                "error_log_path": str(tmp_path / "errors.jsonl"),
            },
        },
    }
    return cfg


def _mirror_source(**overrides):
    source = {
        "source_id": "src-lane-1",
        "source_name": "arxiv",
        "source_type": "papers",
        "collector": "arxiv",
        "account": None,
        "native_source_id": "arxiv:cs.CL",
        "base_uri": None,
        "status": "active",
    }
    source.update(overrides)
    return source


def _artifact_payload(service: CcfDualWriteService, receipt: dict) -> dict:
    artifact = service.archive.get_object(receipt["objects"]["artifact_id"])
    assert artifact is not None
    return artifact["compartments"]["semantic"]["envelope"]["content"]["payload"]


def test_mirror_lanes_artifact_from_connector_manifest(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    service = CcfDualWriteService.create_or_open(
        resolve_dual_write_settings(_dualwrite_config(tmp_path, ccf_settings.schema)),
        connector_registry=load_connector_registry(),
    )

    receipt = service.mirror_capture(
        source=_mirror_source(),
        session=None,
        raw_ref=raw_ref(raw_ref_id="raw-ref:lane-e2e"),
        data=PDF_BYTES,
    )
    assert receipt["status"] == "accepted"

    payload = _artifact_payload(service, receipt)
    assert payload["artifact_role"] == "raw_capture"
    assert payload["extensions"]["thoth_lane"] == "paper"
    assert payload["extensions"]["thoth_collector"] == "arxiv"
    assert payload["extensions"]["thoth_source_name"] == "arxiv"
    assert payload["extensions"]["thoth_native_source_id"] == "arxiv:cs.CL"


def test_mirror_without_manifest_or_block_keeps_legacy_behavior(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    service = CcfDualWriteService.create_or_open(
        resolve_dual_write_settings(_dualwrite_config(tmp_path, ccf_settings.schema)),
        connector_registry=load_connector_registry(),
    )

    # Unknown collector: no manifest resolves, so no lane is applied.
    unknown = service.mirror_capture(
        source=_mirror_source(source_id="src-lane-2", collector="no_such_connector"),
        session=None,
        raw_ref=raw_ref(raw_ref_id="raw-ref:lane-legacy-1"),
        data=PDF_BYTES,
    )
    payload = _artifact_payload(service, unknown)
    assert payload["artifact_role"] == "raw_capture"
    assert set(payload["extensions"]) == {
        "thoth_raw_ref_id",
        "thoth_path",
        "thoth_sha256",
    }

    # No collector at all: same legacy mapping, no failure.
    anonymous = service.mirror_capture(
        source=_mirror_source(source_id="src-lane-3", collector=None),
        session=None,
        raw_ref=raw_ref(raw_ref_id="raw-ref:lane-legacy-2"),
        data=PDF_BYTES,
    )
    payload = _artifact_payload(service, anonymous)
    assert payload["artifact_role"] == "raw_capture"
    assert "thoth_lane" not in payload["extensions"]
