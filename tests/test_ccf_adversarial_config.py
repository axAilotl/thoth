"""Adversarial config-attack suite (checklist 10b).

Contradictory, missing, or unsafe configuration must fail CLOSED at
startup/resolution with a typed error — never a silent default, never a
partially initialized archive, never a world-readable private key.

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from ccf.db import CcfConfigError, CcfPostgresSettings, open_ccf_connection
from ccf.dualwrite import resolve_dual_write_settings
from ccf.dualwrite.service import CcfDualWriteService, DualWriteError
from ccf.keys import CcfKeyError, generate_signing_key, load_signing_key

from ccf_helpers import make_rig


def _config(tmp_path: Path, schema: str, **overrides):
    from core.config import Config

    store = {
        "enabled": True,
        "dual_write": True,
        "backend": "postgres",
        "dsn_env": "THOTH_CCF_POSTGRES_DSN",
        "schema": schema,
        "device_key_path": str(tmp_path / "ccf" / "device.pem"),
        "archive_key_path": str(tmp_path / "ccf" / "archive.pem"),
        "error_log_path": str(tmp_path / "errors.jsonl"),
    }
    store.update(overrides)
    cfg = Config()
    cfg.data = {"database": {"ccf_archive": store}}
    return cfg


# ---------------------------------------------------------------------------
# Contradictory flags
# ---------------------------------------------------------------------------


def test_dual_write_true_enabled_false_fails_closed(tmp_path):
    cfg = _config(tmp_path, "ccf_unused", enabled=False)
    with pytest.raises(CcfConfigError, match="dual_write is true but enabled is false"):
        resolve_dual_write_settings(cfg)


def test_dual_write_true_missing_dsn_fails_closed(tmp_path, monkeypatch):
    monkeypatch.delenv("THOTH_CCF_POSTGRES_DSN", raising=False)
    cfg = _config(tmp_path, "ccf_unused")
    with pytest.raises(CcfConfigError, match="THOTH_CCF_POSTGRES_DSN is not set"):
        resolve_dual_write_settings(cfg)


def test_dual_write_missing_device_key_path_fails_closed(
    tmp_path, ccf_postgres_dsn, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    monkeypatch.delenv("THOTH_CCF_DEVICE_KEY", raising=False)
    cfg = _config(tmp_path, "ccf_unused")
    del cfg.data["database"]["ccf_archive"]["device_key_path"]
    with pytest.raises(CcfConfigError, match="device_key_path"):
        resolve_dual_write_settings(cfg)


def test_schema_name_injection_fails_closed(tmp_path, ccf_postgres_dsn, monkeypatch):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _config(tmp_path, 'ccf"; DROP TABLE archive; --')
    with pytest.raises(CcfConfigError):
        resolve_dual_write_settings(cfg)


# ---------------------------------------------------------------------------
# Key material safety
# ---------------------------------------------------------------------------


def test_world_readable_private_key_refused(tmp_path):
    key_path = tmp_path / "world.pem"
    generate_signing_key(key_path)
    key_path.chmod(0o644)
    with pytest.raises(CcfKeyError, match="accessible by group/other"):
        load_signing_key(key_path)


def test_group_readable_private_key_refused(tmp_path):
    key_path = tmp_path / "group.pem"
    generate_signing_key(key_path)
    key_path.chmod(0o640)
    with pytest.raises(CcfKeyError, match="accessible by group/other"):
        load_signing_key(key_path)


def test_malformed_key_file_refused(tmp_path):
    key_path = tmp_path / "garbage.pem"
    key_path.write_bytes(b"not a PEM at all \x00\x01\x02")
    key_path.chmod(0o600)
    with pytest.raises(CcfKeyError, match="invalid Ed25519 signing key"):
        load_signing_key(key_path)


def test_missing_key_file_refused(tmp_path):
    with pytest.raises(CcfKeyError, match="signing key not found"):
        load_signing_key(tmp_path / "absent.pem")


def test_key_path_pointing_at_directory_fails_closed(
    tmp_path, ccf_postgres_dsn, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _config(
        tmp_path, "ccf_dir_key", device_key_path=str(tmp_path / "ccf")
    )
    settings = resolve_dual_write_settings(cfg)
    (tmp_path / "ccf").mkdir(parents=True, exist_ok=True)
    with pytest.raises(CcfKeyError, match="refusing to overwrite"):
        CcfDualWriteService.create_or_open(settings)
    # Nothing was bootstrapped: no archive row exists.
    import psycopg

    with psycopg.connect(ccf_postgres_dsn) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = 'archive'",
            ("ccf_dir_key",),
        ).fetchone()
    if exists is not None:
        with open_ccf_connection(
            CcfPostgresSettings(enabled=True, dsn=ccf_postgres_dsn, schema="ccf_dir_key")
        ) as conn:
            row = conn.execute("SELECT COUNT(*) FROM archive").fetchone()
            assert row[0] == 0


# ---------------------------------------------------------------------------
# Wrong schema / wrong database
# ---------------------------------------------------------------------------


def test_unreachable_dsn_fails_closed_without_partial_state(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "THOTH_CCF_POSTGRES_DSN", "postgresql://127.0.0.1:1/no-such-db"
    )
    cfg = _config(tmp_path, "ccf_down")
    settings = resolve_dual_write_settings(cfg)
    import psycopg

    with pytest.raises(psycopg.OperationalError):
        CcfDualWriteService.create_or_open(settings)


def test_bootstrap_records_missing_fails_closed(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch, ccf_package_root
):
    """An archive that exists but lacks the dual-write founding Records is
    refused: the service never mirrors into an archive it did not bootstrap."""
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    # Build a foreign archive (test rig shape) in the schema — no
    # dual-write bootstrap IDs.
    (tmp_path / "rigkeys").mkdir()
    make_rig(ccf_settings, tmp_path / "rigkeys", ccf_package_root)

    cfg = _config(tmp_path, ccf_settings.schema)
    settings = resolve_dual_write_settings(cfg)
    with pytest.raises(DualWriteError, match="did not bootstrap"):
        CcfDualWriteService.create_or_open(settings)


def test_swapped_schemas_fail_closed_on_key_consistency(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    """Pointing the DSN at the OTHER workspace's schema must not silently
    mirror: the admitted credential pins the bootstrapped key material."""
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _config(tmp_path, ccf_settings.schema)
    service = CcfDualWriteService.create_or_open(resolve_dual_write_settings(cfg))
    assert service.archive.archive_id

    # Swap in fresh device key material; the archive pins the original.
    device_key = tmp_path / "ccf" / "device.pem"
    device_key.unlink()
    generate_signing_key(device_key)
    with pytest.raises(DualWriteError, match="does not match the admitted"):
        CcfDualWriteService.create_or_open(resolve_dual_write_settings(cfg))


# ---------------------------------------------------------------------------
# Suppression key safety
# ---------------------------------------------------------------------------


def test_suppression_entries_without_key_fail_closed(
    ccf_settings, tmp_path, ccf_package_root
):
    """Once suppression entries exist, admission without the key must fail
    closed rather than skip the check (spec 12.7)."""
    from dataclasses import replace

    from ccf.erasure.errors import SuppressionKeyError
    from ccf.erasure.suppression import generate_suppression_key

    from ccf_helpers import authority

    key_path = generate_suppression_key(tmp_path / "suppression.key")
    (tmp_path / "keyed").mkdir()
    keyed = make_rig(
        replace(ccf_settings, suppression_key_path=str(key_path)),
        tmp_path / "keyed",
        ccf_package_root,
    )
    record = keyed.producer.new_record(
        type="experience.utterance",
        claims=keyed.claims(),
        payload={
            "text": "erase me", "language": "en", "speaker_id": None,
            "sequence": None, "transcription": None, "extensions": {},
        },
    )
    result = keyed.archive.admit_batch(keyed.producer.create_batch(records=[record]))
    assert result["status"] == "accepted", result

    svc = keyed.archive.erasure()
    request = svc.submit_request(
        requester_id=keyed.person_id,
        subject_id=keyed.person_id,
        requested_scope={"targets": [
            {"object_id": record["id"], "compartments": ["semantic"]}
        ]},
        reason="key drill",
        authority=authority("first_person_statement", keyed.person_id, keyed.person_id),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=[{"object_id": record["id"], "compartments": ["semantic"]}],
        reasoning="approved",
        decided_by=keyed.person_id,
        authority=authority(
            "explicit_authorization", keyed.person_id, keyed.person_id
        ),
    )
    status = svc.execute(decided["operation_id"])
    assert status["stage"] == "receipt", status

    # Reopen WITHOUT the suppression key: admission refuses outright.
    from ccf.archive import Archive

    unkeyed = Archive.open(
        ccf_settings,
        package_root=ccf_package_root,
        archive_key_path=keyed.archive_key_path,
    )
    followup = keyed.producer.new_record(
        type="experience.utterance",
        claims=keyed.claims(),
        payload={
            "text": "post-erasure traffic", "language": "en", "speaker_id": None,
            "sequence": None, "transcription": None, "extensions": {},
        },
    )
    batch = keyed.producer.create_batch(records=[followup])
    with pytest.raises(SuppressionKeyError, match="no suppression key is configured"):
        unkeyed.admit_batch(batch)


def test_truncated_suppression_key_refused(ccf_settings):
    from ccf.erasure.errors import SuppressionKeyError
    from ccf.erasure.suppression import load_suppression_key

    import tempfile

    with tempfile.NamedTemporaryFile(delete=False) as handle:
        handle.write(b"too-short")
        handle.flush()
        os.chmod(handle.name, 0o600)
        settings = CcfPostgresSettings(
            enabled=True,
            dsn=ccf_settings.dsn,
            schema=ccf_settings.schema,
            suppression_key_path=handle.name,
        )
        with pytest.raises(SuppressionKeyError, match="32"):
            load_suppression_key(settings)
    os.unlink(handle.name)
