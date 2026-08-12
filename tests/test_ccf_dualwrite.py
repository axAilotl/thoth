"""CCF dual-write mirror tests (checklist section 10a).

Covers: config gating (flag off = CCF never touched; contradictory config
fails closed), end-to-end mirroring through the real imported-markdown
collector path, idempotent re-runs, loud failure recording with the
legacy write intact, and the zero-mismatch harness (clean run plus
planted missing/drift/extra mismatches).

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import pytest

from core.config import Config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout

from ccf.db import CcfConfigError, CcfPostgresSettings, open_ccf_connection
from ccf.dualwrite import resolve_dual_write_settings

from scripts.ccf_dualwrite_check import (
    CcfSnapshot,
    load_legacy_inventory,
    main as check_main,
    reconcile,
)


def _write_note(path: Path, *, title: str, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"---\ntitle: {title}\n---\n{body}\n", encoding="utf-8")
    return path


def _dualwrite_config(tmp_path: Path, schema: str, *, dual_write=True, enabled=True) -> Config:
    cfg = Config()
    cfg.data = {
        "paths": {
            "vault_dir": str(tmp_path / "knowledge_vault"),
            "system_dir": str(tmp_path / ".thoth_system"),
            "cache_dir": str(tmp_path / ".thoth_system" / "cache"),
        },
        "database": {
            "enabled": True,
            "path": str(tmp_path / ".thoth_system" / "meta.db"),
            "ccf_archive": {
                "enabled": enabled,
                "dual_write": dual_write,
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


def _collect(cfg: Config, import_dir: Path, tmp_path: Path):
    from collectors.imported_markdown_connector import ImportedMarkdownConnector

    layout = build_path_layout(cfg)
    db = MetadataDB(str(tmp_path / ".thoth_system" / "meta.db"))
    connector = ImportedMarkdownConnector(cfg, layout=layout, db=db)
    return asyncio.run(
        connector.collect(import_dirs=[import_dir], source_name="test_corpus")
    )


def _reconcile_workspace(tmp_path: Path, schema: str, dsn: str) -> dict:
    inventory = load_legacy_inventory(
        tmp_path / ".thoth_system" / "meta.db",
        vault_root=tmp_path / "knowledge_vault",
    )
    snapshot = CcfSnapshot(CcfPostgresSettings(enabled=True, dsn=dsn, schema=schema))
    return reconcile(inventory, snapshot)


def _object_counts(settings: CcfPostgresSettings) -> dict:
    with open_ccf_connection(settings) as conn:
        return {
            kind: count
            for kind, count in conn.execute(
                "SELECT object_kind, COUNT(*) FROM object_header GROUP BY 1"
            ).fetchall()
        }


# ---------------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------------


def test_dual_write_off_never_touches_ccf(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    """Flag off: the capture works and the CCF schema is never created."""
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema, dual_write=False)
    notes = _write_note(
        tmp_path / "inbox" / "note.md", title="Note", body="a benign note"
    )
    result = _collect(cfg, notes.parent, tmp_path)
    assert result.records

    import psycopg

    with psycopg.connect(ccf_postgres_dsn) as conn:
        row = conn.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s", (ccf_settings.schema,)
        ).fetchone()
    assert row is None, "CCF schema was created with dual_write off"
    assert not (tmp_path / "errors.jsonl").exists()


def test_dual_write_without_enabled_fails_closed(tmp_path):
    cfg = _dualwrite_config(tmp_path, "ccf_unused", dual_write=True, enabled=False)
    with pytest.raises(CcfConfigError):
        resolve_dual_write_settings(cfg)


def test_dual_write_missing_dsn_fails_closed(tmp_path, monkeypatch):
    monkeypatch.delenv("THOTH_CCF_POSTGRES_DSN", raising=False)
    cfg = _dualwrite_config(tmp_path, "ccf_unused")
    with pytest.raises(CcfConfigError):
        resolve_dual_write_settings(cfg)


def test_dual_write_missing_key_paths_fails_closed(tmp_path, ccf_postgres_dsn, monkeypatch):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, "ccf_unused")
    del cfg.data["database"]["ccf_archive"]["device_key_path"]
    with pytest.raises(CcfConfigError):
        resolve_dual_write_settings(cfg)


# ---------------------------------------------------------------------------
# End-to-end mirror
# ---------------------------------------------------------------------------


def test_capture_lands_in_both_stores_with_matching_commitments(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    notes = _write_note(
        tmp_path / "inbox" / "note.md",
        title="Mirrored",
        body="dual-write coverage ‍ with an invisible joiner",
    )
    result = _collect(cfg, notes.parent, tmp_path)
    assert result.records

    # Legacy store: queue entry persisted.
    db = MetadataDB(str(tmp_path / ".thoth_system" / "meta.db"))
    assert db.get_ingestion_entry(result.records[0].artifact_id) is not None

    # CCF store: source/session/run/artifact/blob mirrored; hostile content
    # produced a mirrored security finding.
    counts = _object_counts(ccf_settings)
    assert counts.get("blob") == 1
    assert counts.get("record", 0) >= 9  # bootstrap(4) + genesis + source + session + run + artifact + finding(s)

    report = _reconcile_workspace(tmp_path, ccf_settings.schema, ccf_postgres_dsn)
    assert report["classes"]["artifacts"]["matched"] == 1
    assert report["classes"]["blobs"]["matched"] == 1
    assert report["classes"]["findings"]["expected"] >= 1
    assert report["summary"]["ok"], report["mismatches"]


def test_mirror_rerun_is_idempotent(tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    notes = _write_note(tmp_path / "inbox" / "note.md", title="Again", body="same file")
    _collect(cfg, notes.parent, tmp_path)
    counts_first = _object_counts(ccf_settings)

    result = _collect(cfg, notes.parent, tmp_path)
    assert result.records
    counts_second = _object_counts(ccf_settings)

    # A re-run adds only the new session Record, its paired run Record, and
    # the commit Record of the mirror batch; artifacts/blobs are skipped via
    # the origin index, never duplicated or conflicted.
    assert counts_second.get("blob") == counts_first.get("blob")
    assert counts_second.get("link") == counts_first.get("link")
    assert counts_second.get("record") == counts_first.get("record") + 3
    assert not (tmp_path / "errors.jsonl").exists()

    report = _reconcile_workspace(tmp_path, ccf_settings.schema, ccf_postgres_dsn)
    # The queue payload now references the latest session only; the first
    # run's session/run stay in the archive as superseded evidence.
    assert report["classes"]["sessions"]["expected"] == 1
    assert report["classes"]["sessions"]["matched"] == 1
    assert report["summary"]["superseded_run_records"] == 2
    assert report["summary"]["ok"], report["mismatches"]


def test_mirror_failure_is_loud_and_legacy_write_intact(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch, caplog
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    notes = _write_note(tmp_path / "inbox" / "note.md", title="Fragile", body="boom")

    from ccf.dualwrite import service as dualwrite_service

    def _explode(self, **kwargs):
        raise RuntimeError("planted CCF-side failure")

    monkeypatch.setattr(
        dualwrite_service.CcfDualWriteService, "mirror_capture", _explode
    )
    with caplog.at_level(logging.ERROR, logger="ccf.dualwrite.service"):
        result = _collect(cfg, notes.parent, tmp_path)

    # Legacy write succeeded and is authoritative.
    assert result.records
    db = MetadataDB(str(tmp_path / ".thoth_system" / "meta.db"))
    assert db.get_ingestion_entry(result.records[0].artifact_id) is not None

    # The failure is loud in logs and durable in the ledger.
    assert any("dual-write mirror failed" in r.message for r in caplog.records)
    ledger = (tmp_path / "errors.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(ledger) == 1
    entry = json.loads(ledger[0])
    assert entry["kind"] == "mirror_failure"
    assert "planted CCF-side failure" in entry["error"]
    assert entry["context"]["queue_artifact_id"] == result.records[0].artifact_id


# ---------------------------------------------------------------------------
# Harness: planted mismatches
# ---------------------------------------------------------------------------


@pytest.fixture()
def mirrored_workspace(tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    notes = _write_note(tmp_path / "inbox" / "note.md", title="Harness", body="content")
    _collect(cfg, notes.parent, tmp_path)
    return tmp_path, ccf_settings, ccf_postgres_dsn


def _origin_tuple(settings) -> tuple:
    with open_ccf_connection(settings) as conn:
        return conn.execute(
            "SELECT source_id, native_id, revision FROM origin_index "
            "WHERE object_kind = 'blob' LIMIT 1"
        ).fetchone()


def test_harness_clean_run_exits_zero(mirrored_workspace, tmp_path):
    tmp_path, ccf_settings, dsn = mirrored_workspace
    out = tmp_path / "report.json"
    rc = check_main(
        [
            "--metadata-db", str(tmp_path / ".thoth_system" / "meta.db"),
            "--vault-root", str(tmp_path / "knowledge_vault"),
            "--dsn", dsn,
            "--schema", ccf_settings.schema,
            "--out", str(out),
        ]
    )
    assert rc == 0
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["summary"]["mismatch_count"] == 0


def test_harness_detects_missing_object(mirrored_workspace):
    tmp_path, ccf_settings, dsn = mirrored_workspace
    with open_ccf_connection(ccf_settings) as conn:
        with conn.transaction():
            conn.execute(
                "DELETE FROM origin_index WHERE object_kind = 'blob'"
            )
    report = _reconcile_workspace(tmp_path, ccf_settings.schema, dsn)
    assert not report["summary"]["ok"]
    assert any(
        m["kind"] == "missing_object" and m["class"] == "blobs"
        for m in report["mismatches"]
    )


def test_harness_detects_content_drift(mirrored_workspace):
    tmp_path, ccf_settings, dsn = mirrored_workspace
    with open_ccf_connection(ccf_settings) as conn:
        with conn.transaction():
            conn.execute(
                "UPDATE blob_content SET plaintext_bytes = '\\x00'::bytea"
            )
    report = _reconcile_workspace(tmp_path, ccf_settings.schema, dsn)
    assert not report["summary"]["ok"]
    assert any(
        m["kind"] == "content_drift" and m["class"] == "blobs"
        for m in report["mismatches"]
    )


def test_harness_detects_extra_object(mirrored_workspace):
    tmp_path, ccf_settings, dsn = mirrored_workspace
    source_id, _native_id, _revision = _origin_tuple(ccf_settings)
    with open_ccf_connection(ccf_settings) as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO origin_index (
                    archive_id, source_id, native_id, revision,
                    submission_hash, object_kind, object_id
                ) VALUES (
                    (SELECT archive_id FROM archive), %s,
                    'raw-ref:planted-extra', '1',
                    'sha256:' || repeat('0', 64), 'blob', 'urn:ccf:blob:00000000-0000-4000-8000-000000000000'
                )
                """,
                (source_id,),
            )
    report = _reconcile_workspace(tmp_path, ccf_settings.schema, dsn)
    assert not report["summary"]["ok"]
    assert any(m["kind"] == "extra_object" for m in report["mismatches"])


def test_harness_reports_dual_write_errors(mirrored_workspace):
    tmp_path, ccf_settings, dsn = mirrored_workspace
    from ccf.dualwrite.ledger import append_error

    ledger = tmp_path / "errors.jsonl"
    append_error(ledger, {"kind": "mirror_failure", "error": "planted"})
    inventory = load_legacy_inventory(
        tmp_path / ".thoth_system" / "meta.db",
        vault_root=tmp_path / "knowledge_vault",
    )
    from ccf.dualwrite.ledger import read_errors

    report = reconcile(
        inventory,
        CcfSnapshot(CcfPostgresSettings(enabled=True, dsn=dsn, schema=ccf_settings.schema)),
        ledger_entries=read_errors(ledger),
    )
    assert not report["summary"]["ok"]
    assert any(m["kind"] == "dual_write_error" for m in report["mismatches"])
