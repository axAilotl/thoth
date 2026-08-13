"""Dual-write rollback and re-enable cutover gates."""

from __future__ import annotations

from pathlib import Path

import pytest

from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf_cutover_test_support import (
    collect_imported_markdown,
    dualwrite_config,
    object_counts,
)


# Gate 4: rollback path
# ---------------------------------------------------------------------------


def _write_note(path: Path, *, title: str, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"---\ntitle: {title}\n---\n{body}\n", encoding="utf-8")
    return path


def _dualwrite_config(tmp_path: Path, schema: str, *, dual_write=True, enabled=True):
    return dualwrite_config(
        tmp_path, schema, dual_write=dual_write, enabled=enabled
    )


def _collect(cfg, import_dir: Path, tmp_path: Path):
    return collect_imported_markdown(
        cfg, import_dir, tmp_path, source_name="rollback_corpus"
    )


def _legacy_queue_count(tmp_path: Path) -> int:
    import sqlite3

    db_path = tmp_path / ".thoth_system" / "meta.db"
    with sqlite3.connect(db_path) as conn:
        return conn.execute("SELECT COUNT(*) FROM ingestion_queue").fetchone()[0]


def _archive_id(settings: CcfPostgresSettings) -> str:
    with open_ccf_connection(settings) as conn:
        return conn.execute("SELECT archive_id FROM archive").fetchone()[0]


def test_gate4_rollback_path(tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    inbox = tmp_path / "inbox"

    # Phase A: dual-write on — captures mirror into CCF.
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    note = _write_note(inbox / "a.md", title="A", body="phase A")
    result = _collect(cfg, note.parent, tmp_path)
    assert result.records
    counts_a = object_counts(ccf_settings)
    assert counts_a.get("blob") == 1
    first_archive_id = _archive_id(ccf_settings)
    queue_a = _legacy_queue_count(tmp_path)

    # Phase B: flags off — legacy capture works with zero CCF contact.
    cfg_off = _dualwrite_config(tmp_path, ccf_settings.schema, dual_write=False)
    note_b = _write_note(inbox / "b.md", title="B", body="phase B")
    result = _collect(cfg_off, note_b.parent, tmp_path)
    assert result.records
    assert _legacy_queue_count(tmp_path) > queue_a
    assert object_counts(ccf_settings) == counts_a
    assert not (tmp_path / "errors.jsonl").exists()

    # Phase C: DROP SCHEMA ccf CASCADE — legacy store + capture unaffected.
    import psycopg

    with psycopg.connect(ccf_postgres_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{ccf_settings.schema}" CASCADE')
    note_c = _write_note(inbox / "c.md", title="C", body="phase C")
    result = _collect(cfg_off, note_c.parent, tmp_path)
    assert result.records
    with psycopg.connect(ccf_postgres_dsn) as conn:
        assert conn.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s",
            (ccf_settings.schema,),
        ).fetchone() is None

    # Phase D: re-enable — the archive re-bootstraps cleanly with a new
    # genesis (the schema was dropped); no stale-key confusion because the
    # same key files now back a fresh archive identity.
    note_d = _write_note(inbox / "d.md", title="D", body="phase D")
    result = _collect(cfg, note_d.parent, tmp_path)
    assert result.records
    second_archive_id = _archive_id(ccf_settings)
    assert second_archive_id != first_archive_id
    counts_d = object_counts(ccf_settings)
    # The fresh archive knows nothing about phases A-C, so the whole
    # re-scanned inbox mirrors into the new genesis: 4 blobs, no replay of
    # the dropped archive's history.
    assert counts_d.get("blob") == 4
    from ccf.archive import Archive

    reopened = Archive.open(
        ccf_settings,
        package_root=Path(__file__).parent.parent / "spec" / "ccf" / "0.1.2",
        archive_key_path=tmp_path / "ccf" / "archive.pem",
    )
    assert reopened.verify_chain()["commits_verified"] >= 3
    assert not (tmp_path / "errors.jsonl").exists()


def test_gate4_stale_key_confusion_fails_closed(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    """Re-bootstrap with mismatched device key material must fail closed."""
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    note = _write_note(tmp_path / "inbox" / "a.md", title="A", body="keyed")
    result = _collect(cfg, note.parent, tmp_path)
    assert result.records
    head_before = None
    from ccf.archive import Archive

    opened = Archive.open(
        ccf_settings,
        package_root=Path(__file__).parent.parent / "spec" / "ccf" / "0.1.2",
        archive_key_path=tmp_path / "ccf" / "archive.pem",
    )
    head_before = opened.head()

    # Swap the device key for fresh material without rolling back the
    # archive: the bootstrap credential no longer matches.
    from ccf.keys import generate_signing_key

    device_key = tmp_path / "ccf" / "device.pem"
    device_key.unlink()
    generate_signing_key(device_key)

    from ccf.dualwrite import resolve_dual_write_settings
    from ccf.dualwrite.service import CcfDualWriteService, DualWriteError

    settings = resolve_dual_write_settings(cfg)
    with pytest.raises(DualWriteError, match="does not match the admitted"):
        CcfDualWriteService.create_or_open(settings)

    # The archive is untouched by the refused open.
    assert opened.head() == head_before


# ---------------------------------------------------------------------------
