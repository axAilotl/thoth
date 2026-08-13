"""Adversarial crash-injection suite (checklist 10b, spec 6.4).

Crashes at every seam of the dual-write and admission pipelines must
leave NO torn state: admission is one serialized transaction, the
producer spool is durable, and replay is idempotent. Faults are injected
with monkeypatches and a faulting connection wrapper; every case asserts
the exact post-crash state and a clean recovery.

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker.
"""

from __future__ import annotations

import contextlib
import json
from pathlib import Path

import pytest

from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf_helpers import make_rig
from ccf_cutover_test_support import collect_imported_markdown, dualwrite_config


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


def _concept(rig, label):
    return rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={
            "label": label,
            "definition": f"definition of {label}",
            "aliases": [],
            "extensions": {},
        },
    )


def _object_count(rig) -> int:
    with open_ccf_connection(rig.settings) as conn:
        return conn.execute("SELECT COUNT(*) FROM object_header").fetchone()[0]


# ---------------------------------------------------------------------------
# Crash between spool and admission
# ---------------------------------------------------------------------------


def test_crash_after_spool_before_admit_recovers_via_pending(rig):
    record = _concept(rig, "spooled-then-crashed")
    batch = rig.producer.create_batch(records=[record])
    head_before = rig.archive.head()
    # "Crash": the process dies here. The batch is durable in the spool.

    pending = rig.producer.pending_batches()
    assert [b["batch_id"] for b in pending] == [batch["batch_id"]]
    provisional = rig.producer.provisional_objects()
    assert {o["status"] for o in provisional} == {"provisional"}

    # Recovery: replay the pending batch through sync_pending.
    results = rig.producer.sync_pending(rig.archive)
    assert len(results) == 1
    assert results[0]["status"] == "accepted", results[0]
    assert rig.archive.head() != head_before

    # A second replay returns the stored outcome — nothing double-admits.
    assert rig.producer.pending_batches() == []
    replay = rig.archive.admit_batch(batch)
    assert replay["status"] == "accepted"
    assert replay["commit_sequence"] == results[0]["commit_sequence"]
    assert rig.archive.verify_chain()["commits_verified"] >= 2


def test_crash_mid_commit_before_journal_signature(rig, monkeypatch):
    """Crash inside the commit: the whole admission transaction rolls back."""
    import ccf.admission as admission_module

    batch = rig.producer.create_batch(records=[_concept(rig, "doomed")])
    head_before = rig.archive.head()
    objects_before = _object_count(rig)

    def _explode(*args, **kwargs):
        raise RuntimeError("simulated crash inside commit_objects")

    monkeypatch.setattr(admission_module, "commit_objects", _explode)
    with pytest.raises(RuntimeError, match="simulated crash"):
        rig.archive.admit_batch(batch)
    monkeypatch.undo()

    # No torn state: head, objects, and journal are exactly as before.
    assert rig.archive.head() == head_before
    assert _object_count(rig) == objects_before
    assert rig.archive.verify_chain()["commits_verified"] >= 1

    # The spooled batch survives and admits cleanly on retry.
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "accepted", result
    assert _object_count(rig) == objects_before + 2  # record + commit Record
    assert rig.archive.verify_chain()["commits_verified"] >= 2


class _CrashOnHeadAdvance:
    """Connection proxy that dies right before the head advance."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None, **kwargs):
        if "UPDATE archive_head" in sql:
            import psycopg

            raise psycopg.OperationalError(
                "simulated crash after journal write, before head advance"
            )
        if params is None:
            return self._conn.execute(sql, **kwargs)
        return self._conn.execute(sql, params, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_crash_after_journal_write_before_head_advance(rig, monkeypatch):
    """Crash at the last write of the admission transaction: still atomic."""
    import ccf.archive as archive_module
    import ccf.db as db_module

    batch = rig.producer.create_batch(records=[_concept(rig, "headless")])
    head_before = rig.archive.head()
    objects_before = _object_count(rig)

    real_open = db_module.open_ccf_connection

    @contextlib.contextmanager
    def faulty_open(settings):
        with real_open(settings) as conn:
            yield _CrashOnHeadAdvance(conn)

    monkeypatch.setattr(archive_module, "open_ccf_connection", faulty_open)
    import psycopg

    with pytest.raises(psycopg.OperationalError, match="simulated crash"):
        rig.archive.admit_batch(batch)
    monkeypatch.undo()

    assert rig.archive.head() == head_before
    assert _object_count(rig) == objects_before
    with open_ccf_connection(rig.settings) as conn:
        journal_rows = conn.execute(
            "SELECT COUNT(*) FROM commit_journal WHERE sequence > %s",
            (int(head_before["sequence"]),),
        ).fetchone()[0]
    assert journal_rows == 0  # no orphaned journal entries

    result = rig.archive.admit_batch(batch)
    assert result["status"] == "accepted", result
    assert rig.archive.verify_chain()["commits_verified"] >= 2


# ---------------------------------------------------------------------------
# Crash between the legacy commit and the CCF mirror
# ---------------------------------------------------------------------------


def _dualwrite_config(tmp_path: Path, schema: str):
    return dualwrite_config(tmp_path, schema)


def _collect(cfg, import_dir: Path, tmp_path: Path):
    return collect_imported_markdown(
        cfg, import_dir, tmp_path, source_name="crash_corpus"
    )


def test_mirror_crash_after_legacy_commit_leaves_no_divergence(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    note = tmp_path / "inbox" / "crash.md"
    note.parent.mkdir(parents=True)
    note.write_text("---\ntitle: Crash\n---\ncrash between stores\n", encoding="utf-8")

    # Kill the mirror exactly once, after the legacy write committed.
    from ccf.archive import Archive

    real_admit = Archive.admit_batch
    calls = {"count": 0}

    def _die_once(self, batch, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("simulated mirror crash after legacy commit")
        return real_admit(self, batch, **kwargs)

    monkeypatch.setattr(Archive, "admit_batch", _die_once)
    result = _collect(cfg, note.parent, tmp_path)
    assert result.records  # legacy write intact

    # The failure is ledgered, and the archive holds NO half-admitted
    # state from the crashed batch.
    ledger = (tmp_path / "errors.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(ledger) == 1
    assert "simulated mirror crash" in json.loads(ledger[0])["error"]
    with open_ccf_connection(ccf_settings) as conn:
        origins = conn.execute("SELECT COUNT(*) FROM origin_index").fetchone()[0]
        kinds = {
            kind: count
            for kind, count in conn.execute(
                "SELECT object_kind, COUNT(*) FROM object_header GROUP BY 1"
            ).fetchall()
        }
    assert origins == 0, "crashed mirror left origin rows behind"
    assert kinds.get("blob", 0) == 0

    # The crashed batch is durable in the producer spool (queued); the
    # re-run's mirror signs and admits a fresh batch on top of it — the
    # chain is not bricked by the crash.
    monkeypatch.setattr(Archive, "admit_batch", real_admit)
    result = _collect(cfg, note.parent, tmp_path)
    assert result.records
    with open_ccf_connection(ccf_settings) as conn:
        blobs = conn.execute("SELECT COUNT(*) FROM blob_content").fetchone()[0]
    assert blobs == 1  # exactly one mirrored blob, no duplicates

    # Reconcile to zero mismatches.
    from scripts.ccf_dualwrite_check import (
        CcfSnapshot,
        load_legacy_inventory,
        reconcile,
    )

    inventory = load_legacy_inventory(
        tmp_path / ".thoth_system" / "meta.db",
        vault_root=tmp_path / "knowledge_vault",
    )
    snapshot = CcfSnapshot(
        CcfPostgresSettings(enabled=True, dsn=ccf_postgres_dsn, schema=ccf_settings.schema)
    )
    report = reconcile(inventory, snapshot)
    assert report["summary"]["ok"], report["mismatches"][:3]
