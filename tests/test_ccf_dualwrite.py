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


def _dualwrite_config(tmp_path: Path, schema: str, *, dual_write=True, enabled=True, **flags) -> Config:
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
                **flags,
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


# ---------------------------------------------------------------------------
# Phase-2 converter families (transcripts / semantic / review / wiki)
# ---------------------------------------------------------------------------


def _lifecycle_service(cfg: Config, tmp_path: Path):
    from core.capture_lifecycle import CaptureLifecycleService

    layout = build_path_layout(cfg)
    layout.ensure_directories()
    db = MetadataDB(str(tmp_path / ".thoth_system" / "meta.db"))
    service = CaptureLifecycleService(
        cfg, layout=layout, db=db, capture_event_store=None
    )
    return service, db


def _capture_transcript(service, *, artifact_id="omi-transcript-1", text="meeting notes"):
    raw_file = service.layout.raw_root / "omi" / f"{artifact_id}.json"
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    raw_file.write_text(
        json.dumps({"session": artifact_id}) + "\n", encoding="utf-8"
    )
    return service.capture_to_queue(
        artifact_type="transcript",
        payload={
            "id": artifact_id,
            "transcript_id": artifact_id,
            "source_type": "omi",
            "title": "Omi Session",
            "raw_transcript": text,
            "language": "en",
        },
        source={
            "source_name": "omi",
            "source_type": "wearable",
            "collector": "omi_connector",
        },
        session={
            "session_type": "sync",
            "native_session_id": f"{artifact_id}-sync",
            "started_at": "2026-08-01T00:00:00Z",
        },
        raw_path=raw_file,
    )


def _record_types(ccf_settings) -> dict:
    return CcfSnapshot(ccf_settings).record_types


def _ids_of_type(ccf_settings, record_type: str) -> list[str]:
    return sorted(
        object_id
        for object_id, rtype in _record_types(ccf_settings).items()
        if rtype == record_type
    )


def _links(ccf_settings) -> list[tuple]:
    return CcfSnapshot(ccf_settings).links


def _ledger_entries(tmp_path: Path) -> list[dict]:
    ledger = tmp_path / "errors.jsonl"
    if not ledger.exists():
        return []
    return [
        json.loads(line)
        for line in ledger.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _add_candidate_with_evidence(db, queue_artifact_id: str):
    from core.semantic_memory import (
        SemanticMemoryCandidate,
        SemanticMemoryEvidence,
        SemanticMemoryStore,
    )

    store = SemanticMemoryStore(db)
    return store.add_candidate(
        SemanticMemoryCandidate(
            candidate_id="candidate-fact-1",
            candidate_type="fact",
            text="Thoth mirrors captures into CCF.",
            subject="Thoth",
            predicate="mirrors captures into",
            object_value="CCF",
            confidence=0.9,
        ),
        evidence=(
            SemanticMemoryEvidence(
                candidate_id="candidate-fact-1",
                evidence_id="evidence-1",
                artifact_id=queue_artifact_id,
                artifact_type="transcript",
                evidence_text="meeting notes",
            ),
        ),
    )


def test_family_flags_default_off(tmp_path, ccf_postgres_dsn, monkeypatch):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, "ccf_unused")
    settings = resolve_dual_write_settings(cfg)
    assert settings.enabled
    assert not settings.mirror_transcripts
    assert not settings.mirror_semantic
    assert not settings.mirror_review
    assert not settings.mirror_wiki


def test_family_flags_parse(tmp_path, ccf_postgres_dsn, monkeypatch):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(
        tmp_path,
        "ccf_unused",
        mirror_transcripts=True,
        mirror_semantic=True,
        mirror_review=True,
        mirror_wiki=True,
    )
    settings = resolve_dual_write_settings(cfg)
    assert settings.mirror_transcripts
    assert settings.mirror_semantic
    assert settings.mirror_review
    assert settings.mirror_wiki


def test_transcript_family_mirrors_utterances_with_links(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(
        tmp_path, ccf_settings.schema, mirror_transcripts=True
    )
    service, _db = _lifecycle_service(cfg, tmp_path)
    result = _capture_transcript(service)
    assert result.queue_artifact_id == "omi-transcript-1"

    utterances = _ids_of_type(ccf_settings, "experience.utterance")
    assert len(utterances) == 1
    utterance = utterances[0]
    links = _links(ccf_settings)
    assert any(t == "ccf.derived_from" and f == utterance for t, f, _ in links)
    assert any(t == "ccf.generated_by" and f == utterance for t, f, _ in links)
    assert any(t == "ccf.has_transcript" and to == utterance for t, _, to in links)
    assert not _ledger_entries(tmp_path)

    # The reconcile harness itemizes phase-2 family objects instead of
    # flagging them as extras.
    report = _reconcile_workspace(tmp_path, ccf_settings.schema, ccf_postgres_dsn)
    assert report["summary"]["ok"], report["mismatches"]
    assert report["summary"]["derived_records"] >= 1
    assert any(d["class"] == "transcripts" for d in report["derived"])


def test_transcript_family_skipped_when_flag_off(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    service, _db = _lifecycle_service(cfg, tmp_path)
    _capture_transcript(service)

    assert _ids_of_type(ccf_settings, "experience.utterance") == []
    assert not _ledger_entries(tmp_path)


def test_semantic_family_mirrors_canonical_entity(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema, mirror_semantic=True)
    service, db = _lifecycle_service(cfg, tmp_path)
    result = _capture_transcript(service)

    canonical_id = result.canonical_record["normalized_metadata"]["canonical_id"]
    assert db.get_canonical_entity(canonical_id) is not None
    assert len(_ids_of_type(ccf_settings, "semantic.entity")) == 1
    assert not _ledger_entries(tmp_path)


def test_semantic_family_skipped_when_flag_off(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    service, _db = _lifecycle_service(cfg, tmp_path)
    _capture_transcript(service)

    assert _ids_of_type(ccf_settings, "semantic.entity") == []
    assert not _ledger_entries(tmp_path)


def test_family_failure_ledgers_without_blocking_others(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(
        tmp_path,
        ccf_settings.schema,
        mirror_transcripts=True,
        mirror_semantic=True,
    )

    from ccf.dualwrite import families

    def _explode(*args, **kwargs):
        raise RuntimeError("planted transcript failure")

    monkeypatch.setattr(
        families.thothmap_transcripts, "utterance_submissions", _explode
    )
    service, _db = _lifecycle_service(cfg, tmp_path)
    result = _capture_transcript(service)

    # Legacy write and the other families are unaffected.
    assert result.queue_artifact_id == "omi-transcript-1"
    assert _ids_of_type(ccf_settings, "experience.artifact")
    assert _ids_of_type(ccf_settings, "semantic.entity")
    assert _ids_of_type(ccf_settings, "experience.utterance") == []

    ledger = _ledger_entries(tmp_path)
    assert len(ledger) == 1
    assert ledger[0]["kind"] == "mirror_failure"
    assert ledger[0]["context"]["family"] == "transcripts"
    assert "planted transcript failure" in ledger[0]["error"]


def test_review_family_mirrors_artifact_review_decision(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema, mirror_review=True)
    service, db = _lifecycle_service(cfg, tmp_path)
    result = _capture_transcript(service)

    from core.artifact_review_queue import ArtifactReviewQueueService

    updated = ArtifactReviewQueueService(db, config=cfg).reject(
        result.queue_artifact_id, actor="operator", reason="bad audio"
    )
    assert updated.status == "rejected"

    decisions = _ids_of_type(ccf_settings, "governance.review_decision")
    assert len(decisions) == 1
    artifacts = _ids_of_type(ccf_settings, "experience.artifact")
    assert len(artifacts) == 1
    links = _links(ccf_settings)
    assert any(
        t == "ccf.covers" and f == decisions[0] and to == artifacts[0]
        for t, f, to in links
    )
    assert not _ledger_entries(tmp_path)


def test_review_family_skipped_when_flag_off(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema)
    service, db = _lifecycle_service(cfg, tmp_path)
    result = _capture_transcript(service)

    from core.artifact_review_queue import ArtifactReviewQueueService

    ArtifactReviewQueueService(db, config=cfg).reject(
        result.queue_artifact_id, actor="operator", reason="bad audio"
    )
    assert _ids_of_type(ccf_settings, "governance.review_decision") == []
    assert not _ledger_entries(tmp_path)


def test_review_family_mirrors_semantic_review(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema, mirror_review=True)
    service, db = _lifecycle_service(cfg, tmp_path)
    result = _capture_transcript(service)
    candidate = _add_candidate_with_evidence(db, result.queue_artifact_id)

    from core.semantic_memory_review import SemanticMemoryReviewService

    reviewed = SemanticMemoryReviewService(db=db, config=cfg).confirm_candidate(
        candidate.candidate_id, actor="operator", reason="looks right"
    )
    assert reviewed["candidate"]["status"] == "confirmed"

    assertions = _ids_of_type(ccf_settings, "semantic.assertion")
    decisions = _ids_of_type(ccf_settings, "governance.review_decision")
    artifacts = _ids_of_type(ccf_settings, "experience.artifact")
    assert len(assertions) == 1
    assert len(decisions) == 1
    assert len(artifacts) == 1
    links = _links(ccf_settings)
    # Evidence artifact -> assertion, decision -> assertion.
    assert any(
        t == "ccf.evidence_for" and f == artifacts[0] and to == assertions[0]
        for t, f, to in links
    )
    assert any(
        t == "ccf.covers" and f == decisions[0] and to == assertions[0]
        for t, f, to in links
    )
    assert not _ledger_entries(tmp_path)


def test_review_family_semantic_review_skips_unmirrored_evidence(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch, caplog
):
    """Candidate evidence that was never mirrored: skip loudly, no ledger."""
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema, mirror_review=True)
    service, db = _lifecycle_service(cfg, tmp_path)
    candidate = _add_candidate_with_evidence(db, "artifact-never-captured")

    from core.semantic_memory_review import SemanticMemoryReviewService

    with caplog.at_level(logging.WARNING, logger="core.semantic_memory_review"):
        SemanticMemoryReviewService(db=db, config=cfg).confirm_candidate(
            candidate.candidate_id, actor="operator", reason="ok"
        )
    assert any("no mirrored evidence artifact" in r.message for r in caplog.records)
    assert _ids_of_type(ccf_settings, "semantic.assertion") == []
    assert _ids_of_type(ccf_settings, "governance.review_decision") == []
    assert not _ledger_entries(tmp_path)


def test_wiki_family_mirrors_semantic_page_projection(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(
        tmp_path, ccf_settings.schema, mirror_review=True, mirror_wiki=True
    )
    service, db = _lifecycle_service(cfg, tmp_path)
    result = _capture_transcript(service)
    candidate = _add_candidate_with_evidence(db, result.queue_artifact_id)

    from core.semantic_memory_review import SemanticMemoryReviewService

    SemanticMemoryReviewService(db=db, config=cfg).confirm_candidate(
        candidate.candidate_id, actor="operator", reason="looks right"
    )

    from core.wiki_updater import CompiledWikiUpdater

    results = CompiledWikiUpdater(
        cfg, layout=service.layout, db=db
    ).update_from_semantic_memory()
    assert results

    snapshot = CcfSnapshot(ccf_settings)
    wiki_origins = [
        (native_id, object_id)
        for _src, native_id, _rev, kind, object_id in snapshot.origins
        if kind == "record" and native_id.startswith("wiki:")
    ]
    assert wiki_origins, "expected a mirrored wiki projection"
    assertions = _ids_of_type(ccf_settings, "semantic.assertion")
    assert len(assertions) == 1
    # Every projection carries derived_from evidence to the assertion.
    for _native_id, projection_id in wiki_origins:
        assert any(
            t == "ccf.derived_from" and f == projection_id and to == assertions[0]
            for t, f, to in snapshot.links
        )
    assert not _ledger_entries(tmp_path)


def test_wiki_family_skips_pages_without_mirrored_evidence(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch, caplog
):
    """mirror_wiki without mirror_review: no assertion exists to cite."""
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    cfg = _dualwrite_config(tmp_path, ccf_settings.schema, mirror_wiki=True)
    service, db = _lifecycle_service(cfg, tmp_path)
    result = _capture_transcript(service)
    candidate = _add_candidate_with_evidence(db, result.queue_artifact_id)

    from core.semantic_memory_review import SemanticMemoryReviewService

    SemanticMemoryReviewService(db=db, config=cfg).confirm_candidate(
        candidate.candidate_id, actor="operator", reason="looks right"
    )

    from core.wiki_updater import CompiledWikiUpdater

    with caplog.at_level(logging.WARNING, logger="core.wiki_updater"):
        results = CompiledWikiUpdater(
            cfg, layout=service.layout, db=db
        ).update_from_semantic_memory()
    assert results  # pages still compile; only the mirror skips
    assert any(
        "no mirrored candidate assertions" in r.message for r in caplog.records
    )

    snapshot = CcfSnapshot(ccf_settings)
    assert not any(
        native_id.startswith("wiki:")
        for _src, native_id, _rev, kind, _oid in snapshot.origins
        if kind == "record"
    )
    assert not _ledger_entries(tmp_path)
