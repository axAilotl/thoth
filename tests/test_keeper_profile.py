"""Tests for the supervised read-only Keeper profile."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from core.config import Config
from core.keeper_profile import KeeperProfile, KeeperProfileConfig, KeeperProfileError
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from tests.fixtures.cissa_like_recording import make_cissa_like_recording


def _make_config(tmp_path: Path) -> Config:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", str(tmp_path / ".thoth_system" / "meta.db"))
    return config


def _make_db(tmp_path: Path) -> tuple[Config, MetadataDB]:
    config = _make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    db_path = layout.database_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = MetadataDB(str(db_path))
    db.ensure_archivist_corpus_tables()
    return config, db


def _cissa_document(recording, tmp_path: Path):
    from core.archivist_retrieval.models import ArchivistCorpusDocument

    transcript_path = tmp_path / recording.transcript_path
    transcript_path.parent.mkdir(parents=True, exist_ok=True)
    transcript_path.write_text(recording.transcript_text, encoding="utf-8")
    return ArchivistCorpusDocument(
        candidate_key=f"vault:{recording.transcript_path}",
        path=transcript_path,
        scope="vault",
        scope_relative_path=recording.transcript_path,
        source_type=recording.source_type,
        file_type="markdown",
        title=recording.title,
        tags=("cissa", "transcript", "voice_recorder"),
        content_text=recording.transcript_text,
        source_hash=recording.transcript_sha256,
        size_bytes=transcript_path.stat().st_size,
        updated_at="2026-08-29T21:42:18Z",
        source_id=recording.session_id,
        source_key=f"{recording.source_type}:{recording.session_id}",
        source_trust_score=1.0,
        source_trust_reason="fixture_trusted",
        source_security_status="allowed",
        artifact_id=recording.artifact_id,
        event_id=None,
    )


def test_keeper_profile_readiness_unavailable_storage(tmp_path: Path):
    config = KeeperProfileConfig(
        db_path=str(tmp_path / "missing.db"),
        allowed_roots=["vault/transcripts"],
    )
    profile = KeeperProfile(config)
    readiness = profile.readiness()
    assert readiness.status == "unavailable_storage"
    assert readiness.document_count == 0


def test_keeper_profile_readiness_stale_index(tmp_path: Path):
    _config, db = _make_db(tmp_path)
    readiness = KeeperProfile(
        KeeperProfileConfig(
            db_path=str(db.db_path), allowed_roots=["vault/transcripts"]
        )
    ).readiness()
    assert readiness.status == "stale_index"
    assert readiness.document_count == 0


def test_keeper_profile_requires_explicit_allowed_roots():
    with pytest.raises(KeeperProfileError, match="At least one allowed root"):
        KeeperProfileConfig("foo.db", [])
    with pytest.raises(KeeperProfileError, match="Unsupported root scope"):
        KeeperProfileConfig("foo.db", ["bad/path"])


def test_keeper_profile_query_retrieves_cissa_evidence(tmp_path: Path):
    _config, db = _make_db(tmp_path)
    recording = make_cissa_like_recording()
    document = _cissa_document(recording, tmp_path)
    db.upsert_archivist_corpus_document(document)

    profile = KeeperProfile(
        KeeperProfileConfig(
            db_path=str(db.db_path),
            allowed_roots=["vault/transcripts"],
            stale_index_seconds=86400 * 365,
        )
    )
    result = profile.query("open schema adoption", limit=5)

    assert result.status == "ok"
    assert result.readiness == "ready"
    assert result.total == 1
    passage = result.passages[0]
    assert passage.artifact_id == recording.artifact_id
    assert passage.source_id == recording.session_id
    assert passage.source_type == recording.source_type
    assert "schema" in passage.snippet.lower()
    assert passage.selector == f"vault:{recording.transcript_path}"
    assert passage.trust_score == 1.0
    assert passage.provenance["artifact_id"] == recording.artifact_id
    assert passage.provenance["source_id"] == recording.session_id


def test_keeper_profile_query_enforces_root_containment(tmp_path: Path):
    _config, db = _make_db(tmp_path)
    recording = make_cissa_like_recording()
    cissa_doc = _cissa_document(recording, tmp_path)
    db.upsert_archivist_corpus_document(cissa_doc)

    from core.archivist_retrieval.models import ArchivistCorpusDocument

    other_path = tmp_path / "vault" / "repos" / "other.md"
    other_path.parent.mkdir(parents=True, exist_ok=True)
    other_path.write_text("Open schema adoption discussion.", encoding="utf-8")
    other_doc = ArchivistCorpusDocument(
        candidate_key="vault:repos/other.md",
        path=other_path,
        scope="vault",
        scope_relative_path="repos/other.md",
        source_type="repository",
        file_type="markdown",
        title="Other",
        tags=(),
        content_text="Open schema adoption discussion.",
        source_hash="other-hash",
        size_bytes=other_path.stat().st_size,
        updated_at="2026-08-29T21:42:18Z",
        source_trust_score=1.0,
    )
    db.upsert_archivist_corpus_document(other_doc)

    profile = KeeperProfile(
        KeeperProfileConfig(
            db_path=str(db.db_path),
            allowed_roots=["vault/transcripts"],
            stale_index_seconds=86400 * 365,
        )
    )
    result = profile.query("open schema", limit=5)
    assert result.total == 1
    assert result.passages[0].candidate_key == cissa_doc.candidate_key


def test_keeper_profile_query_excludes_untrusted_and_quarantined(tmp_path: Path):
    _config, db = _make_db(tmp_path)
    recording = make_cissa_like_recording()
    cissa_doc = _cissa_document(recording, tmp_path)

    from core.archivist_retrieval.models import ArchivistCorpusDocument

    quarantined = ArchivistCorpusDocument(
        candidate_key="vault:transcripts/quarantined.md",
        path=tmp_path / "vault" / "transcripts" / "quarantined.md",
        scope="vault",
        scope_relative_path="transcripts/quarantined.md",
        source_type="note",
        file_type="markdown",
        title="Quarantined",
        tags=(),
        content_text="Open schema adoption and pipeline auditability.",
        source_hash="q-hash",
        size_bytes=10,
        updated_at="2026-08-29T21:42:18Z",
        source_trust_score=0.0,
        source_security_status="blocked",
    )

    db.upsert_archivist_corpus_document(cissa_doc)
    db.upsert_archivist_corpus_document(quarantined)

    profile = KeeperProfile(
        KeeperProfileConfig(
            db_path=str(db.db_path),
            allowed_roots=["vault/transcripts"],
            stale_index_seconds=86400 * 365,
        )
    )
    result = profile.query("open schema", limit=5)
    assert result.total == 1
    assert result.passages[0].candidate_key == cissa_doc.candidate_key
    assert result.passages[0].security_status == "allowed"


def test_keeper_profile_stdio_command_wired():
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "thoth_keeper.py", "--help"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "--roots" in result.stdout
    assert "--db" in result.stdout


def test_keeper_profile_stdio_readiness_and_query(tmp_path: Path):
    _config, db = _make_db(tmp_path)
    recording = make_cissa_like_recording()
    db.upsert_archivist_corpus_document(_cissa_document(recording, tmp_path))

    repo_root = Path(__file__).resolve().parents[1]
    proc = subprocess.Popen(
        [
            sys.executable,
            "thoth_keeper.py",
            "--db",
            str(db.db_path),
            "--roots",
            "vault/transcripts",
            "--stale-index-seconds",
            "8640000",
        ],
        cwd=repo_root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        messages = [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
            },
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "keeper_readiness",
                    "arguments": {},
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "keeper_query",
                    "arguments": {"query": "open schema", "limit": 5},
                },
            },
        ]
        for message in messages:
            proc.stdin.write(json.dumps(message) + "\n")
        proc.stdin.flush()
        proc.stdin.close()

        lines = []
        for line in proc.stdout:
            lines.append(line.strip())
            if len(lines) >= 4:
                break

        assert len(lines) == 4
        init_resp = json.loads(lines[0])
        tools_resp = json.loads(lines[1])
        readiness_resp = json.loads(lines[2])
        query_resp = json.loads(lines[3])

        assert init_resp["id"] == 1
        assert init_resp["result"]["serverInfo"]["name"] == "thoth-keeper"

        assert tools_resp["id"] == 2
        tool_names = {tool["name"] for tool in tools_resp["result"]["tools"]}
        assert tool_names == {"keeper_readiness", "keeper_query"}

        assert readiness_resp["id"] == 3
        readiness_payload = json.loads(
            readiness_resp["result"]["content"][0]["text"]
        )
        assert readiness_payload["status"] == "ready"
        assert readiness_payload["document_count"] == 1

        assert query_resp["id"] == 4
        query_payload = json.loads(query_resp["result"]["content"][0]["text"])
        assert query_payload["status"] == "ok"
        assert query_payload["total"] == 1
        passage = query_payload["passages"][0]
        assert passage["artifact_id"] == recording.artifact_id
        assert passage["source_id"] == recording.session_id
        assert passage["provenance"]["artifact_id"] == recording.artifact_id
    finally:
        proc.wait(timeout=5)
