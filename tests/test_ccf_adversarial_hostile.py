"""Adversarial hostile-content suite (checklist 10b, spec 1.5/12.6).

Hostile bytes are evidence, never instruction. Every fixture here must be
either stored verbatim with zero behavioral effect, or refused LOUDLY
with a precise error (malformed-document recording, a typed exception).
Nothing may silently execute, escape the vault, corrupt the archive, or
hang.

Fixtures: ``tests/fixtures/security_hostile/payloads.json`` (prompt
injection corpus) plus locally generated traversal / symlink / NUL /
surrogate / unicode / wikilink-graph / YAML-bomb cases.

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf.obsidian.importer import ObsidianImporter
from ccf.obsidian.notes import NoteParseError, parse_note
from ccf.obsidian.vault import VaultScanError, scan_vault
from ccf.thothmap import MapContext

from ccf_helpers import make_rig

import sys

sys.path.insert(0, str(Path(__file__).parent))
from security_hostile_fixtures import hostile_fixture_corpus


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


def _importer(rig, vault: Path) -> ObsidianImporter:
    ctx = MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)
    return ObsidianImporter(
        producer=rig.producer, archive=rig.archive, ctx=ctx, vault_root=vault
    )


def _blob_bytes(rig, blob_id: str) -> bytes:
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            "SELECT plaintext_bytes FROM blob_content WHERE blob_id = %s",
            (blob_id,),
        ).fetchone()
    assert row is not None
    return bytes(row[0])


# ---------------------------------------------------------------------------
# Prompt-injection corpus through both import paths
# ---------------------------------------------------------------------------


def test_hostile_corpus_through_obsidian_import(rig, tmp_path):
    """Every hostile fixture is stored verbatim; none changes behavior."""
    vault = tmp_path / "vault"
    vault.mkdir()
    corpus = hostile_fixture_corpus()
    assert len(corpus) == 8
    expected = {}
    for fixture in corpus:
        text = fixture["text"]
        path = vault / f"{fixture['id']}.md"
        path.write_text(f"---\ntitle: {fixture['id']}\n---\n{text}\n", encoding="utf-8")
        expected[path.name] = path.read_bytes()

    report = _importer(rig, vault).import_vault()
    assert not report.malformed, report.malformed
    assert not report.admission_errors, report.admission_errors[:2]
    assert len(report.notes) == len(corpus)

    # Stored bytes are byte-identical to the hostile inputs.
    for name, raw in expected.items():
        record = report.notes[name]
        assert _blob_bytes(rig, record.blob_id) == raw

    # The archive still verifies end-to-end; content was evidence only.
    assert rig.archive.verify_chain()["commits_verified"] >= 2


def test_hostile_corpus_through_dual_write_mirror(
    tmp_path, ccf_postgres_dsn, ccf_settings, monkeypatch
):
    """The same corpus through the real collector + dual-write mirror."""
    monkeypatch.setenv("THOTH_CCF_POSTGRES_DSN", ccf_postgres_dsn)
    from core.config import Config
    from core.metadata_db import MetadataDB
    from core.path_layout import build_path_layout

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
                "enabled": True,
                "dual_write": True,
                "backend": "postgres",
                "dsn_env": "THOTH_CCF_POSTGRES_DSN",
                "schema": ccf_settings.schema,
                "device_key_path": str(tmp_path / "ccf" / "device.pem"),
                "archive_key_path": str(tmp_path / "ccf" / "archive.pem"),
                "error_log_path": str(tmp_path / "errors.jsonl"),
            },
        },
    }
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    corpus = hostile_fixture_corpus()
    for fixture in corpus:
        (inbox / f"{fixture['id']}.md").write_text(
            f"---\ntitle: {fixture['id']}\n---\n{fixture['text']}\n",
            encoding="utf-8",
        )

    from collectors.imported_markdown_connector import ImportedMarkdownConnector

    layout = build_path_layout(cfg)
    db = MetadataDB(str(tmp_path / ".thoth_system" / "meta.db"))
    connector = ImportedMarkdownConnector(cfg, layout=layout, db=db)
    result = asyncio.run(
        connector.collect(import_dirs=[inbox], source_name="hostile_corpus")
    )
    assert len(result.records) == len(corpus)

    # Every capture mirrored: one blob per hostile file, bytes exact.
    with open_ccf_connection(ccf_settings) as conn:
        blob_count = conn.execute("SELECT COUNT(*) FROM blob_content").fetchone()[0]
        archive_id = conn.execute("SELECT archive_id FROM archive").fetchone()[0]
    assert blob_count == len(corpus)
    # Mirror failures, if any, must be loud and ledgered — never silent.
    assert not (tmp_path / "errors.jsonl").exists()

    # Reconcile: zero mismatches across the hostile corpus.
    from scripts.ccf_dualwrite_check import CcfSnapshot, load_legacy_inventory, reconcile

    inventory = load_legacy_inventory(
        tmp_path / ".thoth_system" / "meta.db",
        vault_root=tmp_path / "knowledge_vault",
    )
    snapshot = CcfSnapshot(
        CcfPostgresSettings(enabled=True, dsn=ccf_postgres_dsn, schema=ccf_settings.schema)
    )
    report = reconcile(inventory, snapshot)
    assert report["summary"]["ok"], report["mismatches"][:3]
    assert archive_id == snapshot.archive_id


# ---------------------------------------------------------------------------
# Path traversal / symlink escapes
# ---------------------------------------------------------------------------


def test_traversal_wikilinks_never_escape_vault(rig, tmp_path):
    vault = tmp_path / "vault"
    vault.mkdir()
    outside = tmp_path / "secret.md"
    outside.write_text("outside the vault\n", encoding="utf-8")
    note = vault / "trap.md"
    note.write_text(
        "---\ntitle: trap\n---\n"
        "[[../../secret]] [[/etc/passwd]] [[..%2f..%2fsecret]]\n"
        "![[../../../etc/shadow.pdf]]\n",
        encoding="utf-8",
    )
    report = _importer(rig, vault).import_vault()
    assert not report.admission_errors, report.admission_errors[:2]
    # The note imports; every traversal target is unresolved or missing —
    # nothing was read from outside the vault.
    assert "trap.md" in report.notes
    assert len(report.unresolved_links) >= 3
    assert len(report.missing_attachments) == 1
    # And the only blob in the archive is the trap note itself.
    with open_ccf_connection(rig.settings) as conn:
        count = conn.execute("SELECT COUNT(*) FROM blob_content").fetchone()[0]
    assert count == 1


def test_symlink_in_vault_is_refused(rig, tmp_path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "real.md").write_text("real\n", encoding="utf-8")
    target = tmp_path / "outside.md"
    target.write_text("outside\n", encoding="utf-8")
    (vault / "escape.md").symlink_to(target)
    with pytest.raises(VaultScanError, match="refusing to follow symlink"):
        scan_vault(vault)


def test_directory_symlink_is_refused(rig, tmp_path):
    vault = tmp_path / "vault"
    vault.mkdir()
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    (outside_dir / "stolen.md").write_text("stolen\n", encoding="utf-8")
    (vault / "linked-dir").symlink_to(outside_dir, target_is_directory=True)
    with pytest.raises(VaultScanError, match="refusing to follow symlink"):
        scan_vault(vault)


# ---------------------------------------------------------------------------
# Hostile encodings and structure
# ---------------------------------------------------------------------------


def test_nul_bytes_in_note_body(rig, tmp_path):
    """NUL bytes: raw bytes preserved in the Blob; derived text is safe."""
    vault = tmp_path / "vault"
    vault.mkdir()
    raw = b"---\ntitle: nul\n---\nbody with \x00 NUL byte\n"
    (vault / "nul.md").write_bytes(raw)
    report = _importer(rig, vault).import_vault()
    assert not report.admission_errors, report.admission_errors[:2]
    record = report.notes["nul.md"]
    # Evidence bytes are byte-identical...
    assert _blob_bytes(rig, record.blob_id) == raw
    # ...while the derived description carries no NUL (jsonb boundary).
    obj = rig.archive.get_object(record.artifact_id)
    semantic = obj["compartments"]["semantic"]["envelope"]["content"]
    assert "\x00" not in semantic["payload"]["description"]
    assert "\ufffd" in semantic["payload"]["description"]


def test_unpaired_surrogate_bytes_are_malformed_not_fatal(rig, tmp_path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "surrogate.md").write_bytes(b"---\ntitle: s\n---\nlone \xed\xa0\x80 here\n")
    (vault / "healthy.md").write_text("healthy\n", encoding="utf-8")
    report = _importer(rig, vault).import_vault()
    # The surrogate file is a malformed document: recorded, skipped.
    assert [m["relpath"] for m in report.malformed] == ["surrogate.md"]
    # The run continued and admitted the healthy note.
    assert "healthy.md" in report.notes
    assert not report.admission_errors, report.admission_errors[:2]


def test_escaped_surrogate_in_frontmatter_is_sanitized(rig, tmp_path):
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "esc.md").write_text(
        '---\ntitle: "lone \\ud800 surrogate"\n---\nbody\n', encoding="utf-8"
    )
    report = _importer(rig, vault).import_vault()
    assert not report.admission_errors, report.admission_errors[:2]
    record = report.notes["esc.md"]
    obj = rig.archive.get_object(record.artifact_id)
    name = obj["compartments"]["semantic"]["envelope"]["content"]["payload"]["name"]
    assert "\ufffd" in name
    assert rig.archive.verify_chain()["commits_verified"] >= 2


def test_hostile_unicode_filename_and_content(rig, tmp_path):
    vault = tmp_path / "vault"
    vault.mkdir()
    # Bidi override + zero-width chars in the filename; NFC/NFKC-lookalike
    # content inside.
    name = "notes\u202e\u200b.md"
    (vault / name).write_text(
        "---\ntitle: bidi\n---\n\u202eflow reversed\u202c \u200bzero\u200dwidth\n",
        encoding="utf-8",
    )
    report = _importer(rig, vault).import_vault()
    assert not report.admission_errors, report.admission_errors[:2]
    assert name in report.notes
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            "SELECT native_id FROM origin_index WHERE object_kind = 'record' "
            "AND native_id LIKE '%\u202e%'"
        ).fetchone()
    assert row is not None and row[0] == name


def test_huge_frontmatter_completes(rig, tmp_path):
    vault = tmp_path / "vault"
    vault.mkdir()
    big = "x" * (2 * 1024 * 1024)
    (vault / "big.md").write_text(
        f"---\ntitle: big\nblob: {big}\n---\nbody\n", encoding="utf-8"
    )
    report = _importer(rig, vault).import_vault()
    assert not report.malformed, report.malformed
    assert "big.md" in report.notes


def test_non_finite_yaml_scalars_do_not_crash_import(rig, tmp_path):
    """`.nan` / `.inf` frontmatter scalars: JCS rejects non-finite floats,
    so they cross the JSON boundary as text — import completes."""
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "nan.md").write_text(
        "---\ntitle: nan\nscore: .nan\nlimit: .inf\n---\nbody\n", encoding="utf-8"
    )
    report = _importer(rig, vault).import_vault()
    assert not report.admission_errors, report.admission_errors[:2]
    record = report.notes["nan.md"]
    obj = rig.archive.get_object(record.artifact_id)
    fm = obj["compartments"]["semantic"]["envelope"]["content"]["payload"][
        "extensions"
    ]["obsidian_frontmatter"]
    assert fm["score"] == "nan"
    assert fm["limit"] == "inf"


def test_yaml_billion_laughs_fails_closed_fast(rig, tmp_path):
    """A 434-byte alias bomb must be refused as malformed, in milliseconds."""
    bomb = "---\na: &a " + str(["x" * 10] * 10) + "\n"
    for name, prev in (("b", "a"), ("c", "b"), ("d", "c"), ("e", "d")):
        bomb += f"{name}: &{name} [" + ",".join([f"*{prev}"] * 10) + "]\n"
    bomb += "z: [*e]\n---\nbody\n"
    with pytest.raises(NoteParseError, match="anchors are not allowed"):
        parse_note(bomb, fallback_title="bomb")

    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "bomb.md").write_text(bomb, encoding="utf-8")
    (vault / "healthy.md").write_text("healthy\n", encoding="utf-8")
    report = _importer(rig, vault).import_vault()
    assert [m["relpath"] for m in report.malformed] == ["bomb.md"]
    assert "healthy.md" in report.notes
    assert not report.admission_errors, report.admission_errors[:2]


def test_yaml_alias_in_frontmatter_alone_is_refused():
    # The anchor is scanned before its first alias, so the refusal names
    # anchors first; a bare alias names aliases. Either way it fails closed.
    with pytest.raises(NoteParseError, match="anchors are not allowed"):
        parse_note("---\na: &x 1\nb: *x\n---\nbody\n", fallback_title="x")


def test_deeply_nested_frontmatter_is_malformed_not_fatal(rig, tmp_path):
    vault = tmp_path / "vault"
    vault.mkdir()
    depth = 5000
    nested = "[" * depth + "1" + "]" * depth
    (vault / "nested.md").write_text(f"---\na: {nested}\n---\nbody\n", encoding="utf-8")
    (vault / "healthy.md").write_text("healthy\n", encoding="utf-8")
    report = _importer(rig, vault).import_vault()
    assert [m["relpath"] for m in report.malformed] == ["nested.md"]
    assert "healthy.md" in report.notes


def test_deep_wikilink_chain_and_cycles(rig, tmp_path):
    """300-note chain + a mutual cycle + self-links: no recursion, no hang."""
    vault = tmp_path / "vault"
    vault.mkdir()
    count = 300
    for i in range(count):
        nxt = f"[[chain-{i + 1:04d}]]" if i + 1 < count else "[[chain-0000]]"
        (vault / f"chain-{i:04d}.md").write_text(
            f"---\ntitle: chain {i}\n---\n{nxt}\n", encoding="utf-8"
        )
    (vault / "cycle-a.md").write_text("[[cycle-b]]\n", encoding="utf-8")
    (vault / "cycle-b.md").write_text("[[cycle-a]]\n", encoding="utf-8")
    (vault / "self.md").write_text("[[self]]\n", encoding="utf-8")

    report = _importer(rig, vault).import_vault()
    assert not report.admission_errors, report.admission_errors[:2]
    assert len(report.notes) == count + 3
    edge_pairs = {(e["from_relpath"], e["to_relpath"]) for e in report.wikilink_edges}
    # The cycle closed both ways; the final chain link wraps to chain-0000.
    assert ("cycle-a.md", "cycle-b.md") in edge_pairs
    assert ("cycle-b.md", "cycle-a.md") in edge_pairs
    assert (f"chain-{count - 1:04d}.md", "chain-0000.md") in edge_pairs
    # Self-links are never graph edges.
    assert not any(f == t for f, t in edge_pairs)
    assert rig.archive.verify_chain()["commits_verified"] >= 2
