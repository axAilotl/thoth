"""CCF cutover gates (checklist section 10b, part 1).

The remaining cutover proofs, each as a real test with direct assertions:

1. **Independent vector gate** — every published vector file under
   ``spec/ccf/0.1.1/vectors`` is reproduced by this pure-Python test suite
   alone (no package JS tooling involved), and no vector file ships
   unconsumed.
2. **Projection destruction gate** (spec 8.7/13.7) — an archive holding
   review decisions, entity resolutions, consent + policy lineages, link
   dispositions, and erasure receipts has every projection table DROPPED;
   after migration + rebuild from canonical state, every human decision,
   lineage head, receipt, and projection row is recovered exactly.
3. **Mindpack restore gate** — export the archive, restore into a freshly
   created empty database schema, and verify head hashes, object counts,
   and spot-checked content.
4. **Rollback path** — dual-write enabled and mirroring; flags flipped off
   (legacy capture keeps working with zero CCF contact); ``DROP SCHEMA
   ccf CASCADE`` (legacy store + capture path fully functional); re-enable
   re-bootstraps a clean new genesis; stale keys fail closed.
5b. **Bootstrap compartment retention** — every bootstrap class (policy,
   person, runtime, source, credential) retains its semantic compartment
   after projection destruction, rebuild, and archive reload.

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path

import pytest

from ccf.db import (
    CcfPostgresSettings,
    migrate_ccf_store,
    open_ccf_connection,
)
from ccf.ids import generate_id

from ccf_helpers import authority, make_rig


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)

# ---------------------------------------------------------------------------
# Gate 1: independent vector reproduction
# ---------------------------------------------------------------------------

#: Every file the spec package publishes under vectors/ must be consumed
#: (reproduced or loaded) by at least one pure-Python test module. The
#: package's own JS tooling is never invoked anywhere in this suite.
PUBLISHED_VECTOR_FILES = {
    "canonicalization.json",
    "commit-signing.json",
    "merkle.json",
    "object-hashes.json",
    "ordering.json",
    "producer-batch.json",
    "submission-hashes.json",
    "archive-ed25519-public.pem",
    "device-ed25519-public.pem",
    "TEST-ONLY-archive-ed25519-private.pem",
    "TEST-ONLY-device-ed25519-private.pem",
}


def test_gate1_every_published_vector_is_reproduced_from_tests(ccf_vectors_dir):
    published = {p.name for p in ccf_vectors_dir.iterdir() if p.is_file()}
    published.discard("README.md")
    assert published == PUBLISHED_VECTOR_FILES, (
        f"spec vectors changed; update the gate: {sorted(published)}"
    )
    test_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in Path(__file__).parent.glob("test_ccf_*.py")
    )
    missing = [
        name for name in sorted(published) if name not in test_sources
    ]
    assert not missing, f"vector files no test reproduces: {missing}"


def test_gate1_vector_counts_are_stable(ccf_vectors_dir, load_ccf_json):
    """Pin the published vector counts the suite reproduces (§10 gate 1)."""
    canon = load_ccf_json(ccf_vectors_dir / "canonicalization.json")
    assert len(canon["cases"]) == 7
    assert len(canon["rejections"]) == 7
    merkle = load_ccf_json(ccf_vectors_dir / "merkle.json")
    assert len(merkle["commit1"]["members"]) >= 1
    assert len(merkle["commit2"]["members"]) == 16
    hashes = load_ccf_json(ccf_vectors_dir / "object-hashes.json")
    assert set(hashes) >= {"record", "link", "blob"}


# ---------------------------------------------------------------------------
# Gate 2: projection destruction, human decisions survive
# ---------------------------------------------------------------------------

PROJECTION_TABLES = (
    "projection_link_state",
    "projection_derivation_closure",
    "projection_entity_cluster",
    "projection_full_text",
    "projection_embedding",
    "projection_checkpoint",
    "projection_invalidation",
    "generation_fence",
)


def _drop_all_projections(settings: CcfPostgresSettings) -> None:
    with open_ccf_connection(settings) as conn:
        with conn.transaction():
            for table in PROJECTION_TABLES:
                conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")


def _reprovision_projection_tables(settings: CcfPostgresSettings) -> None:
    """Recreate destroyed projection tables from the pinned migration.

    Migration 0002 is recorded as applied, so ``migrate_ccf_store`` will
    not re-run it; its statements are idempotent by design
    (``CREATE TABLE/INDEX IF NOT EXISTS``), which is the operator recovery
    path after destroying projection tables.
    """
    from ccf.projections.schema import CCF_PROJECTION_MIGRATION

    with open_ccf_connection(settings) as conn:
        with conn.transaction():
            for statement in CCF_PROJECTION_MIGRATION.statements:
                conn.execute(statement)


def _semantic(rig, object_id: str) -> dict:
    obj = rig.archive.get_object(object_id)
    assert obj is not None, f"object missing: {object_id}"
    envelope = obj["compartments"]["semantic"]["envelope"]
    assert envelope is not None, f"semantic compartment unavailable: {object_id}"
    return envelope["content"]


def _lineage_states(rig) -> dict:
    with open_ccf_connection(rig.settings) as conn:
        return {
            row[0]: (row[1], row[2])
            for row in conn.execute(
                "SELECT lineage_id, state, head_record_id FROM lineage_head "
                "WHERE archive_id = %s",
                (rig.archive.archive_id,),
            ).fetchall()
        }


@pytest.fixture()
def decision_archive(rig):
    """An archive holding every human-decision class the gate covers."""
    from ccf.thothmap import MapContext
    from ccf.thothmap.review import review_submissions
    from ccf.thothmap.semantic import assertion_submissions
    from ccf.thothmap.sources import source_submission

    ctx = MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)

    # -- review decision over a memory candidate (governance.review_decision)
    source = source_submission(
        rig.producer,
        ctx,
        {"source_name": "omi", "source_type": "wearable_audio",
         "collector": "thoth.capture"},
    )
    candidate = assertion_submissions(
        rig.producer,
        ctx,
        {
            "candidate_id": "gate2-candidate",
            "candidate_type": "preference",
            "status": "proposed",
            "subject": "Ada",
            "predicate": "prefers",
            "object_value": "projection destruction drills",
            "text": "I prefer drills before cutover.",
            "confidence": 0.8,
        },
        source_ccf_id=source.records[0]["id"],
    )
    mapped = review_submissions(
        rig.producer,
        ctx,
        {"action": "confirm", "actor": "ada",
         "at": "2026-08-12T00:00:00Z", "reason": "gate 2 review"},
        source_ccf_id=source.records[0]["id"],
        target_ccf_ids=[candidate.records[0]["id"]],
        reviewer_ccf_id=rig.person_id,
        accepted_type="semantic.assertion",
        accepted_payload=candidate.records[0]["payload"],
    )
    batch = rig.producer.create_batch(
        records=source.records + candidate.records + mapped.records,
        links=mapped.links,
    )
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result
    review_decision_id = mapped.records[0]["id"]

    # -- entity resolution (human merge adjudication) --------------------
    def _entity(label):
        return rig.producer.new_record(
            type="semantic.entity",
            claims=rig.claims(),
            payload={
                "entity_kind": "person",
                "label": label,
                "aliases": [],
                "description": f"entity {label}",
                "extensions": {},
            },
        )

    e1, e2 = _entity("Ada Lovelace"), _entity("A. Lovelace")
    same_as = rig.producer.new_link(
        type="ccf.same_as", from_id=e1["id"], to_id=e2["id"], claims=rig.claims()
    )
    resolution_claims = rig.claims()
    resolution_claims["authority"] = authority("person_accepted", rig.person_id)
    resolution = rig.producer.new_record(
        type="semantic.entity_resolution",
        claims=resolution_claims,
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "create",
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload={
            "action": "same_as",
            "entity_ids": [e1["id"], e2["id"]],
            "canonical_entity_id": e1["id"],
            "reason": "gate 2 merge",
            "evidence_refs": [],
            "extensions": {},
        },
    )
    batch = rig.producer.create_batch(
        records=[e1, e2, resolution], links=[same_as]
    )
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result

    # -- consent lineage (governance.consent) ----------------------------
    consent_claims = rig.claims()
    consent_claims["authority"] = authority(
        "first_person_statement", rig.person_id, rig.person_id
    )
    consent_lineage_id = generate_id("lineage")
    consent = rig.producer.new_record(
        type="governance.consent",
        claims=consent_claims,
        lineage={
            "lineage_id": consent_lineage_id,
            "previous_head_id": None,
            "transition": "give",
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload={
            "subject_id": rig.person_id,
            "controller_id": rig.person_id,
            "decision": "given",
            "purposes": ["gate-2"],
            "operations": ["read_local"],
            "data_classes": ["document_content"],
            "scope": {},
            "valid_from": "2026-08-12T00:00:00.000Z",
            "expires_at": None,
            "evidence_refs": [],
            "extensions": {},
        },
    )
    batch = rig.producer.create_batch(records=[consent])
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result

    # -- link disposition (retract a derived_from link) ------------------
    target_a = rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={"label": "gate2-a", "definition": "a", "aliases": [],
                 "extensions": {}},
    )
    target_b = rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={"label": "gate2-b", "definition": "b", "aliases": [],
                 "extensions": {}},
    )
    target_c = rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={"label": "gate2-c", "definition": "c", "aliases": [],
                 "extensions": {}},
    )
    derived = rig.producer.new_link(
        type="ccf.derived_from", from_id=target_b["id"], to_id=target_a["id"],
        claims=rig.claims(), selector={},
    )
    # A second derived_from link stays active so the derivation closure
    # projection has rows to destroy and recover.
    active_derived = rig.producer.new_link(
        type="ccf.derived_from", from_id=target_c["id"], to_id=target_a["id"],
        claims=rig.claims(), selector={},
    )
    retract = rig.producer.new_record(
        type="lineage.link_disposition",
        claims=rig.claims(),
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "retract",
            "valid_from": rig.clock(),
            "expires_at": None,
        },
        payload={
            "target_link_id": derived["id"],
            "action": "retract",
            "reason": "gate 2 disposition",
            "previous_disposition_id": None,
            "replacement_link_id": None,
            "extensions": {},
        },
    )
    batch = rig.producer.create_batch(
        records=[target_a, target_b, target_c, retract],
        links=[derived, active_derived],
    )
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result

    # -- erasure saga to receipt (lineage.erasure_receipt) ---------------
    utterance = rig.producer.new_record(
        type="experience.utterance",
        claims=rig.claims(),
        payload={
            "text": "gate 2 erases this utterance",
            "language": "en",
            "speaker_id": None,
            "sequence": None,
            "transcription": None,
            "extensions": {},
        },
    )
    batch = rig.producer.create_batch(records=[utterance])
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "committed", result

    svc = rig.archive.erasure()
    request = svc.submit_request(
        requester_id=rig.person_id,
        subject_id=rig.person_id,
        requested_scope={"targets": [
            {"object_id": utterance["id"], "compartments": ["semantic"]}
        ]},
        reason="gate 2 erasure",
        authority=authority("first_person_statement", rig.person_id, rig.person_id),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=[{"object_id": utterance["id"], "compartments": ["semantic"]}],
        reasoning="approved for gate 2",
        decided_by=rig.person_id,
        authority=authority(
            "explicit_authorization", rig.person_id, rig.person_id
        ),
    )
    status = svc.execute(decided["operation_id"])
    assert status["stage"] == "receipt", status

    with open_ccf_connection(rig.settings) as conn:
        receipt_id = conn.execute(
            """
            SELECT object_id FROM compartment
            WHERE compartment = 'structural' AND state = 'plaintext'
              AND plaintext_json ->> 'type' = 'lineage.erasure_receipt'
            LIMIT 1
            """
        ).fetchone()[0]

    return {
        "review_decision_id": review_decision_id,
        "entity_ids": [e1["id"], e2["id"]],
        "resolution_id": resolution["id"],
        "canonical_entity_id": e1["id"],
        "same_as_link_id": same_as["id"],
        "consent_id": consent["id"],
        "consent_lineage_id": consent_lineage_id,
        "derived_link_id": derived["id"],
        "disposition_id": retract["id"],
        "erased_utterance_id": utterance["id"],
        "receipt_id": receipt_id,
        "operation_id": decided["operation_id"],
    }


def test_gate2_projection_destruction_recovers_every_decision(rig, decision_archive):
    ids = decision_archive

    # Rebuild so every projection is populated, then snapshot canonical
    # human-decision state and projection rows.
    rebuilt = rig.archive.projections.rebuild_all()
    assert all(count >= 1 for count in rebuilt.values()), rebuilt
    before_semantics = {
        label: _semantic(rig, ids[key])
        for label, key in (
            ("review", "review_decision_id"),
            ("resolution", "resolution_id"),
            ("consent", "consent_id"),
            ("disposition", "disposition_id"),
            ("receipt", "receipt_id"),
        )
    }
    before_lineages = _lineage_states(rig)
    content_projections = (
        "projection_link_state",
        "projection_derivation_closure",
        "projection_entity_cluster",
        "projection_full_text",
    )
    with open_ccf_connection(rig.settings) as conn:
        # Content columns only: the trailing metadata pair
        # (computed_through_sequence, generation) is machinery state — the
        # fence generation legitimately resets when the fence table is
        # destroyed, and is asserted separately below.
        before_projections = {
            table: [
                row[:-1]
                for row in conn.execute(
                    f"SELECT * FROM {table} WHERE archive_id = %s ORDER BY 1, 2",
                    (rig.archive.archive_id,),
                ).fetchall()
            ]
            for table in content_projections
        }
        before_link_state = conn.execute(
            "SELECT state, selector_available FROM projection_link_state "
            "WHERE archive_id = %s AND link_id = %s",
            (rig.archive.archive_id, ids["derived_link_id"]),
        ).fetchone()
        before_cluster = conn.execute(
            "SELECT cluster_id, canonical_member_id FROM projection_entity_cluster "
            "WHERE archive_id = %s AND member_id = %s",
            (rig.archive.archive_id, ids["entity_ids"][0]),
        ).fetchone()
    assert before_link_state == ("retracted", True)
    assert before_cluster is not None
    assert before_cluster[1] == ids["canonical_entity_id"]

    # Destroy every projection table outright (not truncate).
    _drop_all_projections(rig.settings)
    with open_ccf_connection(rig.settings) as conn:
        for table in PROJECTION_TABLES:
            assert conn.execute(
                "SELECT 1 FROM pg_tables WHERE tablename = %s "
                "AND schemaname = current_schema()",
                (table,),
            ).fetchone() is None

    # Rebuild from canonical state only.
    _reprovision_projection_tables(rig.settings)
    rebuilt = rig.archive.projections.rebuild_all()
    assert all(count >= 1 for count in rebuilt.values()), rebuilt

    # The signed chain is intact through the whole drill.
    report = rig.archive.verify_chain()
    assert report["commits_verified"] >= 8

    # Every human decision survived with its exact semantic content.
    for label, key in (
        ("review", "review_decision_id"),
        ("resolution", "resolution_id"),
        ("consent", "consent_id"),
        ("disposition", "disposition_id"),
        ("receipt", "receipt_id"),
    ):
        assert _semantic(rig, ids[key]) == before_semantics[label], label

    # Lineage heads survived exactly (policy, consent, resolution,
    # disposition, erasure decision lineage, credential).
    assert _lineage_states(rig) == before_lineages
    assert before_lineages[ids["consent_lineage_id"]][0] == "give"

    # The erasure stayed real: erased compartment serves no plaintext.
    erased = rig.archive.get_object(ids["erased_utterance_id"])
    assert erased["compartments"]["semantic"]["state"] == "erased"
    assert erased["compartments"]["semantic"]["envelope"] is None

    # Projection content recovered row-for-row; every row is stamped with
    # the current head sequence under the fresh fence generation.
    head_sequence = int(rig.archive.head()["sequence"])
    with open_ccf_connection(rig.settings) as conn:
        for table, before_rows in before_projections.items():
            after_rows = conn.execute(
                f"SELECT * FROM {table} WHERE archive_id = %s ORDER BY 1, 2",
                (rig.archive.archive_id,),
            ).fetchall()
            assert [row[:-1] for row in after_rows] == before_rows, table
            assert all(
                int(row[-2]) == head_sequence for row in after_rows
            ), table
        link_state = conn.execute(
            "SELECT state, selector_available FROM projection_link_state "
            "WHERE archive_id = %s AND link_id = %s",
            (rig.archive.archive_id, ids["derived_link_id"]),
        ).fetchone()
        cluster = conn.execute(
            "SELECT cluster_id, canonical_member_id FROM projection_entity_cluster "
            "WHERE archive_id = %s AND member_id = %s",
            (rig.archive.archive_id, ids["entity_ids"][0]),
        ).fetchone()
    assert link_state == ("retracted", True)
    assert cluster == before_cluster

    # The embedding table is recreated by migration but explicitly not
    # rebuildable from canonical state (fail closed, never zeroed).
    from ccf.projections import EMBEDDING
    from ccf.projections.rebuild import RebuildError

    with pytest.raises(RebuildError, match="caller-supplied"):
        rig.archive.projections.rebuild(EMBEDDING)


# ---------------------------------------------------------------------------
# Gate 3: mindpack export -> empty database restore
# ---------------------------------------------------------------------------


def _second_schema(ccf_postgres_dsn: str) -> CcfPostgresSettings:
    return CcfPostgresSettings(
        enabled=True,
        dsn=ccf_postgres_dsn,
        schema=f"ccf_restore_{uuid.uuid4().hex[:12]}",
    )


def _object_counts(settings: CcfPostgresSettings) -> dict:
    with open_ccf_connection(settings) as conn:
        return {
            kind: count
            for kind, count in conn.execute(
                "SELECT object_kind, COUNT(*) FROM object_header GROUP BY 1"
            ).fetchall()
        }


def test_gate3_mindpack_restores_into_empty_database(
    rig, ccf_postgres_dsn, tmp_path, ccf_package_root
):
    from ccf.obsidian.importer import ObsidianImporter
    from ccf.sync.restore import restore_mindpack
    from ccf.thothmap import MapContext

    # Populate a dual-write-shaped archive through the obsidian importer.
    vault = tmp_path / "vault"
    (vault / "notes").mkdir(parents=True)
    (vault / "notes" / "alpha.md").write_text(
        "---\ntitle: Alpha\n---\nAlpha links to [[beta]].\n", encoding="utf-8"
    )
    (vault / "notes" / "beta.md").write_text(
        "---\ntitle: Beta\n---\nBeta body with an attachment ![[asset.png]].\n",
        encoding="utf-8",
    )
    (vault / "notes" / "asset.png").write_bytes(b"\x89PNG\r\n\x1a\n-gate3-bytes")
    ctx = MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)
    importer = ObsidianImporter(
        producer=rig.producer, archive=rig.archive, ctx=ctx, vault_root=vault
    )
    report = importer.import_vault()
    assert not report.admission_errors, report.admission_errors[:2]
    assert len(report.notes) == 2

    source_head = rig.archive.head()
    source_counts = _object_counts(rig.settings)

    # Export the complete mindpack.
    pack_dir = tmp_path / "mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    assert manifest["head_commit_hash"] == source_head["commit_hash"]

    # Restore into a freshly created empty schema.
    restore_settings = _second_schema(ccf_postgres_dsn)
    try:
        restored = restore_mindpack(
            restore_settings,
            package_root=ccf_package_root,
            pack_path=pack_dir,
            trusted_genesis_hash=manifest["genesis_commit_hash"],
            trusted_head_hash=manifest["head_commit_hash"],
        )
        assert restored["status"] == "restored"
        assert restored["archive_id"] == rig.archive.archive_id

        # Head hashes and object counts match the source exactly.
        assert restored["head_commit_hash"] == source_head["commit_hash"]
        assert restored["head_sequence"] == source_head["sequence"]
        assert _object_counts(restore_settings) == source_counts
        assert restored["objects_restored"] == sum(source_counts.values())

        # Spot-check content: note artifact semantics and blob bytes.
        from ccf.archive import Archive

        reopened = Archive.open(
            restore_settings,
            package_root=ccf_package_root,
            archive_key_path=rig.archive_key_path,
        )
        alpha = report.notes["notes/alpha.md"]
        obj = reopened.get_object(alpha.artifact_id)
        assert obj is not None
        semantic = obj["compartments"]["semantic"]["envelope"]["content"]
        assert semantic["payload"]["name"] == "Alpha"
        with open_ccf_connection(restore_settings) as conn:
            row = conn.execute(
                "SELECT plaintext_bytes FROM blob_content WHERE blob_id = %s",
                (alpha.blob_id,),
            ).fetchone()
        assert bytes(row[0]) == (vault / "notes" / "alpha.md").read_bytes()

        # The restored chain verifies end-to-end on its own.
        verification = reopened.verify_chain()
        assert verification["head_commit_hash"] == source_head["commit_hash"]
        assert verification["commits_verified"] == int(source_head["sequence"]) + 1
    finally:
        import psycopg

        with psycopg.connect(ccf_postgres_dsn, autocommit=True) as conn:
            conn.execute(
                f'DROP SCHEMA IF EXISTS "{restore_settings.schema}" CASCADE'
            )


# ---------------------------------------------------------------------------
# Gate 4: rollback path
# ---------------------------------------------------------------------------


def _write_note(path: Path, *, title: str, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"---\ntitle: {title}\n---\n{body}\n", encoding="utf-8")
    return path


def _dualwrite_config(tmp_path: Path, schema: str, *, dual_write=True, enabled=True):
    from core.config import Config

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


def _collect(cfg, import_dir: Path, tmp_path: Path):
    from collectors.imported_markdown_connector import ImportedMarkdownConnector
    from core.metadata_db import MetadataDB
    from core.path_layout import build_path_layout

    layout = build_path_layout(cfg)
    db = MetadataDB(str(tmp_path / ".thoth_system" / "meta.db"))
    connector = ImportedMarkdownConnector(cfg, layout=layout, db=db)
    return asyncio.run(
        connector.collect(import_dirs=[import_dir], source_name="rollback_corpus")
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
    counts_a = _object_counts(ccf_settings)
    assert counts_a.get("blob") == 1
    first_archive_id = _archive_id(ccf_settings)
    queue_a = _legacy_queue_count(tmp_path)

    # Phase B: flags off — legacy capture works with zero CCF contact.
    cfg_off = _dualwrite_config(tmp_path, ccf_settings.schema, dual_write=False)
    note_b = _write_note(inbox / "b.md", title="B", body="phase B")
    result = _collect(cfg_off, note_b.parent, tmp_path)
    assert result.records
    assert _legacy_queue_count(tmp_path) > queue_a
    assert _object_counts(ccf_settings) == counts_a
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
    counts_d = _object_counts(ccf_settings)
    # The fresh archive knows nothing about phases A-C, so the whole
    # re-scanned inbox mirrors into the new genesis: 4 blobs, no replay of
    # the dropped archive's history.
    assert counts_d.get("blob") == 4
    from ccf.archive import Archive

    reopened = Archive.open(
        ccf_settings,
        package_root=Path(__file__).parent.parent / "spec" / "ccf" / "0.1.1",
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
        package_root=Path(__file__).parent.parent / "spec" / "ccf" / "0.1.1",
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
# Gate 5b: bootstrap compartment retention
# ---------------------------------------------------------------------------


def test_gate5b_bootstrap_compartments_survive_projection_destruction(
    rig, ccf_package_root
):
    # A fifth bootstrap class beyond the rig's four: an operator-admitted
    # core.source (bootstrap compartment coverage: policy, person,
    # runtime, credential, source).
    ts = rig.clock()
    source_id = generate_id("record")
    rig.archive.admit_bootstrap(
        [
            {
                "type": "core.source",
                "object_id": source_id,
                "recorded_by": rig.runtime_id,
                "recorded_at": ts,
                "person_id": rig.person_id,
                "authority": authority("runtime_import", rig.runtime_id),
                "privacy": {
                    "data_subjects": [],
                    "data_classes": [],
                    "consent_refs": [],
                    "legal_basis_refs": [],
                    "subject_coverage": "unknown",
                },
                "policy_hint": rig.policy_lineage_id,
                "payload": {
                    "kind": "obsidian_vault",
                    "name": "gate5b vault",
                    "connector": "ccf.obsidian",
                    "native_identity": "gate5b",
                    "trust_class": "trusted",
                    "producer_key_id": None,
                    "extensions": {},
                },
            }
        ]
    )

    bootstrap_ids = {
        "policy": None,  # resolved below from the policy lineage head
        "person": rig.person_id,
        "runtime": rig.runtime_id,
        "credential": None,
        "source": source_id,
    }
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            "SELECT head_record_id FROM lineage_head WHERE lineage_id = %s",
            (rig.policy_lineage_id,),
        ).fetchone()
        bootstrap_ids["policy"] = row[0]
        row = conn.execute(
            "SELECT head_record_id FROM lineage_head WHERE lineage_id = %s",
            (rig.credential_lineage_id,),
        ).fetchone()
        bootstrap_ids["credential"] = row[0]

    def snapshot() -> dict:
        snap = {}
        for label, object_id in bootstrap_ids.items():
            obj = rig.archive.get_object(object_id)
            assert obj is not None, label
            snap[label] = {
                compartment: obj["compartments"].get(compartment, {}).get(
                    "envelope"
                )
                for compartment in ("structural", "semantic")
            }
            if label == "credential":
                # The device credential is admitted semantic=False:
                # structural-only by design.
                assert snap[label]["semantic"] is None
                assert snap[label]["structural"] is not None
            else:
                # Every other bootstrap class carries a semantic compartment.
                assert snap[label]["semantic"] is not None, label
        return snap

    before = snapshot()
    before_head = rig.archive.head()

    # Destroy projections, rebuild, and reload the archive from scratch.
    _drop_all_projections(rig.settings)
    _reprovision_projection_tables(rig.settings)
    rig.archive.projections.rebuild_all()

    from ccf.archive import Archive

    reloaded = Archive.open(
        rig.settings,
        package_root=ccf_package_root,
        archive_key_path=rig.archive_key_path,
    )
    assert reloaded.archive_id == rig.archive.archive_id
    assert reloaded.head() == before_head
    assert reloaded.verify_chain()["commits_verified"] >= 3

    after = snapshot()
    assert after == before
    # Spot-check one semantic payload per class for semantic retention.
    assert after["person"]["semantic"]["content"]["payload"]["kind"] == "human"
    assert after["runtime"]["semantic"]["content"]["payload"]["kind"] == "backend"
    assert after["source"]["semantic"]["content"]["payload"]["kind"] == (
        "obsidian_vault"
    )
    assert (
        after["policy"]["semantic"]["content"]["payload"]["profile"]
        == "ccf.policy/0.1.1"
    )
    credential_structural = after["credential"]["structural"]["content"]
    assert credential_structural["type"] == "core.device_credential"
