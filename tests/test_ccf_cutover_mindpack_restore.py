"""Mindpack export-to-empty-database restore cutover gate."""

from __future__ import annotations

import uuid

import pytest

from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf_cutover_test_support import make_cutover_rig, object_counts


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_cutover_rig(ccf_settings, tmp_path, ccf_package_root)


# Gate 3: mindpack export -> empty database restore
# ---------------------------------------------------------------------------


def _second_schema(ccf_postgres_dsn: str) -> CcfPostgresSettings:
    return CcfPostgresSettings(
        enabled=True,
        dsn=ccf_postgres_dsn,
        schema=f"ccf_restore_{uuid.uuid4().hex[:12]}",
    )


def _object_counts(settings: CcfPostgresSettings) -> dict:
    return object_counts(settings)


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
