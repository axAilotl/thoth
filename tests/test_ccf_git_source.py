"""Git-source importer fixture (checklist 10b, 0.1.2 follow-up).

Exercises the obsidian importer's git handling against a REAL ``.git``
repository, generated with live commits:

- commit 1: add ``README`` + notes;
- commit 2: rename + modify + add a binary;
- commit 3: delete + create a branch and a tag.

Covers: initial import, retry idempotency (identical replay), post-commit
re-import revision behavior, rename surfacing as delete+create in the
origin index, ``.git`` internals never imported, the provenance chain, and
projection rebuild after destruction. A nested repo directory becomes a
``git_repository`` source record; its working tree is never blob-dumped.

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker or git.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from ccf.db import open_ccf_connection
from ccf.obsidian.importer import ObsidianImporter
from ccf.thothmap import MapContext

from ccf_helpers import make_rig

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git unavailable")


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        [
            "git", "-C", str(repo),
            "-c", "user.name=ccf-test", "-c", "user.email=ccf@test.invalid",
            *args,
        ],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def _init_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-b", "main")


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _three_commit_repo(repo: Path) -> None:
    """The 0.1.2 fixture history: add, rename+modify+binary, delete+refs."""
    _init_repo(repo)
    (repo / "README.md").write_text("# fixture\nv1\n", encoding="utf-8")
    (repo / "notes").mkdir()
    (repo / "notes" / "n1.md").write_text("note one v1\n", encoding="utf-8")
    _commit(repo, "add README + notes")

    _git(repo, "mv", "notes/n1.md", "notes/n2.md")
    (repo / "README.md").write_text("# fixture\nv2 modified\n", encoding="utf-8")
    (repo / "data.bin").write_bytes(b"\x00\x01\x02binary-fixture")
    _commit(repo, "rename + modify + add binary")

    _git(repo, "rm", "README.md")
    _commit(repo, "delete README")
    _git(repo, "branch", "feature")
    _git(repo, "tag", "v1.0")


def _origin_rows(rig) -> list[tuple]:
    with open_ccf_connection(rig.settings) as conn:
        return conn.execute(
            "SELECT source_id, native_id, revision, object_kind, object_id, "
            "lifecycle FROM origin_index WHERE archive_id = %s ORDER BY 2, 3",
            (rig.archive.archive_id,),
        ).fetchall()


def _object_count(rig) -> int:
    with open_ccf_connection(rig.settings) as conn:
        return conn.execute("SELECT COUNT(*) FROM object_header").fetchone()[0]


def _importer(rig, vault: Path, run_tag: str = "git-fixture") -> ObsidianImporter:
    ctx = MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)
    return ObsidianImporter(
        producer=rig.producer,
        archive=rig.archive,
        ctx=ctx,
        vault_root=vault,
        run_tag=run_tag,
    )


# ---------------------------------------------------------------------------
# Nested repo: source record, tree never dumped, .git ignored
# ---------------------------------------------------------------------------


def test_nested_git_repo_import(rig, tmp_path):
    vault = tmp_path / "vault"
    (vault / "notes").mkdir(parents=True)
    (vault / "notes" / "index.md").write_text(
        "---\ntitle: Index\n---\nOrdinary vault note.\n", encoding="utf-8"
    )
    _three_commit_repo(vault / "repos" / "sample")

    importer = _importer(rig, vault)
    report = importer.import_vault()
    assert not report.admission_errors, report.admission_errors[:2]
    assert len(report.notes) == 1

    # The repo directory became a git_repository source record.
    repo_source_id = report.sources.get("repo:repos/sample")
    assert repo_source_id is not None, report.sources
    obj = rig.archive.get_object(repo_source_id)
    payload = obj["compartments"]["semantic"]["envelope"]["content"]["payload"]
    assert payload["kind"] == "git_repository"

    # The repo working tree and every .git internal stayed out of the
    # archive: no origin tuple references them, no blob carries them.
    origins = _origin_rows(rig)
    assert not [r for r in origins if "repos/sample" in r[1]]
    assert not [r for r in origins if ".git" in r[1]]
    with open_ccf_connection(rig.settings) as conn:
        blobs = conn.execute("SELECT COUNT(*) FROM blob_content").fetchone()[0]
    assert blobs == 1  # the one ordinary note's embedded text blob

    assert rig.archive.verify_chain()["commits_verified"] >= 2


# ---------------------------------------------------------------------------
# Vault-as-repo: commits drive revision behavior across import passes
# ---------------------------------------------------------------------------


@pytest.fixture()
def repo_vault(tmp_path):
    """A flat vault that IS a git repo (working tree imported, .git not)."""
    vault = tmp_path / "repo-vault"
    _init_repo(vault)
    return vault


def _admit_remap(rig, importer, relpath, **kwargs):
    mapped = importer.remap_note(relpath, **kwargs)
    batch = rig.producer.create_batch(
        records=mapped.records,
        links=mapped.links,
        blobs=mapped.blobs,
        blob_data=mapped.blob_data or None,
    )
    return rig.archive.admit_batch(batch, blob_bytes=mapped.blob_data or None)


def test_git_worktree_revision_behavior(rig, repo_vault):
    vault = repo_vault
    (vault / "README.md").write_text("# vault\nv1\n", encoding="utf-8")
    (vault / "note1.md").write_text("note one v1\n", encoding="utf-8")
    (vault / "asset.png").write_bytes(b"\x89PNG-v1")
    _commit(vault, "commit 1: add README + notes")

    # -- initial import ---------------------------------------------------
    importer = _importer(rig, vault)
    report = importer.import_vault()
    assert not report.admission_errors, report.admission_errors[:2]
    assert set(report.notes) == {"README.md", "note1.md"}
    assert "asset.png" in report.attachment_blobs
    count_after_import = _object_count(rig)

    origins = _origin_rows(rig)
    assert not [r for r in origins if ".git" in r[1]]
    readme_v1_sha = report.notes["README.md"].revision

    # -- retry idempotency: identical replay, zero new objects -------------
    for relpath in ("README.md", "note1.md"):
        result = _admit_remap(rig, importer, relpath, reuse_ids=True)
        statuses = {a["status"] for a in result["admissions"]}
        assert statuses == {"existing"}, (relpath, result["admissions"])
    assert _object_count(rig) == count_after_import

    # -- commit 2: rename + modify + add binary ----------------------------
    _git(vault, "mv", "note1.md", "note2.md")
    (vault / "README.md").write_text("# vault\nv2 modified\n", encoding="utf-8")
    (vault / "img.png").write_bytes(b"\x89PNG-v2-new-binary")
    _commit(vault, "commit 2: rename + modify + add binary")

    # Re-import pass over the changed tree (same importer: source and
    # session identity carry over, so origin tuples stay comparable).
    errors_before = len(report.admission_errors)
    importer.import_probe_tree("_vault_root", vault)
    assert len(report.admission_errors) == errors_before

    # README modified: admitted again under its NEW content revision; the
    # v1 revision stays as immutable history.
    readme_origins = [r for r in _origin_rows(rig) if r[1] == "README.md"]
    assert len({r[2] for r in readme_origins}) == 2
    assert {r[2] for r in readme_origins} >= {readme_v1_sha}
    v2_sha = max(
        (r[2] for r in readme_origins if r[3] == "record"),
        key=lambda rev: rev != readme_v1_sha,
    )

    # Rename surfaced as delete+create: the old native ID keeps its single
    # historical admission; the new native ID is admitted fresh. No linkage
    # between them is fabricated.
    note1_origins = [r for r in _origin_rows(rig) if r[1] == "note1.md"]
    note2_origins = [r for r in _origin_rows(rig) if r[1] == "note2.md"]
    assert len(note1_origins) == 2  # artifact + blob, one revision each
    assert {r[2] for r in note1_origins} == {report.notes["note1.md"].revision}
    assert len(note2_origins) == 2
    note1_artifact = rig.archive.get_object(
        [r[4] for r in note1_origins if r[3] == "record"][0]
    )
    assert note1_artifact is not None  # history retained, nothing purged

    # The new binary was admitted; the unchanged attachment was NOT
    # re-submitted (per-file importer state, no conflict noise).
    assert "img.png" in report.attachment_blobs
    assert len(report.admission_errors) == errors_before

    # -- provenance chain of the revised README ----------------------------
    v2_artifact_id = [
        r[4] for r in readme_origins if r[2] == v2_sha and r[3] == "record"
    ][0]
    v2_obj = rig.archive.get_object(v2_artifact_id)
    v2_semantic = v2_obj["compartments"]["semantic"]["envelope"]["content"]
    assert v2_semantic["origin"]["revision"] == v2_sha
    assert v2_semantic["origin"]["source_id"] == report.sources["_vault_root"]
    with open_ccf_connection(rig.settings) as conn:
        link_row = conn.execute(
            """
            SELECT plaintext_json ->> 'to_id'
            FROM compartment
            WHERE compartment = 'structural' AND state = 'plaintext'
              AND plaintext_json ->> 'type' = 'ccf.has_blob'
              AND plaintext_json ->> 'from_id' = %s
            """,
            (v2_artifact_id,),
        ).fetchone()
        assert link_row is not None
        blob_row = conn.execute(
            "SELECT plaintext_bytes FROM blob_content WHERE blob_id = %s",
            (link_row[0],),
        ).fetchone()
    assert blob_row is not None
    assert bytes(blob_row[0]) == (vault / "README.md").read_bytes()

    # -- dishonest re-submission: old revision, altered content ------------
    # The forged artifact conflicts on the occupied origin tuple; with the
    # forged record unadmitted, its provenance links reference an unknown
    # ID and the whole batch is rejected. Nothing is silently accepted.
    result = _admit_remap(
        rig,
        importer,
        "README.md",
        reuse_ids=False,
        revision=readme_v1_sha,
        text_override="# forged v1 content\n",
    )
    assert result["status"] == "content_rejected", result
    assert "unknown ID" in result["extensions"]["reason"], result
    # The pure per-object view: the forged record alone gets the precise
    # origin_revision_conflict outcome.
    mapped = importer.remap_note(
        "README.md",
        reuse_ids=False,
        revision=readme_v1_sha,
        text_override="# forged v1 content\n",
    )
    batch = rig.producer.create_batch(records=mapped.records)
    result = rig.archive.admit_batch(batch)
    assert result["status"] == "conflict", result
    assert result["admissions"][0]["status"] == "origin_revision_conflict"
    assert result["admissions"][0]["reason"] == (
        "origin tuple already admitted with different content"
    )

    # -- commit 3: delete + branch/tag; unchanged re-scan stays honest -----
    _git(vault, "rm", "README.md")
    _commit(vault, "commit 3: delete README")
    _git(vault, "branch", "feature")
    _git(vault, "tag", "v1.0")

    artifacts_before = _object_count(rig)
    importer.import_probe_tree("_vault_root", vault)
    # The deleted README is simply absent from the scan: no admission, no
    # error, and both historical revisions remain retrievable.
    for revision in {r[2] for r in readme_origins}:
        still = [
            r for r in _origin_rows(rig)
            if r[1] == "README.md" and r[2] == revision and r[3] == "record"
        ]
        assert rig.archive.get_object(still[0][4]) is not None
    # The unchanged note re-scan neither duplicates nor overwrites: the
    # note2 artifact/blob conflict on their occupied origin tuples, which
    # leaves their provenance links referencing non-admitted IDs — the
    # whole notes batch is loudly rejected, and no new artifact lands.
    pass3_batches = [
        b for b in report.batches
        if b["purpose"] == "notes" and b["status"] == "content_rejected"
    ]
    assert pass3_batches, report.batches
    with open_ccf_connection(rig.settings) as conn:
        note_artifacts = conn.execute(
            """
            SELECT COUNT(*) FROM compartment
            WHERE compartment = 'structural' AND state = 'plaintext'
              AND plaintext_json ->> 'type' = 'experience.artifact'
            """
        ).fetchone()[0]
    # README v1 + README v2 + note2 notes, plus the asset.png and img.png
    # attachment artifacts — nothing from the rejected pass-3 batch.
    assert note_artifacts == 6
    assert _object_count(rig) < artifacts_before + 4

    # .git internals (objects, refs, the new branch/tag) never imported.
    origins = _origin_rows(rig)
    assert not [r for r in origins if ".git" in r[1]]

    # -- rebuild after projection destruction ------------------------------
    with open_ccf_connection(rig.settings) as conn:
        before_links = conn.execute(
            "SELECT link_id, state, selector_available "
            "FROM projection_link_state WHERE archive_id = %s ORDER BY 1",
            (rig.archive.archive_id,),
        ).fetchall() if _table_exists(conn, "projection_link_state") else []
    rig.archive.projections.rebuild_all()
    with open_ccf_connection(rig.settings) as conn:
        before_links = conn.execute(
            "SELECT link_id, state, selector_available "
            "FROM projection_link_state WHERE archive_id = %s ORDER BY 1",
            (rig.archive.archive_id,),
        ).fetchall()
    assert before_links
    _drop_and_reprovision(rig)
    with open_ccf_connection(rig.settings) as conn:
        after_links = conn.execute(
            "SELECT link_id, state, selector_available "
            "FROM projection_link_state WHERE archive_id = %s ORDER BY 1",
            (rig.archive.archive_id,),
        ).fetchall()
    assert after_links == before_links
    assert rig.archive.verify_chain()["commits_verified"] >= 4


def _table_exists(conn, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM pg_tables WHERE tablename = %s "
        "AND schemaname = current_schema()",
        (table,),
    ).fetchone() is not None


def _drop_and_reprovision(rig) -> None:
    """Destroy every projection table and rebuild from canonical state."""
    from ccf.projections.schema import CCF_PROJECTION_MIGRATION

    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            for table in (
                "projection_link_state",
                "projection_derivation_closure",
                "projection_entity_cluster",
                "projection_full_text",
                "projection_embedding",
                "projection_checkpoint",
                "projection_invalidation",
                "generation_fence",
            ):
                conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            for statement in CCF_PROJECTION_MIGRATION.statements:
                conn.execute(statement)
            from ccf.projections.rebuild import rebuild_all

            rebuild_all(conn, archive_id=rig.archive.archive_id)
