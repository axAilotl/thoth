"""Obsidian cross-pass re-import identity (thoth-doz).

Two FRESH importer instances pointed at the same vault must derive the
same stable source/native identity: source Records keep deterministic
URNs, unchanged notes and attachments are recognized through the origin
index, and the second pass commits only its own session/run. Distinct
vaults must never collide, and a changed file must re-admit under its
new content revision.

Ephemeral Postgres per ``tests/conftest.py``; skipped cleanly without
docker.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ccf.db import open_ccf_connection
from ccf.obsidian.importer import ObsidianImporter
from ccf.thothmap import MapContext

from ccf_helpers import make_rig


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


def _vault(root: Path) -> Path:
    vault = root / "vault"
    (vault / "notes").mkdir(parents=True)
    (vault / "assets").mkdir(parents=True)
    (vault / "notes" / "a.md").write_text(
        "---\ntitle: Alpha\n---\nAlpha links to [[b]].\n", encoding="utf-8"
    )
    (vault / "notes" / "b.md").write_text(
        "---\ntitle: Beta\n---\nBeta embeds ![[clip.png]].\n", encoding="utf-8"
    )
    (vault / "assets" / "clip.png").write_bytes(b"\x89PNG-fixture-bytes")
    return vault


def _importer(rig, vault: Path, run_tag: str) -> ObsidianImporter:
    ctx = MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)
    return ObsidianImporter(
        producer=rig.producer,
        archive=rig.archive,
        ctx=ctx,
        vault_root=vault,
        run_tag=run_tag,
    )


def _origin_rows(rig) -> list[tuple]:
    with open_ccf_connection(rig.settings) as conn:
        return conn.execute(
            "SELECT source_id, native_id, revision, object_kind, object_id "
            "FROM origin_index WHERE archive_id = %s ORDER BY 1, 2, 3, 4",
            (rig.archive.archive_id,),
        ).fetchall()


def test_fresh_instance_reimport_is_idempotent(rig, tmp_path):
    vault = _vault(tmp_path)

    first = _importer(rig, vault, "pass-1").import_vault()
    assert not first.admission_errors, first.admission_errors[:2]
    assert first.objects_committed > 2  # sources + session/run + notes + links
    origins_after_first = _origin_rows(rig)

    second_report = _importer(rig, vault, "pass-2").import_vault()
    assert not second_report.admission_errors, second_report.admission_errors[:2]

    # Stable source identity across instances.
    assert second_report.sources == first.sources

    # Every note and attachment was reused from the first pass.
    assert second_report.notes.keys() == first.notes.keys()
    for relpath, record in second_report.notes.items():
        assert record.existing, relpath
        assert record.artifact_id == first.notes[relpath].artifact_id
        assert record.blob_id == first.notes[relpath].blob_id
    assert second_report.attachment_blobs == first.attachment_blobs

    # The second pass committed only its own session and run, and claimed
    # no new origin tuples beyond theirs.
    assert second_report.objects_committed == 2
    assert len(_origin_rows(rig)) == len(origins_after_first) + 2

    # No duplicate edges from the re-import.
    assert second_report.wikilink_edges == []
    assert second_report.attachment_links == []


def test_distinct_vaults_never_collide(rig, tmp_path):
    vault_one = _vault(tmp_path / "one")
    vault_two = _vault(tmp_path / "two")

    first = _importer(rig, vault_one, "vault-1").import_vault()
    second = _importer(rig, vault_two, "vault-2").import_vault()
    assert not first.admission_errors, first.admission_errors[:2]
    assert not second.admission_errors, second.admission_errors[:2]

    # Same layout, different roots: no shared source URNs, and the second
    # vault's identical content is a distinct origin, not a reuse.
    assert set(first.sources.values()).isdisjoint(second.sources.values())
    assert second.objects_committed == first.objects_committed
    for relpath, record in second.notes.items():
        assert not record.existing, relpath


def test_changed_note_reimports_under_new_revision(rig, tmp_path):
    vault = _vault(tmp_path)
    first = _importer(rig, vault, "pass-1").import_vault()
    assert not first.admission_errors, first.admission_errors[:2]

    target = vault / "notes" / "b.md"
    target.write_text(
        "---\ntitle: Beta\n---\nBeta revised, no embed.\n", encoding="utf-8"
    )
    second = _importer(rig, vault, "pass-2").import_vault()
    assert not second.admission_errors, second.admission_errors[:2]

    # The unchanged note and attachment were reused; the changed note was
    # re-admitted (artifact + blob + has_blob + captured_in) alongside the
    # pass's session and run.
    assert second.notes["notes/a.md"].existing
    assert second.notes["notes/a.md"].artifact_id == first.notes["notes/a.md"].artifact_id
    changed = second.notes["notes/b.md"]
    assert not changed.existing
    assert changed.artifact_id != first.notes["notes/b.md"].artifact_id
    assert changed.revision != first.notes["notes/b.md"].revision
    assert second.attachment_blobs == first.attachment_blobs
    assert second.objects_committed == 6
