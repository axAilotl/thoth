"""Mindpack export/restore/merge tests (checklist phase 8, spec 11).

Covers the full round-trip (export a populated archive, restore into an
empty store, same genesis/head, byte-identical semantics), the vendored
``examples/mindpack`` directory, tamper rejection, foreign merge with
custody proofs and unchanged portable IDs, and fail-closed handling of
incomplete packs with explicit partial import.
"""

from __future__ import annotations

import json

import pytest

from ccf.archive import Archive
from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf.sync.packio import IncompletePackError, PackError
from ccf.sync.restore import RestoreError, restore_mindpack, verify_mindpack

from ccf_helpers import make_rig


@pytest.fixture()
def settings_factory(ccf_postgres_dsn):
    """Factory for extra store schemas in one test (restores, merges)."""
    import uuid

    import psycopg

    made: list[str] = []

    def _make() -> CcfPostgresSettings:
        schema = f"ccf_test_{uuid.uuid4().hex[:12]}"
        made.append(schema)
        return CcfPostgresSettings(enabled=True, dsn=ccf_postgres_dsn, schema=schema)

    yield _make
    with psycopg.connect(ccf_postgres_dsn, autocommit=True) as conn:
        for schema in made:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


def _populate(rig):
    """Admit one batch with a record pair, a link, and a blob with bytes."""
    source = rig.producer.new_record(
        type="core.source",
        claims=rig.claims(),
        payload={
            "kind": "wearable_audio",
            "name": "recorder",
            "connector": "thoth.capture",
            "native_identity": "device:recorder",
            "trust_class": "authenticated",
            "producer_key_id": rig.device_key_id,
            "extensions": {},
        },
    )
    session = rig.producer.new_record(
        type="core.session",
        claims=rig.claims(),
        origin={"source_id": source["id"], "native_id": "s-1", "revision": "1"},
        payload={
            "source_id": source["id"],
            "native_id": "s-1",
            "channel": "ambient",
            "started_at": "2026-08-12T00:00:00.000Z",
            "ended_at": "2026-08-12T00:00:01.000Z",
            "participants": [rig.person_id],
            "capture_mode": "test",
            "extensions": {},
        },
    )
    link = rig.producer.new_link(
        type="ccf.captured_in",
        from_id=session["id"],
        to_id=source["id"],
        claims=rig.claims(),
    )
    blob_sub, blob_bytes = rig.producer.new_blob(
        data=b"mindpack-blob-payload" * 100,
        media_type="application/octet-stream",
        claims=rig.claims(),
    )
    batch = rig.producer.create_batch(
        records=[source, session],
        links=[link],
        blobs=[blob_sub],
        blob_data={blob_sub["id"]: blob_bytes},
    )
    result = rig.archive.admit_batch(batch, blob_bytes={blob_sub["id"]: blob_bytes})
    assert result["status"] == "accepted"
    return {
        "source": source["id"],
        "session": session["id"],
        "link": link["id"],
        "blob": blob_sub["id"],
        "blob_bytes": blob_bytes,
    }


def test_export_restore_roundtrip(rig, settings_factory, tmp_path, ccf_package_root):
    ids = _populate(rig)
    head_before = rig.archive.head()
    pack_dir = tmp_path / "archive.mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    assert manifest["head_commit_hash"] == head_before["commit_hash"]
    assert manifest["extensions"]["completeness"]["complete"] is True

    settings_b = settings_factory()
    report = restore_mindpack(
        settings_b,
        package_root=ccf_package_root,
        pack_path=pack_dir,
        trusted_genesis_hash=manifest["genesis_commit_hash"],
        trusted_head_hash=manifest["head_commit_hash"],
    )
    assert report["status"] == "restored"
    assert report["partial"] is False
    verification = report["verification"]
    assert verification["genesis_commit_hash"] == manifest["genesis_commit_hash"]
    assert verification["head_commit_hash"] == head_before["commit_hash"]

    replica = Archive.open(
        settings_b,
        package_root=ccf_package_root,
        archive_key_path=rig.archive_key_path,
    )
    assert replica.archive_id == rig.archive.archive_id
    assert replica.head() == head_before
    replica.verify_chain(trusted_genesis_hash=manifest["genesis_commit_hash"])

    # Portable IDs are never remapped; object hashes match one for one.
    for object_id in (ids["source"], ids["session"], ids["link"], ids["blob"]):
        original = rig.archive.get_object(object_id)
        restored = replica.get_object(object_id)
        assert restored is not None
        assert restored["header"] == original["header"]
        assert restored["admission"] == original["admission"]

    with open_ccf_connection(settings_b) as conn:
        row = conn.execute(
            "SELECT plaintext_bytes FROM blob_content WHERE blob_id = %s",
            (ids["blob"],),
        ).fetchone()
    assert bytes(row[0]) == ids["blob_bytes"]


def test_restore_zip_container(rig, settings_factory, tmp_path, ccf_package_root):
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    from ccf.sync.packio import zip_pack_dir

    zip_path = zip_pack_dir(pack_dir, tmp_path / "archive.mindpack.zip")
    report = restore_mindpack(
        settings_factory(), package_root=ccf_package_root, pack_path=zip_path
    )
    assert report["head_commit_hash"] == manifest["head_commit_hash"]


def test_restore_rejects_tampered_stream(rig, settings_factory, tmp_path, ccf_package_root):
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    rig.archive.sync().export_mindpack(pack_dir)
    target = next((pack_dir / "compartments" / "records").glob("*.structural.json"))
    data = bytearray(target.read_bytes())
    data[-5] ^= 0xFF
    target.write_bytes(bytes(data))
    with pytest.raises(PackError):
        restore_mindpack(settings_factory(), package_root=ccf_package_root,
                         pack_path=pack_dir)


def test_restore_refuses_non_empty_store(rig, tmp_path, ccf_package_root):
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    rig.archive.sync().export_mindpack(pack_dir)
    with pytest.raises(RestoreError, match="not empty"):
        restore_mindpack(rig.settings, package_root=ccf_package_root, pack_path=pack_dir)


def test_restore_rejects_wrong_trusted_head(rig, settings_factory, tmp_path,
                                            ccf_package_root):
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    rig.archive.sync().export_mindpack(pack_dir)
    bogus = "sha256:" + "00" * 32
    with pytest.raises(RestoreError, match="trusted head"):
        restore_mindpack(
            settings_factory(),
            package_root=ccf_package_root,
            pack_path=pack_dir,
            trusted_head_hash=bogus,
        )


def test_vendored_example_mindpack_verifies(ccf_package_root):
    pack = verify_mindpack(
        ccf_package_root / "examples" / "mindpack", package_root=ccf_package_root
    )
    manifest = pack.manifest
    assert manifest["format"] == "ccf.mindpack/0.1.2-rc1"
    assert manifest["mode"] == "restore"
    assert int(manifest["counts"]["records"]) == 15
    assert int(manifest["counts"]["commits"]) == 3
    assert pack.chain["commits_verified"] == 3
    assert pack.chain["head_commit_hash"] == manifest["head_commit_hash"]
    assert pack.chain["head_sequence"] == manifest["head_sequence"] == "2"
    assert pack.completeness.complete, pack.completeness.to_dict()
    # Every object hash in the pack recomputes from its compartments.
    assert len(pack.objects) == 15 + 7 + 1
    assert len(pack.blob_data) == 1


def _remove_object_from_pack(pack_dir, object_id):
    """Withhold an object without declaring it (non-compliant pack)."""
    kind = object_id.split(":")[2]
    uuid_part = object_id.rsplit(":", 1)[1]
    stream = pack_dir / "objects" / f"{kind}s.ndjson"
    lines = [
        line
        for line in stream.read_text().splitlines()
        if object_id not in line
    ]
    stream.write_text("\n".join(lines) + "\n")
    manifest_path = pack_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    kept = []
    from ccf.hashing import digest_string

    new_digest = digest_string(stream.read_bytes())
    new_length = len(stream.read_bytes())
    for entry in manifest["streams"]:
        if f"{uuid_part}." in entry["path"]:
            continue
        if entry["path"] == f"objects/{kind}s.ndjson":
            entry = {**entry, "digest": new_digest, "byte_length": str(new_length)}
        kept.append(entry)
    manifest["streams"] = kept
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    for compartment_file in (pack_dir / "compartments" / f"{kind}s").glob(
        f"{uuid_part}.*.json"
    ):
        compartment_file.unlink()


def test_incomplete_pack_fails_closed_then_partial_merge(
    rig, settings_factory, tmp_path, ccf_package_root
):
    b_dir = tmp_path / "b"
    b_dir.mkdir()
    rig_b = make_rig(settings_factory(), b_dir, ccf_package_root)
    ids = _populate(rig_b)
    pack_dir = tmp_path / "b.mindpack"
    rig_b.archive.sync().export_mindpack(pack_dir)
    _remove_object_from_pack(pack_dir, ids["source"])

    sync = rig.archive.sync()
    with pytest.raises(IncompletePackError) as excinfo:
        sync.import_mindpack(pack_dir)
    assert ids["source"] in str(excinfo.value)

    report = sync.import_mindpack(pack_dir, allow_partial=True)
    assert report["status"] == "merged"
    assert report["partial"] is True
    assert ids["source"] in report["completeness"]["dangling"]
    assert ids["source"] in report["not_included"]
    assert ids["source"] not in report["admitted"]
    # Referencing objects still merged; the destination chain verifies.
    assert ids["session"] in report["admitted"]
    rig.archive.verify_chain()


def test_foreign_merge_preserves_ids_and_custody(
    rig, settings_factory, tmp_path, ccf_package_root
):
    b_dir = tmp_path / "b"
    b_dir.mkdir()
    rig_b = make_rig(settings_factory(), b_dir, ccf_package_root)
    ids = _populate(rig_b)
    head_b = rig_b.archive.head()
    pack_dir = tmp_path / "b.mindpack"
    rig_b.archive.sync().export_mindpack(pack_dir)

    head_a_before = rig.archive.head()
    report = rig.archive.sync().import_mindpack(pack_dir)
    assert report["status"] == "merged"
    assert report["partial"] is False
    assert report["source_archive_id"] == rig_b.archive.archive_id
    assert report["custody_proof"] == (
        f"{rig_b.archive.archive_id}:{head_b['commit_hash']}"
    )
    # One new destination commit over all imported objects.
    assert int(report["commit_sequence"]) == int(head_a_before["sequence"]) + 1

    # Portable IDs and object hashes are unchanged (never remapped).
    for object_id in (ids["source"], ids["session"], ids["link"], ids["blob"]):
        merged = rig.archive.get_object(object_id)
        assert merged is not None
        assert merged["header"]["object_hash"] == (
            rig_b.archive.get_object(object_id)["header"]["object_hash"]
        )
        # Destination admission coordinates, not source coordinates.
        assert merged["admission"]["commit_sequence"] == report["commit_sequence"]

    # Custody proof preserved and listed.
    forks = rig.archive.sync().forks()
    assert [f["head_commit_hash"] for f in forks] == [head_b["commit_hash"]]
    assert forks[0]["source_archive_id"] == rig_b.archive.archive_id

    # Source commit Records are ordinary destination members.
    with open_ccf_connection(rig.settings) as conn:
        member_records = conn.execute(
            """
            SELECT COUNT(*) FROM commit_member
            WHERE archive_id = %s AND commit_sequence = %s
            """,
            (rig.archive.archive_id, int(report["commit_sequence"])),
        ).fetchone()[0]
    assert int(member_records) == len(report["admitted"])

    rig.archive.verify_chain()

    # Re-import is idempotent: all objects already present.
    again = rig.archive.sync().import_mindpack(pack_dir)
    assert again["status"] == "merged"
    assert again["admitted"] == []
    assert sorted(again["skipped_existing"]) == sorted(report["admitted"])


def test_foreign_merge_records_erased_compartments(
    settings_factory, tmp_path, ccf_package_root
):
    """Stage 9 regression: merging a pack with an erased object must record
    the unavailable compartment, not drop the row (destination headers keep
    the compartment commitment; a missing row breaks chain verification).
    """
    from dataclasses import replace

    from ccf.erasure.suppression import generate_suppression_key
    from ccf_helpers import authority

    key_path = generate_suppression_key(tmp_path / "suppression.key")
    rig_a_dir = tmp_path / "a"
    rig_a_dir.mkdir()
    rig_a = make_rig(
        replace(settings_factory(), suppression_key_path=str(key_path)),
        rig_a_dir,
        ccf_package_root,
    )
    ids = _populate(rig_a)
    svc = rig_a.archive.erasure()
    targets = [{"object_id": ids["session"], "compartments": ["semantic"]}]
    request = svc.submit_request(
        requester_id=rig_a.person_id,
        subject_id=rig_a.person_id,
        requested_scope={"targets": targets},
        reason="merge regression erasure",
        authority=authority("first_person_statement", rig_a.person_id, rig_a.person_id),
    )
    decided = svc.decide(
        request_id=request["request_id"],
        decision="approve",
        targets=targets,
        reasoning="approved",
        decided_by=rig_a.person_id,
        authority=authority("explicit_authorization", rig_a.person_id, rig_a.person_id),
        authorized_producers=[rig_a.producer.producer_id],
    )
    status = svc.execute(decided["operation_id"])
    assert status["stage"] == "receipt"

    pack_dir = tmp_path / "erased.mindpack"
    manifest = rig_a.archive.sync().export_mindpack(pack_dir)
    assert ids["session"] in manifest["erased"]

    rig_b_dir = tmp_path / "b"
    rig_b_dir.mkdir()
    rig_b = make_rig(settings_factory(), rig_b_dir, ccf_package_root)
    report = rig_b.archive.sync().import_mindpack(pack_dir)
    assert report["status"] == "merged"

    merged = rig_b.archive.get_object(ids["session"])
    assert merged is not None
    semantic = merged["compartments"]["semantic"]
    # 0.1.2-rc1: the exact source availability state is preserved.
    assert semantic["state"] == "erased"
    assert semantic["envelope"] is None  # no fabricated content
    rig_b.archive.verify_chain()
