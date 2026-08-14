"""Mindpack export/restore/merge tests (checklist phase 8, spec 11).

Covers the full round-trip (export a populated archive, restore into an
empty store, same genesis/head, byte-identical semantics), the vendored
``examples/mindpack`` directory, tamper rejection, foreign merge with
custody proofs and unchanged portable IDs, and fail-closed handling of
incomplete packs with explicit partial import.
"""

from __future__ import annotations

import copy
import json

import pytest

from ccf.archive import Archive
from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf.sync.packio import PackError
from ccf.sync.restore import RestoreError, restore_mindpack, verify_mindpack
from ccf.sync.verify import PackVerificationError

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
    assert manifest["custody"] == {
        "completeness": "complete",
        "restore_capable": True,
    }

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


def test_export_represents_withheld_partial_custody(
    rig, tmp_path, ccf_package_root
):
    """Withholding is portable without fabricating a lineage Record."""
    ids = _populate(rig)
    with open_ccf_connection(rig.settings) as conn:
        conn.execute(
            """
            UPDATE compartment
            SET state = 'withheld', salt = NULL, plaintext_json = NULL,
                ciphertext = NULL, ciphertext_digest = NULL, storage_ref = NULL
            WHERE object_id = %s AND compartment = 'semantic'
            """,
            (ids["session"],),
        )
    pack_dir = tmp_path / "withheld.mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    assert manifest["custody"] == {
        "completeness": "partial",
        "restore_capable": False,
    }
    assert manifest["withheld"] == [ids["session"]]
    entry = next(
        row
        for row in manifest["compartment_availability"]
        if row["object_id"] == ids["session"]
        and row["compartment"] == "semantic"
    )
    assert entry["availability"] == "withheld"
    assert entry["source_custody_proof"].startswith("commit:")
    assert entry["unavailability_lineage_id"] is None

    verified = verify_mindpack(
        pack_dir,
        package_root=ccf_package_root,
        allow_partial=True,
        operation="import",
    )
    assert verified.partial is True
    assert verified.inventory.withheld == {ids["session"]}


def test_restore_preserves_content_rejected_producer_predecessor(
    rig, settings_factory, tmp_path, ccf_package_root
):
    """A valid rejected batch remains in the restored producer chain."""
    from ccf.ids import generate_id

    ghost_link = rig.producer.new_link(
        type="ccf.about",
        from_id=generate_id("record"),
        to_id=generate_id("record"),
        claims=rig.claims(),
        selector={},
    )
    rejected_batch = rig.producer.create_batch(links=[ghost_link])
    rejected = rig.archive.admit_batch(rejected_batch)
    assert rejected["status"] == "content_rejected"
    _populate(rig)
    with open_ccf_connection(rig.settings) as conn:
        expected_head_hash = conn.execute(
            "SELECT batch_hash FROM producer_head WHERE producer_id = %s",
            (rig.producer.producer_id,),
        ).fetchone()[0]
    pack_dir = tmp_path / "producer-continuity.mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    settings_b = settings_factory()
    restore_mindpack(
        settings_b,
        package_root=ccf_package_root,
        pack_path=pack_dir,
        trusted_genesis_hash=manifest["genesis_commit_hash"],
    )
    with open_ccf_connection(settings_b) as conn:
        rows = conn.execute(
            """
            SELECT producer_sequence, status FROM producer_batch
            WHERE producer_id = %s ORDER BY producer_sequence
            """,
            (rig.producer.producer_id,),
        ).fetchall()
        head = conn.execute(
            """
            SELECT producer_sequence, batch_hash FROM producer_head
            WHERE producer_id = %s
            """,
            (rig.producer.producer_id,),
        ).fetchone()
    assert [(int(row[0]), row[1]) for row in rows] == [
        (1, "verifying"),
        (2, "verifying"),
    ]
    assert int(head[0]) == 2
    assert head[1] == expected_head_hash
    replica = Archive.open(
        settings_b,
        package_root=ccf_package_root,
        archive_key_path=rig.archive_key_path,
    )
    replay = replica.admit_batch(rejected_batch)
    assert replay["status"] == "content_rejected"


def test_restore_zip_container(rig, settings_factory, tmp_path, ccf_package_root):
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    from ccf.sync.packio import zip_pack_dir

    zip_path = zip_pack_dir(pack_dir, tmp_path / "archive.mindpack.zip")
    report = restore_mindpack(
        settings_factory(),
        package_root=ccf_package_root,
        pack_path=zip_path,
        trusted_genesis_hash=manifest["genesis_commit_hash"],
    )
    assert report["head_commit_hash"] == manifest["head_commit_hash"]


def test_restore_rejects_tampered_stream(rig, settings_factory, tmp_path, ccf_package_root):
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    target = next((pack_dir / "compartments" / "records").glob("*.structural.json"))
    data = bytearray(target.read_bytes())
    data[-5] ^= 0xFF
    target.write_bytes(bytes(data))
    with pytest.raises(PackError):
        restore_mindpack(settings_factory(), package_root=ccf_package_root,
                         pack_path=pack_dir,
                         trusted_genesis_hash=manifest["genesis_commit_hash"])


def test_restore_refuses_non_empty_store(rig, tmp_path, ccf_package_root):
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    with pytest.raises(RestoreError, match="not empty"):
        restore_mindpack(rig.settings, package_root=ccf_package_root, pack_path=pack_dir,
                         trusted_genesis_hash=manifest["genesis_commit_hash"])


def test_restore_rejects_wrong_trusted_head(rig, settings_factory, tmp_path,
                                            ccf_package_root):
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    bogus = "sha256:" + "00" * 32
    with pytest.raises(RestoreError, match="trusted head"):
        restore_mindpack(
            settings_factory(),
            package_root=ccf_package_root,
            pack_path=pack_dir,
            trusted_genesis_hash=manifest["genesis_commit_hash"],
            trusted_head_hash=bogus,
        )


def test_vendored_example_mindpack_verifies(ccf_package_root):
    pack = verify_mindpack(
        ccf_package_root / "examples" / "mindpack", package_root=ccf_package_root
    )
    manifest = pack.manifest
    assert manifest["format"] == "ccf.mindpack/0.1.2"
    assert manifest["mode"] == "restore"
    assert manifest["custody"] == {
        "completeness": "complete",
        "restore_capable": True,
    }
    assert int(manifest["counts"]["records"]) == 17
    assert int(manifest["counts"]["commits"]) == 3
    assert pack.chain["commits_verified"] == 3
    assert pack.chain["head_commit_hash"] == manifest["head_commit_hash"]
    assert pack.chain["head_sequence"] == manifest["head_sequence"] == "2"
    assert pack.completeness.complete, pack.completeness.to_dict()
    # Every object hash in the pack recomputes from its compartments.
    assert len(pack.objects) == 17 + 8 + 1
    assert len(pack.blob_data) == 1
    assert any(
        row["state"] == "revoke" for row in pack.derived_lineage_heads
    ), "example must exercise an issue-to-revoke credential lineage"
    assert len(pack.verified_batches) == 1


def test_vendored_example_rejects_batch_at_credential_revocation(
    ccf_package_root,
):
    from ccf.hashing import (
        encode_b64url,
        producer_batch_hash,
        producer_batch_signing_digest,
        sign_digest,
    )
    from ccf.schemas import SchemaSet
    from ccf.sync.operational import verify_producer_state
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    pack = verify_mindpack(
        ccf_package_root / "examples" / "mindpack", package_root=ccf_package_root
    )
    batch = copy.deepcopy(pack.batch_claims[0])
    batch["created_at"] = "2026-08-11T21:42:20.800Z"
    batch.pop("batch_hash")
    batch.pop("signature")
    batch["batch_hash"] = producer_batch_hash(batch)
    private_key = load_pem_private_key(
        (
            ccf_package_root
            / "vectors"
            / "TEST-ONLY-device-ed25519-private.pem"
        ).read_bytes(),
        password=None,
    )
    batch["signature"] = encode_b64url(
        sign_digest(
            private_key,
            producer_batch_signing_digest(batch["batch_hash"]),
        )
    )
    with pytest.raises(
        PackVerificationError, match="credential is not valid at batch creation time"
    ):
        verify_producer_state(
            pack.objects,
            pack.members,
            [batch],
            [],
            catalog_root=pack.chain["semantic_catalog_root"],
            schemas=SchemaSet.load(ccf_package_root),
        )


def _remove_object_from_pack(pack_dir, object_id, *, declare_partial=False):
    """Remove an object, optionally making the manifest truthfully partial."""
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
    if declare_partial:
        count_key = f"{kind}s"
        manifest["counts"][count_key] = str(
            int(manifest["counts"][count_key]) - 1
        )
        manifest["compartment_availability"] = [
            entry
            for entry in manifest["compartment_availability"]
            if entry["object_id"] != object_id
        ]
        manifest["external_dependencies"].append(
            {"object_id": object_id, "reason": "unresolved_reference"}
        )
        manifest["custody"] = {
            "completeness": "partial",
            "restore_capable": False,
        }
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
    _remove_object_from_pack(pack_dir, ids["source"], declare_partial=True)

    sync = rig.archive.sync()
    with pytest.raises(PackVerificationError) as excinfo:
        sync.import_mindpack(pack_dir)
    assert excinfo.value.reason == "manifest_completeness_mismatch"

    report = sync.import_mindpack(pack_dir, allow_partial=True)
    assert report["status"] == "merged"
    assert report["partial"] is True
    assert ids["source"] in report["completeness"]["external"]
    assert report["completeness"]["dangling"] == []
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

    exported = tmp_path / "merged.mindpack"
    manifest = rig.archive.sync().export_mindpack(exported)
    verified = verify_mindpack(
        exported, package_root=ccf_package_root, operation="restore"
    )
    expected_prefix = f"{rig_b.archive.archive_id}:sha256:"
    assert manifest["foreign_custody_proofs"]
    assert all(
        proof.startswith(expected_prefix)
        for proof in manifest["foreign_custody_proofs"]
    )
    assert (
        set(manifest["foreign_custody_proofs"])
        == verified.inventory.foreign_custody_proofs
    )

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
    # 0.1.2: the exact source availability state is preserved.
    assert semantic["state"] == "erased"
    assert semantic["envelope"] is None  # no fabricated content
    rig_b.archive.verify_chain()


# ---------------------------------------------------------------------------
# Security review regression tests (ccf-0.1.2)
# ---------------------------------------------------------------------------


def test_restore_requires_identity_anchor(rig, settings_factory, tmp_path,
                                          ccf_package_root):
    """H1: without an out-of-band anchor, restore fails closed."""
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    rig.archive.sync().export_mindpack(pack_dir)
    with pytest.raises(RestoreError, match="trusted genesis hash"):
        restore_mindpack(
            settings_factory(), package_root=ccf_package_root, pack_path=pack_dir
        )


def test_restore_bootstrap_new_archive(rig, settings_factory, tmp_path,
                                       ccf_package_root):
    """H1: explicit bootstrap opt-in restores and surfaces the genesis hash."""
    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    report = restore_mindpack(
        settings_factory(),
        package_root=ccf_package_root,
        pack_path=pack_dir,
        bootstrap_new_archive=True,
    )
    assert report["status"] == "restored"
    assert report["genesis_commit_hash"] == manifest["genesis_commit_hash"]


def test_operational_reread_detects_post_verification_swap(
    rig, tmp_path, ccf_package_root
):
    """M1: an operational stream swapped after verification fails the digest re-check."""
    from ccf.sync.restore import _reread_operational_streams

    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    rig.archive.sync().export_mindpack(pack_dir)
    pack = verify_mindpack(pack_dir, package_root=ccf_package_root)
    target = pack_dir / "origin-index.ndjson"
    data = bytearray(target.read_bytes())
    data[-1] ^= 0xFF  # length-preserving tamper: only the digest re-check catches it
    target.write_bytes(bytes(data))
    with pytest.raises(RestoreError, match="changed since verification"):
        _reread_operational_streams(pack_dir, pack)


def test_pack_reader_enforces_entry_size_cap(tmp_path):
    """M2: a ZIP entry beyond the cap fails before it is fully decompressed."""
    import zipfile

    from ccf.sync.packio import PackReader

    zip_path = tmp_path / "bomb.mindpack"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("big.bin", b"A" * 4096)
    with PackReader(zip_path, max_entry_bytes=1024) as reader:
        with pytest.raises(PackError, match="limit"):
            reader.read("big.bin")


def test_pack_reader_enforces_total_size_cap(tmp_path):
    """M2: cumulative uncompressed reads across entries are capped too."""
    from ccf.sync.packio import PackReader

    pack_dir = tmp_path / "pack"
    pack_dir.mkdir()
    (pack_dir / "a.bin").write_bytes(b"a" * 64)
    (pack_dir / "b.bin").write_bytes(b"b" * 64)
    with PackReader(pack_dir, max_total_bytes=96) as reader:
        reader.read("a.bin")
        with pytest.raises(PackError, match="total size limit"):
            reader.read("b.bin")


def test_verify_stream_digests_rejects_inflated_stream(tmp_path):
    """M2: a stream longer than its manifest byte_length fails at the declaration."""
    from ccf.hashing import digest_string
    from ccf.sync.packio import PackReader, StreamEntry, verify_stream_digests

    pack_dir = tmp_path / "pack"
    pack_dir.mkdir()
    data = b"x" * 100
    (pack_dir / "data.bin").write_bytes(data)
    entry = StreamEntry(
        path="data.bin", digest=digest_string(data), byte_length=10
    )
    with PackReader(pack_dir) as reader:
        with pytest.raises(PackError, match="size limit"):
            verify_stream_digests(reader, [entry])


def test_pack_writer_outputs_owner_only(tmp_path):
    """L2: exported pack trees and zip containers are owner-only."""
    import stat

    from ccf.sync.packio import PackWriter, zip_pack_dir

    root = tmp_path / "pack"
    writer = PackWriter(root)
    writer.write_json("manifest.json", {"a": 1})
    writer.write_bytes("blob-data/x.bin", b"data")
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "manifest.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((root / "blob-data").stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "blob-data" / "x.bin").stat().st_mode) == 0o600
    out = zip_pack_dir(root, tmp_path / "pack.zip")
    assert stat.S_IMODE(out.stat().st_mode) == 0o600


def test_verify_commit_chain_wraps_malformed_members(rig, tmp_path, ccf_package_root):
    """L4: malformed member fields surface as PackVerificationError, not KeyError."""
    from ccf.sync.verify import PackVerificationError, verify_commit_chain

    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    rig.archive.sync().export_mindpack(pack_dir)
    pack = verify_mindpack(pack_dir, package_root=ccf_package_root)
    bad_members = [dict(m) for m in pack.members]
    bad_members[0].pop("commit_position")
    with pytest.raises(PackVerificationError, match="malformed"):
        verify_commit_chain(pack.commits, bad_members, pack.objects)


def test_verify_commit_chain_rejects_orphan_member_sequence(
    rig, tmp_path, ccf_package_root
):
    """An unsigned member row cannot create journal coverage."""
    from ccf.sync.verify import verify_commit_chain

    _populate(rig)
    pack_dir = tmp_path / "archive.mindpack"
    rig.archive.sync().export_mindpack(pack_dir)
    pack = verify_mindpack(pack_dir, package_root=ccf_package_root)
    bad_members = [dict(member) for member in pack.members]
    orphan = dict(bad_members[0])
    orphan["commit_sequence"] = "999"
    orphan["object_id"] = "urn:ccf:record:00000000-0000-4000-8000-000000000099"
    bad_members.append(orphan)
    with pytest.raises(PackVerificationError, match="absent commit sequence"):
        verify_commit_chain(pack.commits, bad_members, pack.objects)
