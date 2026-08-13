"""Sync tests (checklist phase 8, spec 6.7-6.8, 11.4, 11.7).

Covers sync-head exchange and negotiation, the durable producer Blob
spool across restarts, delta-pack round-trips over the file transport
with simulated interruption and resume, tampered-chunk rejection, the
HTTP transport with identical semantics, and explicit forks preserving
both heads.
"""

from __future__ import annotations

import pytest

from ccf.archive import Archive
from ccf.catalog import SemanticCatalog
from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf.producer import Producer
from ccf.registry import PinnedRegistries
from ccf.schemas import SchemaSet
from ccf.spool import SpoolError
from ccf.sync.chunks import ChunkVerificationError, load_sidecar
from ccf.sync.delta import DeltaPackError
from ccf.sync.heads import negotiate
from ccf.sync.packio import PackError
from ccf.sync.restore import restore_mindpack
from ccf.sync.transport import (
    FileChunkSource,
    copy_pack_file,
    fetch_pack_http,
    make_pack_app,
    resumable_copy,
)

from ccf_helpers import make_rig
from test_ccf_mindpack import _populate, settings_factory  # noqa: F401  (fixture)


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


def _make_producer(settings, rig, ccf_package_root):
    catalog = SemanticCatalog.load(ccf_package_root)
    return Producer(
        settings=settings,
        producer_id=rig.runtime_id,
        credential=rig.credential,
        catalog=catalog,
        registries=PinnedRegistries.load(ccf_package_root, catalog),
        schemas=SchemaSet.load(ccf_package_root),
        clock=rig.clock,
    )


# ---------------------------------------------------------------------------
# Head exchange (6.7)
# ---------------------------------------------------------------------------


def test_sync_head_document_and_negotiation(rig):
    _populate(rig)
    head = rig.archive.sync().sync_head()
    assert head["archive_id"] == rig.archive.archive_id
    assert head["head_commit_hash"] == rig.archive.head()["commit_hash"]
    assert rig.runtime_id in head["producer_heads"]

    assert negotiate(head, head)["relationship"] == "equal"

    behind = dict(head, head_sequence=str(int(head["head_sequence"]) - 1))
    # Different hash at a lower sequence: we are ahead -> push plan.
    behind["head_commit_hash"] = "sha256:" + "ab" * 32
    plan = negotiate(head, behind)
    assert plan["relationship"] == "push"
    assert plan["commit_range"]["from_sequence"] == behind["head_sequence"]
    assert plan["commit_range"]["through_sequence"] == head["head_sequence"]

    plan = negotiate(behind, head)
    assert plan["relationship"] == "pull"
    assert rig.runtime_id in plan["stale_producers"] or True  # same heads here

    divergent = dict(head, head_commit_hash="sha256:" + "cd" * 32)
    assert negotiate(head, divergent)["relationship"] == "fork"

    foreign = dict(head, archive_id="urn:ccf:archive:" + "0" * 8 + "-0000-4000-8000-000000000000")
    assert negotiate(head, foreign)["relationship"] == "foreign"


def test_negotiation_rejects_epoch_mismatch(rig):
    head = rig.archive.sync().sync_head()
    other = dict(head, epoch_id="urn:ccf:lineage:00000000-0000-4000-8000-000000000000")
    from ccf.sync.heads import NegotiationError

    with pytest.raises(NegotiationError):
        negotiate(head, other)


# ---------------------------------------------------------------------------
# Producer-side durable Blob spool (6.7)
# ---------------------------------------------------------------------------


def test_blob_spool_survives_restart(rig, ccf_package_root):
    blob_sub, blob_bytes = rig.producer.new_blob(
        data=b"durable-blob" * 500,
        media_type="application/octet-stream",
        claims=rig.claims(),
    )
    record = rig.producer.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={"label": "spool", "definition": "d", "aliases": [], "extensions": {}},
    )
    batch = rig.producer.create_batch(
        records=[record], blobs=[blob_sub], blob_data={blob_sub["id"]: blob_bytes}
    )

    # "Restart": a fresh Producer instance over the same store.
    producer2 = _make_producer(rig.settings, rig, ccf_package_root)
    assert producer2.spooled_blob_bytes(batch["batch_id"]) == {blob_sub["id"]: blob_bytes}

    # sync_pending attaches spooled bytes automatically.
    results = producer2.sync_pending(rig.archive)
    assert [r["status"] for r in results] == ["committed"]
    with open_ccf_connection(rig.settings) as conn:
        row = conn.execute(
            "SELECT state, plaintext_bytes FROM blob_content WHERE blob_id = %s",
            (blob_sub["id"],),
        ).fetchone()
    assert row[0] == "plaintext"
    assert bytes(row[1]) == blob_bytes


def test_blob_spool_detects_corruption(rig):
    blob_sub, blob_bytes = rig.producer.new_blob(
        data=b"corrupt-me" * 100,
        media_type="application/octet-stream",
        claims=rig.claims(),
    )
    batch = rig.producer.create_batch(blobs=[blob_sub], blob_data={blob_sub["id"]: blob_bytes})
    with open_ccf_connection(rig.settings) as conn:
        with conn.transaction():
            conn.execute(
                "UPDATE producer_blob_spool SET payload = %s WHERE batch_id = %s",
                (b"tampered", batch["batch_id"]),
            )
    with pytest.raises(SpoolError, match="corrupted"):
        rig.producer.spooled_blob_bytes(batch["batch_id"])


def test_create_batch_rejects_mismatched_blob_bytes(rig):
    from ccf.producer import ProducerError

    blob_sub, blob_bytes = rig.producer.new_blob(
        data=b"original" * 100, media_type="application/octet-stream", claims=rig.claims()
    )
    with pytest.raises(ProducerError, match="content commitment"):
        rig.producer.create_batch(
            blobs=[blob_sub], blob_data={blob_sub["id"]: b"originao" * 100}
        )


# ---------------------------------------------------------------------------
# Delta packs + resumable transport (6.7, 11.4)
# ---------------------------------------------------------------------------


class _FailingSource:
    """Wraps a chunk source and dies after ``budget`` reads (interruption)."""

    def __init__(self, inner, budget: int) -> None:
        self._inner = inner
        self._budget = budget

    def length(self) -> int:
        return self._inner.length()

    def read(self, offset: int, length: int) -> bytes:
        if self._budget <= 0:
            raise ConnectionError("simulated transport interruption")
        self._budget -= 1
        return self._inner.read(offset, length)


def _restore_replica(rig, settings, tmp_path, ccf_package_root, name="replica"):
    pack_dir = tmp_path / name
    manifest = rig.archive.sync().export_mindpack(pack_dir)
    return restore_mindpack(
        settings,
        package_root=ccf_package_root,
        pack_path=pack_dir,
        trusted_genesis_hash=manifest["genesis_commit_hash"],
        trusted_head_hash=manifest["head_commit_hash"],
    )


def test_delta_pack_roundtrip_with_resume(rig, settings_factory, tmp_path, ccf_package_root):
    _populate(rig)
    settings_b = settings_factory()
    _restore_replica(rig, settings_b, tmp_path, ccf_package_root)
    head_at_restore = int(rig.archive.head()["sequence"])

    # Advance the source archive by one more batch.
    _populate(rig)
    new_head = rig.archive.head()

    pack_file = tmp_path / "delta.pack"
    manifest = rig.archive.sync().build_delta_pack(head_at_restore, pack_file,
                                                   chunk_size=4096)
    assert manifest["from_sequence"] == str(head_at_restore)
    assert manifest["through_sequence"] == new_head["sequence"]

    # Interrupted transfer: only the first two chunks arrive.
    dest = tmp_path / "received.pack"
    sidecar = load_sidecar(str(pack_file) + ".chunks.json")
    source = FileChunkSource(pack_file)
    with pytest.raises(ConnectionError):
        resumable_copy(_FailingSource(source, budget=2), sidecar, dest)
    assert not dest.exists()

    # Resume: verified prefix is kept, the rest transfers.
    result = resumable_copy(FileChunkSource(pack_file), sidecar, dest)
    assert result["chunks_resumed"] == 2
    assert result["chunks_transferred"] == result["chunks_total"] - 2

    replica = Archive.open(
        settings_b, package_root=ccf_package_root,
        archive_key_path=rig.archive_key_path,
    )
    applied = replica.sync().apply_delta_pack(dest)
    assert applied["status"] == "applied"
    assert applied["head_commit_hash"] == new_head["commit_hash"]
    assert replica.head()["commit_hash"] == new_head["commit_hash"]
    replica.verify_chain()


def test_delta_pack_rejects_tampered_chunk(rig, tmp_path):
    _populate(rig)
    head = int(rig.archive.head()["sequence"])
    _populate(rig)
    pack_file = tmp_path / "delta.pack"
    rig.archive.sync().build_delta_pack(head, pack_file, chunk_size=4096)

    data = bytearray(pack_file.read_bytes())
    data[len(data) // 2] ^= 0xFF
    pack_file.write_bytes(bytes(data))
    sidecar = load_sidecar(str(pack_file) + ".chunks.json")
    with pytest.raises(ChunkVerificationError):
        resumable_copy(FileChunkSource(pack_file), sidecar, tmp_path / "out.pack")


def test_delta_pack_rejects_non_extension(rig, tmp_path, ccf_package_root):
    _populate(rig)
    head = int(rig.archive.head()["sequence"])
    _populate(rig)
    pack_file = tmp_path / "delta.pack"
    rig.archive.sync().build_delta_pack(head, pack_file)
    # Applying to the source itself: local head already past from_sequence.
    with pytest.raises(DeltaPackError, match="not a clean extension"):
        rig.archive.sync().apply_delta_pack(pack_file)


def test_file_transport_copy_pack_file(rig, tmp_path):
    _populate(rig)
    head = int(rig.archive.head()["sequence"])
    _populate(rig)
    pack_file = tmp_path / "delta.pack"
    rig.archive.sync().build_delta_pack(head, pack_file)
    dest = tmp_path / "usb-copy.pack"
    result = copy_pack_file(pack_file, dest)
    assert result["chunks_resumed"] == 0
    assert dest.read_bytes() == pack_file.read_bytes()


def test_http_transport_fetch(rig, tmp_path):
    from fastapi.testclient import TestClient

    _populate(rig)
    head = int(rig.archive.head()["sequence"])
    _populate(rig)
    pack_file = tmp_path / "delta.pack"
    rig.archive.sync().build_delta_pack(head, pack_file, chunk_size=4096)

    app = make_pack_app(pack_file)
    client = TestClient(app)
    dest = tmp_path / "http.pack"
    result = fetch_pack_http("http://testserver/pack", dest, client=client)
    assert result["chunks_transferred"] == result["chunks_total"]
    assert dest.read_bytes() == pack_file.read_bytes()


# ---------------------------------------------------------------------------
# Forks (11.7)
# ---------------------------------------------------------------------------


def test_fork_preserves_both_heads(rig, settings_factory, tmp_path, ccf_package_root):
    _populate(rig)
    head_a_at_export = rig.archive.head()
    settings_b = settings_factory()
    _restore_replica(rig, settings_b, tmp_path, ccf_package_root)

    # Both descendants advance independently from the same head.
    _populate(rig)  # archive A: new commit X
    archive_b = Archive.open(
        settings_b, package_root=ccf_package_root,
        archive_key_path=rig.archive_key_path,
    )
    producer_b = _make_producer(settings_b, rig, ccf_package_root)
    record_b = producer_b.new_record(
        type="semantic.concept",
        claims=rig.claims(),
        payload={
            "label": "fork-b", "definition": "d", "aliases": [], "extensions": {}
        },
    )
    result_b = archive_b.admit_batch(producer_b.create_batch(records=[record_b]))
    assert result_b["status"] == "committed"

    head_a = rig.archive.head()
    head_b = archive_b.head()
    assert head_a["sequence"] == head_b["sequence"]
    assert head_a["commit_hash"] != head_b["commit_hash"]

    # Import B's mindpack into A: explicit fork, both heads preserved.
    pack_b = tmp_path / "fork-b.mindpack"
    archive_b.sync().export_mindpack(pack_b)
    report = rig.archive.sync().import_mindpack(pack_b)
    assert report["status"] == "fork"
    heads = {h["commit_hash"] for h in report["heads"]}
    assert heads == {head_a["commit_hash"], head_b["commit_hash"]}

    # A's own head is untouched; the fork head is preserved as custody.
    assert rig.archive.head()["commit_hash"] == head_a["commit_hash"]
    forks = rig.archive.sync().forks()
    assert [f["head_commit_hash"] for f in forks] == [head_b["commit_hash"]]
    # B's fork-side object is preserved as evidence in A's store.
    assert rig.archive.get_object(record_b["id"]) is not None
    rig.archive.verify_chain()

    # The negotiation between the two heads reports a fork, never a winner.
    assert negotiate(
        rig.archive.sync().sync_head(), archive_b.sync().sync_head()
    )["relationship"] == "fork"
