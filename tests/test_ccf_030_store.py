"""Store durability, concurrency, merge and acknowledgement guarantees."""

from __future__ import annotations

import copy
import os
import sqlite3
import stat
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from ccf.ids import generate_id
from ccf.objects import hash_data, now_timestamp
from ccf.store import ObjectStore


@pytest.fixture
def store(tmp_path):
    return ObjectStore(tmp_path / "state/objects.sqlite3", device="test-device")


def note(store, key="note:1", title="Note"):
    return store.upsert(key, "record", {"type": "note", "title": title})


def foreign(**fields):
    timestamp = now_timestamp()
    obj = dict(id=generate_id(), kind="record", type="note", created=timestamp, updated=timestamp, rev=1, source={"app": "foreign", "device": "phone"})
    obj.update(fields)
    return obj


def test_upsert_stable_identity_and_source_survive_reopen(store):
    first = note(store)
    reopened = ObjectStore(store.path, device="changed-device")
    assert note(reopened) == first
    changed = reopened.upsert("note:1", "record", {"type": "note", "title": "Changed"}, source={"app": "different"})
    for field in ("id", "kind", "created", "source"):
        assert changed[field] == first[field]
    assert changed["rev"] == 2
    assert reopened.stats()["change_counter"] == 2
    assert reopened.find("note:1") == changed


def test_upsert_hashes_inline_data_and_does_not_hash_location_content(store):
    obj = store.upsert("inline", "record", {"type": "note", "data": {"text": "hello"}})
    assert obj["hash"] == hash_data(obj["data"])
    assert store.upsert("inline", "record", {"type": "note", "data": {"text": "hello"}}) == obj
    revised = store.upsert("inline", "record", {"type": "note", "data": {"text": "changed"}})
    assert revised["hash"] == hash_data(revised["data"])
    assert revised["rev"] == 2
    located = store.upsert("located", "record", {"type": "note", "loc": "mystery:keep", "hash": "sha256:" + "0" * 64, "data": {"text": "metadata"}})
    assert located["loc"] == "mystery:keep"
    assert located["hash"] == "sha256:" + "0" * 64


def test_upsert_replaces_mutable_fields_and_removes_obsolete_hash(store):
    first = store.upsert("native", "record", {"type": "note", "title": "Old", "data": {"x": 1}})
    second = store.upsert("native", "record", {"type": "note"})
    assert second["id"] == first["id"]
    assert "title" not in second and "data" not in second and "hash" not in second


@pytest.mark.parametrize("field", ["id", "kind", "created", "updated", "rev", "source"])
def test_managed_fields_cannot_be_overridden(store, field):
    with pytest.raises(ValueError, match="store-managed"):
        store.upsert("native", "record", {"type": "note", field: "override"})
    assert store.stats()["objects"] == 0


def test_native_key_cannot_change_kind(store):
    original = note(store)
    with pytest.raises(ValueError, match="kind cannot change"):
        store.upsert("note:1", "link", {"from": generate_id(), "to": generate_id(), "rel": "related"})
    assert store.find("note:1") == original


def test_invalid_upsert_and_apply_leave_counter_and_native_map_untouched(store):
    with pytest.raises(ValueError):
        store.upsert("native", "record", {"type": "note", "ext": {"confidence": 0.5}})
    with pytest.raises(ValueError):
        store.apply(foreign(rev=False))
    assert store.find("native") is None
    assert store.stats()["objects"] == 0
    assert store.stats()["change_counter"] == 0
    assert store.apply(foreign())["status"] == "inserted"


def test_failed_write_rolls_back_object_and_change_counter(store, monkeypatch):
    original = note(store)
    original_write = store._write
    def fail_after_write(db, obj):
        original_write(db, obj)
        raise RuntimeError("simulated interrupted transaction")
    monkeypatch.setattr(store, "_write", fail_after_write)
    with pytest.raises(RuntimeError, match="interrupted transaction"):
        note(store, title="This write must not survive")
    assert store.find("note:1") == original
    assert store.stats()["change_counter"] == 1
    with pytest.raises(RuntimeError, match="interrupted transaction"):
        note(store, "new")
    assert store.find("new") is None
    assert store.stats()["objects"] == 1


def test_higher_revision_beats_later_clock_without_false_conflict(store):
    higher = foreign(rev=3, updated="2026-01-01T00:00:00.000Z", title="higher")
    lower = dict(higher, rev=2, updated="2026-12-01T00:00:00.000Z", title="lower")
    store.apply(higher)
    result = store.apply(lower)
    assert result["object"] == higher
    assert result["status"] == "unchanged"
    assert not result["conflict"]
    assert store.conflicts() == []
    assert store.stats()["change_counter"] == 1


@pytest.mark.parametrize("reverse", [False, True])
def test_equal_revision_conflicts_keep_loser_and_converge(store, reverse):
    a = foreign(title="Alpha", ext={"opaque": [1, True]})
    b = dict(a, title="Beta")
    first, second = (b, a) if reverse else (a, b)
    store.apply(first)
    result = store.apply(second)
    assert result["conflict"]
    assert result["object"] == b
    assert store.conflicts(a["id"]) == [a]
    assert ObjectStore(store.path).conflicts() == [a]
    assert store.stats()["conflicts"] == 1
    store.apply(a)
    assert store.stats()["conflicts"] == 1
    assert not store.apply(b)["conflict"]


def test_hash_mismatch_and_unknown_fields_are_retained_with_warning(store):
    obj = foreign(type="future.note", data={"text": "current"}, hash=hash_data({"text": "old"}), ext={"vendor": [1, "a"]}, unknown={"key": True})
    obj["source"]["unknown"] = {"metadata": "kept"}
    untouched = copy.deepcopy(obj)
    result = store.apply(obj)
    assert result["status"] == "inserted"
    assert any("hash does not match" in warning for warning in result["warnings"])
    assert store.get(obj["id"]) == untouched
    assert obj == untouched


def test_missing_link_endpoints_and_locations_do_not_block(store):
    obj = foreign(kind="link", **{"from": generate_id(), "to": generate_id(), "rel": "future.related"})
    del obj["type"]
    assert store.apply(obj)["status"] == "inserted"
    located = foreign(loc="https://example.invalid/never-fetch")
    assert store.apply(located)["object"] == located


@pytest.mark.parametrize("kind", ["record", "link", "blob"])
def test_minimal_tombstones_persist_sync_and_do_not_delete_external_files(store, tmp_path, kind):
    path = tmp_path / "source.txt"
    path.write_text("keep", encoding="utf-8")
    fields = {
        "record": {"type": "note", "title": "Name", "loc": path.as_uri()},
        "link": {"from": generate_id(), "to": generate_id(), "rel": "related"},
        "blob": {"loc": path.as_uri(), "hash": "sha256:" + "0" * 64, "size": 4, "media_type": "text/plain"},
    }[kind]
    first = store.upsert("native", kind, fields)
    deleted_at = now_timestamp()
    tombstone = store.upsert("native", kind, {"deleted": deleted_at})
    assert tombstone["id"] == first["id"]
    assert tombstone["rev"] == 2
    assert set(tombstone) == {"id", "kind", "created", "updated", "rev", "source", "deleted"} | ({"type"} if kind == "record" else set())
    assert list(store.objects()) == [tombstone]
    assert store.changes("phone")["objects"] == [tombstone]
    assert store.apply(first)["object"] == tombstone
    assert store.stats()["tombstones"] == 1
    assert path.read_text(encoding="utf-8") == "keep"
    restored = store.upsert("native", kind, fields)
    assert "deleted" not in restored and restored["rev"] == 3


def test_clock_rollback_does_not_hide_changes(store, monkeypatch):
    monkeypatch.setattr("ccf.store.now_timestamp", lambda: "2026-09-26T12:00:00.000Z")
    first = note(store)
    page = store.changes("phone")
    store.acknowledge("phone", page["cursor"])
    monkeypatch.setattr("ccf.store.now_timestamp", lambda: "2026-01-01T00:00:00.000Z")
    second = note(store, title="After clock rollback")
    assert second["updated"] < first["updated"]
    assert store.changes("phone")["objects"] == [second]
    assert store.changes("phone")["cursor"] > page["cursor"]


def test_export_read_is_not_ack_and_peers_are_isolated(store):
    obj = note(store)
    page = store.changes("phone")
    assert store.changes("phone") == page
    store.acknowledge("phone", page["cursor"])
    assert store.changes("phone")["objects"] == []
    assert store.changes("laptop")["objects"] == [obj]
    reopened = ObjectStore(store.path)
    assert reopened.changes("phone")["objects"] == []
    assert reopened.changes("laptop")["objects"] == [obj]
    reopened.acknowledge("phone", 0)
    assert reopened.changes("phone")["objects"] == []


def test_limited_pages_and_interleaved_updates_do_not_skip_objects(store):
    first = note(store, "first")
    second = note(store, "second")
    third = note(store, "third")
    page = store.changes("phone", limit=2)
    assert page["objects"] == [first, second]
    changed = note(store, "first", "Changed during delivery")
    store.acknowledge("phone", page["cursor"])
    remaining = store.changes("phone")
    assert remaining["objects"] == [third, changed]
    store.acknowledge("phone", remaining["cursor"])
    assert store.changes("phone")["objects"] == []


@pytest.mark.parametrize("cursor", [-1, True, 1.0, "1", 2**80, 1])
def test_invalid_or_future_ack_is_rejected(store, cursor):
    with pytest.raises(ValueError):
        store.acknowledge("peer", cursor)
    assert store.stats()["peers"] == 0


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "2", 2**80])
def test_invalid_page_limit_is_rejected(store, limit):
    with pytest.raises(ValueError):
        store.changes("peer", limit=limit)


def test_concurrent_initialization_and_idempotent_upserts_create_one_identity(tmp_path):
    path = tmp_path / "concurrent.sqlite3"
    def write(_):
        return note(ObjectStore(path))
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(write, range(24)))
    assert len({obj["id"] for obj in results}) == 1
    assert {obj["rev"] for obj in results} == {1}
    assert ObjectStore(path).stats()["change_counter"] == 1


def test_concurrent_different_edits_increment_without_lost_revision(store):
    original = note(store)
    def write(number):
        return note(store, title=f"Edit {number}")
    with ThreadPoolExecutor(max_workers=8) as pool:
        edits = list(pool.map(write, range(24)))
    assert {obj["id"] for obj in edits} == {original["id"]}
    assert {obj["rev"] for obj in edits} == set(range(2, 26))
    assert store.find("note:1")["rev"] == 25
    assert store.stats()["change_counter"] == 25


def test_unrelated_sqlite_database_is_not_repurposed(tmp_path):
    path = tmp_path / "unrelated.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE important (value TEXT)")
        before_mode = db.execute("PRAGMA journal_mode").fetchone()[0]
    before_permissions = path.stat().st_mode
    before_contents = path.read_bytes()
    with pytest.raises(ValueError, match="non-CCF database"):
        ObjectStore(path)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [("important",)]
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == before_mode
    assert path.stat().st_mode == before_permissions
    assert path.read_bytes() == before_contents


def test_new_database_directories_and_wal_are_private_with_permissive_umask(tmp_path):
    previous_umask = os.umask(0)
    try:
        path = tmp_path / "new" / "nested" / "objects.sqlite3"
        store = ObjectStore(path)
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(path.parent.parent.stat().st_mode) == 0o700
        with sqlite3.connect(path) as db:
            db.execute("BEGIN")
            db.execute("SELECT * FROM objects").fetchall()
            note(store)
            for suffix in ("-wal", "-shm"):
                assert stat.S_IMODE(Path(str(path) + suffix).stat().st_mode) == 0o600
    finally:
        os.umask(previous_umask)


def test_store_uses_wal_and_rejects_memory_database(store):
    with sqlite3.connect(store.path) as db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    with pytest.raises(ValueError, match="persistent"):
        ObjectStore(":memory:")
