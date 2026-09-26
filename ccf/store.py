"""Durable CCF objects and per-peer delivery cursors in one SQLite database."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from ccf.ids import generate_id
from ccf.jcs import MAX_SAFE_INTEGER, canonicalize, loads
from ccf.objects import KINDS, hash_data, merge_key, now_timestamp, validate_object

_MANAGED = frozenset({"id", "kind", "created", "updated", "rev", "source"})
_APPLICATION_ID = 0x43434633  # CCF3
_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS counter (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), value INTEGER NOT NULL)",
    "INSERT OR IGNORE INTO counter VALUES (1, 0)",
    "CREATE TABLE IF NOT EXISTS objects (id TEXT PRIMARY KEY, kind TEXT NOT NULL, body TEXT NOT NULL, changed INTEGER UNIQUE NOT NULL, deleted INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS native_keys (native_key TEXT PRIMARY KEY, object_id TEXT UNIQUE NOT NULL REFERENCES objects(id))",
    "CREATE TABLE IF NOT EXISTS peers (peer TEXT PRIMARY KEY, acknowledged INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS conflicts (object_id TEXT NOT NULL REFERENCES objects(id), body TEXT NOT NULL, PRIMARY KEY (object_id, body))",
)


def _nonempty_string(value: object, name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a nonempty string")
    canonicalize(value)


def _create_private_path(path: Path) -> None:
    """Create only missing directories/files, without changing existing paths."""
    missing = []
    directory = path.parent
    while not directory.exists():
        missing.append(directory)
        directory = directory.parent
    for directory in reversed(missing):
        directory.mkdir(mode=0o700, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return
    os.close(descriptor)


def _validate_database(db) -> None:
    application = db.execute("PRAGMA application_id").fetchone()[0]
    version = db.execute("PRAGMA user_version").fetchone()[0]
    if (application, version) not in ((0, 0), (_APPLICATION_ID, 1)):
        raise ValueError("database is not a supported CCF 0.3.0 store")
    if application == 0 and db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' LIMIT 1"
    ).fetchone():
        raise ValueError("refusing to initialize a non-CCF database")


class ObjectStore:
    """A file-backed store; each operation owns and closes its connection.

    Transactions serialize writers across threads and processes. Object IDs,
    local native-key bindings, and acknowledgements survive process restarts.
    No remote location is ever dereferenced by this class.
    """

    def __init__(self, path: str | Path, *, device: str = "thoth") -> None:
        _nonempty_string(device, "device")
        if str(path) == ":memory:":
            raise ValueError("ObjectStore requires a persistent file path")
        self.path = Path(path)
        self.device = device
        _create_private_path(self.path)
        with self._connection() as db:
            # Inspect a consistent snapshot before changing journal mode or
            # permissions: an accidentally selected native database is untouched.
            db.execute("BEGIN")
            _validate_database(db)
            db.commit()
            self.path.chmod(0o600)
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("BEGIN IMMEDIATE")
            _validate_database(db)
            for statement in _SCHEMA:
                db.execute(statement)
            db.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            db.execute("PRAGMA user_version=1")

    @contextmanager
    def _connection(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            db.execute("PRAGMA busy_timeout=30000")
            db.execute("PRAGMA foreign_keys=ON")
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _get(db, object_id: str) -> dict | None:
        row = db.execute("SELECT body FROM objects WHERE id=?", (object_id,)).fetchone()
        return loads(row[0]) if row else None

    @staticmethod
    def _write(db, obj: dict) -> None:
        counter = db.execute("SELECT value FROM counter WHERE singleton=1").fetchone()[0]
        if counter >= MAX_SAFE_INTEGER:
            raise ValueError("local change counter exhausted")
        counter += 1
        db.execute("UPDATE counter SET value=? WHERE singleton=1", (counter,))
        db.execute(
            "INSERT INTO objects VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
            "kind=excluded.kind, body=excluded.body, changed=excluded.changed, deleted=excluded.deleted",
            (obj["id"], obj["kind"], canonicalize(obj), counter, int("deleted" in obj)),
        )

    def get(self, object_id: str) -> dict | None:
        with self._connection() as db:
            return self._get(db, object_id)

    def find(self, native_key: str) -> dict | None:
        with self._connection() as db:
            row = db.execute(
                "SELECT o.body FROM objects o JOIN native_keys n ON n.object_id=o.id "
                "WHERE n.native_key=?", (native_key,),
            ).fetchone()
            return loads(row[0]) if row else None

    def objects(self) -> Iterator[dict]:
        """Iterate a consistent snapshot, including tombstones."""
        with self._connection() as db:
            rows = db.execute("SELECT body FROM objects ORDER BY id").fetchall()
        return (loads(row[0]) for row in rows)

    def upsert(
        self, native_key: str, kind: str, fields: dict, *, source: dict | None = None,
    ) -> dict:
        """Replace local mutable fields, creating a stable ID only once.

        ``fields`` is the complete mutable state, not a partial patch. Managed
        identity/revision fields are forbidden. Source is used only at creation.
        Inline Record data receives its canonical hash automatically. Setting
        ``deleted`` produces a minimal tombstone, retaining a Record's type.
        """
        _nonempty_string(native_key, "native_key")
        if not isinstance(kind, str) or kind not in KINDS:
            raise ValueError("kind must be record, link, or blob")
        if not isinstance(fields, dict) or _MANAGED.intersection(fields):
            raise ValueError("fields must be a dict without store-managed fields")
        mutable = loads(canonicalize(fields))
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT object_id FROM native_keys WHERE native_key=?", (native_key,),
            ).fetchone()
            previous = self._get(db, row[0]) if row else None
            if previous is not None and previous["kind"] != kind:
                raise ValueError("an object's kind cannot change")
            if "deleted" in mutable:
                mutable = {"deleted": mutable["deleted"]}
                if kind == "record":
                    mutable["type"] = fields.get("type", (previous or {}).get("type"))
            elif kind == "record" and "loc" not in mutable:
                if "data" in mutable:
                    mutable["hash"] = hash_data(mutable["data"])
                else:
                    mutable.pop("hash", None)
            if previous is not None:
                old_fields = {key: value for key, value in previous.items() if key not in _MANAGED}
                if canonicalize(old_fields) == canonicalize(mutable):
                    return previous
                obj = {key: previous[key] for key in _MANAGED}
                obj.update(mutable, rev=previous["rev"] + 1, updated=now_timestamp())
            else:
                timestamp = now_timestamp()
                obj = dict(
                    mutable, id=generate_id(), kind=kind, created=timestamp,
                    updated=timestamp, rev=1,
                    source=source if source is not None else {
                        "app": "thoth", "device": self.device, "native_id": native_key,
                    },
                )
            validate_object(obj)
            self._write(db, obj)
            if previous is None:
                db.execute("INSERT INTO native_keys VALUES (?, ?)", (native_key, obj["id"]))
            return loads(canonicalize(obj))

    def apply(self, obj: dict) -> dict:
        """Merge one incoming object; callers report errors per object and continue."""
        warnings = validate_object(obj)
        incoming = loads(canonicalize(obj))
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = self._get(db, incoming["id"])
            conflict = False
            winner = incoming
            status = "inserted"
            if previous is not None:
                different = canonicalize(previous) != canonicalize(incoming)
                conflict = different and previous["rev"] == incoming["rev"]
                winner = previous if merge_key(previous) >= merge_key(incoming) else incoming
                status = "unchanged" if winner is previous else "updated"
                changed_identity = [
                    key for key in ("kind", "created", "source") if previous[key] != incoming[key]
                ]
                if changed_identity:
                    warnings.append(
                        f"{incoming['id']}: copies differ in immutable fields: {', '.join(changed_identity)}"
                    )
                if conflict:
                    loser = incoming if winner is previous else previous
                    db.execute(
                        "INSERT OR IGNORE INTO conflicts VALUES (?, ?)",
                        (loser["id"], canonicalize(loser)),
                    )
                    warnings.append(f"{incoming['id']}: same-revision conflict retained")
            if status != "unchanged":
                self._write(db, winner)
            return {"status": status, "object": winner, "warnings": warnings, "conflict": conflict}

    def conflicts(self, object_id: str | None = None) -> list[dict]:
        """Return retained losing copies for inspection, including unknown fields."""
        with self._connection() as db:
            if object_id is None:
                rows = db.execute("SELECT body FROM conflicts ORDER BY object_id, body").fetchall()
            else:
                rows = db.execute(
                    "SELECT body FROM conflicts WHERE object_id=? ORDER BY body", (object_id,),
                ).fetchall()
        return [loads(row[0]) for row in rows]

    def changes(self, peer: str, *, limit: int = 1000) -> dict:
        """Read pending current objects without acknowledging delivery."""
        _nonempty_string(peer, "peer")
        if type(limit) is not int or not 1 <= limit <= MAX_SAFE_INTEGER:
            raise ValueError("limit must be a positive safe integer")
        with self._connection() as db:
            db.execute("BEGIN")
            row = db.execute("SELECT acknowledged FROM peers WHERE peer=?", (peer,)).fetchone()
            acknowledged = row[0] if row else 0
            rows = db.execute(
                "SELECT changed, body FROM objects WHERE changed>? ORDER BY changed LIMIT ?",
                (acknowledged, limit),
            ).fetchall()
        return {
            "cursor": rows[-1][0] if rows else acknowledged,
            "objects": [loads(row[1]) for row in rows],
        }

    def acknowledge(self, peer: str, cursor: int) -> None:
        """Advance only this peer after successful delivery; stale acks are harmless."""
        _nonempty_string(peer, "peer")
        if type(cursor) is not int or cursor < 0:
            raise ValueError("cursor must be a nonnegative integer")
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            maximum = db.execute("SELECT value FROM counter WHERE singleton=1").fetchone()[0]
            if cursor > maximum:
                raise ValueError("cursor exceeds the local change counter")
            db.execute(
                "INSERT INTO peers VALUES (?, ?) ON CONFLICT(peer) DO UPDATE SET "
                "acknowledged=MAX(peers.acknowledged, excluded.acknowledged)", (peer, cursor),
            )

    def stats(self) -> dict[str, int]:
        with self._connection() as db:
            db.execute("BEGIN")
            counts = {"objects": 0, "records": 0, "links": 0, "blobs": 0, "tombstones": 0}
            for kind, count, deleted in db.execute("SELECT kind, COUNT(*), SUM(deleted) FROM objects GROUP BY kind"):
                counts[kind + "s"] = count
                counts["objects"] += count
                counts["tombstones"] += deleted
            counts["conflicts"] = db.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0]
            counts["peers"] = db.execute("SELECT COUNT(*) FROM peers").fetchone()[0]
            counts["change_counter"] = db.execute("SELECT value FROM counter WHERE singleton=1").fetchone()[0]
            return counts
