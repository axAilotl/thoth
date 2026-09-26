"""The migration reads native state, keeps IDs stable, and isolates failures."""

import json
from pathlib import Path
import sqlite3

from ccf.backfill import backfill
from ccf.metadata import CcfMetadataService
from ccf.store import ObjectStore


def test_backfill_idempotent_read_only_and_reports_each_bad_row(tmp_path):
    database = tmp_path / "native.sqlite3"
    vault = tmp_path / "vault"
    vault.mkdir()
    note = vault / "note.md"
    note.write_text("original content")
    payload = {"title": "Note", "normalized_metadata": {"capture_source_id": "source-1"}}
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE ingestion_queue (id INTEGER PRIMARY KEY, artifact_id TEXT, artifact_type TEXT, payload_json TEXT, review_json TEXT)")
        connection.executemany("INSERT INTO ingestion_queue VALUES (?,?,?,?,?)", [(1, "good", "web_clipper", json.dumps(payload), None), (2, "bad", "paper", "{", None)])
        connection.execute("CREATE TABLE files_index (path TEXT, type TEXT)")
        connection.executemany("INSERT INTO files_index VALUES (?,?)", [(str(note), "markdown"), (str(vault / "missing.md"), "markdown")])
    before_db = database.read_bytes()
    service = CcfMetadataService(ObjectStore(tmp_path / "ccf.sqlite3", device="test"), vault_root=vault)
    report = backfill(service, database)
    assert report["counts"] == {"captures": 1, "files": 1}
    assert {item["native_id"] for item in report["errors"]} == {"bad", str(vault / "missing.md")}
    objects = list(service.store.objects())
    backfill(service, database)
    assert list(service.store.objects()) == objects
    assert database.read_bytes() == before_db
    assert note.read_text() == "original content"
