"""Exercise portable exchange, partial failures and hostile file locations."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

from ccf.exchange import export_objects, import_objects
from ccf.objects import hash_bytes
from ccf.store import ObjectStore


@pytest.mark.parametrize("suffix", ["", ".zip"])
def test_roundtrip_preserves_ids_relationships_unknown_fields_and_files(tmp_path, suffix):
    source = ObjectStore(tmp_path / "source.sqlite3", device="phone")
    target = ObjectStore(tmp_path / "target.sqlite3", device="homelab")
    data = b"a private recording"
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "audio.ogg").write_bytes(data)
    blob = source.upsert("blob", "blob", {"loc": "vault:audio.ogg", "hash": hash_bytes(data), "size": len(data), "media_type": "audio/ogg"})
    note = source.upsert("note", "record", {"type": "custom.note", "data": {"score": "0.93"}, "ext": {"custom": [1, 2]}, "future": "kept"})
    source.upsert("link", "link", {"from": note["id"], "to": blob["id"], "rel": "custom.attachment"})
    destination = tmp_path / ("export" + suffix)
    report = export_objects(source.objects(), destination, source={"app": "thoth"}, include_files=True,
                            vault_root=vault, allowed_roots=[vault])
    assert not report["errors"] and not report["warnings"]
    assert report["counts"] == {"records": 1, "links": 1, "blobs": 1, "files": 1}
    report = import_objects(target, destination, content_dir=tmp_path / "copies")
    assert not report["errors"]
    assert report["counts"]["inserted"] == 3
    assert sorted(source.objects(), key=lambda obj: obj["id"]) == sorted(target.objects(), key=lambda obj: obj["id"])
    assert (tmp_path / "copies" / hash_bytes(data)[7:]).read_bytes() == data
    assert import_objects(target, destination)["counts"]["unchanged"] == 3
    if not suffix:
        validator = Path(__file__).resolve().parents[1] / "spec/ccf/0.3.0/tools/validate.py"
        result = subprocess.run([sys.executable, str(validator), "export", str(destination)], capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr


def test_import_continues_after_invalid_json_utf8_and_schema(tmp_path):
    source = ObjectStore(tmp_path / "source.sqlite3")
    obj = source.upsert("one", "record", {"type": "note", "data": {"v": 2}})
    destination = tmp_path / "export"
    export_objects(source.objects(), destination, source={"app": "thoth"})
    path = destination / "records.jsonl"
    valid = path.read_bytes()
    path.write_bytes(b'{"kind":"record","rev":1.0}\n\xff\n{}\n' + valid)
    target = ObjectStore(tmp_path / "target.sqlite3")
    report = import_objects(target, destination)
    assert target.get(obj["id"]) == obj
    assert [item["location"] for item in report["errors"]] == ["records.jsonl:1", "records.jsonl:2", "records.jsonl:3", "manifest.json"]


def test_hash_mismatch_is_retained_and_reported(tmp_path):
    store = ObjectStore(tmp_path / "store.sqlite3")
    obj = store.upsert("one", "record", {"type": "note", "data": {"v": 2}})
    bad = deepcopy(obj)
    bad["data"]["v"] = 3
    dest = tmp_path / "export"
    report = export_objects([bad], dest, source={"app": "thoth"})
    assert report["warnings"] and report["counts"]["records"] == 1
    target = ObjectStore(tmp_path / "target.sqlite3")
    report = import_objects(target, dest)
    assert report["warnings"] and target.get(bad["id"]) == bad


def test_export_restricts_files_and_reports_changed_content(tmp_path):
    store = ObjectStore(tmp_path / "store.sqlite3")
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    private = tmp_path / "private"
    private.write_bytes(b"secret")
    (allowed / "escape").symlink_to(private)
    (allowed / "changed").write_bytes(b"changed")
    for name, loc in [("escape", "vault:escape"), ("changed", "vault:changed"), ("network", "https://example.invalid/private")]:
        store.upsert(name, "record", {"type": "note", "loc": loc, "hash": hash_bytes(b"secret")})
    dest = tmp_path / "export"
    report = export_objects(store.objects(), dest, source={"app": "thoth"}, include_files=True,
                            vault_root=allowed, allowed_roots=[allowed])
    assert report["counts"]["records"] == 3 and report["counts"]["files"] == 0
    assert len(report["warnings"]) == 3
    with pytest.raises(FileExistsError):
        export_objects([], dest, source={"app": "thoth"})


def test_zip_traversal_is_not_extracted_and_valid_objects_survive(tmp_path):
    store = ObjectStore(tmp_path / "store.sqlite3")
    obj = store.upsert("one", "record", {"type": "note"})
    dest = tmp_path / "export.zip"
    export_objects(store.objects(), dest, source={"app": "thoth"})
    with zipfile.ZipFile(dest, "a") as archive:
        archive.writestr("files/../../outside", b"hostile")
    target = ObjectStore(tmp_path / "target.sqlite3")
    report = import_objects(target, dest, content_dir=tmp_path / "copies")
    assert target.get(obj["id"]) == obj and report["errors"]
    assert not (tmp_path / "outside").exists()


def test_invalid_export_object_does_not_block_valid_neighbors(tmp_path):
    store = ObjectStore(tmp_path / "store.sqlite3")
    obj = store.upsert("one", "record", {"type": "note"})
    report = export_objects([{"id": "bad"}, obj], tmp_path / "export", source={"app": "thoth"})
    assert report["counts"]["records"] == 1
    assert report["errors"][0]["id"] == "bad"
