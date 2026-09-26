"""Portable CCF exports and imports; bad objects never reject their neighbors."""

from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
import shutil
import tempfile
from urllib.parse import unquote, urlparse
import zipfile

from .jcs import canonicalize, loads
from .objects import hash_bytes, now_timestamp, validate_object
from .schemas import validate_manifest

KIND_FILES = {"record": "records.jsonl", "link": "links.jsonl", "blob": "blobs.jsonl"}


def _issue(report, location, reason, *, object_id=None, warning=False):
    item = {"location": location, "reason": str(reason)}
    if object_id is not None:
        item["id"] = object_id
    report["warnings" if warning else "errors"].append(item)


def _content_path(loc, vault_root, allowed_roots):
    """Resolve only explicitly allowed local roots; never fetch imported URLs."""
    parsed = urlparse(loc)
    if parsed.scheme == "vault" and vault_root is not None:
        path = Path(vault_root) / unquote(parsed.path)
    elif parsed.scheme == "file" and not parsed.netloc:
        path = Path(unquote(parsed.path))
    else:
        raise ValueError(f"location cannot be copied: {loc}")
    path = path.resolve()
    if not any(path.is_relative_to(Path(root).resolve()) for root in allowed_roots):
        raise ValueError(f"location is outside authorized content roots: {loc}")
    if not path.is_file():
        raise ValueError(f"location is not a regular file: {loc}")
    return path


def export_objects(objects, destination, *, source, include_files=False,
                   vault_root=None, allowed_roots=(), content_dir=None):
    """Write a new directory or ZIP. Metadata-only exports never read locs.

    With explicit file inclusion, only allowed local content or previously
    imported hash-named copies are read. Changed content is reported while its
    old fingerprint stays intact; other objects and files still export.
    """
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError(f"export destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    report = {"errors": [], "warnings": [], "counts": {name: 0 for name in ("records", "links", "blobs", "files")}}
    manifest = {"format": "ccf-export", "version": "0.3.0", "exported_at": now_timestamp(),
                "source": source, "counts": report["counts"]}
    validate_manifest(manifest)
    with tempfile.TemporaryDirectory(prefix=".ccf-export-", dir=destination.parent) as temp:
        root = Path(temp) / "export"
        root.mkdir(mode=0o700)
        with ExitStack() as stack:
            streams = {kind: stack.enter_context((root / filename).open("w", encoding="utf-8"))
                       for kind, filename in KIND_FILES.items()}
            seen = set()
            for index, obj in enumerate(objects, 1):
                object_id = obj.get("id") if isinstance(obj, dict) else None
                try:
                    warnings = validate_object(obj)
                    if object_id in seen:
                        raise ValueError("duplicate object ID")
                    seen.add(object_id)
                    streams[obj["kind"]].write(canonicalize(obj) + "\n")
                    report["counts"][obj["kind"] + "s"] += 1
                    for warning in warnings:
                        _issue(report, f"object:{index}", warning, object_id=object_id, warning=True)
                except (ValueError, TypeError) as exc:
                    _issue(report, f"object:{index}", exc, object_id=object_id)
                    continue
                if not include_files or "deleted" in obj or not (obj.get("hash") and obj.get("loc")):
                    continue
                digest = obj["hash"].removeprefix("sha256:")
                target = root / "files" / digest
                if target.exists():
                    continue
                try:
                    cached = Path(content_dir) / digest if content_dir else None
                    path = cached if cached and cached.is_file() and not cached.is_symlink() else _content_path(obj["loc"], vault_root, allowed_roots)
                    data = path.read_bytes()
                    if hash_bytes(data) != obj["hash"]:
                        raise ValueError("content changed: hash mismatch; copy omitted")
                    if obj["kind"] == "blob" and len(data) != obj["size"]:
                        raise ValueError("content size mismatch; copy omitted")
                    target.parent.mkdir(exist_ok=True)
                    target.write_bytes(data)
                    report["counts"]["files"] += 1
                except (OSError, ValueError) as exc:
                    _issue(report, f"object:{index}", exc, object_id=object_id, warning=True)
        (root / "manifest.json").write_text(canonicalize(manifest) + "\n", encoding="utf-8")
        if destination.suffix.lower() == ".zip":
            with destination.open("xb") as output, zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
                destination.chmod(0o600)
                for path in sorted(root.rglob("*")):
                    if path.is_file():
                        archive.write(path, path.relative_to(root).as_posix())
        else:
            destination.mkdir(mode=0o700)
            shutil.copytree(root, destination, dirs_exist_ok=True)
    return report


def import_objects(store, source, *, content_dir=None):
    """Merge an export without changing IDs or resolving any object's loc.

    ZIP members are read directly, never extracted. Each invalid line or file
    is named in the result; valid neighbors are always applied.
    """
    from .import_reader import ExportReader

    report = {"errors": [], "warnings": [], "counts": {"inserted": 0, "updated": 0, "unchanged": 0, "conflicts": 0, "files": 0}}
    with ExportReader(source) as reader:
        manifest = loads(reader.read("manifest.json"))
        validate_manifest(manifest)
        actual = {"records": 0, "links": 0, "blobs": 0, "files": 0}
        referenced = {}
        for kind, filename in KIND_FILES.items():
            if not reader.exists(filename):
                continue
            for number, raw in enumerate(reader.lines(filename), 1):
                if not raw.strip():
                    continue
                location = f"{filename}:{number}"
                actual[kind + "s"] += 1
                object_id = None
                try:
                    obj = loads(raw)
                    if not isinstance(obj, dict) or obj.get("kind") != kind:
                        raise ValueError(f"expected {kind} object")
                    object_id = obj.get("id")
                    result = store.apply(obj)
                    report["counts"][result["status"]] += 1
                    report["counts"]["conflicts"] += int(result["conflict"])
                    for warning in result["warnings"]:
                        _issue(report, location, warning, object_id=object_id, warning=True)
                    if obj.get("hash") and obj.get("loc") and "deleted" not in obj:
                        referenced.setdefault(obj["hash"], []).append(obj)
                except (ValueError, TypeError, UnicodeError) as exc:
                    _issue(report, location, exc, object_id=object_id)
        for name in reader.file_names():
            actual["files"] += 1
            try:
                data = reader.read(name)
                digest = hash_bytes(data)
                if name != "files/" + digest.removeprefix("sha256:"):
                    raise ValueError("file name does not match content hash")
                for obj in referenced.get(digest, []):
                    if obj["kind"] == "blob" and obj["size"] != len(data):
                        _issue(report, name, "blob size does not match content", object_id=obj["id"], warning=True)
                if content_dir is not None:
                    directory = Path(content_dir)
                    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
                    target = directory / digest.removeprefix("sha256:")
                    if target.exists():
                        if target.is_symlink() or hash_bytes(target.read_bytes()) != digest:
                            raise ValueError("existing content copy is unsafe or corrupt")
                    else:
                        with target.open("xb") as output:
                            target.chmod(0o600)
                            output.write(data)
                report["counts"]["files"] += 1
            except (OSError, ValueError) as exc:
                _issue(report, name, exc)
        for kind in actual:
            count = manifest["counts"][kind]
            if actual[kind] != count:
                _issue(report, "manifest.json", f"counts.{kind}: expected {count}, found {actual[kind]}")
    return report
