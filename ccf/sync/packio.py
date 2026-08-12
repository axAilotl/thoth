"""Pack container I/O shared by mindpacks and delta packs (spec 11.1, 11.4).

A pack is a directory layout (``manifest.json``, ``objects/*.ndjson``,
``compartments/...``, ``blob-data/*``, ``integrity/*.ndjson``) that is
ZIP-compatible: the same tree travels as a plain directory, a ``.zip`` /
``.mindpack`` file, or over HTTP. NDJSON streams hold one JCS-canonical
JSON document per line, matching the vendored ``examples/mindpack``.

All reads fail closed: path traversal, missing required streams, and
digest or size mismatches raise :class:`PackError`.
"""

from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from ccf.hashing import digest_string, parse_digest
from ccf.jcs import canonical_bytes
from ccf.jcs import loads as jcs_loads


class PackError(RuntimeError):
    """Raised when a pack is malformed, tampered with, or incomplete."""


class IncompletePackError(PackError):
    """Raised when a pack has undeclared dangling references (spec 2.5)."""


def _check_name(name: str) -> PurePosixPath:
    """Validate a pack-relative path; reject traversal and absolutes."""
    if not isinstance(name, str) or not name:
        raise PackError(f"invalid pack entry name: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        raise PackError(f"pack entry name escapes the pack: {name!r}")
    return path


def ndjson_bytes(records: list[dict]) -> bytes:
    """One JCS-canonical line per record, trailing newline on each."""
    return b"".join(canonical_bytes(record) + b"\n" for record in records)


def parse_ndjson(data: bytes, *, what: str) -> list[dict]:
    """Strictly parse an NDJSON stream (duplicate keys rejected via JCS)."""
    records: list[dict] = []
    for lineno, line in enumerate(data.decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        value = jcs_loads(line)
        if not isinstance(value, dict):
            raise PackError(f"{what} line {lineno} is not a JSON object")
        records.append(value)
    return records


def json_bytes(document: dict) -> bytes:
    """Stable pretty JSON for standalone pack documents."""
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


@dataclass
class StreamEntry:
    """One manifest stream entry: path, digest, byte length, required flag."""

    path: str
    digest: str
    byte_length: int
    required: bool = True

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "digest": self.digest,
            "byte_length": str(self.byte_length),
            "required": self.required,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "StreamEntry":
        try:
            return cls(
                path=data["path"],
                digest=data["digest"],
                byte_length=int(data["byte_length"]),
                required=bool(data["required"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise PackError(f"malformed stream entry: {data!r}") from exc


class PackWriter:
    """Writes a pack tree under a fresh directory, tracking stream digests."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        if self.root.exists() and any(self.root.iterdir()):
            raise PackError(f"pack output directory is not empty: {self.root}")
        self.root.mkdir(parents=True, exist_ok=True)
        self._entries: list[StreamEntry] = []

    def write_bytes(self, name: str, data: bytes, *, required: bool = True) -> None:
        rel = _check_name(name)
        target = self.root / Path(*rel.parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        self._entries.append(
            StreamEntry(str(rel), digest_string(data), len(data), required)
        )

    def write_json(self, name: str, document: dict, *, required: bool = True) -> None:
        self.write_bytes(name, json_bytes(document), required=required)

    def write_ndjson(self, name: str, records: list[dict], *, required: bool = True) -> None:
        self.write_bytes(name, ndjson_bytes(records), required=required)

    def write_file(self, name: str, source: str | Path, *, required: bool = True) -> None:
        self.write_bytes(name, Path(source).read_bytes(), required=required)

    @property
    def streams(self) -> list[StreamEntry]:
        return list(self._entries)


class PackReader:
    """Read access to a pack, whether a directory or a ZIP-compatible file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._zip: zipfile.ZipFile | None = None
        if self.path.is_dir():
            self._names = {
                str(p.relative_to(self.path)).replace("\\", "/")
                for p in self.path.rglob("*")
                if p.is_file()
            }
        elif self.path.is_file():
            try:
                self._zip = zipfile.ZipFile(self.path)
            except zipfile.BadZipFile as exc:
                raise PackError(f"not a ZIP-compatible pack: {self.path}") from exc
            self._names = {
                info.filename for info in self._zip.infolist() if not info.is_dir()
            }
        else:
            raise PackError(f"pack not found: {self.path}")

    def close(self) -> None:
        if self._zip is not None:
            self._zip.close()
            self._zip = None

    def __enter__(self) -> "PackReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def names(self) -> set[str]:
        return set(self._names)

    def has(self, name: str) -> bool:
        return str(_check_name(name)) in self._names

    def read(self, name: str) -> bytes:
        rel = str(_check_name(name))
        if rel not in self._names:
            raise PackError(f"pack entry missing: {rel}")
        if self._zip is not None:
            return self._zip.read(rel)
        return (self.path / Path(*PurePosixPath(rel).parts)).read_bytes()

    def read_json(self, name: str) -> dict:
        value = jcs_loads(self.read(name))
        if not isinstance(value, dict):
            raise PackError(f"pack entry {name} is not a JSON object")
        return value

    def read_ndjson(self, name: str) -> list[dict]:
        return parse_ndjson(self.read(name), what=name)


def verify_stream_digests(reader: PackReader, streams: list[StreamEntry]) -> list[str]:
    """Verify every manifest stream against pack bytes; fail closed.

    A digest or size mismatch always raises (tampering). A missing stream
    raises when required; optional misses are returned as notes.
    """
    notes: list[str] = []
    for entry in streams:
        if not reader.has(entry.path):
            if entry.required:
                raise PackError(f"required pack stream missing: {entry.path}")
            notes.append(f"optional stream absent: {entry.path}")
            continue
        data = reader.read(entry.path)
        if len(data) != entry.byte_length:
            raise PackError(
                f"stream {entry.path} byte length {len(data)} != "
                f"manifest {entry.byte_length}"
            )
        if digest_string(data) != entry.digest:
            raise PackError(f"stream {entry.path} digest mismatch (tampered)")
        parse_digest(entry.digest)
    return notes


def zip_pack_dir(pack_dir: str | Path, out_file: str | Path) -> Path:
    """Pack a directory tree into a ZIP-compatible ``.mindpack`` file."""
    pack_dir = Path(pack_dir)
    out_file = Path(out_file)
    if not pack_dir.is_dir():
        raise PackError(f"pack directory not found: {pack_dir}")
    with zipfile.ZipFile(out_file, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(pack_dir.rglob("*")):
            if path.is_file():
                archive.write(path, str(path.relative_to(pack_dir)))
    return out_file


@dataclass
class PackObject:
    """One portable object inside a pack (header + compartment envelopes)."""

    header: dict
    structural: dict | None = None  # envelope; None when unavailable
    semantic: dict | None = None
    blob_data_name: str | None = None  # pack path of blob-data bytes

    @property
    def object_id(self) -> str:
        return self.header["id"]

    @property
    def object_kind(self) -> str:
        return self.header["object_kind"]


@dataclass
class PackContents:
    """Parsed object/compartment/blob view of a pack (pre-verification)."""

    objects: dict[str, PackObject] = field(default_factory=dict)
    blob_data: dict[str, bytes] = field(default_factory=dict)  # blob_id -> bytes


def load_pack_objects(reader: PackReader) -> PackContents:
    """Load object streams, compartment envelopes, and Blob bytes from a pack."""
    contents = PackContents()
    for kind in ("record", "link", "blob"):
        name = f"objects/{kind}s.ndjson"
        if not reader.has(name):
            continue
        for header in reader.read_ndjson(name):
            if header.get("object_kind") != kind:
                raise PackError(
                    f"{name}: header kind {header.get('object_kind')!r} != {kind!r}"
                )
            object_id = header.get("id")
            if not isinstance(object_id, str) or object_id in contents.objects:
                raise PackError(f"{name}: duplicate or missing object id")
            contents.objects[object_id] = PackObject(header=header)

    for name in sorted(reader.names()):
        parts = PurePosixPath(name).parts
        if len(parts) == 3 and parts[0] == "compartments":
            kind_dir, filename = parts[1], parts[2]
            kind = kind_dir[:-1] if kind_dir.endswith("s") else kind_dir
            for compartment in ("structural", "semantic"):
                suffix = f".{compartment}.json"
                if filename.endswith(suffix):
                    object_id = f"urn:ccf:{kind}:{filename[: -len(suffix)]}"
                    obj = contents.objects.get(object_id)
                    if obj is None:
                        raise PackError(f"compartment without object header: {name}")
                    envelope = reader.read_json(name)
                    setattr(obj, compartment, envelope)
        elif len(parts) == 2 and parts[0] == "blob-data":
            object_id = f"urn:ccf:blob:{PurePosixPath(name).stem}"
            obj = contents.objects.get(object_id)
            if obj is None:
                raise PackError(f"blob data without object header: {name}")
            data = reader.read(name)
            contents.blob_data[object_id] = data
            obj.blob_data_name = name
    return contents
