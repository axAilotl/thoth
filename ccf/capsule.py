"""CCF Capsule: scoped exchange transport (0.2.0 draft section 6).

Capsule is not a fourth object kind. It carries submissions or opaque
bytes with explicit custody, activation requirements, and stream digests.
Mindpack remains the archive-oriented restore/merge container.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from ccf.hashing import submission_hash
from ccf.ids import parse_id
from ccf.layered import LayeredError, LayeredRegistries, raw_digest
from ccf.schemas import CcfSchemaError, SchemaSet

SCHEMA_CAPSULE = "urn:ccf:schema:0.2.0:exchange.capsule-manifest"
SCHEMA_RECORD_SUBMISSION = "urn:ccf:schema:0.1.2:submissions.record"
SCHEMA_LINK_SUBMISSION = "urn:ccf:schema:0.1.2:submissions.link"
SCHEMA_BLOB_SUBMISSION = "urn:ccf:schema:0.1.2:submissions.blob"

_SUBMISSION_SCHEMAS = {
    "record": SCHEMA_RECORD_SUBMISSION,
    "link": SCHEMA_LINK_SUBMISSION,
    "blob": SCHEMA_BLOB_SUBMISSION,
}


class CapsuleError(ValueError):
    """Raised when a Capsule is malformed or violates activation rules."""


def _read_ndjson(data: bytes) -> list[dict]:
    values = []
    for line in data.splitlines():
        if not line.strip():
            continue
        values.append(json.loads(line))
    return values


def _write_ndjson(path: Path, values: list[dict]) -> bytes:
    payload = "".join(json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n" for value in values)
    encoded = payload.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded)
    return encoded


@dataclass
class CapsuleStream:
    """One verified Capsule stream and its bytes."""

    spec: dict
    data: bytes
    values: list[object] = field(default_factory=list)

    @property
    def path(self) -> str:
        return self.spec["path"]

    @property
    def handling(self) -> str:
        return self.spec["handling"]

    @property
    def content_role(self) -> str:
        return self.spec["content_role"]


@dataclass
class Capsule:
    """A loaded, digest-verified Capsule directory."""

    root: Path
    manifest: dict
    streams: list[CapsuleStream]

    @property
    def submissions(self) -> list[dict]:
        items: list[dict] = []
        for stream in self.streams:
            if stream.content_role == "submissions" and stream.handling == "activate":
                items.extend(value for value in stream.values if isinstance(value, dict))
        return items

    @property
    def opaque_values(self) -> list[dict]:
        items: list[dict] = []
        for stream in self.streams:
            if stream.handling == "preserve_opaque":
                items.extend(value for value in stream.values if isinstance(value, dict))
        return items


def _is_canonical_relative_posix(rel: str) -> bool:
    """True iff ``rel`` is a non-empty, relative, normalized POSIX path.

    Rejects absolute paths, dot-dot traversal, empty segments (``a//b``),
    and ``.`` segments (``a/./b``, ``./a``). Trailing slashes are also
    rejected because they produce empty final segments.
    """
    if not rel or rel.startswith("/"):
        return False
    parts = rel.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        return False
    return True


def _resolve_package_path(
    root: Path,
    rel: str,
    *,
    must_exist: bool = True,
    must_be_file: bool = True,
) -> Path:
    """Resolve ``rel`` under ``root`` with fail-closed containment.

    Rejects absolute paths, dot-dot traversal, and non-normalized paths
    (empty or ``.`` segments). Resolves symlinks and proves the result
    stays inside ``root``. When ``must_exist`` is true the path must exist;
    when ``must_be_file`` is true it must be a regular file.
    """
    if not _is_canonical_relative_posix(rel):
        raise CapsuleError(f"path is not canonical relative POSIX: {rel!r}")
    resolved = (root / rel).resolve()
    root_resolved = root.resolve()
    if root_resolved not in resolved.parents and resolved != root_resolved:
        raise CapsuleError(f"path escapes root: {rel!r}")
    if must_exist and not resolved.exists():
        raise CapsuleError(f"path does not exist: {rel!r}")
    if must_be_file and not resolved.is_file():
        raise CapsuleError(f"path is not a file: {rel!r}")
    return resolved


def _enumerate_package_files(root: Path) -> set[str]:
    """Return every regular file path under ``root`` as canonical relative POSIX.

    Walks without following symlink directories and rejects every symlink
    entry (file or directory) and every non-regular filesystem entry. This is
    the safe tree inventory used to prove exact physical coverage of a
    Capsule or downgrade source/export package.
    """

    def _on_walk_error(exc: OSError) -> None:
        raise CapsuleError(f"package tree traversal failed: {exc}") from exc

    root_resolved = root.resolve()
    files: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(
        root_resolved, followlinks=False, onerror=_on_walk_error
    ):
        for name in dirnames + filenames:
            full = Path(dirpath) / name
            if full.is_symlink():
                raise CapsuleError(
                    f"package contains symlink: {full.relative_to(root_resolved).as_posix()}"
                )
            if full.is_dir():
                continue
            if not full.is_file():
                raise CapsuleError(
                    f"package contains non-regular entry: {full.relative_to(root_resolved).as_posix()}"
                )
            rel = full.relative_to(root_resolved).as_posix()
            files.add(rel)
    return files


def load_capsule(path: str | Path, *, schemas: SchemaSet | None = None) -> Capsule:
    """Load a Capsule directory and verify every stream digest and length."""
    root = Path(path)
    _enumerate_package_files(root)
    manifest_path = _resolve_package_path(root, "manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if schemas is not None:
        try:
            schemas.validate(SCHEMA_CAPSULE, manifest, what="capsule manifest")
        except CcfSchemaError as exc:
            raise CapsuleError(str(exc)) from exc
    if manifest.get("format") != "ccf.capsule/0.2.0":
        raise CapsuleError(f"unsupported capsule format: {manifest.get('format')!r}")

    stream_paths = [stream["path"] for stream in manifest["streams"]]
    if len(stream_paths) != len(set(stream_paths)):
        raise CapsuleError("duplicate capsule stream path")

    streams: list[CapsuleStream] = []
    for spec in manifest["streams"]:
        rel = spec["path"]
        stream_path = _resolve_package_path(root, rel)
        data = stream_path.read_bytes()
        if raw_digest(data) != spec["digest"]:
            raise CapsuleError(f"capsule stream digest mismatch: {rel}")
        if str(len(data)) != spec["byte_length"]:
            raise CapsuleError(f"capsule stream byte_length mismatch: {rel}")
        values: list[object] = []
        if spec["media_type"] == "application/x-ndjson":
            values = _read_ndjson(data)
        streams.append(CapsuleStream(spec=spec, data=data, values=values))
    return Capsule(root=root, manifest=manifest, streams=streams)


def verify_capsule(
    capsule: Capsule,
    *,
    layered: LayeredRegistries,
    schemas: SchemaSet,
    recipient_level: str,
    recipient_capabilities: list[str] | tuple[str, ...] = (),
) -> None:
    """Verify membership, references, activation, and unknown preservation."""
    schemas.validate(SCHEMA_CAPSULE, capsule.manifest, what="capsule manifest")
    layered.level(capsule.manifest["level"])
    unknown_features = set(capsule.manifest["capabilities"]) - layered.known_feature_ids()
    if unknown_features:
        raise CapsuleError(
            f"capsule declares unknown features: {sorted(unknown_features)}"
        )
    layered.features_fit_level(
        capsule.manifest["level"], capsule.manifest["capabilities"]
    )

    submissions: list[dict] = []
    for stream in capsule.streams:
        may_activate = layered.stream_may_activate(
            stream.spec,
            level_id=recipient_level,
            capabilities=recipient_capabilities,
        )
        if stream.handling == "activate" and not may_activate:
            raise CapsuleError(
                f"activate stream exceeds recipient: {stream.path}"
            )
        if stream.content_role == "submissions" and stream.handling != "activate":
            raise CapsuleError(
                f"submissions stream must use handling=activate: {stream.path}"
            )
        if stream.content_role == "submissions" and stream.handling == "activate":
            for value in stream.values:
                if not isinstance(value, dict):
                    raise CapsuleError(f"non-object submission in {stream.path}")
                submissions.append(value)
        if stream.handling == "preserve_opaque":
            # Byte-exact preservation is the digest check in load_capsule.
            continue

    ids = [submission["id"] for submission in submissions]
    if len(ids) != len(set(ids)):
        raise CapsuleError("duplicate object ID in capsule")
    root_id = capsule.manifest["root_record_id"]
    if root_id not in ids:
        raise CapsuleError("capsule root Record is absent")

    declared = set(ids) | {
        dependency["object_id"] for dependency in capsule.manifest["dependencies"]
    }
    membership_types = set(capsule.manifest["membership_link_types"])
    member_ids = set(ids) - {root_id}
    membership_targets: set[str] = set()
    for submission in submissions:
        kind = submission.get("submission_kind")
        schema_id = _SUBMISSION_SCHEMAS.get(kind)
        if schema_id is None:
            raise CapsuleError(f"unknown submission kind: {kind!r}")
        schemas.validate(schema_id, submission, what=f"submission {submission['id']}")
        try:
            requirement = layered.requirement_for_submission(submission)
        except LayeredError as exc:
            raise CapsuleError(str(exc)) from exc
        if stream_for_id(capsule, submission["id"]).handling == "activate":
            if not layered.can_activate(
                requirement,
                level_id=recipient_level,
                capabilities=recipient_capabilities,
            ):
                raise CapsuleError(
                    f"cannot activate {submission['type']} {submission['id']}"
                )
        if kind == "link":
            for endpoint in (submission["from_id"], submission["to_id"]):
                if endpoint not in declared:
                    raise CapsuleError(f"capsule Link has undeclared endpoint {endpoint}")
            if submission["type"] in membership_types and submission["to_id"] == root_id:
                membership_targets.add(submission["from_id"])
            if submission["type"] in membership_types and submission["from_id"] == root_id:
                membership_targets.add(submission["to_id"])

    link_ids = {
        submission["id"]
        for submission in submissions
        if submission["submission_kind"] == "link"
    }
    if not member_ids - link_ids <= membership_targets:
        raise CapsuleError(
            "capsule object is not connected to the root by a membership Link"
        )


def stream_for_id(capsule: Capsule, object_id: str) -> CapsuleStream:
    for stream in capsule.streams:
        if stream.content_role != "submissions":
            continue
        for value in stream.values:
            if isinstance(value, dict) and value.get("id") == object_id:
                return stream
    raise CapsuleError(f"no stream contains {object_id}")


def write_capsule(
    out_dir: str | Path,
    *,
    manifest: dict,
    submission_streams: dict[str, list[dict]],
    opaque_streams: dict[str, bytes] | None = None,
    schemas: SchemaSet | None = None,
) -> Capsule:
    """Write a Capsule directory, filling stream digests from the bytes written."""
    root = Path(out_dir)

    # Validate the input manifest and every stream path for containment and
    # duplicates before creating the output directory so an invalid manifest or
    # escape leaves a previously absent ``out_dir`` absent.
    if schemas is not None:
        try:
            schemas.validate(SCHEMA_CAPSULE, manifest, what="input capsule manifest")
        except CcfSchemaError as exc:
            raise CapsuleError(str(exc)) from exc
    seen_paths: set[str] = set()
    for spec in manifest["streams"]:
        rel = spec["path"]
        if rel in seen_paths:
            raise CapsuleError(f"duplicate capsule stream path: {rel!r}")
        seen_paths.add(rel)
        _resolve_package_path(root, rel, must_exist=False, must_be_file=False)
        if spec["content_role"] == "submissions":
            if rel not in submission_streams:
                raise CapsuleError(f"missing submission stream values: {rel!r}")
        elif spec["handling"] == "preserve_opaque":
            if opaque_streams is None or rel not in opaque_streams:
                raise CapsuleError(f"missing opaque stream bytes: {rel}")
        else:
            raise CapsuleError(f"write_capsule does not emit {spec['content_role']}")

    root.mkdir(parents=True, exist_ok=True)
    streams = []
    for spec in manifest["streams"]:
        rel = spec["path"]
        path = root / rel
        if spec["content_role"] == "submissions":
            values = submission_streams[rel]
            data = _write_ndjson(path, values)
        elif spec["handling"] == "preserve_opaque":
            data = opaque_streams[rel]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        filled = dict(spec)
        filled["digest"] = raw_digest(data)
        filled["byte_length"] = str(len(data))
        streams.append(filled)
    document = dict(manifest)
    document["streams"] = streams
    if schemas is not None:
        schemas.validate(SCHEMA_CAPSULE, document, what="capsule manifest")
    (root / "manifest.json").write_text(
        json.dumps(document, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return load_capsule(root, schemas=schemas)


def submission_hashes_for(capsule: Capsule) -> dict[str, str]:
    """JCS submission hash for every activate submission."""
    return {
        submission["id"]: submission_hash(submission)
        for submission in capsule.submissions
    }


def parse_pack_id(pack_id: str) -> None:
    parsed = parse_id(pack_id)
    if parsed.kind != "pack":
        raise CapsuleError(f"capsule pack_id must be a pack URN: {pack_id!r}")
