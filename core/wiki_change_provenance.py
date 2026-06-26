"""Input hashing and change provenance helpers for compiled wiki pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .capture_event_store import CaptureEvent, CaptureEventStore, RawArtifactRef
from .path_layout import PathLayout


InputRecord = dict[str, Any]


@dataclass(frozen=True)
class WikiInputSnapshot:
    """Deterministic record of wiki compiler inputs."""

    input_hash: str
    input_manifest: tuple[InputRecord, ...]


@dataclass(frozen=True)
class WikiInputChange:
    """Single input-level provenance diff."""

    change_type: str
    input_id: str
    input_kind: str | None
    reason: str
    previous: Mapping[str, Any] | None = None
    current: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "change_type": self.change_type,
            "input_id": self.input_id,
            "input_kind": self.input_kind,
            "reason": self.reason,
            "previous": dict(self.previous) if self.previous is not None else None,
            "current": dict(self.current) if self.current is not None else None,
        }
        return {key: value for key, value in payload.items() if value is not None}


def source_file_snapshot(
    layout: PathLayout,
    source_paths: Sequence[str],
    *,
    source_type: str | None = None,
    artifact_id: str | None = None,
) -> WikiInputSnapshot:
    """Snapshot local source files referenced by a compiled wiki page."""

    records: list[InputRecord] = []
    for source_path in _stable_unique_strings(source_paths):
        record: InputRecord = {
            "input_id": f"source_path:{source_path}",
            "input_kind": "source_file",
            "source_path": source_path,
            "source_type": source_type,
            "artifact_id": artifact_id,
        }
        path = resolve_source_path(layout, source_path)
        if path is None or not path.exists() or not path.is_file():
            record["missing"] = True
        else:
            stat = path.stat()
            record.update(
                {
                    "sha256": sha256_file(path),
                    "size_bytes": stat.st_size,
                    "modified_at": _datetime_from_timestamp(stat.st_mtime),
                }
            )
        records.append(_compact_record(record))
    return snapshot_from_manifest(records)


def capture_records_snapshot(
    records: Sequence[Any],
    *,
    layout: PathLayout,
) -> WikiInputSnapshot:
    """Snapshot capture events and raw artifact refs used by a capture wiki page."""

    manifest: list[InputRecord] = []
    for record in records:
        event = getattr(record, "event", None)
        if isinstance(event, CaptureEvent):
            manifest.append(capture_event_input_record(event))
        for raw_ref in getattr(record, "raw_refs", ()) or ():
            if isinstance(raw_ref, RawArtifactRef):
                manifest.append(raw_ref_input_record(raw_ref, layout=layout))
    return snapshot_from_manifest(manifest)


def capture_event_store_snapshot(
    event_store: CaptureEventStore | None,
    event_ids: Sequence[str],
    *,
    layout: PathLayout,
) -> WikiInputSnapshot:
    """Recompute capture event input records from the durable event store."""

    manifest: list[InputRecord] = []
    for event_id in _stable_unique_strings(event_ids):
        if event_store is None:
            manifest.append(
                {
                    "input_id": f"capture_event:{event_id}",
                    "input_kind": "capture_event",
                    "event_id": event_id,
                    "missing": True,
                    "unavailable": "capture_event_store",
                }
            )
            continue
        event = event_store.get_event(event_id)
        if event is None:
            manifest.append(
                {
                    "input_id": f"capture_event:{event_id}",
                    "input_kind": "capture_event",
                    "event_id": event_id,
                    "missing": True,
                }
            )
            continue
        manifest.append(capture_event_input_record(event))
        for raw_ref in event_store.list_raw_refs(event_id=event_id):
            manifest.append(raw_ref_input_record(raw_ref, layout=layout))
    return snapshot_from_manifest(manifest)


def capture_event_input_record(event: CaptureEvent) -> InputRecord:
    """Build a deterministic hash record for one capture event row."""

    event_payload = {
        "event_id": event.event_id,
        "source_id": event.source_id,
        "session_id": event.session_id,
        "native_event_id": event.native_event_id,
        "event_type": event.event_type,
        "status": event.status,
        "occurred_at": _json_safe(event.occurred_at),
        "captured_at": _json_safe(event.captured_at),
        "event_hash": event.event_hash,
        "payload": _json_safe(event.payload),
        "privacy": _json_safe(event.privacy),
        "retention": _json_safe(event.retention),
        "provenance": _json_safe(event.provenance),
        "created_at": _json_safe(event.created_at),
        "updated_at": _json_safe(event.updated_at),
    }
    return _compact_record(
        {
            "input_id": f"capture_event:{event.event_id}",
            "input_kind": "capture_event",
            "event_id": event.event_id,
            "source_id": event.source_id,
            "session_id": event.session_id,
            "native_event_id": event.native_event_id,
            "event_type": event.event_type,
            "status": event.status,
            "event_hash": event.event_hash,
            "sha256": stable_hash(event_payload),
            "updated_at": _json_safe(event.updated_at),
        }
    )


def raw_ref_input_record(raw_ref: RawArtifactRef, *, layout: PathLayout) -> InputRecord:
    """Build a deterministic hash record for one raw artifact ref and file."""

    source_path = _source_path_for_raw_ref(raw_ref, layout=layout)
    record: InputRecord = {
        "input_id": f"raw_ref:{raw_ref.raw_ref_id}",
        "input_kind": "raw_ref",
        "raw_ref_id": raw_ref.raw_ref_id,
        "event_id": raw_ref.event_id,
        "source_id": raw_ref.source_id,
        "session_id": raw_ref.session_id,
        "source_path": source_path,
        "recorded_sha256": raw_ref.sha256,
        "recorded_size_bytes": raw_ref.size_bytes,
        "mime_type": raw_ref.mime_type,
        "updated_at": _json_safe(raw_ref.updated_at),
    }
    path = Path(raw_ref.path)
    if not path.exists() or not path.is_file():
        record["missing"] = True
    else:
        stat = path.stat()
        record.update(
            {
                "sha256": sha256_file(path),
                "size_bytes": stat.st_size,
                "modified_at": _datetime_from_timestamp(stat.st_mtime),
            }
        )
    return _compact_record(record)


def snapshot_from_manifest(records: Iterable[Mapping[str, Any]]) -> WikiInputSnapshot:
    """Normalize input records and return their aggregate hash."""

    manifest = tuple(
        sorted(
            (_compact_record(_stable_value(record)) for record in records),
            key=lambda item: str(item.get("input_id") or ""),
        )
    )
    return WikiInputSnapshot(
        input_hash=stable_hash(manifest),
        input_manifest=manifest,
    )


def change_provenance(
    *,
    previous_hash: str | None,
    previous_manifest: Sequence[Mapping[str, Any]] | None,
    current_snapshot: WikiInputSnapshot,
    compiled_at: str,
) -> dict[str, Any]:
    """Describe why a compiled wiki page's input snapshot changed."""

    changes = diff_input_manifests(
        previous_manifest or (),
        current_snapshot.input_manifest,
    )
    reason = "initial_compile"
    if previous_hash and previous_hash != current_snapshot.input_hash:
        reason = "inputs_changed"
    elif previous_hash == current_snapshot.input_hash:
        reason = "inputs_unchanged"
    elif previous_manifest:
        reason = "input_hash_added"

    payload: dict[str, Any] = {
        "compiled_at": compiled_at,
        "reason": reason,
        "input_hash_before": previous_hash,
        "input_hash_after": current_snapshot.input_hash,
        "changes": [change.to_dict() for change in changes],
    }
    return {key: value for key, value in payload.items() if value not in (None, [], {})}


def diff_input_manifests(
    previous_manifest: Sequence[Mapping[str, Any]],
    current_manifest: Sequence[Mapping[str, Any]],
) -> tuple[WikiInputChange, ...]:
    """Return deterministic input-level changes between two manifests."""

    previous_by_id = _manifest_by_id(previous_manifest)
    current_by_id = _manifest_by_id(current_manifest)
    changes: list[WikiInputChange] = []

    for input_id in sorted(set(previous_by_id) - set(current_by_id)):
        previous = previous_by_id[input_id]
        changes.append(
            WikiInputChange(
                change_type="removed",
                input_id=input_id,
                input_kind=_optional_text(previous.get("input_kind")),
                reason=f"Input {input_id} is no longer referenced.",
                previous=previous,
            )
        )

    for input_id in sorted(set(current_by_id) - set(previous_by_id)):
        current = current_by_id[input_id]
        changes.append(
            WikiInputChange(
                change_type="added",
                input_id=input_id,
                input_kind=_optional_text(current.get("input_kind")),
                reason=f"Input {input_id} is newly referenced.",
                current=current,
            )
        )

    for input_id in sorted(set(previous_by_id) & set(current_by_id)):
        previous = previous_by_id[input_id]
        current = current_by_id[input_id]
        field_changes = _changed_fields(previous, current)
        if not field_changes:
            continue
        input_kind = _optional_text(current.get("input_kind") or previous.get("input_kind"))
        reason = _change_reason(input_id, input_kind, field_changes)
        changes.append(
            WikiInputChange(
                change_type="changed",
                input_id=input_id,
                input_kind=input_kind,
                reason=reason,
                previous=previous,
                current=current,
            )
        )

    return tuple(changes)


def influence_with_input_hashes(
    influence_sources: Sequence[Mapping[str, Any]],
    snapshot: WikiInputSnapshot,
) -> tuple[dict[str, Any], ...]:
    """Attach input IDs and hashes to source-influence records."""

    by_source_path = {
        str(record.get("source_path")): record
        for record in snapshot.input_manifest
        if record.get("source_path")
    }
    by_event_id = {
        str(record.get("event_id")): record
        for record in snapshot.input_manifest
        if record.get("input_kind") == "capture_event" and record.get("event_id")
    }
    enriched: list[dict[str, Any]] = []
    for influence in influence_sources:
        record = dict(influence)
        input_record = None
        source_path = str(record.get("source_path") or "")
        event_id = str(record.get("event_id") or "")
        if source_path:
            input_record = by_source_path.get(source_path)
        if input_record is None and event_id:
            input_record = by_event_id.get(event_id)
        if input_record:
            record["input_id"] = input_record.get("input_id")
            record["sha256"] = input_record.get("sha256")
            for key in ("size_bytes", "modified_at", "event_hash"):
                if input_record.get(key) is not None:
                    record[key] = input_record[key]
        enriched.append(_compact_record(record))
    return tuple(enriched)


def capture_influence_sources(
    records: Sequence[Any],
    snapshot: WikiInputSnapshot,
) -> tuple[dict[str, Any], ...]:
    """Build compact influence records for capture wiki pages."""

    by_input_id = {
        str(record.get("input_id")): record for record in snapshot.input_manifest
    }
    influences: list[dict[str, Any]] = []
    for record in records:
        event = getattr(record, "event", None)
        source = getattr(record, "source", None)
        if isinstance(event, CaptureEvent):
            input_record = by_input_id.get(f"capture_event:{event.event_id}", {})
            influences.append(
                _compact_record(
                    {
                        "input_id": input_record.get("input_id"),
                        "event_id": event.event_id,
                        "source_id": event.source_id,
                        "source_type": getattr(source, "source_type", None),
                        "source_name": getattr(source, "source_name", None),
                        "event_type": event.event_type,
                        "sha256": input_record.get("sha256"),
                        "event_hash": input_record.get("event_hash"),
                    }
                )
            )
        for raw_ref in getattr(record, "raw_refs", ()) or ():
            if not isinstance(raw_ref, RawArtifactRef):
                continue
            input_record = by_input_id.get(f"raw_ref:{raw_ref.raw_ref_id}", {})
            influences.append(
                _compact_record(
                    {
                        "input_id": input_record.get("input_id"),
                        "raw_ref_id": raw_ref.raw_ref_id,
                        "event_id": raw_ref.event_id,
                        "source_path": input_record.get("source_path"),
                        "source_type": "raw_ref",
                        "sha256": input_record.get("sha256"),
                        "recorded_sha256": input_record.get("recorded_sha256"),
                        "size_bytes": input_record.get("size_bytes"),
                    }
                )
            )
    return tuple(influences)


def resolve_source_path(layout: PathLayout, source_path: str) -> Path | None:
    """Resolve a wiki source path to a local file path without trusting absolutes."""

    value = str(source_path or "").strip()
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return None

    vault_candidate = layout.vault_root / value
    if vault_candidate.exists():
        return vault_candidate

    parts = path.parts
    if parts and parts[0] == "raw":
        return layout.raw_root.joinpath(*parts[1:])
    if parts and parts[0] == "library":
        return layout.library_root.joinpath(*parts[1:])
    return vault_candidate


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            _stable_value(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _manifest_by_id(
    manifest: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for raw_record in manifest:
        if not isinstance(raw_record, Mapping):
            continue
        record = _compact_record(_stable_value(raw_record))
        input_id = _optional_text(record.get("input_id"))
        if input_id:
            records[input_id] = record
    return records


def _changed_fields(previous: Mapping[str, Any], current: Mapping[str, Any]) -> tuple[str, ...]:
    changed: list[str] = []
    for key in sorted(set(previous) | set(current)):
        if previous.get(key) != current.get(key):
            changed.append(str(key))
    return tuple(changed)


def _change_reason(
    input_id: str,
    input_kind: str | None,
    changed_fields: Sequence[str],
) -> str:
    field_set = set(changed_fields)
    if "missing" in field_set:
        return f"Input {input_id} availability changed."
    if "sha256" in field_set:
        if input_kind == "capture_event":
            return f"Capture event {input_id.removeprefix('capture_event:')} hash changed."
        if input_kind == "raw_ref":
            return f"Raw artifact {input_id.removeprefix('raw_ref:')} hash changed."
        return f"Source file {input_id.removeprefix('source_path:')} hash changed."
    if "event_hash" in field_set:
        return f"Capture event {input_id.removeprefix('capture_event:')} event_hash changed."
    return f"Input {input_id} metadata changed: {', '.join(changed_fields)}."


def _source_path_for_raw_ref(raw_ref: RawArtifactRef, *, layout: PathLayout) -> str | None:
    raw_path = Path(raw_ref.path)
    try:
        return raw_path.resolve(strict=False).relative_to(
            layout.vault_root.resolve(strict=False)
        ).as_posix()
    except ValueError:
        pass
    try:
        return "raw/" + raw_path.resolve(strict=False).relative_to(
            layout.raw_root.resolve(strict=False)
        ).as_posix()
    except ValueError:
        return None


def _compact_record(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in record.items()
        if value not in (None, "", [], {}, ())
    }


def _stable_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _stable_value(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple, set)):
        normalized = [_stable_value(item) for item in value]
        return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True, default=str))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _stable_unique_strings(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in values if str(value).strip()}))


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _datetime_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace(
        "+00:00",
        "Z",
    )
