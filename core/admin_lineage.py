"""Read-only artifact and wiki lineage views for the admin console."""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .admin_status_utils import (
    RECENT_LIMIT,
    dt_text,
    min_datetime,
    parse_datetime,
    safe_reason,
)
from .capture_event_store import ArtifactLink, CaptureEvent, RawArtifactRef
from .config import Config
from .metadata_db import IngestionQueueEntry, MetadataDB
from .path_layout import PathLayout
from .wiki_change_provenance import resolve_source_path
from .wiki_contract import build_wiki_contract, is_legacy_tweet_slug
from .wiki_io import read_frontmatter


def wiki_lineage_status(
    config: Config,
    *,
    layout: PathLayout,
    db: MetadataDB,
    project_root: Path,
    event_store: Any | None = None,
    limit: int = RECENT_LIMIT,
) -> dict[str, Any]:
    """Build a read-only lineage index from wiki frontmatter and local stores."""

    try:
        contract = build_wiki_contract(config, project_root=project_root)
        pages_dir = contract.pages_dir
        page_paths = sorted(pages_dir.glob("*.md"))
    except Exception as exc:
        return {
            "status": "error",
            "pages_dir": str(layout.wiki_root / "pages"),
            "total_pages": 0,
            "pages_with_lineage": 0,
            "recent_pages": [],
            "by_page_type": {},
            "relation_counts": _empty_relation_counts(),
            "errors": [{"section": "lineage", "reason": safe_reason(exc)}],
        }

    errors: list[dict[str, str]] = []
    pages: list[dict[str, Any]] = []
    for page_path in page_paths:
        try:
            frontmatter = read_frontmatter(page_path)
            slug = _page_slug(frontmatter, page_path)
            if is_legacy_tweet_slug(slug):
                continue
            pages.append(
                _page_lineage(
                    page_path,
                    frontmatter,
                    layout=layout,
                    db=db,
                    event_store=event_store,
                )
            )
        except Exception as exc:
            errors.append(
                {
                    "section": "lineage",
                    "reason": f"{page_path.name}: {safe_reason(exc) or 'unknown error'}",
                }
            )

    pages.sort(
        key=lambda item: parse_datetime(item.get("updated_at")) or min_datetime(),
        reverse=True,
    )
    pages_with_lineage = [page for page in pages if page.get("has_lineage")]
    relation_counts = Counter()
    for page in pages:
        summary = page.get("relation_summary") or {}
        relation_counts["local_files"] += int(summary.get("local_files") or 0)
        relation_counts["capture_events"] += int(summary.get("capture_events") or 0)
        relation_counts["raw_refs"] += int(summary.get("raw_refs") or 0)
        relation_counts["artifacts"] += int(summary.get("artifacts") or 0)
        relation_counts["semantic_candidates"] += int(
            summary.get("semantic_candidates") or 0
        )

    status = "ok" if not errors else "degraded"
    return {
        "status": status,
        "pages_dir": str(pages_dir),
        "capture_store": "available" if event_store is not None else "unavailable",
        "total_pages": len(pages),
        "pages_with_lineage": len(pages_with_lineage),
        "recent_pages": pages[: max(1, int(limit))],
        "by_page_type": dict(
            sorted(
                Counter(str(page.get("page_type") or "wiki") for page in pages).items()
            )
        ),
        "relation_counts": {
            **_empty_relation_counts(),
            **dict(sorted(relation_counts.items())),
        },
        "errors": errors,
    }


def _page_lineage(
    page_path: Path,
    frontmatter: Mapping[str, Any],
    *,
    layout: PathLayout,
    db: MetadataDB,
    event_store: Any | None,
) -> dict[str, Any]:
    source_paths = _frontmatter_sequence(
        frontmatter,
        "thoth_source_paths",
        "source_paths",
    )
    input_manifest = _frontmatter_mapping_sequence(
        frontmatter,
        "thoth_input_manifest",
        "input_manifest",
    )
    influence_sources = _frontmatter_mapping_sequence(
        frontmatter,
        "thoth_influence_sources",
        "influence_sources",
    )
    event_ids = _stable_strings(
        (
            *_frontmatter_sequence(frontmatter, "thoth_event_ids"),
            *(
                str(record.get("event_id"))
                for record in input_manifest
                if record.get("event_id")
            ),
            *(
                str(record.get("capture_event_id"))
                for record in influence_sources
                if record.get("capture_event_id")
            ),
        )
    )
    semantic_candidate_ids = _stable_strings(
        (
            *_frontmatter_sequence(frontmatter, "thoth_semantic_candidate_ids"),
            *(
                str(record.get("semantic_candidate_id"))
                for record in influence_sources
                if record.get("semantic_candidate_id")
            ),
        )
    )
    local_files = _local_file_payloads(
        layout,
        source_paths=source_paths,
        input_manifest=input_manifest,
        influence_sources=influence_sources,
    )
    capture_events = _capture_event_payloads(
        event_ids,
        input_manifest=input_manifest,
        influence_sources=influence_sources,
        event_store=event_store,
    )
    raw_refs = _raw_ref_payloads(
        event_ids,
        input_manifest=input_manifest,
        influence_sources=influence_sources,
        event_store=event_store,
    )
    artifact_keys = _artifact_keys(
        frontmatter,
        influence_sources=influence_sources,
        input_manifest=input_manifest,
        capture_events=capture_events,
        raw_refs=raw_refs,
        event_store=event_store,
    )
    artifacts = _artifact_payloads(artifact_keys, db=db)
    semantic_candidates = _semantic_candidate_payloads(
        semantic_candidate_ids,
        db=db,
    )
    change_provenance = (
        dict(frontmatter["thoth_change_provenance"])
        if isinstance(frontmatter.get("thoth_change_provenance"), Mapping)
        else None
    )
    relation_summary = {
        "local_files": len(local_files),
        "capture_events": len(capture_events),
        "raw_refs": len(raw_refs),
        "artifacts": len(artifacts),
        "semantic_candidates": len(semantic_candidates),
    }
    return _compact(
        {
            "slug": _page_slug(frontmatter, page_path),
            "title": _text(frontmatter.get("title")) or page_path.stem,
            "page_path": str(page_path),
            "record_type": _text(frontmatter.get("thoth_type"))
            or _text(frontmatter.get("record_type"))
            or "wiki_page",
            "kind": _text(frontmatter.get("thoth_kind"))
            or _text(frontmatter.get("kind"))
            or "topic",
            "page_type": _page_type(frontmatter),
            "updated_at": dt_text(
                frontmatter.get("thoth_updated_at")
                or frontmatter.get("updated_at")
                or frontmatter.get("timestamp")
            ),
            "why_changed": _why_changed(change_provenance),
            "change_provenance": _change_payload(change_provenance),
            "input_hash": _text(
                frontmatter.get("thoth_input_hash") or frontmatter.get("input_hash")
            ),
            "input_count": len(input_manifest),
            "source_paths": list(source_paths),
            "semantic_evidence_ids": list(
                _frontmatter_sequence(frontmatter, "thoth_semantic_evidence_ids")
            ),
            "relation_summary": relation_summary,
            "local_files": local_files,
            "capture_events": capture_events,
            "raw_refs": raw_refs,
            "artifacts": artifacts,
            "semantic_candidates": semantic_candidates,
            "has_lineage": any(relation_summary.values())
            or bool(input_manifest)
            or bool(change_provenance),
        }
    )


def _local_file_payloads(
    layout: PathLayout,
    *,
    source_paths: Sequence[str],
    input_manifest: Sequence[Mapping[str, Any]],
    influence_sources: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_source_path: dict[str, dict[str, Any]] = {}
    for record in input_manifest:
        source_path = _text(record.get("source_path"))
        if source_path:
            by_source_path.setdefault(source_path, dict(record))
    for record in influence_sources:
        source_path = _text(record.get("source_path"))
        if source_path:
            by_source_path.setdefault(source_path, dict(record))
    for source_path in source_paths:
        by_source_path.setdefault(source_path, {"source_path": source_path})

    payloads: list[dict[str, Any]] = []
    for source_path, record in sorted(by_source_path.items()):
        resolved = resolve_source_path(layout, source_path)
        payload = {
            "source_path": source_path,
            "local_path": str(resolved) if resolved is not None else None,
            "exists": bool(resolved and resolved.exists()),
            "input_id": record.get("input_id"),
            "input_kind": record.get("input_kind") or "source_file",
            "artifact_id": record.get("artifact_id"),
            "artifact_type": record.get("artifact_type"),
            "event_id": record.get("event_id") or record.get("capture_event_id"),
            "raw_ref_id": record.get("raw_ref_id"),
            "sha256": record.get("sha256"),
            "recorded_sha256": record.get("recorded_sha256"),
            "size_bytes": record.get("size_bytes") or record.get("recorded_size_bytes"),
            "modified_at": dt_text(record.get("modified_at")),
            "missing": bool(record.get("missing")),
        }
        payloads.append(_compact(payload))
    return payloads


def _capture_event_payloads(
    event_ids: Sequence[str],
    *,
    input_manifest: Sequence[Mapping[str, Any]],
    influence_sources: Sequence[Mapping[str, Any]],
    event_store: Any | None,
) -> list[dict[str, Any]]:
    manifest_by_event = {
        str(record.get("event_id")): dict(record)
        for record in input_manifest
        if record.get("event_id") and record.get("input_kind") == "capture_event"
    }
    influence_by_event = {
        str(record.get("capture_event_id")): dict(record)
        for record in influence_sources
        if record.get("capture_event_id")
    }
    payloads: list[dict[str, Any]] = []
    for event_id in event_ids:
        event = _get_capture_event(event_store, event_id)
        raw_refs = _list_raw_refs(event_store, event_id=event_id)
        links = _list_artifact_links(event_store, event_id=event_id)
        record = {
            **manifest_by_event.get(event_id, {}),
            **influence_by_event.get(event_id, {}),
        }
        if isinstance(event, CaptureEvent):
            payload = {
                "event_id": event.event_id,
                "source_id": event.source_id,
                "session_id": event.session_id,
                "native_event_id": event.native_event_id,
                "event_type": event.event_type,
                "status": event.status,
                "occurred_at": dt_text(event.occurred_at),
                "captured_at": dt_text(event.captured_at),
                "event_hash": event.event_hash,
                "raw_ref_ids": [raw_ref.raw_ref_id for raw_ref in raw_refs],
                "artifact_ids": [link.artifact_id for link in links if link.artifact_id],
            }
        else:
            payload = {
                "event_id": event_id,
                "source_id": record.get("source_id"),
                "session_id": record.get("session_id"),
                "event_type": record.get("event_type"),
                "status": record.get("status") or "unavailable",
                "event_hash": record.get("event_hash"),
                "raw_ref_ids": [
                    str(item.get("raw_ref_id"))
                    for item in input_manifest
                    if item.get("event_id") == event_id and item.get("raw_ref_id")
                ],
                "artifact_ids": [],
            }
        payloads.append(_compact(payload))
    return payloads


def _raw_ref_payloads(
    event_ids: Sequence[str],
    *,
    input_manifest: Sequence[Mapping[str, Any]],
    influence_sources: Sequence[Mapping[str, Any]],
    event_store: Any | None,
) -> list[dict[str, Any]]:
    raw_records: dict[str, dict[str, Any]] = {}
    for record in input_manifest:
        raw_ref_id = _text(record.get("raw_ref_id"))
        if raw_ref_id:
            raw_records.setdefault(raw_ref_id, dict(record))
    for record in influence_sources:
        raw_ref_id = _text(record.get("raw_ref_id"))
        if raw_ref_id:
            raw_records.setdefault(raw_ref_id, dict(record))

    for event_id in event_ids:
        for raw_ref in _list_raw_refs(event_store, event_id=event_id):
            raw_records[raw_ref.raw_ref_id] = _raw_ref_record(raw_ref)

    payloads: list[dict[str, Any]] = []
    for raw_ref_id, record in sorted(raw_records.items()):
        payloads.append(
            _compact(
                {
                    "raw_ref_id": raw_ref_id,
                    "event_id": record.get("event_id"),
                    "source_id": record.get("source_id"),
                    "session_id": record.get("session_id"),
                    "source_path": record.get("source_path"),
                    "path": record.get("path"),
                    "sha256": record.get("sha256"),
                    "recorded_sha256": record.get("recorded_sha256"),
                    "size_bytes": record.get("size_bytes")
                    or record.get("recorded_size_bytes"),
                    "mime_type": record.get("mime_type"),
                    "updated_at": dt_text(record.get("updated_at")),
                }
            )
        )
    return payloads


def _artifact_keys(
    frontmatter: Mapping[str, Any],
    *,
    influence_sources: Sequence[Mapping[str, Any]],
    input_manifest: Sequence[Mapping[str, Any]],
    capture_events: Sequence[Mapping[str, Any]],
    raw_refs: Sequence[Mapping[str, Any]],
    event_store: Any | None,
) -> list[tuple[str, str | None, str | None]]:
    keys: dict[tuple[str, str | None, str | None], None] = {}

    artifact_id = _text(frontmatter.get("thoth_artifact_id"))
    source_type = _text(frontmatter.get("thoth_source_type"))
    if artifact_id:
        keys[(artifact_id, None, source_type)] = None
    for value in _frontmatter_sequence(frontmatter, "thoth_artifact_ids"):
        keys[(value, None, source_type)] = None

    for record in (*influence_sources, *input_manifest):
        record_artifact_id = _text(record.get("artifact_id"))
        if record_artifact_id:
            keys[
                (
                    record_artifact_id,
                    _text(record.get("artifact_type")),
                    _text(record.get("source_type")) or source_type,
                )
            ] = None

    for event in capture_events:
        for linked_id in event.get("artifact_ids") or ():
            if linked_id:
                keys[(str(linked_id), None, source_type)] = None

    for event_id in _stable_strings(event.get("event_id") for event in capture_events):
        for link in _list_artifact_links(event_store, event_id=event_id):
            keys[(link.artifact_id, link.artifact_type, source_type)] = None

    all_links_by_raw_ref: dict[str, list[ArtifactLink]] | None = None
    for raw_ref in raw_refs:
        raw_ref_id = _text(raw_ref.get("raw_ref_id"))
        if not raw_ref_id:
            continue
        if all_links_by_raw_ref is None:
            all_links_by_raw_ref = {}
            for link in _list_artifact_links(event_store):
                if link.raw_ref_id:
                    all_links_by_raw_ref.setdefault(link.raw_ref_id, []).append(link)
        for link in all_links_by_raw_ref.get(raw_ref_id, ()):
            if link.raw_ref_id == raw_ref_id:
                keys[(link.artifact_id, link.artifact_type, source_type)] = None

    return list(keys)


def _artifact_payloads(
    artifact_keys: Sequence[tuple[str, str | None, str | None]],
    *,
    db: MetadataDB,
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for artifact_id, artifact_type, source_type in artifact_keys:
        entry = db.get_ingestion_entry(artifact_id)
        canonical_link = db.get_canonical_link_for_artifact(
            artifact_id,
            artifact_type=artifact_type or (entry.artifact_type if entry else None),
            source_type=source_type or (entry.source if entry else None),
        )
        canonical = (
            db.get_canonical_entity(canonical_link.canonical_id)
            if canonical_link is not None
            else None
        )
        payload = {
            "artifact_id": artifact_id,
            "artifact_type": artifact_type or (entry.artifact_type if entry else None),
            "source": entry.source if entry else source_type,
            "status": entry.status if entry else "unavailable",
            "created_at": dt_text(entry.created_at if entry else None),
            "processed_at": dt_text(entry.processed_at if entry else None),
            "attempts": entry.attempts if entry else None,
            "last_error": safe_reason(entry.last_error if entry else None),
            "canonical_id": canonical_link.canonical_id if canonical_link else None,
            "canonical_entity_type": canonical_link.entity_type
            if canonical_link
            else None,
            "canonical_wiki_slug": canonical.wiki_slug if canonical else None,
            "display_name": canonical.display_name if canonical else None,
            "payload_summary": _ingestion_payload_summary(entry) if entry else None,
        }
        payloads.append(_compact(payload))
    payloads.sort(
        key=lambda item: (
            str(item.get("artifact_type") or ""),
            str(item.get("artifact_id") or ""),
        )
    )
    return payloads


def _semantic_candidate_payloads(
    candidate_ids: Sequence[str],
    *,
    db: MetadataDB,
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for candidate_id in candidate_ids:
        record = _read_semantic_candidate(db, candidate_id)
        if record is None:
            payloads.append(
                {
                    "candidate_id": candidate_id,
                    "status": "unavailable",
                    "evidence": [],
                    "evidence_count": 0,
                }
            )
        else:
            payloads.append(record)
    return payloads


def _read_semantic_candidate(
    db: MetadataDB,
    candidate_id: str,
) -> dict[str, Any] | None:
    try:
        with db._get_connection() as conn:
            table = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name = 'semantic_memory_candidates'
                """
            ).fetchone()
            if table is None:
                return None
            row = conn.execute(
                """
                SELECT candidate_id, candidate_type, status, text, entity_id,
                       entity_type, entity_name, confidence, privacy_class,
                       updated_at, status_updated_at
                FROM semantic_memory_candidates
                WHERE candidate_id = ?
                """,
                (candidate_id,),
            ).fetchone()
            if row is None:
                return None
            evidence_rows = conn.execute(
                """
                SELECT evidence_id, artifact_id, artifact_type, capture_event_id,
                       source_path, source_timestamp, confidence, privacy_class,
                       created_at, updated_at
                FROM semantic_memory_evidence
                WHERE candidate_id = ?
                ORDER BY COALESCE(source_timestamp, created_at) DESC, evidence_id
                """,
                (candidate_id,),
            ).fetchall()
    except Exception:
        return None

    evidence = [
        _compact(
            {
                "evidence_id": item["evidence_id"],
                "artifact_id": item["artifact_id"],
                "artifact_type": item["artifact_type"],
                "capture_event_id": item["capture_event_id"],
                "source_path": item["source_path"],
                "source_timestamp": dt_text(item["source_timestamp"]),
                "confidence": item["confidence"],
                "privacy_class": item["privacy_class"],
                "created_at": dt_text(item["created_at"]),
                "updated_at": dt_text(item["updated_at"]),
            }
        )
        for item in evidence_rows
    ]
    return _compact(
        {
            "candidate_id": row["candidate_id"],
            "candidate_type": row["candidate_type"],
            "status": row["status"],
            "text": row["text"],
            "entity_id": row["entity_id"],
            "entity_type": row["entity_type"],
            "entity_name": row["entity_name"],
            "confidence": row["confidence"],
            "privacy_class": row["privacy_class"],
            "updated_at": dt_text(row["updated_at"]),
            "status_updated_at": dt_text(row["status_updated_at"]),
            "evidence_count": len(evidence),
            "evidence": evidence,
        }
    )


def _get_capture_event(event_store: Any | None, event_id: str) -> CaptureEvent | None:
    if event_store is None:
        return None
    try:
        event = event_store.get_event(event_id)
    except Exception:
        return None
    return event if isinstance(event, CaptureEvent) else None


def _list_raw_refs(
    event_store: Any | None,
    *,
    event_id: str | None = None,
) -> tuple[RawArtifactRef, ...]:
    if event_store is None:
        return ()
    try:
        return tuple(event_store.list_raw_refs(event_id=event_id))
    except Exception:
        return ()


def _list_artifact_links(
    event_store: Any | None,
    *,
    event_id: str | None = None,
) -> tuple[ArtifactLink, ...]:
    if event_store is None:
        return ()
    try:
        return tuple(event_store.list_artifact_links(event_id=event_id))
    except Exception:
        return ()


def _raw_ref_record(raw_ref: RawArtifactRef) -> dict[str, Any]:
    return {
        "raw_ref_id": raw_ref.raw_ref_id,
        "event_id": raw_ref.event_id,
        "source_id": raw_ref.source_id,
        "session_id": raw_ref.session_id,
        "path": raw_ref.path,
        "sha256": raw_ref.sha256,
        "size_bytes": raw_ref.size_bytes,
        "mime_type": raw_ref.mime_type,
        "updated_at": raw_ref.updated_at,
    }


def _ingestion_payload_summary(entry: IngestionQueueEntry) -> dict[str, Any]:
    try:
        payload = json.loads(entry.payload_json)
    except Exception:
        return {}
    if not isinstance(payload, Mapping):
        return {}
    summary = {
        key: payload.get(key)
        for key in (
            "id",
            "title",
            "name",
            "repo_name",
            "arxiv_id",
            "source_url",
            "url",
            "source_path",
        )
        if payload.get(key) not in (None, "", [], {})
    }
    custom_metadata = payload.get("custom_metadata")
    if isinstance(custom_metadata, Mapping):
        for key in ("source_path", "raw_payload_path", "original_path"):
            if key in custom_metadata:
                summary[key] = custom_metadata[key]
    return _compact(summary)


def _change_payload(
    change_provenance: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(change_provenance, Mapping):
        return None
    changes = [
        dict(change)
        for change in change_provenance.get("changes") or []
        if isinstance(change, Mapping)
    ]
    return _compact(
        {
            "compiled_at": dt_text(change_provenance.get("compiled_at")),
            "reason": change_provenance.get("reason"),
            "input_hash_before": change_provenance.get("input_hash_before"),
            "input_hash_after": change_provenance.get("input_hash_after"),
            "change_count": len(changes),
            "changes": changes[:5],
        }
    )


def _why_changed(change_provenance: Mapping[str, Any] | None) -> str:
    if not isinstance(change_provenance, Mapping):
        return "No input change provenance is recorded for this page."
    reason = str(change_provenance.get("reason") or "").strip()
    changes = [
        change
        for change in change_provenance.get("changes") or []
        if isinstance(change, Mapping)
    ]
    if reason == "initial_compile":
        return "Initial compile from recorded inputs."
    if reason == "inputs_unchanged":
        return "Inputs matched the previous recorded hash."
    if changes:
        first_reason = safe_reason(changes[0].get("reason")) or reason or "inputs changed"
        if len(changes) > 1:
            return f"{first_reason} (+{len(changes) - 1} more input change(s))"
        return first_reason
    if reason:
        return reason.replace("_", " ")
    return "Input change provenance is present but did not include a reason."


def _page_type(frontmatter: Mapping[str, Any]) -> str:
    for key in (
        "thoth_capture_page_type",
        "thoth_semantic_page_type",
        "thoth_type",
        "record_type",
    ):
        value = _text(frontmatter.get(key))
        if value:
            if key == "thoth_capture_page_type":
                return f"capture:{value}"
            if key == "thoth_semantic_page_type":
                return f"semantic:{value}"
            return value
    return "wiki"


def _page_slug(frontmatter: Mapping[str, Any], page_path: Path) -> str:
    return (
        _text(frontmatter.get("thoth_slug"))
        or _text(frontmatter.get("slug"))
        or page_path.stem
    )


def _frontmatter_sequence(frontmatter: Mapping[str, Any], *keys: str) -> tuple[str, ...]:
    for key in keys:
        value = frontmatter.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            return _stable_strings((value,))
        if isinstance(value, Mapping):
            return ()
        if isinstance(value, Iterable):
            return _stable_strings(str(item) for item in value)
    return ()


def _frontmatter_mapping_sequence(
    frontmatter: Mapping[str, Any],
    *keys: str,
) -> tuple[dict[str, Any], ...]:
    for key in keys:
        value = frontmatter.get(key)
        if value is None:
            continue
        if isinstance(value, Mapping) or isinstance(value, str):
            return ()
        if isinstance(value, Iterable):
            return tuple(dict(item) for item in value if isinstance(item, Mapping))
    return ()


def _stable_strings(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in values if str(value).strip()}))


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _compact(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in payload.items()
        if value not in (None, "", [], {}, ())
    }


def _empty_relation_counts() -> dict[str, int]:
    return {
        "local_files": 0,
        "capture_events": 0,
        "raw_refs": 0,
        "artifacts": 0,
        "semantic_candidates": 0,
    }
