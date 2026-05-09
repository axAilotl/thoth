"""Structured plan payloads for agent-safe CLI mutation previews."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from .path_layout import build_path_layout


def counts_by(records: list[Any], attr_name: str) -> dict[str, int]:
    return dict(
        sorted(
            Counter(str(getattr(record, attr_name, "")) for record in records).items()
        )
    )


def web_clipper_record_payload(record: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "path": str(record.path),
        "root": str(record.root),
        "source_id": record.source_id,
        "file_type": record.file_type,
        "size_bytes": record.size_bytes,
        "sha256": record.sha256,
        "is_new_or_changed": record.is_new_or_changed,
    }
    for attr_name in ("action", "would_queue", "would_stage", "updated_at"):
        if hasattr(record, attr_name):
            payload[attr_name] = getattr(record, attr_name)
    managed_path = getattr(record, "managed_path", None)
    if managed_path is not None:
        payload["managed_path"] = str(managed_path)
    artifact = getattr(record, "artifact", None)
    if artifact is not None:
        payload["artifact"] = {
            "id": getattr(artifact, "id", ""),
            "source_type": getattr(artifact, "source_type", ""),
            "file_type": getattr(artifact, "file_type", ""),
            "title": getattr(artifact, "title", ""),
            "source_url": getattr(artifact, "source_url", None),
        }
    return payload


def build_web_clipper_plan_payload(
    *,
    collector: Any | None,
    records: list[Any],
    ready: bool,
    issues: list[str] | None = None,
) -> dict[str, Any]:
    issues = issues or []
    watch_dirs = list(collector.contract.watch_dirs) if collector is not None else []
    return {
        "schema_version": "1.0",
        "tool": "thoth",
        "surface": "web-clipper plan",
        "ready": ready,
        "issues": issues,
        "source_directories": [str(path) for path in watch_dirs],
        "mutation": {
            "will_index_files": False,
            "will_queue_notes": False,
            "will_stage_attachments": False,
            "will_publish_english_companions": False,
        },
        "counts": {
            "source_directories": len(watch_dirs),
            "files": len(records),
            "new_or_changed": sum(1 for record in records if record.is_new_or_changed),
            "would_queue_notes": sum(
                1 for record in records if getattr(record, "would_queue", False)
            ),
            "would_stage_attachments": sum(
                1 for record in records if getattr(record, "would_stage", False)
            ),
            "by_action": counts_by(records, "action"),
            "by_file_type": counts_by(records, "file_type"),
        },
        "records": [web_clipper_record_payload(record) for record in records],
    }


def render_web_clipper_plan(payload: dict[str, Any]) -> None:
    print("Web Clipper plan")
    if not payload["ready"]:
        print("   Ready: no")
        for issue in payload["issues"]:
            print(f"   Issue: {issue}")
        print("   Re-run after fixing the Web Clipper source configuration.")
        return

    counts = payload["counts"]
    print(f"   Source directories: {counts['source_directories']}")
    print(f"   Files scanned: {counts['files']}")
    print(f"   New or changed: {counts['new_or_changed']}")
    print(f"   Notes that would be queued: {counts['would_queue_notes']}")
    print(f"   Attachments that would be staged: {counts['would_stage_attachments']}")
    print("   No files, queue entries, or attachments were changed.")
    print("   Run without --plan to execute the Web Clipper ingest.")


def _safe_payload_keys(payload_json: str | None) -> list[str]:
    if not payload_json:
        return []
    try:
        payload = json.loads(payload_json)
    except Exception:
        return []
    if isinstance(payload, dict):
        return sorted(str(key) for key in payload)
    return []


def _ingestion_entry_payload(entry: Any) -> dict[str, Any]:
    return {
        "artifact_id": entry.artifact_id,
        "artifact_type": entry.artifact_type,
        "source": entry.source,
        "priority": entry.priority,
        "status": entry.status,
        "attempts": entry.attempts,
        "last_error": entry.last_error,
        "next_attempt_at": entry.next_attempt_at,
        "created_at": entry.created_at,
        "processed_at": entry.processed_at,
        "payload_json_bytes": len(entry.payload_json or ""),
        "payload_keys": _safe_payload_keys(entry.payload_json),
        "capabilities": _safe_payload_keys(entry.capabilities_json),
    }


def build_ingest_queue_plan_payload(
    *, entries: list[Any], limit: int | None
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "tool": "thoth",
        "surface": "ingest-queue plan",
        "ready": True,
        "limit": limit,
        "mutation": {
            "will_mark_processing": False,
            "will_dispatch_artifacts": False,
            "will_update_wiki": False,
            "will_update_queue_status": False,
        },
        "counts": {
            "pending": len(entries),
            "by_artifact_type": counts_by(entries, "artifact_type"),
            "by_source": counts_by(entries, "source"),
        },
        "entries": [_ingestion_entry_payload(entry) for entry in entries],
    }


def render_ingest_queue_plan(payload: dict[str, Any]) -> None:
    counts = payload["counts"]
    print("Ingestion queue plan")
    print(f"   Pending entries selected: {counts['pending']}")
    if payload["limit"] is not None:
        print(f"   Limit: {payload['limit']}")
    print("   No queue rows, artifacts, or wiki pages were changed.")
    print("   Run without --plan to process these entries.")


def build_x_api_sync_plan_payload(config: Any, args: Any) -> dict[str, Any]:
    layout = build_path_layout(config)
    x_api_config = config.get("sources.x_api", {}) or {}
    if not isinstance(x_api_config, dict):
        x_api_config = {}

    scopes = [str(scope) for scope in x_api_config.get("scopes", []) or []]
    normalized_scopes = {scope.strip() for scope in scopes if scope.strip()}
    required_scopes = {"bookmark.read", "tweet.read", "users.read", "offline.access"}
    missing_scopes = sorted(required_scopes.difference(normalized_scopes))
    client_id_configured = bool(str(x_api_config.get("client_id") or "").strip())
    redirect_uri_configured = bool(str(x_api_config.get("redirect_uri") or "").strip())
    enabled = bool(x_api_config.get("enabled", False))

    issues: list[str] = []
    if not enabled:
        issues.append("sources.x_api.enabled is false")
    if not client_id_configured:
        issues.append("sources.x_api.client_id is not configured")
    if not redirect_uri_configured:
        issues.append("sources.x_api.redirect_uri is not configured")
    if missing_scopes:
        issues.append("sources.x_api.scopes is missing: " + ", ".join(missing_scopes))

    token_path = layout.auth_root / "x_api_tokens.json"
    checkpoint_path = layout.auth_root / "x_api_bookmark_sync_checkpoint.json"
    if not token_path.exists():
        issues.append("Stored X API token bundle is missing")

    return {
        "schema_version": "1.0",
        "tool": "thoth",
        "surface": "x-api-sync plan",
        "ready": not issues,
        "issues": issues,
        "parameters": {
            "max_results": args.max_results,
            "max_pages": args.max_pages,
            "resume_from_checkpoint": not args.no_resume,
            "process_immediately": True,
        },
        "mutation": {
            "will_contact_x_api": False,
            "will_enqueue_bookmarks": False,
            "will_process_bookmarks": False,
            "will_update_checkpoint": False,
        },
        "auth": {
            "enabled": enabled,
            "client_id_configured": client_id_configured,
            "redirect_uri_configured": redirect_uri_configured,
            "scopes": scopes,
            "missing_required_scopes": missing_scopes,
            "token_bundle_present": token_path.exists(),
            "checkpoint_present": checkpoint_path.exists(),
        },
    }


def render_x_api_sync_plan(payload: dict[str, Any]) -> None:
    print("X API sync plan")
    print(f"   Ready: {'yes' if payload['ready'] else 'no'}")
    for issue in payload["issues"]:
        print(f"   Issue: {issue}")
    params = payload["parameters"]
    print(f"   Max pages: {params['max_pages']}")
    print(f"   Max results per page: {params['max_results']}")
    print(f"   Resume from checkpoint: {params['resume_from_checkpoint']}")
    print("   No network request, queue write, processing run, or checkpoint write occurred.")
    print("   Run without --plan to execute the X API sync.")
