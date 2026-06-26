"""Wiki stale-page and compiler metadata health for admin status."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from .admin_status_utils import (
    RECENT_LIMIT,
    STALE_PAGE_CODES,
    dt_text,
    error_item,
    min_datetime,
    parse_datetime,
    safe_reason,
)
from .archivist_state import load_archivist_topic_state
from .archivist_topics import load_archivist_topic_registry, resolve_archivist_topics_path
from .config import Config
from .metadata_db import MetadataDB
from .wiki_contract import build_wiki_contract
from .wiki_io import read_frontmatter
from .wiki_lint import WikiLintIssue, WikiLintRunner


def stale_pages_status(
    config: Config,
    *,
    layout: Any,
    event_store: Any | None,
    project_root: Path,
    stale_after_days: int,
) -> dict[str, Any]:
    try:
        report = WikiLintRunner(
            config,
            layout=layout,
            contract=build_wiki_contract(config, project_root=project_root),
            event_store=event_store,
        ).lint(stale_after_days=stale_after_days)
    except Exception as exc:
        return {
            "status": "error",
            "pages_checked": 0,
            "total": 0,
            "items": [],
            "error": safe_reason(exc),
        }

    stale_issues = [
        _wiki_issue_payload(issue)
        for issue in report.issues
        if issue.code in STALE_PAGE_CODES
    ]
    return {
        "status": "stale" if stale_issues else "ok",
        "pages_checked": report.pages_checked,
        "total": len(stale_issues),
        "items": stale_issues[:RECENT_LIMIT],
        "issue_count": len(report.issues),
        "error_count": sum(1 for issue in report.issues if issue.severity == "error"),
        "warning_count": sum(1 for issue in report.issues if issue.severity == "warning"),
        "stale_after_days": stale_after_days,
    }


def compiler_status(
    config: Config,
    *,
    layout: Any,
    db: MetadataDB,
    project_root: Path,
    now: datetime,
) -> dict[str, Any]:
    archivist = _archivist_compiler_status(
        config,
        db=db,
        project_root=project_root,
        now=now,
    )
    capture_wiki = _capture_wiki_compiler_status(
        config,
        layout=layout,
        project_root=project_root,
    )
    errors = [
        item
        for item in (
            error_item("archivist", archivist.get("error")),
            error_item("capture_wiki", capture_wiki.get("error")),
        )
        if item
    ]
    return {
        "status": "degraded" if errors else "ok",
        "archivist": archivist,
        "capture_wiki": capture_wiki,
        "errors": errors,
        "error": "; ".join(item["reason"] for item in errors) if errors else None,
    }


def _archivist_compiler_status(
    config: Config,
    *,
    db: MetadataDB,
    project_root: Path,
    now: datetime,
) -> dict[str, Any]:
    registry_path = resolve_archivist_topics_path(config, project_root=project_root)
    explicit_path = bool(str(config.get("paths.archivist_topics_file", "") or "").strip())
    try:
        registry = load_archivist_topic_registry(
            config,
            project_root=project_root,
            required=explicit_path,
        )
    except Exception as exc:
        return {
            "status": "error",
            "registry_path": str(registry_path),
            "exists": registry_path.exists(),
            "topic_count": 0,
            "runs": [],
            "error": safe_reason(exc),
        }

    runs = []
    for topic in registry.topics:
        try:
            state = load_archivist_topic_state(topic.id, db=db)
            next_due_at = _next_due_at(state.last_success_at, topic.cadence_hours)
            status, reason = _archivist_run_status(
                state,
                next_due_at=next_due_at,
                now=now,
            )
            runs.append(
                {
                    "topic_id": topic.id,
                    "title": topic.title,
                    "status": status,
                    "reason": reason,
                    "last_run_at": state.last_run_at,
                    "last_success_at": state.last_success_at,
                    "next_due_at": dt_text(next_due_at),
                    "last_candidate_count": state.last_candidate_count,
                    "last_model_provider": state.last_model_provider,
                    "last_model": state.last_model,
                    "force_requested_at": state.force_requested_at,
                    "force_reason": safe_reason(state.force_reason),
                }
            )
        except Exception as exc:
            runs.append(
                {
                    "topic_id": topic.id,
                    "title": topic.title,
                    "status": "error",
                    "reason": safe_reason(exc),
                }
            )

    runs.sort(
        key=lambda item: parse_datetime(item.get("last_run_at")) or min_datetime(),
        reverse=True,
    )
    return {
        "status": "degraded" if any(item["status"] == "error" for item in runs) else "ok",
        "registry_path": str(registry_path),
        "exists": registry_path.exists(),
        "topic_count": len(registry.topics),
        "runs": runs,
        "recent": runs[:RECENT_LIMIT],
        "forced": [item for item in runs if item.get("force_requested_at")],
        "due": [item for item in runs if item.get("status") == "due"],
    }


def _capture_wiki_compiler_status(
    config: Config,
    *,
    layout: Any,
    project_root: Path,
) -> dict[str, Any]:
    try:
        contract = build_wiki_contract(config, project_root=project_root)
        pages_dir = contract.pages_dir
        pages = []
        for page_path in sorted(pages_dir.glob("*.md")):
            frontmatter = read_frontmatter(page_path)
            page_type = frontmatter.get("thoth_capture_page_type")
            if not page_type:
                continue
            pages.append(
                {
                    "page_path": str(page_path),
                    "slug": frontmatter.get("thoth_slug")
                    or frontmatter.get("slug")
                    or page_path.stem,
                    "page_type": page_type,
                    "page_key": frontmatter.get("thoth_capture_page_key"),
                    "event_count": int(frontmatter.get("thoth_capture_event_count") or 0),
                    "updated_at": dt_text(frontmatter.get("updated_at")),
                    "input_hash": frontmatter.get("thoth_input_hash")
                    or frontmatter.get("input_hash"),
                    "change_reason": _change_reason(
                        frontmatter.get("thoth_change_provenance")
                    ),
                    "audit": frontmatter.get("thoth_capture_audit") or {},
                }
            )
        pages.sort(
            key=lambda item: parse_datetime(item.get("updated_at")) or min_datetime(),
            reverse=True,
        )
    except Exception as exc:
        return {
            "status": "error",
            "pages_dir": str(layout.wiki_root / "pages"),
            "compiled_page_count": 0,
            "recent_pages": [],
            "error": safe_reason(exc),
        }

    return {
        "status": "ok",
        "pages_dir": str(pages_dir),
        "compiled_page_count": len(pages),
        "recent_pages": pages[:RECENT_LIMIT],
        "event_count": sum(item["event_count"] for item in pages),
        "by_page_type": dict(Counter(str(item["page_type"]) for item in pages)),
    }


def _wiki_issue_payload(issue: WikiLintIssue) -> dict[str, Any]:
    return {
        "code": issue.code,
        "severity": issue.severity,
        "message": issue.message,
        "page_path": str(issue.page_path) if issue.page_path else None,
        "details": dict(issue.details or {}),
    }


def _archivist_run_status(
    state: Any,
    *,
    next_due_at: datetime | None,
    now: datetime,
) -> tuple[str, str]:
    if state.force_requested_at:
        return "force_queued", safe_reason(state.force_reason) or "manual force queued"
    if not state.last_run_at:
        return "never_run", "topic has never compiled"
    if state.last_success_at != state.last_run_at:
        return "last_run_failed", "last run did not record success"
    if next_due_at and now >= next_due_at:
        return "due", "cadence due"
    return "ok", "last run succeeded"


def _next_due_at(last_success_at: str | None, cadence_hours: float) -> datetime | None:
    last_success = parse_datetime(last_success_at)
    if last_success is None:
        return None
    return last_success + timedelta(hours=float(cadence_hours))


def _change_reason(value: Any) -> str | None:
    if isinstance(value, Mapping):
        return safe_reason(value.get("reason"))
    return None
