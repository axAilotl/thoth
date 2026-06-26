"""Operator lint actions for the settings console."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Config
from .metadata_db import MetadataDB
from .okf import OKFLintIssue, OKFLintReport, OKFLintRunner
from .path_layout import build_path_layout
from .wiki_contract import build_wiki_contract
from .wiki_lint import WikiLintIssue, WikiLintReport, WikiLintRunner

LINT_KINDS = frozenset({"okf", "wiki", "security"})


def admin_lint_report_path(
    config_data: dict[str, Any],
    *,
    project_root: Path,
    lint_kind: str,
) -> Path:
    """Return the persisted JSON report path for a lint kind."""
    normalized_kind = _normalize_lint_kind(lint_kind)
    _runtime_config, layout = _runtime_config_and_layout(
        config_data,
        project_root=project_root,
    )
    report_dir = layout.system_root / "admin_lint"
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / f"{normalized_kind}-lint.json"


def run_admin_lint(
    config_data: dict[str, Any],
    *,
    project_root: Path,
    lint_kind: str,
) -> dict[str, Any]:
    """Run an operator lint check and persist its JSON report."""
    normalized_kind = _normalize_lint_kind(lint_kind)
    runtime_config, layout = _runtime_config_and_layout(
        config_data,
        project_root=project_root,
    )
    contract = build_wiki_contract(runtime_config, project_root=project_root)
    if normalized_kind == "okf":
        report = OKFLintRunner(
            runtime_config,
            layout=layout,
            contract=contract,
        ).lint()
        return _persist_admin_lint_report(
            layout,
            normalized_kind,
            _okf_lint_payload(report),
        )
    if normalized_kind == "wiki":
        report = WikiLintRunner(
            runtime_config,
            layout=layout,
            contract=contract,
        ).lint()
        return _persist_admin_lint_report(
            layout,
            normalized_kind,
            _wiki_lint_payload(report),
        )

    summary = MetadataDB(str(layout.database_path)).get_ingestion_security_summary(
        limit=100
    )
    return _persist_admin_lint_report(
        layout,
        normalized_kind,
        _security_lint_payload(summary),
    )


def _runtime_config_and_layout(
    config_data: dict[str, Any],
    *,
    project_root: Path,
):
    runtime_config = Config()
    runtime_config.data = config_data
    layout = build_path_layout(runtime_config, project_root=project_root)
    return runtime_config, layout


def _normalize_lint_kind(lint_kind: str) -> str:
    normalized_kind = str(lint_kind or "").strip().lower()
    if normalized_kind not in LINT_KINDS:
        raise ValueError("lint kind must be one of: okf, wiki, security")
    return normalized_kind


def _persist_admin_lint_report(
    layout,
    lint_kind: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    report_dir = layout.system_root / "admin_lint"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{lint_kind}-lint.json"
    response_payload = {
        **payload,
        "report_path": str(report_path),
        "download_url": f"/api/settings/lint/{lint_kind}/download",
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(response_payload, f, indent=2, sort_keys=True)
        f.write("\n")
    return response_payload


def _serialize_okf_lint_issue(issue: OKFLintIssue) -> dict[str, Any]:
    return {
        "code": issue.code,
        "severity": issue.severity,
        "message": issue.message,
        "path": str(issue.path) if issue.path else None,
    }


def _serialize_wiki_lint_issue(issue: WikiLintIssue) -> dict[str, Any]:
    return {
        "code": issue.code,
        "severity": issue.severity,
        "message": issue.message,
        "page_path": str(issue.page_path) if issue.page_path else None,
        "related_paths": [str(path) for path in issue.related_paths],
    }


def _okf_lint_payload(report: OKFLintReport) -> dict[str, Any]:
    return {
        "kind": "okf",
        "status": "failed" if report.has_errors else "ok",
        "checked_at": report.checked_at,
        "summary": {
            "okf_version": report.okf_version,
            "concepts_checked": report.concepts_checked,
            "reserved_files_checked": report.reserved_files_checked,
            "issue_count": len(report.issues),
            "error_count": sum(1 for issue in report.issues if issue.severity == "error"),
            "warning_count": sum(
                1 for issue in report.issues if issue.severity == "warning"
            ),
        },
        "issues": [_serialize_okf_lint_issue(issue) for issue in report.issues],
    }


def _wiki_lint_payload(report: WikiLintReport) -> dict[str, Any]:
    return {
        "kind": "wiki",
        "status": "failed" if report.has_errors else "ok",
        "checked_at": report.checked_at,
        "summary": {
            "pages_checked": report.pages_checked,
            "issue_count": len(report.issues),
            "error_count": sum(1 for issue in report.issues if issue.severity == "error"),
            "warning_count": sum(
                1 for issue in report.issues if issue.severity == "warning"
            ),
        },
        "issues": [_serialize_wiki_lint_issue(issue) for issue in report.issues],
    }


def _security_lint_payload(summary: dict[str, Any]) -> dict[str, Any]:
    counts = summary.get("counts") or {}
    issues: list[dict[str, Any]] = []
    for item in summary.get("strict_failures") or []:
        issues.append(
            {
                "code": "strict-security-failure",
                "severity": "error",
                "artifact_id": item.get("artifact_id"),
                "artifact_type": item.get("artifact_type"),
                "source": item.get("source"),
                "status": item.get("status"),
                "policy_status": item.get("policy_status"),
                "policy_reason": item.get("policy_reason"),
                "pattern_ids": item.get("pattern_ids") or [],
                "strict_pattern_ids": item.get("strict_pattern_ids") or [],
            }
        )
    for item in summary.get("quarantined_artifacts") or []:
        if item.get("policy_status") == "blocked":
            continue
        issues.append(
            {
                "code": "security-review-required",
                "severity": "warning",
                "artifact_id": item.get("artifact_id"),
                "artifact_type": item.get("artifact_type"),
                "source": item.get("source"),
                "status": item.get("status"),
                "policy_status": item.get("policy_status"),
                "policy_reason": item.get("policy_reason"),
                "pattern_ids": item.get("pattern_ids") or [],
            }
        )

    return {
        "kind": "security",
        "status": "failed" if int(counts.get("strict_failures") or 0) else "ok",
        "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "summary": {
            "artifacts_checked": counts.get("total", 0),
            "artifact_findings": counts.get("with_findings", 0),
            "finding_count": counts.get("findings", 0),
            "redaction_count": counts.get("redactions", 0),
            "quarantined": counts.get("quarantined", 0),
            "strict_failures": counts.get("strict_failures", 0),
            "by_source": counts.get("by_source", {}),
            "by_status": counts.get("by_status", {}),
            "by_pattern": counts.get("by_pattern", {}),
            "by_redaction_category": counts.get("by_redaction_category", {}),
        },
        "issues": issues,
        "findings_by_source": summary.get("findings_by_source", []),
        "redactions": summary.get("redactions", {"total": 0, "by_category": {}}),
        "quarantined_artifacts": summary.get("quarantined_artifacts", []),
    }
