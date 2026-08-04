"""Read-only lint and migration reports for pre-event-backbone artifacts."""

from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterator, Mapping

import yaml

from .config import Config
from .metadata_db import MetadataDB
from .path_layout import PathLayout, build_path_layout
from .prompt_security import (
    THOTH_SECURITY_FINDINGS_KEY,
    THOTH_SECURITY_POLICY_KEY,
    THOTH_SECURITY_SCANNED_LENGTH_KEY,
)
from .wiki_contract import WikiContract, build_wiki_contract, is_legacy_tweet_slug


_SECURITY_FRONTMATTER_KEYS = frozenset(
    {
        "thoth_security_findings",
        "thoth_security_policy",
        "security_findings",
        "prompt_security_findings",
        "security_policy",
        THOTH_SECURITY_FINDINGS_KEY,
        THOTH_SECURITY_POLICY_KEY,
        THOTH_SECURITY_SCANNED_LENGTH_KEY,
    }
)
_EVENT_KEYS = (
    "capture_event_id",
    "event_id",
    "event_ids",
    "thoth_event_ids",
)
_RAW_REF_KEYS = (
    "raw_ref_id",
    "raw_ref_ids",
    "raw_artifact_ref_id",
    "raw_artifact_ref_ids",
)
_RAW_PAYLOAD_PATH_KEYS = (
    "raw_payload_path",
    "raw_payload_sha256",
)


@dataclass(frozen=True)
class LegacyArtifactLintIssue:
    """Single legacy artifact migration finding."""

    code: str
    severity: str
    message: str
    subject_type: str
    artifact_id: str | None = None
    artifact_type: str | None = None
    source: str | None = None
    page_path: Path | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    @property
    def subject_id(self) -> str:
        if self.subject_type == "wiki_page" and self.page_path is not None:
            return str(self.page_path)
        return self.artifact_id or str(self.page_path or "")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "subject_type": self.subject_type,
            "artifact_id": self.artifact_id,
            "artifact_type": self.artifact_type,
            "source": self.source,
            "page_path": str(self.page_path) if self.page_path else None,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class LegacyArtifactMigrationAction:
    """Read-only migration plan item derived from lint findings."""

    subject_type: str
    subject_id: str
    action: str
    reason_codes: tuple[str, ...]
    missing_fields: tuple[str, ...]
    description: str
    destructive: bool = False
    requires_explicit_command: bool = True
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "action": self.action,
            "reason_codes": list(self.reason_codes),
            "missing_fields": list(self.missing_fields),
            "description": self.description,
            "destructive": self.destructive,
            "requires_explicit_command": self.requires_explicit_command,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class LegacyArtifactLintReport:
    """Aggregate legacy artifact lint result."""

    checked_at: str
    artifacts_checked: int
    wiki_pages_checked: int
    issues: tuple[LegacyArtifactLintIssue, ...]
    migration_actions: tuple[LegacyArtifactMigrationAction, ...]

    @property
    def has_errors(self) -> bool:
        return any(issue.severity == "error" for issue in self.issues)

    @property
    def has_findings(self) -> bool:
        return bool(self.issues)


@dataclass(frozen=True)
class _ArtifactRecord:
    artifact_id: str
    artifact_type: str
    source: str
    status: str
    payload_json: str
    created_at: str | None
    processed_at: str | None


@dataclass(frozen=True)
class _ArtifactLinkSnapshot:
    canonical_id: str | None = None
    capture_event_ids: tuple[str, ...] = ()
    raw_ref_ids: tuple[str, ...] = ()
    artifact_link_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ParsedWikiPage:
    path: Path
    frontmatter: dict[str, Any]
    parse_error: str | None = None


class LegacyArtifactLintRunner:
    """Audit old artifact rows and wiki pages without mutating them."""

    def __init__(
        self,
        config: Config,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
        contract: WikiContract | None = None,
    ) -> None:
        self.config = config
        self.layout = layout or build_path_layout(config)
        self.db = db
        self.contract = contract or build_wiki_contract(config)

    def lint(
        self,
        *,
        limit: int | None = None,
        include_artifacts: bool = True,
        include_wiki: bool = True,
    ) -> LegacyArtifactLintReport:
        """Return a read-only legacy migration readiness report."""
        if limit is not None and int(limit) <= 0:
            raise ValueError("limit must be positive")
        issues: list[LegacyArtifactLintIssue] = []
        artifacts_checked = 0
        wiki_pages_checked = 0

        if include_artifacts:
            artifact_issues, artifacts_checked = self._lint_artifact_rows(limit=limit)
            issues.extend(artifact_issues)
        if include_wiki:
            wiki_issues, wiki_pages_checked = self._lint_wiki_pages()
            issues.extend(wiki_issues)

        issues.sort(
            key=lambda issue: (
                issue.severity,
                issue.subject_type,
                issue.code,
                issue.subject_id,
            )
        )
        actions = _build_migration_actions(issues)
        return LegacyArtifactLintReport(
            checked_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            artifacts_checked=artifacts_checked,
            wiki_pages_checked=wiki_pages_checked,
            issues=tuple(issues),
            migration_actions=actions,
        )

    def _lint_artifact_rows(
        self,
        *,
        limit: int | None,
    ) -> tuple[list[LegacyArtifactLintIssue], int]:
        if not self.db and not self.layout.database_path.exists():
            return [], 0

        issues: list[LegacyArtifactLintIssue] = []
        with self._metadata_connection() as conn:
            if conn is None or not _table_exists(conn, "ingestion_queue"):
                return [], 0
            records = _read_artifact_records(conn, limit=limit)
            for record in records:
                links = _read_artifact_link_snapshot(conn, record)
                payload = _json_object_or_none(record.payload_json)
                if payload is None:
                    issues.append(
                        LegacyArtifactLintIssue(
                            code="artifact-invalid-payload-json",
                            severity="error",
                            subject_type="artifact",
                            artifact_id=record.artifact_id,
                            artifact_type=record.artifact_type,
                            source=record.source,
                            message=(
                                "Artifact queue payload is not valid JSON; "
                                "migration cannot infer provenance safely."
                            ),
                        )
                    )
                    payload = {}

                metadata = _metadata_mappings(payload)
                common = {
                    "status": record.status,
                    "created_at": record.created_at,
                    "processed_at": record.processed_at,
                }
                if not _artifact_has_canonical_id(metadata, links):
                    issues.append(
                        _artifact_issue(
                            "artifact-missing-canonical-id",
                            record,
                            "Artifact lacks a canonical identity link.",
                            details=common,
                        )
                    )
                if not _artifact_has_raw_ref(metadata, links):
                    issues.append(
                        _artifact_issue(
                            "artifact-missing-raw-ref",
                            record,
                            "Artifact lacks an immutable raw artifact reference.",
                            details=common,
                        )
                    )
                if not _artifact_has_threat_metadata(metadata):
                    issues.append(
                        _artifact_issue(
                            "artifact-missing-threat-metadata",
                            record,
                            "Artifact lacks prompt-threat scan metadata.",
                            details=common,
                        )
                    )
                if not _artifact_has_event_link(metadata, links):
                    issues.append(
                        _artifact_issue(
                            "artifact-missing-event-link",
                            record,
                            "Artifact is not linked to a capture event.",
                            details=common,
                        )
                    )
        return issues, len(records)

    def _lint_wiki_pages(self) -> tuple[list[LegacyArtifactLintIssue], int]:
        pages_dir = self.contract.pages_dir
        if not pages_dir.exists():
            return [], 0

        issues: list[LegacyArtifactLintIssue] = []
        pages = [_parse_wiki_page(path) for path in sorted(pages_dir.glob("*.md"))]
        for page in pages:
            if page.parse_error:
                issues.append(
                    LegacyArtifactLintIssue(
                        code="wiki-page-invalid-frontmatter",
                        severity="error",
                        subject_type="wiki_page",
                        page_path=page.path,
                        message=f"Wiki page frontmatter is invalid: {page.parse_error}",
                    )
                )
                continue

            frontmatter = page.frontmatter
            slug = str(
                _first_present(frontmatter, "thoth_slug", "slug") or page.path.stem
            ).strip()
            artifact_id = _string_or_none(
                _first_present(frontmatter, "thoth_artifact_id", "artifact_id")
            )
            artifact_ids = _string_tuple(
                _first_present(frontmatter, "thoth_artifact_ids", "artifact_ids")
            )
            source_type = _string_or_none(
                _first_present(frontmatter, "thoth_source_type", "source_type")
            )
            is_artifact_page = bool(
                artifact_id or artifact_ids or source_type or is_legacy_tweet_slug(slug)
            )
            is_capture_page = bool(
                _first_present(frontmatter, "thoth_capture_page_type")
                or _first_present(frontmatter, "thoth_capture_event_count")
            )
            if not is_artifact_page and not is_capture_page:
                continue

            if is_artifact_page and not _wiki_has_canonical_id(frontmatter):
                issues.append(
                    _wiki_issue(
                        "wiki-page-missing-canonical-id",
                        page,
                        "Wiki artifact page lacks thoth_canonical_id.",
                        artifact_id=artifact_id,
                        source=source_type,
                    )
                )
            if not _wiki_has_raw_ref(frontmatter):
                issues.append(
                    _wiki_issue(
                        "wiki-page-missing-raw-ref",
                        page,
                        "Wiki page lacks raw-ref input provenance.",
                        artifact_id=artifact_id,
                        source=source_type,
                    )
                )
            if is_artifact_page and not _wiki_has_threat_metadata(frontmatter):
                issues.append(
                    _wiki_issue(
                        "wiki-page-missing-threat-metadata",
                        page,
                        "Wiki artifact page lacks prompt-threat metadata.",
                        artifact_id=artifact_id,
                        source=source_type,
                    )
                )
            if not _wiki_has_event_link(frontmatter):
                issues.append(
                    _wiki_issue(
                        "wiki-page-missing-event-link",
                        page,
                        "Wiki page lacks capture event IDs.",
                        artifact_id=artifact_id,
                        source=source_type,
                    )
                )
        return issues, len(pages)

    @contextmanager
    def _metadata_connection(self) -> Iterator[sqlite3.Connection | None]:
        if self.db is not None:
            with self.db._get_connection() as conn:
                yield conn
            return

        db_path = self.layout.database_path
        if not db_path.exists():
            yield None
            return

        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()


def legacy_artifact_lint_report_payload(
    report: LegacyArtifactLintReport,
) -> dict[str, Any]:
    """Serialize a legacy lint report for CLI and admin API output."""
    by_code = Counter(issue.code for issue in report.issues)
    by_subject_type = Counter(issue.subject_type for issue in report.issues)
    error_count = sum(1 for issue in report.issues if issue.severity == "error")
    warning_count = sum(1 for issue in report.issues if issue.severity == "warning")
    return {
        "kind": "legacy-artifacts",
        "status": (
            "failed"
            if report.has_errors
            else "needs_migration"
            if report.has_findings
            else "ok"
        ),
        "checked_at": report.checked_at,
        "mutated": False,
        "summary": {
            "artifacts_checked": report.artifacts_checked,
            "wiki_pages_checked": report.wiki_pages_checked,
            "issue_count": len(report.issues),
            "error_count": error_count,
            "warning_count": warning_count,
            "migration_action_count": len(report.migration_actions),
            "by_code": dict(sorted(by_code.items())),
            "by_subject_type": dict(sorted(by_subject_type.items())),
        },
        "issues": [issue.to_dict() for issue in report.issues],
        "migration_actions": [
            action.to_dict() for action in report.migration_actions
        ],
    }


def _read_artifact_records(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
) -> tuple[_ArtifactRecord, ...]:
    query = (
        "SELECT artifact_id, artifact_type, source, status, payload_json, "
        "created_at, processed_at FROM ingestion_queue "
        "ORDER BY created_at DESC, artifact_id ASC"
    )
    params: tuple[Any, ...] = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (int(limit),)
    rows = conn.execute(query, params).fetchall()
    return tuple(
        _ArtifactRecord(
            artifact_id=str(row["artifact_id"] or ""),
            artifact_type=str(row["artifact_type"] or ""),
            source=str(row["source"] or ""),
            status=str(row["status"] or ""),
            payload_json=str(row["payload_json"] or ""),
            created_at=row["created_at"],
            processed_at=row["processed_at"],
        )
        for row in rows
    )


def _read_artifact_link_snapshot(
    conn: sqlite3.Connection,
    record: _ArtifactRecord,
) -> _ArtifactLinkSnapshot:
    canonical_id = None
    if _table_exists(conn, "artifact_canonical_links"):
        row = conn.execute(
            """
            SELECT canonical_id
            FROM artifact_canonical_links
            WHERE artifact_id = ?
                AND (? = '' OR artifact_type = ?)
                AND (? = '' OR source_type = ?)
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (
                record.artifact_id,
                record.artifact_type,
                record.artifact_type,
                record.source,
                record.source,
            ),
        ).fetchone()
        if row:
            canonical_id = _string_or_none(row["canonical_id"])

    capture_event_ids: list[str] = []
    raw_ref_ids: list[str] = []
    artifact_link_ids: list[str] = []
    for table_name in ("connector_checkpoint_outputs", "connector_run_outputs"):
        if not _table_exists(conn, table_name):
            continue
        columns = _table_columns(conn, table_name)
        wanted = {"capture_event_id", "raw_ref_id", "artifact_link_id"}
        if not wanted.issubset(columns):
            continue
        rows = conn.execute(
            f"""
            SELECT capture_event_id, raw_ref_id, artifact_link_id
            FROM {table_name}
            WHERE artifact_id = ?
                AND (? = '' OR artifact_type = ?)
            """,
            (record.artifact_id, record.artifact_type, record.artifact_type),
        ).fetchall()
        for row in rows:
            _append_if_present(capture_event_ids, row["capture_event_id"])
            _append_if_present(raw_ref_ids, row["raw_ref_id"])
            _append_if_present(artifact_link_ids, row["artifact_link_id"])

    return _ArtifactLinkSnapshot(
        canonical_id=canonical_id,
        capture_event_ids=tuple(dict.fromkeys(capture_event_ids)),
        raw_ref_ids=tuple(dict.fromkeys(raw_ref_ids)),
        artifact_link_ids=tuple(dict.fromkeys(artifact_link_ids)),
    )


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _json_object_or_none(payload_json: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _metadata_mappings(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    mappings: list[Mapping[str, Any]] = [payload]
    for key in (
        "normalized_metadata",
        "custom_metadata",
        "provenance",
        "raw_payload",
        "source_identity",
    ):
        value = payload.get(key)
        if isinstance(value, Mapping):
            mappings.append(value)
    provenance = payload.get("provenance")
    if isinstance(provenance, Mapping):
        raw_payload = provenance.get("raw_payload")
        if isinstance(raw_payload, Mapping):
            mappings.append(raw_payload)
        source_identity = provenance.get("source_identity")
        if isinstance(source_identity, Mapping):
            mappings.append(source_identity)
    return tuple(mappings)


def _artifact_has_canonical_id(
    mappings: tuple[Mapping[str, Any], ...],
    links: _ArtifactLinkSnapshot,
) -> bool:
    if links.canonical_id:
        return True
    return any(_string_or_none(mapping.get("canonical_id")) for mapping in mappings)


def _artifact_has_raw_ref(
    mappings: tuple[Mapping[str, Any], ...],
    links: _ArtifactLinkSnapshot,
) -> bool:
    if links.raw_ref_ids:
        return True
    for mapping in mappings:
        if any(_has_value(mapping.get(key)) for key in _RAW_REF_KEYS):
            return True
        if any(_has_value(mapping.get(key)) for key in _RAW_PAYLOAD_PATH_KEYS):
            return True
        if _has_value(mapping.get("path")) and _has_value(
            mapping.get("sha256") or mapping.get("hash")
        ):
            return True
    return False


def _artifact_has_threat_metadata(
    mappings: tuple[Mapping[str, Any], ...],
) -> bool:
    for mapping in mappings:
        if any(
            key in mapping and mapping.get(key) not in (None, "", [], {})
            for key in (
                THOTH_SECURITY_FINDINGS_KEY,
                THOTH_SECURITY_POLICY_KEY,
                THOTH_SECURITY_SCANNED_LENGTH_KEY,
                "thoth_security_findings",
                "thoth_security_policy",
                "security_findings",
                "prompt_security_findings",
            )
        ):
            return True
    return False


def _artifact_has_event_link(
    mappings: tuple[Mapping[str, Any], ...],
    links: _ArtifactLinkSnapshot,
) -> bool:
    if links.capture_event_ids or links.artifact_link_ids:
        return True
    return any(
        any(_has_value(mapping.get(key)) for key in _EVENT_KEYS)
        for mapping in mappings
    )


def _artifact_issue(
    code: str,
    record: _ArtifactRecord,
    message: str,
    *,
    details: Mapping[str, Any],
) -> LegacyArtifactLintIssue:
    return LegacyArtifactLintIssue(
        code=code,
        severity="warning",
        subject_type="artifact",
        artifact_id=record.artifact_id,
        artifact_type=record.artifact_type,
        source=record.source,
        message=message,
        details=details,
    )


def _parse_wiki_page(path: Path) -> _ParsedWikiPage:
    content = path.read_text(encoding="utf-8")
    if not content.startswith("---\n"):
        return _ParsedWikiPage(path=path, frontmatter={})
    end_marker = content.find("\n---\n", 4)
    if end_marker == -1:
        return _ParsedWikiPage(
            path=path,
            frontmatter={},
            parse_error="missing closing frontmatter delimiter",
        )
    raw_frontmatter = content[4:end_marker]
    try:
        payload = yaml.safe_load(raw_frontmatter) or {}
    except yaml.YAMLError as exc:
        return _ParsedWikiPage(path=path, frontmatter={}, parse_error=str(exc))
    if not isinstance(payload, dict):
        return _ParsedWikiPage(
            path=path,
            frontmatter={},
            parse_error="frontmatter root is not a mapping",
        )
    return _ParsedWikiPage(path=path, frontmatter=payload)


def _wiki_has_canonical_id(frontmatter: Mapping[str, Any]) -> bool:
    return bool(_first_present(frontmatter, "thoth_canonical_id", "canonical_id"))


def _wiki_has_raw_ref(frontmatter: Mapping[str, Any]) -> bool:
    if any(_has_value(frontmatter.get(key)) for key in _RAW_REF_KEYS):
        return True
    for item in _mapping_sequence(
        _first_present(frontmatter, "thoth_input_manifest", "input_manifest")
    ):
        if item.get("input_kind") == "raw_ref" or item.get("raw_ref_id"):
            return True
    for item in _mapping_sequence(
        _first_present(frontmatter, "thoth_influence_sources", "influence_sources")
    ):
        if item.get("source_type") == "raw_ref" or item.get("raw_ref_id"):
            return True
    return False


def _wiki_has_threat_metadata(frontmatter: Mapping[str, Any]) -> bool:
    return any(
        key in frontmatter and frontmatter.get(key) not in (None, "", [], {})
        for key in _SECURITY_FRONTMATTER_KEYS
    )


def _wiki_has_event_link(frontmatter: Mapping[str, Any]) -> bool:
    if _has_value(_first_present(frontmatter, "thoth_event_ids", "event_ids")):
        return True
    for item in _mapping_sequence(
        _first_present(frontmatter, "thoth_input_manifest", "input_manifest")
    ):
        if item.get("input_kind") == "capture_event" or item.get("event_id"):
            return True
    for item in _mapping_sequence(
        _first_present(frontmatter, "thoth_influence_sources", "influence_sources")
    ):
        if item.get("source_type") == "capture_event" or item.get("event_id"):
            return True
    return False


def _wiki_issue(
    code: str,
    page: _ParsedWikiPage,
    message: str,
    *,
    artifact_id: str | None,
    source: str | None,
) -> LegacyArtifactLintIssue:
    return LegacyArtifactLintIssue(
        code=code,
        severity="warning",
        subject_type="wiki_page",
        artifact_id=artifact_id,
        source=source,
        page_path=page.path,
        message=message,
    )


def _build_migration_actions(
    issues: list[LegacyArtifactLintIssue],
) -> tuple[LegacyArtifactMigrationAction, ...]:
    grouped: dict[tuple[str, str], list[LegacyArtifactLintIssue]] = defaultdict(list)
    for issue in issues:
        if issue.severity == "error":
            continue
        if not issue.subject_id:
            continue
        grouped[(issue.subject_type, issue.subject_id)].append(issue)

    actions: list[LegacyArtifactMigrationAction] = []
    for (subject_type, subject_id), subject_issues in sorted(grouped.items()):
        reason_codes = tuple(sorted({issue.code for issue in subject_issues}))
        missing_fields = tuple(
            sorted(
                {
                    field
                    for issue in subject_issues
                    for field in _missing_fields_for_code(issue.code)
                }
            )
        )
        if subject_type == "artifact":
            action = "reingest_or_backfill_capture_lifecycle"
            description = (
                "Backfill this artifact through the capture lifecycle so canonical "
                "identity, raw-ref, threat scan, and event-link metadata are recorded."
            )
        else:
            action = "recompile_event_backed_wiki_page"
            description = (
                "Recompile this wiki page from event-backed artifact inputs so "
                "frontmatter records canonical, raw-ref, threat, and event metadata."
            )
        actions.append(
            LegacyArtifactMigrationAction(
                subject_type=subject_type,
                subject_id=subject_id,
                action=action,
                reason_codes=reason_codes,
                missing_fields=missing_fields,
                description=description,
            )
        )
    return tuple(actions)


def _missing_fields_for_code(code: str) -> tuple[str, ...]:
    if "canonical-id" in code:
        return ("canonical_id",)
    if "raw-ref" in code:
        return ("raw_ref",)
    if "threat-metadata" in code:
        return ("threat_metadata",)
    if "event-link" in code:
        return ("event_link",)
    return ()


def _first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def _string_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, (list, tuple, set)):
        values = tuple(value)
    else:
        values = (value,)
    return tuple(str(item).strip() for item in values if str(item).strip())


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping):
        return bool(value)
    if isinstance(value, (list, tuple, set)):
        return any(_has_value(item) for item in value)
    return True


def _append_if_present(values: list[str], value: Any) -> None:
    text = _string_or_none(value)
    if text:
        values.append(text)
