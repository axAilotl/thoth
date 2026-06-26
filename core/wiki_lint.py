"""Wiki lint and health checks."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
import re
from typing import Any, Mapping

import yaml

from .config import Config
from .path_layout import PathLayout, build_path_layout
from .wiki_contract import (
    OKF_VERSION,
    WikiContract,
    build_wiki_contract,
    is_legacy_tweet_slug,
)
from .wiki_scaffold import ensure_wiki_scaffold

_LINK_TARGET_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
_SOURCE_LINK_RE = re.compile(r"\[[^\]]+\]\([^)]+\)")
_CITATION_RE = re.compile(r"^\[(\d+)\]\s+\[([^\]]+)\]\(([^)]+)\)$")
_SUMMARY_FIELDS = ("description", "thoth_summary", "summary")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _extract_markdown_links(body: str) -> tuple[str, ...]:
    targets: list[str] = []
    for match in _LINK_TARGET_RE.finditer(body):
        target = match.group(1).strip()
        if target and not target.startswith(("http://", "https://", "mailto:", "#")):
            targets.append(target)
    return tuple(targets)


def _frontmatter_value(frontmatter: dict, *keys: str):
    for key in keys:
        value = frontmatter.get(key)
        if value is not None:
            return value
    return None


def _frontmatter_sequence(frontmatter: dict, *keys: str) -> tuple[str, ...]:
    value = _frontmatter_value(frontmatter, *keys)
    if value is None:
        return tuple()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Mapping):
        return tuple()
    return tuple(str(item) for item in value or ())


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_sequence_value(value: Any) -> bool:
    return value is None or (
        isinstance(value, (list, tuple, set)) and not isinstance(value, (str, bytes))
    )


def _parse_markdown(path: Path) -> "_ParsedMarkdown":
    content = path.read_text(encoding="utf-8")
    if not content.startswith("---\n"):
        return _ParsedMarkdown(
            frontmatter={},
            body=content,
        )

    end_marker = content.find("\n---\n", 4)
    if end_marker == -1:
        return _ParsedMarkdown(
            frontmatter={},
            body=content,
            parse_error="missing closing frontmatter delimiter",
        )

    raw_frontmatter = content[4:end_marker]
    try:
        payload = yaml.safe_load(raw_frontmatter) or {}
    except yaml.YAMLError as exc:
        return _ParsedMarkdown(
            frontmatter={},
            body=content[end_marker + len("\n---\n") :],
            parse_error=str(exc),
        )

    if not isinstance(payload, dict):
        return _ParsedMarkdown(
            frontmatter={},
            body=content[end_marker + len("\n---\n") :],
            parse_error="frontmatter root is not a mapping",
        )

    return _ParsedMarkdown(
        frontmatter=payload,
        body=content[end_marker + len("\n---\n") :],
    )


def _is_generated_frontmatter(frontmatter: dict[str, Any]) -> bool:
    if any(
        key in frontmatter
        for key in (
            "thoth_okf_version",
            "thoth_id",
            "thoth_slug",
            "thoth_artifact_id",
            "thoth_source_type",
        )
    ):
        return True
    return (
        "type" in frontmatter
        and "id" in frontmatter
        and any(
            key in frontmatter
            for key in ("thoth_type", "thoth_kind", "thoth_source_paths")
        )
    )


def _section_lines(body: str, heading: str) -> tuple[str, ...] | None:
    lines = body.splitlines()
    for index, line in enumerate(lines):
        if line.strip() != heading:
            continue
        section: list[str] = []
        for section_line in lines[index + 1 :]:
            if section_line.startswith("#") and section_line.strip():
                break
            section.append(section_line)
        return tuple(section)
    return None


@dataclass(frozen=True)
class WikiLintIssue:
    """Single lint finding."""

    code: str
    severity: str
    message: str
    page_path: Path | None = None
    related_paths: tuple[Path, ...] = ()


@dataclass(frozen=True)
class WikiLintReport:
    """Aggregate wiki lint results."""

    checked_at: str
    pages_checked: int
    issues: tuple[WikiLintIssue, ...]

    @property
    def has_errors(self) -> bool:
        return any(issue.severity == "error" for issue in self.issues)


@dataclass(frozen=True)
class _ParsedMarkdown:
    frontmatter: dict[str, Any]
    body: str
    parse_error: str | None = None


@dataclass(frozen=True)
class _WikiPageLoadResult:
    records: tuple["_WikiPageRecord", ...]
    issues: tuple[WikiLintIssue, ...]
    pages_checked: int


@dataclass(frozen=True)
class _WikiPageRecord:
    path: Path
    frontmatter: dict[str, Any]
    body: str
    is_generated: bool
    slug: str
    okf_id: str | None
    thoth_id: str | None
    title: str
    kind: str
    record_type: str
    source_paths: tuple[str, ...]
    related_slugs: tuple[str, ...]
    updated_at: str | None
    resource: str | None
    artifact_id: str | None
    source_type: str | None
    body_links: tuple[str, ...]


class WikiLintRunner:
    """Filesystem-backed wiki health checks."""

    def __init__(
        self,
        config: Config,
        *,
        layout: PathLayout | None = None,
        contract: WikiContract | None = None,
    ):
        self.config = config
        self.layout = layout or build_path_layout(config)
        self.layout.ensure_directories()
        self.scaffold = ensure_wiki_scaffold(config)
        self.contract = contract or build_wiki_contract(config)

    def lint(self, *, stale_after_days: int = 30) -> WikiLintReport:
        """Lint the wiki layer and return a structured report."""
        if stale_after_days <= 0:
            raise ValueError("stale_after_days must be positive")

        load_result = self._load_pages()
        records = list(load_result.records)
        issues: list[WikiLintIssue] = list(load_result.issues)

        source_claims: dict[str, list[_WikiPageRecord]] = defaultdict(list)
        inbound_links: dict[str, set[str]] = defaultdict(set)
        slug_claims: dict[str, list[_WikiPageRecord]] = defaultdict(list)
        id_claims: dict[str, list[_WikiPageRecord]] = defaultdict(list)

        for record in records:
            frontmatter = record.frontmatter
            if not frontmatter:
                issues.append(
                    WikiLintIssue(
                        code="missing-frontmatter",
                        severity="error",
                        message="Wiki page is missing required frontmatter.",
                        page_path=record.path,
                    )
                )
                continue

            issues.extend(self._lint_record_schema(record))
            issues.extend(self._lint_summary_schema(record))
            issues.extend(self._lint_generated_metadata(record))
            issues.extend(self._lint_source_section(record))
            issues.extend(self._lint_citations(record))

            if record.slug:
                slug_claims[record.slug].append(record)
            for page_id in {record.okf_id, record.thoth_id}:
                if page_id:
                    id_claims[page_id].append(record)

            source_paths = record.source_paths
            timestamp = _parse_timestamp(record.updated_at)
            if record.updated_at and timestamp is None:
                issues.append(
                    WikiLintIssue(
                        code="invalid-updated-at",
                        severity="error",
                        message=f"Page updated_at is not a valid timestamp: {record.updated_at}",
                        page_path=record.path,
                    )
                )
            elif timestamp is not None:
                age = _now_utc() - timestamp
                if age > timedelta(days=stale_after_days):
                    issues.append(
                        WikiLintIssue(
                            code="stale-page",
                            severity="warning",
                            message=(
                                f"Page has not been updated in {age.days} day(s), "
                                f"exceeding the {stale_after_days}-day threshold."
                            ),
                            page_path=record.path,
                        )
                    )

            if record.record_type not in {"wiki_page", "wiki_query"}:
                issues.append(
                    WikiLintIssue(
                        code="invalid-record-type",
                        severity="error",
                        message=f"Unsupported wiki record type: {record.record_type}",
                        page_path=record.path,
                    )
                )

            for source_path in source_paths:
                source_claims[source_path].append(record)
            for related_slug in record.related_slugs:
                inbound_links[related_slug].add(record.slug)
            for link_target in record.body_links:
                target_slug = Path(link_target).stem
                inbound_links[target_slug].add(record.slug)

        for slug, claimants in slug_claims.items():
            if len({claim.path for claim in claimants}) <= 1:
                continue
            issues.append(
                WikiLintIssue(
                    code="duplicate-page-slug",
                    severity="error",
                    message=f"Wiki slug is claimed by multiple pages: {slug}",
                    page_path=claimants[0].path,
                    related_paths=tuple(claim.path for claim in claimants),
                )
            )

        for page_id, claimants in id_claims.items():
            if len({claim.path for claim in claimants}) <= 1:
                continue
            issues.append(
                WikiLintIssue(
                    code="duplicate-page-id",
                    severity="error",
                    message=f"Wiki page id is claimed by multiple pages: {page_id}",
                    page_path=claimants[0].path,
                    related_paths=tuple(claim.path for claim in claimants),
                )
            )

        for source_path, claimants in source_claims.items():
            if len(claimants) <= 1:
                continue
            titles = {claim.title for claim in claimants}
            kinds = {claim.kind for claim in claimants}
            record_types = {claim.record_type for claim in claimants}
            if len(titles) > 1 or len(kinds) > 1 or len(record_types) > 1:
                related_paths = tuple(claim.path for claim in claimants)
                issues.append(
                    WikiLintIssue(
                        code="contradicting-source-path",
                        severity="error",
                        message=(
                            "Source path is claimed by multiple wiki pages with "
                            f"conflicting metadata: {source_path}"
                        ),
                        page_path=claimants[0].path,
                        related_paths=related_paths,
                    )
                )

        for record in records:
            if record.source_paths:
                continue
            if inbound_links.get(record.slug):
                continue
            if record.related_slugs:
                continue
            if record.body_links:
                continue
            issues.append(
                WikiLintIssue(
                    code="orphan-page",
                    severity="warning",
                    message=(
                        "Wiki page is disconnected from other wiki pages and has "
                        "no source paths."
                    ),
                    page_path=record.path,
                )
            )

        issues.sort(key=lambda issue: (issue.severity, issue.code, str(issue.page_path or "")))
        return WikiLintReport(
            checked_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            pages_checked=load_result.pages_checked,
            issues=tuple(issues),
        )

    def _lint_record_schema(self, record: _WikiPageRecord) -> list[WikiLintIssue]:
        issues: list[WikiLintIssue] = []

        try:
            self.contract.validate_slug(record.slug)
        except ValueError as exc:
            issues.append(
                WikiLintIssue(
                    code="invalid-page-slug",
                    severity="error",
                    message=str(exc),
                    page_path=record.path,
                )
            )

        if record.kind not in self.contract.supported_kinds:
            issues.append(
                WikiLintIssue(
                    code="invalid-page-kind",
                    severity="error",
                    message=f"Unsupported wiki page kind: {record.kind}",
                    page_path=record.path,
                )
            )

        for field_name in (
            "thoth_source_paths",
            "source_paths",
            "thoth_related_slugs",
            "related_slugs",
        ):
            if field_name in record.frontmatter and not _is_sequence_value(
                record.frontmatter[field_name]
            ):
                issues.append(
                    WikiLintIssue(
                        code="invalid-frontmatter-sequence",
                        severity="error",
                        message=f"Frontmatter field {field_name} must be a sequence.",
                        page_path=record.path,
                    )
                )

        return issues

    def _lint_summary_schema(self, record: _WikiPageRecord) -> list[WikiLintIssue]:
        issues: list[WikiLintIssue] = []
        present_fields = {
            field_name: record.frontmatter[field_name]
            for field_name in _SUMMARY_FIELDS
            if field_name in record.frontmatter
        }

        invalid_fields = [
            field_name
            for field_name, value in present_fields.items()
            if not isinstance(value, str)
        ]
        if invalid_fields:
            issues.append(
                WikiLintIssue(
                    code="invalid-summary-schema",
                    severity="error",
                    message=(
                        "Wiki summary frontmatter fields must be strings: "
                        + ", ".join(sorted(invalid_fields))
                    ),
                    page_path=record.path,
                )
            )
            return issues

        if record.is_generated:
            missing_fields = [
                field_name for field_name in _SUMMARY_FIELDS if field_name not in present_fields
            ]
            if missing_fields:
                issues.append(
                    WikiLintIssue(
                        code="invalid-summary-schema",
                        severity="error",
                        message=(
                            "Generated wiki pages must include summary fields: "
                            + ", ".join(missing_fields)
                        ),
                        page_path=record.path,
                    )
                )
                return issues

            values = {present_fields[field_name] for field_name in _SUMMARY_FIELDS}
            if len(values) > 1:
                issues.append(
                    WikiLintIssue(
                        code="invalid-summary-schema",
                        severity="error",
                        message="Generated wiki summary fields must agree.",
                        page_path=record.path,
                    )
                )

        return issues

    def _lint_generated_metadata(self, record: _WikiPageRecord) -> list[WikiLintIssue]:
        if not record.is_generated:
            return []

        issues: list[WikiLintIssue] = []
        frontmatter = record.frontmatter

        missing_common = [
            field_name
            for field_name in (
                "type",
                "id",
                "thoth_okf_version",
                "thoth_type",
                "thoth_id",
                "thoth_slug",
                "thoth_kind",
                "thoth_updated_at",
            )
            if not _is_non_empty_string(frontmatter.get(field_name))
        ]
        if missing_common:
            issues.append(
                WikiLintIssue(
                    code="missing-thoth-metadata",
                    severity="error",
                    message=(
                        "Generated wiki page is missing required metadata fields: "
                        + ", ".join(missing_common)
                    ),
                    page_path=record.path,
                )
            )

        if frontmatter.get("thoth_okf_version") not in (None, OKF_VERSION):
            issues.append(
                WikiLintIssue(
                    code="invalid-okf-version",
                    severity="error",
                    message=(
                        "Unsupported generated wiki OKF version: "
                        f"{frontmatter.get('thoth_okf_version')}"
                    ),
                    page_path=record.path,
                )
            )

        for field_name, value in (
            ("id", record.okf_id),
            ("thoth_id", record.thoth_id),
        ):
            if value and value != record.slug:
                issues.append(
                    WikiLintIssue(
                        code="identity-mismatch",
                        severity="error",
                        message=(
                            f"Generated wiki {field_name} does not match slug "
                            f"{record.slug}: {value}"
                        ),
                        page_path=record.path,
                    )
                )

        if record.record_type == "wiki_page":
            source_paths_present = "thoth_source_paths" in frontmatter
            if not source_paths_present:
                issues.append(
                    WikiLintIssue(
                        code="missing-source-metadata",
                        severity="error",
                        message="Generated wiki page is missing thoth_source_paths metadata.",
                        page_path=record.path,
                    )
                )

            artifact_marker = (
                "thoth_artifact_id" in frontmatter or "thoth_source_type" in frontmatter
            )
            if artifact_marker:
                missing_artifact_fields = [
                    field_name
                    for field_name in ("thoth_artifact_id", "thoth_source_type")
                    if not _is_non_empty_string(frontmatter.get(field_name))
                ]
                if missing_artifact_fields:
                    issues.append(
                        WikiLintIssue(
                            code="missing-provenance",
                            severity="error",
                            message=(
                                "Generated artifact wiki page is missing provenance fields: "
                                + ", ".join(missing_artifact_fields)
                            ),
                            page_path=record.path,
                        )
                    )
            elif not record.source_paths and not _is_non_empty_string(record.resource):
                issues.append(
                    WikiLintIssue(
                        code="missing-provenance",
                        severity="error",
                        message=(
                            "Generated wiki page must include artifact provenance, "
                            "source paths, or a canonical resource."
                        ),
                        page_path=record.path,
                    )
                )

        if record.record_type == "wiki_query":
            missing_query_fields = [
                field_name
                for field_name in (
                    "thoth_query",
                    "thoth_query_terms",
                    "thoth_related_slugs",
                    "thoth_source_paths",
                )
                if field_name not in frontmatter
            ]
            if missing_query_fields:
                issues.append(
                    WikiLintIssue(
                        code="missing-query-provenance",
                        severity="error",
                        message=(
                            "Generated query wiki page is missing provenance fields: "
                            + ", ".join(missing_query_fields)
                        ),
                        page_path=record.path,
                    )
                )

        return issues

    def _lint_source_section(self, record: _WikiPageRecord) -> list[WikiLintIssue]:
        if not record.is_generated or record.record_type != "wiki_page" or not record.source_paths:
            return []

        section = _section_lines(record.body, "## Sources")
        if section is None:
            return [
                WikiLintIssue(
                    code="missing-source-section",
                    severity="error",
                    message="Generated wiki page with source paths is missing a Sources section.",
                    page_path=record.path,
                )
            ]

        source_text = "\n".join(section)
        missing_source_paths = [
            source_path for source_path in record.source_paths if source_path not in source_text
        ]
        nonempty_lines = [line.strip() for line in section if line.strip()]
        has_linked_source = any(_SOURCE_LINK_RE.search(line) for line in nonempty_lines)
        if missing_source_paths or not has_linked_source:
            details = []
            if missing_source_paths:
                details.append("missing " + ", ".join(missing_source_paths))
            if not has_linked_source:
                details.append("no markdown source links")
            return [
                WikiLintIssue(
                    code="malformed-source-section",
                    severity="error",
                    message="Generated wiki Sources section is malformed: " + "; ".join(details),
                    page_path=record.path,
                )
            ]

        return []

    def _lint_citations(self, record: _WikiPageRecord) -> list[WikiLintIssue]:
        if not record.is_generated or record.record_type != "wiki_page":
            return []

        has_artifact_marker = bool(record.artifact_id or record.source_type)
        citation_section = _section_lines(record.body, "# Citations")
        expected_labels = (
            (["Canonical resource"] if _is_non_empty_string(record.resource) else [])
            + list(record.source_paths)
        )
        requires_citations = has_artifact_marker and bool(expected_labels)
        if not requires_citations and citation_section is None:
            return []

        if citation_section is None:
            return [
                WikiLintIssue(
                    code="missing-citations",
                    severity="error",
                    message="Generated artifact wiki page is missing a Citations section.",
                    page_path=record.path,
                )
            ]

        citation_lines = [line.strip() for line in citation_section if line.strip()]
        parsed: list[tuple[int, str, str]] = []
        malformed = False
        for line in citation_lines:
            match = _CITATION_RE.match(line)
            if not match:
                malformed = True
                continue
            parsed.append((int(match.group(1)), match.group(2), match.group(3)))

        expected_numbers = list(range(1, len(parsed) + 1))
        actual_numbers = [number for number, _label, _target in parsed]
        missing_labels = [
            label
            for label in expected_labels
            if label not in {parsed_label for _number, parsed_label, _target in parsed}
        ]
        if malformed or not parsed or actual_numbers != expected_numbers or missing_labels:
            details = []
            if malformed:
                details.append("invalid citation line")
            if not parsed:
                details.append("no citation entries")
            if actual_numbers != expected_numbers:
                details.append("citation numbers are not sequential")
            if missing_labels:
                details.append("missing " + ", ".join(missing_labels))
            return [
                WikiLintIssue(
                    code="malformed-citations",
                    severity="error",
                    message="Generated wiki Citations section is malformed: " + "; ".join(details),
                    page_path=record.path,
                )
            ]

        return []

    def _load_pages(self) -> _WikiPageLoadResult:
        records: list[_WikiPageRecord] = []
        issues: list[WikiLintIssue] = []
        pages_checked = 0
        for page_path in sorted(self.contract.pages_dir.glob("*.md")):
            document = _parse_markdown(page_path)
            frontmatter = document.frontmatter
            slug = str(_frontmatter_value(frontmatter, "thoth_slug", "slug") or page_path.stem)
            if is_legacy_tweet_slug(slug):
                continue
            pages_checked += 1
            if document.parse_error:
                issues.append(
                    WikiLintIssue(
                        code="invalid-frontmatter",
                        severity="error",
                        message=f"Wiki page frontmatter is invalid: {document.parse_error}",
                        page_path=page_path,
                    )
                )
                continue
            records.append(
                _WikiPageRecord(
                    path=page_path,
                    frontmatter=frontmatter,
                    body=document.body,
                    is_generated=_is_generated_frontmatter(frontmatter),
                    slug=slug,
                    okf_id=(
                        str(frontmatter["id"]).strip()
                        if "id" in frontmatter and frontmatter["id"] is not None
                        else None
                    ),
                    thoth_id=(
                        str(frontmatter["thoth_id"]).strip()
                        if "thoth_id" in frontmatter and frontmatter["thoth_id"] is not None
                        else None
                    ),
                    title=str(frontmatter.get("title") or page_path.stem),
                    kind=str(_frontmatter_value(frontmatter, "thoth_kind", "kind") or "topic"),
                    record_type=str(frontmatter.get("thoth_type") or "wiki_page"),
                    source_paths=_frontmatter_sequence(
                        frontmatter,
                        "thoth_source_paths",
                        "source_paths",
                    ),
                    related_slugs=_frontmatter_sequence(
                        frontmatter,
                        "thoth_related_slugs",
                        "related_slugs",
                    ),
                    updated_at=_frontmatter_value(
                        frontmatter,
                        "thoth_updated_at",
                        "updated_at",
                        "timestamp",
                    ),
                    resource=(
                        str(frontmatter["resource"]).strip()
                        if "resource" in frontmatter and frontmatter["resource"] is not None
                        else None
                    ),
                    artifact_id=(
                        str(frontmatter["thoth_artifact_id"]).strip()
                        if "thoth_artifact_id" in frontmatter
                        and frontmatter["thoth_artifact_id"] is not None
                        else None
                    ),
                    source_type=(
                        str(frontmatter["thoth_source_type"]).strip()
                        if "thoth_source_type" in frontmatter
                        and frontmatter["thoth_source_type"] is not None
                        else None
                    ),
                    body_links=_extract_markdown_links(document.body),
                )
            )
        return _WikiPageLoadResult(
            records=tuple(records),
            issues=tuple(issues),
            pages_checked=pages_checked,
        )
