"""Wiki lint and health checks."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
import re

from .config import Config
from .path_layout import PathLayout, build_path_layout
from .wiki_contract import WikiContract, build_wiki_contract, is_legacy_tweet_slug
from .wiki_io import read_document
from .wiki_scaffold import ensure_wiki_scaffold

_LINK_TARGET_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


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
class _WikiPageRecord:
    path: Path
    slug: str
    title: str
    kind: str
    record_type: str
    source_paths: tuple[str, ...]
    related_slugs: tuple[str, ...]
    updated_at: str | None
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

        records = [record for record in self._load_pages()]
        issues: list[WikiLintIssue] = []

        source_claims: dict[str, list[_WikiPageRecord]] = defaultdict(list)
        inbound_links: dict[str, set[str]] = defaultdict(set)

        for record in records:
            frontmatter = read_document(record.path).frontmatter
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
                                f"Page has not been updated in {age.days} day(s), exceeding the {stale_after_days}-day threshold."
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
                            f"Source path is claimed by multiple wiki pages with conflicting metadata: {source_path}"
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
                    message="Wiki page is disconnected from other wiki pages and has no source paths.",
                    page_path=record.path,
                )
            )

        issues.sort(key=lambda issue: (issue.severity, issue.code, str(issue.page_path or "")))
        return WikiLintReport(
            checked_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            pages_checked=len(records),
            issues=tuple(issues),
        )

    def _load_pages(self) -> list[_WikiPageRecord]:
        records: list[_WikiPageRecord] = []
        for page_path in sorted(self.contract.pages_dir.glob("*.md")):
            document = read_document(page_path)
            frontmatter = document.frontmatter
            slug = str(frontmatter.get("slug") or page_path.stem)
            if is_legacy_tweet_slug(slug):
                continue
            records.append(
                _WikiPageRecord(
                    path=page_path,
                    slug=slug,
                    title=str(frontmatter.get("title") or page_path.stem),
                    kind=str(frontmatter.get("kind") or "topic"),
                    record_type=str(frontmatter.get("thoth_type") or "wiki_page"),
                    source_paths=tuple(str(path) for path in frontmatter.get("source_paths") or ()),
                    related_slugs=tuple(str(path) for path in frontmatter.get("related_slugs") or ()),
                    updated_at=frontmatter.get("updated_at"),
                    body_links=_extract_markdown_links(document.body),
                )
            )
        return records
