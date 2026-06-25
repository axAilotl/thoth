"""OKF v0.1 linting for compiled wiki bundles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any

import yaml

from .config import Config
from .path_layout import PathLayout, build_path_layout
from .wiki_contract import OKF_VERSION, WikiContract, build_wiki_contract
from .wiki_scaffold import ensure_wiki_scaffold

_DATE_HEADING_RE = re.compile(r"^##\s+\d{4}-\d{2}-\d{2}\s*$")
_HEADING_RE = re.compile(r"^#\s+\S")


@dataclass(frozen=True)
class OKFLintIssue:
    """Single OKF lint finding."""

    code: str
    severity: str
    message: str
    path: Path | None = None


@dataclass(frozen=True)
class OKFLintReport:
    """Aggregate OKF lint results."""

    checked_at: str
    okf_version: str
    concepts_checked: int
    reserved_files_checked: int
    issues: tuple[OKFLintIssue, ...]

    @property
    def has_errors(self) -> bool:
        return any(issue.severity == "error" for issue in self.issues)


@dataclass(frozen=True)
class _ParsedMarkdown:
    frontmatter: dict[str, Any]
    body: str
    has_frontmatter: bool
    parse_error: str | None = None


class OKFLintRunner:
    """Filesystem-backed OKF v0.1 conformance checks."""

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

    def lint(self) -> OKFLintReport:
        """Lint the configured wiki root as an OKF bundle."""
        issues: list[OKFLintIssue] = []
        concepts_checked = 0
        reserved_files_checked = 0

        for markdown_path in sorted(self.contract.root.rglob("*.md")):
            if markdown_path.name in self.contract.reserved_filenames:
                reserved_files_checked += 1
                issues.extend(self._lint_reserved_file(markdown_path))
                continue

            concepts_checked += 1
            issues.extend(self._lint_concept_file(markdown_path))

        issues.sort(key=lambda issue: (issue.severity, issue.code, str(issue.path or "")))
        return OKFLintReport(
            checked_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            okf_version=OKF_VERSION,
            concepts_checked=concepts_checked,
            reserved_files_checked=reserved_files_checked,
            issues=tuple(issues),
        )

    def _lint_concept_file(self, path: Path) -> list[OKFLintIssue]:
        parsed = self._parse_markdown(path)
        issues: list[OKFLintIssue] = []

        if parsed.parse_error:
            return [
                OKFLintIssue(
                    code="invalid-frontmatter",
                    severity="error",
                    message=f"Concept frontmatter is not parseable YAML: {parsed.parse_error}",
                    path=path,
                )
            ]

        if not parsed.has_frontmatter:
            issues.append(
                OKFLintIssue(
                    code="missing-frontmatter",
                    severity="error",
                    message="Concept document is missing required YAML frontmatter.",
                    path=path,
                )
            )
            return issues

        concept_type = parsed.frontmatter.get("type")
        if not isinstance(concept_type, str) or not concept_type.strip():
            issues.append(
                OKFLintIssue(
                    code="missing-type",
                    severity="error",
                    message="Concept frontmatter must contain a non-empty OKF type field.",
                    path=path,
                )
            )

        return issues

    def _lint_reserved_file(self, path: Path) -> list[OKFLintIssue]:
        parsed = self._parse_markdown(path)
        issues: list[OKFLintIssue] = []

        if parsed.has_frontmatter:
            issues.append(
                OKFLintIssue(
                    code="reserved-frontmatter",
                    severity="error",
                    message=f"{path.name} is an OKF reserved file and must not contain frontmatter.",
                    path=path,
                )
            )

        body_lines = [line for line in parsed.body.splitlines() if line.strip()]
        if not body_lines or not _HEADING_RE.match(body_lines[0]):
            issues.append(
                OKFLintIssue(
                    code="reserved-missing-heading",
                    severity="error",
                    message=f"{path.name} must start with a markdown heading.",
                    path=path,
                )
            )

        if path.name == self.contract.log_filename:
            for line in body_lines[1:]:
                if line.startswith("## ") and not _DATE_HEADING_RE.match(line):
                    issues.append(
                        OKFLintIssue(
                            code="invalid-log-date",
                            severity="error",
                            message="log.md date headings must use YYYY-MM-DD form.",
                            path=path,
                        )
                    )

        return issues

    def _parse_markdown(self, path: Path) -> _ParsedMarkdown:
        content = path.read_text(encoding="utf-8")
        if not content.startswith("---\n"):
            return _ParsedMarkdown(frontmatter={}, body=content, has_frontmatter=False)

        end_marker = content.find("\n---\n", 4)
        if end_marker == -1:
            return _ParsedMarkdown(
                frontmatter={},
                body=content,
                has_frontmatter=True,
                parse_error="missing closing frontmatter delimiter",
            )

        raw_frontmatter = content[4:end_marker]
        try:
            payload = yaml.safe_load(raw_frontmatter) or {}
        except yaml.YAMLError as exc:
            return _ParsedMarkdown(
                frontmatter={},
                body=content[end_marker + len("\n---\n") :],
                has_frontmatter=True,
                parse_error=str(exc),
            )

        if not isinstance(payload, dict):
            return _ParsedMarkdown(
                frontmatter={},
                body=content[end_marker + len("\n---\n") :],
                has_frontmatter=True,
                parse_error="frontmatter root is not a mapping",
            )

        return _ParsedMarkdown(
            frontmatter=payload,
            body=content[end_marker + len("\n---\n") :],
            has_frontmatter=True,
        )
