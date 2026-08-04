"""Sensitive data detection and redaction for LLM-bound text."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Iterable


class SensitiveRedactionError(ValueError):
    """Raised when sensitive text cannot be safely redacted."""


@dataclass(frozen=True)
class SensitiveFinding:
    """One redacted sensitive value without the sensitive value itself."""

    category: str
    pattern_id: str
    placeholder: str


@dataclass(frozen=True)
class RedactionResult:
    """Redacted text plus non-sensitive redaction metadata."""

    redacted_text: str
    findings: tuple[SensitiveFinding, ...]
    original_length: int
    redacted_length: int

    @property
    def has_findings(self) -> bool:
        return bool(self.findings)

    @property
    def categories(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(finding.category for finding in self.findings))

    def to_metadata(self) -> dict[str, object]:
        counts: dict[str, int] = {}
        for finding in self.findings:
            counts[finding.category] = counts.get(finding.category, 0) + 1
        return {
            "redacted": self.has_findings,
            "finding_count": len(self.findings),
            "categories": counts,
            "findings": [
                {
                    "category": finding.category,
                    "pattern_id": finding.pattern_id,
                    "placeholder": finding.placeholder,
                }
                for finding in self.findings
            ],
            "original_length": self.original_length,
            "redacted_length": self.redacted_length,
        }


@dataclass(frozen=True)
class _SensitivePattern:
    pattern: re.Pattern[str]
    category: str
    pattern_id: str
    value_group: str | int = 0
    validator: Callable[[str], bool] | None = None


def _not_redaction_placeholder(value: str) -> bool:
    return not value.startswith("[[REDACTED_")


def _looks_like_private_email(value: str) -> bool:
    domain = value.rsplit("@", 1)[-1].lower()
    return domain not in {
        "example.com",
        "example.org",
        "example.net",
        "localhost",
    }


_ENV_SECRET_NAME = (
    r"(?:"
    r"[A-Z0-9_]*(?:API[_-]?KEY|ACCESS[_-]?KEY|SECRET|TOKEN|PASSWORD|PASS|PWD|"
    r"CREDENTIAL|PRIVATE[_-]?KEY|CLIENT[_-]?SECRET|SESSION|COOKIE)[A-Z0-9_]*"
    r"|DATABASE_URL|DB_URL|REDIS_URL|POSTGRES_URL|MYSQL_URL|MONGO(?:DB)?_URL"
    r")"
)


_PATTERNS: tuple[_SensitivePattern, ...] = (
    _SensitivePattern(
        re.compile(
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
            re.DOTALL,
        ),
        "private_key",
        "private_key_block",
    ),
    _SensitivePattern(
        re.compile(
            rf"(?im)(\b(?:export\s+)?{_ENV_SECRET_NAME}\s*=\s*)(?P<quote>[\"']?)(?P<value>[^\s\"'#]+)(?P=quote)"
        ),
        "env_secret",
        "env_style_assignment",
        "value",
        _not_redaction_placeholder,
    ),
    _SensitivePattern(
        re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
        "api_key",
        "aws_access_key_id",
    ),
    _SensitivePattern(
        re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
        "api_key",
        "google_api_key",
    ),
    _SensitivePattern(
        re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b"),
        "api_key",
        "openai_api_key",
    ),
    _SensitivePattern(
        re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
        "api_key",
        "github_token",
    ),
    _SensitivePattern(
        re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
        "api_key",
        "slack_token",
    ),
    _SensitivePattern(
        re.compile(r"\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b"),
        "api_key",
        "stripe_secret_key",
    ),
    _SensitivePattern(
        re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),
        "bearer_token",
        "jwt",
    ),
    _SensitivePattern(
        re.compile(r"\b(?:Bearer|OAuth)\s+(?P<value>[A-Za-z0-9._~+/=-]{20,})\b", re.IGNORECASE),
        "bearer_token",
        "authorization_token",
        "value",
        _not_redaction_placeholder,
    ),
    _SensitivePattern(
        re.compile(r"://(?P<value>[^/\s:@]+:[^/\s@]+)@"),
        "credential",
        "url_embedded_credentials",
        "value",
        _not_redaction_placeholder,
    ),
    _SensitivePattern(
        re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
        "email",
        "private_email",
        0,
        _looks_like_private_email,
    ),
    _SensitivePattern(
        re.compile(
            r"(?<!\w)(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]\d{3}[\s.-]\d{4}"
            r"(?:\s*(?:x|ext\.?)\s*\d{1,6})?(?!\w)"
        ),
        "phone",
        "phone_number",
    ),
)


def redact_sensitive_text(text: str | None) -> RedactionResult:
    """Redact likely credentials and private PII without exposing originals."""
    original = text or ""
    spans = _collect_spans(original)
    if not spans:
        return RedactionResult(
            redacted_text=original,
            findings=(),
            original_length=len(original),
            redacted_length=len(original),
        )

    output: list[str] = []
    findings: list[SensitiveFinding] = []
    cursor = 0
    counters: dict[str, int] = {}
    for start, end, category, pattern_id in spans:
        if start < cursor:
            continue
        counters[category] = counters.get(category, 0) + 1
        placeholder = f"[[REDACTED_{category.upper()}_{counters[category]}]]"
        output.append(original[cursor:start])
        output.append(placeholder)
        findings.append(
            SensitiveFinding(
                category=category,
                pattern_id=pattern_id,
                placeholder=placeholder,
            )
        )
        cursor = end
    output.append(original[cursor:])
    redacted = "".join(output)
    return RedactionResult(
        redacted_text=redacted,
        findings=tuple(findings),
        original_length=len(original),
        redacted_length=len(redacted),
    )


def summarize_redactions(results: Iterable[RedactionResult]) -> dict[str, object] | None:
    """Merge multiple redaction reports into compact metadata."""
    findings: list[dict[str, str]] = []
    counts: dict[str, int] = {}
    for result in results:
        for finding in result.findings:
            counts[finding.category] = counts.get(finding.category, 0) + 1
            findings.append(
                {
                    "category": finding.category,
                    "pattern_id": finding.pattern_id,
                    "placeholder": finding.placeholder,
                }
            )
    if not findings:
        return None
    return {
        "redacted": True,
        "finding_count": len(findings),
        "categories": counts,
        "findings": findings,
    }


def _collect_spans(text: str) -> list[tuple[int, int, str, str]]:
    spans: list[tuple[int, int, str, str]] = []
    for pattern_def in _PATTERNS:
        for match in pattern_def.pattern.finditer(text):
            value = match.group(pattern_def.value_group)
            if not value:
                continue
            if pattern_def.validator and not pattern_def.validator(value.strip()):
                continue
            try:
                start, end = match.span(pattern_def.value_group)
            except IndexError as exc:
                raise SensitiveRedactionError(
                    f"Invalid sensitive redaction pattern group: {pattern_def.pattern_id}"
                ) from exc
            spans.append((start, end, pattern_def.category, pattern_def.pattern_id))

    spans.sort(key=lambda item: (item[0], -(item[1] - item[0])))
    deduped: list[tuple[int, int, str, str]] = []
    covered_until = -1
    for span in spans:
        if span[0] < covered_until:
            continue
        deduped.append(span)
        covered_until = span[1]
    return deduped


__all__ = [
    "RedactionResult",
    "SensitiveFinding",
    "SensitiveRedactionError",
    "redact_sensitive_text",
    "summarize_redactions",
]
