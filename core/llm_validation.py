"""Structured validation for untrusted LLM output."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from .prompt_security import PromptThreatScope, scan_prompt_threats


class LLMOutputValidationError(ValueError):
    """Raised when model output does not match an expected structured shape."""


@dataclass(frozen=True)
class LLMJSONField:
    """Validation rule for one expected JSON object field."""

    name: str
    expected_type: type | tuple[type, ...] = str
    required: bool = True
    allow_empty: bool = True
    min_length: int = 0
    max_length: int | None = None
    reject_prompt_threats: bool = False
    prompt_scope: PromptThreatScope = "strict"
    validator: Callable[[Any], None] | None = None


def parse_llm_json_response(
    content: str,
    *,
    fields: Iterable[LLMJSONField],
    object_name: str = "LLM response",
    reject_extra_fields: bool = True,
    allow_code_fence: bool = False,
    allow_trailing_commas: bool = False,
) -> dict[str, Any]:
    """Parse and validate a JSON object returned by an LLM.

    By default, the parser accepts only a JSON object as the whole response.
    Callers that need compatibility with common model formatting can opt into
    whole-response code fences or trailing-comma repair. Preambles, trailing
    prose, and extracted substrings still fail closed.
    """
    if not isinstance(content, str) or not content.strip():
        raise LLMOutputValidationError(f"{object_name} was empty")

    normalized = _normalize_json_response_text(
        content,
        allow_code_fence=allow_code_fence,
        allow_trailing_commas=allow_trailing_commas,
    )
    try:
        payload = json.loads(normalized)
    except json.JSONDecodeError as exc:
        raise LLMOutputValidationError(
            f"{object_name} was not valid JSON: {exc.msg}"
        ) from exc

    return validate_llm_json_object(
        payload,
        fields=fields,
        object_name=object_name,
        reject_extra_fields=reject_extra_fields,
    )


_FENCED_JSON_RE = re.compile(
    r"\A```(?:json|JSON)?[ \t]*\r?\n(?P<body>.*)\r?\n?```[ \t]*\Z",
    re.DOTALL,
)


def _normalize_json_response_text(
    content: str,
    *,
    allow_code_fence: bool,
    allow_trailing_commas: bool,
) -> str:
    text = content.strip()
    if allow_code_fence:
        match = _FENCED_JSON_RE.fullmatch(text)
        if match:
            text = match.group("body").strip()
    if allow_trailing_commas:
        text = _remove_json_trailing_commas(text)
    return text


def _remove_json_trailing_commas(text: str) -> str:
    result: list[str] = []
    in_string = False
    escape = False
    length = len(text)
    for index, char in enumerate(text):
        if in_string:
            result.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            result.append(char)
            continue

        if char == ",":
            next_index = index + 1
            while next_index < length and text[next_index].isspace():
                next_index += 1
            if next_index < length and text[next_index] in "}]":
                continue

        result.append(char)
    return "".join(result)


def validate_llm_json_object(
    payload: Any,
    *,
    fields: Iterable[LLMJSONField],
    object_name: str = "LLM response",
    reject_extra_fields: bool = True,
) -> dict[str, Any]:
    """Validate a parsed JSON object without coercing field values."""
    if not isinstance(payload, dict):
        raise LLMOutputValidationError(f"{object_name} must be a JSON object")

    field_rules = tuple(fields)
    expected_names = {field.name for field in field_rules}
    if reject_extra_fields:
        extra_fields = sorted(set(payload) - expected_names)
        if extra_fields:
            raise LLMOutputValidationError(
                f"{object_name} included unexpected fields: {', '.join(extra_fields)}"
            )

    validated: dict[str, Any] = {}
    for field in field_rules:
        if field.name not in payload:
            if field.required:
                raise LLMOutputValidationError(
                    f"{object_name} missing required field: {field.name}"
                )
            continue

        value = payload[field.name]
        if not isinstance(value, field.expected_type):
            expected = _format_expected_type(field.expected_type)
            raise LLMOutputValidationError(
                f"{object_name} field {field.name} must be {expected}"
            )

        if isinstance(value, str):
            _validate_string_field(value, field, object_name)

        if field.validator:
            field.validator(value)

        validated[field.name] = value

    return validated


def validate_comma_separated_tags(
    value: Any,
    *,
    min_tags: int = 1,
    max_tags: int = 12,
    max_tag_length: int = 64,
) -> None:
    """Validate comma-separated model-generated tags without normalizing them."""
    if not isinstance(value, str):
        raise LLMOutputValidationError("tags must be a string")

    tags = [tag.strip() for tag in value.split(",") if tag.strip()]
    if len(tags) < min_tags:
        raise LLMOutputValidationError(f"tags must include at least {min_tags} item(s)")
    if len(tags) > max_tags:
        raise LLMOutputValidationError(f"tags must include at most {max_tags} items")

    for tag in tags:
        if len(tag) > max_tag_length:
            raise LLMOutputValidationError(
                f"tag exceeds maximum length of {max_tag_length} characters"
            )
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 _./+-]*", tag):
            raise LLMOutputValidationError("tag contains unsupported characters")
        findings = scan_prompt_threats(tag, scope="strict")
        if findings:
            pattern_ids = ", ".join(finding.pattern_id for finding in findings)
            raise LLMOutputValidationError(
                f"tag matched prompt-threat pattern(s): {pattern_ids}"
            )


def _validate_string_field(
    value: str,
    field: LLMJSONField,
    object_name: str,
) -> None:
    if not field.allow_empty and not value.strip():
        raise LLMOutputValidationError(
            f"{object_name} field {field.name} must not be empty"
        )
    if len(value) < field.min_length:
        raise LLMOutputValidationError(
            f"{object_name} field {field.name} is shorter than {field.min_length} characters"
        )
    if field.max_length is not None and len(value) > field.max_length:
        raise LLMOutputValidationError(
            f"{object_name} field {field.name} exceeds {field.max_length} characters"
        )
    if field.reject_prompt_threats:
        findings = scan_prompt_threats(value, scope=field.prompt_scope)
        if findings:
            pattern_ids = ", ".join(finding.pattern_id for finding in findings)
            raise LLMOutputValidationError(
                f"{object_name} field {field.name} matched prompt-threat pattern(s): {pattern_ids}"
            )


def _format_expected_type(expected_type: type | tuple[type, ...]) -> str:
    if isinstance(expected_type, tuple):
        return " or ".join(item.__name__ for item in expected_type)
    return expected_type.__name__
