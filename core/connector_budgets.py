"""Budget parsing and enforcement for connector runs."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol


class ConfigLike(Protocol):
    def get(self, key: str, default: Any = None) -> Any:
        ...


class ConnectorBudgetError(ValueError):
    """Raised when connector budget configuration or usage fails closed."""

    def __init__(
        self,
        message: str,
        *,
        connector_name: str,
        field: str,
        observed: int | float | None = None,
        limit: int | float | None = None,
        subject: str | None = None,
        usage: Mapping[str, Any] | None = None,
        budget: Mapping[str, Any] | None = None,
    ) -> None:
        self.connector_name = connector_name
        self.field = field
        self.observed = observed
        self.limit = limit
        self.subject = subject
        self.usage = dict(usage or {})
        self.budget = dict(budget or {})
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "connector": self.connector_name,
            "field": self.field,
            "observed": self.observed,
            "limit": self.limit,
            "subject": self.subject,
            "message": str(self),
            "usage": self.usage,
            "budget": self.budget,
        }


DEFAULT_CONNECTOR_BUDGETS: dict[str, Any] = {
    "max_bytes_per_file": 50 * 1024 * 1024,
    "max_bytes_per_run": 250 * 1024 * 1024,
    "max_files_per_run": 500,
    "max_input_tokens_per_run": 500_000,
    "max_output_tokens_per_run": 100_000,
    "max_transcript_chunks_per_run": 64,
    "transcript_chunk_chars": 75_000,
    "estimated_output_tokens_per_artifact": 512,
    "input_cost_per_1k_tokens_usd": 0.0,
    "output_cost_per_1k_tokens_usd": 0.0,
    "max_estimated_cost_usd": None,
}

_BUDGET_FIELDS = frozenset(DEFAULT_CONNECTOR_BUDGETS)
_OPTIONAL_LIMIT_FIELDS = frozenset(
    {
        "max_bytes_per_file",
        "max_bytes_per_run",
        "max_files_per_run",
        "max_input_tokens_per_run",
        "max_output_tokens_per_run",
        "max_transcript_chunks_per_run",
        "max_estimated_cost_usd",
    }
)
_INTEGER_FIELDS = frozenset(
    {
        "max_bytes_per_file",
        "max_bytes_per_run",
        "max_files_per_run",
        "max_input_tokens_per_run",
        "max_output_tokens_per_run",
        "max_transcript_chunks_per_run",
        "transcript_chunk_chars",
        "estimated_output_tokens_per_artifact",
    }
)
_FLOAT_FIELDS = frozenset(
    {
        "input_cost_per_1k_tokens_usd",
        "output_cost_per_1k_tokens_usd",
        "max_estimated_cost_usd",
    }
)


@dataclass(frozen=True)
class ConnectorBudget:
    """Effective budget limits for one connector."""

    connector_name: str
    limits: Mapping[str, Any]
    configured: bool = False

    def to_dict(self) -> dict[str, Any]:
        cost = {
            "currency": "USD",
            "input_cost_per_1k_tokens": self.limits["input_cost_per_1k_tokens_usd"],
            "output_cost_per_1k_tokens": self.limits["output_cost_per_1k_tokens_usd"],
            "max_estimated_cost": self.limits["max_estimated_cost_usd"],
        }
        estimation = {
            "approximate": True,
            "token_estimate": "ceil(characters / 4)",
            "transcript_chunk_chars": self.limits["transcript_chunk_chars"],
            "estimated_output_tokens_per_artifact": self.limits[
                "estimated_output_tokens_per_artifact"
            ],
        }
        return {
            "connector": self.connector_name,
            "configured": self.configured,
            "limits": dict(self.limits),
            "cost": cost,
            "estimation": estimation,
        }


@dataclass
class ConnectorBudgetUsage:
    """Mutable usage accumulator for one connector run."""

    connector_name: str
    budget: ConnectorBudget
    files: int = 0
    bytes: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    transcript_chunks: int = 0

    def to_dict(self) -> dict[str, Any]:
        input_cost = (
            self.input_tokens
            * float(self.budget.limits["input_cost_per_1k_tokens_usd"])
            / 1000
        )
        output_cost = (
            self.output_tokens
            * float(self.budget.limits["output_cost_per_1k_tokens_usd"])
            / 1000
        )
        total_cost = input_cost + output_cost
        return {
            "status": "within_budget",
            "connector": self.connector_name,
            "approximate": True,
            "files": self.files,
            "bytes": self.bytes,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.input_tokens + self.output_tokens,
            "transcript_chunks": self.transcript_chunks,
            "estimated_cost": {
                "currency": "USD",
                "input": round(input_cost, 8),
                "output": round(output_cost, 8),
                "total": round(total_cost, 8),
            },
            "limits": dict(self.budget.limits),
        }


class ConnectorBudgetTracker:
    """Accumulate and enforce connector budget usage."""

    def __init__(self, budget: ConnectorBudget):
        self.budget = budget
        self.usage = ConnectorBudgetUsage(
            connector_name=budget.connector_name,
            budget=budget,
        )

    def add_file(
        self,
        path: str | Path,
        *,
        count_input_tokens: bool = True,
        label: str | None = None,
    ) -> None:
        resolved = Path(path)
        size_bytes = resolved.stat().st_size
        self.usage.files += 1
        self.usage.bytes += size_bytes
        if count_input_tokens:
            self.usage.input_tokens += estimate_tokens_from_bytes(size_bytes)
        self._check_file_size(size_bytes, label or str(path))
        self.check(subject=label or str(path))

    def add_files(
        self,
        paths: list[str | Path] | tuple[str | Path, ...],
        *,
        count_input_tokens: bool = True,
    ) -> None:
        for path in paths:
            self.add_file(path, count_input_tokens=count_input_tokens)

    def add_bytes(
        self,
        size_bytes: int,
        *,
        label: str,
        count_file: bool = False,
        count_input_tokens: bool = False,
    ) -> None:
        size = _nonnegative_int_value(size_bytes, "size_bytes", origin=label)
        if count_file:
            self.usage.files += 1
        self.usage.bytes += size
        if count_input_tokens:
            self.usage.input_tokens += estimate_tokens_from_bytes(size)
        self._check_file_size(size, label)
        self.check(subject=label)

    def add_input_text(self, text: str, *, label: str) -> None:
        self.usage.input_tokens += estimate_tokens(text)
        self.check(subject=label)

    def add_output_text(self, text: str, *, label: str) -> None:
        self.usage.output_tokens += estimate_tokens(text)
        self.check(subject=label)

    def add_estimated_output_artifacts(self, count: int, *, label: str) -> None:
        artifact_count = _nonnegative_int_value(count, "artifact_count", origin=label)
        self.usage.output_tokens += (
            artifact_count
            * int(self.budget.limits["estimated_output_tokens_per_artifact"])
        )
        self.check(subject=label)

    def add_transcript_text(self, text: str, *, label: str) -> None:
        chunks = estimate_transcript_chunks(
            text,
            chunk_chars=int(self.budget.limits["transcript_chunk_chars"]),
        )
        self.usage.transcript_chunks += chunks
        self.check(subject=label)

    def add_json_payload(self, payload: Any, *, label: str) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        self.add_input_text(encoded, label=label)

    def check(self, *, subject: str | None = None) -> None:
        for field_name, observed in (
            ("max_files_per_run", self.usage.files),
            ("max_bytes_per_run", self.usage.bytes),
            ("max_input_tokens_per_run", self.usage.input_tokens),
            ("max_output_tokens_per_run", self.usage.output_tokens),
            ("max_transcript_chunks_per_run", self.usage.transcript_chunks),
        ):
            limit = self.budget.limits[field_name]
            if limit is not None and observed > limit:
                self._raise_exceeded(
                    field_name,
                    observed=observed,
                    limit=limit,
                    subject=subject,
                )

        max_cost = self.budget.limits["max_estimated_cost_usd"]
        if max_cost is not None:
            cost = self.usage.to_dict()["estimated_cost"]["total"]
            if cost > max_cost:
                self._raise_exceeded(
                    "max_estimated_cost_usd",
                    observed=cost,
                    limit=max_cost,
                    subject=subject,
                )

    def summary(self) -> dict[str, Any]:
        return {
            "budget": self.budget.to_dict(),
            "usage": self.usage.to_dict(),
        }

    def _check_file_size(self, size_bytes: int, subject: str) -> None:
        limit = self.budget.limits["max_bytes_per_file"]
        if limit is not None and size_bytes > limit:
            self._raise_exceeded(
                "max_bytes_per_file",
                observed=size_bytes,
                limit=limit,
                subject=subject,
            )

    def _raise_exceeded(
        self,
        field_name: str,
        *,
        observed: int | float,
        limit: int | float,
        subject: str | None,
    ) -> None:
        subject_text = f" for {subject}" if subject else ""
        message = (
            f"Connector budget exceeded for {self.budget.connector_name}"
            f"{subject_text}: {field_name} observed {observed} > limit {limit}"
        )
        usage = self.usage.to_dict()
        usage["status"] = "exceeded"
        usage["reason"] = message
        raise ConnectorBudgetError(
            message,
            connector_name=self.budget.connector_name,
            field=field_name,
            observed=observed,
            limit=limit,
            subject=subject,
            usage=usage,
            budget=self.budget.to_dict(),
        )


def start_connector_budget_run(
    config: ConfigLike,
    connector_name: str,
) -> ConnectorBudgetTracker:
    return ConnectorBudgetTracker(resolve_connector_budget(config, connector_name))


def resolve_connector_budget(
    config: ConfigLike | None,
    connector_name: str,
) -> ConnectorBudget:
    raw_config = config.get("connectors.budgets") if config is not None else None
    budget_config = _budget_root(raw_config)
    effective = dict(DEFAULT_CONNECTOR_BUDGETS)
    configured = False

    top_level_defaults = {
        key: value for key, value in budget_config.items() if key in _BUDGET_FIELDS
    }
    if top_level_defaults:
        configured = True
        _merge_budget_payload(
            effective,
            top_level_defaults,
            origin="connectors.budgets",
        )

    defaults = budget_config.get("defaults", {})
    if defaults:
        configured = True
        _merge_budget_payload(
            effective,
            _budget_mapping(defaults, "connectors.budgets.defaults"),
            origin="connectors.budgets.defaults",
        )

    per_connector = _per_connector_budget_mapping(budget_config)
    if per_connector:
        connector_budget = per_connector.get(connector_name)
        if connector_budget is not None:
            configured = True
            _merge_budget_payload(
                effective,
                _budget_mapping(
                    connector_budget,
                    f"connectors.budgets.per_connector.{connector_name}",
                ),
                origin=f"connectors.budgets.per_connector.{connector_name}",
            )

    if config is not None:
        source_budget = config.get(f"sources.{connector_name}.budgets")
        if source_budget is None:
            source_budget = config.get(f"sources.{connector_name}.budget")
        if source_budget:
            configured = True
            _merge_budget_payload(
                effective,
                _budget_mapping(source_budget, f"sources.{connector_name}.budgets"),
                origin=f"sources.{connector_name}.budgets",
            )

    _validate_effective_budget(effective, connector_name=connector_name)
    return ConnectorBudget(
        connector_name=connector_name,
        limits=effective,
        configured=configured or bool(budget_config),
    )


def validate_connector_budget_config(config: ConfigLike) -> list[str]:
    errors: list[str] = []
    raw_config = config.get("connectors.budgets")
    try:
        budget_config = _budget_root(raw_config)
        connector_names = {"__default__"}
        per_connector = _per_connector_budget_mapping(budget_config)
        if per_connector:
            connector_names.update(
                str(name) for name in per_connector.keys()
            )
        sources = config.get("sources", {}) or {}
        if isinstance(sources, Mapping):
            for source_name, source_config in sources.items():
                if not isinstance(source_config, Mapping):
                    continue
                if "budgets" in source_config or "budget" in source_config:
                    connector_names.add(str(source_name))
        for connector_name in connector_names:
            resolve_connector_budget(config, connector_name)
    except ConnectorBudgetError as exc:
        errors.append(str(exc))
    return errors


def estimate_tokens(text: str | None) -> int:
    if not text:
        return 0
    return max(1, math.ceil(len(text) / 4))


def estimate_tokens_from_bytes(size_bytes: int) -> int:
    if size_bytes <= 0:
        return 0
    return max(1, math.ceil(size_bytes / 4))


def estimate_transcript_chunks(text: str | None, *, chunk_chars: int) -> int:
    if not text:
        return 0
    if chunk_chars <= 0:
        raise ConnectorBudgetError(
            "transcript_chunk_chars must be positive",
            connector_name="unknown",
            field="transcript_chunk_chars",
            observed=chunk_chars,
            limit=1,
        )
    return max(1, math.ceil(len(text) / chunk_chars))


def transcript_text_from_payload(payload: Mapping[str, Any]) -> str:
    for key in (
        "raw_transcript",
        "processed_transcript",
        "transcript",
        "text",
        "body",
    ):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def _budget_root(raw_config: Any) -> Mapping[str, Any]:
    if raw_config in (None, {}):
        return {}
    if not isinstance(raw_config, Mapping):
        raise ConnectorBudgetError(
            "connectors.budgets must be an object",
            connector_name="__default__",
            field="connectors.budgets",
        )
    allowed_top_level = _BUDGET_FIELDS | {"defaults", "per_connector", "connectors"}
    unknown = sorted(str(key) for key in raw_config if key not in allowed_top_level)
    if unknown:
        raise ConnectorBudgetError(
            "Unknown connector budget field(s): " + ", ".join(unknown),
            connector_name="__default__",
            field="connectors.budgets",
        )
    return raw_config


def _budget_mapping(value: Any, origin: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConnectorBudgetError(
            f"{origin} must be an object",
            connector_name="__default__",
            field=origin,
        )
    return value


def _per_connector_budget_mapping(budget_config: Mapping[str, Any]) -> Mapping[str, Any]:
    if "per_connector" in budget_config and "connectors" in budget_config:
        raise ConnectorBudgetError(
            "connectors.budgets cannot set both per_connector and connectors",
            connector_name="__default__",
            field="connectors.budgets",
        )
    if "per_connector" in budget_config:
        value = budget_config.get("per_connector")
        if value is None:
            return {}
        return _budget_mapping(value, "connectors.budgets.per_connector")
    if "connectors" in budget_config:
        value = budget_config.get("connectors")
        if value is None:
            return {}
        return _budget_mapping(value, "connectors.budgets.connectors")
    return {}


def _merge_budget_payload(
    effective: dict[str, Any],
    payload: Mapping[str, Any],
    *,
    origin: str,
) -> None:
    unknown = sorted(str(key) for key in payload if key not in _BUDGET_FIELDS)
    if unknown:
        raise ConnectorBudgetError(
            f"{origin} has unknown connector budget field(s): " + ", ".join(unknown),
            connector_name="__default__",
            field=origin,
        )
    for key, value in payload.items():
        if key in _INTEGER_FIELDS:
            effective[key] = _optional_int_budget_value(
                value,
                key,
                origin=origin,
                allow_zero=(key == "estimated_output_tokens_per_artifact"),
            )
        elif key in _FLOAT_FIELDS:
            effective[key] = _optional_float_budget_value(value, key, origin=origin)


def _validate_effective_budget(
    effective: Mapping[str, Any],
    *,
    connector_name: str,
) -> None:
    for key in _INTEGER_FIELDS:
        value = effective[key]
        if key in _OPTIONAL_LIMIT_FIELDS and value is None:
            continue
        minimum = 0 if key == "estimated_output_tokens_per_artifact" else 1
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            raise ConnectorBudgetError(
                f"{key} must be an integer >= {minimum}",
                connector_name=connector_name,
                field=key,
                observed=value,
                limit=minimum,
            )
    for key in _FLOAT_FIELDS:
        value = effective[key]
        if key == "max_estimated_cost_usd" and value is None:
            continue
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            raise ConnectorBudgetError(
                f"{key} must be a non-negative number",
                connector_name=connector_name,
                field=key,
                observed=value,
                limit=0,
            )


def _optional_int_budget_value(
    value: Any,
    field_name: str,
    *,
    origin: str,
    allow_zero: bool = False,
) -> int | None:
    if value is None:
        if field_name in _OPTIONAL_LIMIT_FIELDS:
            return None
        raise ConnectorBudgetError(
            f"{origin}.{field_name} cannot be null",
            connector_name="__default__",
            field=field_name,
        )
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConnectorBudgetError(
            f"{origin}.{field_name} must be an integer >= {minimum}",
            connector_name="__default__",
            field=field_name,
            observed=value,
            limit=minimum,
        )
    return value


def _optional_float_budget_value(
    value: Any,
    field_name: str,
    *,
    origin: str,
) -> float | None:
    if value is None:
        if field_name == "max_estimated_cost_usd":
            return None
        raise ConnectorBudgetError(
            f"{origin}.{field_name} cannot be null",
            connector_name="__default__",
            field=field_name,
        )
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ConnectorBudgetError(
            f"{origin}.{field_name} must be a non-negative number",
            connector_name="__default__",
            field=field_name,
            observed=value,
            limit=0,
        )
    return float(value)


def _nonnegative_int_value(value: Any, field_name: str, *, origin: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConnectorBudgetError(
            f"{origin}.{field_name} must be a non-negative integer",
            connector_name="__default__",
            field=field_name,
            observed=value,
            limit=0,
        )
    return value
