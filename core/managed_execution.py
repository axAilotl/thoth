"""Explicit Cissa-authorized collection context; never resolves credentials.

The authenticated Cissa worker constructs this envelope from its integration
record. It is not an agent tool input or an authorization mechanism by itself.
Credentials and filesystem handles are supplied separately by that worker.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .config import Config
from .path_layout import PathLayout


def utc_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("Managed collection times must be UTC with an offset")
    return parsed


@dataclass(frozen=True)
class ManagedCollectionRun:
    run_id: str
    idempotency_key: str
    account_id: str
    archive_id: str
    integration_id: str
    connector: str
    connector_version: str
    lease_generation: int
    selected_resources: tuple[str, ...]
    source_config: Mapping[str, Any]
    options: Mapping[str, Any]
    window_start: str
    window_end: str
    budgets: Mapping[str, Any]
    cursor: str | None = None

    def __post_init__(self) -> None:
        for name in ("run_id", "idempotency_key", "account_id", "archive_id",
                     "integration_id", "connector", "connector_version"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"Managed collection {name} is required")
        if type(self.lease_generation) is not int or self.lease_generation < 1:
            raise ValueError("Managed collection lease_generation must be positive")
        if not self.selected_resources or any(
            not isinstance(value, str) or not value.strip() for value in self.selected_resources
        ):
            raise ValueError("Managed collection selected_resources must be explicit")
        if utc_time(self.window_start) >= utc_time(self.window_end):
            raise ValueError("Managed collection window must have start before end")
        if not self.budgets:
            raise ValueError("Managed collection budgets are required")
        for name, value in self.budgets.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < float("inf"):
                raise ValueError(f"Managed collection budget {name} must be finite and positive")
        # Secrets travel through a separate argument, never history/config JSON.
        for config in (self.source_config, self.options):
            _reject_credentials(config)

    def identity(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in (
            "run_id", "idempotency_key", "account_id", "archive_id", "integration_id",
            "connector", "connector_version", "lease_generation",
        )}

    def configuration(self, base: Config) -> Config:
        config = Config()
        config.data = deepcopy(base.data)
        # The request's resolved source configuration is authoritative. Do not
        # inherit a token, username, selection or schedule from another runtime.
        config.set(f"sources.{self.connector}", deepcopy(dict(self.source_config)))
        config.set(f"sources.{self.connector}.enabled", True)
        config.set("connectors.actor", self.account_id)
        config.set("managed.window_start", self.window_start)
        config.set("managed.window_end", self.window_end)
        config.set("managed.cursor", self.cursor)
        config.set("managed.selected_resources", list(self.selected_resources))
        config.set("managed.run_id", self.run_id)
        for key, value in self.budgets.items():
            config.set(f"collection.{key}", value)
        return config

    def validate_paths(self, root: Path, layout: PathLayout, db: Any) -> None:
        if not root.is_absolute():
            raise ValueError("Managed account root must be absolute")
        resolved_root = root.resolve()
        for field in fields(layout):
            value = getattr(layout, field.name)
            if isinstance(value, Path) and not value.resolve().is_relative_to(resolved_root):
                raise ValueError(f"Managed {field.name} is outside account root")
        for content_root in layout.content_roots:
            if not content_root.base_path.resolve().is_relative_to(resolved_root):
                raise ValueError("Managed content root is outside account root")
        if Path(db.db_path).resolve() != layout.database_path.resolve():
            raise ValueError("Managed database does not match account layout")

    def execution_options(self, options: Mapping[str, Any] | None) -> dict[str, Any]:
        if options is not None and dict(options) != dict(self.options):
            raise ValueError("Managed options cannot override the authorized request")
        result = deepcopy(dict(self.options))
        # Discovery resources are usernames or arXiv topics/categories. Later
        # connectors validate their own named resource schema before network I/O.
        if self.connector in {"github", "huggingface"}:
            username = result.get("username") or result.get("github_user") or result.get("hf_user") or self.source_config.get("username")
            if f"{self.connector}:user:{username}" not in self.selected_resources:
                raise ValueError("Managed discovery username is outside selected resources")
        elif self.connector == "arxiv":
            key = "categories" if result.get("source", self.source_config.get("source", "api")) == "rss" else "topics"
            selected = result.get(key, self.source_config.get(key, []))
            prefix = "arxiv:category:" if key == "categories" else "arxiv:topic:"
            if not isinstance(selected, (tuple, list)) or not selected or not {prefix + str(item) for item in selected} <= set(self.selected_resources):
                raise ValueError("Managed arXiv selection is outside selected resources")
        return result


def _reject_credentials(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if any(part in str(key).lower() for part in ("token", "secret", "password", "credential", "api_key")):
                raise ValueError("Managed configuration must not contain credentials")
            _reject_credentials(item)
    elif isinstance(value, (tuple, list)):
        for item in value:
            _reject_credentials(item)
