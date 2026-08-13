"""Connector manifest discovery for source ingestion backends."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol

from .connector_budgets import resolve_connector_budget


class ConfigLike(Protocol):
    def get(self, key: str, default: Any = None) -> Any:
        ...


class ConnectorManifestError(ValueError):
    """Raised when a connector manifest is missing required contract fields."""


FORBIDDEN_ALLOWED_SIDE_EFFECTS = {
    "direct_wiki_write",
    "wiki_write",
    "wiki_file_write",
    "write_wiki",
    "wiki:write",
}
FORBIDDEN_DIRECT_WIKI_OUTPUTS = {
    "compiled_wiki",
    "compiled_wiki_page",
    "compiled_wiki_path",
    "direct_wiki",
    "page_path",
    "wiki",
    "wiki_file",
    "wiki_output",
    "wiki_output_path",
    "wiki_page",
    "wiki_path",
}

#: CCF lane vocabulary a manifest ``ccf`` block may declare. The lane is
#: carried onto mirrored artifacts by the dual-write thothmap, so the set
#: is closed: an unknown lane fails manifest loading instead of silently
#: inventing archive vocabulary. ``mixed`` covers wildcard connectors
#: whose artifact types span several lanes.
CCF_LANES = frozenset(
    {
        "markdown",
        "mixed",
        "paper",
        "repository",
        "transcript",
        "tweet",
        "video",
        "web_clipper",
    }
)

_CCF_ROLE_TOKEN = re.compile(r"[a-z][a-z0-9_]*")
#: Extension keys must be namespaced dotted keys (``thoth.lane``), per the
#: ``thoth.<...>`` precedent in ``ccf.thothmap.semantic``.
_CCF_EXTENSION_KEY = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z0-9_]+)+")
_CCF_BLOCK_FIELDS = {"lane", "artifact_role", "extensions"}


@dataclass(frozen=True)
class ConnectorCcfBlock:
    """Optional CCF lane declaration of a connector manifest.

    Consumed by the dual-write mirror (``ccf.dualwrite`` /
    ``ccf.thothmap``): mirrored artifacts carry the lane and the block's
    namespaced extensions instead of one generic role. ``None`` on the
    manifest means legacy generic behavior.
    """

    lane: str
    artifact_role: str
    extensions: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "artifact_role": self.artifact_role,
            "extensions": dict(self.extensions),
        }


def _parse_ccf_block(value: Any, *, origin: str) -> "ConnectorCcfBlock | None":
    """Validate the optional manifest ``ccf`` block (fail closed)."""
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ConnectorManifestError(
            f"{origin}: connector manifest 'ccf' block must be an object"
        )
    unknown_fields = sorted(set(value) - _CCF_BLOCK_FIELDS)
    if unknown_fields:
        raise ConnectorManifestError(
            f"{origin}: connector manifest 'ccf' block has unknown fields "
            f"{unknown_fields}"
        )

    lane = value.get("lane")
    if not isinstance(lane, str) or not lane.strip():
        raise ConnectorManifestError(
            f"{origin}: connector manifest 'ccf' block requires a non-empty lane"
        )
    lane = lane.strip()
    if lane not in CCF_LANES:
        raise ConnectorManifestError(
            f"{origin}: unknown ccf lane {lane!r}; known lanes: {sorted(CCF_LANES)}"
        )

    role = value.get("artifact_role")
    if not isinstance(role, str) or not _CCF_ROLE_TOKEN.fullmatch(role.strip()):
        raise ConnectorManifestError(
            f"{origin}: connector manifest 'ccf' block requires a well-formed "
            "artifact_role token"
        )

    raw_extensions = value.get("extensions", {})
    if not isinstance(raw_extensions, Mapping):
        raise ConnectorManifestError(
            f"{origin}: connector manifest 'ccf' extensions must be an object"
        )
    extensions: dict[str, Any] = {}
    for key, extension_value in raw_extensions.items():
        if not isinstance(key, str) or not _CCF_EXTENSION_KEY.fullmatch(key):
            raise ConnectorManifestError(
                f"{origin}: ccf extension key {key!r} must be a namespaced "
                "dotted key (e.g. 'thoth.lane')"
            )
        if extension_value is not None and not isinstance(
            extension_value, (str, int, float, bool)
        ):
            raise ConnectorManifestError(
                f"{origin}: ccf extension {key!r} must be a scalar value"
            )
        extensions[key] = extension_value

    return ConnectorCcfBlock(
        lane=lane,
        artifact_role=role.strip(),
        extensions=extensions,
    )


@dataclass(frozen=True)
class ConnectorManifest:
    """Declarative metadata for a connector that can produce artifacts."""

    name: str
    source_name: str
    display_name: str
    artifact_types: tuple[str, ...]
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    entrypoint: str
    queue_capability: bool
    safety_mode: str
    queue_behavior: str
    allowed_side_effects: tuple[str, ...]
    capabilities: tuple[str, ...] = field(default_factory=tuple)
    config_keys: tuple[str, ...] = field(default_factory=tuple)
    auth: tuple[str, ...] = field(default_factory=tuple)
    source_aliases: tuple[str, ...] = field(default_factory=tuple)
    runner: str | None = None
    cli_command: str | None = None
    config_namespace: str | None = None
    default_enabled: bool = True
    description: str = ""
    origin: str = "builtin"
    ccf: ConnectorCcfBlock | None = None

    @property
    def source_names(self) -> tuple[str, ...]:
        names = [self.source_name, *self.source_aliases]
        return tuple(dict.fromkeys(name for name in names if name))

    def is_enabled(self, config: ConfigLike | None = None) -> bool:
        if config is None or not self.config_namespace:
            return self.default_enabled
        source_config = config.get(self.config_namespace, None)
        if not isinstance(source_config, Mapping):
            return self.default_enabled
        return bool(source_config.get("enabled", self.default_enabled))

    def to_dict(self, *, config: ConfigLike | None = None) -> dict[str, Any]:
        payload = {
            "name": self.name,
            "source_name": self.source_name,
            "source_aliases": list(self.source_aliases),
            "source_names": list(self.source_names),
            "runner": self.runner or self.name,
            "display_name": self.display_name,
            "description": self.description,
            "artifact_types": list(self.artifact_types),
            "inputs": list(self.inputs),
            "outputs": list(self.outputs),
            "entrypoint": self.entrypoint,
            "queue_capability": self.queue_capability,
            "queue_behavior": self.queue_behavior,
            "safety_mode": self.safety_mode,
            "allowed_side_effects": list(self.allowed_side_effects),
            "capabilities": list(self.capabilities),
            "config_keys": list(self.config_keys),
            "auth": list(self.auth),
            "cli_command": self.cli_command,
            "config_namespace": self.config_namespace,
            "enabled": self.is_enabled(config),
            "origin": self.origin,
            "ccf": self.ccf.to_dict() if self.ccf is not None else None,
        }
        if config is not None:
            payload["policy"] = connector_policy_status(self, config)
            payload["budgets"] = resolve_connector_budget(config, self.name).to_dict()
        return payload

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        origin: str,
    ) -> "ConnectorManifest":
        name = _required_string(value, "name", origin=origin)
        source_name = _required_string(value, "source_name", origin=origin)
        display_name = _optional_string(value.get("display_name")) or name
        entrypoint = _required_string(value, "entrypoint", origin=origin)
        artifact_types = _required_string_tuple(
            value.get("artifact_types"),
            field_name="artifact_types",
            origin=origin,
        )
        inputs = _required_string_tuple(
            value.get("inputs"),
            field_name="inputs",
            origin=origin,
        )
        outputs = _required_string_tuple(
            value.get("outputs"),
            field_name="outputs",
            origin=origin,
        )
        validate_manifest_outputs(outputs, origin=origin)
        capabilities = _string_tuple(value.get("capabilities"))
        config_keys = _string_tuple(value.get("config_keys"))
        auth = _required_string_tuple(
            value.get("auth"),
            field_name="auth",
            origin=origin,
            allow_empty=True,
        )
        safety_mode = _required_string(value, "safety_mode", origin=origin)
        queue_behavior = _required_string(value, "queue_behavior", origin=origin)
        allowed_side_effects = _required_string_tuple(
            value.get("allowed_side_effects"),
            field_name="allowed_side_effects",
            origin=origin,
            allow_empty=True,
        )
        validate_allowed_side_effects(allowed_side_effects, origin=origin)
        source_aliases = _string_tuple(value.get("source_aliases"))
        runner = _optional_string(value.get("runner"))
        cli_command = _optional_string(value.get("cli_command"))
        config_namespace = _optional_string(value.get("config_namespace"))
        description = _optional_string(value.get("description")) or ""
        default_enabled = bool(value.get("default_enabled", True))
        ccf = _parse_ccf_block(value.get("ccf"), origin=origin)

        queue_capability = value.get("queue_capability")
        if not isinstance(queue_capability, bool):
            raise ConnectorManifestError(
                f"{origin}: connector {name!r} requires boolean queue_capability"
            )

        return cls(
            name=name,
            source_name=source_name,
            display_name=display_name,
            artifact_types=artifact_types,
            inputs=inputs,
            outputs=outputs,
            entrypoint=entrypoint,
            queue_capability=queue_capability,
            safety_mode=safety_mode,
            queue_behavior=queue_behavior,
            allowed_side_effects=allowed_side_effects,
            capabilities=capabilities,
            config_keys=config_keys,
            auth=auth,
            source_aliases=source_aliases,
            runner=runner,
            cli_command=cli_command,
            config_namespace=config_namespace,
            default_enabled=default_enabled,
            description=description,
            origin=origin,
            ccf=ccf,
        )


class ConnectorRegistry:
    """Resolved connector manifests in discovery order."""

    def __init__(self, manifests: Iterable[ConnectorManifest]):
        self._manifests = tuple(manifests)
        self._by_name = {manifest.name: manifest for manifest in self._manifests}
        self._by_source_name: dict[str, ConnectorManifest] = {}
        for manifest in self._manifests:
            for source_name in manifest.source_names:
                self._by_source_name.setdefault(source_name, manifest)

    def list(self) -> list[ConnectorManifest]:
        return list(self._manifests)

    def get(self, name: str) -> ConnectorManifest:
        manifest = self._by_name.get(name) or self._by_source_name.get(name)
        if manifest is None:
            raise KeyError(f"Unknown connector: {name}")
        return manifest

    def to_dict(self, *, config: ConfigLike | None = None) -> dict[str, Any]:
        return {
            "connectors": [
                manifest.to_dict(config=config) for manifest in self._manifests
            ],
            "total": len(self._manifests),
        }


def connector_policy_status(
    manifest: ConnectorManifest,
    config: ConfigLike,
) -> dict[str, Any]:
    """Return allowlist and pin status for a connector manifest."""
    identity_names = tuple(dict.fromkeys((manifest.name, *manifest.source_names)))
    allowlist = _optional_string_set(config.get("connectors.allowlist"))
    if allowlist is None:
        allowlist_status = {
            "configured": False,
            "allowed": True,
            "matched": [],
        }
    else:
        matched = [name for name in identity_names if name in allowlist]
        allowlist_status = {
            "configured": True,
            "allowed": bool(matched),
            "matched": matched,
        }

    return {
        "allowlist": allowlist_status,
        "pins": _connector_pin_status(manifest, config, identity_names),
    }


def _connector_pin_status(
    manifest: ConnectorManifest,
    config: ConfigLike,
    identity_names: tuple[str, ...],
) -> dict[str, Any]:
    pins = config.get("connectors.pins", {}) or {}
    if not isinstance(pins, Mapping):
        raise ConnectorManifestError("connectors.pins must be an object")
    matched_key = next((name for name in identity_names if name in pins), None)
    if matched_key is None:
        return {
            "configured": False,
            "matched": None,
            "drift": [],
        }
    pin = pins.get(matched_key) or {}
    if not isinstance(pin, Mapping):
        raise ConnectorManifestError(
            f"connectors.pins.{matched_key} must be an object"
        )

    actual = {
        "name": manifest.name,
        "source_name": manifest.source_name,
        "entrypoint": manifest.entrypoint,
        "cli_command": manifest.cli_command,
        "origin": manifest.origin,
    }
    drift = []
    for field_name, actual_value in actual.items():
        if field_name not in pin:
            continue
        expected_value = pin.get(field_name)
        if expected_value != actual_value:
            drift.append(
                {
                    "field": field_name,
                    "expected": expected_value,
                    "actual": actual_value,
                }
            )
    return {
        "configured": True,
        "matched": matched_key,
        "drift": drift,
    }


# Built-in manifests live next to the collector code and are discovered through
# the same file machinery as plugin manifests. The path is resolved relative to
# this package, never the process cwd, so discovery is location-independent.
BUILTIN_MANIFEST_DIR = Path(__file__).resolve().parent.parent / "collectors"


def load_connector_registry(
    config: ConfigLike | None = None,
    *,
    project_root: Path | None = None,
) -> ConnectorRegistry:
    """Discover built-in connectors first, then optional plugin manifests."""
    manifests: list[ConnectorManifest] = []
    names: set[str] = set()

    for manifest_path in _iter_builtin_manifest_paths():
        manifest = _load_manifest_file(manifest_path, origin="builtin")
        _register_manifest(manifests, names, manifest, manifest_path)

    if not manifests:
        raise ConnectorManifestError(
            f"{BUILTIN_MANIFEST_DIR}: no built-in connector manifests discovered"
        )

    for manifest_path in _iter_plugin_manifest_paths(config, project_root=project_root):
        manifest = _load_manifest_file(manifest_path)
        _register_manifest(manifests, names, manifest, manifest_path)

    return ConnectorRegistry(manifests)


def _register_manifest(
    manifests: list[ConnectorManifest],
    names: set[str],
    manifest: ConnectorManifest,
    manifest_path: Path,
) -> None:
    if manifest.name in names:
        raise ConnectorManifestError(
            f"{manifest_path}: duplicate connector name {manifest.name!r}"
        )
    manifests.append(manifest)
    names.add(manifest.name)


def _load_manifest_file(path: Path, *, origin: str | None = None) -> ConnectorManifest:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConnectorManifestError(f"{path}: invalid connector JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ConnectorManifestError(f"{path}: connector manifest must be an object")
    return ConnectorManifest.from_mapping(payload, origin=origin or str(path))


def _iter_builtin_manifest_paths() -> Iterable[Path]:
    return _manifest_paths_under(BUILTIN_MANIFEST_DIR)


def _iter_plugin_manifest_paths(
    config: ConfigLike | None,
    *,
    project_root: Path | None,
) -> Iterable[Path]:
    root = project_root or Path.cwd()
    raw_dirs: list[str] = []
    if config is not None:
        raw_dirs.extend(_configured_paths(config.get("connectors.plugin_dirs")))
        raw_dirs.extend(_configured_paths(config.get("connectors.skill_dirs")))
        raw_dirs.extend(_configured_paths(config.get("skills.connector_dirs")))
    env_path = os.getenv("THOTH_CONNECTOR_PATH")
    if env_path:
        raw_dirs.extend(item for item in env_path.split(os.pathsep) if item.strip())

    seen: set[Path] = set()
    for raw_dir in raw_dirs:
        candidate = Path(raw_dir).expanduser()
        if not candidate.is_absolute():
            candidate = root / candidate
        candidate = candidate.resolve()
        for manifest_path in _manifest_paths_under(candidate):
            if manifest_path in seen:
                continue
            seen.add(manifest_path)
            yield manifest_path


def _manifest_paths_under(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    if not path.exists():
        return
    direct_names = ("connector.json", "manifest.json")
    for name in direct_names:
        candidate = path / name
        if candidate.is_file():
            yield candidate
    for candidate in sorted(path.glob("*.connector.json")):
        if candidate.is_file():
            yield candidate
    for child in sorted(path.iterdir(), key=lambda item: item.name):
        if not child.is_dir():
            continue
        for name in direct_names:
            candidate = child / name
            if candidate.is_file():
                yield candidate


def _configured_paths(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(os.pathsep) if item.strip()]
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ConnectorManifestError("connector directory configuration must be a string or list")


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_string(
    value: Mapping[str, Any],
    field_name: str,
    *,
    origin: str,
) -> str:
    text = _optional_string(value.get(field_name))
    if not text:
        raise ConnectorManifestError(f"{origin}: connector manifest requires {field_name}")
    return text


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ConnectorManifestError("connector manifest list fields must be arrays")
    return tuple(str(item).strip() for item in value if str(item).strip())


def _optional_string_set(value: Any) -> set[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return {item.strip() for item in value.split(",") if item.strip()}
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    raise ConnectorManifestError("connector allowlist must be an array or string")


def _required_string_tuple(
    value: Any,
    *,
    field_name: str,
    origin: str,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if value is None:
        raise ConnectorManifestError(f"{origin}: connector manifest requires {field_name}")
    items = _string_tuple(value)
    if not items and not allow_empty:
        raise ConnectorManifestError(f"{origin}: connector manifest requires {field_name}")
    return items


def validate_allowed_side_effects(
    side_effects: Iterable[str],
    *,
    origin: str,
) -> None:
    for side_effect in side_effects:
        normalized = str(side_effect).strip().lower().replace("-", "_")
        if normalized in FORBIDDEN_ALLOWED_SIDE_EFFECTS:
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot allow direct wiki writes"
            )
        if "wiki" in normalized and "write" in normalized:
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot allow direct wiki writes"
            )


def validate_manifest_outputs(
    outputs: Iterable[str],
    *,
    origin: str,
) -> None:
    """Reject connector output contracts that target compiled wiki files directly."""
    for output in outputs:
        normalized = str(output).strip().lower().replace("\\", "/").replace("-", "_")
        if not normalized:
            continue
        output_parts = [
            part for part in normalized.replace(":", "/").split("/") if part
        ]
        if normalized in FORBIDDEN_DIRECT_WIKI_OUTPUTS:
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot declare direct wiki outputs"
            )
        if any(part in FORBIDDEN_DIRECT_WIKI_OUTPUTS for part in output_parts):
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot declare direct wiki outputs"
            )
        if normalized.startswith(("wiki/", "./wiki/", "../wiki/")):
            raise ConnectorManifestError(
                f"{origin}: connector manifest cannot declare direct wiki outputs"
            )
