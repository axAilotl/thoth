"""Run local Pi skills and ingest their artifact envelopes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from core.config import Config, config
from core.connector_registry import (
    ConnectorManifestError,
    validate_allowed_side_effects,
    validate_manifest_outputs,
)
from core.llm_interface import LLMInterface
from core.metadata_db import MetadataDB, get_metadata_db
from core.path_layout import PathLayout, build_path_layout
from core.prompt_security import wrap_untrusted_content

from .skill_output_connector import (
    SUPPORTED_ARTIFACT_TYPES,
    SkillOutputConnector,
    reject_direct_wiki_write_claims,
)


DEFAULT_SOURCE_NAME = "pi_skill"
DEFAULT_MAX_INPUT_BYTES = 200_000
SYSTEM_PROMPT = """You are a Thoth data collection skill runner.
Return only JSON or JSONL skill output envelopes. Do not write wiki files.
Each artifact must include artifact_type and payload, and may include artifact_id,
source_name, priority, and capabilities."""


@dataclass(frozen=True)
class PiSkillDefinition:
    """Configured Pi skill profile."""

    id: str
    description: str = ""
    prompt: str = ""
    artifact_types: tuple[str, ...] = field(default_factory=tuple)
    inputs: tuple[str, ...] = field(default_factory=tuple)
    outputs: tuple[str, ...] = field(default_factory=tuple)
    auth: tuple[str, ...] = field(default_factory=tuple)
    safety_mode: str = ""
    queue_behavior: str = ""
    allowed_side_effects: tuple[str, ...] = field(default_factory=tuple)
    source_name: str = DEFAULT_SOURCE_NAME
    input_roots: tuple[Path, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class PiSkillRoute:
    """Resolved Pi provider/model route."""

    provider: str
    model: str | None = None


@dataclass(frozen=True)
class PiSkillRunResult:
    """Summary of one Pi skill run."""

    skill_id: str
    output_path: Path
    provider: str
    model: str | None
    skill_output: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "skill_id": self.skill_id,
            "output_path": str(self.output_path),
            "provider": self.provider,
            "model": self.model,
            "skill_output": self.skill_output,
            "queued_count": self.skill_output.get("queued_count", 0),
        }


class PiSkillConnector:
    """Execute configured Pi skills and queue their artifact envelopes."""

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
    ):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.db = db or get_metadata_db()

    def plan(
        self,
        *,
        skill_id: str | None = None,
        prompt: str | None = None,
        input_paths: Iterable[str | Path] | None = None,
        output_dir: str | Path | None = None,
        provider: str | None = None,
        model: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Return a dry-run execution plan without invoking Pi."""
        skill = self._resolve_skill(skill_id)
        routes = self._resolve_routes(provider=provider, model=model)
        route = routes[0]
        command = self._command_preview(route)
        route_identity = self._command_identity(route)
        resolved_inputs = self._resolve_input_paths(input_paths, skill=skill, limit=limit)
        return {
            "skill_id": skill.id,
            "allowlist": self._skill_allowlist_status(skill.id),
            "description": skill.description,
            "artifact_types": list(skill.artifact_types),
            "inputs": list(skill.inputs),
            "outputs": list(skill.outputs),
            "auth": list(skill.auth),
            "safety_mode": skill.safety_mode,
            "queue_behavior": skill.queue_behavior,
            "allowed_side_effects": list(skill.allowed_side_effects),
            "source_name": skill.source_name,
            "output_dir": str(self._resolve_output_dir(output_dir)),
            "input_paths": [str(path) for path in resolved_inputs],
            "route": {
                "provider": route.provider,
                "model": route.model,
                "command": command,
                "command_identity": route_identity,
                "pin_drift": list(route_identity["drift"]),
                "remote_install_blocked": bool(route_identity["install_if_missing"]),
            },
            "routes": [
                {
                    "provider": item.provider,
                    "model": item.model,
                    "command_identity": self._command_identity(item),
                }
                for item in routes
            ],
            "has_prompt": bool(str(prompt or "").strip() or skill.prompt.strip()),
        }

    async def collect(
        self,
        *,
        skill_id: str | None = None,
        prompt: str | None = None,
        input_paths: Iterable[str | Path] | None = None,
        output_dir: str | Path | None = None,
        provider: str | None = None,
        model: str | None = None,
        limit: int | None = None,
    ) -> PiSkillRunResult:
        """Run a Pi skill, persist its raw output, and ingest validated envelopes."""
        self.layout.ensure_directories()
        skill = self._resolve_skill(skill_id)
        routes = self._resolve_routes(provider=provider, model=model)
        self._assert_route_policy(routes)
        resolved_inputs = self._resolve_input_paths(input_paths, skill=skill, limit=limit)
        run_prompt = await asyncio.to_thread(
            self._build_prompt,
            skill,
            prompt,
            resolved_inputs,
        )
        raw_output, route = await self._generate_with_routes(run_prompt, routes)
        parsed, output_format = _parse_pi_output(raw_output)
        self._validate_payload(parsed, allowed_artifact_types=set(skill.artifact_types))
        output_path = await asyncio.to_thread(
            self._write_output,
            parsed,
            output_format=output_format,
            skill_id=skill.id,
            output_dir=self._resolve_output_dir(output_dir),
        )

        skill_output_result = await SkillOutputConnector(
            self.config,
            layout=self.layout,
            db=self.db,
            collector_name="pi_skill_connector",
        ).collect(
            output_paths=[output_path],
            source_name=skill.source_name,
            limit=limit,
        )
        return PiSkillRunResult(
            skill_id=skill.id,
            output_path=output_path,
            provider=route.provider,
            model=route.model,
            skill_output=skill_output_result.to_dict(),
        )

    def _configured(self) -> Mapping[str, Any]:
        value = self.config.get("sources.pi_skills", {}) or {}
        return value if isinstance(value, Mapping) else {}

    def _resolve_skill(self, skill_id: str | None) -> PiSkillDefinition:
        skills = self._load_skills()
        requested = _clean_string(skill_id)
        if not requested:
            if len(skills) == 1:
                skill = next(iter(skills.values()))
                self._assert_skill_allowlisted(skill.id)
                return skill
            raise ValueError("pi_skills connector requires a skill id")
        try:
            skill = skills[requested]
        except KeyError as exc:
            raise ValueError(f"Unknown Pi skill: {requested}") from exc
        self._assert_skill_allowlisted(skill.id)
        return skill

    def _skill_allowlist_status(self, skill_id: str) -> dict[str, Any]:
        allowlist = _optional_string_set(self._configured().get("allowlist"))
        if allowlist is None:
            return {
                "configured": False,
                "allowed": True,
                "matched": [],
            }
        matched = [skill_id] if skill_id in allowlist else []
        return {
            "configured": True,
            "allowed": bool(matched),
            "matched": matched,
        }

    def _assert_skill_allowlisted(self, skill_id: str) -> None:
        status = self._skill_allowlist_status(skill_id)
        if not status["allowed"]:
            raise ValueError(f"Pi skill is not allowlisted: {skill_id}")

    def _load_skills(self) -> dict[str, PiSkillDefinition]:
        configured = self._configured()
        raw_skills = configured.get("skills") or []
        if isinstance(raw_skills, Mapping):
            iterable = [
                {"id": key, **(value if isinstance(value, Mapping) else {})}
                for key, value in raw_skills.items()
            ]
        elif isinstance(raw_skills, list):
            iterable = raw_skills
        else:
            raise ValueError("sources.pi_skills.skills must be an object or array")

        skills: dict[str, PiSkillDefinition] = {}
        for raw_skill in iterable:
            if not isinstance(raw_skill, Mapping):
                raise ValueError("Pi skill definitions must be objects")
            skill_id = _clean_string(raw_skill.get("id"))
            if not skill_id:
                raise ValueError("Pi skill definition missing id")
            prompt = _clean_string(raw_skill.get("prompt") or raw_skill.get("prompt_template"))
            if not prompt and raw_skill.get("prompt_file"):
                prompt = _read_text_file(Path(str(raw_skill["prompt_file"])))
            artifact_types = tuple(
                dict.fromkeys(
                    artifact_type.lower()
                    for artifact_type in _string_list(raw_skill.get("artifact_types"))
                )
            )
            if not artifact_types:
                raise ValueError(f"Pi skill {skill_id!r} requires artifact_types")
            unsupported = set(artifact_types) - SUPPORTED_ARTIFACT_TYPES
            if unsupported:
                raise ValueError(
                    f"Pi skill {skill_id!r} declares unsupported artifact types: "
                    f"{', '.join(sorted(unsupported))}"
                )
            inputs = _required_string_list(
                raw_skill,
                "inputs",
                skill_id=skill_id,
                allow_empty=False,
            )
            outputs = _required_string_list(
                raw_skill,
                "outputs",
                skill_id=skill_id,
                allow_empty=False,
            )
            try:
                validate_manifest_outputs(outputs, origin=f"Pi skill {skill_id!r}")
            except ConnectorManifestError as exc:
                raise ValueError(str(exc)) from exc
            auth = _required_string_list(
                raw_skill,
                "auth",
                skill_id=skill_id,
                allow_empty=True,
            )
            safety_mode = _required_string(raw_skill, "safety_mode", skill_id=skill_id)
            if safety_mode != "no_tools_json":
                raise ValueError(
                    f"Pi skill {skill_id!r} safety_mode must be 'no_tools_json'"
                )
            queue_behavior = _required_string(
                raw_skill,
                "queue_behavior",
                skill_id=skill_id,
            )
            allowed_side_effects = _required_string_list(
                raw_skill,
                "allowed_side_effects",
                skill_id=skill_id,
                allow_empty=True,
            )
            try:
                validate_allowed_side_effects(
                    allowed_side_effects,
                    origin=f"Pi skill {skill_id!r}",
                )
            except ConnectorManifestError as exc:
                raise ValueError(str(exc)) from exc
            input_roots = tuple(
                self._resolve_root(path)
                for path in _string_list(raw_skill.get("input_roots"))
            )
            skills[skill_id] = PiSkillDefinition(
                id=skill_id,
                description=_clean_string(raw_skill.get("description")) or "",
                prompt=prompt or "",
                artifact_types=artifact_types,
                inputs=inputs,
                outputs=outputs,
                auth=auth,
                safety_mode=safety_mode,
                queue_behavior=queue_behavior,
                allowed_side_effects=allowed_side_effects,
                source_name=(
                    _clean_string(raw_skill.get("source_name"))
                    or f"{DEFAULT_SOURCE_NAME}:{skill_id}"
                ),
                input_roots=input_roots,
            )

        if not skills:
            raise ValueError("sources.pi_skills.skills must define at least one skill")
        return skills

    def _resolve_routes(
        self,
        *,
        provider: str | None,
        model: str | None,
    ) -> tuple[PiSkillRoute, ...]:
        configured = self._configured()
        if provider:
            routes = (PiSkillRoute(str(provider), _clean_string(model)),)
        else:
            raw_fallback = configured.get("fallback")
            route_items = raw_fallback if isinstance(raw_fallback, list) else []
            routes = tuple(
                PiSkillRoute(
                    provider=str(item.get("provider") or "").strip(),
                    model=_clean_string(item.get("model")),
                )
                for item in route_items
                if isinstance(item, Mapping) and str(item.get("provider") or "").strip()
            )
            if not routes:
                routes = (
                    PiSkillRoute(
                        provider=str(configured.get("default_provider") or "pi"),
                        model=_clean_string(configured.get("default_model"))
                        or _clean_string(model)
                        or "archivist_agent",
                    ),
                )

        providers = self.config.get("llm.providers", {}) or {}
        if not isinstance(providers, Mapping):
            raise ValueError("llm.providers must be an object for pi_skills")
        validated = []
        for route in routes:
            provider_cfg = providers.get(route.provider)
            if not isinstance(provider_cfg, Mapping):
                raise ValueError(f"Pi skill provider is not configured: {route.provider}")
            provider_type = str(provider_cfg.get("type") or route.provider)
            if provider_type != "pi":
                raise ValueError(
                    f"pi_skills provider {route.provider!r} must be type 'pi'"
                )
            validated.append(route)
        return tuple(validated)

    def _command_preview(self, route: PiSkillRoute) -> list[str]:
        provider_cfg = self.config.get(f"llm.providers.{route.provider}", {}) or {}
        if not isinstance(provider_cfg, Mapping):
            provider_cfg = {}
        model = _model_id(provider_cfg, route.model) or route.model
        command = [
            str(provider_cfg.get("command") or "pi"),
            "--print",
            "--mode",
            "text",
            "--no-tools",
            "--no-session",
            "--no-context-files",
        ]
        pi_provider = _clean_string(provider_cfg.get("pi_provider"))
        if pi_provider:
            command.extend(["--provider", pi_provider])
        if model:
            command.extend(["--model", model])
        command.append("<prompt>")
        return command

    def _command_identity(self, route: PiSkillRoute) -> dict[str, Any]:
        provider_cfg = self.config.get(f"llm.providers.{route.provider}", {}) or {}
        if not isinstance(provider_cfg, Mapping):
            provider_cfg = {}
        command = str(provider_cfg.get("command") or "pi")
        model = _model_id(provider_cfg, route.model) or route.model
        identity = {
            "provider": route.provider,
            "configured_command": command,
            "resolved_command": _resolve_command_path(command),
            "pi_provider": _clean_string(provider_cfg.get("pi_provider")),
            "model": model,
            "install_if_missing": bool(provider_cfg.get("install_if_missing", False)),
            "install_command_configured": bool(provider_cfg.get("install_command")),
        }
        pin = self._command_pin(route.provider, provider_cfg)
        drift = []
        if pin:
            field_map = {
                "command": "configured_command",
                "configured_command": "configured_command",
                "resolved_command": "resolved_command",
                "pi_provider": "pi_provider",
                "model": "model",
            }
            for pin_field, identity_field in field_map.items():
                if pin_field not in pin:
                    continue
                expected = pin.get(pin_field)
                actual = identity.get(identity_field)
                if expected != actual:
                    drift.append(
                        {
                            "field": pin_field,
                            "expected": expected,
                            "actual": actual,
                        }
                    )
        identity["pin"] = dict(pin) if pin else {}
        identity["pinned"] = bool(pin)
        identity["drift"] = drift
        return identity

    def _command_pin(
        self,
        provider: str,
        provider_cfg: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        configured = self._configured()
        pins = configured.get("command_pins") or {}
        if pins and not isinstance(pins, Mapping):
            raise ValueError("sources.pi_skills.command_pins must be an object")
        pin = pins.get(provider) if isinstance(pins, Mapping) else None
        if pin is None:
            pin = provider_cfg.get("command_pin")
        if pin is None:
            return {}
        if not isinstance(pin, Mapping):
            raise ValueError(f"Pi provider command pin must be an object: {provider}")
        return pin

    def _assert_route_policy(self, routes: Iterable[PiSkillRoute]) -> None:
        problems = []
        for route in routes:
            identity = self._command_identity(route)
            if identity["install_if_missing"]:
                problems.append(
                    f"provider {route.provider!r} enables install_if_missing"
                )
            if identity["drift"]:
                drift_fields = ", ".join(
                    str(item["field"]) for item in identity["drift"]
                )
                problems.append(
                    f"provider {route.provider!r} command pin drift: {drift_fields}"
                )
        if problems:
            raise ValueError("; ".join(problems))

    def _resolve_output_dir(self, value: str | Path | None) -> Path:
        configured = self._configured()
        raw_value = value or configured.get("output_dir")
        if raw_value:
            path = Path(raw_value).expanduser()
            if path.is_absolute():
                resolved = path
            else:
                resolved = Path.cwd() / path
        else:
            resolved = self.layout.system_root / "skill_outputs" / "pi"
        if _is_relative_to(resolved.resolve(), self.layout.wiki_root):
            raise ValueError(
                f"Pi skill output_dir cannot target direct wiki paths: {resolved}"
            )
        return resolved

    def _resolve_root(self, value: str | Path) -> Path:
        path = Path(value).expanduser()
        if path.is_absolute():
            return path.resolve()
        return (Path.cwd() / path).resolve()

    def _input_roots(self, skill: PiSkillDefinition) -> tuple[Path, ...]:
        configured = self._configured()
        default_roots = tuple(
            self._resolve_root(path)
            for path in _string_list(configured.get("default_input_roots"))
        )
        roots = skill.input_roots or default_roots
        if roots:
            return tuple(path.resolve() for path in roots)
        return (
            self.layout.vault_root.resolve(),
            self.layout.library_root.resolve(),
            self.layout.raw_root.resolve(),
        )

    def _resolve_input_paths(
        self,
        value: Iterable[str | Path] | None,
        *,
        skill: PiSkillDefinition,
        limit: int | None,
    ) -> list[Path]:
        paths = [Path(item).expanduser() for item in _string_list(value)]
        resolved: list[Path] = []
        allowed_roots = self._input_roots(skill)
        for path in paths:
            if not path.is_absolute():
                path = Path.cwd() / path
            if not path.exists():
                raise FileNotFoundError(f"Pi skill input path does not exist: {path}")
            if not path.is_file():
                raise ValueError(f"Pi skill input path must be a file: {path}")
            resolved_path = path.resolve()
            if not any(_is_relative_to(resolved_path, root) for root in allowed_roots):
                raise ValueError(
                    f"Pi skill input path is outside allowed roots: {resolved_path}"
                )
            resolved.append(resolved_path)
        if limit is not None:
            return resolved[: max(0, int(limit))]
        return resolved

    def _build_prompt(
        self,
        skill: PiSkillDefinition,
        operator_prompt: str | None,
        input_paths: Iterable[Path],
    ) -> str:
        configured = self._configured()
        max_bytes = int(
            configured.get("max_input_bytes", DEFAULT_MAX_INPUT_BYTES)
            or DEFAULT_MAX_INPUT_BYTES
        )
        sections = [
            f"Skill id: {skill.id}",
            f"Allowed artifact types: {', '.join(skill.artifact_types)}",
            "Return JSON shaped as {'artifacts': [skill output envelopes...]} or JSONL.",
            "Do not include compiled_wiki_path, page_path, thoth_slug, wiki_output_path, or wiki_path.",
        ]
        if skill.prompt:
            sections.extend(["", "Skill instructions:", skill.prompt])
        if operator_prompt and operator_prompt.strip():
            sections.extend(["", "Operator prompt:", operator_prompt.strip()])
        for path in input_paths:
            payload = path.read_bytes()[:max_bytes]
            text = payload.decode("utf-8", errors="replace")
            truncated = path.stat().st_size > max_bytes
            sections.extend(
                [
                    "",
                    f"Input file: {path}",
                    f"Truncated: {'yes' if truncated else 'no'}",
                    wrap_untrusted_content(
                        text,
                        label=f"pi_skill_input:{skill.id}:{path.name}",
                        scope="context",
                    ),
                ]
            )
        return "\n".join(sections).strip()

    async def _generate_with_routes(
        self,
        prompt: str,
        routes: Iterable[PiSkillRoute],
    ) -> tuple[str, PiSkillRoute]:
        interface = LLMInterface(self.config.get("llm", {}) or {})
        last_error = None
        for route in routes:
            if route.provider not in interface.providers:
                last_error = f"Pi provider is unavailable: {route.provider}"
                continue
            response = await interface.generate(
                prompt,
                system_prompt=SYSTEM_PROMPT,
                provider=route.provider,
                model=route.model,
            )
            if response.error:
                last_error = response.error
                continue
            return response.content, route
        raise RuntimeError(last_error or "No Pi skill route was available")

    def _write_output(
        self,
        payload: Any,
        *,
        output_format: str,
        skill_id: str,
        output_dir: Path,
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        serialized = _serialize_payload(payload, output_format=output_format)
        digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:12]
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = ".jsonl" if output_format == "jsonl" else ".json"
        output_path = output_dir / f"{_safe_slug(skill_id)}-{timestamp}-{digest}{suffix}"
        output_path.write_text(serialized, encoding="utf-8")
        return output_path

    def _validate_payload(
        self,
        payload: Any,
        *,
        allowed_artifact_types: set[str],
    ) -> None:
        envelopes = _envelope_payloads(payload)
        if not envelopes:
            raise ValueError("Pi skill output did not contain any artifact envelopes")
        for envelope in envelopes:
            if not isinstance(envelope, Mapping):
                raise ValueError("Pi skill output envelopes must be objects")
            reject_direct_wiki_write_claims(
                envelope,
                wiki_root=self.layout.wiki_root,
            )
            artifact_type = _clean_string(
                envelope.get("artifact_type") or envelope.get("type")
            )
            if not artifact_type:
                raise ValueError("Pi skill output envelope missing artifact_type")
            artifact_type = artifact_type.lower()
            if artifact_type not in SUPPORTED_ARTIFACT_TYPES:
                raise ValueError(f"Unsupported Pi skill artifact type: {artifact_type}")
            if artifact_type not in allowed_artifact_types:
                raise ValueError(
                    f"Pi skill output artifact type {artifact_type!r} is not allowed"
                )
            payload_value = envelope.get("payload")
            if isinstance(payload_value, Mapping):
                reject_direct_wiki_write_claims(
                    payload_value,
                    wiki_root=self.layout.wiki_root,
                )


def _parse_pi_output(text: str) -> tuple[Any, str]:
    clean_text = text.strip()
    if not clean_text:
        raise ValueError("Pi skill returned empty output")
    try:
        return json.loads(clean_text), "json"
    except json.JSONDecodeError as json_error:
        payloads = []
        for line in clean_text.splitlines():
            if not line.strip():
                continue
            try:
                payloads.append(json.loads(line))
            except json.JSONDecodeError as line_error:
                raise ValueError(
                    "Pi skill output must be valid JSON or JSONL"
                ) from line_error
        if not payloads:
            raise ValueError("Pi skill returned no JSONL records") from json_error
        return payloads, "jsonl"


def _serialize_payload(payload: Any, *, output_format: str) -> str:
    if output_format == "jsonl":
        envelopes = _envelope_payloads(payload)
        return "\n".join(json.dumps(item, ensure_ascii=False) for item in envelopes) + "\n"
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def _envelope_payloads(payload: Any) -> list[Any]:
    if isinstance(payload, Mapping):
        artifacts = payload.get("artifacts") or payload.get("items")
        if isinstance(artifacts, list):
            return artifacts
        return [payload]
    if isinstance(payload, list):
        return payload
    return []


def _read_text_file(path: Path) -> str:
    resolved = path.expanduser()
    if not resolved.is_absolute():
        resolved = Path.cwd() / resolved
    if not resolved.exists():
        raise FileNotFoundError(f"Pi skill prompt file does not exist: {resolved}")
    return resolved.read_text(encoding="utf-8")


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _optional_string_set(value: Any) -> set[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return {item.strip() for item in value.split(",") if item.strip()}
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    raise ValueError("Pi skill allowlist must be an array or string")


def _required_string(
    value: Mapping[str, Any],
    field_name: str,
    *,
    skill_id: str,
) -> str:
    text = _clean_string(value.get(field_name))
    if not text:
        raise ValueError(f"Pi skill {skill_id!r} requires {field_name}")
    return text


def _required_string_list(
    value: Mapping[str, Any],
    field_name: str,
    *,
    skill_id: str,
    allow_empty: bool,
) -> tuple[str, ...]:
    if field_name not in value:
        raise ValueError(f"Pi skill {skill_id!r} requires {field_name}")
    raw_items = value.get(field_name)
    if not isinstance(raw_items, (list, tuple, set)):
        raise ValueError(f"Pi skill {skill_id!r} {field_name} must be an array")
    items = tuple(str(item).strip() for item in raw_items if str(item).strip())
    if not items and not allow_empty:
        raise ValueError(f"Pi skill {skill_id!r} requires {field_name}")
    return items


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    chars = [char if char.isalnum() else "-" for char in text]
    slug = "-".join(part for part in "".join(chars).split("-") if part)
    return slug or "pi-skill"


def _model_id(provider_cfg: Mapping[str, Any], model: str | None) -> str | None:
    models = provider_cfg.get("models")
    if not isinstance(models, Mapping):
        return model
    if model and isinstance(models.get(model), Mapping):
        return _clean_string(models[model].get("id")) or model
    if model:
        return model
    default_model = models.get("default")
    if isinstance(default_model, Mapping):
        return _clean_string(default_model.get("id"))
    return None


def _resolve_command_path(command: str) -> str | None:
    resolved = shutil.which(command)
    if resolved:
        return str(Path(resolved).resolve())
    command_path = Path(command).expanduser()
    if command_path.exists():
        return str(command_path.resolve())
    return None


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root.resolve())
    except ValueError:
        return False
    return True
