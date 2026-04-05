"""Prompt loading helpers for staged archivist compilation."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ..config import Config

DEFAULT_ARCHIVIST_FINAL_SYSTEM_PROMPT = "prompts/archivist_system.md"
DEFAULT_ARCHIVIST_FINAL_USER_PROMPT = "prompts/archivist_user.md"
DEFAULT_ARCHIVIST_SOURCE_SYSTEM_PROMPT = "prompts/archivist_source_system.md"
DEFAULT_ARCHIVIST_SOURCE_USER_PROMPT = "prompts/archivist_source_user.md"
DEFAULT_ARCHIVIST_REPOSITORY_SYSTEM_PROMPT = "prompts/archivist_repository_system.md"
DEFAULT_ARCHIVIST_REPOSITORY_USER_PROMPT = "prompts/archivist_repository_user.md"


class ArchivistPromptError(ValueError):
    """Raised when staged archivist prompt files are missing or invalid."""


def load_source_prompt_bundle(
    config: Config,
    *,
    project_root: Path,
    source_type: str,
    context: Mapping[str, Any],
) -> tuple[str, str]:
    """Return the system/user prompt pair for a single source-type brief."""

    prompt_namespace = "repository" if source_type == "repository" else "source"
    system_prompt_path = resolve_prompt_path(
        config,
        f"llm.prompts.archivist.{prompt_namespace}_system_file",
        _default_source_system_path(source_type),
        project_root=project_root,
    )
    user_prompt_path = resolve_prompt_path(
        config,
        f"llm.prompts.archivist.{prompt_namespace}_user_file",
        _default_source_user_path(source_type),
        project_root=project_root,
    )
    return (
        read_prompt_file(system_prompt_path),
        render_prompt_template(read_prompt_file(user_prompt_path), context, prompt_path=user_prompt_path),
    )


def load_final_prompt_bundle(
    config: Config,
    *,
    project_root: Path,
    context: Mapping[str, Any],
) -> tuple[str, str]:
    """Return the system/user prompt pair for final archivist synthesis."""

    system_prompt_path = resolve_prompt_path(
        config,
        "llm.prompts.archivist.system_file",
        DEFAULT_ARCHIVIST_FINAL_SYSTEM_PROMPT,
        project_root=project_root,
    )
    user_prompt_path = resolve_prompt_path(
        config,
        "llm.prompts.archivist.user_file",
        DEFAULT_ARCHIVIST_FINAL_USER_PROMPT,
        project_root=project_root,
    )
    return (
        read_prompt_file(system_prompt_path),
        render_prompt_template(read_prompt_file(user_prompt_path), context, prompt_path=user_prompt_path),
    )


def resolve_prompt_path(
    config: Config,
    config_key: str,
    default_value: str,
    *,
    project_root: Path,
) -> Path:
    raw_value = config.get(config_key)
    candidate = Path(str(raw_value).strip()) if raw_value else Path(default_value)
    if candidate.is_absolute():
        return candidate
    return project_root / candidate


def read_prompt_file(path: Path) -> str:
    if not path.exists():
        raise ArchivistPromptError(f"Archivist prompt file not found: {path}")
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        raise ArchivistPromptError(f"Archivist prompt file is empty: {path}")
    return content


def render_prompt_template(
    template: str,
    context: Mapping[str, Any],
    *,
    prompt_path: Path,
) -> str:
    try:
        return template.format(**context)
    except KeyError as exc:
        raise ArchivistPromptError(
            f"Archivist prompt template {prompt_path} has an unknown placeholder: {exc}"
        ) from exc


def _default_source_system_path(source_type: str) -> str:
    if source_type == "repository":
        return DEFAULT_ARCHIVIST_REPOSITORY_SYSTEM_PROMPT
    return DEFAULT_ARCHIVIST_SOURCE_SYSTEM_PROMPT


def _default_source_user_path(source_type: str) -> str:
    if source_type == "repository":
        return DEFAULT_ARCHIVIST_REPOSITORY_USER_PROMPT
    return DEFAULT_ARCHIVIST_SOURCE_USER_PROMPT
