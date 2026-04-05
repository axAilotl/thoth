"""English companion publication for non-English source documents."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

from .artifacts.web_clipper import WebClipperArtifact
from .config import Config
from .llm_interface import LLMInterface
from .metadata_db import FileMetadata, MetadataDB, get_metadata_db
from .path_layout import PathLayout, build_path_layout

DEFAULT_TRANSLATION_SYSTEM_PROMPT = (
    "You translate markdown source documents into English.\n"
    "Preserve markdown structure, headings, lists, tables, links, code blocks, and inline formatting.\n"
    "Return only valid JSON with keys 'title' and 'body'.\n"
    "'title' must be the translated title.\n"
    "'body' must be the translated markdown body without a top-level title heading.\n"
    "Do not include code fences, commentary, or extra keys."
)

ENGLISH_LANGUAGE_CODES = {
    "en",
    "en-us",
    "en-gb",
    "en-ca",
    "en-au",
    "eng",
    "english",
}


class TranslationCompanionError(RuntimeError):
    """Base error for English companion publication failures."""


class TranslationConfigurationError(TranslationCompanionError):
    """Raised when translation is enabled but misconfigured."""


class TranslationRuntimeError(TranslationCompanionError):
    """Raised when translation generation or publication fails."""


@dataclass(frozen=True)
class TranslationCompanionResult:
    """Summary of a translation publication event."""

    source_path: Path
    output_path: Path | None
    source_language: str | None
    target_language: str
    status: str
    reason: str | None = None
    source_title: str | None = None
    translated_title: str | None = None
    provider: str | None = None
    model: str | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write(content)
        if not content.endswith("\n"):
            handle.write("\n")
    os.replace(tmp_path, path)


def _read_frontmatter(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    content = path.read_text(encoding="utf-8")
    if not content.startswith("---\n"):
        return {}
    end = content.find("\n---\n", 4)
    if end == -1:
        return {}
    payload = yaml.safe_load(content[4:end]) or {}
    return payload if isinstance(payload, dict) else {}


def _render_frontmatter(data: Mapping[str, Any]) -> str:
    return "---\n" + yaml.safe_dump(
        dict(data),
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    ) + "---\n"


def _normalize_language_code(value: str | None) -> str:
    return " ".join((value or "").strip().lower().replace("_", "-").split())


def _is_english_language(value: str | None) -> bool:
    language = _normalize_language_code(value)
    if not language:
        return False
    if language in ENGLISH_LANGUAGE_CODES:
        return True
    return language.startswith("en-")


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def _strip_code_fences(value: str) -> str:
    text = value.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 2 and lines[-1].strip() == "```":
            text = "\n".join(lines[1:-1]).strip()
    return text


def _extract_vault_relative_path(layout: PathLayout, artifact: WebClipperArtifact) -> str:
    source_path = Path(artifact.source_path) if artifact.source_path else None
    if source_path:
        try:
            return source_path.resolve().relative_to(layout.vault_root).as_posix()
        except Exception:
            pass
    if artifact.source_relative_path:
        return str(artifact.source_relative_path).strip().lstrip("/")
    raise TranslationRuntimeError(
        f"Web Clipper artifact {artifact.id} does not include a usable source path"
    )


def _translation_output_path(layout: PathLayout, source_relative_path: str) -> Path:
    source_rel = Path(source_relative_path)
    if source_rel.suffix:
        translated_name = f"{source_rel.stem}.en{source_rel.suffix}"
    else:
        translated_name = f"{source_rel.name}.en.md"
    return layout.vault_root / "translations" / source_rel.with_name(translated_name)


class EnglishCompanionPublisher:
    """Generate and publish English companions for source documents."""

    def __init__(
        self,
        config: Config,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
        llm_interface: LLMInterface | None = None,
    ):
        self.config = config
        self.layout = layout or build_path_layout(config)
        self.layout.ensure_directories()
        self.db = db or get_metadata_db()
        self.llm_interface = llm_interface or self._build_llm_interface()

        if self.is_enabled() and not self.llm_interface.resolve_task_route("translation"):
            raise TranslationConfigurationError(
                "llm.tasks.translation is enabled but no translation route is configured"
            )

    def is_enabled(self) -> bool:
        return bool(self.config.get("llm.tasks.translation.enabled", False))

    async def publish_web_clipper_artifact(
        self,
        artifact: WebClipperArtifact,
        *,
        dry_run: bool = False,
    ) -> TranslationCompanionResult:
        if artifact.file_type != "note":
            return TranslationCompanionResult(
                source_path=Path(artifact.source_path or artifact.id),
                output_path=None,
                source_language=artifact.source_language,
                target_language="en",
                status="skipped",
                reason="unsupported file type",
            )

        source_language = self._resolve_source_language(artifact)
        source_path = Path(artifact.source_path or artifact.id)
        if not source_language:
            return TranslationCompanionResult(
                source_path=source_path,
                output_path=None,
                source_language=None,
                target_language="en",
                status="skipped",
                reason="missing source_language",
                source_title=artifact.title,
            )

        if _is_english_language(source_language):
            return TranslationCompanionResult(
                source_path=source_path,
                output_path=None,
                source_language=source_language,
                target_language="en",
                status="skipped",
                reason="source already English",
                source_title=artifact.title,
            )

        if not self.is_enabled():
            return TranslationCompanionResult(
                source_path=source_path,
                output_path=None,
                source_language=source_language,
                target_language="en",
                status="skipped",
                reason="translation disabled",
                source_title=artifact.title,
            )

        source_relative_path = _extract_vault_relative_path(self.layout, artifact)
        output_path = _translation_output_path(self.layout, source_relative_path)
        existing = _read_frontmatter(output_path)
        source_checksum = str(artifact.source_checksum or "").strip() or None
        if (
            existing.get("source_checksum") == source_checksum
            and _normalize_language_code(str(existing.get("translated_from") or existing.get("source_language")))
            == _normalize_language_code(source_language)
            and _normalize_language_code(str(existing.get("language") or "")) == "en"
        ):
            return TranslationCompanionResult(
                source_path=source_path,
                output_path=output_path,
                source_language=source_language,
                target_language="en",
                status="skipped",
                reason="already up to date",
                source_title=artifact.title,
                translated_title=str(existing.get("title") or ""),
            )

        translation = await self._translate_web_clipper_artifact(
            artifact,
            source_language,
        )
        created_at = str(existing.get("created_at") or _now_iso())
        updated_at = _now_iso()
        content = self._render_companion_markdown(
            artifact,
            output_path=output_path,
            source_language=source_language,
            source_relative_path=source_relative_path,
            translated_title=translation["title"],
            translated_body=translation["body"],
            created_at=created_at,
            updated_at=updated_at,
            provider=translation["provider"],
            model=translation["model"],
        )

        if dry_run:
            return TranslationCompanionResult(
                source_path=source_path,
                output_path=output_path,
                source_language=source_language,
                target_language="en",
                status="dry_run",
                source_title=artifact.title,
                translated_title=translation["title"],
                provider=translation["provider"],
                model=translation["model"],
            )

        was_existing = output_path.exists()
        _atomic_write_text(output_path, content)
        self._index_companion_file(output_path, source_relative_path)

        status = "updated" if was_existing else "created"
        return TranslationCompanionResult(
            source_path=source_path,
            output_path=output_path,
            source_language=source_language,
            target_language="en",
            status=status,
            source_title=artifact.title,
            translated_title=translation["title"],
            provider=translation["provider"],
            model=translation["model"],
        )

    def _build_llm_interface(self) -> LLMInterface | None:
        llm_config = self.config.get("llm", {})
        if not self.is_enabled():
            return None
        interface = LLMInterface(llm_config)
        return interface

    def _resolve_source_language(self, artifact: WebClipperArtifact) -> str | None:
        source_language = artifact.source_language
        if not source_language and isinstance(artifact.frontmatter, dict):
            for key in ("lang", "language", "locale"):
                value = artifact.frontmatter.get(key)
                if isinstance(value, str) and value.strip():
                    source_language = value.strip()
                    break
        return _normalize_language_code(source_language) or None

    async def _translate_web_clipper_artifact(
        self,
        artifact: WebClipperArtifact,
        source_language: str,
    ) -> dict[str, str]:
        if not self.llm_interface:
            raise TranslationConfigurationError("Translation is enabled but no LLM interface is available")

        route = self.llm_interface.resolve_task_route("translation")
        if not route:
            raise TranslationConfigurationError(
                "llm.tasks.translation is enabled but no translation route is configured"
            )

        provider, model_id, model_cfg = route
        system_prompt = (
            self.config.get("llm", {})
            .get("prompts", {})
            .get("translation", {})
            .get("system", DEFAULT_TRANSLATION_SYSTEM_PROMPT)
        )

        source_title = artifact.title or Path(artifact.source_path or artifact.id).stem
        source_body = artifact.body or artifact.raw_content or ""
        prompt = (
            f"Source language: {source_language}\n"
            f"Target language: en\n"
            f"Source title: {source_title}\n"
            f"Source markdown:\n\n{source_body}"
        )

        response = await self.llm_interface.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            provider=provider,
            model=model_id,
            max_tokens=model_cfg.get("max_tokens", 1200),
            temperature=model_cfg.get("temperature", 0.2),
        )
        if response.error:
            raise TranslationRuntimeError(f"Translation generation failed: {response.error}")

        content = _strip_code_fences(response.content or "")
        if not content.strip():
            raise TranslationRuntimeError("Translation generation returned empty content")

        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise TranslationRuntimeError(
                f"Translation response was not valid JSON: {exc}"
            ) from exc

        if not isinstance(payload, dict):
            raise TranslationRuntimeError("Translation response must be a JSON object")

        translated_title = _coerce_text(payload.get("title"))
        translated_body = _coerce_text(payload.get("body"))
        if not translated_title or not translated_body:
            raise TranslationRuntimeError(
                "Translation response must include non-empty title and body fields"
            )

        return {
            "title": translated_title,
            "body": translated_body,
            "provider": response.provider or provider,
            "model": response.model or model_id,
        }

    def _render_companion_markdown(
        self,
        artifact: WebClipperArtifact,
        *,
        output_path: Path,
        source_language: str,
        source_relative_path: str,
        translated_title: str,
        translated_body: str,
        created_at: str,
        updated_at: str,
        provider: str,
        model: str,
    ) -> str:
        source_vault_path = self._source_vault_path(artifact, source_relative_path)
        source_link = os.path.relpath(source_vault_path, output_path.parent)
        frontmatter = {
            "thoth_type": "translation_companion",
            "title": translated_title,
            "language": "en",
            "translated_from": source_language,
            "source_language": source_language,
            "source_title": artifact.title,
            "source_path": source_vault_path.relative_to(self.layout.vault_root).as_posix(),
            "source_relative_path": source_relative_path,
            "source_url": artifact.source_url,
            "source_checksum": artifact.source_checksum,
            "source_size_bytes": artifact.source_size_bytes,
            "provider": provider,
            "model": model,
            "created_at": created_at,
            "updated_at": updated_at,
        }

        lines = [
            _render_frontmatter(frontmatter).rstrip(),
            "",
            f"# {translated_title}",
            "",
            f"> English companion for [{source_relative_path}]({source_link})",
            f"> Original language: `{source_language}`",
        ]
        if artifact.source_url:
            lines.append(f"> Source URL: {artifact.source_url}")
        if artifact.title:
            lines.append(f"> Source title: {artifact.title}")
        lines.extend(["", translated_body.rstrip(), ""])
        return "\n".join(lines)

    def _source_vault_path(
        self,
        artifact: WebClipperArtifact,
        source_relative_path: str,
    ) -> Path:
        if artifact.source_path:
            source_path = Path(artifact.source_path)
            try:
                source_path.resolve().relative_to(self.layout.vault_root)
                return source_path.resolve()
            except Exception:
                pass
        return self.layout.vault_root / source_relative_path

    def _index_companion_file(self, output_path: Path, source_relative_path: str) -> None:
        try:
            rel_path = output_path.relative_to(self.layout.vault_root)
        except ValueError as exc:
            raise TranslationRuntimeError(
                f"Translation output escaped the vault root: {output_path}"
            ) from exc

        stat = output_path.stat()
        file_meta = FileMetadata(
            path=str(rel_path),
            file_type="translation",
            size_bytes=stat.st_size,
            hash=self._sha256_file(output_path),
            updated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            source_id=source_relative_path,
        )
        if not self.db.upsert_file(file_meta):
            raise TranslationRuntimeError(
                f"Failed to index translation output: {output_path}"
            )

    def _sha256_file(self, path: Path, *, chunk_size: int = 1024 * 1024) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(chunk_size)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()
