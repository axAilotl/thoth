"""Archivist topic compiler for baseline multi-source wiki pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any, Sequence

from .archivist_retrieval.service import select_archivist_candidates_async
from .archivist_selection import ArchivistCandidate
from .archivist_state import evaluate_archivist_dirty_check, record_archivist_topic_run
from .archivist_topics import (
    ArchivistTopicDefinition,
    ArchivistTopicRegistry,
    load_archivist_topic_registry,
)
from .config import Config
from .llm_interface import LLMInterface
from .metadata_db import MetadataDB, get_metadata_db
from .path_layout import PathLayout, build_path_layout
from .wiki_contract import WikiContract, WikiPageSpec, build_wiki_contract
from .wiki_io import atomic_write_text, read_document, render_frontmatter, truncate_summary
from .wiki_scaffold import append_wiki_log_entry, ensure_wiki_scaffold
from .wiki_updater import CompiledWikiUpdater

DEFAULT_ARCHIVIST_SYSTEM_PROMPT = "prompts/archivist_system.md"
DEFAULT_ARCHIVIST_USER_PROMPT = "prompts/archivist_user.md"


class ArchivistCompilerError(ValueError):
    """Raised when the archivist compiler cannot run safely."""


@dataclass(frozen=True)
class ArchivistCompileResult:
    """Outcome of evaluating or compiling a single archivist topic."""

    topic_id: str
    status: str
    reason: str
    page_path: Path | None
    candidate_count: int
    source_paths: tuple[str, ...]
    model_provider: str | None
    model: str | None


class ArchivistCompiler:
    """Compile configured archivist topics into baseline wiki pages."""

    def __init__(
        self,
        config: Config,
        *,
        project_root: Path | None = None,
        layout: PathLayout | None = None,
        contract: WikiContract | None = None,
        db: MetadataDB | None = None,
        llm_interface: LLMInterface | None = None,
    ):
        self.config = config
        self.project_root = project_root or Path.cwd()
        self.layout = layout or build_path_layout(config, project_root=self.project_root)
        self.layout.ensure_directories()
        self.contract = contract or build_wiki_contract(config, project_root=self.project_root)
        self.db = db or get_metadata_db()
        self.llm_interface = llm_interface or LLMInterface(config.get("llm", {}))
        self.scaffold = ensure_wiki_scaffold(config, project_root=self.project_root)

    async def run(
        self,
        *,
        topic_ids: Sequence[str] | None = None,
        force: bool = False,
        dry_run: bool = False,
        limit: int | None = None,
    ) -> list[ArchivistCompileResult]:
        registry = load_archivist_topic_registry(
            self.config,
            project_root=self.project_root,
            required=True,
        )
        topics = self._select_topics(registry, topic_ids=topic_ids, limit=limit)
        results: list[ArchivistCompileResult] = []
        for topic in topics:
            results.append(await self.compile_topic(topic, force=force, dry_run=dry_run))
        return results

    async def compile_topic(
        self,
        topic: ArchivistTopicDefinition,
        *,
        force: bool = False,
        dry_run: bool = False,
    ) -> ArchivistCompileResult:
        route = self._resolve_archivist_route()
        selection = await select_archivist_candidates_async(
            topic,
            config=self.config,
            layout=self.layout,
            db=self.db,
            llm_interface=self.llm_interface,
        )
        candidates = selection.candidates
        dirty = evaluate_archivist_dirty_check(
            topic,
            candidates,
            route=route,
            db=self.db,
        )

        should_run = force or dirty.should_run
        if not should_run:
            return ArchivistCompileResult(
                topic_id=topic.id,
                status="skipped",
                reason=dirty.reason,
                page_path=topic.output_path_for_root(self.layout.wiki_root),
                candidate_count=len(candidates),
                source_paths=self._source_paths_for_candidates(candidates),
                model_provider=route[0],
                model=route[1],
            )

        if not candidates:
            if not dry_run:
                record_archivist_topic_run(
                    topic,
                    candidates,
                    route=route,
                    db=self.db,
                    succeeded=True,
                )
                append_wiki_log_entry(
                    self.scaffold,
                    f"Archivist skipped `{topic.id}` because no matching source candidates were selected.",
                )
            return ArchivistCompileResult(
                topic_id=topic.id,
                status="skipped",
                reason="no_candidates",
                page_path=topic.output_path_for_root(self.layout.wiki_root),
                candidate_count=0,
                source_paths=(),
                model_provider=route[0],
                model=route[1],
            )

        page_path = topic.output_path_for_root(self.layout.wiki_root)
        if dry_run:
            return ArchivistCompileResult(
                topic_id=topic.id,
                status="dry_run",
                reason="forced" if force else dirty.reason,
                page_path=page_path,
                candidate_count=len(candidates),
                source_paths=self._source_paths_for_candidates(candidates),
                model_provider=route[0],
                model=route[1],
            )

        body = await self._generate_topic_body(topic, candidates, route=route)
        self._write_topic_page(topic, candidates, body=body, page_path=page_path)
        record_archivist_topic_run(
            topic,
            candidates,
            route=route,
            db=self.db,
            succeeded=True,
        )
        append_wiki_log_entry(
            self.scaffold,
            f"Archivist compiled `{topic.id}` from `{len(candidates)}` source(s).",
        )
        CompiledWikiUpdater(
            self.config,
            layout=self.layout,
            contract=self.contract,
        ).refresh_index()
        return ArchivistCompileResult(
            topic_id=topic.id,
            status="compiled",
            reason="forced" if force else dirty.reason,
            page_path=page_path,
            candidate_count=len(candidates),
            source_paths=self._source_paths_for_candidates(candidates),
            model_provider=route[0],
            model=route[1],
        )

    def _select_topics(
        self,
        registry: ArchivistTopicRegistry,
        *,
        topic_ids: Sequence[str] | None,
        limit: int | None,
    ) -> tuple[ArchivistTopicDefinition, ...]:
        topics = registry.topics
        if topic_ids:
            wanted = {str(topic_id).strip().lower() for topic_id in topic_ids if str(topic_id).strip()}
            selected = tuple(topic for topic in topics if topic.id in wanted)
            missing = sorted(wanted.difference({topic.id for topic in selected}))
            if missing:
                raise ArchivistCompilerError(
                    "Unknown archivist topics: " + ", ".join(missing)
                )
            topics = selected
        if limit is not None:
            topics = topics[: max(0, int(limit))]
        return topics

    def _resolve_archivist_route(self) -> tuple[str, str, dict[str, Any]]:
        route = self.llm_interface.resolve_task_route("archivist")
        if route is None:
            raise ArchivistCompilerError(
                "No archivist LLM route is configured. Enable llm.tasks.archivist and provide a valid fallback route."
            )
        return route

    async def _generate_topic_body(
        self,
        topic: ArchivistTopicDefinition,
        candidates: Sequence[ArchivistCandidate],
        *,
        route: tuple[str, str, dict[str, Any]],
    ) -> str:
        system_prompt, user_prompt = self._render_prompts(topic, candidates)
        provider_name, model_id, model_cfg = route
        response = await self.llm_interface.generate(
            prompt=user_prompt,
            system_prompt=system_prompt,
            provider=provider_name,
            model=model_id,
            max_tokens=int(model_cfg.get("max_tokens", 1800) or 1800),
            temperature=float(model_cfg.get("temperature", 0.2) or 0.2),
        )
        if response.error:
            raise ArchivistCompilerError(
                f"Archivist generation failed for topic {topic.id}: {response.error}"
            )
        content = (response.content or "").strip()
        if not content:
            raise ArchivistCompilerError(
                f"Archivist generation returned empty content for topic {topic.id}"
            )
        return self._normalize_llm_body(topic.title, content)

    def _render_prompts(
        self,
        topic: ArchivistTopicDefinition,
        candidates: Sequence[ArchivistCandidate],
    ) -> tuple[str, str]:
        system_prompt_path = self._resolve_prompt_path(
            "llm.prompts.archivist.system_file",
            DEFAULT_ARCHIVIST_SYSTEM_PROMPT,
        )
        user_prompt_path = self._resolve_prompt_path(
            "llm.prompts.archivist.user_file",
            DEFAULT_ARCHIVIST_USER_PROMPT,
        )
        system_prompt = self._read_prompt_file(system_prompt_path)
        user_template = self._read_prompt_file(user_prompt_path)
        manifest = self._render_source_manifest(candidates)
        try:
            user_prompt = user_template.format(
                topic_id=topic.id,
                topic_title=topic.title,
                topic_description=topic.description or "No additional topic description provided.",
                candidate_count=len(candidates),
                source_manifest=manifest,
            )
        except KeyError as exc:
            raise ArchivistCompilerError(
                f"Archivist user prompt template {user_prompt_path} has an unknown placeholder: {exc}"
            ) from exc
        return system_prompt, user_prompt

    def _resolve_prompt_path(self, config_key: str, default_value: str) -> Path:
        raw_value = self.config.get(config_key)
        candidate = Path(str(raw_value).strip()) if raw_value else Path(default_value)
        if candidate.is_absolute():
            return candidate
        return self.project_root / candidate

    def _read_prompt_file(self, path: Path) -> str:
        if not path.exists():
            raise ArchivistCompilerError(f"Archivist prompt file not found: {path}")
        content = path.read_text(encoding="utf-8").strip()
        if not content:
            raise ArchivistCompilerError(f"Archivist prompt file is empty: {path}")
        return content

    def _render_source_manifest(self, candidates: Sequence[ArchivistCandidate]) -> str:
        rendered: list[str] = []
        for index, candidate in enumerate(candidates, start=1):
            excerpt = self._candidate_excerpt(candidate)
            tags = ", ".join(candidate.tags) if candidate.tags else "none"
            rendered.extend(
                [
                    f"[S{index}]",
                    f"- Title: {candidate.title}",
                    f"- Source Type: {candidate.source_type}",
                    f"- File Type: {candidate.file_type}",
                    f"- Scope Path: {self._normalized_source_path(candidate)}",
                    f"- Tags: {tags}",
                    f"- Updated At: {candidate.updated_at}",
                    f"- Source ID: {candidate.source_id or 'n/a'}",
                    "- Excerpt:",
                    excerpt,
                    "",
                ]
            )
        return "\n".join(rendered).strip()

    def _candidate_excerpt(self, candidate: ArchivistCandidate, *, limit: int = 1800) -> str:
        text = " ".join((candidate.content_text or "").split())
        if text:
            if len(text) > limit:
                return text[: limit - 1].rstrip() + "..."
            return text
        return "[No extracted text available. Rely on title, path, type, and tags only.]"

    def _write_topic_page(
        self,
        topic: ArchivistTopicDefinition,
        candidates: Sequence[ArchivistCandidate],
        *,
        body: str,
        page_path: Path,
    ) -> None:
        page_path.parent.mkdir(parents=True, exist_ok=True)
        existing = read_document(page_path) if page_path.exists() else None
        created_at = str(
            (existing.frontmatter.get("created_at") if existing else None)
            or self._now_iso()
        )
        spec = WikiPageSpec(
            title=topic.title,
            slug=page_path.stem,
            kind="topic",
            summary=self._summary_from_body(body, fallback=topic.description or topic.title),
            source_paths=self._source_paths_for_candidates(candidates),
            language="en",
            created_at=created_at,
            updated_at=self._now_iso(),
        )
        frontmatter = render_frontmatter(self.contract.frontmatter_for(spec)).rstrip()
        source_lines = self._render_sources_section(candidates, page_path=page_path)
        content = "\n".join(
            [
                frontmatter,
                "",
                f"# {topic.title}",
                "",
                body.strip(),
                "",
                "## Sources",
                "",
                *source_lines,
                "",
            ]
        )
        atomic_write_text(page_path, content)

    def _render_sources_section(
        self,
        candidates: Sequence[ArchivistCandidate],
        *,
        page_path: Path,
    ) -> list[str]:
        lines: list[str] = []
        page_dir = page_path.parent
        for index, candidate in enumerate(candidates, start=1):
            source_abs_path = self._absolute_path_for_candidate(candidate)
            rel_link = os.path.relpath(source_abs_path, page_dir)
            tags = ", ".join(candidate.tags) if candidate.tags else "none"
            lines.append(f"- [S{index}] [{candidate.title}]({rel_link})")
            lines.append(f"  - Path: `{self._normalized_source_path(candidate)}`")
            lines.append(f"  - Type: `{candidate.source_type}` / `{candidate.file_type}`")
            lines.append(f"  - Tags: `{tags}`")
            lines.append(f"  - Updated: `{candidate.updated_at}`")
        return lines

    def _absolute_path_for_candidate(self, candidate: ArchivistCandidate) -> Path:
        if candidate.scope == "vault":
            return self.layout.vault_root / candidate.scope_relative_path
        if candidate.scope == "raw":
            return self.layout.raw_root / candidate.scope_relative_path
        if candidate.scope == "library":
            return self.layout.library_root / candidate.scope_relative_path
        raise ArchivistCompilerError(
            f"Unsupported archivist candidate scope: {candidate.scope}"
        )

    def _source_paths_for_candidates(
        self,
        candidates: Sequence[ArchivistCandidate],
    ) -> tuple[str, ...]:
        normalized = [self._normalized_source_path(candidate) for candidate in candidates]
        return tuple(dict.fromkeys(normalized))

    def _normalized_source_path(self, candidate: ArchivistCandidate) -> str:
        if candidate.scope == "vault":
            return candidate.scope_relative_path
        return f"{candidate.scope}/{candidate.scope_relative_path}"

    def _normalize_llm_body(self, title: str, content: str) -> str:
        lines = content.strip().splitlines()
        while lines and not lines[0].strip():
            lines.pop(0)
        if lines and lines[0].strip().startswith("# "):
            lines = lines[1:]
            while lines and not lines[0].strip():
                lines.pop(0)
        body = "\n".join(lines).strip()
        if not body:
            raise ArchivistCompilerError(
                f"Archivist returned empty body for topic {title}"
            )
        return body

    def _summary_from_body(self, body: str, *, fallback: str) -> str:
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            return truncate_summary(line)
        return truncate_summary(fallback)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
