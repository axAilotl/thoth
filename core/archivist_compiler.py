"""Archivist topic compiler for staged multi-source wiki pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any, Sequence

from .archivist_compilation import (
    ArchivistSourceBrief,
    build_stage_planning_result,
    extract_cited_candidate_keys,
    load_final_prompt_bundle,
    load_source_prompt_bundle,
    source_type_sort_key,
)
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
    brief_count: int = 0
    used_source_count: int = 0
    source_type_counts: dict[str, int] | None = None


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
        usage_by_key = self._load_usage_by_key(topic.id, candidates)
        stage_planning = build_stage_planning_result(
            topic,
            candidates,
            usage_by_key=usage_by_key,
            force=force or dirty.forced,
        )
        page_path = topic.output_path_for_root(self.layout.wiki_root)
        source_type_counts = self._source_type_counts(candidates)

        should_run = force or dirty.should_run
        if not should_run:
            self._record_source_usage(
                topic,
                candidates,
                usage_by_key=usage_by_key,
                stage_briefs=(),
                final_used_candidates=(),
                stage_planning=stage_planning,
                run_at=self._now_iso(),
                default_reason=dirty.reason,
            )
            return ArchivistCompileResult(
                topic_id=topic.id,
                status="skipped",
                reason=dirty.reason,
                page_path=page_path,
                candidate_count=len(candidates),
                source_paths=self._source_paths_for_candidates(candidates),
                model_provider=route[0],
                model=route[1],
                source_type_counts=source_type_counts,
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
                page_path=page_path,
                candidate_count=0,
                source_paths=(),
                model_provider=route[0],
                model=route[1],
                source_type_counts={},
            )

        if not stage_planning.stage_plans and not (force or dirty.forced):
            if not dry_run:
                self._record_source_usage(
                    topic,
                    candidates,
                    usage_by_key=usage_by_key,
                    stage_briefs=(),
                    final_used_candidates=(),
                    stage_planning=stage_planning,
                    run_at=self._now_iso(),
                    default_reason="no_source_delta",
                )
            return ArchivistCompileResult(
                topic_id=topic.id,
                status="skipped",
                reason="no_source_delta",
                page_path=page_path,
                candidate_count=len(candidates),
                source_paths=self._source_paths_for_candidates(candidates),
                model_provider=route[0],
                model=route[1],
                source_type_counts=source_type_counts,
            )

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
                brief_count=len(stage_planning.stage_plans),
                source_type_counts=source_type_counts,
            )

        stage_briefs = await self._generate_source_briefs(
            topic,
            stage_planning.stage_plans,
            route=route,
        )
        promoted_candidates = self._promoted_candidates_from_briefs(candidates, stage_briefs)
        body, final_used_candidates = await self._generate_final_topic_body(
            topic,
            stage_briefs=stage_briefs,
            promoted_candidates=promoted_candidates,
            route=route,
        )
        self._write_topic_page(
            topic,
            final_used_candidates,
            body=body,
            page_path=page_path,
        )
        run_at = self._now_iso()
        self._record_source_usage(
            topic,
            candidates,
            usage_by_key=usage_by_key,
            stage_briefs=stage_briefs,
            final_used_candidates=final_used_candidates,
            stage_planning=stage_planning,
            run_at=run_at,
            default_reason="compiled",
        )
        record_archivist_topic_run(
            topic,
            candidates,
            route=route,
            db=self.db,
            succeeded=True,
            run_at=run_at,
        )
        append_wiki_log_entry(
            self.scaffold,
            f"Archivist compiled `{topic.id}` from `{len(candidates)}` candidate source(s), producing `{len(stage_briefs)}` staged brief(s) and `{len(final_used_candidates)}` final citations.",
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
            source_paths=self._source_paths_for_candidates(final_used_candidates),
            model_provider=route[0],
            model=route[1],
            brief_count=len(stage_briefs),
            used_source_count=len(final_used_candidates),
            source_type_counts=source_type_counts,
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

    async def _generate_source_briefs(
        self,
        topic: ArchivistTopicDefinition,
        stage_plans,
        *,
        route: tuple[str, str, dict[str, Any]],
    ) -> tuple[ArchivistSourceBrief, ...]:
        briefs: list[ArchivistSourceBrief] = []
        for stage_plan in stage_plans:
            system_prompt, user_prompt = self._render_source_prompts(topic, stage_plan)
            content = await self._generate_markdown(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                route=route,
                topic_id=topic.id,
                stage_label=stage_plan.source_type,
            )
            promoted_keys = extract_cited_candidate_keys(content, stage_plan.selected_candidates)
            if not promoted_keys:
                raise ArchivistCompilerError(
                    f"Archivist source brief for topic {topic.id} and source type {stage_plan.source_type} did not cite any supplied sources"
                )
            briefs.append(
                ArchivistSourceBrief(
                    source_type=stage_plan.source_type,
                    source_label=stage_plan.source_label,
                    body=self._normalize_llm_body(stage_plan.source_label, content),
                    selected_candidate_keys=tuple(
                        candidate.candidate_key for candidate in stage_plan.selected_candidates
                    ),
                    promoted_candidate_keys=promoted_keys,
                    skipped_unchanged_candidate_keys=stage_plan.skipped_unchanged_candidate_keys,
                    skipped_limited_candidate_keys=stage_plan.skipped_limited_candidate_keys,
                )
            )
        return tuple(briefs)

    async def _generate_final_topic_body(
        self,
        topic: ArchivistTopicDefinition,
        *,
        stage_briefs: Sequence[ArchivistSourceBrief],
        promoted_candidates: Sequence[ArchivistCandidate],
        route: tuple[str, str, dict[str, Any]],
    ) -> tuple[str, tuple[ArchivistCandidate, ...]]:
        system_prompt, user_prompt = self._render_final_prompts(
            topic,
            stage_briefs=stage_briefs,
            promoted_candidates=promoted_candidates,
        )
        content = await self._generate_markdown(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            route=route,
            topic_id=topic.id,
            stage_label="final",
        )
        normalized_body = self._normalize_llm_body(topic.title, content)
        final_used_keys = extract_cited_candidate_keys(normalized_body, promoted_candidates)
        if promoted_candidates and not final_used_keys:
            raise ArchivistCompilerError(
                f"Archivist final synthesis for topic {topic.id} did not cite any promoted evidence sources"
            )
        final_candidate_map = {
            candidate.candidate_key: candidate for candidate in promoted_candidates
        }
        final_used_candidates = tuple(
            final_candidate_map[candidate_key]
            for candidate_key in final_used_keys
            if candidate_key in final_candidate_map
        )
        return normalized_body, final_used_candidates

    async def _generate_markdown(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        route: tuple[str, str, dict[str, Any]],
        topic_id: str,
        stage_label: str,
    ) -> str:
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
                f"Archivist generation failed for topic {topic_id} at stage {stage_label}: {response.error}"
            )
        content = (response.content or "").strip()
        if not content:
            raise ArchivistCompilerError(
                f"Archivist generation returned empty content for topic {topic_id} at stage {stage_label}"
            )
        return content

    def _render_source_prompts(
        self,
        topic: ArchivistTopicDefinition,
        stage_plan,
    ) -> tuple[str, str]:
        manifest = self._render_source_manifest(stage_plan.selected_candidates)
        return load_source_prompt_bundle(
            self.config,
            project_root=self.project_root,
            source_type=stage_plan.source_type,
            context={
                "topic_id": topic.id,
                "topic_title": topic.title,
                "topic_description": topic.description or "No additional topic description provided.",
                "source_type": stage_plan.source_type,
                "source_label": stage_plan.source_label,
                "candidate_count": len(stage_plan.selected_candidates),
                "new_source_count": len(stage_plan.new_candidate_keys),
                "carryover_source_count": len(stage_plan.carryover_candidate_keys),
                "source_manifest": manifest,
            },
        )

    def _render_final_prompts(
        self,
        topic: ArchivistTopicDefinition,
        *,
        stage_briefs: Sequence[ArchivistSourceBrief],
        promoted_candidates: Sequence[ArchivistCandidate],
    ) -> tuple[str, str]:
        return load_final_prompt_bundle(
            self.config,
            project_root=self.project_root,
            context={
                "topic_id": topic.id,
                "topic_title": topic.title,
                "topic_description": topic.description or "No additional topic description provided.",
                "brief_count": len(stage_briefs),
                "promoted_source_count": len(promoted_candidates),
                "brief_manifest": self._render_brief_manifest(stage_briefs),
                "source_manifest": self._render_source_manifest(promoted_candidates),
            },
        )

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

    def _render_brief_manifest(
        self,
        briefs: Sequence[ArchivistSourceBrief],
    ) -> str:
        lines: list[str] = []
        for index, brief in enumerate(
            sorted(briefs, key=lambda item: source_type_sort_key(item.source_type)),
            start=1,
        ):
            lines.extend(
                [
                    f"[B{index}] {brief.source_label}",
                    f"- Selected Sources: {len(brief.selected_candidate_keys)}",
                    f"- Promoted Sources: {len(brief.promoted_candidate_keys)}",
                    brief.body.strip(),
                    "",
                ]
            )
        return "\n".join(lines).strip()

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

    def _promoted_candidates_from_briefs(
        self,
        candidates: Sequence[ArchivistCandidate],
        briefs: Sequence[ArchivistSourceBrief],
    ) -> tuple[ArchivistCandidate, ...]:
        candidate_map = {candidate.candidate_key: candidate for candidate in candidates}
        promoted_keys: list[str] = []
        seen: set[str] = set()
        for brief in briefs:
            for candidate_key in brief.promoted_candidate_keys:
                if candidate_key in seen or candidate_key not in candidate_map:
                    continue
                seen.add(candidate_key)
                promoted_keys.append(candidate_key)
        return tuple(candidate_map[candidate_key] for candidate_key in promoted_keys)

    def _load_usage_by_key(
        self,
        topic_id: str,
        candidates: Sequence[ArchivistCandidate],
    ) -> dict[str, Any]:
        candidate_keys = tuple(candidate.candidate_key for candidate in candidates)
        return {
            record.candidate_key: record
            for record in self.db.list_archivist_topic_source_usage(
                topic_id=topic_id,
                candidate_keys=candidate_keys,
            )
        }

    def _record_source_usage(
        self,
        topic: ArchivistTopicDefinition,
        candidates: Sequence[ArchivistCandidate],
        *,
        usage_by_key: dict[str, Any],
        stage_briefs: Sequence[ArchivistSourceBrief],
        final_used_candidates: Sequence[ArchivistCandidate],
        stage_planning,
        run_at: str,
        default_reason: str,
    ) -> None:
        stage_used_keys = {
            candidate_key
            for brief in stage_briefs
            for candidate_key in brief.promoted_candidate_keys
        }
        final_used_keys = {
            candidate.candidate_key for candidate in final_used_candidates
        }
        selected_keys = set(stage_planning.selected_candidate_keys)
        skipped_unchanged_keys = set(stage_planning.skipped_unchanged_candidate_keys)
        skipped_limited_keys = set(stage_planning.skipped_limited_candidate_keys)
        records = []
        for candidate in candidates:
            existing = usage_by_key.get(candidate.candidate_key)
            selected = candidate.candidate_key in selected_keys
            stage_used = candidate.candidate_key in stage_used_keys
            final_used = candidate.candidate_key in final_used_keys
            if final_used:
                decision = "final_used"
                reason = "final_citation"
            elif stage_used:
                decision = "source_used"
                reason = "source_brief_citation"
            elif selected:
                decision = "read_not_used"
                reason = "stage_not_cited"
            elif candidate.candidate_key in skipped_unchanged_keys:
                decision = "polled_only"
                reason = "unchanged_unused"
            elif candidate.candidate_key in skipped_limited_keys:
                decision = "polled_only"
                reason = "stage_limit"
            else:
                decision = "polled_only"
                reason = default_reason
            records.append(
                self._build_usage_record(
                    topic_id=topic.id,
                    candidate=candidate,
                    existing=existing,
                    run_at=run_at,
                    selected=selected,
                    stage_used=stage_used,
                    final_used=final_used,
                    decision=decision,
                    reason=reason,
                )
            )
        self.db.upsert_archivist_topic_source_usage(records)

    def _build_usage_record(
        self,
        *,
        topic_id: str,
        candidate: ArchivistCandidate,
        existing: Any,
        run_at: str,
        selected: bool,
        stage_used: bool,
        final_used: bool,
        decision: str,
        reason: str,
    ) -> Any:
        from .archivist_compilation.models import ArchivistTopicSourceUsage

        return ArchivistTopicSourceUsage(
            topic_id=topic_id,
            candidate_key=candidate.candidate_key,
            source_type=candidate.source_type,
            source_hash=candidate.source_hash,
            retrieval_score=float(candidate.retrieval_score),
            last_polled_at=run_at,
            last_selected_at=run_at if selected else getattr(existing, "last_selected_at", None),
            last_read_at=run_at if selected else getattr(existing, "last_read_at", None),
            last_source_used_at=run_at if stage_used else getattr(existing, "last_source_used_at", None),
            last_final_used_at=run_at if final_used else getattr(existing, "last_final_used_at", None),
            selected_count=(getattr(existing, "selected_count", 0) or 0) + (1 if selected else 0),
            read_count=(getattr(existing, "read_count", 0) or 0) + (1 if selected else 0),
            source_used_count=(getattr(existing, "source_used_count", 0) or 0) + (1 if stage_used else 0),
            final_used_count=(getattr(existing, "final_used_count", 0) or 0) + (1 if final_used else 0),
            last_decision=decision,
            last_reason=reason,
            updated_at=run_at,
        )

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

    def _source_type_counts(
        self,
        candidates: Sequence[ArchivistCandidate],
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for candidate in candidates:
            counts[candidate.source_type] = counts.get(candidate.source_type, 0) + 1
        return counts

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
