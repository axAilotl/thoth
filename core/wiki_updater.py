"""Compiled wiki update loop driven by processed artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any

import yaml

from .artifacts import (
    KnowledgeArtifact,
    PaperArtifact,
    RepositoryArtifact,
    TweetArtifact,
    WebClipperArtifact,
)
from .config import Config
from .path_layout import PathLayout, build_path_layout
from .wiki_contract import (
    WikiContract,
    WikiPageSpec,
    is_legacy_tweet_slug,
    normalize_wiki_slug,
)
from .wiki_scaffold import (
    WIKI_INDEX_TITLE,
    append_wiki_log_entry,
    ensure_wiki_scaffold,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    with open(temp_path, "w", encoding="utf-8") as handle:
        handle.write(content)
        if not content.endswith("\n"):
            handle.write("\n")
    os.replace(temp_path, path)


def _truncate_summary(value: str, *, limit: int = 320) -> str:
    normalized = " ".join((value or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "..."


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


def _render_frontmatter(data: dict[str, Any]) -> str:
    return "---\n" + yaml.safe_dump(
        data,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    ) + "---\n"


@dataclass(frozen=True)
class WikiUpdateResult:
    """Summary of a single compiled wiki update."""

    slug: str
    page_path: Path
    source_paths: tuple[str, ...]
    action: str


class CompiledWikiUpdater:
    """Render compiled wiki pages from processed artifacts."""

    def __init__(
        self,
        config: Config,
        *,
        layout: PathLayout | None = None,
        contract: WikiContract | None = None,
    ):
        self.config = config
        self.layout = layout or build_path_layout(config)
        self.layout.ensure_directories()
        self.contract = contract or WikiContract(root=self.layout.wiki_root)
        self._legacy_pages_pruned = False
        self.scaffold = ensure_wiki_scaffold(
            config,
            project_root=self.layout.vault_root.parent,
        )

    def supports_artifact(self, artifact: KnowledgeArtifact) -> bool:
        """Return True when an artifact should compile into the wiki layer."""
        return not isinstance(artifact, TweetArtifact)

    def prune_legacy_tweet_pages(self) -> tuple[Path, ...]:
        """Delete legacy generated tweet pages from the wiki pages directory."""
        if self._legacy_pages_pruned:
            return tuple()

        removed: list[Path] = []
        for page_path in sorted(self.contract.pages_dir.glob("tweet-*.md")):
            frontmatter = _read_frontmatter(page_path)
            slug = str(frontmatter.get("slug") or page_path.stem)
            if not is_legacy_tweet_slug(slug):
                continue
            try:
                page_path.unlink()
                removed.append(page_path)
            except FileNotFoundError:
                continue

        if removed:
            append_wiki_log_entry(
                self.scaffold,
                "Pruned legacy compiled tweet wiki pages: "
                + ", ".join(f"`{path.stem}`" for path in removed)
                + ".",
            )
        self._legacy_pages_pruned = True
        return tuple(removed)

    def update_from_artifact(
        self,
        artifact: KnowledgeArtifact,
        *,
        dispatch_details: dict[str, Any] | None = None,
    ) -> WikiUpdateResult:
        if not self.supports_artifact(artifact):
            raise ValueError(
                f"Compiled wiki pages are not supported for {artifact.__class__.__name__}"
            )
        spec = self._page_spec_for_artifact(artifact)
        page_path = self.contract.page_path_for(spec)
        existing = _read_frontmatter(page_path)
        created_at = str(existing.get("created_at") or spec.created_at or _now_iso())
        updated_spec = WikiPageSpec(
            title=spec.title,
            slug=spec.slug,
            kind=spec.kind,
            summary=spec.summary,
            aliases=spec.aliases,
            source_paths=spec.source_paths,
            related_slugs=spec.related_slugs,
            language=spec.language,
            translated_from=spec.translated_from,
            created_at=created_at,
            updated_at=_now_iso(),
        )
        content = self._render_page(updated_spec, artifact, dispatch_details=dispatch_details)
        action = "updated" if page_path.exists() else "created"
        _atomic_write_text(page_path, content)
        self.refresh_index()
        append_wiki_log_entry(
            self.scaffold,
            f"{action.title()} `{updated_spec.slug}` from `{artifact.source_type}:{artifact.id}`.",
        )
        return WikiUpdateResult(
            slug=updated_spec.slug,
            page_path=page_path,
            source_paths=updated_spec.source_paths,
            action=action,
        )

    def refresh_index(self) -> Path:
        self.prune_legacy_tweet_pages()
        created_at = str(_read_frontmatter(self.contract.index_path).get("created_at") or _now_iso())
        entries = []
        for page_path in sorted(self.contract.pages_dir.glob("*.md")):
            frontmatter = _read_frontmatter(page_path)
            slug = str(frontmatter.get("slug") or page_path.stem)
            if is_legacy_tweet_slug(slug):
                continue
            title = str(frontmatter.get("title") or page_path.stem)
            summary = _truncate_summary(str(frontmatter.get("summary") or ""))
            rel_link = page_path.relative_to(self.contract.root).as_posix()
            entries.append((title, rel_link, summary))

        lines = [
            _render_frontmatter(
                {
                    "thoth_type": "wiki_index",
                    "title": WIKI_INDEX_TITLE,
                    "root": str(self.contract.root),
                    "created_at": created_at,
                    "updated_at": _now_iso(),
                }
            ).rstrip(),
            "",
            f"# {WIKI_INDEX_TITLE}",
            "",
            "This directory stores the compiled wiki layer.",
            "",
            "## Pages",
        ]
        if not entries:
            lines.append("")
            lines.append("- No compiled pages yet.")
        else:
            lines.append("")
            for title, rel_link, summary in entries:
                line = f"- [{title}]({rel_link})"
                if summary:
                    line += f": {summary}"
                lines.append(line)

        _atomic_write_text(self.contract.index_path, "\n".join(lines) + "\n")
        return self.contract.index_path

    def _page_spec_for_artifact(self, artifact: KnowledgeArtifact) -> WikiPageSpec:
        title, slug, kind, summary, aliases = self._title_slug_and_summary(artifact)
        return WikiPageSpec(
            title=title,
            slug=slug,
            kind=kind,
            summary=summary,
            aliases=aliases,
            source_paths=self._source_paths_for_artifact(artifact),
            language=self._language_for_artifact(artifact),
            created_at=_now_iso(),
            updated_at=_now_iso(),
        )

    def _title_slug_and_summary(
        self, artifact: KnowledgeArtifact
    ) -> tuple[str, str, str, str, tuple[str, ...]]:
        if isinstance(artifact, RepositoryArtifact):
            repo_name = artifact.repo_name or artifact.id
            return (
                repo_name,
                f"repo-{normalize_wiki_slug(repo_name)}",
                "entity",
                _truncate_summary(artifact.description),
                (repo_name,),
            )
        if isinstance(artifact, PaperArtifact):
            paper_key = artifact.arxiv_id or artifact.id
            return (
                artifact.title or paper_key,
                f"paper-{normalize_wiki_slug(paper_key)}",
                "concept",
                _truncate_summary(artifact.abstract),
                tuple(alias for alias in (artifact.arxiv_id, artifact.doi) if alias),
            )
        if isinstance(artifact, TweetArtifact):
            raise ValueError("Tweet artifacts do not compile into wiki pages")
        if isinstance(artifact, WebClipperArtifact):
            title = artifact.title or artifact.source_relative_path or artifact.id
            return (
                title,
                f"clip-{normalize_wiki_slug(title)}",
                "concept",
                _truncate_summary(artifact.body or artifact.raw_content),
                tuple(alias for alias in (artifact.source_url,) if alias),
            )
        return (
            artifact.id,
            normalize_wiki_slug(artifact.id),
            "topic",
            _truncate_summary(artifact.raw_content),
            (),
        )

    def _language_for_artifact(self, artifact: KnowledgeArtifact) -> str:
        if isinstance(artifact, WebClipperArtifact):
            return artifact.source_language or "und"
        return "en"

    def _source_paths_for_artifact(self, artifact: KnowledgeArtifact) -> tuple[str, ...]:
        candidates: list[Path] = []
        if isinstance(artifact, TweetArtifact):
            raise ValueError("Tweet artifacts do not compile into wiki pages")
        elif isinstance(artifact, PaperArtifact):
            paper_key = artifact.arxiv_id or artifact.id
            candidates.extend(sorted((self.layout.vault_root / "papers").glob(f"{paper_key}*.pdf")))
        elif isinstance(artifact, RepositoryArtifact):
            safe_name = (artifact.repo_name or artifact.id).replace("/", "_")
            candidates.extend(sorted((self.layout.vault_root / "stars").glob(f"*{safe_name}*_summary.md")))
            candidates.extend(sorted((self.layout.vault_root / "repos").glob(f"*{safe_name}*README.md")))
        elif isinstance(artifact, WebClipperArtifact):
            if artifact.source_relative_path:
                candidates.append(self.layout.vault_root / artifact.source_relative_path)
            elif artifact.source_path:
                candidates.append(Path(artifact.source_path))
            for managed_path in artifact.output_paths.values():
                if managed_path:
                    candidates.append(Path(managed_path))

        normalized: list[str] = []
        for candidate in candidates:
            if not candidate.exists():
                continue
            try:
                normalized.append(candidate.relative_to(self.layout.vault_root).as_posix())
            except ValueError:
                continue
        deduped = dict.fromkeys(normalized)
        return tuple(deduped.keys())

    def _render_page(
        self,
        spec: WikiPageSpec,
        artifact: KnowledgeArtifact,
        *,
        dispatch_details: dict[str, Any] | None,
    ) -> str:
        frontmatter = self.contract.frontmatter_for(spec)
        lines = [
            _render_frontmatter(frontmatter).rstrip(),
            "",
            f"# {spec.title}",
            "",
        ]
        if spec.summary:
            lines.extend([spec.summary, ""])
        lines.extend(
            [
                "## Artifact",
                "",
                f"- ID: `{artifact.id}`",
                f"- Source: `{artifact.source_type}`",
            ]
        )
        if artifact.created_at:
            lines.append(f"- Created At: `{artifact.created_at}`")
        if artifact.processing_status:
            lines.append(f"- Processing Status: `{artifact.processing_status}`")
        lines.append("")

        detail_lines = self._artifact_detail_lines(artifact)
        if detail_lines:
            lines.extend(["## Details", ""])
            lines.extend(detail_lines)
            lines.append("")

        if dispatch_details:
            lines.extend(["## Processing", ""])
            for key, value in sorted(dispatch_details.items()):
                lines.append(f"- {key.replace('_', ' ').title()}: `{value}`")
            lines.append("")

        if spec.source_paths:
            lines.extend(["## Sources", ""])
            for source_path in spec.source_paths:
                absolute_source = self.layout.vault_root / source_path
                relative_link = os.path.relpath(absolute_source, self.contract.pages_dir)
                lines.append(f"- [{source_path}]({relative_link})")
            lines.append("")

        return "\n".join(lines) + "\n"

    def _artifact_detail_lines(self, artifact: KnowledgeArtifact) -> list[str]:
        if isinstance(artifact, RepositoryArtifact):
            lines = []
            if artifact.description:
                lines.append(f"- Description: {artifact.description}")
            lines.append(f"- Stars: `{artifact.stars}`")
            if artifact.language:
                lines.append(f"- Language: `{artifact.language}`")
            if artifact.topics:
                lines.append(f"- Topics: {', '.join(f'`{topic}`' for topic in artifact.topics)}")
            return lines

        if isinstance(artifact, PaperArtifact):
            lines = []
            if artifact.authors:
                lines.append(f"- Authors: {', '.join(artifact.authors)}")
            if artifact.arxiv_id:
                lines.append(f"- arXiv ID: `{artifact.arxiv_id}`")
            if artifact.doi:
                lines.append(f"- DOI: `{artifact.doi}`")
            if artifact.pdf_url:
                lines.append(f"- PDF URL: `{artifact.pdf_url}`")
            if artifact.abstract:
                lines.append(f"- Abstract: {artifact.abstract}")
            return lines

        if isinstance(artifact, TweetArtifact):
            return []

        if isinstance(artifact, WebClipperArtifact):
            lines = []
            if artifact.title:
                lines.append(f"- Title: {artifact.title}")
            if artifact.source_url:
                lines.append(f"- Source URL: `{artifact.source_url}`")
            if artifact.source_language:
                lines.append(f"- Source Language: `{artifact.source_language}`")
            if artifact.body:
                lines.append(f"- Body: {_truncate_summary(artifact.body)}")
            return lines

        return []
