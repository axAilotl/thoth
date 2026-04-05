"""Wiki search and curated write-back helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import os
import re
from typing import Iterable, Sequence

from .config import Config
from .path_layout import PathLayout, build_path_layout
from .wiki_contract import (
    WikiContract,
    WikiPageSpec,
    build_wiki_contract,
    is_legacy_tweet_slug,
    normalize_wiki_slug,
)
from .wiki_io import atomic_write_text, read_document, render_frontmatter, truncate_summary
from .wiki_scaffold import append_wiki_log_entry, ensure_wiki_scaffold

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _query_tokens(query: str) -> tuple[str, ...]:
    tokens = tuple(token for token in _TOKEN_RE.findall(query.lower()) if token)
    if not tokens:
        raise ValueError("Wiki query cannot be empty")
    return tokens


@dataclass(frozen=True)
class WikiQueryHit:
    """Single wiki search match."""

    slug: str
    title: str
    page_path: Path
    summary: str
    record_type: str
    kind: str
    source_paths: tuple[str, ...]
    related_slugs: tuple[str, ...]
    matched_fields: tuple[str, ...]
    score: int


@dataclass(frozen=True)
class WikiQueryResult:
    """Search results for a wiki query."""

    query: str
    hits: tuple[WikiQueryHit, ...]
    queried_at: str


@dataclass(frozen=True)
class WikiQueryWriteBackResult:
    """Summary of a curated wiki query write-back."""

    query: str
    page_path: Path
    slug: str
    selected_slugs: tuple[str, ...]
    hit_count: int
    created_at: str


class WikiQueryRunner:
    """Filesystem-backed wiki search and curated output writer."""

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
        self.scaffold = ensure_wiki_scaffold(config)
        self.contract = contract or build_wiki_contract(config)

    def search(self, query: str, *, limit: int = 10) -> WikiQueryResult:
        """Search compiled wiki pages using deterministic text matching."""
        if limit <= 0:
            raise ValueError("Wiki query limit must be positive")
        tokens = _query_tokens(query)
        normalized_query = query.strip().lower()
        hits: list[WikiQueryHit] = []

        for page_path in sorted(self.contract.pages_dir.glob("*.md")):
            document = read_document(page_path)
            frontmatter = document.frontmatter
            title = str(frontmatter.get("title") or page_path.stem)
            slug = str(frontmatter.get("slug") or page_path.stem)
            if is_legacy_tweet_slug(slug):
                continue
            summary = str(frontmatter.get("summary") or "")
            record_type = str(frontmatter.get("thoth_type") or "wiki_page")
            kind = str(frontmatter.get("kind") or "topic")
            source_paths = tuple(str(path) for path in frontmatter.get("source_paths") or ())
            related_slugs = tuple(str(path) for path in frontmatter.get("related_slugs") or ())
            aliases = tuple(str(alias) for alias in frontmatter.get("aliases") or ())

            haystacks = {
                "title": title.lower(),
                "summary": summary.lower(),
                "aliases": " ".join(alias.lower() for alias in aliases),
                "related_slugs": " ".join(item.lower() for item in related_slugs),
                "source_paths": " ".join(item.lower() for item in source_paths),
                "body": document.body.lower(),
            }

            matched_fields: list[str] = []
            score = 0
            if normalized_query and normalized_query in " ".join(haystacks.values()):
                score += 5
                matched_fields.append("phrase")

            for token in tokens:
                token_score = 0
                for field_name, field_value in haystacks.items():
                    if token in field_value:
                        matched_fields.append(field_name)
                        token_score += 1
                        if field_name == "title":
                            token_score += 3
                        elif field_name == "summary":
                            token_score += 2
                score += token_score

            if score <= 0:
                continue

            hits.append(
                WikiQueryHit(
                    slug=slug,
                    title=title,
                    page_path=page_path,
                    summary=truncate_summary(summary),
                    record_type=record_type,
                    kind=kind,
                    source_paths=source_paths,
                    related_slugs=related_slugs,
                    matched_fields=tuple(dict.fromkeys(matched_fields)),
                    score=score,
                )
            )

        hits.sort(key=lambda hit: (-hit.score, hit.title.lower(), hit.slug))
        return WikiQueryResult(query=query, hits=tuple(hits[:limit]), queried_at=_now_iso())

    def curated_write_back(
        self,
        query: str,
        *,
        limit: int = 10,
        selected_slugs: Sequence[str] | None = None,
        curated_notes: str | None = None,
        curated_title: str | None = None,
    ) -> WikiQueryWriteBackResult:
        """Persist a curated query result back into the wiki."""
        if limit <= 0:
            raise ValueError("Wiki query limit must be positive")
        result = self.search(query, limit=limit)
        if not result.hits:
            raise ValueError(f"No wiki pages matched query: {query}")

        selected = self._select_hits(result.hits, selected_slugs)
        if not selected:
            raise ValueError("Curated write-back requires at least one selected page")

        slug = f"query-{normalize_wiki_slug(query)}"
        page_path = self.contract.page_path(slug)
        existing_frontmatter = read_document(page_path).frontmatter if page_path.exists() else {}
        now = _now_iso()
        created_at = str(existing_frontmatter.get("created_at") or now)
        title = curated_title or f"Query: {query}"
        summary = truncate_summary(curated_notes or query)
        spec = WikiPageSpec(
            title=title,
            slug=slug,
            kind="topic",
            summary=summary,
            source_paths=tuple(
                self._relative_wiki_path(hit.page_path) for hit in selected
            ),
            related_slugs=tuple(hit.slug for hit in selected),
            language="en",
            record_type="wiki_query",
            query=query,
            query_terms=_query_tokens(query),
            curated=True,
            result_count=len(selected),
            created_at=created_at,
            updated_at=now,
        )

        body_lines = [
            render_frontmatter(self.contract.frontmatter_for(spec)).rstrip(),
            "",
            f"# {title}",
            "",
            "## Query",
            "",
            f"- Query: `{query}`",
            f"- Selected Pages: `{len(selected)}`",
            "",
            "## Matches",
            "",
        ]
        for hit in selected:
            rel_link = os.path.relpath(hit.page_path, self.contract.pages_dir)
            fields = ", ".join(hit.matched_fields) if hit.matched_fields else "none"
            body_lines.append(f"- [{hit.title}]({rel_link})")
            body_lines.append(f"  - Score: `{hit.score}`")
            body_lines.append(f"  - Matched Fields: `{fields}`")

        if curated_notes:
            body_lines.extend(["", "## Curated Notes", "", curated_notes.strip(), ""])

        atomic_write_text(page_path, "\n".join(body_lines) + "\n")

        from .wiki_updater import CompiledWikiUpdater

        CompiledWikiUpdater(self.config, layout=self.layout, contract=self.contract).refresh_index()
        append_wiki_log_entry(
            self.scaffold,
            f"Wrote curated query output `{slug}` from `{len(selected)}` selected page(s).",
        )
        return WikiQueryWriteBackResult(
            query=query,
            page_path=page_path,
            slug=slug,
            selected_slugs=tuple(hit.slug for hit in selected),
            hit_count=len(result.hits),
            created_at=created_at,
        )

    def _select_hits(
        self,
        hits: Iterable[WikiQueryHit],
        selected_slugs: Sequence[str] | None,
    ) -> tuple[WikiQueryHit, ...]:
        if selected_slugs is None:
            return tuple(hits)

        wanted = {normalize_wiki_slug(slug) for slug in selected_slugs}
        selected = [hit for hit in hits if hit.slug in wanted]
        missing = sorted(wanted.difference({hit.slug for hit in selected}))
        if missing:
            raise ValueError(f"Selected wiki slugs not found in query results: {', '.join(missing)}")
        return tuple(selected)

    def _relative_wiki_path(self, page_path: Path) -> str:
        return page_path.relative_to(self.layout.vault_root.parent).as_posix()
