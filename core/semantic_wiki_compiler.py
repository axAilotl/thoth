"""Compile confirmed/promoted semantic memory into OKF-compatible wiki pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .path_layout import PathLayout
from .prompt_security import prompt_security_requires_review
from .semantic_memory import (
    SemanticMemoryCandidate,
    SemanticMemoryEvidence,
    SemanticMemoryStore,
)
from .wiki_contract import WikiContract, WikiPageSpec, normalize_wiki_slug
from .wiki_io import atomic_write_text, read_frontmatter, render_frontmatter


SEMANTIC_WIKI_ALLOWED_STATUSES = ("confirmed", "promoted")
SEMANTIC_WIKI_PAGE_MARKER = "thoth_semantic_memory_page"
SEMANTIC_WIKI_PAGE_TYPE_KEY = "thoth_semantic_page_type"

_QUARANTINE_MARKERS = {
    "blocked",
    "needs_review",
    "quarantine",
    "quarantined",
    "security_review",
}


@dataclass(frozen=True)
class SemanticWikiPageResult:
    """Summary of one semantic-memory wiki page write."""

    slug: str
    page_path: Path
    source_paths: tuple[str, ...]
    action: str


@dataclass(frozen=True)
class _SemanticFactRecord:
    candidate: SemanticMemoryCandidate
    evidence: tuple[SemanticMemoryEvidence, ...]


@dataclass(frozen=True)
class _SemanticPageRef:
    page_type: str
    key: str
    title: str
    slug: str
    kind: str


@dataclass(frozen=True)
class _SemanticPageGroup:
    page_type: str
    key: str
    title: str
    slug: str
    kind: str
    facts: tuple[_SemanticFactRecord, ...]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _compact_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): item
        for key, item in value.items()
        if item not in (None, "", [], {}, ())
    }


def _quarantine_marker(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().casefold() in _QUARANTINE_MARKERS
    return False


def _metadata_has_quarantine_marker(metadata: Mapping[str, Any]) -> bool:
    for key, value in metadata.items():
        key_text = str(key).strip().casefold()
        if key_text in {
            "quarantined",
            "requires_review",
            "security_review",
        } and _quarantine_marker(value):
            return True
        if key_text in {
            "quarantine_status",
            "review_status",
            "security_status",
            "status",
            "state",
        } and _quarantine_marker(value):
            return True
    return False


def _metadata_requires_review(metadata: Mapping[str, Any]) -> bool:
    return prompt_security_requires_review(metadata) or _metadata_has_quarantine_marker(
        metadata
    )


def _candidate_is_quarantined(candidate: SemanticMemoryCandidate) -> bool:
    if _quarantine_marker(candidate.privacy_class):
        return True
    return _metadata_requires_review(candidate.metadata) or _metadata_requires_review(
        candidate.write_provenance
    )


def _evidence_is_quarantined(evidence: SemanticMemoryEvidence) -> bool:
    if _quarantine_marker(evidence.privacy_class):
        return True
    return _metadata_requires_review(evidence.metadata) or _metadata_requires_review(
        evidence.write_provenance
    )


def _safe_source_path(value: str | None) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    path = Path(text)
    if path.is_absolute() or ".." in path.parts:
        return None
    return path.as_posix()


def _stable_unique_strings(values: Iterable[str | None]) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in values if _clean_text(value)}))


def _slug_component(value: str, fallback: str) -> str:
    try:
        return normalize_wiki_slug(value)
    except ValueError:
        try:
            return normalize_wiki_slug(fallback)
        except ValueError:
            return "unknown"


def _entity_slug_seed(candidate: SemanticMemoryCandidate, page_type: str) -> str:
    entity_id = _clean_text(candidate.entity_id)
    if entity_id:
        prefix = f"{page_type}:"
        if entity_id.casefold().startswith(prefix):
            return entity_id[len(prefix) :]
        return entity_id
    return (
        _clean_text(candidate.entity_name)
        or _clean_text(candidate.subject)
        or _clean_text(candidate.object_value)
        or candidate.candidate_id
    )


def _entity_label(candidate: SemanticMemoryCandidate, page_type: str) -> str:
    if candidate.entity_name:
        return candidate.entity_name
    if candidate.subject:
        return candidate.subject
    if candidate.candidate_type == page_type and candidate.object_value:
        return candidate.object_value
    if candidate.entity_id:
        return candidate.entity_id
    return candidate.text


def _page_ref_for_type(
    candidate: SemanticMemoryCandidate,
    page_type: str,
) -> _SemanticPageRef:
    label = _entity_label(candidate, page_type)
    slug_seed = _entity_slug_seed(candidate, page_type)
    slug = f"{page_type}-{_slug_component(slug_seed, candidate.candidate_id)}"
    return _SemanticPageRef(
        page_type=page_type,
        key=slug.removeprefix(f"{page_type}-"),
        title=f"{page_type.title()}: {label}",
        slug=slug,
        kind="entity" if page_type in {"person", "project"} else "topic",
    )


def _page_refs_for_candidate(
    candidate: SemanticMemoryCandidate,
) -> tuple[_SemanticPageRef, ...]:
    refs: list[_SemanticPageRef] = []
    for page_type in ("person", "project", "topic"):
        entity_type = (candidate.entity_type or "").strip().casefold()
        entity_id = (candidate.entity_id or "").strip().casefold()
        if (
            candidate.candidate_type == page_type
            or entity_type == page_type
            or entity_id.startswith(f"{page_type}:")
        ):
            refs.append(_page_ref_for_type(candidate, page_type))

    deduped: dict[str, _SemanticPageRef] = {}
    for ref in refs:
        deduped.setdefault(ref.slug, ref)
    return tuple(deduped[key] for key in sorted(deduped))


def _fact_sort_key(record: _SemanticFactRecord) -> tuple[str, str, str]:
    candidate = record.candidate
    return (
        candidate.status_updated_at or candidate.updated_at or candidate.created_at or "",
        candidate.candidate_type,
        candidate.candidate_id,
    )


def _evidence_sort_key(evidence: SemanticMemoryEvidence) -> tuple[str, str]:
    return (
        evidence.source_timestamp or evidence.created_at or evidence.updated_at or "",
        evidence.evidence_id,
    )


class SemanticMemoryWikiCompiler:
    """Render confirmed/promoted semantic memory facts into wiki pages."""

    def __init__(
        self,
        *,
        layout: PathLayout,
        contract: WikiContract,
    ) -> None:
        self.layout = layout
        self.contract = contract

    def compile(
        self,
        store: SemanticMemoryStore,
        *,
        statuses: Sequence[str] = SEMANTIC_WIKI_ALLOWED_STATUSES,
    ) -> tuple[SemanticWikiPageResult, ...]:
        """Compile semantic memory pages from durable reviewed candidates."""
        allowed_statuses = tuple(dict.fromkeys(statuses))
        facts = self._facts_from_store(store, statuses=allowed_statuses)
        groups = self._page_groups(facts)
        current_slugs = {group.slug for group in groups}
        related_slugs = tuple(sorted(current_slugs))

        results = [
            self._update_page(group, related_slugs=related_slugs)
            for group in groups
        ]
        results.extend(self._prune_stale_semantic_pages(current_slugs))
        return tuple(results)

    def _facts_from_store(
        self,
        store: SemanticMemoryStore,
        *,
        statuses: tuple[str, ...],
    ) -> tuple[_SemanticFactRecord, ...]:
        records: list[_SemanticFactRecord] = []
        for status in statuses:
            for candidate in store.list_candidates(status=status):
                if candidate.status not in SEMANTIC_WIKI_ALLOWED_STATUSES:
                    continue
                if _candidate_is_quarantined(candidate):
                    continue
                safe_evidence = tuple(
                    sorted(
                        (
                            evidence
                            for evidence in store.list_evidence(
                                candidate_id=candidate.candidate_id
                            )
                            if not _evidence_is_quarantined(evidence)
                        ),
                        key=_evidence_sort_key,
                    )
                )
                if not safe_evidence:
                    continue
                records.append(
                    _SemanticFactRecord(candidate=candidate, evidence=safe_evidence)
                )
        return tuple(sorted(records, key=_fact_sort_key))

    def _page_groups(
        self,
        facts: tuple[_SemanticFactRecord, ...],
    ) -> tuple[_SemanticPageGroup, ...]:
        buckets: dict[str, dict[str, Any]] = {}

        def add(ref: _SemanticPageRef, fact: _SemanticFactRecord) -> None:
            bucket = buckets.setdefault(
                ref.slug,
                {
                    "page_type": ref.page_type,
                    "key": ref.key,
                    "title": ref.title,
                    "slug": ref.slug,
                    "kind": ref.kind,
                    "facts": [],
                },
            )
            bucket["facts"].append(fact)

        for fact in facts:
            for ref in _page_refs_for_candidate(fact.candidate):
                add(ref, fact)

        if facts:
            buckets["semantic-memory-digest"] = {
                "page_type": "digest",
                "key": "semantic-memory",
                "title": "Semantic Memory Digest",
                "slug": "semantic-memory-digest",
                "kind": "topic",
                "facts": list(facts),
            }

        order = {"person": 0, "project": 1, "topic": 2, "digest": 3}
        groups: list[_SemanticPageGroup] = []
        for _slug, bucket in sorted(
            buckets.items(),
            key=lambda item: (order.get(item[1]["page_type"], 99), item[0]),
        ):
            fact_by_id: dict[str, _SemanticFactRecord] = {}
            for fact in bucket["facts"]:
                fact_by_id.setdefault(fact.candidate.candidate_id, fact)
            groups.append(
                _SemanticPageGroup(
                    page_type=bucket["page_type"],
                    key=bucket["key"],
                    title=bucket["title"],
                    slug=bucket["slug"],
                    kind=bucket["kind"],
                    facts=tuple(
                        sorted(fact_by_id.values(), key=_fact_sort_key)
                    ),
                )
            )
        return tuple(groups)

    def _update_page(
        self,
        group: _SemanticPageGroup,
        *,
        related_slugs: tuple[str, ...],
    ) -> SemanticWikiPageResult:
        source_paths = self._source_paths_for_facts(group.facts)
        event_ids = self._event_ids_for_facts(group.facts)
        candidate_ids = tuple(fact.candidate.candidate_id for fact in group.facts)
        evidence_ids = tuple(
            evidence.evidence_id for fact in group.facts for evidence in fact.evidence
        )
        updated_at = _now_iso()
        summary = self._summary_for_group(group)
        spec = WikiPageSpec(
            title=group.title,
            slug=group.slug,
            kind=group.kind,
            summary=summary,
            source_paths=source_paths,
            influence_sources=self._influence_sources_for_facts(group.facts),
            related_slugs=tuple(slug for slug in related_slugs if slug != group.slug),
            created_at=updated_at,
            updated_at=updated_at,
            event_ids=event_ids,
            semantic_page_type=group.page_type,
            semantic_candidate_ids=candidate_ids,
            semantic_evidence_ids=evidence_ids,
        )
        page_path = self.contract.page_path_for(spec)
        existing = read_frontmatter(page_path) if page_path.exists() else {}
        created_at = str(existing.get("created_at") or spec.created_at or updated_at)
        updated_spec = WikiPageSpec(
            title=spec.title,
            slug=spec.slug,
            kind=spec.kind,
            summary=spec.summary,
            source_paths=spec.source_paths,
            influence_sources=spec.influence_sources,
            related_slugs=spec.related_slugs,
            created_at=created_at,
            updated_at=updated_at,
            event_ids=spec.event_ids,
            semantic_page_type=spec.semantic_page_type,
            semantic_candidate_ids=spec.semantic_candidate_ids,
            semantic_evidence_ids=spec.semantic_evidence_ids,
        )
        content = self._render_page(updated_spec, group)
        action = "updated" if page_path.exists() else "created"
        atomic_write_text(page_path, content)
        return SemanticWikiPageResult(
            slug=updated_spec.slug,
            page_path=page_path,
            source_paths=updated_spec.source_paths,
            action=action,
        )

    def _prune_stale_semantic_pages(
        self,
        current_slugs: set[str],
    ) -> list[SemanticWikiPageResult]:
        results: list[SemanticWikiPageResult] = []
        self.contract.pages_dir.mkdir(parents=True, exist_ok=True)
        for page_path in sorted(self.contract.pages_dir.glob("*.md")):
            frontmatter = read_frontmatter(page_path)
            if frontmatter.get(SEMANTIC_WIKI_PAGE_MARKER) is not True:
                continue
            slug = str(
                frontmatter.get("thoth_slug")
                or frontmatter.get("slug")
                or page_path.stem
            )
            if slug in current_slugs:
                continue
            source_paths = tuple(
                str(item)
                for item in frontmatter.get("thoth_source_paths", []) or []
                if _clean_text(item)
            )
            page_path.unlink(missing_ok=True)
            results.append(
                SemanticWikiPageResult(
                    slug=slug,
                    page_path=page_path,
                    source_paths=source_paths,
                    action="deleted",
                )
            )
        return results

    def _summary_for_group(self, group: _SemanticPageGroup) -> str:
        if group.page_type == "digest":
            return (
                f"Digest of {len(group.facts)} confirmed or promoted semantic "
                "memory fact(s)."
            )
        return (
            f"Confirmed or promoted semantic memory facts for "
            f"{group.page_type} `{group.key}`."
        )

    def _source_paths_for_facts(
        self,
        facts: tuple[_SemanticFactRecord, ...],
    ) -> tuple[str, ...]:
        return _stable_unique_strings(
            _safe_source_path(evidence.source_path)
            for fact in facts
            for evidence in fact.evidence
        )

    def _event_ids_for_facts(
        self,
        facts: tuple[_SemanticFactRecord, ...],
    ) -> tuple[str, ...]:
        return _stable_unique_strings(
            evidence.capture_event_id
            for fact in facts
            for evidence in fact.evidence
        )

    def _artifact_refs_for_facts(
        self,
        facts: tuple[_SemanticFactRecord, ...],
    ) -> tuple[tuple[str, str | None], ...]:
        refs = {
            (evidence.artifact_id, evidence.artifact_type)
            for fact in facts
            for evidence in fact.evidence
            if evidence.artifact_id
        }
        return tuple(sorted(refs, key=lambda item: (item[1] or "", item[0] or "")))

    def _influence_sources_for_facts(
        self,
        facts: tuple[_SemanticFactRecord, ...],
    ) -> tuple[dict[str, Any], ...]:
        records: list[dict[str, Any]] = []
        for fact in facts:
            candidate = fact.candidate
            records.append(
                _compact_mapping(
                    {
                        "semantic_candidate_id": candidate.candidate_id,
                        "candidate_status": candidate.status,
                        "candidate_type": candidate.candidate_type,
                        "entity_id": candidate.entity_id,
                        "entity_type": candidate.entity_type,
                        "entity_name": candidate.entity_name,
                    }
                )
            )
            for evidence in fact.evidence:
                records.append(
                    _compact_mapping(
                        {
                            "semantic_candidate_id": candidate.candidate_id,
                            "semantic_evidence_id": evidence.evidence_id,
                            "artifact_id": evidence.artifact_id,
                            "artifact_type": evidence.artifact_type,
                            "capture_event_id": evidence.capture_event_id,
                            "source_path": _safe_source_path(evidence.source_path),
                            "source_timestamp": evidence.source_timestamp,
                        }
                    )
                )
        return tuple(records)

    def _render_page(
        self,
        spec: WikiPageSpec,
        group: _SemanticPageGroup,
    ) -> str:
        frontmatter = self.contract.frontmatter_for(spec)
        citation_numbers = self._citation_numbers(group.facts)
        lines = [
            render_frontmatter(frontmatter).rstrip(),
            "",
            f"# {spec.title}",
            "",
            spec.summary,
            "",
            "## Semantic Facts",
            "",
        ]
        for fact in group.facts:
            lines.extend(self._fact_lines(fact, citation_numbers))
        lines.append("")

        lines.extend(["## Sources", ""])
        source_lines = self._source_lines(spec, group)
        lines.extend(source_lines or ["- Candidate evidence records are cited below."])
        lines.append("")

        lines.extend(["## Source Evidence", ""])
        for fact in group.facts:
            lines.extend(self._evidence_lines(fact, citation_numbers))
        lines.append("")

        citation_lines = self._citation_lines(group.facts, citation_numbers)
        if citation_lines:
            lines.extend(["# Citations", ""])
            lines.extend(citation_lines)
            lines.append("")

        return "\n".join(lines) + "\n"

    def _fact_lines(
        self,
        fact: _SemanticFactRecord,
        citation_numbers: Mapping[str, int],
    ) -> list[str]:
        candidate = fact.candidate
        citations = " ".join(
            f"[{citation_numbers[evidence.evidence_id]}]"
            for evidence in fact.evidence
        )
        line = (
            f"- <a id=\"{self._candidate_anchor(candidate.candidate_id)}\"></a>"
            f"`{candidate.candidate_id}` "
            f"({candidate.status} {candidate.candidate_type}): "
            f"{candidate.text}"
        )
        if citations:
            line += f" {citations}"
        lines = [line]
        if candidate.subject or candidate.predicate or candidate.object_value:
            triple = " ".join(
                f"`{item}`"
                for item in (
                    candidate.subject,
                    candidate.predicate,
                    candidate.object_value,
                )
                if item
            )
            lines.append(f"  - Triple: {triple}")
        entity = candidate.entity_name or candidate.entity_id or candidate.entity_type
        if entity:
            lines.append(f"  - Entity: `{entity}`")
        if candidate.confidence is not None:
            lines.append(f"  - Confidence: `{candidate.confidence:.2f}`")
        return lines

    def _source_lines(
        self,
        spec: WikiPageSpec,
        group: _SemanticPageGroup,
    ) -> list[str]:
        lines: list[str] = []
        for source_path in spec.source_paths:
            lines.append(f"- [{source_path}]({self._source_link(source_path)})")
        for event_id in spec.event_ids:
            lines.append(f"- Capture event `{event_id}`")
        for artifact_id, artifact_type in self._artifact_refs_for_facts(group.facts):
            type_text = f" (`{artifact_type}`)" if artifact_type else ""
            lines.append(f"- Artifact `{artifact_id}`{type_text}")
        return lines

    def _evidence_lines(
        self,
        fact: _SemanticFactRecord,
        citation_numbers: Mapping[str, int],
    ) -> list[str]:
        lines = [f"### Candidate `{fact.candidate.candidate_id}`", ""]
        for evidence in fact.evidence:
            number = citation_numbers[evidence.evidence_id]
            details: list[str] = []
            if evidence.source_path:
                safe_source = _safe_source_path(evidence.source_path)
                if safe_source:
                    details.append(f"source `{safe_source}`")
            if evidence.capture_event_id:
                details.append(f"capture event `{evidence.capture_event_id}`")
            if evidence.artifact_id:
                artifact = f"artifact `{evidence.artifact_id}`"
                if evidence.artifact_type:
                    artifact += f" (`{evidence.artifact_type}`)"
                details.append(artifact)
            if evidence.source_timestamp:
                details.append(f"timestamp `{evidence.source_timestamp}`")
            if evidence.confidence is not None:
                details.append(f"confidence `{evidence.confidence:.2f}`")
            suffix = "; ".join(details) if details else "stored evidence record"
            lines.append(
                f"- <a id=\"{self._evidence_anchor(evidence.evidence_id)}\"></a>"
                f"[{number}] `{evidence.evidence_id}` - {suffix}"
            )
            if evidence.evidence_text:
                lines.append(f"  - Evidence: {evidence.evidence_text}")
        lines.append("")
        return lines

    def _citation_numbers(
        self,
        facts: tuple[_SemanticFactRecord, ...],
    ) -> dict[str, int]:
        evidence_ids = [
            evidence.evidence_id for fact in facts for evidence in fact.evidence
        ]
        return {
            evidence_id: index
            for index, evidence_id in enumerate(dict.fromkeys(evidence_ids), start=1)
        }

    def _citation_lines(
        self,
        facts: tuple[_SemanticFactRecord, ...],
        citation_numbers: Mapping[str, int],
    ) -> list[str]:
        by_id = {
            evidence.evidence_id: evidence
            for fact in facts
            for evidence in fact.evidence
        }
        lines: list[str] = []
        for evidence_id, number in sorted(
            citation_numbers.items(),
            key=lambda item: item[1],
        ):
            evidence = by_id[evidence_id]
            label = self._citation_label(evidence)
            lines.append(
                f"[{number}] [{label}](#{self._evidence_anchor(evidence_id)})"
            )
        return lines

    def _citation_label(self, evidence: SemanticMemoryEvidence) -> str:
        source_path = _safe_source_path(evidence.source_path)
        if source_path:
            return source_path
        if evidence.capture_event_id:
            return f"Capture event {evidence.capture_event_id}"
        if evidence.artifact_id:
            if evidence.artifact_type:
                return f"{evidence.artifact_type}:{evidence.artifact_id}"
            return f"Artifact {evidence.artifact_id}"
        return f"Evidence {evidence.evidence_id}"

    def _source_link(self, source_path: str) -> str:
        absolute_source = self.layout.vault_root / source_path
        return os.path.relpath(absolute_source, self.contract.pages_dir)

    def _candidate_anchor(self, candidate_id: str) -> str:
        return f"candidate-{_slug_component(candidate_id, 'semantic-candidate')}"

    def _evidence_anchor(self, evidence_id: str) -> str:
        return f"evidence-{_slug_component(evidence_id, 'semantic-evidence')}"


__all__ = [
    "SEMANTIC_WIKI_ALLOWED_STATUSES",
    "SEMANTIC_WIKI_PAGE_MARKER",
    "SEMANTIC_WIKI_PAGE_TYPE_KEY",
    "SemanticMemoryWikiCompiler",
    "SemanticWikiPageResult",
]
