"""Render research graph context for compiled wiki paper pages."""

from __future__ import annotations

from typing import Any, Mapping

from .wiki_contract import normalize_wiki_slug


def research_context_lines(research_context: Mapping[str, Any]) -> list[str]:
    """Render local relevance lines for a research paper wiki page."""
    referenced_by = list(research_context.get("referenced_by") or [])
    references = list(research_context.get("references") or [])
    co_referenced = list(research_context.get("co_referenced") or [])
    local_context = (
        research_context.get("local")
        if isinstance(research_context.get("local"), Mapping)
        else {}
    )
    projects = list(local_context.get("projects") or [])
    events = list(local_context.get("events") or [])
    lines: list[str] = []

    if referenced_by:
        lines.append(
            "- Why it matters: "
            f"`{len(referenced_by)}` collected local paper(s) reference this work, "
            "so this page anchors an active local research thread instead of only "
            "restating the abstract."
        )
    elif references:
        missing_count = sum(1 for item in references if not item.get("collected"))
        local_count = len(references) - missing_count
        lines.append(
            "- Why it matters: this paper adds local research context through "
            f"`{local_count}` collected reference(s) and "
            f"`{missing_count}` missing follow-up candidate(s)."
        )
    elif projects or events:
        lines.append(
            "- Why it matters: this collected paper is tied to local project or "
            "capture context even though no research graph edges have been "
            "discovered yet."
        )
    else:
        lines.append(
            "- Why it matters: this paper is in the local research corpus, but no "
            "paper graph, project, or capture-event relationships have been "
            "recorded yet."
        )

    local_count = sum(1 for item in references if item.get("collected"))
    missing_count = sum(1 for item in references if not item.get("collected"))
    if references or co_referenced:
        lines.append(
            "- New context added: "
            f"`{local_count}` local reference(s), "
            f"`{missing_count}` missing candidate(s), and "
            f"`{len(co_referenced)}` co-reference connection(s)."
        )

    if projects:
        lines.append("- Relationship to current projects:")
        for project in projects[:10]:
            label = str(project.get("label") or project.get("id") or "").strip()
            slug = str(project.get("slug") or "").strip()
            if not label:
                continue
            if slug:
                lines.append(f"  - [{label}](project-{slug}.md)")
            else:
                lines.append(f"  - {label}")

    if events:
        lines.append("- Local capture events:")
        for event in events[:10]:
            event_id = str(event.get("event_id") or "").strip()
            if not event_id:
                continue
            line = (
                f'  - <a id="{event_citation_anchor(event_id)}"></a>'
                f"`{event_id}`"
            )
            timestamp = str(event.get("timestamp") or "").strip()
            source = str(event.get("source") or "").strip()
            if timestamp:
                line += f" at `{timestamp}`"
            if source:
                line += f" from `{source}`"
            lines.append(line)

    if referenced_by:
        lines.append("- Local papers referencing this:")
        for item in referenced_by[:10]:
            lines.append(f"  - `{item['paper_id']}` - {item['title']}")

    if references:
        lines.append("- References discovered from this paper:")
        for item in references[:15]:
            status = "local" if item.get("collected") else "missing"
            lines.append(f"  - `{item['paper_id']}` ({status}) - {item['title']}")

    if co_referenced:
        lines.append("- Co-referenced local papers:")
        for item in co_referenced[:10]:
            lines.append(f"  - `{item['paper_id']}` - {item['title']}")

    return lines


def research_citation_lines(
    research_context: Mapping[str, Any] | None,
    *,
    start_index: int,
) -> list[str]:
    """Render optional research citations after required wiki citations."""
    if not research_context:
        return []
    citations: list[str] = []
    next_index = start_index
    for item in _local_paper_citations(research_context):
        title = _citation_label(item["title"])
        citations.append(
            f"[{next_index}] [Local paper: {title}]({item['target']})"
        )
        next_index += 1
    for event_id in _event_citation_ids(research_context):
        label = _citation_label(event_id)
        citations.append(
            f"[{next_index}] [Capture event {label}]"
            f"(#{event_citation_anchor(event_id)})"
        )
        next_index += 1
    return citations


def event_citation_anchor(event_id: str) -> str:
    return f"capture-event-{normalize_wiki_slug(event_id)}"


def _local_paper_citations(
    research_context: Mapping[str, Any],
) -> list[dict[str, str]]:
    citations: dict[str, dict[str, str]] = {}
    for key in ("referenced_by", "references", "co_referenced"):
        for item in research_context.get(key) or []:
            if key == "references" and not item.get("collected"):
                continue
            paper_id = str(item.get("paper_id") or "").strip()
            if not paper_id:
                continue
            title = str(item.get("title") or paper_id).strip()
            citations.setdefault(
                paper_id,
                {
                    "title": title,
                    "target": _paper_link_target(paper_id),
                },
            )
    return [citations[key] for key in sorted(citations)]


def _event_citation_ids(
    research_context: Mapping[str, Any],
) -> list[str]:
    local_context = research_context.get("local")
    if not isinstance(local_context, Mapping):
        return []
    event_ids = {
        str(event.get("event_id") or "").strip()
        for event in local_context.get("events") or []
        if isinstance(event, Mapping)
    }
    return sorted(event_id for event_id in event_ids if event_id)


def _paper_link_target(paper_id: str) -> str:
    if paper_id.startswith("arxiv:"):
        slug_source = paper_id.removeprefix("arxiv:")
    else:
        slug_source = paper_id
    return f"paper-{normalize_wiki_slug(slug_source)}.md"


def _citation_label(value: str) -> str:
    return value.replace("[", "(").replace("]", ")")
