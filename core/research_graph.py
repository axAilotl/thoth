"""Research paper graph ingestion, ranking, and reporting."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote

from .artifacts import PaperArtifact
from .capture_event_store import CaptureEventStore
from .config import Config, config as runtime_config
from .metadata_db import (
    MetadataDB,
    ResearchPaperEdge,
    ResearchPaperRecord,
    get_metadata_db,
)
from .path_layout import PathLayout, build_path_layout
from .wiki_contract import normalize_wiki_slug


DOI_RE = re.compile(r"\b(10\.\d{4,9}/[^\s\"<>]+)", re.IGNORECASE)
ARXIV_RE = re.compile(
    r"(?:arxiv:|arxiv\.org/(?:abs|pdf)/)?([0-9]{4}\.[0-9]{4,5})(?:v\d+)?",
    re.IGNORECASE,
)
OPENALEX_RE = re.compile(r"(?:https?://openalex\.org/)?(W\d+)", re.IGNORECASE)


@dataclass(frozen=True)
class PaperReference:
    """Normalized reference or citation discovered for a paper."""

    paper_id: str
    title: str = ""
    doi: str | None = None
    arxiv_id: str | None = None
    pdf_url: str | None = None
    venue: str | None = None
    published_at: str | None = None
    authors: tuple[str, ...] = ()
    source_evidence: str = ""
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def to_record(self, *, collected: bool = False) -> ResearchPaperRecord:
        return ResearchPaperRecord(
            paper_id=self.paper_id,
            title=self.title,
            authors=self.authors,
            doi=self.doi,
            arxiv_id=self.arxiv_id,
            pdf_url=self.pdf_url,
            venue=self.venue,
            published_at=self.published_at,
            collected=collected,
            raw_payload=self.raw_payload,
        )


class ResearchGraphService:
    """Service object for paper graph updates and missing-paper reporting."""

    def __init__(
        self,
        db: MetadataDB | None = None,
        *,
        metadata_provider: "ResearchMetadataProvider | None" = None,
        config: Config | None = None,
        layout: PathLayout | None = None,
        capture_event_store: CaptureEventStore | None = None,
    ):
        self.config = config or runtime_config
        self.layout = layout or build_path_layout(self.config)
        self.db = db or get_metadata_db()
        self.metadata_provider = metadata_provider
        from .connector_capture import ConnectorCaptureQueue

        self.capture_queue = ConnectorCaptureQueue(
            self.config,
            layout=self.layout,
            db=self.db,
            capture_event_store=capture_event_store,
        )

    def record_paper_artifact(
        self,
        artifact: PaperArtifact,
        *,
        discovery_source: str | None = None,
        queue_missing: bool = True,
        pdf_paths: list[str | Path] | tuple[str | Path, ...] | None = None,
    ) -> dict[str, Any]:
        """Persist a collected paper and relationships extracted from metadata."""
        source_record = paper_record_from_artifact(artifact, collected=True)
        self.db.upsert_research_paper(source_record)

        discovered_at = datetime.now().isoformat()
        inserted_edges = 0
        references = _dedupe_references(
            [
                *extract_references_from_paper(artifact),
                *extract_references_from_pdf_paths(
                    pdf_paths or discover_pdf_paths_from_artifact(artifact)
                ),
                *self._metadata_references_for_artifact(artifact),
            ]
        )
        citations = extract_citations_from_paper(artifact)

        for reference in references:
            self.db.upsert_research_paper(reference.to_record(collected=False))
            if self.db.upsert_research_paper_edge(
                ResearchPaperEdge(
                    source_paper_id=source_record.paper_id,
                    target_paper_id=reference.paper_id,
                    edge_type="references",
                    source_evidence=reference.source_evidence,
                    discovery_source=discovery_source or artifact.source_provider or artifact.source_type,
                    discovered_at=discovered_at,
                    metadata=reference.raw_payload,
                )
            ):
                inserted_edges += 1

        for citation in citations:
            self.db.upsert_research_paper(citation.to_record(collected=False))
            if self.db.upsert_research_paper_edge(
                ResearchPaperEdge(
                    source_paper_id=source_record.paper_id,
                    target_paper_id=citation.paper_id,
                    edge_type="cited_by",
                    source_evidence=citation.source_evidence,
                    discovery_source=discovery_source or artifact.source_provider or artifact.source_type,
                    discovered_at=discovered_at,
                    metadata=citation.raw_payload,
                )
            ):
                inserted_edges += 1

        co_referenced_edges = self._derive_co_referenced_edges(source_record.paper_id)
        queued_missing = (
            self.queue_high_confidence_missing_papers()["queued"]
            if queue_missing
            else []
        )
        return {
            "paper_id": source_record.paper_id,
            "references": len(references),
            "citations": len(citations),
            "inserted_edges": inserted_edges,
            "co_referenced_edges": co_referenced_edges,
            "queued_missing": queued_missing,
        }

    def _metadata_references_for_artifact(
        self,
        artifact: PaperArtifact,
    ) -> list[PaperReference]:
        if not self.metadata_provider:
            return []
        return self.metadata_provider.references_for_artifact(artifact)

    def missing_papers_report(
        self,
        *,
        min_references: int = 2,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Return ranked missing-paper candidates split by review posture."""
        candidates = self.db.list_missing_research_paper_candidates(
            min_references=min_references,
            limit=limit,
        )
        high_confidence = [
            candidate for candidate in candidates if candidate["status"] == "high_confidence"
        ]
        ambiguous = [
            candidate for candidate in candidates if candidate["status"] == "ambiguous"
        ]
        return {
            "min_references": min_references,
            "total": len(candidates),
            "high_confidence": high_confidence,
            "ambiguous": ambiguous,
        }

    def queue_high_confidence_missing_papers(
        self,
        *,
        min_references: int = 2,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Queue high-confidence missing paper candidates when an identifier is available."""
        report = self.missing_papers_report(
            min_references=min_references,
            limit=limit,
        )
        queued: list[str] = []
        skipped: list[str] = []
        run_id = datetime.now().isoformat()
        with self.capture_queue.lifecycle() as lifecycle:
            for candidate in report["high_confidence"]:
                artifact = paper_artifact_from_missing_candidate(candidate)
                queue_id = f"research_graph:{candidate['paper_id']}"
                if self.db.get_ingestion_entry(queue_id):
                    skipped.append(queue_id)
                    continue
                self._queue_missing_paper_candidate(
                    artifact,
                    candidate=candidate,
                    queue_id=queue_id,
                    lifecycle=lifecycle,
                    run_id=run_id,
                )
                queued.append(queue_id)
        return {"queued": queued, "skipped": skipped, "report": report}

    def _queue_missing_paper_candidate(
        self,
        artifact: PaperArtifact,
        *,
        candidate: Mapping[str, Any],
        queue_id: str,
        lifecycle,
        run_id: str,
    ) -> None:
        raw_path = None
        if lifecycle.capture_event_store is not None:
            from .connector_capture import write_connector_raw_json

            raw_path = write_connector_raw_json(
                self.layout,
                connector_name="research_graph",
                subdir="missing_papers",
                native_id=str(candidate.get("paper_id") or queue_id),
                payload=dict(candidate),
                captured_at=artifact.ingested_at,
            )

        self.capture_queue.queue_payload(
            lifecycle,
            artifact_type="paper",
            payload=artifact.to_dict(),
            source={
                "source_name": "research_graph",
                "source_type": "research_graph",
                "collector": "research_graph",
                "native_source_id": str(candidate.get("paper_id") or queue_id),
                "base_uri": "research_graph://missing-papers",
                "metadata": {
                    "candidate_status": candidate.get("status"),
                    "referenced_by_count": candidate.get("referenced_by_count"),
                },
            },
            session={
                "session_type": "research_graph_missing_papers",
                "native_session_id": f"research_graph:{run_id}",
                "started_at": run_id,
                "metadata": {
                    "min_references": candidate.get("min_references"),
                },
            },
            event={
                "event_type": "research_graph_missing_paper",
                "native_event_id": str(candidate.get("paper_id") or queue_id),
                "captured_at": artifact.ingested_at,
                "privacy": {"classification": "public"},
                "provenance": {"collector": "research_graph"},
            },
            raw_path=raw_path,
            queue_artifact_id=queue_id,
            priority=int(candidate["referenced_by_count"]),
            capabilities=artifact.capabilities,
        )
        if self.db.get_ingestion_entry(queue_id) is None:
            raise RuntimeError(f"Failed to queue missing paper candidate: {queue_id}")

    def paper_context(self, artifact_or_paper_id: PaperArtifact | str) -> dict[str, Any]:
        """Return graph context for wiki/API consumers."""
        artifact = (
            artifact_or_paper_id
            if isinstance(artifact_or_paper_id, PaperArtifact)
            else None
        )
        paper_id = (
            paper_record_from_artifact(artifact_or_paper_id, collected=True).paper_id
            if artifact is not None
            else str(artifact_or_paper_id)
        )
        context = self.db.get_research_paper_context(paper_id)
        record = self.db.get_research_paper(paper_id)
        if record is not None:
            context["paper"] = _research_paper_context_record(record)
        local_context = _local_research_context(
            artifact=artifact,
            record=record,
        )
        if local_context:
            context["local"] = local_context
        return context

    def _derive_co_referenced_edges(self, source_paper_id: str) -> int:
        inserted = 0
        source_references = self.db.list_research_paper_edges(
            source_paper_id=source_paper_id,
            edge_type="references",
        )
        for reference_edge in source_references:
            sibling_edges = self.db.list_research_paper_edges(
                target_paper_id=reference_edge.target_paper_id,
                edge_type="references",
            )
            for sibling in sibling_edges:
                if sibling.source_paper_id == source_paper_id:
                    continue
                evidence = f"Shared reference: {reference_edge.target_paper_id}"
                metadata = {"shared_reference": reference_edge.target_paper_id}
                if self.db.upsert_research_paper_edge(
                    ResearchPaperEdge(
                        source_paper_id=source_paper_id,
                        target_paper_id=sibling.source_paper_id,
                        edge_type="co_referenced",
                        source_evidence=evidence,
                        discovery_source="research_graph",
                        metadata=metadata,
                    )
                ):
                    inserted += 1
                self.db.upsert_research_paper_edge(
                    ResearchPaperEdge(
                        source_paper_id=sibling.source_paper_id,
                        target_paper_id=source_paper_id,
                        edge_type="co_referenced",
                        source_evidence=evidence,
                        discovery_source="research_graph",
                        metadata=metadata,
                    )
                )
        return inserted


def paper_record_from_artifact(
    artifact: PaperArtifact,
    *,
    collected: bool,
) -> ResearchPaperRecord:
    paper_id = normalized_paper_id(
        artifact_id=artifact.id,
        doi=artifact.doi,
        arxiv_id=artifact.arxiv_id,
        title=artifact.title,
    )
    raw_payload = artifact.canonical_record()
    if artifact.custom_metadata:
        raw_payload["custom_metadata"] = dict(artifact.custom_metadata)

    return ResearchPaperRecord(
        paper_id=paper_id,
        title=artifact.title,
        authors=tuple(str(author) for author in artifact.authors or []),
        abstract=artifact.abstract,
        doi=normalize_doi(artifact.doi),
        arxiv_id=normalize_arxiv_id(artifact.arxiv_id),
        pdf_url=artifact.pdf_url,
        venue=artifact.venue,
        published_at=artifact.published_at or artifact.created_at,
        source_provider=artifact.source_provider or artifact.source_type,
        collected=collected,
        raw_payload=raw_payload,
        updated_at=artifact.ingested_at or datetime.now().isoformat(),
    )


def _research_paper_context_record(record: ResearchPaperRecord) -> dict[str, Any]:
    return {
        "paper_id": record.paper_id,
        "title": record.title or record.paper_id,
        "authors": list(record.authors),
        "doi": record.doi,
        "arxiv_id": record.arxiv_id,
        "pdf_url": record.pdf_url,
        "venue": record.venue,
        "published_at": record.published_at,
        "source_provider": record.source_provider,
        "collected": record.collected,
        "updated_at": record.updated_at,
    }


def _local_research_context(
    *,
    artifact: PaperArtifact | None,
    record: ResearchPaperRecord | None,
) -> dict[str, Any]:
    mappings: list[Mapping[str, Any]] = []
    if record and isinstance(record.raw_payload, Mapping):
        mappings.extend(_metadata_mappings_from_payload(record.raw_payload))
    if artifact is not None:
        mappings.extend(_metadata_mappings_from_artifact(artifact))

    projects = _project_refs_from_mappings(mappings)
    events = _event_refs_from_mappings(mappings)
    source_paths = _source_paths_from_mappings(mappings)

    context: dict[str, Any] = {}
    if projects:
        context["projects"] = projects
    if events:
        context["events"] = events
    if source_paths:
        context["source_paths"] = source_paths
    return context


def _metadata_mappings_from_artifact(
    artifact: PaperArtifact,
) -> list[Mapping[str, Any]]:
    mappings: list[Mapping[str, Any]] = []
    for value in (artifact.normalized_metadata, artifact.custom_metadata):
        if isinstance(value, Mapping):
            mappings.append(value)
    mappings.extend(_metadata_mappings_from_payload(artifact.canonical_record()))
    return mappings


def _metadata_mappings_from_payload(
    payload: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    mappings: list[Mapping[str, Any]] = [payload]
    for key in (
        "custom_metadata",
        "normalized_metadata",
        "provenance",
        "raw_payload",
        "source_identity",
        "timestamps",
    ):
        value = payload.get(key)
        if isinstance(value, Mapping):
            mappings.append(value)
    provenance = payload.get("provenance")
    if isinstance(provenance, Mapping):
        raw_payload = provenance.get("raw_payload")
        source_identity = provenance.get("source_identity")
        if isinstance(raw_payload, Mapping):
            mappings.append(raw_payload)
        if isinstance(source_identity, Mapping):
            mappings.append(source_identity)
    return mappings


_EVENT_METADATA_KEYS = (
    "thoth_event_id",
    "thoth_event_ids",
    "event_id",
    "event_ids",
    "capture_event_id",
    "capture_event_ids",
)
_PROJECT_METADATA_KEYS = ("projects", "repositories")
_PROJECT_SINGLE_METADATA_KEYS = ("project", "repository", "repo_name")


def _event_refs_from_mappings(
    mappings: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    source = _first_mapping_string(
        mappings,
        "source_name",
        "source_type",
        "source",
        "collector",
    )
    timestamp = _first_mapping_string(
        mappings,
        "captured_at",
        "ingested_at",
        "occurred_at",
        "created_at",
        "published_at",
        "updated_at",
    )
    events: dict[str, dict[str, Any]] = {}
    for mapping in mappings:
        for key in _EVENT_METADATA_KEYS:
            for event_id in _string_values(mapping.get(key)):
                events.setdefault(
                    event_id,
                    {
                        "event_id": event_id,
                        "source": source,
                        "timestamp": timestamp,
                    },
                )
    return [events[key] for key in sorted(events)]


def _project_refs_from_mappings(
    mappings: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    projects: dict[str, dict[str, Any]] = {}
    for mapping in mappings:
        for key in _PROJECT_METADATA_KEYS:
            for value in _as_sequence(mapping.get(key)):
                project = _project_ref_from_value(value)
                if project:
                    projects.setdefault(project["slug"], project)
        for key in _PROJECT_SINGLE_METADATA_KEYS:
            project = _project_ref_from_value(mapping.get(key))
            if project:
                projects.setdefault(project["slug"], project)
    return [projects[key] for key in sorted(projects)]


def _project_ref_from_value(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        label = _first_string(
            value,
            "name",
            "title",
            "label",
            "full_name",
            "repo_name",
            "id",
            "slug",
        )
        identifier = _first_string(value, "id", "slug", "repo_name", "full_name", "name")
    else:
        label = str(value).strip()
        identifier = label
    if not label:
        return None
    slug_source = identifier or label
    try:
        slug = normalize_wiki_slug(slug_source)
    except ValueError:
        return None
    return {
        "id": identifier,
        "label": label,
        "slug": slug,
    }


def _source_paths_from_mappings(
    mappings: list[Mapping[str, Any]],
) -> list[str]:
    paths: dict[str, None] = {}
    for mapping in mappings:
        for key in ("source_path", "source_paths", "path", "evidence_paths"):
            for value in _string_values(mapping.get(key)):
                paths.setdefault(value, None)
    return sorted(paths)


def _string_values(value: Any) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, Mapping):
        values: list[str] = []
        for key in ("id", "event_id", "capture_event_id", "path"):
            if key in value:
                values.extend(_string_values(value[key]))
        return tuple(values)
    values: list[str] = []
    for item in _as_sequence(value):
        if isinstance(item, Mapping):
            values.extend(_string_values(item))
            continue
        text = str(item).strip()
        if text:
            values.append(text)
    return tuple(values)


def _first_mapping_string(
    mappings: list[Mapping[str, Any]],
    *keys: str,
) -> str | None:
    for mapping in mappings:
        value = _first_string(mapping, *keys)
        if value:
            return value
    return None


def paper_artifact_from_missing_candidate(candidate: Mapping[str, Any]) -> PaperArtifact:
    paper_id = str(candidate.get("paper_id") or "").strip()
    arxiv_id = normalize_arxiv_id(candidate.get("arxiv_id"))
    doi = normalize_doi(candidate.get("doi"))
    artifact_id = arxiv_id or doi or paper_id
    return PaperArtifact(
        id=artifact_id,
        source_type="research_graph",
        raw_content=json.dumps(dict(candidate), ensure_ascii=False),
        created_at=candidate.get("published_at"),
        ingested_at=datetime.now().isoformat(),
        title=str(candidate.get("title") or paper_id),
        doi=doi,
        arxiv_id=arxiv_id,
        pdf_url=candidate.get("pdf_url"),
        venue=candidate.get("venue"),
        published_at=candidate.get("published_at"),
        source_provider="research_graph",
        custom_metadata={
            "missing_candidate": True,
            "referenced_by": list(candidate.get("referenced_by") or []),
            "referenced_by_count": candidate.get("referenced_by_count"),
        },
    )


def extract_references_from_paper(artifact: PaperArtifact) -> list[PaperReference]:
    return _extract_related_papers(artifact, field_names=("references", "reference_ids"))


def extract_citations_from_paper(artifact: PaperArtifact) -> list[PaperReference]:
    return _extract_related_papers(artifact, field_names=("citations", "cited_by"))


def _extract_related_papers(
    artifact: PaperArtifact,
    *,
    field_names: tuple[str, ...],
) -> list[PaperReference]:
    payloads: list[Any] = []
    for field_name in field_names:
        payloads.extend(_as_sequence(getattr(artifact, field_name, None)))

    metadata = artifact.custom_metadata if isinstance(artifact.custom_metadata, Mapping) else {}
    for field_name in field_names:
        payloads.extend(_as_sequence(metadata.get(field_name)))

    raw_payload = _parse_raw_content(artifact.raw_content)
    if isinstance(raw_payload, Mapping):
        for field_name in field_names:
            payloads.extend(_as_sequence(raw_payload.get(field_name)))
        if "references" in field_names:
            payloads.extend(_as_sequence(raw_payload.get("outbound_citations")))
            payloads.extend(_as_sequence(raw_payload.get("bibliography")))
            bib_entries = raw_payload.get("bib_entries")
            if isinstance(bib_entries, Mapping):
                payloads.extend(bib_entries.values())
        if "citations" in field_names:
            payloads.extend(_as_sequence(raw_payload.get("inbound_citations")))

    if isinstance(raw_payload, str) and "references" in field_names:
        payloads.extend(_reference_lines_from_text(raw_payload))
    elif not isinstance(raw_payload, Mapping) and "references" in field_names:
        payloads.extend(_reference_lines_from_text(artifact.raw_content))

    references: dict[str, PaperReference] = {}
    for payload in payloads:
        reference = _paper_reference_from_payload(payload)
        if not reference:
            continue
        references[reference.paper_id] = reference
    return list(references.values())


def normalized_paper_id(
    *,
    artifact_id: str | None = None,
    doi: str | None = None,
    arxiv_id: str | None = None,
    openalex_id: str | None = None,
    title: str | None = None,
) -> str:
    normalized_doi = normalize_doi(doi)
    if normalized_doi:
        return f"doi:{normalized_doi}"
    normalized_arxiv = normalize_arxiv_id(arxiv_id or artifact_id)
    if normalized_arxiv:
        return f"arxiv:{normalized_arxiv}"
    normalized_openalex = normalize_openalex_id(openalex_id or artifact_id)
    if normalized_openalex:
        return f"openalex:{normalized_openalex}"
    title_text = str(title or "").strip()
    if title_text:
        return f"title:{normalize_wiki_slug(title_text)}"
    fallback = str(artifact_id or "").strip()
    if fallback:
        return f"paper:{normalize_wiki_slug(fallback)}"
    raise ValueError("paper identity requires DOI, arXiv ID, title, or artifact ID")


def normalize_doi(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().rstrip(".,;")
    text = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^doi:\s*", "", text, flags=re.IGNORECASE)
    match = DOI_RE.search(text)
    if match:
        return match.group(1).rstrip(".,;").lower()
    return text.lower() if text.startswith("10.") else None


def normalize_arxiv_id(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().rstrip(".,;")
    match = ARXIV_RE.search(text)
    if not match:
        return None
    return match.group(1)


def normalize_openalex_id(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().rstrip(".,;")
    match = OPENALEX_RE.search(text)
    if not match:
        return None
    return match.group(1).upper()


def _paper_reference_from_payload(payload: Any) -> PaperReference | None:
    raw_payload: dict[str, Any]
    if isinstance(payload, Mapping):
        raw_payload = dict(payload)
        title = _first_string(raw_payload, "title", "paper_title", "name")
        doi = normalize_doi(_first_string(raw_payload, "doi", "DOI"))
        arxiv_id = normalize_arxiv_id(
            _first_string(raw_payload, "arxiv_id", "arxiv", "arxivId", "paper_id", "id")
        )
        openalex_id = normalize_openalex_id(
            _first_string(raw_payload, "openalex_id", "openalex", "paper_id", "id")
        )
        pdf_url = _first_string(raw_payload, "pdf_url", "pdfUrl", "url", "href")
        if pdf_url and not arxiv_id:
            arxiv_id = normalize_arxiv_id(pdf_url)
        if pdf_url and not openalex_id:
            openalex_id = normalize_openalex_id(pdf_url)
        venue = _first_string(raw_payload, "venue", "journal", "conference")
        published_at = _first_string(raw_payload, "published_at", "published", "year")
        authors = tuple(str(author) for author in _as_sequence(raw_payload.get("authors")))
        evidence = _first_string(raw_payload, "source_evidence", "citation", "raw", "text")
        identifier = _first_string(raw_payload, "paper_id", "id")
    else:
        text = str(payload or "").strip()
        if not text:
            return None
        raw_payload = {"text": text}
        title = text if not DOI_RE.search(text) and not ARXIV_RE.search(text) else ""
        doi = normalize_doi(text)
        arxiv_id = normalize_arxiv_id(text)
        openalex_id = normalize_openalex_id(text)
        pdf_url = _first_url(text)
        venue = None
        published_at = None
        authors = ()
        evidence = text
        identifier = None

    try:
        paper_id = normalized_paper_id(
            artifact_id=identifier,
            doi=doi,
            arxiv_id=arxiv_id,
            openalex_id=openalex_id,
            title=title,
        )
    except ValueError:
        return None

    return PaperReference(
        paper_id=paper_id,
        title=title or paper_id,
        doi=doi,
        arxiv_id=arxiv_id,
        pdf_url=pdf_url,
        venue=venue,
        published_at=published_at,
        authors=authors,
        source_evidence=evidence or title or paper_id,
        raw_payload=raw_payload,
    )


def _parse_raw_content(raw_content: str) -> Any:
    if not raw_content:
        return None
    try:
        return json.loads(raw_content)
    except Exception:
        return raw_content


def _reference_lines_from_text(text: str) -> list[str]:
    lines = [line.strip() for line in (text or "").splitlines()]
    if not lines:
        return []
    lowered = [line.lower() for line in lines]
    start = 0
    for index, line in enumerate(lowered):
        if line in {"references", "bibliography"} or line.startswith("references"):
            start = index + 1
            break
    candidates = []
    for line in lines[start:]:
        if not line:
            continue
        if DOI_RE.search(line) or ARXIV_RE.search(line):
            candidates.append(line)
    return candidates[:200]


def _as_sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _first_string(payload: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _first_url(text: str) -> str | None:
    match = re.search(r"https?://[^\s\"<>]+", text or "")
    return match.group(0).rstrip(".,;") if match else None


class ResearchMetadataProvider:
    """Protocol-like base for optional metadata reference providers."""

    def references_for_artifact(self, artifact: PaperArtifact) -> list[PaperReference]:
        raise NotImplementedError


class OpenAlexMetadataProvider(ResearchMetadataProvider):
    """Fetch reference metadata from OpenAlex when enabled by config."""

    base_url = "https://api.openalex.org"

    def __init__(
        self,
        *,
        timeout_seconds: float = 10.0,
        max_references: int = 20,
        session: Any = None,
        mailto: str | None = None,
        api_key: str | None = None,
    ):
        if session is None:
            import requests

            session = requests.Session()
        self.session = session
        self.timeout_seconds = timeout_seconds
        self.max_references = max(0, int(max_references))
        self.mailto = mailto
        self.api_key = api_key

    def references_for_artifact(self, artifact: PaperArtifact) -> list[PaperReference]:
        work_id = self._work_identifier_for_artifact(artifact)
        if not work_id:
            return []
        work = self._fetch_work(work_id)
        referenced_work_ids = [
            str(value)
            for value in (work.get("referenced_works") or [])
            if str(value).strip()
        ][: self.max_references]
        references: list[PaperReference] = []
        for referenced_work_id in referenced_work_ids:
            try:
                referenced_work = self._fetch_work(referenced_work_id)
            except Exception:
                referenced_work = {"id": referenced_work_id}
            reference = paper_reference_from_openalex_work(
                referenced_work,
                source_evidence=f"OpenAlex referenced_works from {work_id}",
            )
            if reference:
                references.append(reference)
        return _dedupe_references(references)

    def _work_identifier_for_artifact(self, artifact: PaperArtifact) -> str | None:
        doi = normalize_doi(artifact.doi)
        if doi:
            return f"doi:{doi}"
        metadata = artifact.custom_metadata if isinstance(artifact.custom_metadata, Mapping) else {}
        openalex_id = normalize_openalex_id(
            metadata.get("openalex_id") or metadata.get("openalex")
        )
        if openalex_id:
            return openalex_id
        return None

    def _fetch_work(self, work_id: str) -> Mapping[str, Any]:
        encoded_id = quote(str(work_id).strip(), safe=":")
        params = {}
        if self.mailto:
            params["mailto"] = self.mailto
        if self.api_key:
            params["api_key"] = self.api_key
        response = self.session.get(
            f"{self.base_url}/works/{encoded_id}",
            params=params or None,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise ValueError("OpenAlex work response must be an object")
        return payload


def build_research_metadata_provider(config: Config) -> ResearchMetadataProvider | None:
    provider_config = config.get("research_graph.metadata_api", {}) or {}
    if not isinstance(provider_config, Mapping):
        return None
    if not provider_config.get("enabled", False):
        return None
    provider_name = str(provider_config.get("provider") or "openalex").strip().lower()
    if provider_name != "openalex":
        raise ValueError(f"Unsupported research metadata provider: {provider_name}")
    return OpenAlexMetadataProvider(
        timeout_seconds=float(provider_config.get("timeout_seconds", 10.0) or 10.0),
        max_references=int(provider_config.get("max_references", 20) or 20),
        mailto=_clean_optional_string(provider_config.get("mailto")),
        api_key=_clean_optional_string(provider_config.get("api_key")),
    )


def paper_reference_from_openalex_work(
    work: Mapping[str, Any],
    *,
    source_evidence: str,
) -> PaperReference | None:
    openalex_id = normalize_openalex_id(work.get("id"))
    doi = normalize_doi(work.get("doi"))
    title = str(work.get("display_name") or work.get("title") or "").strip()
    pdf_url = _openalex_pdf_url(work)
    authors = tuple(_openalex_author_names(work))
    venue = _openalex_venue(work)
    published_at = _clean_optional_string(
        work.get("publication_date") or work.get("publication_year")
    )
    try:
        paper_id = normalized_paper_id(
            doi=doi,
            openalex_id=openalex_id,
            title=title,
        )
    except ValueError:
        return None
    return PaperReference(
        paper_id=paper_id,
        title=title or paper_id,
        doi=doi,
        pdf_url=pdf_url,
        venue=venue,
        published_at=published_at,
        authors=authors,
        source_evidence=source_evidence,
        raw_payload=dict(work),
    )


def discover_pdf_paths_from_artifact(artifact: PaperArtifact) -> list[Path]:
    metadata = artifact.custom_metadata if isinstance(artifact.custom_metadata, Mapping) else {}
    candidates: list[Path] = []
    for key in ("pdf_path", "local_pdf_path", "downloaded_pdf_path"):
        value = metadata.get(key)
        if value:
            candidates.append(Path(str(value)))
    for key, value in (artifact.output_paths or {}).items():
        if "pdf" in str(key).lower() and value:
            candidates.append(Path(str(value)))
    return _dedupe_paths(candidates)


def extract_references_from_pdf_paths(paths: Iterable[str | Path]) -> list[PaperReference]:
    references: dict[str, PaperReference] = {}
    for path_value in paths:
        path = Path(path_value)
        if not path.exists() or not path.is_file():
            continue
        text = extract_text_from_pdf(path)
        for line in _reference_lines_from_text(text):
            reference = _paper_reference_from_payload(line)
            if not reference:
                continue
            raw_payload = {
                **reference.raw_payload,
                "pdf_path": str(path),
                "source": "pdf_reference_extraction",
            }
            references[reference.paper_id] = PaperReference(
                paper_id=reference.paper_id,
                title=reference.title,
                doi=reference.doi,
                arxiv_id=reference.arxiv_id,
                pdf_url=reference.pdf_url,
                venue=reference.venue,
                published_at=reference.published_at,
                authors=reference.authors,
                source_evidence=f"PDF references in {path.name}: {reference.source_evidence}",
                raw_payload=raw_payload,
            )
    return list(references.values())


def extract_text_from_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader

        reader = PdfReader(str(path))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception:
        pass

    try:
        from PyPDF2 import PdfReader

        reader = PdfReader(str(path))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception:
        pass

    try:
        from pdfminer.high_level import extract_text

        return extract_text(str(path)) or ""
    except Exception:
        pass

    try:
        return path.read_bytes().decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _dedupe_references(references: Iterable[PaperReference]) -> list[PaperReference]:
    deduped: dict[str, PaperReference] = {}
    for reference in references:
        deduped[reference.paper_id] = reference
    return list(deduped.values())


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    deduped: dict[Path, Path] = {}
    for path in paths:
        deduped[path] = path
    return list(deduped.values())


def _openalex_pdf_url(work: Mapping[str, Any]) -> str | None:
    open_access = work.get("open_access")
    if isinstance(open_access, Mapping):
        url = _clean_optional_string(open_access.get("oa_url"))
        if url:
            return url
    primary_location = work.get("primary_location")
    if isinstance(primary_location, Mapping):
        return _clean_optional_string(
            primary_location.get("pdf_url") or primary_location.get("landing_page_url")
        )
    return None


def _openalex_author_names(work: Mapping[str, Any]) -> list[str]:
    names: list[str] = []
    authorships = work.get("authorships")
    if not isinstance(authorships, list):
        return names
    for authorship in authorships:
        if not isinstance(authorship, Mapping):
            continue
        author = authorship.get("author")
        if isinstance(author, Mapping):
            name = _clean_optional_string(author.get("display_name"))
            if name:
                names.append(name)
    return names


def _openalex_venue(work: Mapping[str, Any]) -> str | None:
    primary_location = work.get("primary_location")
    if not isinstance(primary_location, Mapping):
        return None
    source = primary_location.get("source")
    if not isinstance(source, Mapping):
        return None
    return _clean_optional_string(source.get("display_name"))


def _clean_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
