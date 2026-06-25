"""Research paper graph ingestion, ranking, and reporting."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping

from .artifacts import PaperArtifact
from .metadata_db import (
    IngestionQueueEntry,
    MetadataDB,
    ResearchPaperEdge,
    ResearchPaperRecord,
    get_metadata_db,
)
from .wiki_contract import normalize_wiki_slug


DOI_RE = re.compile(r"\b(10\.\d{4,9}/[^\s\"<>]+)", re.IGNORECASE)
ARXIV_RE = re.compile(
    r"(?:arxiv:|arxiv\.org/(?:abs|pdf)/)?([0-9]{4}\.[0-9]{4,5})(?:v\d+)?",
    re.IGNORECASE,
)


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

    def __init__(self, db: MetadataDB | None = None):
        self.db = db or get_metadata_db()

    def record_paper_artifact(
        self,
        artifact: PaperArtifact,
        *,
        discovery_source: str | None = None,
        queue_missing: bool = True,
    ) -> dict[str, Any]:
        """Persist a collected paper and relationships extracted from metadata."""
        source_record = paper_record_from_artifact(artifact, collected=True)
        self.db.upsert_research_paper(source_record)

        discovered_at = datetime.now().isoformat()
        inserted_edges = 0
        references = extract_references_from_paper(artifact)
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
        for candidate in report["high_confidence"]:
            artifact = paper_artifact_from_missing_candidate(candidate)
            queue_id = f"research_graph:{candidate['paper_id']}"
            if self.db.get_ingestion_entry(queue_id):
                skipped.append(queue_id)
                continue
            entry = IngestionQueueEntry(
                artifact_id=queue_id,
                artifact_type="paper",
                source="research_graph",
                payload_json=json.dumps(artifact.to_dict(), ensure_ascii=False),
                capabilities_json=json.dumps(list(artifact.capabilities)),
                created_at=datetime.now().isoformat(),
                priority=int(candidate["referenced_by_count"]),
            )
            if not self.db.upsert_ingestion_entry(entry):
                raise RuntimeError(f"Failed to queue missing paper candidate: {queue_id}")
            queued.append(queue_id)
        return {"queued": queued, "skipped": skipped, "report": report}

    def paper_context(self, artifact_or_paper_id: PaperArtifact | str) -> dict[str, Any]:
        """Return graph context for wiki/API consumers."""
        paper_id = (
            paper_record_from_artifact(artifact_or_paper_id, collected=True).paper_id
            if isinstance(artifact_or_paper_id, PaperArtifact)
            else str(artifact_or_paper_id)
        )
        return self.db.get_research_paper_context(paper_id)

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
        raw_payload=artifact.canonical_record(),
        updated_at=artifact.ingested_at or datetime.now().isoformat(),
    )


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
    title: str | None = None,
) -> str:
    normalized_doi = normalize_doi(doi)
    if normalized_doi:
        return f"doi:{normalized_doi}"
    normalized_arxiv = normalize_arxiv_id(arxiv_id or artifact_id)
    if normalized_arxiv:
        return f"arxiv:{normalized_arxiv}"
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


def _paper_reference_from_payload(payload: Any) -> PaperReference | None:
    raw_payload: dict[str, Any]
    if isinstance(payload, Mapping):
        raw_payload = dict(payload)
        title = _first_string(raw_payload, "title", "paper_title", "name")
        doi = normalize_doi(_first_string(raw_payload, "doi", "DOI"))
        arxiv_id = normalize_arxiv_id(
            _first_string(raw_payload, "arxiv_id", "arxiv", "arxivId", "paper_id", "id")
        )
        pdf_url = _first_string(raw_payload, "pdf_url", "pdfUrl", "url", "href")
        if pdf_url and not arxiv_id:
            arxiv_id = normalize_arxiv_id(pdf_url)
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
