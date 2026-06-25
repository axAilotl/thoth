import json
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

import thoth_api
from core.artifacts import PaperArtifact
from core.config import Config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.research_graph import OpenAlexMetadataProvider, ResearchGraphService
from core.wiki_updater import CompiledWikiUpdater


def _config(tmp_path: Path) -> Config:
    config = Config()
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    return config


def _paper(
    paper_id: str,
    *,
    title: str,
    references=None,
    arxiv_id: str | None = None,
) -> PaperArtifact:
    return PaperArtifact(
        id=paper_id,
        source_type="arxiv",
        raw_content=json.dumps({"id": paper_id, "references": references or []}),
        title=title,
        authors=["Ada Lovelace"],
        abstract=f"Abstract for {title}",
        arxiv_id=arxiv_id or paper_id,
        pdf_url=f"https://arxiv.org/pdf/{arxiv_id or paper_id}.pdf",
        references=references or [],
        source_provider="arxiv",
        ingested_at="2026-04-04T00:00:00",
    )


def test_research_graph_dedupes_edges_and_ranks_missing_candidates(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    service = ResearchGraphService(db)
    shared_reference = {
        "title": "Shared Missing Paper",
        "arxiv_id": "2501.12345",
        "pdf_url": "https://arxiv.org/pdf/2501.12345.pdf",
    }

    service.record_paper_artifact(
        _paper("2401.00001", title="First Paper", references=[shared_reference]),
        queue_missing=False,
    )
    service.record_paper_artifact(
        _paper("2401.00002", title="Second Paper", references=[shared_reference]),
        queue_missing=False,
    )
    service.record_paper_artifact(
        _paper("2401.00001", title="First Paper", references=[shared_reference]),
        queue_missing=False,
    )

    edges = db.list_research_paper_edges(edge_type="references")
    assert len(edges) == 2

    report = service.missing_papers_report(min_references=2)
    assert [item["paper_id"] for item in report["high_confidence"]] == [
        "arxiv:2501.12345"
    ]
    assert report["high_confidence"][0]["referenced_by_count"] == 2
    assert report["ambiguous"] == []

    queued = service.queue_high_confidence_missing_papers(min_references=2)
    assert queued["queued"] == ["research_graph:arxiv:2501.12345"]
    entry = db.get_ingestion_entry("research_graph:arxiv:2501.12345")
    assert entry is not None
    assert entry.artifact_type == "paper"
    assert entry.source == "research_graph"
    assert entry.priority == 2


def test_research_graph_reports_ambiguous_candidates_separately(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    service = ResearchGraphService(db)
    ambiguous_reference = "A missing paper without stable identifier"

    service.record_paper_artifact(
        _paper("2401.00003", title="Third Paper", references=[ambiguous_reference]),
        queue_missing=False,
    )
    service.record_paper_artifact(
        _paper("2401.00004", title="Fourth Paper", references=[ambiguous_reference]),
        queue_missing=False,
    )

    report = service.missing_papers_report(min_references=2)
    assert report["high_confidence"] == []
    assert report["ambiguous"][0]["paper_id"] == (
        "title:a-missing-paper-without-stable-identifier"
    )
    assert report["ambiguous"][0]["queueable"] is False


def test_research_graph_extracts_reference_edges_from_local_pdf_text(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    service = ResearchGraphService(db)
    pdf_path = tmp_path / "paper.pdf"
    pdf_path.write_text(
        "Body\n\nReferences\n[1] Example Paper. doi:10.5555/example\n",
        encoding="utf-8",
    )

    result = service.record_paper_artifact(
        _paper("2401.00008", title="PDF Source"),
        pdf_paths=[pdf_path],
        queue_missing=False,
    )

    assert result["references"] == 1
    edges = db.list_research_paper_edges(edge_type="references")
    assert edges[0].target_paper_id == "doi:10.5555/example"
    assert "PDF references" in edges[0].source_evidence


class FakeOpenAlexResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeOpenAlexSession:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        if url.endswith("/works/doi:10.1234%2Fsource"):
            return FakeOpenAlexResponse(
                {
                    "id": "https://openalex.org/Wsource",
                    "referenced_works": ["https://openalex.org/Wref"],
                }
            )
        if url.endswith("/works/https:%2F%2Fopenalex.org%2FWref"):
            return FakeOpenAlexResponse(
                {
                    "id": "https://openalex.org/Wref",
                    "display_name": "OpenAlex Referenced Work",
                    "doi": "https://doi.org/10.5678/reference",
                    "publication_date": "2024-01-01",
                    "authorships": [
                        {"author": {"display_name": "Grace Hopper"}},
                    ],
                    "primary_location": {
                        "source": {"display_name": "Journal of Fixtures"},
                        "pdf_url": "https://example.test/reference.pdf",
                    },
                }
            )
        raise AssertionError(f"unexpected OpenAlex URL: {url}")


def test_research_graph_uses_openalex_metadata_provider_for_references(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    session = FakeOpenAlexSession()
    provider = OpenAlexMetadataProvider(
        session=session,
        timeout_seconds=3,
        max_references=5,
        mailto="ada@example.test",
    )
    service = ResearchGraphService(db, metadata_provider=provider)
    paper = _paper("paper-with-doi", title="Source With DOI", arxiv_id="2401.00009")
    paper.doi = "10.1234/source"

    result = service.record_paper_artifact(paper, queue_missing=False)

    assert result["references"] == 1
    assert session.calls[0][1] == {"mailto": "ada@example.test"}
    assert session.calls[0][2] == 3
    context = service.paper_context("doi:10.1234/source")
    assert context["references"][0]["paper_id"] == "doi:10.5678/reference"
    assert context["references"][0]["title"] == "OpenAlex Referenced Work"
    referenced_record = db.get_research_paper("doi:10.5678/reference")
    assert referenced_record is not None
    assert referenced_record.venue == "Journal of Fixtures"


def test_paper_wiki_page_includes_research_context(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    db = MetadataDB(str(layout.database_path))
    service = ResearchGraphService(db)

    target = _paper("2501.12345", title="Shared Missing Paper")
    source = _paper(
        "2401.00005",
        title="Local Paper",
        references=[
            {
                "title": target.title,
                "arxiv_id": target.arxiv_id,
                "pdf_url": target.pdf_url,
            }
        ],
    )
    service.record_paper_artifact(source, queue_missing=False)
    service.record_paper_artifact(target, queue_missing=False)

    updater = CompiledWikiUpdater(config, layout=layout, db=db)
    result = updater.update_from_artifact(target)
    content = result.page_path.read_text(encoding="utf-8")

    assert "## Research Context" in content
    assert "`1` local paper(s) reference this work" in content
    assert "Local Paper" in content


def test_research_missing_papers_cli_smoke_returns_json():
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [
            sys.executable,
            "thoth.py",
            "research",
            "missing-papers",
            "--json",
            "--limit",
            "1",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert set(payload) >= {"high_confidence", "ambiguous", "min_references"}


def test_research_missing_papers_api_returns_report(monkeypatch, tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    service = ResearchGraphService(db)
    service.record_paper_artifact(
        _paper(
            "2401.00006",
            title="API Paper One",
            references=[{"title": "API Missing", "doi": "10.1234/example"}],
        ),
        queue_missing=False,
    )
    service.record_paper_artifact(
        _paper(
            "2401.00007",
            title="API Paper Two",
            references=[{"title": "API Missing", "doi": "10.1234/example"}],
        ),
        queue_missing=False,
    )

    async def noop_async(*args, **kwargs):
        return None

    monkeypatch.setattr(thoth_api, "get_metadata_db", lambda: db)
    monkeypatch.setattr(thoth_api, "background_processor", noop_async)
    monkeypatch.setattr(thoth_api, "ingestion_worker", noop_async)
    monkeypatch.setattr(thoth_api, "social_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "x_api_sync_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "archivist_scheduler", noop_async)
    monkeypatch.setattr(thoth_api, "load_pending_bookmarks_from_db", noop_async)
    monkeypatch.setattr(thoth_api, "ensure_wiki_scaffold", lambda *args, **kwargs: None)
    monkeypatch.setattr(thoth_api, "resolve_x_api_sync_config", lambda: None)

    with TestClient(thoth_api.app) as client:
        response = client.get("/api/research/missing-papers?min_references=2")

    assert response.status_code == 200
    payload = response.json()
    assert payload["high_confidence"][0]["paper_id"] == "doi:10.1234/example"
