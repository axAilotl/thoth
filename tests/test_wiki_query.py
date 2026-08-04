import json
from copy import deepcopy
from pathlib import Path

import core.wiki_io as wiki_io
from core.capture_event_store import (
    ArtifactLink,
    CaptureEvent,
    CaptureEventStore,
    CaptureSource,
    RawArtifactRef,
    SecurityFinding,
)
from core.config import config
from core.hybrid_search import HybridSearchFilters
from core.metadata_db import IngestionQueueEntry, MetadataDB
from core.path_layout import build_path_layout
from core.wiki_contract import WikiPageSpec, build_wiki_contract
from core.wiki_io import atomic_write_text, clear_wiki_document_cache, render_frontmatter
from core.wiki_query import WikiQueryRunner

from test_capture_event_store import FakeCaptureConnection


def make_config(tmp_path: Path):
    original = deepcopy(config.data)
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    return original


def _write_page(contract, spec, body: str) -> Path:
    page_path = contract.page_path_for(spec)
    content = render_frontmatter(contract.frontmatter_for(spec)).rstrip() + "\n\n" + body
    atomic_write_text(page_path, content)
    return page_path


class CountingCaptureConnection(FakeCaptureConnection):
    def __init__(self):
        super().__init__()
        self.select_counts = {
            "capture_sources": 0,
            "capture_events": 0,
            "raw_artifact_refs": 0,
            "artifact_links": 0,
            "security_findings": 0,
        }

    def execute(self, sql, params=None):
        normalized = sql.lower()
        if " from " in f" {normalized} ":
            for table_name in self.select_counts:
                if table_name in normalized:
                    self.select_counts[table_name] += 1
                    break
        return super().execute(sql, params)


def test_wiki_query_searches_and_writes_back_curated_pages(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)

        _write_page(
            contract,
            WikiPageSpec(
                title="Owner Repo",
                slug="repo-owner-repo",
                kind="entity",
                summary="Repository summary",
                source_paths=("stars/owner_repo_summary.md",),
                influence_sources=(
                    {
                        "label": "S1",
                        "source_path": "stars/owner_repo_summary.md",
                        "source_type": "repository",
                    },
                ),
                related_slugs=("owner-repo",),
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Owner Repo\n\nThis page discusses agentic workflows and retrieval.",
        )
        _write_page(
            contract,
            WikiPageSpec(
                title="Unrelated Note",
                slug="unrelated-note",
                kind="topic",
                summary="A disconnected note",
                source_paths=("notes/unrelated.md",),
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Unrelated Note\n\nNo query term here.",
        )
        _write_page(
            contract,
            WikiPageSpec(
                title="Quarantined Agentic Workflow",
                slug="quarantined-agentic-workflow",
                kind="topic",
                summary="Agentic workflows that require review",
                source_paths=("notes/quarantined.md",),
                updated_at="2026-04-04T00:00:00Z",
                security_policy={
                    "status": "needs_review",
                    "reason": "high_risk_finding",
                },
            ),
            "# Quarantined\n\nThis page discusses agentic workflows.",
        )

        runner = WikiQueryRunner(config, layout=layout, contract=contract)
        result = runner.search("agentic workflows", limit=5)

        assert len(result.hits) == 1
        assert result.hits[0].slug == "repo-owner-repo"
        assert result.hits[0].influence_sources[0]["source_path"] == "stars/owner_repo_summary.md"
        assert "body" in result.hits[0].matched_fields or "phrase" in result.hits[0].matched_fields
        review_result = runner.search(
            "agentic workflows",
            limit=5,
            include_quarantined=True,
        )
        assert {hit.slug for hit in review_result.hits} == {
            "repo-owner-repo",
            "quarantined-agentic-workflow",
        }

        write_back = runner.curated_write_back(
            "agentic workflows",
            limit=5,
            selected_slugs=["repo-owner-repo"],
            curated_notes="Curated answer for the wiki loop.",
        )

        query_page = layout.wiki_root / "pages" / "query-agentic-workflows.md"
        index_path = layout.wiki_root / "index.md"

        assert write_back.page_path == query_page
        assert query_page.exists()
        page_content = query_page.read_text(encoding="utf-8")
        assert "thoth_type: wiki_query" in page_content
        assert "Curated answer for the wiki loop." in page_content
        assert "repo-owner-repo" in page_content
        assert "Influence Sources: `stars/owner_repo_summary.md`" in page_content
        assert index_path.read_text(encoding="utf-8").count("query-agentic-workflows.md") == 1
    finally:
        config.data = original


def test_hybrid_wiki_search_reuses_parsed_pages_between_queries(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    clear_wiki_document_cache()
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)

        _write_page(
            contract,
            WikiPageSpec(
                title="Agentic Retrieval",
                slug="agentic-retrieval",
                kind="topic",
                summary="Agentic workflow retrieval",
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Agentic Retrieval\n\nAgentic local search.",
        )
        _write_page(
            contract,
            WikiPageSpec(
                title="Unrelated",
                slug="unrelated",
                kind="topic",
                summary="No matching term",
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Unrelated\n\nDifferent content.",
        )

        original_read_document = wiki_io.read_document
        read_paths = []

        def counted_read_document(path: Path):
            read_paths.append(Path(path).name)
            return original_read_document(path)

        monkeypatch.setattr(wiki_io, "read_document", counted_read_document)
        runner = WikiQueryRunner(config, layout=layout, contract=contract)

        first = runner.hybrid_search(
            "agentic",
            limit=5,
            filters=HybridSearchFilters(result_types=("wiki_page",)),
        )
        first_read_paths = tuple(read_paths)
        read_paths.clear()
        second = runner.hybrid_search(
            "agentic",
            limit=5,
            filters=HybridSearchFilters(result_types=("wiki_page",)),
        )

        assert [hit.slug for hit in first.hits] == ["agentic-retrieval"]
        assert [hit.slug for hit in second.hits] == ["agentic-retrieval"]
        assert sorted(first_read_paths) == ["agentic-retrieval.md", "unrelated.md"]
        assert read_paths == []
    finally:
        clear_wiki_document_cache()
        config.data = original


def test_hybrid_capture_search_batches_event_side_tables(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        layout.ensure_directories()
        contract = build_wiki_contract(config, project_root=tmp_path)
        conn = CountingCaptureConnection()
        store = CaptureEventStore(
            conn,
            schema="capture_unit",
            raw_roots=[layout.raw_root],
        )
        source = store.upsert_source(
            CaptureSource(
                source_id="source-search",
                source_name="search-source",
                source_type="manual",
            )
        )
        for index in range(2):
            event = store.upsert_event(
                CaptureEvent(
                    event_id=f"event-search-{index}",
                    source_id=source.source_id,
                    event_type="note",
                    payload={"title": f"Agentic capture note {index}"},
                )
            )
            raw_path = layout.raw_root / f"search-{index}.json"
            raw_path.write_text('{"title":"agentic"}\n', encoding="utf-8")
            raw_ref = store.upsert_raw_ref(
                RawArtifactRef.from_file(
                    raw_path,
                    source_id=source.source_id,
                    event_id=event.event_id,
                    raw_roots=[layout.raw_root],
                )
            )
            store.upsert_artifact_link(
                ArtifactLink(
                    artifact_link_id=f"link-search-{index}",
                    event_id=event.event_id,
                    raw_ref_id=raw_ref.raw_ref_id,
                    artifact_id=f"artifact-search-{index}",
                    artifact_type="note",
                )
            )
            store.upsert_security_finding(
                SecurityFinding(
                    finding_id=f"finding-search-{index}",
                    event_id=event.event_id,
                    raw_ref_id=raw_ref.raw_ref_id,
                    finding_type="prompt_security",
                    severity="low",
                    status="closed",
                    fingerprint=f"finding-search-{index}",
                )
            )

        for key in conn.select_counts:
            conn.select_counts[key] = 0

        runner = WikiQueryRunner(
            config,
            layout=layout,
            contract=contract,
            event_store=store,
        )
        result = runner.hybrid_search(
            "agentic",
            limit=10,
            filters=HybridSearchFilters(result_types=("capture_event",)),
        )

        assert {hit.event_id for hit in result.hits} == {
            "event-search-0",
            "event-search-1",
        }
        assert conn.select_counts == {
            "capture_sources": 1,
            "capture_events": 1,
            "raw_artifact_refs": 1,
            "artifact_links": 1,
            "security_findings": 1,
        }
    finally:
        config.data = original


def test_hybrid_search_filters_artifacts_and_excludes_quarantined_by_default(
    tmp_path: Path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)
        db = MetadataDB(str(layout.database_path))

        _write_page(
            contract,
            WikiPageSpec(
                title="Hybrid Retrieval",
                slug="hybrid-retrieval",
                kind="topic",
                summary="Agentic workflow retrieval with wiki provenance",
                source_paths=("pages/hybrid.md",),
                source_type="wiki",
                source_ids=("wiki-source",),
                updated_at="2026-04-04T00:00:00Z",
            ),
            "# Hybrid Retrieval\n\nAgentic workflows use lexical local search.",
        )
        db.upsert_ingestion_entry(
            IngestionQueueEntry(
                artifact_id="artifact-safe",
                artifact_type="repository",
                source="github",
                status="pending",
                payload_json=(
                    '{"id":"repo-safe","source_type":"github",'
                    '"repo_name":"Agentic Retrieval Repo",'
                    '"description":"Agentic workflows with lexical filters",'
                    '"tags":["retrieval"]}'
                ),
                created_at="2026-04-05T00:00:00",
            )
        )
        db.upsert_ingestion_entry(
            IngestionQueueEntry(
                artifact_id="artifact-blocked",
                artifact_type="repository",
                source="github",
                status="blocked",
                payload_json=(
                    '{"id":"repo-blocked","source_type":"github",'
                    '"repo_name":"Blocked Agentic Retrieval",'
                    '"description":"Agentic workflows with blocked content",'
                    '"tags":["retrieval"]}'
                ),
                created_at="2026-04-06T00:00:00",
            )
        )
        db.upsert_ingestion_entry(
            IngestionQueueEntry(
                artifact_id="artifact-zero-trust",
                artifact_type="repository",
                source="github",
                status="pending",
                payload_json=(
                    '{"id":"repo-zero-trust","source_type":"github",'
                    '"repo_name":"Zero Trust Agentic Retrieval",'
                    '"description":"Agentic workflows with explicit zero trust",'
                    '"tags":["untrusted"],'
                    '"source_trust_score":0,'
                    '"trust_score":1.0}'
                ),
                created_at="2026-04-05T12:00:00",
            )
        )

        runner = WikiQueryRunner(config, layout=layout, contract=contract, db=db)
        result = runner.hybrid_search(
            "agentic workflows",
            limit=10,
            filters=HybridSearchFilters(
                result_types=("wiki_page", "artifact"),
                source_types=("github", "wiki"),
            ),
        )

        ids = {hit.result_id for hit in result.hits}
        assert "wiki_page:hybrid-retrieval" in ids
        assert "artifact:artifact-safe" in ids
        assert "artifact:artifact-zero-trust" in ids
        assert "artifact:artifact-blocked" not in ids
        assert result.capabilities["embedding"]["available"] is False
        assert all(hit.provenance for hit in result.hits)
        assert all(hit.security["status"] == "allowed" for hit in result.hits)
        assert all("score" in hit.trust for hit in result.hits)
        zero_trust = next(
            hit
            for hit in result.hits
            if hit.result_id == "artifact:artifact-zero-trust"
        )
        assert zero_trust.trust["score"] == 0.0

        artifact_only = runner.hybrid_search(
            "agentic workflows",
            limit=10,
            filters=HybridSearchFilters(
                result_types=("artifact",),
                tags=("retrieval",),
                time_after="2026-04-04T12:00:00Z",
            ),
        )
        assert [hit.result_id for hit in artifact_only.hits] == ["artifact:artifact-safe"]

        review_result = runner.hybrid_search(
            "agentic workflows",
            limit=10,
            filters=HybridSearchFilters(
                result_types=("artifact",),
                include_quarantined=True,
            ),
        )
        assert {hit.result_id for hit in review_result.hits} == {
            "artifact:artifact-safe",
            "artifact:artifact-blocked",
            "artifact:artifact-zero-trust",
        }
        blocked = next(
            hit
            for hit in review_result.hits
            if hit.result_id == "artifact:artifact-blocked"
        )
        assert blocked.security["requires_review"] is True
        assert blocked.trust["score"] == 0.0

        trusted_result = runner.hybrid_search(
            "agentic workflows",
            limit=10,
            filters=HybridSearchFilters(
                result_types=("artifact",),
                include_quarantined=True,
                min_trust_score=0.5,
            ),
        )
        assert [hit.result_id for hit in trusted_result.hits] == ["artifact:artifact-safe"]
    finally:
        config.data = original


def test_hybrid_search_treats_legacy_naive_timestamps_as_utc(
    tmp_path: Path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    original = make_config(tmp_path)
    try:
        layout = build_path_layout(config, project_root=tmp_path)
        contract = build_wiki_contract(config, project_root=tmp_path)
        db = MetadataDB(str(layout.database_path))
        for artifact_id, created_at in (
            ("naive-time", "2026-05-01T12:00:00"),
            ("utc-time", "2026-05-01T12:00:00Z"),
        ):
            db.upsert_ingestion_entry(
                IngestionQueueEntry(
                    artifact_id=artifact_id,
                    artifact_type="repository",
                    source="github",
                    status="pending",
                    payload_json=json.dumps(
                        {
                            "id": artifact_id,
                            "source_type": "github",
                            "repo_name": f"owner/{artifact_id}",
                            "description": "Timezone boundary artifact",
                        }
                    ),
                    created_at=created_at,
                )
            )

        runner = WikiQueryRunner(config, layout=layout, contract=contract, db=db)
        inclusive = runner.hybrid_search(
            "timezone boundary",
            filters=HybridSearchFilters(
                result_types=("artifact",),
                created_before="2026-05-01T12:00:00Z",
            ),
            limit=10,
        )
        exclusive = runner.hybrid_search(
            "timezone boundary",
            filters=HybridSearchFilters(
                result_types=("artifact",),
                created_after="2026-05-01T12:00:01Z",
            ),
            limit=10,
        )

        assert {hit.artifact_id for hit in inclusive.hits} == {
            "naive-time",
            "utc-time",
        }
        assert exclusive.hits == ()
    finally:
        config.data = original
