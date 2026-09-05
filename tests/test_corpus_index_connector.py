import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from collectors.corpus_index_connector import CorpusIndexConnector
from core.archivist_retrieval.models import ArchivistCorpusDocument
from core.archivist_retrieval.semantic import (
    EMBEDDING_METHOD, corpus_embedding_source_hash, ensure_corpus_embeddings,
)
from core.connector_registry import load_connector_registry
from core.connector_runners import ConnectorRunContext, connector_run_handler
from core.corpus_query import query_corpus
from core.path_layout import build_path_layout
from core.metadata_db import FileMetadata, IngestionQueueEntry
from core.archivist_retrieval.inventory import sync_archivist_inventory
from tests.test_archivist_retrieval import FakeEmbeddingLLM, make_config


def setup_corpus(tmp_path):
    config, db = make_config(tmp_path)
    config.set("sources.corpus_index.enabled", True)
    config.set("sources.corpus_index.include_roots", ["papers"])
    config.set("sources.corpus_index.embeddings_enabled", True)
    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()
    (layout.vault_root / "papers").mkdir()
    return config, db, layout


def document(tmp_path, content="Companion memory", **kwargs):
    return ArchivistCorpusDocument(
        candidate_key="vault:papers/a.md", path=tmp_path / "a.md", scope="vault",
        scope_relative_path="papers/a.md", source_type="paper", file_type="markdown",
        title="Research", tags=(), content_text=content, source_hash="first",
        size_bytes=len(content), updated_at="2026-09-04", **kwargs,
    )


def backfill(db, llm, docs, maximum=16):
    return asyncio.run(ensure_corpus_embeddings(
        db=db, llm_interface=llm, documents=docs, max_new_embeddings_per_run=maximum,
    ))


def test_full_text_chunks_cached_and_hash_change_invalidates(tmp_path):
    _config, db, _layout = setup_corpus(tmp_path)
    llm = FakeEmbeddingLLM()
    doc = document(tmp_path, "ordinary words " * 700 + "TAIL memory evidence")
    first = backfill(db, llm, [doc])
    assert first.embedded_count == 1 and first.chunk_inputs >= 2
    assert any("TAIL memory evidence" in text for call in llm.embed_calls for text in call)
    assert all(len(text) <= 6000 for call in llm.embed_calls for text in call)
    count = len(llm.embed_calls)
    second = backfill(db, llm, [doc])
    assert second.reused_count == 1 and len(llm.embed_calls) == count
    changed = replace(doc, source_hash="second", content_text="changed companion memory")
    assert backfill(db, llm, [changed]).embedded_count == 1
    assert corpus_embedding_source_hash(doc) != doc.embedding_source_hash()
    assert second.to_dict()["method"] == EMBEDDING_METHOD


def test_chunk_budget_resumes_documents_and_reports_oversized(tmp_path):
    _config, db, _layout = setup_corpus(tmp_path)
    llm = FakeEmbeddingLLM()
    one = document(tmp_path)
    two = replace(one, candidate_key="vault:papers/b.md")
    huge = replace(one, candidate_key="vault:papers/huge.md", content_text="word " * 3000)
    first = backfill(db, llm, [one, two, huge], maximum=1)
    assert first.embedded_count == 1 and first.pending_count == 2
    assert first.oversized_count == 1 and first.chunk_inputs == 1
    second = backfill(db, llm, [one, two, huge], maximum=1)
    assert second.reused_count == 1 and second.embedded_count == 1
    assert second.pending_count == 1


def test_security_checks_entire_text_and_empty_pdf_not_covered(tmp_path):
    _config, db, _layout = setup_corpus(tmp_path)
    llm = FakeEmbeddingLLM()
    restricted = document(tmp_path, privacy_class="restricted")
    secret = replace(restricted, candidate_key="vault:papers/secret.md", privacy_class="unspecified",
                     content_text="benign " * 1200 + "\nOPENAI_API_KEY=sk-1234567890abcdefghijklmnopqrstuvwxyz1234567890")
    empty = replace(restricted, candidate_key="vault:papers/empty.pdf", privacy_class="unspecified", file_type="pdf", content_text="")
    result = backfill(db, llm, [restricted, secret, empty])
    assert result.blocked_count == 2 and result.empty_count == 1
    assert result.embedded_count == 0 and not llm.embed_calls


def test_provider_error_does_not_mark_partial_document_covered(tmp_path):
    _config, db, _layout = setup_corpus(tmp_path)
    class Broken(FakeEmbeddingLLM):
        async def embed_texts(self, *args, **kwargs):
            return SimpleNamespace(error=None, vectors=[[float("nan")]])
    with pytest.raises(ValueError, match="invalid vector"):
        backfill(db, Broken(), [document(tmp_path)])
    assert not db.list_archivist_corpus_embeddings_for_candidate_keys(("vault:papers/a.md",))


def test_connector_registered_incremental_and_keyword_tail_search(tmp_path, monkeypatch):
    config, db, layout = setup_corpus(tmp_path)
    config.set("sources.corpus_index.embeddings_enabled", False)
    note = layout.vault_root / "papers" / "notes.md"
    note.write_text("# Research\n" + "ordinary material " * 600 + "\nplatypus tail evidence")
    manifest = load_connector_registry(config).get("corpus_index")
    handler = connector_run_handler(manifest, ConnectorRunContext(config, layout, db))
    first = handler({})
    assert first["indexed_count"] == 1 and first["keyword_content_count"] == 1
    assert handler({})["reused_count"] == 1
    def forbid_full_inventory(**kwargs):
        raise AssertionError("Keyword query must use FTS candidates, not hydrate the whole corpus")
    monkeypatch.setattr(db, 'list_archivist_corpus_documents', forbid_full_inventory)
    class NoProvider:
        def resolve_task_route(self, task):
            raise AssertionError("Keyword search must not call an embedding provider")
    response = asyncio.run(query_corpus(
        config=config, layout=layout, db=db, query="platypus", llm_interface=NoProvider(),
    ))
    assert len(response["results"]) == 1
    assert response["results"][0]["path"] == str(note)
    assert response["results"][0]["provenance"]["source_type"]


def test_hybrid_read_only_query_uses_cached_embeddings(tmp_path):
    config, db, layout = setup_corpus(tmp_path)
    (layout.vault_root / "papers" / "notes.md").write_text("# Research\nCompanion memory evidence")
    llm = FakeEmbeddingLLM()
    result = asyncio.run(CorpusIndexConnector(config, layout=layout, db=db, llm_interface=llm).collect())
    assert result["embeddings"]["covered_count"] == 1
    llm.embed_calls.clear()
    response = asyncio.run(query_corpus(
        config=config, layout=layout, db=db, query="companion memory", mode="hybrid", llm_interface=llm,
    ))
    assert llm.embed_calls == [["companion memory"]]
    assert set(response["results"][0]["retrieval_sources"]) == {"keyword", "semantic"}
    assert response["document_backfill_performed"] is False


def test_public_author_contact_does_not_hide_offline_keyword_evidence(tmp_path):
    config, db, layout = setup_corpus(tmp_path)
    config.set('sources.corpus_index.embeddings_enabled', False)
    (layout.vault_root / 'papers' / 'paper.md').write_text(
        '# Research evidence\nAuthor contact: researcher@university.edu\nA study of platypus memory.'
    )
    asyncio.run(CorpusIndexConnector(config, layout=layout, db=db).collect())
    result = asyncio.run(query_corpus(config=config, layout=layout, db=db, query='platypus'))
    assert len(result['results']) == 1
    config.set('sources.corpus_index.embeddings_enabled', True)
    llm = FakeEmbeddingLLM()
    run = asyncio.run(CorpusIndexConnector(config, layout=layout, db=db, llm_interface=llm).collect())
    assert run['embeddings']['blocked_count'] == 1
    assert llm.embed_calls == []


@pytest.mark.parametrize('folders', [('clippings', 'Clippings'), ('clip_%', 'clip_other')])
def test_inventory_roots_remain_literal_and_case_sensitive(tmp_path, folders):
    config, db, layout = setup_corpus(tmp_path)
    config.set('sources.corpus_index.embeddings_enabled', False)
    config.set('sources.corpus_index.include_roots', list(folders))
    for folder in folders:
        root = layout.vault_root / folder
        root.mkdir(exist_ok=True)
        (root / 'note.md').write_text('---\ntitle: Evidence\n---\nCompanion evidence.')
    asyncio.run(CorpusIndexConnector(config, layout=layout, db=db).collect())
    assert len(db.list_archivist_corpus_documents()) == 2
    filtered = db.list_archivist_corpus_documents(root_filters=(('vault', folders[0]),))
    assert [doc.scope_relative_path for doc in filtered] == [f'{folders[0]}/note.md']
    hits = db.search_archivist_corpus_full_text(query='evidence', root_filters=(('vault', folders[0]),))
    assert len(hits) == 1
    assert db.prune_archivist_corpus_documents(scope='vault', relative_prefix=folders[0], keep_candidate_keys=()) == 1
    assert db.list_archivist_corpus_documents()[0].scope_relative_path == f'{folders[1]}/note.md'


@pytest.mark.parametrize("root", ["../private", "/tmp", "vault/../../private"])
def test_root_escape_fails_before_inventory(tmp_path, root):
    config, db, layout = setup_corpus(tmp_path)
    config.set("sources.corpus_index.include_roots", [root])
    with pytest.raises(ValueError, match="Invalid corpus root"):
        asyncio.run(CorpusIndexConnector(config, layout=layout, db=db).collect())


def test_symlink_refused_before_inventory(tmp_path):
    config, db, layout = setup_corpus(tmp_path)
    (layout.vault_root / "papers" / "secret.md").symlink_to(tmp_path / "private.md")
    with pytest.raises(ValueError, match="symlinks"):
        asyncio.run(CorpusIndexConnector(config, layout=layout, db=db).collect())


def test_empty_pdf_inventory_retries_extraction(tmp_path, monkeypatch):
    config, db, layout = setup_corpus(tmp_path)
    config.set("sources.corpus_index.embeddings_enabled", False)
    (layout.vault_root / "papers" / "scan.pdf").write_bytes(b"%PDF-1.4")
    monkeypatch.setattr("core.archivist_retrieval.inventory.extract_pdf_title", lambda path: "Scan")
    monkeypatch.setattr("core.archivist_retrieval.inventory.extract_pdf_text", lambda path: "")
    connector = CorpusIndexConnector(config, layout=layout, db=db)
    first = asyncio.run(connector.collect())
    assert first["empty_pdf_paths"] == ["papers/scan.pdf"]
    assert first["keyword_content_count"] == 0
    monkeypatch.setattr("core.archivist_retrieval.inventory.extract_pdf_text", lambda path: "Recovered text")
    second = asyncio.run(connector.collect())
    assert second["indexed_count"] == 1 and second["keyword_content_count"] == 1
    assert not second["empty_pdf_paths"]


def test_api_corpus_route_reaches_index_and_rejects_bad_mode(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    import thoth_api
    config, db, layout = setup_corpus(tmp_path)
    config.set("sources.corpus_index.embeddings_enabled", False)
    (layout.vault_root / "papers" / "note.md").write_text("# Evidence\nplatypus research")
    asyncio.run(CorpusIndexConnector(config, layout=layout, db=db).collect())
    monkeypatch.setattr(thoth_api, "get_knowledge_artifact_runtime", lambda: SimpleNamespace(
        config=config, db=db, layout=layout,
    ))
    client = TestClient(thoth_api.app)
    response = client.get("/api/query/corpus", params={"query": "platypus"})
    assert response.status_code == 200
    assert response.json()["results"][0]["relative_path"] == "papers/note.md"
    assert client.get("/api/query/corpus", params={"query": "platypus", "mode": "bad"}).status_code == 400


def queue_backed_pdf(tmp_path, monkeypatch):
    config, db, layout = setup_corpus(tmp_path)
    path = layout.vault_root / "papers" / "paper.pdf"
    path.write_bytes(b"%PDF-1.4")
    monkeypatch.setattr("core.archivist_retrieval.inventory.extract_pdf_title", lambda path: "Research")
    monkeypatch.setattr("core.archivist_retrieval.inventory.extract_pdf_text", lambda path: "Companion memory evidence")
    db.upsert_file(FileMetadata(str(path), "attachment", path.stat().st_size, source_id="papers/paper.pdf"))
    identity = "webclip:papers/paper.pdf"
    db.upsert_ingestion_entry(IngestionQueueEntry(
        identity, "web_clipper", "web_clipper", json.dumps({
            "id": identity,
            "raw_content": "Companion memory evidence",
            "custom_metadata": {"event_id": "event-capture-123", "privacy_class": "public"},
        }), status="processed",
    ))
    return config, db, layout, identity


def test_webclip_file_identity_resolves_canonical_provenance(tmp_path, monkeypatch):
    config, db, layout, identity = queue_backed_pdf(tmp_path, monkeypatch)
    inventory = sync_archivist_inventory(("papers",), exclude_root_specs=(), config=config, layout=layout, db=db)
    doc, = inventory.documents
    assert doc.source_id == identity and doc.artifact_id == identity
    assert doc.event_id == "event-capture-123" and doc.privacy_class == "public"


@pytest.mark.parametrize("change", ["quarantine", "privacy"])
def test_webclip_queue_changes_block_cached_embeddings_queries_and_reuse(tmp_path, monkeypatch, change):
    config, db, layout, identity = queue_backed_pdf(tmp_path, monkeypatch)
    llm = FakeEmbeddingLLM()
    asyncio.run(CorpusIndexConnector(config, layout=layout, db=db, llm_interface=llm).collect())
    cached = db.list_archivist_corpus_documents()
    assert len(cached) == 1
    if change == "quarantine":
        db.mark_ingestion_review_required(identity, reason="operator_hold", category="security")
    else:
        entry = db.get_ingestion_entry(identity)
        payload = json.loads(entry.payload_json)
        payload["custom_metadata"]["privacy_class"] = "restricted"
        db.update_ingestion_payload_json(identity, json.dumps(payload))
    llm.embed_calls.clear()
    result = backfill(db, llm, cached)
    assert result.blocked_count == 1 and result.to_dict()["covered_count"] == 0
    for mode in ("keyword", "hybrid"):
        response = asyncio.run(query_corpus(
            config=config, layout=layout, db=db, query="companion", mode=mode, llm_interface=llm,
        ))
        assert response["results"] == []
    assert llm.embed_calls == []
    inventory = sync_archivist_inventory(("papers",), exclude_root_specs=(), config=config, layout=layout, db=db)
    if change == "quarantine":
        assert inventory.documents == ()
    else:
        assert inventory.documents[0].privacy_class == "restricted"


def test_preindex_webclip_quarantine_excludes_pdf(tmp_path, monkeypatch):
    config, db, layout, identity = queue_backed_pdf(tmp_path, monkeypatch)
    db.mark_ingestion_review_required(identity, reason="operator_hold", category="security")
    result = sync_archivist_inventory(("papers",), exclude_root_specs=(), config=config, layout=layout, db=db)
    assert result.documents == ()


def test_excluded_tree_never_preflighted_read_or_stored_even_on_later_failure(tmp_path, monkeypatch):
    from pathlib import Path
    config, db, layout = setup_corpus(tmp_path)
    config.set("sources.corpus_index.embeddings_enabled", False)
    config.set("sources.corpus_index.exclude_roots", ["papers/private"])
    root = layout.vault_root / "papers"
    private = root / "private"
    private.mkdir()
    secret = private / "secret.md"
    secret.write_text("Confidential evidence")
    (private / "external.md").symlink_to(tmp_path / "not-in-scope")
    (root / "good.md").write_text("Public source")
    (root / "z-broken.md").write_bytes(b"\xff")
    original = Path.read_text
    def guarded_read(path, *args, **kwargs):
        assert not path.is_relative_to(private), "Excluded content was read"
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", guarded_read)
    with pytest.raises(UnicodeDecodeError):
        asyncio.run(CorpusIndexConnector(config, layout=layout, db=db).collect())
    assert db.get_archivist_corpus_document("vault:papers/private/secret.md") is None
