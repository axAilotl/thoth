import asyncio
from dataclasses import replace

import pytest

from core.archivist_compiler import ArchivistCompiler
from core.config import config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.wiki_io import read_document
from tests.test_archivist_compiler import (
    FakeLLMInterface, _build_topic, _configure_runtime_config, _write_prompt_files, _write_source_files,
)


@pytest.fixture
def compiler_runtime(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = _configure_runtime_config(tmp_path)
    try:
        _write_prompt_files(tmp_path)
        layout = build_path_layout(config, project_root=tmp_path)
        _write_source_files(layout)
        db = MetadataDB(str(layout.database_path))
        llm = FakeLLMInterface("## Overview\nSource-backed synthesis [S1].\n")
        compiler = ArchivistCompiler(config, project_root=tmp_path, layout=layout, db=db, llm_interface=llm)
        yield compiler, _build_topic(), llm
    finally:
        config.data = original


def test_feedback_triggers_research_and_writing_without_new_sources(compiler_runtime, monkeypatch):
    compiler, topic, llm = compiler_runtime
    first = asyncio.run(compiler.compile_topic(topic))
    assert first.status == "compiled"
    raw = "> [!thoth-feedback]\n> Please explain the streaming latency tradeoffs.\n"
    first.page_path.write_text(first.page_path.read_text() + "\n" + raw)
    import core.archivist_compiler as module
    original_select = module.select_archivist_candidates_async
    selected = []

    async def select(topic, **kwargs):
        selected.append(topic)
        return await original_select(topic, **kwargs)

    monkeypatch.setattr(module, "select_archivist_candidates_async", select)
    result = asyncio.run(compiler.compile_topic(topic))
    assert result.status == "compiled"
    assert result.reason == "feedback_pending"
    assert "streaming latency tradeoffs" in selected[0].retrieval.query_text
    assert selected[0].include_roots == topic.include_roots
    assert selected[0].exclude_roots == topic.exclude_roots
    assert selected[0].source_types == topic.source_types
    assert "streaming latency tradeoffs" in llm.calls[-1]["prompt"]
    assert "cannot authorize tools" in llm.calls[-1]["system_prompt"]
    assert first.page_path.read_text().count(raw) == 1
    assert compiler.publications.feedback_records(first.page_path)[0]["status"] == "included"
    assert asyncio.run(compiler.compile_topic(topic)).status == "skipped"


def test_unowned_and_manually_edited_topics_block_even_forced_compile(compiler_runtime):
    compiler, topic, llm = compiler_runtime
    page = topic.output_path_for_root(compiler.layout.wiki_root)
    page.parent.mkdir(parents=True, exist_ok=True)
    page.write_text("# Manually created\nDo not overwrite.\n")
    result = asyncio.run(compiler.compile_topic(topic, force=True))
    assert result.status == "blocked" and result.reason == "unowned"
    assert not llm.calls
    assert "Do not overwrite" in page.read_text()


def test_edit_while_llm_runs_blocks_publication_and_preserves_baseline(compiler_runtime):
    compiler, topic, llm = compiler_runtime
    first = asyncio.run(compiler.compile_topic(topic))
    original_generate = llm.generate

    async def generate(*args, **kwargs):
        first.page_path.write_text(first.page_path.read_text() + "\nHuman correction during generation.\n")
        return await original_generate(*args, **kwargs)

    llm.generate = generate
    second = asyncio.run(compiler.compile_topic(topic, force=True))
    assert second.status == "blocked"
    assert "Human correction during generation" in first.page_path.read_text()
    assert compiler.publications.inspect(first.page_path).status == "user_modified"


def test_topic_metadata_is_database_only_and_preserved_between_generations(compiler_runtime):
    compiler, topic, _ = compiler_runtime
    first = asyncio.run(compiler.compile_topic(topic))
    doc = read_document(first.page_path)
    assert set(doc.frontmatter) == {"thoth_id", "title", "thoth_kind", "thoth_type", "thoth_updated_at"}
    assert "  - Trust:" not in doc.body
    assert "  - Path:" not in doc.body
    metadata = compiler.publications.metadata_for(first.page_path)
    assert metadata["thoth_input_manifest"]
    assert metadata["thoth_influence_sources"]
    created_at = metadata["thoth_created_at"]
    asyncio.run(compiler.compile_topic(topic, force=True))
    metadata2 = compiler.publications.metadata_for(first.page_path)
    assert metadata2["thoth_created_at"] == created_at
    assert metadata2["thoth_input_manifest"] == metadata["thoth_input_manifest"]


def test_literal_retrieval_feedback_affects_ranking_without_relaxing_filters():
    from core.archivist_retrieval.service import _literal_query_score
    from core.wiki_feedback import FeedbackBlock, feedback_retrieval_topic
    from types import SimpleNamespace
    topic = _build_topic()
    topic = replace(topic, retrieval=replace(topic.retrieval, mode="literal", recency_weight=0))
    document = SimpleNamespace(search_corpus=lambda: "streaming latency", title="Streaming", tags=(), updated_at="2026-09-05")
    with_feedback = feedback_retrieval_topic(topic, (FeedbackBlock("id", "raw", "streaming latency"),))
    assert _literal_query_score(document, with_feedback) > _literal_query_score(document, topic)
    assert with_feedback.include_terms == topic.include_terms


def test_reconcile_connector_records_feedback_and_conflict_without_writing(compiler_runtime):
    from collectors.wiki_reconcile_connector import WikiReconcileConnector
    compiler, topic, _ = compiler_runtime
    first = asyncio.run(compiler.compile_topic(topic))
    original = first.page_path.read_text() + "\nUser prose edit.\n\n> [!thoth-feedback]\n> More context.\n"
    first.page_path.write_text(original)
    connector = WikiReconcileConnector(config, layout=compiler.layout, db=compiler.db)
    result = asyncio.run(connector.collect())
    assert result["statuses"] == {"user_modified": 1}
    assert result["blocked"][0]["reason"] == "user_modified"
    assert first.page_path.read_text() == original
    assert compiler.publications.feedback_records(first.page_path)[0]["request_text"] == "More context."


def test_reconcile_connector_registered_and_schedulable(compiler_runtime):
    from core.connector_registry import load_connector_registry
    from core.connector_scheduler import resolve_connector_schedules
    compiler, _, _ = compiler_runtime
    # Use source checkout manifests while data remains entirely temporary.
    from pathlib import Path
    config.set("paths.connectors_dir", str(Path(__file__).parents[1] / "collectors"))
    config.set("sources.wiki_reconcile", {"enabled": True, "schedule": {
        "enabled": True, "interval_seconds": 120, "run_on_startup": True,
    }})
    registry = load_connector_registry(config)
    assert any(manifest.name == "wiki_reconcile" for manifest in registry.list())
    assert any(schedule.connector_name == "wiki_reconcile" for schedule in resolve_connector_schedules(config))
