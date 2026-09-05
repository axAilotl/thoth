"""The uncluttered intake -> search -> topic -> feedback seam, without paid calls."""
import asyncio
from pathlib import Path

from collectors.corpus_index_connector import CorpusIndexConnector
from core.archivist_compiler import ArchivistCompiler
from core.archivist_topics import ArchivistTopicDefinition
from core.corpus_query import query_corpus
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.source_records import SourceRecordStore
from tests.test_archivist_compiler import FakeLLMInterface, _write_prompt_files
from tests.test_inbox_connector import ready, setup_inbox, two_scans


def test_inbox_source_to_topic_and_feedback_without_source_sidecars(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config, db, layout, inbox, clock, handler = setup_inbox(tmp_path, monkeypatch, consume=True)
    config.set("wiki.publish_source_pages", False)
    config.set("sources.corpus_index.enabled", True)
    config.set("sources.corpus_index.include_roots", ["documents"])
    config.set("sources.corpus_index.embeddings_enabled", False)
    config.set("llm.prompts.archivist.system_file", "prompts/archivist_system.md")
    config.set("llm.prompts.archivist.user_file", "prompts/archivist_user.md")
    _write_prompt_files(tmp_path)
    original = b"# Streaming ASR\nStreaming speech recognition trades latency against accuracy.\n"
    ready(inbox / "speech.md", original)
    _, intake = two_scans(handler, clock)
    record = intake["records"][0]
    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    asyncio.run(runtime.process_ingestion_entry(db.get_ingestion_entry(record["artifact_id"])))
    assert SourceRecordStore(db).get(record["artifact_id"]) is not None
    assert not list((layout.wiki_root / "pages").glob("*.md"))
    asyncio.run(CorpusIndexConnector(config, layout=layout, db=db).collect())
    found = asyncio.run(query_corpus(config=config, layout=layout, db=db, query="latency"))
    assert found["results"]
    topic = ArchivistTopicDefinition(id="speech", title="Streaming ASR",
        output_path="pages/topic-speech.md", include_roots=("documents",),
        include_terms=("speech",), max_sources=5, allow_manual_force=True)
    llm = FakeLLMInterface("## Findings\nStreaming has latency tradeoffs [S1].\n")
    compiler = ArchivistCompiler(config, project_root=tmp_path, layout=layout, db=db, llm_interface=llm)
    first = asyncio.run(compiler.compile_topic(topic))
    assert first.status == "compiled"
    feedback = "> [!thoth-feedback]\n> Explain the accuracy tradeoff in more detail.\n"
    first.page_path.write_text(first.page_path.read_text() + "\n" + feedback)
    second = asyncio.run(compiler.compile_topic(topic))
    assert second.status == "compiled" and second.reason == "feedback_pending"
    assert "accuracy tradeoff" in llm.calls[-1]["prompt"]
    assert first.page_path.read_text().count(feedback) == 1
    assert compiler.publications.feedback_records(first.page_path)[0]["status"] == "included"
    assert Path(record["destination"]).read_bytes() == original
    assert not (inbox / "speech.md").exists()
    assert len(list((layout.wiki_root / "pages").glob("*.md"))) == 1
    first.page_path.write_text(first.page_path.read_text() + "\nMy direct correction.\n")
    assert asyncio.run(compiler.compile_topic(topic, force=True)).status == "blocked"
    assert "My direct correction." in first.page_path.read_text()
