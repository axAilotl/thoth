import asyncio
import json

from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.source_records import SourceRecordStore
from core.wiki_updater import CompiledWikiUpdater
from tests.test_web_clipper_collector import make_collector


def test_default_intake_records_source_without_publishing_clip(tmp_path):
    collector, vault = make_collector(tmp_path)
    collector.config.set("wiki.publish_source_pages", False)
    source = vault / "Clippings" / "example.md"
    source.parent.mkdir(parents=True, exist_ok=True)
    original = "---\ntitle: Source article\nsource: https://example.test/article\n---\nUseful evidence.\n"
    source.write_text(original)
    collector.collect()
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    asyncio.run(runtime.process_pending_ingestions_once())
    assert not list((collector.layout.wiki_root / "pages").glob("clip-*.md"))
    assert not (collector.layout.wiki_root / "log.md").exists()
    record = SourceRecordStore(collector.db).get("webclip:Clippings/example.md")
    assert record and record["canonical_id"]
    assert "Useful evidence" in record["payload"]["body"]
    assert source.read_text() == original
    exported = SourceRecordStore(collector.db).export()
    assert json.loads(json.dumps(exported))["records"][0]["artifact_id"] == record["artifact_id"]


def test_index_omits_source_records_and_metadata_summaries(tmp_path):
    collector, _ = make_collector(tmp_path)
    updater = CompiledWikiUpdater(collector.config, layout=collector.layout, db=collector.db)
    pages = collector.layout.wiki_root / "pages"
    (pages / "clip-example.md").write_text("---\nthoth_artifact_id: webclip:example\ntitle: Source\n---\n")
    (pages / "topic-example.md").write_text("---\nthoth_kind: topic\ntitle: Topic\ndescription: enormous metadata summary\n---\n")
    result = updater.refresh_index().read_text()
    assert "topic-example.md" in result
    assert "clip-example.md" not in result
    assert "enormous metadata" not in result
