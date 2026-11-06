import asyncio
from dataclasses import replace
import hashlib
from pathlib import Path

import pytest

from core.archivist_compiler import ArchivistCompiler
from core.archivist_state import load_archivist_topic_state
from core.archivist_topics import ArchivistTopicRegistry
from core.archivist_runtime import run_archivist_topics
from core.managed_model_gateway import ManagedModelGateway
from core.managed_wiki import ManagedWikiContext, acknowledge_wiki_proposal
from test_connector_capture_events import _layout_and_db
from tests.test_archivist_compiler import _build_topic, _write_prompt_files, _write_source_files


def environment(tmp_path, invoke=None):
    config, layout, db = _layout_and_db(tmp_path)
    _write_prompt_files(tmp_path)
    _write_source_files(layout)
    topic = _build_topic()
    source = layout.vault_root / "tweets/companion_note.md"
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    context = ManagedWikiContext(account_id="a", archive_id="company", job_id="job", lease_generation=1,
        author="Cissa agent", topic_ids=(topic.id,), source_cutoff="2026-04-30T00:00:00Z", account_root=tmp_path,
        source_manifest={"tweets/companion_note.md": {"account_id": "a", "archive_id": "company",
            "source_id": "source-1", "revision": "rev-1", "content_hash": digest, "file_sha256": digest,
            "observed_at": "2026-04-03T00:00:00Z", "uri": "cissa://sources/source-1"}},
        expected_pages={topic.output_path: {"revision": None, "content_hash": None}})
    calls = []
    async def gateway_call(request):
        calls.append(request)
        return await invoke(request) if invoke else {"content": "## Findings\nSource-backed finding [S1]."}
    gateway = ManagedModelGateway(identity=context.identity(), tasks={"archivist": {"model": "configured-model", "max_tokens": 16384, "temperature": 0.2}}, invoke=gateway_call)
    compiler = ArchivistCompiler(config, project_root=tmp_path, layout=layout, db=db,
                                 llm_interface=gateway, managed_context=context)
    return compiler, context, topic, calls


def test_managed_wiki_uses_only_scoped_sources_and_never_writes_before_receipt(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "hostile-global")
    monkeypatch.setattr("core.archivist_compiler.LLMInterface", lambda *a, **k: pytest.fail("ambient model config must not be used"))
    compiler, context, topic, calls = environment(tmp_path)
    before = list(compiler.layout.wiki_root.rglob("*"))
    result = asyncio.run(compiler.compile_topic(topic))
    assert result.status == "proposed"
    assert len(result.proposal["source_manifest"]) == 1
    assert calls[0]["max_tokens"] == 16384 and calls[0]["account_id"] == "a"
    assert "Persona Memory Toolkit" not in calls[0]["prompt"]
    assert "hostile-global" not in str(calls)
    assert list(compiler.layout.wiki_root.rglob("*")) == before
    assert load_archivist_topic_state(topic.id, db=compiler.db).last_success_at is None
    proposal = result.proposal
    rejected = {**context.identity(), "proposal_id": proposal["proposal_id"], "status": "conflict"}
    assert acknowledge_wiki_proposal(context=context, layout=compiler.layout, db=compiler.db,
        proposal_id=proposal["proposal_id"], receipt=rejected)["status"] == "unapplied"
    assert load_archivist_topic_state(topic.id, db=compiler.db).last_success_at is None
    assert asyncio.run(compiler.compile_topic(topic)).status == "proposed"
    applied = {**rejected, "status": "applied", "revision": "revision-2", "content_hash": proposal["content_hash"]}
    assert acknowledge_wiki_proposal(context=context, layout=compiler.layout, db=compiler.db,
        proposal_id=proposal["proposal_id"], receipt=applied)["status"] == "acknowledged"
    assert load_archivist_topic_state(topic.id, db=compiler.db).last_success_at is not None
    assert list(compiler.layout.wiki_root.rglob("*")) == before


def test_managed_wiki_preserves_human_edit_and_rejects_foreign_authority(tmp_path):
    compiler, context, topic, calls = environment(tmp_path)
    page = compiler.layout.wiki_root / topic.output_path
    page.parent.mkdir(parents=True)
    page.write_text("Human content")
    result = asyncio.run(compiler.compile_topic(topic))
    assert result.status == "blocked" and calls == [] and page.read_text() == "Human content"
    with pytest.raises(ValueError, match="gateway identity"):
        ArchivistCompiler(compiler.config, project_root=tmp_path, layout=compiler.layout, db=compiler.db,
            llm_interface=compiler.llm_interface, managed_context=replace(context, account_id="other"))


def test_managed_wiki_rechecks_sources_after_model_call(tmp_path):
    async def edit(request):
        (tmp_path / "vault/tweets/companion_note.md").write_text("changed source")
        return {"content": "Finding [S1]."}
    compiler, context, topic, calls = environment(tmp_path, invoke=edit)
    with pytest.raises(ValueError, match="revision changed"):
        asyncio.run(compiler.compile_topic(topic))
    assert load_archivist_topic_state(topic.id, db=compiler.db).last_success_at is None


def test_runtime_preserves_successful_proposal_when_another_topic_fails(tmp_path, monkeypatch):
    async def generate(request):
        return {"content": "Invalid citation [S99]." if "Topic=Bad topic" in request["prompt"] else "Valid claim [S1]."}
    compiler, context, topic, calls = environment(tmp_path, invoke=generate)
    bad = replace(topic, id="bad-topic", title="Bad topic", output_path="pages/bad.md")
    context = replace(context, topic_ids=(bad.id, topic.id), expected_pages={**context.expected_pages, bad.output_path: {"revision": None, "content_hash": None}})
    monkeypatch.setattr("core.archivist_compiler.load_archivist_topic_registry", lambda *a, **k: ArchivistTopicRegistry(topics=(bad, topic)))
    result = asyncio.run(run_archivist_topics(compiler.config, project_root=tmp_path, layout=compiler.layout,
        db=compiler.db, managed_context=context, llm_interface=compiler.llm_interface))
    assert result["status"] == "partial"
    assert result["summary"]["proposed"] == 1 and result["summary"]["failed"] == 1


def test_source_cutoff_and_changed_apply_manifest_are_enforced(tmp_path):
    compiler, context, topic, calls = environment(tmp_path)
    future = {key: {**value, "observed_at": "2026-05-01T00:00:00Z"} for key, value in context.source_manifest.items()}
    compiler.managed_context = replace(context, source_manifest=future)
    assert asyncio.run(compiler.compile_topic(topic)).reason == "no_candidates"
    assert calls == []
    compiler.managed_context = context
    proposal = asyncio.run(compiler.compile_topic(topic)).proposal
    receipt = {**context.identity(), "proposal_id": proposal["proposal_id"], "content_hash": proposal["content_hash"], "revision": "new", "status": "applied"}
    with pytest.raises(ValueError, match="no longer authorized"):
        acknowledge_wiki_proposal(context=replace(context, source_manifest=future), layout=compiler.layout,
            db=compiler.db, proposal_id=proposal["proposal_id"], receipt=receipt)
