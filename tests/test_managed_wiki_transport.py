import asyncio
from copy import deepcopy
import hashlib
import json

import httpx
import pytest

from core.archivist_state import load_archivist_topic_state
from core.managed_api import create_app
from core.managed_runtime import ManagedRuntimeError
from core.managed_wiki_runtime import ManagedWikiRuntime
from test_managed_runtime import environment


def request_and_runtime(tmp_path, monkeypatch):
    runtime, _, expiry, _ = environment(tmp_path, monkeypatch)
    runtime.config.set("managed_wiki", {"enabled": True, "callback_socket": str(tmp_path / "model.sock"),
        "callback_timeout_seconds": 5, "max_callback_response_bytes": 100000,
        "max_sources": 5, "max_topics": 2, "max_page_bytes": 10000})
    content = "# Company evidence\n\nThe selected project uses a batch audio pipeline.\n"
    digest = hashlib.sha256(content.encode()).hexdigest()
    context = {"account_id": "a", "archive_id": "archive-a", "job_id": "wiki-job", "lease_generation": 1,
        "author": "Cissa agent", "topic_ids": ["batch-audio"], "source_cutoff": "2026-05-01T00:00:00Z",
        "source_manifest": {"notes/evidence.md": {"account_id": "a", "archive_id": "archive-a", "source_id": "source1",
            "revision": "r1", "content_hash": digest, "file_sha256": digest, "observed_at": "2026-04-01T00:00:00Z",
            "uri": "cissa://sources/source1"}},
        "expected_pages": {"pages/batch.md": {"revision": None, "content_hash": None}},
        "budgets": {"max_source_bytes": 10000, "max_total_source_bytes": 20000}}
    request = {"context": context, "source_documents": {"notes/evidence.md": content},
        "page_snapshots": {"pages/batch.md": None}, "topics": {"version": 1,
            "defaults": {"retrieval": {"mode": "literal"}}, "topics": [{"id": "batch-audio", "title": "Batch audio",
                "output_path": "pages/batch.md", "include_roots": ["notes"], "max_sources": 5}]},
        "model_routes": {"archivist": {"model": "approved-model", "max_tokens": 16384, "max_input_characters": 100000}},
        "lease_expires_at": expiry}
    calls = []
    async def callback(payload):
        calls.append(payload)
        return {"content": "## Pipeline\nThe project uses batch audio processing [S1]."}
    runtime.wiki_invoke = callback
    return runtime, request, calls


def test_protected_wiki_proposal_replay_and_ack_advance_only_after_publication(tmp_path, monkeypatch):
    runtime, request, calls = request_and_runtime(tmp_path, monkeypatch)
    async def flow():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(runtime), client=None), base_url="http://managed",
            headers={"X-Cissa-Managed-Peer": "cissa-dispatcher"}) as client:
            response = await client.post("/v1/wiki/propose", json=request)
            assert response.status_code == 200, response.text
            result = response.json()
            assert result["summary"]["proposed"] == 1, result
            proposal = result["results"][0]["proposal"]
            assert not list(runtime.layout.wiki_root.rglob("*.md"))
            assert load_archivist_topic_state("batch-audio", db=runtime.db).last_success_at is None
            assert (await client.post("/v1/wiki/propose", json=request)).json() == result
            assert len(calls) == 1 and calls[0]["identity"] == {key: request["context"][key] for key in ("account_id", "archive_id", "job_id", "lease_generation")}
            assert calls[0]["lease_expires_at"] == request["lease_expires_at"] and calls[0]["max_tokens"] == 16384
            receipt = {"operation_id": "write1", "job_id": "wiki-job", "status": "applied", "path": proposal["path"],
                "revision": "whole-published-page-hash", "published_content_hash": "whole-published-page-hash",
                "content_hash": proposal["content_hash"], "sync_status": "pending", "source_refs": proposal["source_manifest"]}
            ack = {"context": request["context"], "proposal_id": proposal["proposal_id"], "receipt": receipt,
                   "lease_expires_at": request["lease_expires_at"]}
            response = await client.post("/v1/wiki/ack", json=ack)
            assert response.status_code == 200 and response.json()["status"] == "acknowledged", response.text
            assert load_archivist_topic_state("batch-audio", db=runtime.db).last_success_at is not None
            assert not list(runtime.layout.wiki_root.rglob("*.md"))
    asyncio.run(flow())


def test_wiki_transport_rejects_foreign_scope_paths_stale_input_and_hashes(tmp_path, monkeypatch):
    runtime, request, calls = request_and_runtime(tmp_path, monkeypatch)
    foreign = deepcopy(request)
    foreign["context"]["account_id"] = "other"
    with pytest.raises(ManagedRuntimeError, match="account_binding"):
        runtime.wiki_propose(**foreign)
    escaped = deepcopy(request)
    escaped["context"]["source_manifest"]["../private.md"] = escaped["context"]["source_manifest"].pop("notes/evidence.md")
    with pytest.raises(ManagedRuntimeError, match="snapshot_path"):
        runtime.wiki_propose(**escaped)
    corrupted = deepcopy(request)
    corrupted["source_documents"]["notes/evidence.md"] += "changed"
    with pytest.raises(ManagedRuntimeError, match="source_digest"):
        runtime.wiki_propose(**corrupted)
    assert calls == []
    runtime.wiki_propose(**request)
    changed = deepcopy(request)
    changed["model_routes"]["archivist"]["model"] = "different"
    with pytest.raises(ManagedRuntimeError, match="inputs_changed"):
        runtime.wiki_propose(**changed)


def test_callback_uses_fixed_private_transport_and_bounded_response(tmp_path, monkeypatch):
    runtime, request, calls = request_and_runtime(tmp_path, monkeypatch)
    service = ManagedWikiRuntime(runtime)
    seen = []
    def handle(http_request):
        seen.append(http_request)
        assert http_request.url.path == "/v1/integrations/thoth/model"
        assert json.loads(http_request.content)["identity"]["job_id"] == "wiki-job"
        return httpx.Response(200, json={"content": "Evidence [S1]."})
    def transport(*, uds):
        assert uds == str(tmp_path / "model.sock")
        return httpx.MockTransport(handle)
    monkeypatch.setattr("core.managed_wiki_runtime.httpx.AsyncHTTPTransport", transport)
    result = asyncio.run(service._callback({"identity": {"job_id": "wiki-job"}, "lease_expires_at": request["lease_expires_at"], "task": "archivist"}))
    assert result["content"] == "Evidence [S1]." and len(seen) == 1
    runtime.config.set("managed_wiki.max_callback_response_bytes", 1)
    with pytest.raises(ManagedRuntimeError, match="response_budget"):
        asyncio.run(service._callback({"identity": {"job_id": "wiki-job"}, "lease_expires_at": request["lease_expires_at"]}))
