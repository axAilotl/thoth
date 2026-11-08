"""Grok/strip history parsing, X archive uploads and managed X bookmark sync.

Fixtures are synthetic and follow the shapes of real exports (structure only).
"""
from dataclasses import asdict
import hashlib
import json

import httpx
import pytest

from collectors.personal.ai_export import AIExportParser
from collectors.x_archive_connector import match_notes, parse_archive_file
from core.managed_worker import execute
from test_connector_capture_events import _managed_run
from test_managed_runtime import environment


def parser(**limits):
    return AIExportParser(**{"max_export_bytes": 100000, "max_messages": 50, "max_message_characters": 5000, **limits})


def test_chatgpt_and_claude_keep_text_and_count_stripped_parts():
    def node(parent, role, content):
        return {"parent": parent, "message": {"id": role + str(parent), "author": {"role": role}, "content": content}}
    chatgpt = [{"id": "s", "current_node": "e", "mapping": {
        "a": {"parent": None, "message": None},
        "b": node("a", "user", {"content_type": "multimodal_text", "parts": [
            {"content_type": "image_asset_pointer", "asset_pointer": "file-service://x"}, "What is in this picture?"]}),
        "c": node("b", "assistant", {"content_type": "code", "text": "print(1)"}),
        "d": node("c", "tool", {"content_type": "execution_output", "text": "1"}),
        "e": node("d", "assistant", {"content_type": "text", "parts": ["A cat."]})}}]
    result = parser().parse(json.dumps(chatgpt), provider="chatgpt", source_id="s", project_id="p")
    assert [(m["role"], m["text"]) for m in result["messages"]] == [("user", "What is in this picture?"), ("assistant", "A cat.")]
    assert result["stripped_parts"] == {"empty_message": 2, "image": 1, "tool_result": 1, "tool_use": 1}
    claude = [{"uuid": "s", "chat_messages": [
        {"uuid": "1", "sender": "human", "content": [{"type": "text", "text": "Plan the trip"}, {"type": "image"}],
         "attachments": [{"file_name": "x.pdf"}]},
        {"uuid": "2", "sender": "assistant", "content": [{"type": "thinking", "thinking": "secret"},
         {"type": "tool_use", "name": "search"}, {"type": "tool_result", "content": []}]},
        {"uuid": "3", "sender": "assistant", "content": [{"type": "text", "text": "Here is a plan"}]}]}]
    result = parser().parse(json.dumps(claude), provider="claude", source_id="s", project_id="p")
    assert [m["text"] for m in result["messages"]] == ["Plan the trip", "Here is a plan"]
    assert result["stripped_parts"] == {"attachment": 1, "empty_message": 1, "image": 1, "thinking": 1,
                                        "tool_result": 1, "tool_use": 1}
    assert "secret" not in json.dumps(result)
    only_tools = [{"uuid": "s", "chat_messages": [{"uuid": "1", "sender": "assistant", "content": [{"type": "tool_use"}]}]}]
    with pytest.raises(ValueError, match="history_no_text_content"):
        parser().parse(json.dumps(only_tools), provider="claude", source_id="s", project_id="p")


def grok_export():
    def response(rid, parent, sender, message, ms, **extra):
        return {"response": {"_id": rid, "conversation_id": "g1", "parent_response_id": parent, "sender": sender,
                             "message": message, "create_time": {"$date": {"$numberLong": str(ms)}},
                             "model": "grok-x", "metadata": {}, **extra}, "share_link": None}
    return {"conversations": [
        {"conversation": {"id": "other", "title": "PRIVATE_UNSELECTED", "leaf_response_id": None}, "responses": []},
        {"conversation": {"id": "g1", "title": "Truck repair", "create_time": "2026-04-01T10:00:00.000000Z",
                          "modify_time": "2026-04-01T10:05:00.000000Z", "leaf_response_id": "r4",
                          "workspace_id": "grok-folder"},
         "responses": [
             response("r1", None, "human", "My truck makes a noise", 1775037600000),
             response("r2", "r1", "assistant", "ABANDONED_BRANCH", 1775037601000),
             response("r3", "r1", "assistant", "Check the belt", 1775037602000,
                      thinking_trace="HIDDEN_REASONING", steps=[{"tool": "search"}], web_search_results=[{"url": "u"}]),
             response("r4", "r3", "human", "", 1775037603000, generated_image_urls=["img"]),
         ]}], "projects": [], "tasks": [], "media_posts": []}


def test_grok_current_branch_strips_reasoning_and_tools():
    result = parser().parse(json.dumps(grok_export()), provider="grok", source_id="g1", project_id="company")
    assert [(m["role"], m["text"]) for m in result["messages"]] == [("user", "My truck makes a noise"), ("assistant", "Check the belt")]
    assert result["messages"][0]["created_at"] == "2026-04-01T10:00:00+00:00"
    assert result["title"] == "Truck repair" and result["updated_at"].startswith("2026-04-01T10:05")
    assert result["stripped_parts"] == {"empty_message": 1, "image": 1, "thinking": 1, "tool_result": 1, "tool_use": 1}
    text = json.dumps(result)
    assert "ABANDONED_BRANCH" not in text and "HIDDEN_REASONING" not in text and "PRIVATE_UNSELECTED" not in text


def _history_worker(runtime, tmp_path):
    def worker(run, credentials, timeout, history_exports=None):
        return execute({"run": run, "credentials": credentials, "config_file": runtime.settings.config_file,
                        "account_root": str(tmp_path), "connector_version": "test-pin", "history_exports": history_exports})
    return worker


def test_grok_imports_through_managed_runtime(tmp_path, monkeypatch):
    runtime, _, expiry, calls = environment(tmp_path, monkeypatch)
    runtime.worker = _history_worker(runtime, tmp_path)
    content = json.dumps(grok_export())
    source = {"provider": "grok", "project_id": "company", "source_id": "g1", "revision": "r1", "operation": "upsert",
              "content_sha256": hashlib.sha256(content.encode()).hexdigest()}
    run = asdict(_managed_run("ai_history", "a", selected_resources=("ai_history:company:g1",),
                              source_config={"sources": [source]}, options={}, budgets={"max_items": 2}))
    result = runtime.execute(run, {}, expiry, [{"resource_id": "ai_history:company:g1", "content": content}])
    assert result["status"] == "completed"
    document, = runtime.read_artifacts(run, ["history:grok:company:g1"], expiry)
    assert document["source_uri"] == "https://grok.com/chat/g1"
    assert "Check the belt" in document["body_text"] and "HIDDEN_REASONING" not in document["body_text"]
    assert document["metadata"]["stripped_parts"]["thinking"] == 1 and document["metadata"]["authority"] == "none"
    assert calls == []


def js(name, part, items):
    return f"window.YTD.{name}.part{part} = " + json.dumps(items, indent=2)


def archive_files():
    tweets = js("tweets", 0, [
        {"tweet": {"id_str": "101", "id": "101", "created_at": "Fri Jan 09 15:12:21 +0000 2026", "lang": "en",
                   "full_text": "Reading https://t.co/a1 tonight", "favorite_count": "3", "retweet_count": "1",
                   "entities": {"hashtags": [], "symbols": [], "user_mentions": [], "urls": [
                       {"url": "https://t.co/a1", "expanded_url": "https://arxiv.org/abs/2504.19413",
                        "display_url": "arxiv.org/abs/2504.19413", "indices": ["8", "23"]}]}}},
        {"tweet": {"id_str": "102", "id": "102", "created_at": "Sat Jan 10 08:00:00 +0000 2026", "lang": "en",
                   "full_text": "A long thought that X truncated here…", "entities": {"urls": []}}},
        {"tweet": {"id_str": "not-a-number", "created_at": "Sat Jan 10 08:00:00 +0000 2026", "full_text": "bad"}},
    ])
    notes = js("note_tweet", 0, [
        {"noteTweet": {"noteTweetId": "9001", "createdAt": "2026-01-10T08:00:00.000Z", "updatedAt": "2026-01-10T08:00:00.000Z",
                       "lifecycle": {}, "core": {"text": "A long thought that X truncated here and then kept going "
                                                         "https://t.co/g1", "mentions": [], "cashtags": [], "hashtags": [],
                                                 "urls": [{"shortUrl": "https://t.co/g1", "expandedUrl": "https://github.com/tobi/qmd",
                                                           "displayUrl": "github.com/tobi/qmd", "fromIndex": 0, "toIndex": 1}]}}},
        {"noteTweet": {"noteTweetId": "9002", "createdAt": "2025-05-05T05:05:05.000Z", "lifecycle": {},
                       "core": {"text": "An orphan long post", "urls": []}}},
    ])
    likes = js("like", 1, [
        {"like": {"tweetId": "201", "fullText": "Great talk https://t.co/yt", "expandedUrl": "https://twitter.com/i/web/status/201"}},
        {"like": {"tweetId": "202", "expandedUrl": "https://twitter.com/i/web/status/202"}},
    ])
    return {"tweets": tweets, "note-tweet": notes, "like-part1": likes}


def archive_run(files, revision="r1"):
    sources = [{"archive_id": "owner", "file": name, "revision": revision, "operation": "upsert",
                "content_sha256": hashlib.sha256(content.encode()).hexdigest()} for name, content in files.items()]
    resources = tuple(f"x_archive:owner:{name}" for name in files)
    run = asdict(_managed_run("x_archive", "a", run_id=f"run-{revision}", idempotency_key=f"key-{revision}",
                              selected_resources=resources, source_config={"sources": sources}, options={},
                              budgets={"max_items": 10}))
    return run, [{"resource_id": f"x_archive:owner:{name}", "content": content} for name, content in files.items()]


def test_x_archive_parses_real_shapes_and_refuses_other_sections():
    section, items = parse_archive_file(archive_files()["tweets"], "tweets", max_bytes=100000)
    assert section == "tweets" and len(items) == 3
    with pytest.raises(ValueError, match="section_not_ingested"):
        parse_archive_file(js("direct_messages", 0, []), "direct-messages", max_bytes=100000)
    with pytest.raises(ValueError, match="section_mismatch"):
        parse_archive_file(js("direct_messages", 0, []), "tweets", max_bytes=100000)
    assert match_notes([{"id": "n", "created_at": "2026-01-10T08:00:00+00:00", "text": "Same second text"}],
                       [{"id": "1", "created_at": "2026-01-10T08:00:00+00:00", "text": "Same second"},
                        {"id": "2", "created_at": "2026-01-10T08:00:00+00:00", "text": "Other"}]) == {"n": "1"}


def test_x_archive_upload_imports_search_only_documents(tmp_path, monkeypatch):
    runtime, _, expiry, calls = environment(tmp_path, monkeypatch)
    runtime.settings = runtime.settings.__class__(**{**asdict(runtime.settings), "max_request_bytes": 200000})
    runtime.worker = _history_worker(runtime, tmp_path)
    run, exports = archive_run(archive_files())
    result = runtime.execute(run, {}, expiry, exports)
    # The malformed tweet is skipped; the rest of the file still imports.
    assert result["status"] == "partial"
    coverage = {item["source"]: item for item in result["coverage"]}
    assert coverage["x_archive:owner:tweets"]["errors"] == [{"code": "x_archive_item_rejected"}]
    assert coverage["x_archive:owner:like-part1"]["status"] == "completed"
    ids = sorted(item["artifact_id"] for item in result["artifacts"])
    # Tweet 102 is the short form of long post 9001, so only the long post is kept.
    assert ids == ["x:like:201", "x:like:202", "x:note:9001", "x:note:9002", "x:tweet:101"]
    documents = {doc["external_id"]: doc for doc in runtime.read_artifacts(run, ids, expiry)}
    tweet = documents["x:tweet:101"]
    assert tweet["body_text"] == "Reading https://arxiv.org/abs/2504.19413 tonight"
    assert tweet["source_uri"] == "https://x.com/i/web/status/101"
    assert tweet["source_updated_at"] == "2026-01-09T15:12:21+00:00"
    assert tweet["metadata"]["x"]["follow_on"] == [{"url": "https://arxiv.org/abs/2504.19413", "kind": "arxiv"}]
    assert all(doc["metadata"]["x"]["search_only"] is True for doc in documents.values())
    note = documents["x:note:9001"]
    assert "kept going https://github.com/tobi/qmd" in note["body_text"]
    assert note["metadata"]["x"]["tweet_id"] == "102" and note["source_uri"] == "https://x.com/i/web/status/102"
    assert documents["x:note:9002"]["source_uri"] == "cissa://x/note/9002"
    assert documents["x:like:202"]["metadata"]["x"]["follow_on"] == [
        {"url": "https://x.com/i/web/status/202", "kind": "x_post"}]
    assert documents["x:like:201"]["metadata"]["x"]["follow_on"] == [{"url": "https://t.co/yt", "kind": "short_link"}]
    # No per-item raw copies; owner content stays out of the ledger.
    assert not list((tmp_path).rglob("*x_archive*/*.json"))
    with runtime.db._get_connection() as connection:
        ledger = connection.execute("SELECT value_json FROM automation_state").fetchall()
    assert "Great talk" not in str([tuple(row) for row in ledger])
    assert calls == []


def test_x_archive_document_budget_leaves_honest_offset(tmp_path, monkeypatch):
    runtime, _, expiry, _ = environment(tmp_path, monkeypatch)
    runtime.settings = runtime.settings.__class__(**{**asdict(runtime.settings), "max_request_bytes": 200000})
    config = json.loads(open(runtime.settings.config_file).read())
    config.setdefault("collection", {})["max_x_archive_documents"] = 1
    open(runtime.settings.config_file, "w").write(json.dumps(config))
    runtime.worker = _history_worker(runtime, tmp_path)
    run, exports = archive_run({"like-part1": archive_files()["like-part1"]})
    result = runtime.execute(run, {}, expiry, exports)
    assert result["status"] == "partial" and len(result["artifacts"]) == 1
    assert result["coverage"][0]["cursor"] == {"offset": 1}
    assert json.loads(result["resume_token"]) == {"x_archive:owner:like-part1": {"offset": 1}}


def test_x_archive_rejects_digest_mismatch_and_wrong_connector_uploads(tmp_path, monkeypatch):
    runtime, github_run, expiry, _ = environment(tmp_path, monkeypatch)
    runtime.worker = _history_worker(runtime, tmp_path)
    run, exports = archive_run({"like-part1": archive_files()["like-part1"]})
    run["source_config"]["sources"][0]["content_sha256"] = "0" * 64
    assert runtime.execute(run, {}, expiry, exports)["status"] == "failed"
    from core.managed_runtime import ManagedRuntimeError
    with pytest.raises(ManagedRuntimeError, match="invalid_history_exports"):
        runtime.execute(github_run, {}, expiry, exports)


def x_api_run(username="owner_x"):
    return asdict(_managed_run("x_api", "a", selected_resources=(f"x_api:bookmarks:{username}",),
                               source_config={"username": username}, options={}, budgets={"max_items": 10, "max_pages": 2}))


def test_managed_x_api_bookmarks_use_cissa_token_and_never_the_bundle(tmp_path, monkeypatch):
    runtime, _, expiry, _ = environment(tmp_path, monkeypatch)
    runtime.worker = _history_worker(runtime, tmp_path)
    requests = []

    def handler(request):
        requests.append((request.url.path, request.headers.get("Authorization")))
        if request.url.path == "/2/users/me":
            return httpx.Response(200, json={"data": {"id": "42", "username": "Owner_X"}})
        if request.url.path == "/2/users/42/bookmarks":
            return httpx.Response(200, json={
                "data": [{"id": "301", "text": "Paper https://t.co/p", "author_id": "7", "created_at": "2026-04-02T00:00:00.000Z",
                          "entities": {"urls": [{"url": "https://t.co/p", "expanded_url": "https://youtu.be/abc"}]}}],
                "includes": {"users": [{"id": "7", "username": "author", "name": "Author"}]}, "meta": {"result_count": 1}})
        return httpx.Response(404)
    real_client = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(transport=httpx.MockTransport(handler)))
    run = x_api_run()
    result = runtime.execute(run, {"token": "cissa-access"}, expiry)
    assert result["status"] == "completed"
    assert result["coverage"][0]["source"] == "x_api:bookmarks:owner_x"
    assert {auth for _, auth in requests} == {"Bearer cissa-access"}
    document, = runtime.read_artifacts(run, ["x:bookmark:301"], expiry)
    assert document["body_text"] == "Paper https://youtu.be/abc"
    assert document["metadata"]["x"]["kind"] == "bookmark" and "search_only" not in document["metadata"]["x"]
    assert document["metadata"]["x"]["follow_on"] == [{"url": "https://youtu.be/abc", "kind": "youtube"}]
    assert "cissa-access" not in json.dumps(result) + json.dumps(document)
    assert not list(tmp_path.rglob("x_api_tokens.json"))
    # The incremental checkpoint lives in the account root: the next run stops at 301.
    second = {**x_api_run(), "run_id": "run-2", "idempotency_key": "key-2"}
    repeat = runtime.execute(second, {"token": "cissa-access"}, expiry)
    assert repeat["status"] == "completed" and repeat["artifacts"] == []


def test_managed_x_api_refuses_other_account_and_missing_token(tmp_path, monkeypatch):
    runtime, _, expiry, _ = environment(tmp_path, monkeypatch)
    runtime.worker = _history_worker(runtime, tmp_path)
    real_client = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json={"data": {"id": "9", "username": "someone_else"}}))))
    result = runtime.execute(x_api_run(), {"token": "t"}, expiry)
    assert result["status"] == "failed" and result["coverage"][0]["errors"] == [{"code": "x_api_account_mismatch"}]
    missing = {**x_api_run(), "run_id": "run-m", "idempotency_key": "key-m"}
    assert runtime.execute(missing, {}, expiry)["coverage"][0]["errors"] == [{"code": "x_api_credential_missing"}]
