from dataclasses import asdict
import hashlib
import json

import pytest

from collectors.personal.ai_export import AIExportParser
from core.managed_worker import execute
from test_managed_runtime import environment
from test_connector_capture_events import _managed_run


def chatgpt(text="Please check the downstream repository tomorrow"):
    def message(role, text, timestamp):
        return {"id": role, "author": {"role": role}, "content": {"parts": [text]}, "create_time": timestamp}
    return [{"id": "personal", "title": "PRIVATE_UNSELECTED", "messages": []},
        {"id": "session", "title": "Company research", "project_id": "company", "current_node": "answer",
         "create_time": 1775001600, "update_time": 1775001800,
         "mapping": {"root": {"parent": None, "message": None},
                     "question": {"parent": "root", "message": message("user", text, 1775001600)},
                     "answer": {"parent": "question", "message": message("assistant", "Research findings", 1775001800)},
                     "abandoned": {"parent": "question", "message": message("assistant", "ABANDONED_BRANCH", 1775001700)}}}]


def history_run(content, revision, operation="upsert"):
    source = {"provider": "chatgpt", "project_id": "company", "source_id": "session",
              "revision": revision, "operation": operation}
    if operation == "upsert":
        source["content_sha256"] = hashlib.sha256(content.encode()).hexdigest()
    return asdict(_managed_run("ai_history", "a", run_id=f"run-{revision}", idempotency_key=f"key-{revision}",
        selected_resources=("ai_history:company:session",), source_config={"sources": [source]}, options={}, budgets={"max_items": 2}))


def test_selected_history_current_branch_update_and_explicit_delete(tmp_path, monkeypatch):
    runtime, _, expiry, network_calls = environment(tmp_path, monkeypatch)
    def worker(run, credentials, timeout, history_exports=None):
        return execute({"run": run, "credentials": credentials, "config_file": runtime.settings.config_file,
            "account_root": str(tmp_path), "connector_version": "test-pin", "history_exports": history_exports})
    runtime.worker = worker
    content = json.dumps(chatgpt())
    run = history_run(content, "rev1")
    exports = [{"resource_id": "ai_history:company:session", "content": content}]
    result = runtime.execute(run, {}, expiry, exports)
    assert result["status"] == "completed"
    assert result["coverage"][0]["source"] == "ai_history:company:session"
    artifact_id = "history:chatgpt:company:session"
    document, = runtime.read_artifacts(run, [artifact_id], expiry)
    assert "user:" in document["body_text"] and "assistant:" in document["body_text"]
    assert "downstream repository" in document["body_text"]
    assert document["metadata"]["authority"] == "none" and document["metadata"]["historical_evidence"]
    assert "PRIVATE_UNSELECTED" not in json.dumps(result) and "ABANDONED_BRANCH" not in document["body_text"]
    payload = json.loads(runtime.db.get_ingestion_entry(artifact_id).payload_json)
    assert len(payload["messages"]) == 2 and payload["messages"][0]["created_at"].endswith("+00:00")
    assert "PRIVATE_UNSELECTED" not in payload["raw_content"] and "ABANDONED_BRANCH" not in payload["raw_content"]
    assert runtime.execute(run, {}, expiry, exports) == result
    updated = json.dumps(chatgpt("Updated company finding"))
    second = history_run(updated, "rev2")
    assert runtime.execute(second, {}, expiry, [{**exports[0], "content": updated}])["status"] == "completed"
    assert "Updated company finding" in runtime.read_artifacts(second, [artifact_id], expiry)[0]["body_text"]
    with runtime.db._get_connection() as connection:
        assert connection.execute("SELECT count(*) FROM ingestion_queue WHERE artifact_id = ?", (artifact_id,)).fetchone()[0] == 1
        ledger = connection.execute("SELECT value_json FROM automation_state").fetchall()
        assert "downstream repository" not in str([tuple(row) for row in ledger])
    deleted = history_run(None, "rev3", "delete")
    assert runtime.execute(deleted, {}, expiry, [])["status"] == "completed"
    tombstone, = runtime.read_artifacts(deleted, [artifact_id], expiry)
    assert tombstone["body_text"] == "" and tombstone["metadata"]["operation"] == "delete"
    assert network_calls == []


def test_history_wrong_project_digest_and_selection_never_capture(tmp_path, monkeypatch):
    runtime, _, expiry, calls = environment(tmp_path, monkeypatch)
    def worker(run, credentials, timeout, history_exports=None):
        return execute({"run": run, "credentials": credentials, "config_file": runtime.settings.config_file,
            "account_root": str(tmp_path), "connector_version": "test-pin", "history_exports": history_exports})
    runtime.worker = worker
    data = chatgpt()
    data[1]["project_id"] = "private"
    content = json.dumps(data)
    result = runtime.execute(history_run(content, "foreign"), {}, expiry,
        [{"resource_id": "ai_history:company:session", "content": content}])
    assert result["status"] == "failed" and result["artifacts"] == []
    run = history_run(content, "digest")
    run["source_config"]["sources"][0]["content_sha256"] = "0" * 64
    assert runtime.execute(run, {}, expiry, [{"resource_id": "ai_history:company:session", "content": content}])["status"] == "failed"
    assert runtime.db.get_ingestion_entry("history:chatgpt:company:session") is None


def test_claude_normalized_parser_and_budgets():
    parser = AIExportParser(max_export_bytes=10000, max_messages=5, max_message_characters=100)
    export = [{"uuid": "s", "name": "Company", "chat_messages": [
        {"uuid": "m1", "sender": "human", "text": "Historical instruction", "created_at": "2026-04-01T12:00:00Z"},
        {"uuid": "m2", "sender": "assistant", "content": [{"type": "text", "text": "Past suggestion"}]}]}]
    result = parser.parse(json.dumps(export), provider="claude", source_id="s", project_id="company")
    assert [item["role"] for item in result["messages"]] == ["user", "assistant"]
    assert result["messages"][1]["created_at"] is None
    normalized = {"id": "s", "messages": [{"role": "tool", "text": "Old tool result"}]}
    assert parser.parse(json.dumps(normalized), provider="normalized", source_id="s", project_id="company")["messages"][0]["role"] == "tool"
    with pytest.raises(ValueError, match="export_budget"):
        parser.parse(" " * 10001, provider="claude", source_id="s", project_id="company")
    branch = chatgpt()[1]
    branch.pop("current_node")
    with pytest.raises(ValueError, match="current_branch"):
        parser.parse(json.dumps(branch), provider="chatgpt", source_id="session", project_id="company")
