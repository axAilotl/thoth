import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from core.agent_surface import AgentSurfaceService
from core.connector_scheduler import ConnectorSchedule, ConnectorScheduler
from test_connector_capture_events import _layout_and_db, _managed_run, _store


def service(tmp_path, *, max_pages=3):
    config, layout, db = _layout_and_db(tmp_path)
    config.set("managed.connector_version", "test-pin")
    request = _managed_run("ugc_discussion", "company", selected_resources=("reddit:example", "hn:example.com"),
        source_config={"reddit": [{"name": "example", "keywords": ["wearable"]}], "hn_queries": ["example.com"],
                       "page_size": 2, "comment_limit": 5, "include_comments": True},
        options={}, budgets={"max_items": 20, "max_pages": max_pages})
    return AgentSurfaceService(config, layout=layout, db=db, event_store=_store(layout),
                               managed_run=request, account_root=tmp_path)


def test_registry_scheduler_collects_discussion_and_replays_receipt(tmp_path, monkeypatch):
    collector = service(tmp_path)
    timestamp = int(datetime(2026, 4, 3, tzinfo=timezone.utc).timestamp())
    calls = []
    def get(session, url, params=None, **kwargs):
        calls.append((url, params))
        assert session.trust_env is False
        if "posts/search" in url:
            result = {"data": [{"id": "p1", "created_utc": timestamp, "title": "A wearable", "num_comments": 1}]}
        elif "comments/tree" in url:
            result = {"data": [{"id": "c1", "created_utc": timestamp, "body": "Useful experience"}]}
        else:
            result = {"hits": [{"objectID": "h1", "created_at_i": timestamp, "title": "example.com experience"}], "nbPages": 1}
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: result)
    monkeypatch.setattr("requests.Session.get", get)
    scheduler = ConnectorScheduler(collector)
    scheduler._execute(ConnectorSchedule("ugc_discussion", 3600, True, {}))
    receipt = collector.run_connector("ugc_discussion", execute=True)
    assert receipt["status"] == "completed"
    assert receipt["result"]["queued_count"] == 3
    assert len(receipt["result"]["artifacts"]) == 3
    assert len(calls) == 3
    assert all(len(row["content_hash"]) == 64 for row in receipt["result"]["artifacts"])
    assert len(list(collector.layout.library_root.rglob("*.md"))) == 3
    assert str(tmp_path) not in json.dumps(receipt)


def test_partial_outage_does_not_advance_failed_discussion_source(tmp_path, monkeypatch):
    collector = service(tmp_path)
    def get(session, url, **kwargs):
        if "arctic-shift" in url:
            raise TimeoutError("credentials-and-private-path-must-not-escape")
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"hits": [], "nbPages": 0})
    monkeypatch.setattr("requests.Session.get", get)
    result = collector.run_connector("ugc_discussion", execute=True)["result"]
    assert result["status"] == "partial"
    coverage = {row["source"]: row for row in result["coverage"]}
    assert coverage["reddit:example"]["covered_through"] is None
    assert coverage["hn:example.com"]["covered_through"] == collector.managed_run.window_end
    assert "must-not-escape" not in json.dumps(result)


def test_source_scope_is_checked_before_network(tmp_path, monkeypatch):
    collector = service(tmp_path)
    collector.config.set("sources.ugc_discussion.hn_queries", ["not-granted"])
    collector.config.set("sources.ugc_discussion.reddit", [])
    monkeypatch.setattr("requests.Session.get", lambda *a, **k: pytest.fail("network must not run"))
    with pytest.raises(ValueError, match="outside selected"):
        collector.run_connector("ugc_discussion", execute=True)


def test_hn_page_budget_cursor_resumes_without_duplicate_evidence(tmp_path, monkeypatch):
    collector = service(tmp_path, max_pages=1)
    collector.config.set("sources.ugc_discussion.reddit", [])
    collector.config.set("collection.retry_base_seconds", 0.000001)
    timestamp = int(datetime(2026, 4, 3, tzinfo=timezone.utc).timestamp())
    pages = []
    def get(session, url, params=None, **kwargs):
        pages.append(params["page"])
        hit = {"objectID": "overlap", "created_at_i": timestamp, "title": "example.com experience"}
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"hits": [hit], "nbPages": 2})
    monkeypatch.setattr("requests.Session.get", get)
    first = collector.run_connector("ugc_discussion", execute=True)
    assert first["status"] == "partial"
    assert first["result"]["coverage"][0]["covered_through"] is None
    second = collector.run_connector("ugc_discussion", execute=True)
    assert second["status"] == "completed"
    assert pages == [0, 1]
    assert len(second["result"]["artifacts"]) == 1
