import json
import sys
from types import SimpleNamespace
from datetime import datetime

import pytest
from core.agent_surface import AgentSurfaceService
from test_connector_capture_events import _layout_and_db, _managed_run, _store


def service(tmp_path, max_pages=3):
    config, layout, db = _layout_and_db(tmp_path)
    config.set("managed.connector_version", "test-pin")
    request = _managed_run("ugc_stores", "company", selected_resources=("apple:123", "play:com.example"),
        source_config={"apps": [{"platform": "apple", "app_id": "123"}, {"platform": "play", "app_id": "com.example"}],
                       "country": "us", "language": "en", "apple_max_pages": 10, "play_page_size": 5},
        options={}, budgets={"max_items": 20, "max_pages": max_pages})
    return AgentSurfaceService(config, layout=layout, db=db, event_store=_store(layout),
                               managed_run=request, account_root=tmp_path)


def test_real_store_entrypoint_keeps_review_dates_sample_limits_and_raw_pages(tmp_path, monkeypatch):
    collector = service(tmp_path)
    def get(session, url, **kwargs):
        entries = [{"id": {"label": "a1"}, "updated": {"label": "2026-04-03T00:00:00Z"},
            "title": {"label": "Review"}, "content": {"label": "Updated experience"}, "im:rating": {"label": "4"}}] if "page=1/" in url else []
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"feed": {"entry": entries}})
    monkeypatch.setattr("requests.Session.get", get)
    monkeypatch.setenv("GITHUB_TOKEN", "hostile-secret")
    def worker(command, **kwargs):
        assert command[-1] == "collectors.play_reviews_worker"
        assert "GITHUB_TOKEN" not in kwargs["env"]
        assert kwargs["env"]["TZ"] == "UTC"
        assert kwargs["timeout"] > 0
        request = json.loads(kwargs["input"])
        assert request["app_id"] == "com.example"
        return SimpleNamespace(returncode=0, stdout=json.dumps({"records": [{"reviewId": "p1", "at": "2026-04-04T00:00:00Z", "content": "Review", "score": 4, "reviewCreatedVersion": "1.2"}], "cursor": None}))
    monkeypatch.setattr("subprocess.run", worker)
    receipt = collector.run_connector("ugc_stores", execute=True)
    assert receipt["status"] == "completed"
    assert len(receipt["result"]["artifacts"]) == 2
    assert all(row["coverage_kind"] == "sample" for row in receipt["result"]["coverage"])
    assert all(row["time_basis"] == "review_updated_at" for row in receipt["result"]["coverage"])
    assert all(row["raw_pages"] for row in receipt["result"]["coverage"])


def test_empty_or_unavailable_stores_are_not_successful_zero_results(tmp_path, monkeypatch):
    collector = service(tmp_path)
    monkeypatch.setattr("requests.Session.get", lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"feed": {}}))
    monkeypatch.setattr("subprocess.run", lambda *a, **k: SimpleNamespace(returncode=1, stdout='{"error":"TimeoutError"}'))
    receipt = collector.run_connector("ugc_stores", execute=True)
    assert receipt["status"] == "partial"
    assert all(row["covered_through"] is None for row in receipt["result"]["coverage"])


def test_play_parser_worker_propagates_network_errors(tmp_path, monkeypatch):
    from collectors.play_reviews_worker import collect_page
    def fetch(*args):
        raise TimeoutError("upstream")
    module = SimpleNamespace(_fetch_review_items=fetch, post=None)
    monkeypatch.setitem(sys.modules, "google_play_scraper.features.reviews", module)
    monkeypatch.setitem(sys.modules, "google_play_scraper.constants.element", SimpleNamespace(ElementSpecs=SimpleNamespace(Review={})))
    monkeypatch.setitem(sys.modules, "google_play_scraper.constants.request", SimpleNamespace(Formats=SimpleNamespace(Reviews=SimpleNamespace(build=lambda **kwargs: "https://play.google.com"))))
    with pytest.raises(TimeoutError):
        collect_page({"language": "en", "country": "us", "app_id": "com.example", "count": 5, "timeout_seconds": 1})


def test_capped_store_sample_keeps_cursor_without_claiming_complete_window(tmp_path, monkeypatch):
    collector = service(tmp_path, max_pages=1)
    collector.config.set("sources.ugc_stores.apps", [{"platform": "apple", "app_id": "123"}])
    entry = {"id": {"label": "a1"}, "updated": {"label": "2026-04-03T00:00:00Z"},
             "title": {"label": "Edited review"}, "content": {"label": "New opinion"}, "im:rating": {"label": "2"}, "im:version": {"label": "2.0"}}
    monkeypatch.setattr("requests.Session.get", lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"feed": {"entry": [entry]}}))
    receipt = collector.run_connector("ugc_stores", execute=True)
    row = receipt["result"]["coverage"][0]
    assert row["status"] == "partial" and row["covered_through"] is None
    assert row["cursor"]["page"] == 2
    queued = collector.db.get_ingestion_entry(receipt["result"]["artifacts"][0]["artifact_id"])
    assert '2.0' in queued.payload_json and 'New opinion' in queued.payload_json
