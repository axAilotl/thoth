"""Exercise recurring managed entrypoints against their real account database."""
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from core.agent_surface import AgentSurfaceService
from collectors.arxiv_collector import ArXivCollector
from test_connector_capture_events import _layout_and_db, _managed_run, _store

STAMP = int(datetime(2026, 4, 3, tzinfo=timezone.utc).timestamp())


def service(tmp_path, connector, resources, settings, *, items=100, pages=10):
    config, layout, db = _layout_and_db(tmp_path)
    config.set("managed.connector_version", "test-pin")
    request = _managed_run(connector, "company", selected_resources=tuple(resources),
        source_config=settings, options={}, budgets={"max_items": items, "max_pages": pages})
    return AgentSurfaceService(config, layout=layout, db=db, event_store=_store(layout),
                               managed_run=request, account_root=tmp_path)


def test_recurring_competitor_budget_rotates_all_sources_and_replay_does_not(tmp_path, monkeypatch):
    urls = [f"https://public.example/page-{i}" for i in range(42)]
    svc = service(tmp_path, "competitor_web", ["web:" + u for u in urls],
        {"pages": [{"url": u} for u in urls], "max_redirects": 1, "user_agent": "test"})
    visited = []
    def fetch(url, **kwargs):
        if not url.endswith("robots.txt"):
            visited.append(url)
        return {"url": url, "status": 404 if url.endswith("robots.txt") else 200,
                "body": b"<html>Evidence</html>", "headers": {"Content-Type": "text/html"}, "redirects": []}
    monkeypatch.setattr("collectors.competitor_web_connector.fetch_public", fetch)
    for period in range(5):
        svc.managed_run = replace(svc.managed_run, run_id=f"week-{period}", idempotency_key=f"week-{period}")
        result = svc.run_connector("competitor_web", execute=True)["result"]
        assert sum(r["observed_count"] for r in result["coverage"]) == 10
        assert all(r["covered_through"] is None for r in result["coverage"] if r["status"] == "partial")
        before = list(visited)
        with pytest.raises(ValueError, match="retry is not due"):
            svc.run_connector("competitor_web", execute=True)
        assert visited == before  # Ledger replay must not reserve a new rotation.
    assert visited[:42] == urls
    assert set(visited) == set(urls)

    complete = service(tmp_path / "completed", "competitor_web", ["web:" + urls[0]],
        {"pages": [{"url": urls[0]}], "max_redirects": 1, "user_agent": "test"})
    result = complete.run_connector("competitor_web", execute=True)
    before = list(visited)
    assert complete.run_connector("competitor_web", execute=True) == result
    assert visited == before


def test_discussion_busy_first_source_cannot_starve_any_of_33_selections(tmp_path, monkeypatch):
    names = [f"brand{i}" for i in range(33)]
    svc = service(tmp_path, "ugc_discussion", [f"reddit:{n}" for n in names],
        {"reddit": [{"name": n} for n in names], "hn_queries": [], "page_size": 100,
         "include_comments": False, "comment_limit": 1000})
    calls = []
    def fetch(self, url, params=None):
        calls.append(params["subreddit"])
        return {"data": [{"id": f"{params['subreddit']}-{i}", "created_utc": STAMP - i,
                           "title": "Evidence", "num_comments": 0} for i in range(10)]}
    monkeypatch.setattr("collectors.research_capture.ResearchCapture.fetch", fetch)
    first = svc.run_connector("ugc_discussion", execute=True)["result"]
    counts = {r["source"]: r["observed_count"] for r in first["coverage"]}
    assert sum(counts.values()) == 100 and min(counts.values()) == 3 and max(counts.values()) == 4
    assert len(calls) == 33 and first["queued_count"] == 100
    assert all(r["status"] == "partial" and r["covered_through"] is None for r in first["coverage"])
    svc.managed_run = replace(svc.managed_run, run_id="next-week", idempotency_key="next-week")
    second = svc.run_connector("ugc_discussion", execute=True)["result"]
    next_counts = {r["source"]: r["observed_count"] for r in second["coverage"]}
    assert counts["reddit:brand0"] == 4 and next_counts["reddit:brand1"] == 4


def test_comments_share_parent_quota_and_global_budget(tmp_path, monkeypatch):
    svc = service(tmp_path, "ugc_discussion", ["reddit:one", "hn:two"],
        {"reddit": [{"name": "one"}], "hn_queries": ["two"], "page_size": 100,
         "include_comments": True, "comment_limit": 1000}, items=6)
    limits = []
    def fetch(self, url, params=None):
        if "posts/search" in url:
            return {"data": [{"id": "p1", "created_utc": STAMP, "title": "Post", "num_comments": 1000}]}
        if "comments/tree" in url:
            limits.append(params["limit"])
            return [{"id": f"c{i}", "created_utc": STAMP, "body": "Comment"} for i in range(1000)]
        return {"hits": [{"objectID": f"h{i}", "created_at_i": STAMP, "title": "Story"} for i in range(10)], "nbPages": 2}
    monkeypatch.setattr("collectors.research_capture.ResearchCapture.fetch", fetch)
    result = svc.run_connector("ugc_discussion", execute=True)["result"]
    assert limits == [2]
    assert sum(r["observed_count"] for r in result["coverage"]) == 6
    assert result["queued_count"] == 6
    assert all(r["covered_through"] is None for r in result["coverage"])


@pytest.mark.parametrize("connector", ["ugc_stores", "ugc_youtube"])
def test_other_research_sources_share_global_budget(tmp_path, monkeypatch, connector):
    if connector == "ugc_stores":
        resources = ["apple:123", "apple:456"]
        settings = {"apps": [{"platform": "apple", "app_id": x} for x in ["123", "456"]],
                    "country": "us", "language": "en", "apple_max_pages": 10, "play_page_size": 100}
        def fetch(self, url, params=None):
            return {"feed": {"entry": [{"id": {"label": str(i)}, "updated": {"label": "2026-04-03T00:00:00Z"},
                "title": {"label": "Review"}, "content": {"label": "Evidence"}, "im:rating": {"label": "4"}} for i in range(10)]}}
        monkeypatch.setattr("collectors.research_capture.ResearchCapture.fetch", fetch)
    else:
        resources = ["youtube:one", "youtube:two"]
        settings = {"queries": ["one", "two"], "search_count": 8}
        def metadata(self, target, *, flat=False):
            if flat:
                return {"entries": [{"id": f"video{i:06d}"} for i in range(8)]}
            return {"id": target.split("=")[-1], "upload_date": "20260403", "title": "Video", "description": "Evidence"}
        monkeypatch.setattr("collectors.ugc_youtube_connector.YouTubeResearchCollector._metadata", metadata)
    svc = service(tmp_path, connector, resources, settings, items=5)
    result = svc.run_connector(connector, execute=True)["result"]
    assert sorted(r["observed_count"] for r in result["coverage"]) == [2, 3]
    assert all(r["status"] == "partial" for r in result["coverage"])


@pytest.mark.parametrize("mode", ["topic", "category"])
def test_arxiv_later_sources_receive_fair_query_budget(tmp_path, monkeypatch, mode):
    config, layout, db = _layout_and_db(tmp_path)
    config.set("collection.max_items", 5)
    collector = ArXivCollector(config=config, layout=layout, db=db, capture_event_store=_store(layout))
    calls = []
    def feed(url, source):
        calls.append((url, source))
        return SimpleNamespace(entries=[SimpleNamespace(published="2026-04-03T00:00:00Z") for _ in range(10)])
    monkeypatch.setattr(collector, "_read_feed", feed)
    monkeypatch.setattr(collector, "_queue_paper_from_entry_safely", lambda *a, **kw: None)
    if mode == "topic":
        collector.discover_papers(["one", "two"])
        assert [int(parse_qs(urlsplit(u).query)["max_results"][0]) for u, _ in calls] == [3, 2]
    else:
        collector.scan_rss_feeds(["cs.AI", "cs.SD"])
    assert sorted(r["observed_count"] for r in collector.last_summary["coverage"]) == [2, 3]
