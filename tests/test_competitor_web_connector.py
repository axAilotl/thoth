from dataclasses import replace
import socket
from types import SimpleNamespace

import pytest
from core.agent_surface import AgentSurfaceService
from core.public_web import fetch_public
from test_connector_capture_events import _layout_and_db, _managed_run, _store


def service(tmp_path, *, render=False, renderer=None):
    config, layout, db = _layout_and_db(tmp_path)
    config.set("managed.connector_version", "test-pin")
    run = _managed_run("competitor_web", "company", selected_resources=("web:https://public.example/pricing",),
        source_config={"pages": [{"url": "https://public.example/pricing", "render": render}], "max_redirects": 2, "user_agent": "ThothResearch/1.0"},
        options={}, budgets={"max_items": 20})
    return AgentSurfaceService(config, layout=layout, db=db, event_store=_store(layout), managed_run=run, account_root=tmp_path, web_renderer=renderer)


def test_registry_snapshots_compare_changes_and_keep_previous_evidence(tmp_path, monkeypatch):
    collector = service(tmp_path)
    page = [b"<html><title>Pricing</title><main>Price $10</main><script>unstable()</script></html>"]
    def fetch(url, **kwargs):
        return {"url": url, "status": 404 if url.endswith("robots.txt") else 200,
                "body": b"" if url.endswith("robots.txt") else page[0], "headers": {"Content-Type": "text/html"}, "redirects": []}
    monkeypatch.setattr("collectors.competitor_web_connector.fetch_public", fetch)
    first = collector.run_connector("competitor_web", execute=True)
    assert first["result"]["coverage"][0]["changed"] is True
    first_hash = first["result"]["coverage"][0]["current_hash"]
    collector.managed_run = replace(collector.managed_run, run_id="second", idempotency_key="second")
    second = collector.run_connector("competitor_web", execute=True)
    assert second["result"]["coverage"][0]["changed"] is False
    assert second["result"]["queued_count"] == 0
    page[0] = b"<html><title>Pricing</title><main>Price $20</main></html>"
    collector.managed_run = replace(collector.managed_run, run_id="third", idempotency_key="third")
    third = collector.run_connector("competitor_web", execute=True)
    assert third["result"]["coverage"][0]["previous_hash"] == first_hash
    assert third["result"]["queued_count"] == 1
    assert len(list(collector.layout.library_root.rglob("*.md"))) == 2


@pytest.mark.parametrize("mode,code", [("robots", "robots_refused"), ("blocked", "http_403"), ("empty", "empty_or_dynamic_content"), ("dynamic", "browser_unavailable")])
def test_refused_or_unrenderable_page_is_not_success(tmp_path, monkeypatch, mode, code):
    collector = service(tmp_path, render=mode == "dynamic")
    def fetch(url, **kwargs):
        robots = url.endswith("robots.txt")
        body = b"User-agent: *\nDisallow: /" if robots and mode == "robots" else (b"<script>only javascript</script>" if mode == "empty" else b"<html>Page</html>")
        status = (200 if mode == "robots" else 404) if robots else (403 if mode == "blocked" else 200)
        return {"url": url, "status": status, "body": body, "headers": {"Content-Type": "text/html"}, "redirects": []}
    monkeypatch.setattr("collectors.competitor_web_connector.fetch_public", fetch)
    result = collector.run_connector("competitor_web", execute=True)["result"]
    assert result["status"] == "failed" and result["coverage"][0]["errors"] == [{"code": code}]


def test_dynamic_page_uses_explicit_managed_renderer(tmp_path, monkeypatch):
    calls = []
    def render(url, **kwargs):
        calls.append((url, kwargs))
        return "<html><main>Rendered pricing</main></html>"
    collector = service(tmp_path, render=True, renderer=render)
    monkeypatch.setattr("collectors.competitor_web_connector.fetch_public", lambda url, **kw: {"url": url, "status": 404 if url.endswith("robots.txt") else 200, "body": b"<html></html>", "headers": {"Content-Type": "text/html"}, "redirects": []})
    result = collector.run_connector("competitor_web", execute=True)["result"]
    assert result["status"] == "completed"
    assert result["coverage"][0]["capture_method"] == "browser"
    assert calls[0][1]["allowed_hosts"] == {"public.example"}


def test_public_fetch_pins_ip_and_rejects_private_redirect(monkeypatch):
    connections, sockets = [], []
    monkeypatch.setattr(socket, "getaddrinfo", lambda host, *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1" if host == "internal.example" else "93.184.216.34", 443))])
    monkeypatch.setattr(socket, "create_connection", lambda address, *a: sockets.append(address))
    class Connection:
        def __init__(self, host, **kw):
            connections.append(host)
        def request(self, *args, **kwargs):
            self._create_connection(("public.example", 443), 1)
        def getresponse(self):
            return SimpleNamespace(status=302, getheader=lambda name: "https://internal.example/private")
        def close(self):
            pass
    monkeypatch.setattr("http.client.HTTPSConnection", Connection)
    with pytest.raises(ValueError, match="non-public"):
        fetch_public("https://public.example/", allowed_hosts={"public.example", "internal.example"}, timeout=1, max_bytes=100, max_redirects=2, user_agent="test")
    assert connections == ["public.example"]
    assert sockets == [("93.184.216.34", 443)]
