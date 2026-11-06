import json
import subprocess
from types import SimpleNamespace

from core.agent_surface import AgentSurfaceService
from test_connector_capture_events import _layout_and_db, _managed_run, _store


def service(tmp_path):
    config, layout, db = _layout_and_db(tmp_path)
    config.set("managed.connector_version", "test-pin")
    run = _managed_run("ugc_youtube", "company", selected_resources=("youtube:query one", "youtube:query two"),
        source_config={"queries": ["query one", "query two"], "search_count": 3}, options={}, budgets={"max_items": 20})
    return AgentSurfaceService(config, layout=layout, db=db, event_store=_store(layout), managed_run=run, account_root=tmp_path)


def test_real_youtube_entrypoint_dates_dedupes_and_sanitizes_metadata(tmp_path, monkeypatch):
    collector = service(tmp_path)
    calls = []
    def worker(command, **kwargs):
        calls.append(command[-1])
        assert "--ignore-config" in command and "--skip-download" in command
        assert "HOME" not in kwargs["env"] and "OPENAI_API_KEY" not in kwargs["env"]
        if command[-1].startswith("ytsearch"):
            value = {"entries": [{"id": "abcdefghijk"}, {"id": "oldoldold00"}, {"id": "undated0000"}]}
        else:
            video = command[-1].split("=")[-1]
            value = {"id": video, "title": "2029 review", "upload_date": {"abcdefghijk": "20260403", "oldoldold00": "20200101", "undated0000": None}[video],
                     "description": "Public review", "formats": [{"url": "private-expiring-url"}], "http_headers": {"Authorization": "must-not-persist"}}
        return SimpleNamespace(returncode=0, stdout=json.dumps(value))
    monkeypatch.setattr("subprocess.run", worker)
    result = collector.run_connector("ugc_youtube", execute=True)["result"]
    assert result["status"] == "partial"
    assert result["queued_count"] == 1 and len(result["artifacts"]) == 1
    assert len(calls) == 5  # two queries, each unique video inspected once
    assert all(row["errors"] == [{"code": "missing_upload_date"}] for row in result["coverage"])
    for raw in collector.layout.raw_root.rglob("*.json"):
        assert "must-not-persist" not in raw.read_text() and "private-expiring-url" not in raw.read_text()


def test_youtube_worker_timeout_is_failed_coverage(tmp_path, monkeypatch):
    collector = service(tmp_path)
    def timeout(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])
    monkeypatch.setattr("subprocess.run", timeout)
    result = collector.run_connector("ugc_youtube", execute=True)["result"]
    assert result["status"] == "failed"
    assert all(row["covered_through"] is None for row in result["coverage"])
