import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import httpx
import pytest

from core.managed_api import create_app
from core.managed_runtime import ManagedRuntime, ManagedRuntimeError, ManagedRuntimeSettings
from core.managed_worker import execute
from test_connector_capture_events import _config, _managed_run


def environment(tmp_path, monkeypatch):
    config = _config(tmp_path)
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config.data))
    settings = ManagedRuntimeSettings(account_id="a", archive_id="archive-a", account_root=str(tmp_path),
        connector_version="test-pin", expected_peer="cissa-dispatcher", socket_path=str(tmp_path / "socket"),
        config_file=str(config_file), max_request_bytes=20000, max_storage_bytes=10000000,
        max_artifact_characters=200000, max_artifacts_per_read=100, execution_timeout_seconds=5,
        termination_grace_seconds=1, max_lease_seconds=120)
    calls = []
    def get(session, url, **kwargs):
        calls.append((url, kwargs.get("headers", {})))
        return SimpleNamespace(status_code=200, json=lambda: [{"starred_at": "2026-04-03T00:00:00Z",
            "repo": {"id": 123, "full_name": "org/repo", "description": "Research repository"}}])
    monkeypatch.setattr("requests.Session.get", get)
    def worker(run, credentials, timeout):
        return execute({"run": run, "credentials": credentials, "config_file": str(config_file),
                        "account_root": str(tmp_path), "connector_version": "test-pin"})
    runtime = ManagedRuntime(settings, worker=worker)
    run = asdict(_managed_run("github", "a", options={"limit": 2}, budgets={"max_items": 2}))
    expiry = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
    return runtime, run, expiry, calls


def test_real_worker_artifact_receipt_restarts_and_replays_without_source_io(tmp_path, monkeypatch):
    runtime, run, expiry, calls = environment(tmp_path, monkeypatch)
    result = runtime.execute(run, {"token": "scoped-secret"}, expiry)
    assert result["status"] == "completed"
    assert result["artifacts"][0]["source"] == "github"
    restarted = ManagedRuntime(runtime.settings, worker=runtime.worker)
    assert restarted.receipt(run, expiry) == result
    document, = restarted.read_artifacts(run, ["gh_123"], expiry)
    assert document["content_sha256"] == hashlib.sha256(document["body_text"].encode()).hexdigest()
    assert document["source_uri"] == "https://github.com/org/repo"
    assert "scoped-secret" not in json.dumps(result) + json.dumps(document)
    newer = {**run, "lease_generation": 2}
    assert restarted.execute(newer, {}, expiry)["managed_run"]["lease_generation"] == 2
    assert restarted.receipt(newer, expiry)["managed_run"]["lease_generation"] == 2
    assert len(calls) == 1
    with pytest.raises(ManagedRuntimeError, match="receipt_lease_mismatch"):
        restarted.receipt(run, expiry)
    with pytest.raises(ManagedRuntimeError, match="receipt_inputs_mismatch"):
        restarted.receipt({**newer, "selected_resources": ["github:user:a", "github:repo:other/private"]}, expiry)
    with pytest.raises(ManagedRuntimeError, match="artifact_not_in_run_receipt"):
        restarted.read_artifacts(newer, ["/etc/passwd"], expiry)
    row = restarted.db.get_ingestion_entry("gh_123")
    payload = json.loads(row.payload_json)
    payload["raw_content"] = "modified evidence"
    with restarted.db._get_connection() as connection:
        connection.execute("UPDATE ingestion_queue SET payload_json = ? WHERE artifact_id = ?", (json.dumps(payload), "gh_123"))
    with pytest.raises(ManagedRuntimeError, match="artifact_revision_changed"):
        restarted.read_artifacts(newer, ["gh_123"], expiry)


def test_authority_expiry_serial_admission_and_storage_budget(tmp_path, monkeypatch):
    runtime, run, expiry, calls = environment(tmp_path, monkeypatch)
    for changed, code in (({"account_id": "other"}, "account_binding_mismatch"),
                          ({"connector_version": "other"}, "connector_version_mismatch"),
                          ({"connector": "pi_skill"}, "connector_not_managed")):
        with pytest.raises(ManagedRuntimeError, match=code):
            runtime.execute({**run, **changed}, {}, expiry)
    with pytest.raises(ManagedRuntimeError, match="invalid_or_expired_lease"):
        runtime.execute(run, {}, "2026-01-01T00:00:00Z")
    runtime.lock.acquire()
    try:
        with pytest.raises(ManagedRuntimeError, match="runtime_busy"):
            runtime.execute(run, {}, expiry)
    finally:
        runtime.lock.release()
    runtime.settings = replace(runtime.settings, max_storage_bytes=1)
    with pytest.raises(ManagedRuntimeError, match="account_storage_budget_exceeded"):
        runtime.execute(run, {}, expiry)
    assert calls == []


def test_asgi_requires_uds_verified_peer_and_exact_bounded_envelope(tmp_path, monkeypatch):
    runtime, run, expiry, calls = environment(tmp_path, monkeypatch)
    async def check():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(runtime), client=None), base_url="http://managed") as client:
            assert (await client.get("/health")).status_code == 403
            headers = {"X-Cissa-Managed-Peer": "cissa-dispatcher"}
            assert (await client.get("/health", headers=headers)).json()["connector_version"] == "test-pin"
            body = {"run": run, "credentials": {}, "lease_expires_at": expiry}
            assert (await client.post("/v1/collections/execute", json={**body, "path": "/etc/passwd"}, headers=headers)).status_code == 422
            assert (await client.post("/v1/collections/execute", content=b"x" * 20001, headers=headers)).status_code == 413
            response = await client.post("/v1/collections/execute", json=body, headers=headers)
            assert response.status_code == 200 and response.json()["managed_run"]["run_id"] == run["run_id"]
            response = await client.post("/v1/collections/artifacts", json={"run": run, "artifact_ids": ["gh_123"], "lease_expires_at": expiry}, headers=headers)
            assert response.status_code == 200 and response.json()[0]["external_id"] == "gh_123"
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(runtime)), base_url="http://managed") as client:
            assert (await client.get("/health", headers=headers)).status_code == 403
    asyncio.run(check())


def test_settings_pin_layout_and_worker_transport_are_explicit(tmp_path, monkeypatch):
    runtime, run, expiry, calls = environment(tmp_path, monkeypatch)
    settings_file = tmp_path / "settings.json"
    settings_file.write_text(json.dumps(asdict(runtime.settings)))
    assert ManagedRuntimeSettings.load(settings_file, build_revision="test-pin") == runtime.settings
    with pytest.raises(ValueError, match="pinned image"):
        ManagedRuntimeSettings.load(settings_file, build_revision="other")
    config = json.loads(Path(runtime.settings.config_file).read_text())
    config["database"]["path"] = str(tmp_path.parent / "outside/meta.db")
    Path(runtime.settings.config_file).write_text(json.dumps(config))
    with pytest.raises(ValueError, match="account root"):
        ManagedRuntime(runtime.settings)
    assert not (tmp_path.parent / "outside").exists()
    def popen(argv, **kwargs):
        assert "scoped-secret" not in str(argv) + str(kwargs)
        assert kwargs["env"] == {"PATH": __import__("os").defpath, "TZ": "UTC"}
        assert kwargs["start_new_session"]
        def communicate(payload, timeout):
            assert json.loads(payload)["credentials"] == {"token": "scoped-secret"}
            return json.dumps({"managed_run": {"run_id": "expected"}}), "suppressed provider error"
        return SimpleNamespace(communicate=communicate, returncode=0)
    monkeypatch.setattr("core.managed_runtime.subprocess.Popen", popen)
    assert runtime._worker(run, {"token": "scoped-secret"}, 1)["managed_run"]["run_id"] == "expected"


def test_worker_deadline_terminates_process_group_without_exposing_secrets(tmp_path, monkeypatch):
    runtime, run, expiry, calls = environment(tmp_path, monkeypatch)
    killed, attempts = [], []
    def communicate(*args, **kwargs):
        attempts.append(kwargs)
        if len(attempts) == 1:
            raise subprocess.TimeoutExpired("worker", 1)
        return "", "secret provider output"
    monkeypatch.setattr("core.managed_runtime.subprocess.Popen", lambda *a, **k: SimpleNamespace(pid=123, communicate=communicate))
    monkeypatch.setattr("core.managed_runtime.os.killpg", lambda pid, sig: killed.append((pid, sig)))
    with pytest.raises(ManagedRuntimeError, match="worker_deadline_exceeded"):
        runtime._worker(run, {"token": "scoped-secret"}, 1)
    assert killed[0][0] == 123 and len(attempts) == 2
