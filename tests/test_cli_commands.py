import subprocess
import sys
import json
from pathlib import Path
from types import SimpleNamespace


def test_removed_playwright_commands_are_rejected():
    repo_root = Path(__file__).resolve().parents[1]

    for command in ("download", "full"):
        result = subprocess.run(
            [sys.executable, "thoth.py", command, "--help"],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "invalid choice" in result.stderr


def test_web_clipper_commands_are_still_wired():
    repo_root = Path(__file__).resolve().parents[1]

    for command in ("web-clipper", "ingest-queue", "okf", "connectors", "capture"):
        result = subprocess.run(
            [sys.executable, "thoth.py", command, "--help"],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert command in result.stdout


def test_okf_lint_command_is_wired():
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "thoth.py", "okf", "lint", "--help"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Validate the compiled wiki" in result.stdout


def test_connectors_list_command_reads_registry_metadata():
    repo_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "thoth.py", "connectors", "list", "--json"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    names = [item["name"] for item in payload["connectors"]]
    assert names[:9] == [
        "x_api",
        "arxiv",
        "github",
        "huggingface",
        "web_clipper",
        "youtube",
        "omi",
        "skill_outputs",
        "pi_skills",
    ]


CAPTURE_EVENT = {
    "event_id": "event-1",
    "source_id": "source-1",
    "session_id": "session-1",
    "event_type": "note",
    "status": "captured",
    "provenance": {"tool": "thoth.py"},
    "raw_ref_ids": ["raw-1"],
    "raw_refs": [{"raw_ref_id": "raw-1"}],
    "privacy": {"classification": "private"},
    "privacy_class": "private",
    "retention": {"policy": "default"},
    "retention_class": "default",
    "artifact_ids": ["artifact-1"],
    "security_state": {
        "state": "open",
        "finding_count": 1,
        "open_finding_count": 1,
        "max_severity": "high",
    },
    "security_findings": [{"finding_id": "finding-1"}],
}


class FakeCaptureSurface:
    def list_events(self, *, source_id=None, session_id=None, limit=None):
        assert source_id is None
        assert session_id is None
        assert limit is None
        return {"events": [CAPTURE_EVENT], "total": 1}

    def get_event(self, event_id):
        assert event_id == "event-1"
        return {**CAPTURE_EVENT, "payload": {"title": "Manual note"}}

    def inspect_retention(self, *, event_id=None, source_id=None, session_id=None, as_of=None):
        assert event_id == "event-1"
        assert source_id is None
        assert session_id is None
        assert as_of == "2026-01-01T00:00:00Z"
        return {
            "as_of": as_of,
            "targets": [
                {
                    "event_id": "event-1",
                    "target_type": "raw_ref",
                    "target_id": "raw-1",
                    "retention_scope": "raw_capture",
                    "retention_class": "raw-expire",
                    "privacy_class": "private",
                    "eligible": True,
                    "eligibility_reason": "eligible",
                }
            ],
            "total": 1,
            "eligible": 1,
            "by_scope": {"raw_capture": {"total": 1, "eligible": 1}},
        }

    def expire_retention(
        self,
        *,
        event_id,
        delete_raw=False,
        delete_distilled=False,
        dry_run=True,
        reason=None,
        actor=None,
        as_of=None,
    ):
        assert event_id == "event-1"
        assert delete_raw is True
        assert delete_distilled is False
        assert dry_run is False
        assert reason == "expired"
        assert actor == "operator"
        assert as_of == "2026-01-01T00:00:00Z"
        return {
            "dry_run": False,
            "delete_raw": True,
            "delete_distilled": False,
            "operations": [{"status": "deleted", "retention_scope": "raw_capture"}],
            "audit_records": [{"operation": "retention.expired"}],
            "total": 1,
            "by_status": {"deleted": 1},
            "by_scope": {"raw_capture": {"deleted": 1}},
            "bytes_deleted": 12,
        }


class FakeCaptureContext:
    def __enter__(self):
        return FakeCaptureSurface()

    def __exit__(self, exc_type, exc, traceback):
        return False


def test_capture_cli_lists_events_and_event_detail(monkeypatch, capsys):
    import thoth

    monkeypatch.setattr(
        thoth,
        "open_capture_surface",
        lambda runtime_config: FakeCaptureContext(),
    )

    thoth.cmd_capture(
        SimpleNamespace(
            capture_action="events",
            source_id=None,
            session_id=None,
            limit=None,
            json=True,
        )
    )
    events_payload = json.loads(capsys.readouterr().out)
    event = events_payload["events"][0]
    assert event["raw_ref_ids"] == ["raw-1"]
    assert event["privacy_class"] == "private"
    assert event["retention_class"] == "default"
    assert event["security_state"]["state"] == "open"

    thoth.cmd_capture(
        SimpleNamespace(
            capture_action="event",
            event_id="event-1",
            json=True,
        )
    )
    detail_payload = json.loads(capsys.readouterr().out)
    assert detail_payload["event_id"] == "event-1"
    assert detail_payload["payload"] == {"title": "Manual note"}
    assert detail_payload["artifact_ids"] == ["artifact-1"]

    thoth.cmd_capture(
        SimpleNamespace(
            capture_action="retention",
            event_id="event-1",
            source_id=None,
            session_id=None,
            as_of="2026-01-01T00:00:00Z",
            json=True,
        )
    )
    retention_payload = json.loads(capsys.readouterr().out)
    assert retention_payload["targets"][0]["retention_class"] == "raw-expire"

    thoth.cmd_capture(
        SimpleNamespace(
            capture_action="expire",
            event_id="event-1",
            raw=True,
            distilled=False,
            execute=True,
            reason="expired",
            actor="operator",
            as_of="2026-01-01T00:00:00Z",
            json=True,
        )
    )
    expire_payload = json.loads(capsys.readouterr().out)
    assert expire_payload["delete_raw"] is True
    assert expire_payload["delete_distilled"] is False
    assert expire_payload["by_status"] == {"deleted": 1}
