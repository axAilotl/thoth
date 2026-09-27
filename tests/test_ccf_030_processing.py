"""Metadata follows native success and cannot turn it into a processing failure."""

from types import SimpleNamespace
from pathlib import Path

from core.ccf_processing import record_processed_artifact
from core.config import Config
from core.path_layout import build_path_layout


def test_completed_output_links_to_its_source_and_failure_is_reported(tmp_path, caplog):
    from core.ccf_metadata import open_metadata_service

    config = Config()
    config.load_from_file(str(Path(__file__).resolve().parents[1] / "config.example.json"))
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", str(tmp_path / "system"))
    config.set("database.ccf", {"enabled": True, "device": "test", "path": "ccf.sqlite3"})
    layout = build_path_layout(config)
    layout.vault_root.mkdir(parents=True, exist_ok=True)
    (layout.vault_root / "summary.md").write_text("Generated summary")
    artifact = SimpleNamespace(output_paths={"summary": "summary.md", "transcript": "missing.md"},
                               to_dict=lambda: {"title": "Recording", "source": "test"})
    entry = SimpleNamespace(artifact_id="transcript-1", artifact_type="transcript")
    record_processed_artifact(config, layout, entry, artifact)
    service = open_metadata_service(config, layout=layout)
    source = service.store.find("artifact:transcript-1")
    summary = next(obj for obj in service.store.objects() if obj.get("loc") == "vault:summary.md" and obj["kind"] == "record")
    assert summary["status"] == "inferred"
    assert service.record_file(layout.vault_root / "summary.md")["status"] == "inferred"
    assert any(obj.get("from") == summary["id"] and obj.get("to") == source["id"] and obj.get("rel") == "derived_from"
               and obj.get("status") == "inferred" for obj in service.store.objects())
    assert "missing.md" in caplog.text
