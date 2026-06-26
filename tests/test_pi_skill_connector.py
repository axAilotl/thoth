import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.agent_surface import AgentSurfaceService
from core.config import Config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout


def _config(tmp_path: Path) -> Config:
    config = Config()
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", str(tmp_path / ".thoth_system"))
    config.set("paths.cache_dir", "cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    config.set(
        "llm.providers.pi",
        {
            "enabled": True,
            "type": "pi",
            "command": "pi",
            "pi_provider": "zai-coding-cn",
            "models": {
                "default": {"id": "glm-5-turbo"},
                "archivist_agent": {"id": "glm-5.2"},
            },
        },
    )
    config.set("sources.skill_outputs.enabled", True)
    config.set(
        "sources.pi_skills",
        {
            "enabled": True,
            "output_dir": str(tmp_path / "pi-output"),
            "default_provider": "pi",
            "default_model": "archivist_agent",
            "skills": [
                {
                    "id": "collect-notes",
                    "description": "Collect notes",
                    "artifact_types": ["transcript"],
                    "inputs": ["operator_prompt", "local_files:allowed_input_roots"],
                    "outputs": ["skill_output_envelopes", "artifact_queue:transcript"],
                    "auth": ["llm.providers.pi"],
                    "safety_mode": "no_tools_json",
                    "queue_behavior": "queues_artifacts",
                    "allowed_side_effects": [
                        "llm_api_call",
                        "local_file_read",
                        "local_file_write",
                        "artifact_queue_write",
                    ],
                    "source_name": "pi_skill:collect-notes",
                    "prompt": "Create transcript envelopes.",
                }
            ],
        },
    )
    return config


def test_pi_skills_dry_run_exposes_locked_down_command(tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)

    payload = service.run_connector(
        "pi_skills",
        options={"skill": "collect-notes"},
    )

    assert payload["status"] == "planned"
    command = payload["run_plan"]["route"]["command"]
    assert command[:6] == [
        "pi",
        "--print",
        "--mode",
        "text",
        "--no-tools",
        "--no-session",
    ]
    assert "--no-context-files" in command
    assert payload["run_plan"]["safety_mode"] == "no_tools_json"
    assert payload["run_plan"]["queue_behavior"] == "queues_artifacts"
    assert payload["run_plan"]["allowed_side_effects"] == [
        "llm_api_call",
        "local_file_read",
        "local_file_write",
        "artifact_queue_write",
    ]


def test_pi_skills_missing_manifest_controls_fail_closed(tmp_path: Path):
    config = _config(tmp_path)
    skills = config.get("sources.pi_skills.skills")
    skills[0].pop("safety_mode")
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    service = AgentSurfaceService(config, layout=layout, db=db)

    with pytest.raises(ValueError, match="requires safety_mode"):
        service.run_connector("pi_skills", options={"skill": "collect-notes"})


def test_pi_skills_execute_queues_valid_skill_output(monkeypatch, tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))

    class FakeLLMInterface:
        def __init__(self, _config):
            self.providers = {"pi": object()}

        async def generate(self, prompt, system_prompt=None, provider=None, model=None):
            assert provider == "pi"
            assert model == "archivist_agent"
            assert "Create transcript envelopes" in prompt
            assert "Return only JSON" in system_prompt
            return SimpleNamespace(
                content=json.dumps(
                    {
                        "artifacts": [
                            {
                                "artifact_type": "transcript",
                                "artifact_id": "pi-skill-note",
                                "payload": {
                                    "title": "Pi Skill Note",
                                    "raw_transcript": "Collected by Pi.",
                                    "processed_transcript": "Collected by Pi.",
                                },
                            }
                        ]
                    }
                ),
                error=None,
            )

    monkeypatch.setattr(
        "collectors.pi_skill_connector.LLMInterface",
        FakeLLMInterface,
    )
    service = AgentSurfaceService(config, layout=layout, db=db)

    payload = service.run_connector(
        "pi_skills",
        execute=True,
        options={"skill": "collect-notes", "prompt": "Collect this."},
    )

    assert payload["status"] == "completed"
    assert payload["result"]["queued_count"] == 1
    entry = db.get_ingestion_entry("pi-skill-note")
    assert entry is not None
    assert entry.source == "pi_skill:collect-notes"
    queued_payload = json.loads(entry.payload_json)
    assert queued_payload["custom_metadata"]["skill_source_name"] == (
        "pi_skill:collect-notes"
    )


def test_pi_skills_rejects_direct_wiki_write_fields(monkeypatch, tmp_path: Path):
    config = _config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))

    class BadLLMInterface:
        def __init__(self, _config):
            self.providers = {"pi": object()}

        async def generate(self, *args, **kwargs):
            return SimpleNamespace(
                content=json.dumps(
                    {
                        "artifact_type": "transcript",
                        "payload": {
                            "title": "Bad",
                            "raw_transcript": "Bad",
                            "wiki_path": "wiki/pages/bad.md",
                        },
                    }
                ),
                error=None,
            )

    monkeypatch.setattr("collectors.pi_skill_connector.LLMInterface", BadLLMInterface)
    service = AgentSurfaceService(config, layout=layout, db=db)

    with pytest.raises(ValueError, match="direct wiki write fields"):
        service.run_connector(
            "pi_skills",
            execute=True,
            options={"skill": "collect-notes"},
        )

    assert db.list_ingestion_entries(limit=10) == []
