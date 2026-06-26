from pathlib import Path

from core.settings_summary import build_settings_runtime_summary


def make_config_data(tmp_path: Path) -> dict:
    return {
        "paths": {
            "vault_dir": str(tmp_path / "vault"),
            "system_dir": ".thoth_system",
            "cache_dir": "graphql_cache",
            "raw_dir": "raw",
            "library_dir": "library",
            "wiki_dir": "wiki",
            "digests_dir": "_digests",
            "archivist_topics_file": "archivist_topics.yaml",
        },
        "database": {
            "path": "meta.db",
        },
        "llm": {
            "providers": {
                "openai": {
                    "enabled": True,
                    "models": {"default": {"id": "gpt-4.1-mini"}},
                    "api_key_env": "OPENAI_API_KEY",
                }
            },
            "tasks": {
                "summary": {
                    "enabled": True,
                    "fallback": [{"provider": "openai", "model": "default"}],
                }
            },
        },
        "sources": {
            "pi_skills": {
                "enabled": True,
                "output_dir": ".thoth_system/skill_outputs/pi",
                "default_provider": "pi",
                "default_model": "archivist_agent",
                "skills": [
                    {
                        "id": "knowledge-collation",
                        "description": "Collate user data",
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
                    }
                ],
            },
            "web_clipper": {
                "enabled": True,
                "note_dirs": ["imports/notes"],
                "attachment_dirs": ["imports/assets"],
                "note_extensions": [".md"],
                "attachment_extensions": [".png", ".pdf"],
            }
        },
    }


def test_settings_summary_reports_resolved_layout_and_archivist_topics(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("CLIENT_ID", "not-the-x-api-client-id")
    config_data = make_config_data(tmp_path)
    (tmp_path / "archivist_topics.yaml").write_text(
        """
version: 1
topics:
  - id: companion-ai-research
    title: Companion AI Research
    output_path: pages/topic-companion-ai-research.md
    include_roots:
      - tweets
  - id: model-evals-and-benchmarks
    title: Model Evals and Benchmarks
    output_path: pages/topic-model-evals-and-benchmarks.md
    include_roots:
      - papers
""".strip(),
        encoding="utf-8",
    )

    summary = build_settings_runtime_summary(config_data, project_root=tmp_path)

    assert summary["layout"]["wiki_root"] == str(tmp_path / "wiki")
    assert summary["layout"]["system_root"] == str(tmp_path / ".thoth_system")

    assert summary["archivist"]["exists"] is True
    assert summary["archivist"]["topic_count"] == 2
    assert summary["archivist"]["corpus"]["document_count"] == 0
    assert summary["archivist"]["topics"] == [
        "companion-ai-research",
        "model-evals-and-benchmarks",
    ]

    assert summary["web_clipper"]["enabled"] is True
    assert summary["web_clipper"]["watch_dirs"] == [
        str(tmp_path / "vault" / "imports" / "notes"),
        str(tmp_path / "vault" / "imports" / "assets"),
    ]
    assert summary["connectors"]["total"] == 9
    assert [item["name"] for item in summary["connectors"]["connectors"]] == [
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
    web_clipper_connector = next(
        item
        for item in summary["connectors"]["connectors"]
        if item["name"] == "web_clipper"
    )
    assert web_clipper_connector["enabled"] is True

    assert summary["groups"]["providers"]["enabled"] == ["openai"]
    assert summary["groups"]["providers"]["tasks"]["summary"] == {
        "enabled": True,
        "fallback_providers": ["openai"],
    }
    assert summary["groups"]["connectors"]["total"] == 9
    assert summary["groups"]["skills"]["total"] == 1
    assert summary["groups"]["skills"]["safety_mode"] == "no_tools_json"
    assert summary["groups"]["skills"]["items"][0]["inputs"] == [
        "operator_prompt",
        "local_files:allowed_input_roots",
    ]
    assert summary["groups"]["skills"]["items"][0]["queue_behavior"] == (
        "queues_artifacts"
    )
    assert "web_clipper" in summary["groups"]["connectors"]["enabled"]
    x_api_connector = next(
        item
        for item in summary["groups"]["connectors"]["items"]
        if item["name"] == "x_api"
    )
    assert "sources.x_api.client_id" in x_api_connector["config_keys"]
    assert x_api_connector["inputs"] == ["remote_api:x_bookmarks"]
    assert x_api_connector["queue_behavior"] == "queues_artifacts"
    assert "artifact_queue_write" in x_api_connector["allowed_side_effects"]
    assert x_api_connector["auth_status"]["keys"] == [
        "sources.x_api.client_id",
        "sources.x_api.redirect_uri",
        "x_api_token_bundle",
    ]
    assert "sources.x_api.client_id" in x_api_connector["auth_status"]["missing"]
    assert summary["groups"]["storage"]["raw_root"] == str(tmp_path / "vault" / "raw")
    assert summary["groups"]["wiki"]["wiki_root"] == str(tmp_path / "wiki")
    assert summary["groups"]["wiki"]["okf_target"] == "v0.1"
    assert summary["groups"]["automation"]["jobs"]["social_sync"]["interval_hours"] == 8


def test_settings_summary_surfaces_archivist_and_web_clipper_errors(tmp_path: Path):
    config_data = make_config_data(tmp_path)
    config_data["paths"]["archivist_topics_file"] = "topics/missing.yaml"
    config_data["sources"]["web_clipper"]["note_dirs"] = [str(tmp_path / "outside")]

    summary = build_settings_runtime_summary(config_data, project_root=tmp_path)

    assert "Archivist topic registry file not found" in summary["archivist"]["error"]
    assert "must stay inside the vault root" in summary["web_clipper"]["error"]


def test_settings_summary_hides_web_clipper_watch_dirs_when_disabled(tmp_path: Path):
    config_data = make_config_data(tmp_path)
    config_data["sources"]["web_clipper"]["enabled"] = False

    summary = build_settings_runtime_summary(config_data, project_root=tmp_path)

    assert summary["web_clipper"]["configured"] is True
    assert summary["web_clipper"]["watch_dirs"] == [
        str(tmp_path / "vault" / "imports" / "notes"),
        str(tmp_path / "vault" / "imports" / "assets"),
    ]
    web_clipper_connector = next(
        item
        for item in summary["connectors"]["connectors"]
        if item["name"] == "web_clipper"
    )
    assert web_clipper_connector["enabled"] is False


def test_settings_summary_reports_invalid_pi_skill_manifest(tmp_path: Path):
    config_data = make_config_data(tmp_path)
    config_data["sources"]["pi_skills"]["skills"][0].pop("allowed_side_effects")

    summary = build_settings_runtime_summary(config_data, project_root=tmp_path)

    assert summary["groups"]["skills"]["total"] == 0
    assert "requires allowed_side_effects" in summary["groups"]["skills"]["error"]
