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
        "sources": {
            "web_clipper": {
                "enabled": True,
                "note_dirs": ["web-clipper/notes"],
                "attachment_dirs": ["web-clipper/assets"],
                "note_extensions": [".md"],
                "attachment_extensions": [".png", ".pdf"],
            }
        },
    }


def test_settings_summary_reports_resolved_layout_and_archivist_topics(tmp_path: Path):
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

    assert summary["layout"]["raw_root"] == str(tmp_path / "vault" / "raw")
    assert summary["layout"]["wiki_root"] == str(tmp_path / "wiki")
    assert summary["layout"]["system_root"] == str(tmp_path / ".thoth_system")

    assert summary["archivist"]["exists"] is True
    assert summary["archivist"]["topic_count"] == 2
    assert summary["archivist"]["topics"] == [
        "companion-ai-research",
        "model-evals-and-benchmarks",
    ]

    assert summary["web_clipper"]["enabled"] is True
    assert summary["web_clipper"]["watch_dirs"] == [
        str(tmp_path / "vault" / "raw" / "web-clipper" / "notes"),
        str(tmp_path / "vault" / "raw" / "web-clipper" / "assets"),
    ]


def test_settings_summary_surfaces_archivist_and_web_clipper_errors(tmp_path: Path):
    config_data = make_config_data(tmp_path)
    config_data["paths"]["archivist_topics_file"] = "topics/missing.yaml"
    config_data["sources"]["web_clipper"]["note_dirs"] = [str(tmp_path / "outside")]

    summary = build_settings_runtime_summary(config_data, project_root=tmp_path)

    assert "Archivist topic registry file not found" in summary["archivist"]["error"]
    assert "must stay inside the raw source root" in summary["web_clipper"]["error"]
