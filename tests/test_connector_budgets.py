from pathlib import Path

import pytest

from core.config import Config
from core.connector_budgets import ConnectorBudgetError, resolve_connector_budget
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.settings_summary import build_settings_runtime_summary


def test_connector_budget_config_rejects_unknown_fields():
    config = Config()
    config.data = {
        "connectors": {
            "budgets": {
                "defaults": {
                    "max_filez_per_run": 2,
                }
            }
        }
    }

    with pytest.raises(ConnectorBudgetError, match="unknown connector budget field"):
        resolve_connector_budget(config, "web_clipper")


def test_settings_summary_surfaces_connector_budgets(tmp_path: Path):
    config_data = {
        "paths": {
            "vault_dir": str(tmp_path / "vault"),
            "system_dir": ".thoth_system",
            "cache_dir": "cache",
            "raw_dir": "raw",
            "library_dir": "library",
            "wiki_dir": "wiki",
            "digests_dir": "_digests",
        },
        "database": {"path": "meta.db"},
        "connectors": {
            "budgets": {
                "per_connector": {
                    "web_clipper": {
                        "max_files_per_run": 3,
                        "max_bytes_per_run": 4096,
                    }
                }
            }
        },
        "sources": {
            "web_clipper": {"enabled": True},
            "pi_skills": {"enabled": False},
        },
    }

    cfg = Config()
    cfg.data = config_data
    layout = build_path_layout(cfg, project_root=tmp_path)
    db = MetadataDB(str(layout.database_path))
    summary = build_settings_runtime_summary(
        config_data, project_root=tmp_path, layout=layout, db=db
    )
    connector = next(
        item
        for item in summary["connectors"]["connectors"]
        if item["name"] == "web_clipper"
    )

    assert connector["budgets"]["limits"]["max_files_per_run"] == 3
    assert connector["budgets"]["limits"]["max_bytes_per_run"] == 4096
    grouped = summary["groups"]["sources_and_skills"]["connectors"]["items"]
    assert next(item for item in grouped if item["name"] == "web_clipper")[
        "budgets"
    ]["limits"]["max_files_per_run"] == 3
