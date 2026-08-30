"""Runtime-composition regression tests for the MCP stdio entrypoint."""

from __future__ import annotations

import io
import sys
from copy import deepcopy
from pathlib import Path

import pytest

import thoth_mcp
from core.config import config
from core.metadata_db import get_metadata_db
from core.path_layout import build_path_layout
from core.runtime_composition import reset_runtime_database


@pytest.fixture(autouse=True)
def isolated_runtime_config(tmp_path: Path, monkeypatch):
    original = deepcopy(config.data)
    reset_runtime_database()
    monkeypatch.chdir(tmp_path)
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    yield
    reset_runtime_database()
    config.data = original


def test_mcp_main_composes_database_and_tears_down_on_eof(
    monkeypatch,
):
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    layout = build_path_layout(config)

    thoth_mcp.main()

    assert layout.database_path.exists()
    with pytest.raises(RuntimeError, match="No metadata database has been registered"):
        get_metadata_db()


def test_mcp_main_tears_down_when_stdio_server_fails(monkeypatch):
    class FailingServer:
        def serve_stdio(self) -> None:
            assert get_metadata_db() is not None
            raise RuntimeError("stdio failed")

    monkeypatch.setattr(thoth_mcp, "ThothMCPServer", FailingServer)

    with pytest.raises(RuntimeError, match="stdio failed"):
        thoth_mcp.main()
    with pytest.raises(RuntimeError, match="No metadata database has been registered"):
        get_metadata_db()
