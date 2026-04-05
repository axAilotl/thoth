from copy import deepcopy
from pathlib import Path

import pytest

from core.config import config
from core.download_tracker import DownloadTracker
from core.llm_cache import LLMCache
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from processors.cache_loader import CacheLoader


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


def _configure_runtime_paths(tmp_path: Path) -> None:
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")


def test_runtime_helpers_pin_state_under_thoth_system(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    layout = build_path_layout(config)

    assert layout.cache_root == tmp_path / ".thoth_system" / "graphql_cache"
    assert layout.llm_cache_root == tmp_path / ".thoth_system" / "llm_cache"
    assert layout.database_path == tmp_path / ".thoth_system" / "meta.db"
    assert layout.temp_root == tmp_path / ".thoth_system" / "tmp"
    assert layout.download_tracking_file == (
        tmp_path / ".thoth_system" / "download_tracking.json"
    )
    assert layout.realtime_bookmarks_file == (
        tmp_path / ".thoth_system" / "realtime_bookmarks.json"
    )
    assert layout.log_file == tmp_path / ".thoth_system" / "thoth.log"

    assert LLMCache().cache_dir == layout.llm_cache_root
    assert DownloadTracker().tracking_file == layout.download_tracking_file
    assert CacheLoader().cache_dir == layout.cache_root
    assert MetadataDB().db_path == layout.database_path
