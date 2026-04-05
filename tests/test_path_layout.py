from pathlib import Path

import pytest

from core.config import Config
from core.path_layout import build_path_layout


def make_config(tmp_path: Path) -> Config:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    return config


def test_build_path_layout_resolves_relative_roots(tmp_path: Path):
    config = make_config(tmp_path)

    layout = build_path_layout(config, project_root=tmp_path)

    assert layout.vault_root == tmp_path / "vault"
    assert layout.system_root == tmp_path / ".thoth_system"
    assert layout.auth_root == tmp_path / ".thoth_system" / "auth"
    assert layout.raw_root == tmp_path / "vault" / "raw"
    assert layout.library_root == tmp_path / "vault" / "library"
    assert layout.wiki_root == tmp_path / "wiki"
    assert layout.digests_root == tmp_path / "vault" / "_digests"
    assert layout.cache_root == tmp_path / ".thoth_system" / "graphql_cache"
    assert layout.llm_cache_root == tmp_path / ".thoth_system" / "llm_cache"
    assert layout.database_path == tmp_path / ".thoth_system" / "meta.db"
    assert layout.download_tracking_file == (
        tmp_path / ".thoth_system" / "download_tracking.json"
    )
    assert layout.realtime_bookmarks_file == (
        tmp_path / ".thoth_system" / "realtime_bookmarks.json"
    )
    assert layout.log_file == tmp_path / ".thoth_system" / "thoth.log"


def test_build_path_layout_ensures_directories(tmp_path: Path):
    config = make_config(tmp_path)

    layout = build_path_layout(config, project_root=tmp_path)
    layout.ensure_directories()

    assert layout.system_root.is_dir()
    assert layout.auth_root.is_dir()
    assert layout.raw_root.is_dir()
    assert layout.library_root.is_dir()
    assert layout.wiki_root.is_dir()
    assert layout.cache_root.is_dir()
    assert layout.llm_cache_root.is_dir()
    assert layout.database_path.parent.is_dir()


def test_build_path_layout_requires_system_dir(tmp_path: Path):
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))

    with pytest.raises(ValueError, match="paths.system_dir"):
        build_path_layout(config, project_root=tmp_path)


def test_build_path_layout_requires_paths_keys(tmp_path: Path):
    config = Config()
    config.set("vault_dir", str(tmp_path / "vault"))
    config.set("system_dir", ".thoth_system")

    with pytest.raises(ValueError, match="paths.vault_dir"):
        build_path_layout(config, project_root=tmp_path)


def test_build_path_layout_requires_database_path(tmp_path: Path):
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")

    with pytest.raises(ValueError, match="database.path"):
        build_path_layout(config, project_root=tmp_path)
