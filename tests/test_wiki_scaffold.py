from pathlib import Path

import pytest

from core.config import Config
from core.wiki_scaffold import append_wiki_log_entry, ensure_wiki_scaffold


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


def test_ensure_wiki_scaffold_seeds_root_files(tmp_path: Path):
    scaffold = ensure_wiki_scaffold(make_config(tmp_path), project_root=tmp_path)

    assert scaffold.root == tmp_path / "wiki"
    assert scaffold.pages_dir.is_dir()
    assert scaffold.index_path.is_file()
    assert scaffold.log_path.is_file()

    index = scaffold.index_path.read_text(encoding="utf-8")
    log = scaffold.log_path.read_text(encoding="utf-8")

    assert "thoth_type: wiki_index" in index
    assert "# Thoth Wiki" in index
    assert "thoth_type: wiki_log" in log
    assert "append_only: true" in log


def test_ensure_wiki_scaffold_preserves_existing_files(tmp_path: Path):
    scaffold = ensure_wiki_scaffold(make_config(tmp_path), project_root=tmp_path)
    scaffold.index_path.write_text("custom index\n", encoding="utf-8")
    scaffold.log_path.write_text("custom log\n", encoding="utf-8")

    ensure_wiki_scaffold(make_config(tmp_path), project_root=tmp_path)

    assert scaffold.index_path.read_text(encoding="utf-8") == "custom index\n"
    assert scaffold.log_path.read_text(encoding="utf-8") == "custom log\n"


def test_append_wiki_log_entry_appends_to_maintenance_log(tmp_path: Path):
    scaffold = ensure_wiki_scaffold(make_config(tmp_path), project_root=tmp_path)

    append_wiki_log_entry(
        scaffold,
        "Seeded the compiled wiki scaffold",
        timestamp="2026-04-04T00:00:00Z",
    )

    log = scaffold.log_path.read_text(encoding="utf-8")
    assert "Seeded the compiled wiki scaffold" in log
    assert "2026-04-04T00:00:00Z" in log


def test_append_wiki_log_entry_rejects_empty_messages(tmp_path: Path):
    scaffold = ensure_wiki_scaffold(make_config(tmp_path), project_root=tmp_path)

    with pytest.raises(ValueError, match="cannot be empty"):
        append_wiki_log_entry(scaffold, "   ")
