from pathlib import Path

import pytest

from core.config import Config
from core.path_layout import build_path_layout
from collectors.web_clipper_layout import build_web_clipper_contract


def make_config(tmp_path: Path) -> Config:
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("database.path", "meta.db")
    config.set("sources.web_clipper.note_dirs", ["Clippings", "clippings"])
    config.set("sources.web_clipper.attachment_dirs", ["assets"])
    return config


def test_web_clipper_contract_resolves_inside_vault_root(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)

    contract = build_web_clipper_contract(config, layout=layout)

    assert contract.note_dirs == (
        tmp_path / "vault" / "Clippings",
        tmp_path / "vault" / "clippings",
    )
    assert contract.attachment_dirs == (
        tmp_path / "vault" / "assets",
    )
    assert contract.watch_dirs == contract.note_dirs + contract.attachment_dirs


def test_web_clipper_contract_classifies_notes_and_attachments(tmp_path: Path):
    config = make_config(tmp_path)
    layout = build_path_layout(config, project_root=tmp_path)
    contract = build_web_clipper_contract(config, layout=layout)

    note_path = contract.note_dirs[0] / "clip.md"
    attachment_path = contract.attachment_dirs[0] / "image.png"
    ignored_path = contract.note_dirs[0] / "notes.txt"

    assert contract.classify_path(note_path) == "note"
    assert contract.classify_path(attachment_path) == "attachment"
    assert contract.classify_path(ignored_path) == "ignored"


def test_web_clipper_contract_requires_configured_directories(tmp_path: Path):
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("database.path", "meta.db")

    with pytest.raises(ValueError, match="sources.web_clipper.note_dirs"):
        build_web_clipper_contract(config, layout=build_path_layout(config, project_root=tmp_path))


def test_web_clipper_contract_rejects_outside_vault_paths(tmp_path: Path):
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("database.path", "meta.db")
    config.set("sources.web_clipper.note_dirs", [str(tmp_path / "outside")])
    config.set("sources.web_clipper.attachment_dirs", ["assets"])

    with pytest.raises(ValueError, match="must stay inside the vault root"):
        build_web_clipper_contract(config, layout=build_path_layout(config, project_root=tmp_path))


def test_web_clipper_contract_allows_note_only_configuration(tmp_path: Path):
    config = Config()
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("database.path", "meta.db")
    config.set("sources.web_clipper.note_dirs", ["Clippings"])

    contract = build_web_clipper_contract(
        config,
        layout=build_path_layout(config, project_root=tmp_path),
    )

    assert contract.note_dirs == (tmp_path / "vault" / "Clippings",)
    assert contract.attachment_dirs == ()
