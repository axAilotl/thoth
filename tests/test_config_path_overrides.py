"""Configured destinations must survive the same layering used by the server."""
import json

from core.config import Config


def test_later_path_overrides_reach_processor_settings(tmp_path):
    layers = [
        {"paths": {"vault_dir": "knowledge_vault", "system_dir": ".thoth_system",
                   "bookmarks_file": "bookmarks.json", "media_dir": "media"}},
        {"paths": {"vault_dir": "/data/vault", "media_dir": "/data/media"}},
        {"paths": {"system_dir": "/runtime/system",
                   "bookmarks_file": "/runtime/system/bookmarks.json"}},
    ]
    files = []
    for index, layer in enumerate(layers):
        path = tmp_path / f"config-{index}.json"
        path.write_text(json.dumps(layer))
        files.append(str(path))
    config = Config()
    config.reload(files)
    for key, expected in {"vault_dir": "/data/vault", "media_dir": "/data/media",
                          "system_dir": "/runtime/system",
                          "bookmarks_file": "/runtime/system/bookmarks.json"}.items():
        assert config.get(f"paths.{key}") == expected
        assert config.get(key) == expected
