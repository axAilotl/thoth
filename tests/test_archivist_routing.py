from core.config import Config
from core.llm_interface import LLMInterface


def test_archivist_task_route_resolves_from_existing_task_router():
    interface = object.__new__(LLMInterface)
    interface.config = {
        "tasks": {
            "archivist": {
                "enabled": True,
                "fallback": [
                    {"provider": "missing"},
                    {"provider": "openrouter", "model": "archivist"},
                ],
            }
        }
    }
    interface.providers = {
        "openrouter": object(),
    }
    interface.provider_models = {
        "openrouter": {
            "default": {"id": "moonshot/default"},
            "archivist": {"id": "anthropic/claude-3-haiku", "max_tokens": 1200},
        }
    }

    route = interface.resolve_task_route("archivist")

    assert route == (
        "openrouter",
        "anthropic/claude-3-haiku",
        {"id": "anthropic/claude-3-haiku", "max_tokens": 1200},
    )


def test_config_validate_requires_archivist_fallback_when_enabled():
    cfg = Config()
    cfg.set("paths.vault_dir", "/tmp/vault")
    cfg.set("paths.system_dir", ".thoth_system")
    cfg.set("paths.cache_dir", "graphql_cache")
    cfg.set("database.path", "meta.db")
    cfg.set("llm.tasks.archivist.enabled", True)

    errors = cfg.validate()

    assert any(
        "llm.tasks.archivist.fallback must be a non-empty list" in error
        for error in errors
    )


def test_config_validate_requires_embedding_route_for_semantic_archivist_topics(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg = Config()
    cfg.set("paths.vault_dir", str(tmp_path / "vault"))
    cfg.set("paths.system_dir", ".thoth_system")
    cfg.set("paths.cache_dir", "graphql_cache")
    cfg.set("paths.images_dir", "images")
    cfg.set("paths.videos_dir", "videos")
    cfg.set("paths.media_dir", "media")
    cfg.set("database.path", "meta.db")
    cfg.set("paths.archivist_topics_file", "archivist_topics.yaml")

    (tmp_path / "bookmarks.json").write_text("[]\n", encoding="utf-8")
    (tmp_path / "cookies.json").write_text("{}\n", encoding="utf-8")
    cfg.set("paths.bookmarks_file", str(tmp_path / "bookmarks.json"))
    cfg.set("paths.cookies_file", str(tmp_path / "cookies.json"))
    (tmp_path / "archivist_topics.yaml").write_text(
        """
version: 1
topics:
  - id: companion
    title: Companion AI
    output_path: pages/topic-companion.md
    include_roots:
      - repos
    retrieval:
      mode: hybrid
      tag_mode: query
      term_mode: query
""".strip(),
        encoding="utf-8",
    )

    errors = cfg.validate()

    assert any(
        "llm.tasks.embedding must be enabled" in error
        for error in errors
    )
