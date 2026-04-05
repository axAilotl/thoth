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
