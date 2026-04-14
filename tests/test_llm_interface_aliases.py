from core.llm_interface import LLMInterface, OpenRouterProvider


def test_llm_interface_supports_aliased_provider_types(monkeypatch):
    monkeypatch.setenv("OPEN_ROUTER_API_KEY", "test-token")

    llm = LLMInterface(
        {
            "providers": {
                "openrouter at home": {
                    "enabled": True,
                    "type": "openrouter",
                    "base_url": "https://inference.local.vega.nyc/v1",
                    "models": {
                        "default": {"id": "openrouter/google/gemini-3-flash-preview"},
                        "embedding": {"id": "openrouter/openai/text-embedding-3-small"},
                        "GLM": {"id": "openrouter/z-ai/glm-5"},
                    },
                }
            },
            "tasks": {
                "summary": {
                    "enabled": True,
                    "fallback": [{"provider": "openrouter at home"}],
                },
                "embedding": {
                    "enabled": True,
                    "fallback": [{"provider": "openrouter at home", "model": "embedding"}],
                },
                "archivist": {
                    "enabled": True,
                    "fallback": [{"provider": "openrouter at home", "model": "GLM"}],
                },
            },
        }
    )

    assert isinstance(llm.providers["openrouter at home"], OpenRouterProvider)
    assert llm.resolve_task_route("summary") == (
        "openrouter at home",
        "openrouter/google/gemini-3-flash-preview",
        {"id": "openrouter/google/gemini-3-flash-preview"},
    )
    assert llm.resolve_task_route("embedding") == (
        "openrouter at home",
        "openrouter/openai/text-embedding-3-small",
        {"id": "openrouter/openai/text-embedding-3-small"},
    )
    assert llm.resolve_task_route("archivist") == (
        "openrouter at home",
        "openrouter/z-ai/glm-5",
        {"id": "openrouter/z-ai/glm-5"},
    )
