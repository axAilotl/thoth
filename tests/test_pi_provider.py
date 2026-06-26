import asyncio

from core.llm_interface import LLMInterface, PiProvider


def test_pi_provider_invokes_locked_down_cli(monkeypatch):
    captured = {}

    class FakeProcess:
        returncode = 0

        async def communicate(self):
            return b"## Overview\nCompiled page [S1].\n", b""

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return FakeProcess()

    monkeypatch.setattr("core.llm_interface.shutil.which", lambda command: "/usr/bin/pi")
    monkeypatch.setattr(
        "core.llm_interface.asyncio.create_subprocess_exec",
        fake_create_subprocess_exec,
    )
    monkeypatch.setenv("OPEN_ROUTER_API_KEY", "secret-from-thoth-env")
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    provider = PiProvider(
        command="pi",
        pi_provider="openrouter",
        api_key_env="OPEN_ROUTER_API_KEY",
        model="z-ai/glm-5.2",
        timeout_seconds=10,
    )
    response = asyncio.run(
        provider.generate(
            "Compile the source packet.",
            system_prompt="You are the Archivist agent.",
        )
    )

    args = captured["args"]
    assert args[:7] == (
        "/usr/bin/pi",
        "--print",
        "--mode",
        "text",
        "--no-tools",
        "--no-session",
        "--no-context-files",
    )
    assert "--provider" in args
    assert "openrouter" in args
    assert "--model" in args
    assert "z-ai/glm-5.2" in args
    assert "--system-prompt" in args
    assert args[-1] == "Compile the source packet."
    assert captured["kwargs"]["env"]["OPENROUTER_API_KEY"] == "secret-from-thoth-env"
    assert response.provider == "pi"
    assert response.model == "z-ai/glm-5.2"
    assert response.content == "## Overview\nCompiled page [S1]."


def test_llm_interface_resolves_pi_archivist_route(monkeypatch):
    monkeypatch.setattr("core.llm_interface.shutil.which", lambda command: "/usr/bin/pi")

    interface = LLMInterface(
        {
            "providers": {
                "pi": {
                    "enabled": True,
                    "command": "pi",
                    "pi_provider": "openrouter",
                    "models": {
                        "default": {"id": "deepseek/deepseek-v4-flash"},
                        "archivist_agent": {
                            "id": "z-ai/glm-5.2",
                            "max_tokens": 6000,
                            "temperature": 0.2,
                        },
                    },
                }
            },
            "tasks": {
                "archivist": {
                    "enabled": True,
                    "fallback": [{"provider": "pi", "model": "archivist_agent"}],
                }
            },
        }
    )

    assert interface.resolve_task_route("archivist") == (
        "pi",
        "z-ai/glm-5.2",
        {"id": "z-ai/glm-5.2", "max_tokens": 6000, "temperature": 0.2},
    )
