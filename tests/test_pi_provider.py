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
                    "type": "pi",
                    "command": "pi",
                    "pi_provider": "zai-coding-cn",
                    "models": {
                        "default": {"id": "glm-5-turbo"},
                        "archivist_agent": {
                            "id": "glm-5.2",
                            "max_tokens": 6000,
                            "temperature": 0.2,
                        },
                    },
                },
                "pi_openrouter": {
                    "enabled": True,
                    "type": "pi",
                    "command": "pi",
                    "pi_provider": "openrouter",
                    "api_key_env": "OPEN_ROUTER_API_KEY",
                    "models": {
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
                    "fallback": [
                        {"provider": "pi", "model": "archivist_agent"},
                        {"provider": "pi_openrouter", "model": "archivist_agent"},
                    ],
                }
            },
        }
    )

    assert interface.resolve_task_route("archivist") == (
        "pi",
        "glm-5.2",
        {"id": "glm-5.2", "max_tokens": 6000, "temperature": 0.2},
    )


def test_pi_provider_can_install_when_missing(monkeypatch):
    calls = {"which": 0, "install": None}

    def fake_which(command):
        calls["which"] += 1
        return None if calls["which"] == 1 else "/usr/local/bin/pi"

    def fake_run(command, **kwargs):
        calls["install"] = (command, kwargs)

    monkeypatch.setattr("core.llm_interface.shutil.which", fake_which)
    monkeypatch.setattr("core.llm_interface.subprocess.run", fake_run)

    provider = PiProvider(
        command="pi",
        pi_provider="openrouter",
        model="z-ai/glm-5.2",
        install_if_missing=True,
        install_command=["npm", "install", "-g", "@earendil-works/pi-coding-agent"],
    )

    assert provider.command == "/usr/local/bin/pi"
    assert calls["install"][0] == [
        "npm",
        "install",
        "-g",
        "@earendil-works/pi-coding-agent",
    ]
    assert calls["install"][1]["check"] is True
