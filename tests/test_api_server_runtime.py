from core.api_server_runtime import resolve_api_server_options


def test_resolve_api_server_options_uses_defaults():
    options = resolve_api_server_options({})

    assert options == {
        "host": "0.0.0.0",
        "port": 8090,
        "reload": True,
    }


def test_resolve_api_server_options_respects_explicit_env():
    options = resolve_api_server_options(
        {
            "THOTH_API_HOST": "127.0.0.1",
            "THOTH_API_PORT": "8002",
            "PORT": "8001",
            "THOTH_API_RELOAD": "false",
        }
    )

    assert options == {
        "host": "127.0.0.1",
        "port": 8002,
        "reload": False,
    }


def test_resolve_api_server_options_ignores_generic_port_env():
    options = resolve_api_server_options({"PORT": "8001"})

    assert options == {
        "host": "0.0.0.0",
        "port": 8090,
        "reload": True,
    }


def test_resolve_api_server_options_rejects_invalid_port():
    try:
        resolve_api_server_options({"THOTH_API_PORT": "not-a-port"})
    except ValueError as exc:
        assert "THOTH_API_PORT must be an integer" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected invalid port to raise ValueError")
