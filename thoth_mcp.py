#!/usr/bin/env python3
"""Run Thoth's MCP-style stdio server."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from core.config import config
from core.mcp_server import ThothMCPServer
from core.runtime_composition import (
    resolve_runtime_database,
    teardown_runtime_services,
)


def main() -> None:
    resolve_runtime_database(config)
    try:
        ThothMCPServer().serve_stdio()
    finally:
        teardown_runtime_services()


if __name__ == "__main__":
    main()
