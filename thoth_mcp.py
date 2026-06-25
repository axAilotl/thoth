#!/usr/bin/env python3
"""Run Thoth's MCP-style stdio server."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from core.mcp_server import ThothMCPServer


def main() -> None:
    ThothMCPServer().serve_stdio()


if __name__ == "__main__":
    main()
