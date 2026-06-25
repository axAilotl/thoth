"""Minimal MCP-style stdio server over Thoth agent surface tools."""

from __future__ import annotations

import json
import sys
from typing import Any, Callable

from .agent_surface import AgentSurfaceError, AgentSurfaceService, serialize_agent_payload


def _schema(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": required or [],
        "additionalProperties": False,
    }


TOOL_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "name": "wiki_query",
        "description": "Search the compiled wiki and return provenance for each hit.",
        "inputSchema": _schema(
            {
                "query": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "default": 10},
            },
            ["query"],
        ),
    },
    {
        "name": "list_artifacts",
        "description": "List queued or processed artifacts.",
        "inputSchema": _schema(
            {
                "artifact_type": {"type": "string"},
                "status": {"type": "string"},
                "source": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "default": 50},
            }
        ),
    },
    {
        "name": "get_artifact",
        "description": "Return a single artifact record and canonical payload.",
        "inputSchema": _schema(
            {"artifact_id": {"type": "string"}},
            ["artifact_id"],
        ),
    },
    {
        "name": "get_artifact_provenance",
        "description": "Return queue and source provenance for one artifact.",
        "inputSchema": _schema(
            {"artifact_id": {"type": "string"}},
            ["artifact_id"],
        ),
    },
    {
        "name": "list_connectors",
        "description": "List connector registry metadata.",
        "inputSchema": _schema({}),
    },
    {
        "name": "run_connector",
        "description": "Plan or execute a connector through the shared Thoth service layer.",
        "inputSchema": _schema(
            {
                "connector_name": {"type": "string"},
                "execute": {"type": "boolean", "default": False},
                "options": {"type": "object"},
            },
            ["connector_name"],
        ),
    },
    {
        "name": "connector_run_plan",
        "description": "Return a safe connector run plan without executing the connector.",
        "inputSchema": _schema(
            {
                "connector_name": {"type": "string"},
                "options": {"type": "object"},
            },
            ["connector_name"],
        ),
    },
    {
        "name": "research_missing_papers",
        "description": "Inspect missing paper candidates from the research graph.",
        "inputSchema": _schema(
            {
                "min_references": {"type": "integer", "minimum": 1, "default": 2},
                "limit": {"type": "integer", "minimum": 1, "default": 50},
            }
        ),
    },
)


class ThothMCPServer:
    """Small JSON-RPC server exposing Thoth tools with MCP-shaped payloads."""

    def __init__(self, service: AgentSurfaceService | None = None):
        self.service = service or AgentSurfaceService()
        self._tool_handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
            "wiki_query": self._wiki_query,
            "list_artifacts": self._list_artifacts,
            "get_artifact": self._get_artifact,
            "get_artifact_provenance": self._get_artifact_provenance,
            "list_connectors": self._list_connectors,
            "run_connector": self._run_connector,
            "connector_run_plan": self._connector_run_plan,
            "research_missing_papers": self._research_missing_papers,
        }

    def list_tools(self) -> dict[str, Any]:
        return {"tools": list(TOOL_DEFINITIONS)}

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        handler = self._tool_handlers.get(name)
        if handler is None:
            raise AgentSurfaceError(f"Unknown MCP tool: {name}")
        payload = serialize_agent_payload(handler(arguments or {}))
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                }
            ],
            "isError": False,
        }

    def handle_request(self, request: dict[str, Any]) -> dict[str, Any] | None:
        request_id = request.get("id")
        method = request.get("method")
        try:
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {"name": "thoth", "version": "0.1.0"},
                    "capabilities": {"tools": {}},
                }
            elif method == "tools/list":
                result = self.list_tools()
            elif method == "tools/call":
                params = request.get("params") or {}
                result = self.call_tool(
                    str(params.get("name") or ""),
                    params.get("arguments") or {},
                )
            elif method == "notifications/initialized":
                return None
            else:
                return self._error_response(request_id, -32601, f"Unknown method: {method}")
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except Exception as exc:
            return self._error_response(request_id, -32000, str(exc))

    def serve_stdio(self) -> None:
        for line in sys.stdin:
            if not line.strip():
                continue
            try:
                request = json.loads(line)
            except json.JSONDecodeError as exc:
                response = self._error_response(None, -32700, str(exc))
            else:
                response = self.handle_request(request)
            if response is None:
                continue
            sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
            sys.stdout.flush()

    def _wiki_query(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.query_wiki(
            str(arguments.get("query") or ""),
            limit=int(arguments.get("limit") or 10),
        )

    def _list_artifacts(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.list_artifacts(
            artifact_type=arguments.get("artifact_type"),
            status=arguments.get("status"),
            source=arguments.get("source"),
            limit=int(arguments.get("limit") or 50),
        )

    def _get_artifact(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.get_artifact(str(arguments.get("artifact_id") or ""))

    def _get_artifact_provenance(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.get_artifact_provenance(str(arguments.get("artifact_id") or ""))

    def _list_connectors(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.list_connectors()

    def _run_connector(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.run_connector(
            str(arguments.get("connector_name") or ""),
            execute=bool(arguments.get("execute", False)),
            options=arguments.get("options") or {},
        )

    def _connector_run_plan(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.run_connector(
            str(arguments.get("connector_name") or ""),
            execute=False,
            options=arguments.get("options") or {},
        )

    def _research_missing_papers(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.missing_papers(
            min_references=int(arguments.get("min_references") or 2),
            limit=int(arguments.get("limit") or 50),
        )

    def _error_response(
        self,
        request_id: Any,
        code: int,
        message: str,
    ) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }
