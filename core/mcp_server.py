"""Minimal read-only MCP-style stdio server over Thoth agent surface tools."""

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
        "description": "Search wiki pages, artifacts, and capture events with provenance.",
        "inputSchema": _schema(
            {
                "query": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "default": 10},
                "include_quarantined": {"type": "boolean", "default": False},
                "result_types": {"type": "array", "items": {"type": "string"}},
                "source_types": {"type": "array", "items": {"type": "string"}},
                "source_ids": {"type": "array", "items": {"type": "string"}},
                "source_paths": {"type": "array", "items": {"type": "string"}},
                "artifact_types": {"type": "array", "items": {"type": "string"}},
                "event_types": {"type": "array", "items": {"type": "string"}},
                "wiki_kinds": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "array", "items": {"type": "string"}},
                "exclude_tags": {"type": "array", "items": {"type": "string"}},
                "security_statuses": {"type": "array", "items": {"type": "string"}},
                "min_trust_score": {"type": "number"},
                "time_after": {"type": "string"},
                "time_before": {"type": "string"},
                "created_after": {"type": "string"},
                "created_before": {"type": "string"},
                "updated_after": {"type": "string"},
                "updated_before": {"type": "string"},
                "use_embedding": {"type": "boolean", "default": False},
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
            {
                "artifact_id": {"type": "string"},
                "include_quarantined": {"type": "boolean", "default": False},
            },
            ["artifact_id"],
        ),
    },
    {
        "name": "get_artifact_provenance",
        "description": "Return queue and source provenance for one artifact.",
        "inputSchema": _schema(
            {
                "artifact_id": {"type": "string"},
                "include_quarantined": {"type": "boolean", "default": False},
            },
            ["artifact_id"],
        ),
    },
    {
        "name": "search_capture_events",
        "description": "Search capture events with cited provenance, security, and trust state.",
        "inputSchema": _schema(
            {
                "query": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "default": 10},
                "include_quarantined": {"type": "boolean", "default": False},
                "source_types": {"type": "array", "items": {"type": "string"}},
                "source_ids": {"type": "array", "items": {"type": "string"}},
                "source_paths": {"type": "array", "items": {"type": "string"}},
                "event_types": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "array", "items": {"type": "string"}},
                "exclude_tags": {"type": "array", "items": {"type": "string"}},
                "security_statuses": {"type": "array", "items": {"type": "string"}},
                "min_trust_score": {"type": "number"},
                "time_after": {"type": "string"},
                "time_before": {"type": "string"},
            },
            ["query"],
        ),
    },
    {
        "name": "get_capture_event",
        "description": "Return one capture event with cited provenance, security, and trust state.",
        "inputSchema": _schema(
            {
                "event_id": {"type": "string"},
                "include_quarantined": {"type": "boolean", "default": False},
            },
            ["event_id"],
        ),
    },
    {
        "name": "inspect_provenance",
        "description": "Inspect artifact or capture-event provenance without taking actions.",
        "inputSchema": _schema(
            {
                "target_type": {"type": "string"},
                "target_id": {"type": "string"},
                "include_quarantined": {"type": "boolean", "default": False},
            },
            ["target_type", "target_id"],
        ),
    },
    {
        "name": "list_connectors",
        "description": "List connector registry metadata.",
        "inputSchema": _schema({}),
    },
    {
        "name": "list_connector_runs",
        "description": "List connector run history, checkpoints, failure reasons, and retry state.",
        "inputSchema": _schema(
            {
                "connector_name": {"type": "string"},
                "status": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "default": 20},
            }
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
            "search_capture_events": self._search_capture_events,
            "get_capture_event": self._get_capture_event,
            "inspect_provenance": self._inspect_provenance,
            "list_connectors": self._list_connectors,
            "list_connector_runs": self._list_connector_runs,
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
            include_quarantined=bool(arguments.get("include_quarantined", False)),
            result_types=arguments.get("result_types"),
            source_types=arguments.get("source_types"),
            source_ids=arguments.get("source_ids"),
            source_paths=arguments.get("source_paths"),
            artifact_types=arguments.get("artifact_types"),
            event_types=arguments.get("event_types"),
            wiki_kinds=arguments.get("wiki_kinds"),
            tags=arguments.get("tags"),
            exclude_tags=arguments.get("exclude_tags"),
            security_statuses=arguments.get("security_statuses"),
            min_trust_score=arguments.get("min_trust_score"),
            time_after=arguments.get("time_after"),
            time_before=arguments.get("time_before"),
            created_after=arguments.get("created_after"),
            created_before=arguments.get("created_before"),
            updated_after=arguments.get("updated_after"),
            updated_before=arguments.get("updated_before"),
            use_embedding=bool(arguments.get("use_embedding", False)),
        )

    def _list_artifacts(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.list_artifacts(
            artifact_type=arguments.get("artifact_type"),
            status=arguments.get("status"),
            source=arguments.get("source"),
            limit=int(arguments.get("limit") or 50),
        )

    def _get_artifact(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.get_artifact(
            str(arguments.get("artifact_id") or ""),
            include_quarantined=bool(arguments.get("include_quarantined", False)),
        )

    def _get_artifact_provenance(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.get_artifact_provenance(
            str(arguments.get("artifact_id") or ""),
            include_quarantined=bool(arguments.get("include_quarantined", False)),
        )

    def _search_capture_events(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.search_capture_events(
            str(arguments.get("query") or ""),
            limit=int(arguments.get("limit") or 10),
            include_quarantined=bool(arguments.get("include_quarantined", False)),
            source_types=arguments.get("source_types"),
            source_ids=arguments.get("source_ids"),
            source_paths=arguments.get("source_paths"),
            event_types=arguments.get("event_types"),
            tags=arguments.get("tags"),
            exclude_tags=arguments.get("exclude_tags"),
            security_statuses=arguments.get("security_statuses"),
            min_trust_score=arguments.get("min_trust_score"),
            time_after=arguments.get("time_after"),
            time_before=arguments.get("time_before"),
        )

    def _get_capture_event(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.get_capture_event(
            str(arguments.get("event_id") or ""),
            include_quarantined=bool(arguments.get("include_quarantined", False)),
        )

    def _inspect_provenance(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.inspect_provenance(
            str(arguments.get("target_type") or ""),
            str(arguments.get("target_id") or ""),
            include_quarantined=bool(arguments.get("include_quarantined", False)),
        )

    def _list_connectors(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.list_connectors()

    def _list_connector_runs(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.service.list_connector_runs(
            connector_name=arguments.get("connector_name"),
            status=arguments.get("status"),
            limit=int(arguments.get("limit") or 20),
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
