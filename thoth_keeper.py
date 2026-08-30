#!/usr/bin/env python3
"""Local stdio entrypoint for the supervised read-only Keeper profile.

Usage::

    python thoth_keeper.py \\
        --db ./.thoth_system/meta.db \\
        --roots vault/transcripts,raw/cissa

This exposes a small MCP-style JSON-RPC stdio server with only read-only tools:
``keeper_readiness`` and ``keeper_query``. It never constructs a ``MetadataDB``,
runs migrations, starts ingestion, loads providers or connectors, accesses the
network, or exposes mutating tools.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

from keeper_profile import (
    KeeperPassage,
    KeeperProfile,
    KeeperProfileConfig,
    KeeperProfileError,
)


TOOL_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "name": "keeper_readiness",
        "description": (
            "Report whether the keeper storage and index are ready, stale, "
            "or unavailable without mutating state."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "keeper_query",
        "description": (
            "Search allowed archivist corpus roots and return bounded passages "
            "with stable artifact IDs, selectors, trust, and provenance."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 10,
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
)


class KeeperProfileMCPServer:
    """MCP-shaped JSON-RPC server for the read-only keeper profile."""

    def __init__(self, profile: KeeperProfile) -> None:
        self.profile = profile
        self._active: dict[str | int, threading.Event] = {}
        self._active_lock = threading.Lock()
        self._output_lock = threading.Lock()

    def list_tools(self) -> dict[str, Any]:
        return {"tools": list(TOOL_DEFINITIONS)}

    def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        *,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        if arguments is None:
            args: dict[str, Any] = {}
        elif type(arguments) is dict:
            args = arguments
        else:
            raise KeeperProfileError("tool arguments must be an object")
        if name == "keeper_readiness":
            if args:
                raise KeeperProfileError("keeper_readiness accepts no arguments")
            result = self.profile.readiness()
            payload = {
                "status": result.status,
                "document_count": result.document_count,
                "last_indexed_at": result.last_indexed_at,
                "reason": result.reason,
            }
        elif name == "keeper_query":
            unknown = set(args) - {"query", "limit"}
            if unknown:
                raise KeeperProfileError(
                    f"unknown keeper_query arguments: {', '.join(sorted(unknown))}"
                )
            query_value = args.get("query")
            if not isinstance(query_value, str):
                raise KeeperProfileError("query must be a string")
            query_text = query_value.strip()
            if not query_text:
                raise KeeperProfileError("query is required")
            limit = args.get("limit", 10)
            result = self.profile.query(
                query_text,
                limit=limit,
                cancel_event=cancel_event,
            )
            payload = {
                "query": result.query,
                "status": result.status,
                "total": result.total,
                "readiness": result.readiness,
                "passages": [_passage_to_dict(p) for p in result.passages],
            }
        else:
            raise KeeperProfileError(f"Unknown tool: {name}")
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                }
            ],
            "isError": False,
        }

    def handle_request(
        self,
        request: dict[str, Any],
        *,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any] | None:
        if type(request) is not dict:
            return self._error_response(None, -32600, "request must be an object")
        request_id = request.get("id")
        method = request.get("method")
        try:
            if request.get("jsonrpc") != "2.0" or not isinstance(method, str):
                raise KeeperProfileError("invalid JSON-RPC request")
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {"name": "thoth-keeper", "version": "0.1.0"},
                    "capabilities": {"tools": {}},
                }
            elif method == "tools/list":
                result = self.list_tools()
            elif method == "tools/call":
                params = request.get("params") or {}
                if type(params) is not dict:
                    raise KeeperProfileError("tools/call params must be an object")
                result = self.call_tool(
                    str(params.get("name") or ""),
                    params.get("arguments"),
                    cancel_event=cancel_event,
                )
            elif method == "notifications/cancelled":
                params = request.get("params") or {}
                if type(params) is not dict:
                    raise KeeperProfileError(
                        "notifications/cancelled params must be an object"
                    )
                self._cancel_request(params.get("requestId"))
                return None
            elif method == "notifications/initialized":
                return None
            else:
                return self._error_response(
                    request_id, -32601, f"Unknown method: {method}"
                )
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except KeeperProfileError as exc:
            return self._error_response(request_id, -32000, str(exc))
        except Exception as exc:
            return self._error_response(
                request_id, -32000, f"Unexpected error: {exc}"
            )

    def serve_stdio(self) -> None:
        workers: list[threading.Thread] = []
        for line in sys.stdin:
            if not line.strip():
                continue
            try:
                request = json.loads(line)
            except json.JSONDecodeError as exc:
                response = self._error_response(None, -32700, str(exc))
            else:
                if (
                    type(request) is dict
                    and request.get("method") == "tools/call"
                    and type(request.get("params")) is dict
                    and request["params"].get("name") == "keeper_query"
                ):
                    request_id = request.get("id")
                    if (
                        isinstance(request_id, bool)
                        or not isinstance(request_id, (str, int))
                    ):
                        response = self._error_response(
                            request_id, -32600, "keeper_query requires a string or integer id"
                        )
                    else:
                        with self._active_lock:
                            if request_id in self._active or self._active:
                                response = self._error_response(
                                    request_id,
                                    -32001,
                                    "another keeper query is already active",
                                )
                            else:
                                cancel_event = threading.Event()
                                self._active[request_id] = cancel_event
                                worker = threading.Thread(
                                    target=self._run_query_request,
                                    args=(request, request_id, cancel_event),
                                )
                                workers.append(worker)
                                worker.start()
                                response = None
                else:
                    response = self.handle_request(request)
            if response is None:
                continue
            self._write_response(response)
        for worker in workers:
            worker.join()

    def _run_query_request(
        self,
        request: dict[str, Any],
        request_id: str | int,
        cancel_event: threading.Event,
    ) -> None:
        try:
            response = self.handle_request(request, cancel_event=cancel_event)
            if response is not None:
                self._write_response(response)
        finally:
            with self._active_lock:
                self._active.pop(request_id, None)

    def _cancel_request(self, request_id: Any) -> None:
        if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
            raise KeeperProfileError(
                "notifications/cancelled requires a string or integer requestId"
            )
        with self._active_lock:
            cancel_event = self._active.get(request_id)
        if cancel_event is not None:
            cancel_event.set()

    def _write_response(self, response: dict[str, Any]) -> None:
        with self._output_lock:
            sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
            sys.stdout.flush()

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


def _passage_to_dict(passage: KeeperPassage) -> dict[str, Any]:
    return {
        "candidate_key": passage.candidate_key,
        "artifact_id": passage.artifact_id,
        "event_id": passage.event_id,
        "source_id": passage.source_id,
        "source_key": passage.source_key,
        "source_type": passage.source_type,
        "scope": passage.scope,
        "scope_relative_path": passage.scope_relative_path,
        "path": passage.path,
        "title": passage.title,
        "tags": list(passage.tags),
        "snippet": passage.snippet,
        "selector": passage.selector,
        "trust": {
            "score": passage.trust_score,
            "reason": passage.trust_reason,
            "security_status": passage.security_status,
        },
        "privacy_class": passage.privacy_class,
        "retention_class": passage.retention_class,
        "provenance": passage.provenance,
        "score": passage.score,
    }


def _build_config_from_args(args: argparse.Namespace) -> KeeperProfileConfig:
    roots = [root.strip() for root in args.roots.split(",") if root.strip()]
    return KeeperProfileConfig(
        db_path=args.db,
        allowed_roots=roots,
        query_timeout_ms=args.query_timeout_ms,
        max_passage_chars=args.max_passage_chars,
        stale_index_seconds=args.stale_index_seconds,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Supervised read-only Keeper profile stdio server.",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the SQLite metadata database.",
    )
    parser.add_argument(
        "--roots",
        required=True,
        help=(
            "Comma-separated allowed archivist roots "
            "(e.g. vault/transcripts,raw/cissa)."
        ),
    )
    parser.add_argument(
        "--query-timeout-ms",
        type=int,
        default=10_000,
        help="Query timeout in milliseconds.",
    )
    parser.add_argument(
        "--max-passage-chars",
        type=int,
        default=2_000,
        help="Maximum characters per passage snippet.",
    )
    parser.add_argument(
        "--stale-index-seconds",
        type=int,
        default=7 * 24 * 3_600,
        help="Index staleness threshold in seconds.",
    )
    args = parser.parse_args(argv)

    try:
        profile_config = _build_config_from_args(args)
        profile = KeeperProfile(profile_config)
    except KeeperProfileError as exc:
        print(f"keeper profile failed to start: {exc}", file=sys.stderr)
        return 1

    server = KeeperProfileMCPServer(profile)
    server.serve_stdio()
    return 0


if __name__ == "__main__":
    sys.exit(main())
