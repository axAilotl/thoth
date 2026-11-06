"""Private UDS ASGI surface. Only the authenticated Cissa proxy owns the socket."""
from __future__ import annotations

import asyncio
import json

from .managed_runtime import ManagedRuntimeError


def create_app(runtime):
    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        try:
            # Uvicorn's UDS transport has no TCP peer. The proxy must replace,
            # never forward, this header using its verified mTLS identity.
            peers = [value.decode() for key, value in scope["headers"] if key.lower() == b"x-cissa-managed-peer"]
            if scope.get("client") is not None or peers != [runtime.settings.expected_peer]:
                raise ManagedRuntimeError("untrusted_managed_peer", 403)
            if scope["path"] == "/health" and scope["method"] == "GET":
                result = {"status": "ok", "connector_version": runtime.settings.connector_version}
            else:
                routes = {"/v1/collections/execute": (runtime.execute, {"run", "credentials", "lease_expires_at"}),
                          "/v1/collections/artifacts": (runtime.read_artifacts, {"run", "artifact_ids", "lease_expires_at"}),
                          "/v1/collections/receipt": (runtime.receipt, {"run", "lease_expires_at"})}
                if scope["method"] != "POST" or scope["path"] not in routes:
                    raise ManagedRuntimeError("route_not_found", 404)
                chunks, size = [], 0
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    size += len(chunk)
                    if size > runtime.settings.max_request_bytes:
                        raise ManagedRuntimeError("request_budget_exceeded", 413)
                    chunks.append(chunk)
                    if not message.get("more_body"):
                        break
                try:
                    payload = json.loads(b"".join(chunks))
                except (ValueError, UnicodeError):
                    raise ManagedRuntimeError("invalid_json", 422) from None
                handler, keys = routes[scope["path"]]
                if isinstance(payload, dict) and "history_exports" in payload and scope["path"] == "/v1/collections/execute":
                    keys = keys | {"history_exports"}
                if not isinstance(payload, dict) or set(payload) != keys:
                    raise ManagedRuntimeError("invalid_envelope", 422)
                arguments = {"raw_run": payload.pop("run"), **payload}
                result = await asyncio.to_thread(handler, **arguments)
            status, body = 200, result
        except ManagedRuntimeError as error:
            status, body = error.status, {"error": {"code": error.code}}
        except Exception:
            # Never reflect input, tokens, filesystem paths or provider errors.
            status, body = 500, {"error": {"code": "managed_runtime_error"}}
        encoded = json.dumps(body).encode()
        await send({"type": "http.response.start", "status": status,
                    "headers": [(b"content-type", b"application/json"), (b"cache-control", b"no-store")]})
        await send({"type": "http.response.body", "body": encoded})
    return app
