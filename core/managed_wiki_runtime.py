"""Protected wiki transport over the existing compiler and Cissa callback."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import httpx

from .archivist_runtime import run_archivist_topics
from .config import Config
from .managed_execution import utc_time
from .managed_model_gateway import ManagedModelGateway
from .managed_runtime import ManagedRuntimeError
from .managed_wiki import ManagedWikiContext, acknowledge_wiki_proposal
from .path_layout import build_path_layout


class ManagedWikiRuntime:
    def __init__(self, runtime, *, invoke=None):
        self.runtime = runtime
        self.settings = runtime.config.get("managed_wiki", {})
        if not self.settings.get("enabled"):
            raise ManagedRuntimeError("managed_wiki_disabled", 503)
        for key in ("max_sources", "max_topics", "max_page_bytes", "callback_timeout_seconds", "max_callback_response_bytes"):
            value = self.settings.get(key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < float("inf"):
                raise ManagedRuntimeError("invalid_managed_wiki_settings", 503)
        socket = self.settings.get("callback_socket")
        if not isinstance(socket, str) or not Path(socket).is_absolute():
            raise ManagedRuntimeError("invalid_model_callback_socket", 503)
        self.invoke = invoke or self._callback

    def _context(self, raw, expiry):
        if not isinstance(raw, dict) or "account_root" in raw:
            raise ManagedRuntimeError("invalid_wiki_context", 422)
        try:
            context = ManagedWikiContext(**raw, account_root=Path(self.runtime.settings.account_root))
            remaining = (utc_time(expiry) - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError):
            raise ManagedRuntimeError("invalid_wiki_context", 422) from None
        if context.account_id != self.runtime.settings.account_id or context.archive_id != self.runtime.settings.archive_id:
            raise ManagedRuntimeError("account_binding_mismatch", 403)
        if not 0 < remaining <= self.runtime.settings.max_lease_seconds:
            raise ManagedRuntimeError("invalid_or_expired_lease", 403)
        if not isinstance(context.source_manifest, dict) or not isinstance(context.expected_pages, dict) or not context.topic_ids or len(context.topic_ids) > self.settings["max_topics"] or len(context.expected_pages) > self.settings["max_topics"] or len(context.source_manifest) > self.settings["max_sources"]:
            raise ManagedRuntimeError("wiki_manifest_budget_exceeded", 413)
        for name in (*context.source_manifest, *context.expected_pages):
            if not isinstance(name, str) or not name or name == "." or Path(name).as_posix() != name or Path(name).is_absolute() or ".." in Path(name).parts or "\\" in name:
                raise ManagedRuntimeError("invalid_wiki_snapshot_path", 422)
        return context, remaining

    def _job_key(self, context):
        return "managed_wiki_job:" + hashlib.sha256(json.dumps({key: value for key, value in context.identity().items() if key != "lease_generation"}, sort_keys=True).encode()).hexdigest()

    def _layout(self, context):
        config = Config()
        config.data = deepcopy(self.runtime.config.data)
        root = self.runtime.layout.system_root / "managed-wiki" / self._job_key(context).split(":")[-1]
        if not root.resolve().is_relative_to(context.account_root.resolve()):
            raise ManagedRuntimeError("wiki_snapshot_escape", 403)
        config.set("paths", {"vault_dir": str(root / "vault"), "wiki_dir": str(root / "wiki"),
            "system_dir": str(root / "system"), "cache_dir": "cache", "archivist_topics_file": str(root / "topics.json")})
        config.set("database.path", str(self.runtime.layout.database_path))
        config.data.pop("content_roots", None)
        return config, build_path_layout(config), root

    def propose(self, context, source_documents, page_snapshots, topics, model_routes, lease_expires_at):
        raw = context
        context, remaining = self._context(raw, lease_expires_at)
        if not isinstance(source_documents, dict) or set(source_documents) != set(context.source_manifest) or not isinstance(page_snapshots, dict) or set(page_snapshots) != set(context.expected_pages):
            raise ManagedRuntimeError("wiki_snapshot_selection_mismatch", 422)
        if not isinstance(topics, dict) or not isinstance(topics.get("topics"), list) or len(topics["topics"]) != len(context.topic_ids) or {topic.get("id") for topic in topics["topics"]} != set(context.topic_ids):
            raise ManagedRuntimeError("wiki_topic_selection_mismatch", 422)
        if {topic.get("output_path") for topic in topics["topics"]} != set(context.expected_pages):
            raise ManagedRuntimeError("wiki_topic_output_mismatch", 422)
        if not self.runtime.lock.acquire(blocking=False):
            raise ManagedRuntimeError("runtime_busy")
        try:
            self.runtime.check_storage()
            fingerprint = hashlib.sha256(json.dumps({"context": raw, "sources": source_documents, "pages": page_snapshots, "topics": topics, "routes": model_routes}, sort_keys=True).encode()).hexdigest()
            key = self._job_key(context)
            previous = self.runtime.db.get_automation_state(key)
            if previous:
                if previous["lease_generation"] > context.lease_generation:
                    raise ManagedRuntimeError("wiki_stale_lease", 403)
                if previous["lease_generation"] == context.lease_generation:
                    if previous["fingerprint"] != fingerprint:
                        raise ManagedRuntimeError("wiki_job_inputs_changed", 409)
                    if previous.get("result"):
                        return previous["result"]
            config, layout, root = self._layout(context)
            layout.ensure_directories()
            total = 0
            for name, content in source_documents.items():
                if not isinstance(content, str):
                    raise ManagedRuntimeError("invalid_wiki_snapshot", 422)
                encoded = content.encode()
                total += len(encoded)
                if len(encoded) > context.budgets["max_source_bytes"] or total > context.budgets["max_total_source_bytes"]:
                    raise ManagedRuntimeError("wiki_source_budget_exceeded", 413)
                if hashlib.sha256(encoded).hexdigest() != context.source_manifest[name]["file_sha256"]:
                    raise ManagedRuntimeError("wiki_source_digest_mismatch", 409)
                self._write_snapshot(layout.vault_root, name, encoded)
            for name, content in page_snapshots.items():
                expected = context.expected_pages[name]
                if content is None:
                    if expected != {"revision": None, "content_hash": None}:
                        raise ManagedRuntimeError("wiki_page_snapshot_missing", 409)
                    path = layout.wiki_root / name
                    if path.exists():
                        path.unlink()
                    continue
                if not isinstance(content, str) or len(content.encode()) > self.settings["max_page_bytes"]:
                    raise ManagedRuntimeError("wiki_page_budget_exceeded", 413)
                if hashlib.sha256(content.encode()).hexdigest() != expected["content_hash"]:
                    raise ManagedRuntimeError("wiki_page_digest_mismatch", 409)
                self._write_snapshot(layout.wiki_root, name, content.encode())
            (root / "topics.json").write_text(json.dumps(topics))
            async def invoke(request):
                self._context(raw, lease_expires_at)
                identity = {key: request[key] for key in context.identity()}
                return await self.invoke({"identity": identity, "lease_expires_at": lease_expires_at,
                    **{key: value for key, value in request.items() if key not in identity}})
            gateway = ManagedModelGateway(identity=context.identity(), tasks=model_routes, invoke=invoke)
            context.validate_runtime(layout, self.runtime.db, gateway)
            state = {"fingerprint": fingerprint, "lease_generation": context.lease_generation, "context": raw, "status": "running"}
            self.runtime.db.upsert_automation_state(key, state)
            async def compile_topics():
                return await asyncio.wait_for(run_archivist_topics(config, project_root=Path(__file__).resolve().parent.parent,
                    layout=layout, db=self.runtime.db, managed_context=context, llm_interface=gateway),
                    timeout=min(remaining, self.runtime.settings.execution_timeout_seconds))
            try:
                result = asyncio.run(compile_topics())
            except TimeoutError:
                raise ManagedRuntimeError("wiki_deadline_exceeded", 504) from None
            self._context(raw, lease_expires_at)
            self.runtime.check_storage()
            state.update(status="completed", result=result)
            self.runtime.db.upsert_automation_state(key, state)
            return result
        finally:
            self.runtime.lock.release()

    def acknowledge(self, context, proposal_id, receipt, lease_expires_at):
        context, _ = self._context(context, lease_expires_at)
        state = self.runtime.db.get_automation_state(self._job_key(context))
        if state is None or state["lease_generation"] != context.lease_generation:
            raise ManagedRuntimeError("wiki_receipt_lease_mismatch", 403)
        if json.dumps(state["context"], sort_keys=True) != json.dumps({key: value for key, value in asdict(context).items() if key != "account_root"}, sort_keys=True):
            raise ManagedRuntimeError("wiki_receipt_context_mismatch", 403)
        proposal = self.runtime.db.get_automation_state("managed_wiki:" + proposal_id)
        if not proposal or receipt.get("path") != proposal["proposal"]["path"] or receipt.get("job_id") != context.job_id:
            raise ManagedRuntimeError("wiki_publication_receipt_mismatch", 403)
        if any(key in receipt and receipt[key] != value for key, value in context.identity().items()):
            raise ManagedRuntimeError("wiki_publication_receipt_mismatch", 403)
        _, layout, _ = self._layout(context)
        return acknowledge_wiki_proposal(context=context, layout=layout, db=self.runtime.db, proposal_id=proposal_id,
            receipt={**receipt, **context.identity(), "proposal_id": proposal_id})

    def _write_snapshot(self, root, name, content):
        path = root / name
        if not path.resolve().is_relative_to(root.resolve()):
            raise ManagedRuntimeError("wiki_snapshot_escape", 403)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    async def _callback(self, request):
        remaining = (utc_time(request["lease_expires_at"]) - datetime.now(timezone.utc)).total_seconds()
        timeout = min(remaining, self.settings["callback_timeout_seconds"])
        if timeout <= 0:
            raise ManagedRuntimeError("invalid_or_expired_lease", 403)
        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=self.settings["callback_socket"]),
                                    timeout=timeout, trust_env=False) as client:
            async with client.stream("POST", "http://cissa/v1/integrations/thoth/model", json=request) as response:
                if response.status_code != 200:
                    raise ManagedRuntimeError("cissa_model_callback_rejected", 502)
                chunks, size = [], 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self.settings["max_callback_response_bytes"]:
                        raise ManagedRuntimeError("model_response_budget_exceeded", 502)
                    chunks.append(chunk)
                return json.loads(b"".join(chunks))
