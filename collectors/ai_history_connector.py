"""Cissa-selected history exports; no scanning paths or instruction execution."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from urllib.parse import quote

from collectors.personal.ai_export import PROVIDERS, AIExportParser
from core.artifacts.conversation import ConversationArtifact
from core.collection_result import CollectionCoverage, collection_limit
from core.connector_capture import ConnectorCaptureQueue, write_connector_raw_json


HISTORY_URI_PREFIXES = {"chatgpt": "https://chatgpt.com/c/", "claude": "https://claude.ai/chat/",
                        "grok": "https://grok.com/chat/", "normalized": "cissa://history/"}


class AIHistoryCollector:
    def __init__(self, config, *, layout, db, capture_event_store=None, history_exports=None):
        self.config, self.layout, self.db = config, layout, db
        self.exports = history_exports or []
        self.queue = ConnectorCaptureQueue(config, layout=layout, db=db, capture_event_store=capture_event_store)
        self.coverage = CollectionCoverage(config)

    def collect(self):
        sources = self.config.get("sources.ai_history.sources")
        selected = self.config.get("managed.selected_resources")
        if not isinstance(sources, list) or not sources or not selected:
            raise ValueError("Managed history sources must be explicitly selected")
        resources = [f'ai_history:{item["project_id"]}:{item["source_id"]}' for item in sources]
        if len(resources) != len(set(resources)) or set(resources) != set(selected):
            raise ValueError("History selection mismatch")
        exports = {item["resource_id"]: item["content"] for item in self.exports}
        expected = {resource for resource, item in zip(resources, sources) if item["operation"] == "upsert"}
        if len(exports) != len(self.exports) or set(exports) != expected:
            raise ValueError("History exports must match selected upserts")
        parser = AIExportParser(max_export_bytes=collection_limit(self.config, "max_history_export_bytes"),
            max_messages=collection_limit(self.config, "max_history_messages"),
            max_message_characters=collection_limit(self.config, "max_history_message_characters"))
        queued = 0
        with self.queue.lifecycle() as lifecycle:
            for index, (resource, source) in enumerate(zip(resources, sources)):
                entry = self.coverage.source(resource, time_basis="observation_time")
                if index >= collection_limit(self.config, "max_items"):
                    self.coverage.limited(resource)
                    continue
                try:
                    queued += self._capture(lifecycle, parser, resource, source, exports.get(resource))
                    entry["observed_count"] = 1
                    entry["queued_count"] = 1
                except (ValueError, TypeError, KeyError):
                    self.coverage.fail(resource, "history_import_rejected")
        return {**self.coverage.receipt(), "queued_count": queued}

    def _capture(self, lifecycle, parser, resource, source, content):
        provider, project, source_id = source["provider"], source["project_id"], source["source_id"]
        if provider not in PROVIDERS or source["operation"] not in {"upsert", "delete"}:
            raise ValueError("Invalid history source")
        if any(not isinstance(value, str) or not value or ":" in value for value in (project, source_id)) or not isinstance(source["revision"], str) or not source["revision"]:
            raise ValueError("Invalid history identity")
        now = datetime.now(timezone.utc).isoformat()
        parsed = {"source_id": source_id, "project_id": project, "provider": provider, "messages": []}
        if source["operation"] == "upsert":
            if hashlib.sha256(content.encode()).hexdigest() != source["content_sha256"]:
                raise ValueError("History export digest mismatch")
            parsed = parser.parse(content, provider=provider, source_id=source_id, project_id=project)
        metadata = {"historical_evidence": True, "authority": "none", "provider": provider,
                    "project_id": project, "source_id": source_id, "revision": source["revision"], "operation": source["operation"],
                    "stripped_parts": parsed.get("stripped_parts", {})}
        raw = json.dumps({**parsed, "history": metadata}, sort_keys=True, ensure_ascii=False)
        artifact_id = f"history:{provider}:{project}:{source_id}"
        previous = self.db.get_ingestion_entry(artifact_id)
        if previous:
            payload = json.loads(previous.payload_json)
            old = payload.get("custom_metadata", {}).get("history", {})
            if old.get("revision") == source["revision"] and payload.get("raw_content") != raw:
                raise ValueError("History revision reused for different content")
        digest = hashlib.sha256(raw.encode()).hexdigest()
        raw_path = write_connector_raw_json(self.layout, connector_name="ai_history", subdir="sessions",
            native_id=f"{artifact_id}:{digest}", payload=json.loads(raw), captured_at=now)
        uri = HISTORY_URI_PREFIXES[provider] + quote(source_id, safe="")
        body = "" if source["operation"] == "delete" else "\n\n".join(
            f'[{message["created_at"] or "time unavailable"}] {message["role"]}:\n{message["text"]}' for message in parsed["messages"])
        artifact = ConversationArtifact(id=artifact_id, source_type="ai_history", capabilities=(),
            raw_content=raw, messages=parsed["messages"], created_at=parsed.get("created_at"), ingested_at=now,
            title=parsed.get("title", source_id), body=body, source_url=uri,
            custom_metadata={"history": metadata, "title": parsed.get("title", source_id), "body": body,
                "source_uri": uri, "source_updated_at": parsed.get("updated_at") or now})
        self.queue.queue_artifact(lifecycle, artifact, artifact_type="markdown",
            source={"source_name": "ai_history", "source_type": "ai_history", "native_source_id": resource, "base_uri": uri},
            session={"session_type": "historical_agent_conversation", "native_session_id": source_id},
            event={"event_type": "historical_evidence", "native_event_id": f'{source_id}:{source["revision"]}',
                "captured_at": now, "provenance": metadata, "privacy": {"classification": "private"}}, raw_path=raw_path)
        return 1
