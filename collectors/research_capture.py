"""Shared evidence capture for configured public research connectors."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import requests

from core.artifacts.markdown import MarkdownArtifact
from core.collection_result import CollectionCoverage, collection_limit
from core.collection_fairness import FairCollectionBudget
from core.connector_capture import ConnectorCaptureQueue, write_connector_raw_json


class ResearchCapture:
    name: str

    def __init__(self, config, *, layout, db, capture_event_store=None):
        self.config, self.layout, self.db = config, layout, db
        self.settings = config.get(f"sources.{self.name}", {})
        for key in ("page_size", "comment_limit", "search_count", "play_page_size", "apple_max_pages"):
            if key in self.settings and (type(self.settings[key]) is not int or self.settings[key] < 1):
                raise ValueError(f"Research setting {key} must be a positive integer")
        self.coverage = CollectionCoverage(config)
        self.queue = ConnectorCaptureQueue(config, layout=layout, db=db, capture_event_store=capture_event_store)
        self.http = requests.Session()
        self.http.trust_env = False
        self.queued = []

    def plan(self, sources, *, slots=None):
        # Validate every selection before reserving rotation or performing I/O.
        sources = list(sources)
        for source in sources:
            self.source(source)
        self.budget = FairCollectionBudget(self.config, self.db, self.name, sources,
            max_items=collection_limit(self.config, "max_items"), slots=slots)
        return self.budget.sources

    def observe(self, source, *, parent=None):
        if not self.budget.observe(parent or source):
            return False
        self.coverage.source(source)["observed_count"] += 1
        return True

    def source(self, name, *, time_basis="source_time"):
        selected = self.config.get("managed.selected_resources")
        if selected is not None and name not in selected:
            raise ValueError("Research source is outside selected resources")
        entry = self.coverage.source(name, time_basis=time_basis)
        entry["time_basis"] = time_basis
        return entry

    def fetch(self, url, params=None):
        response = self.http.get(url, params=params,
            timeout=collection_limit(self.config, "request_timeout_seconds"),
            headers={"User-Agent": self.settings.get("user_agent", "ThothResearch/1.0")})
        response.raise_for_status()
        return response.json()

    def capture(self, lifecycle, *, source, native_id, uri, timestamp, title, text, raw, filter_window=True):
        if filter_window and not self.coverage.in_window(source, timestamp):
            return
        raw_text = json.dumps(raw, sort_keys=True, ensure_ascii=False, default=str)
        digest = hashlib.sha256(raw_text.encode()).hexdigest()
        artifact_id = f"{self.name}:{hashlib.sha256((uri + ':' + native_id).encode()).hexdigest()}:{digest}"
        if self.db.get_ingestion_entry(artifact_id):
            return
        now = datetime.now(timezone.utc).isoformat()
        raw_path = write_connector_raw_json(self.layout, connector_name=self.name,
            subdir="evidence", native_id=artifact_id, payload=raw, captured_at=now)
        body = f"# {title}\n\n{text}\n\nSource: {uri}\n"
        page = self.layout.library_root / "research" / self.name / f"{digest}.md"
        page.parent.mkdir(parents=True, exist_ok=True)
        # Content-addressed immutable derivative; races can only write identical bytes.
        try:
            with page.open("x", encoding="utf-8") as handle:
                handle.write(body)
        except FileExistsError:
            if page.read_text(encoding="utf-8") != body:
                raise ValueError("Research evidence hash collision")
        artifact = MarkdownArtifact(id=artifact_id, source_type=self.name,
            raw_content=raw_text, created_at=timestamp, ingested_at=now,
            title=title, body=body, source_url=uri, source_path=str(page),
            source_checksum=hashlib.sha256(body.encode()).hexdigest())
        result = self.queue.queue_artifact(lifecycle, artifact, artifact_type="markdown",
            source={"source_name": self.name, "source_type": self.name,
                    "native_source_id": source, "base_uri": uri},
            session={"session_type": "research_scan", "native_session_id": self.config.get("managed.run_id", now)},
            event={"event_type": "research_evidence", "native_event_id": native_id,
                   "occurred_at": timestamp, "captured_at": now,
                   "provenance": {"source": source, "content_hash": digest},
                   "privacy": {"classification": "public"}}, raw_path=raw_path)
        self.queued.append({"artifact_id": result.queue_artifact_id, "content_hash": digest, "source_uri": uri})
        self.coverage.source(source)["queued_count"] += 1

    def raw_page(self, source, page, raw):
        digest = hashlib.sha256(json.dumps(raw, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()
        write_connector_raw_json(self.layout, connector_name=self.name, subdir="pages",
            native_id=f"{source}:{page}:{digest}", payload=raw, captured_at=datetime.now(timezone.utc).isoformat())
        self.coverage.source(source).setdefault("raw_pages", []).append({"page": page, "content_hash": digest})
        return digest

    def receipt(self):
        return {**self.coverage.receipt(), "queued": self.queued, "queued_count": len(self.queued)}


def epoch_time(value):
    return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
