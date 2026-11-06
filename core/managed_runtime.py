"""Single-account managed collection worker and receipt-scoped artifact reads."""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
from urllib.parse import quote, urlsplit

from .config import Config
from .managed_collection_receipts import managed_collection_key, managed_collection_fingerprint
from .managed_execution import ManagedCollectionRun, utc_time
from .metadata_db import MetadataDB
from .path_layout import build_path_layout


class ManagedRuntimeError(ValueError):
    def __init__(self, code, status=409):
        self.code, self.status = code, status
        super().__init__(code)


@dataclass(frozen=True)
class ManagedRuntimeSettings:
    account_id: str
    archive_id: str
    account_root: str
    connector_version: str
    expected_peer: str
    socket_path: str
    config_file: str
    max_request_bytes: int
    max_storage_bytes: int
    max_artifact_characters: int
    max_artifacts_per_read: int
    execution_timeout_seconds: float
    termination_grace_seconds: float
    max_lease_seconds: float

    @classmethod
    def load(cls, path, *, build_revision=None):
        settings = cls(**json.loads(Path(path).read_text()))
        if build_revision is None:
            build_revision = (Path(__file__).resolve().parent.parent / "BUILD_REVISION").read_text().strip()
        if not build_revision or build_revision == "unversioned" or settings.connector_version != build_revision:
            raise ValueError("Managed runtime must match the pinned image revision")
        for key in ("max_request_bytes", "max_storage_bytes", "max_artifact_characters", "max_artifacts_per_read",
                    "execution_timeout_seconds", "termination_grace_seconds", "max_lease_seconds"):
            value = getattr(settings, key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < float("inf"):
                raise ValueError(f"Managed runtime setting {key} must be finite and positive")
        if not all((settings.account_id, settings.archive_id, settings.expected_peer)):
            raise ValueError("Managed account binding and peer are required")
        if not all(Path(getattr(settings, key)).is_absolute() for key in ("account_root", "socket_path", "config_file")):
            raise ValueError("Managed runtime paths must be absolute")
        if settings.max_artifact_characters > 200000:
            raise ValueError("Artifact character budget exceeds the ExternalDocument contract")
        for key in ("max_request_bytes", "max_storage_bytes", "max_artifact_characters", "max_artifacts_per_read"):
            if type(getattr(settings, key)) is not int:
                raise ValueError(f"Managed runtime setting {key} must be an integer")
        return settings


class ManagedRuntime:
    def __init__(self, settings, *, worker=None):
        self.settings = settings
        self.config = Config()
        self.config.data = json.loads(Path(settings.config_file).read_text())
        self.config.set("managed.connector_version", settings.connector_version)
        if self.config.get("database.capture_event_store.enabled", False):
            raise ValueError("Managed capture store requires an explicit protected store adapter")
        self.layout = build_path_layout(self.config)
        root = Path(settings.account_root).resolve()
        paths = [getattr(self.layout, field.name) for field in fields(self.layout)]
        paths.extend(item.base_path for item in self.layout.content_roots)
        if any(not path.resolve().is_relative_to(root) for path in paths if isinstance(path, Path)):
            raise ValueError("Managed layout must stay inside account root")
        self.layout.ensure_directories()
        self.db = MetadataDB(str(self.layout.database_path))
        self.worker = worker or self._worker
        self.lock = threading.Lock()

    def authorize(self, raw_run, lease_expires_at):
        try:
            run = ManagedCollectionRun(**raw_run)
        except (ValueError, TypeError, KeyError):
            raise ManagedRuntimeError("invalid_run", 422) from None
        if run.account_id != self.settings.account_id or run.archive_id != self.settings.archive_id:
            raise ManagedRuntimeError("account_binding_mismatch", 403)
        if run.connector_version != self.settings.connector_version:
            raise ManagedRuntimeError("connector_version_mismatch")
        if run.connector not in {"github", "huggingface", "arxiv", "ugc_discussion", "ugc_stores", "ugc_youtube", "competitor_web"}:
            raise ManagedRuntimeError("connector_not_managed", 403)
        try:
            run.validate_paths(Path(self.settings.account_root), self.layout, self.db)
            expiry = utc_time(lease_expires_at)
        except (ValueError, TypeError):
            raise ManagedRuntimeError("invalid_execution_binding", 422) from None
        remaining = (expiry - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0 or remaining > self.settings.max_lease_seconds:
            raise ManagedRuntimeError("invalid_or_expired_lease", 403)
        try:
            run.execution_options(None)
        except (ValueError, TypeError):
            raise ManagedRuntimeError("invalid_source_selection", 422) from None
        return run, remaining

    def check_storage(self):
        root = Path(self.settings.account_root).resolve()
        total = 0
        for directory, _, names in os.walk(root, followlinks=False):
            for name in names:
                path = Path(directory) / name
                if not path.resolve().is_relative_to(root):
                    raise ManagedRuntimeError("account_storage_escape", 403)
                total += path.stat().st_size
                if total > self.settings.max_storage_bytes:
                    raise ManagedRuntimeError("account_storage_budget_exceeded", 507)

    def execute(self, raw_run, credentials, lease_expires_at):
        run, remaining = self.authorize(raw_run, lease_expires_at)
        if not isinstance(credentials, dict) or set(credentials) - {"token"} or any(not isinstance(value, str) for value in credentials.values()):
            raise ManagedRuntimeError("invalid_credentials", 422)
        if not self.lock.acquire(blocking=False):
            raise ManagedRuntimeError("runtime_busy")
        try:
            self.check_storage()
            result = self.worker(raw_run, credentials, min(remaining, self.settings.execution_timeout_seconds))
            self.authorize(raw_run, lease_expires_at)
            self.check_storage()
            if result.get("managed_run") != run.identity():
                raise ManagedRuntimeError("worker_receipt_identity_mismatch", 502)
            return result
        finally:
            self.lock.release()

    def receipt(self, raw_run, lease_expires_at):
        run, _ = self.authorize(raw_run, lease_expires_at)
        state = self.db.get_automation_state(managed_collection_key(run))
        if state and state.get("fingerprint") != managed_collection_fingerprint(run):
            raise ManagedRuntimeError("receipt_inputs_mismatch", 403)
        if not state or state.get("status") == "running" or not state.get("receipt"):
            raise ManagedRuntimeError("receipt_unavailable")
        result = state["receipt"]["result"]
        if state.get("lease_generation") != run.lease_generation or result.get("managed_run") != run.identity():
            raise ManagedRuntimeError("receipt_lease_mismatch", 403)
        return result

    def read_artifacts(self, raw_run, artifact_ids, lease_expires_at):
        run, _ = self.authorize(raw_run, lease_expires_at)
        if not isinstance(artifact_ids, list) or not all(isinstance(value, str) for value in artifact_ids) or len(artifact_ids) != len(set(artifact_ids)) or len(artifact_ids) > self.settings.max_artifacts_per_read:
            raise ManagedRuntimeError("invalid_artifact_selection", 422)
        receipt = self.receipt(raw_run, lease_expires_at)
        allowed = {artifact["artifact_id"]: artifact for artifact in receipt["artifacts"]}
        if not set(artifact_ids) <= set(allowed):
            raise ManagedRuntimeError("artifact_not_in_run_receipt", 403)
        documents = []
        for artifact_id in artifact_ids:
            row = self.db.get_ingestion_entry(artifact_id)
            if row is None:
                raise ManagedRuntimeError("artifact_unavailable", 404)
            payload = json.loads(row.payload_json)
            raw = payload.get("raw_content", "")
            raw = raw if isinstance(raw, str) else json.dumps(raw, sort_keys=True)
            if hashlib.sha256(raw.encode()).hexdigest() != allowed[artifact_id]["content_hash"]:
                raise ManagedRuntimeError("artifact_revision_changed")
            body = payload.get("body") or payload.get("abstract") or raw
            if not isinstance(body, str) or len(body) > self.settings.max_artifact_characters:
                raise ManagedRuntimeError("artifact_text_budget_exceeded", 413)
            uri = payload.get("source_url") or payload.get("source_uri")
            if not uri and payload.get("arxiv_id"):
                uri = "https://arxiv.org/abs/" + quote(payload["arxiv_id"], safe="/")
            if not uri and payload.get("repo_name"):
                uri = ("https://github.com/" if run.connector == "github" else "https://huggingface.co/") + quote(payload["repo_name"], safe="/")
            if uri and urlsplit(uri).scheme not in {"https", "http"}:
                raise ManagedRuntimeError("invalid_artifact_source_uri")
            documents.append({"external_id": artifact_id, "source_uri": uri,
                "title": str(payload.get("title") or payload.get("repo_name") or artifact_id)[:1024],
                "body_text": body, "content_sha256": hashlib.sha256(body.encode()).hexdigest(),
                "source_updated_at": payload.get("created_at") or payload.get("ingested_at") or allowed[artifact_id].get("observed_at"),
                "metadata": {"capture_content_hash": allowed[artifact_id]["content_hash"], "run_id": run.run_id}})
        return documents

    def _worker(self, run, credentials, timeout):
        request = {"run": run, "credentials": credentials, "config_file": self.settings.config_file,
                   "account_root": self.settings.account_root, "connector_version": self.settings.connector_version}
        process = subprocess.Popen([sys.executable, "-m", "core.managed_worker"],
            cwd=Path(__file__).resolve().parent.parent, env={"PATH": os.defpath, "TZ": "UTC"},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            stdout, _ = process.communicate(json.dumps(request), timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.communicate(timeout=self.settings.termination_grace_seconds)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
            raise ManagedRuntimeError("worker_deadline_exceeded", 504) from None
        if process.returncode:
            raise ManagedRuntimeError("worker_failed", 502)
        try:
            return json.loads(stdout)
        except (ValueError, TypeError):
            raise ManagedRuntimeError("worker_invalid_response", 502) from None
