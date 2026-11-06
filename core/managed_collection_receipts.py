"""Serialize managed attempts and replay durable receipts using existing state."""
from __future__ import annotations

from dataclasses import asdict
import fcntl
import hashlib
import json
from datetime import datetime, timezone

from .collection_result import collection_limit


def managed_collection_key(request):
    identity = request.identity()
    return "managed_collection:" + hashlib.sha256(json.dumps({
        name: identity[name] for name in ("account_id", "archive_id", "integration_id", "idempotency_key")
    }, sort_keys=True).encode()).hexdigest()


def managed_collection_fingerprint(request):
    inputs = asdict(request)
    inputs.pop("lease_generation")
    return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()


def execute_managed(service, invoke):
    request = service.managed_run
    identity = request.identity()
    key = managed_collection_key(request)
    fingerprint = managed_collection_fingerprint(request)
    lock_dir = service.layout.system_root / "managed-collection-locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    with (lock_dir / key.split(":")[-1]).open("a") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Managed collection is already running") from None
        state = service.db.get_automation_state(key) or {}
        if state:
            if state.get("fingerprint") != fingerprint:
                raise ValueError("Managed idempotency key reused with different inputs")
            if state.get("lease_generation", 0) > request.lease_generation:
                raise ValueError("Stale managed lease generation")
            if state.get("status") == "completed":
                receipt = state["receipt"]
                receipt["result"]["managed_run"] = identity
                state.update(lease_generation=request.lease_generation, receipt=receipt)
                service.db.upsert_automation_state(key, state)
                return receipt
            if state.get("attempt", 0) >= collection_limit(service.config, "max_attempts"):
                raise ValueError("Managed collection retry budget exhausted")
            next_retry = state.get("next_retry_at")
            if next_retry and datetime.fromisoformat(next_retry) > datetime.now(timezone.utc):
                raise ValueError("Managed collection retry is not due")
            cursor = state.get("receipt", {}).get("result", {}).get("resume_token")
            if cursor:
                service.config.set("managed.cursor", cursor)
        state.update(fingerprint=fingerprint, lease_generation=request.lease_generation,
                     attempt=state.get("attempt", 0) + 1, status="running")
        service.db.upsert_automation_state(key, state)
        try:
            receipt = invoke()
            state.update(status=receipt["status"], receipt=receipt)
        except Exception as exc:
            state.update(status="failed", failure=type(exc).__name__)
            _finish(service, key, state)
            raise
        _finish(service, key, state)
        return receipt


def _finish(service, key, state):
    from datetime import timedelta
    delay = min(collection_limit(service.config, "retry_max_seconds"),
                collection_limit(service.config, "retry_base_seconds") * 2 ** (state["attempt"] - 1))
    state["next_retry_at"] = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat() if state["status"] in {"partial", "failed"} else None
    service.db.upsert_automation_state(key, state)


def artifact_receipts(db, checkpoint_id, *, connector):
    """Include captures from interrupted attempts, without disclosing paths."""
    with db._get_connection() as conn:
        rows = conn.execute(
            "SELECT artifact_id, artifact_type, source, capture_event_id, raw_ref_id FROM connector_checkpoint_outputs WHERE checkpoint_id = ? ORDER BY artifact_id",
            (checkpoint_id,),
        ).fetchall()
    result = []
    for row in rows:
        entry = db.get_ingestion_entry(row["artifact_id"])
        if entry is None:
            raise RuntimeError("Persisted collection output has no ingestion artifact")
        payload = json.loads(entry.payload_json)
        raw = payload.get("raw_content", "")
        if not isinstance(raw, str):
            raw = json.dumps(raw, sort_keys=True)
        result.append({**dict(row), "source": connector, "content_hash": hashlib.sha256(raw.encode()).hexdigest(),
                       "source_time": payload.get("created_at"), "observed_at": payload.get("ingested_at")})
    return result
