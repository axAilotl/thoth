"""Import/capture runs -> ``core.session`` + ``process.run`` (checklist 4, row 2).

Two Thoth concepts are mapped here:

- ``CaptureSession`` (``core.capture_event_store.CaptureSession`` /
  ``capture_sessions`` table) becomes a ``core.session`` Record whose origin
  carries the Thoth ``session_id`` as the source-native ID.
- ``ConnectorRunRecord`` (``core.metadata_db`` / ``connector_runs`` table)
  becomes a stateful ``process.run`` Record (registry lineage mode
  ``compare_and_swap``, machine ``ccf.state.process-run-v1``). One lineage
  per run; the initial transition is derived from the run status.

Both carry origin tuples keyed on the admitting ``core.source`` Record, so
re-imports of the same unchanged run/session are idempotent and a changed
snapshot at the same revision surfaces as ``origin_revision_conflict``
(spec section 2.2).
"""

from __future__ import annotations

from ccf.ids import generate_id, parse_id
from ccf.producer import Producer

from ccf.thothmap.context import (
    MapContext,
    MappedSubmissions,
    ThothMapError,
    ccf_timestamp,
    claims,
    occurred_at,
    optional_str,
    origin,
    require_str,
    require_urn,
)

# connector_runs.status -> (process.run status, process-run-v1 initial transition)
_RUN_STATUS_MAP = {
    "queued": ("queued", "queue"),
    "running": ("running", "start"),
    "completed": ("succeeded", "succeed"),
    "succeeded": ("succeeded", "succeed"),
    "failed": ("failed", "fail"),
    "cancelled": ("cancelled", "cancel"),
    "interrupted": ("interrupted", "interrupt"),
}


def session_submission(
    producer: Producer,
    ctx: MapContext,
    snapshot: dict,
    *,
    source_ccf_id: str,
    revision: str | int | None = "1",
    participants: list[str] | None = None,
    subjects: list[dict] | None = None,
    data_classes: list[str] | None = None,
    channel: str | None = None,
    capture_mode: str | None = None,
) -> MappedSubmissions:
    """Convert a ``CaptureSession`` snapshot to a ``core.session`` Record.

    ``participants`` are claimed participant Person URNs (empty when Thoth
    has no participant attribution); ``subjects`` follow the conservative
    propagation rule of spec section 3.9.
    """
    require_urn(source_ccf_id, "record", field="source_ccf_id")
    session_id = require_str(snapshot, "session_id", what="capture session")
    native_id = optional_str(snapshot, "native_session_id") or session_id
    started = snapshot.get("started_at")
    if started is None:
        raise ThothMapError("capture session snapshot requires 'started_at'")
    ended = snapshot.get("ended_at")
    participants = list(participants or [])
    for participant in participants:
        require_urn(participant, "record", field="participants[]")

    metadata = snapshot.get("metadata") or {}
    if not isinstance(metadata, dict):
        raise ThothMapError("capture session 'metadata' must be an object")

    record = producer.new_record(
        type="core.session",
        claims=claims(ctx, subjects=subjects, data_classes=data_classes),
        occurred_at=occurred_at(started, ended),
        origin=origin(source_ccf_id, session_id, revision),
        payload={
            "source_id": source_ccf_id,
            "native_id": native_id,
            "channel": channel or optional_str(snapshot, "session_type") or "unknown",
            "started_at": ccf_timestamp(started, field="started_at"),
            "ended_at": ccf_timestamp(ended, field="ended_at") if ended is not None else None,
            "participants": participants,
            "capture_mode": capture_mode or str(metadata.get("capture_mode") or "import"),
            "extensions": {"thoth_status": snapshot.get("status") or "open"},
        },
    )
    return MappedSubmissions(records=[record])


def run_submission(
    producer: Producer,
    ctx: MapContext,
    snapshot: dict,
    *,
    source_ccf_id: str,
    revision: str | int | None = "1",
    run_kind: str = "ingestion",
    task: str | None = None,
    parent_run_ccf_id: str | None = None,
    recorded_at=None,
) -> MappedSubmissions:
    """Convert a ``ConnectorRunRecord`` snapshot to a stateful ``process.run``.

    The run's lineage ID is fresh per run; the initial transition encodes
    the Thoth run status (``running`` -> ``start``, ``completed`` ->
    ``succeed``, ``failed`` -> ``fail``, ...). Later status changes are new
    Records on the same lineage, which is a dual-write-phase concern.
    """
    require_urn(source_ccf_id, "record", field="source_ccf_id")
    run_id = require_str(snapshot, "run_id", what="connector run")
    status_raw = require_str(snapshot, "status", what="connector run")
    mapped = _RUN_STATUS_MAP.get(status_raw)
    if mapped is None:
        raise ThothMapError(f"unmappable connector run status {status_raw!r}")
    status, transition = mapped
    started = snapshot.get("started_at")
    if started is None:
        raise ThothMapError("connector run snapshot requires 'started_at'")
    finished = snapshot.get("finished_at")
    if parent_run_ccf_id is not None:
        require_urn(parent_run_ccf_id, "record", field="parent_run_ccf_id")
    connector = optional_str(snapshot, "connector_name")

    valid_from = ccf_timestamp(finished or started, field="run valid_from")
    record = producer.new_record(
        type="process.run",
        claims=claims(ctx),
        recorded_at=ccf_timestamp(recorded_at, field="recorded_at") if recorded_at else None,
        origin=origin(source_ccf_id, run_id, revision),
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": transition,
            "valid_from": valid_from,
            "expires_at": None,
        },
        payload={
            "run_kind": run_kind,
            "framework": "thoth",
            "task": task or f"{connector or 'thoth'} connector run",
            "status": status,
            "configuration_ref": None,
            "parent_run_id": parent_run_ccf_id,
            "started_at": ccf_timestamp(started, field="started_at"),
            "terminal_at": (
                ccf_timestamp(finished, field="finished_at") if finished is not None else None
            ),
            "extensions": {
                "thoth_connector_name": connector,
                "thoth_checkpoint_key": snapshot.get("checkpoint_key"),
                "thoth_output_count": snapshot.get("output_count", 0),
                "thoth_failure_reason": snapshot.get("failure_reason"),
            },
        },
    )
    return MappedSubmissions(records=[record])


def run_lineage_id(run_record_id: str) -> str:
    """Lineage URN paired to a run Record ID (same UUID, lineage kind)."""
    parsed = parse_id(run_record_id)
    if parsed.kind != "record":
        raise ThothMapError(f"run record ID must be a record URN: {run_record_id!r}")
    return f"urn:ccf:lineage:{parsed.uuid}"
