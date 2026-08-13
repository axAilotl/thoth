"""thothmap session/run mapping tests (checklist 4: run -> core.session + process.run)."""

from __future__ import annotations

import pytest

from ccf.thothmap import MapContext
from ccf.thothmap.context import ThothMapError
from ccf.thothmap.sessions import run_submission, session_submission
from ccf.thothmap.sources import source_submission

from ccf_helpers import admit_mapped, compartment, make_rig, outcome_for


@pytest.fixture()
def rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


@pytest.fixture()
def ctx(rig):
    return MapContext(person_id=rig.person_id, policy_hint=rig.policy_lineage_id)


@pytest.fixture()
def source_id(rig, ctx):
    mapped = source_submission(
        rig.producer,
        ctx,
        {"source_name": "arxiv", "source_type": "paper_feed", "collector": "arxiv"},
    )
    admit_mapped(rig, mapped)
    return mapped.records[0]["id"]


SESSION = {
    "session_id": "b7c1d2e3-4f5a-6b7c-8d9e-0f1a2b3c4d5e",
    "source_id": "3f6c2e64-5c8a-4e0c-9c6f-0f6b1a2d3e4f",
    "native_session_id": "boot-8891/session-1",
    "session_type": "ambient",
    "status": "closed",
    "started_at": "2026-08-11T21:41:48Z",
    "ended_at": "2026-08-11T21:42:18Z",
    "metadata": {"capture_mode": "manual-test"},
}

RUN = {
    "run_id": "connector_run_9f31bc2ad4e54f8db2e17c0d6a5c9e11",
    "connector_name": "arxiv",
    "checkpoint_key": "c0ffee" * 10,
    "status": "completed",
    "started_at": "2026-08-11T21:41:48+00:00",
    "finished_at": "2026-08-11T21:42:19.123456+00:00",
    "output_count": 3,
    "failure_reason": None,
}


def test_session_maps_with_origin_and_times(rig, ctx, source_id):
    mapped = session_submission(
        rig.producer, ctx, SESSION, source_ccf_id=source_id, participants=[rig.person_id]
    )
    record = mapped.records[0]
    assert record["type"] == "core.session"
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.2-rc1:payload.core.session", record["payload"], what="core.session"
    )

    result = admit_mapped(rig, mapped)
    assert outcome_for(result, record["id"])["status"] == "admitted"
    semantic = compartment(rig, record["id"], "semantic")
    # Origin tuple carries the Thoth session_id as the source-native ID.
    assert semantic["origin"]["native_id"] == SESSION["session_id"]
    assert semantic["origin"]["source_id"] == source_id
    assert semantic["origin"]["revision"] == "1"
    assert semantic["occurred_at"]["start"] == "2026-08-11T21:41:48.000Z"
    assert semantic["occurred_at"]["end"] == "2026-08-11T21:42:18.000Z"
    payload = semantic["payload"]
    assert payload["native_id"] == "boot-8891/session-1"
    assert payload["capture_mode"] == "manual-test"


def test_session_reimport_idempotent_and_revision_conflict(rig, ctx, source_id):
    mapped = session_submission(rig.producer, ctx, SESSION, source_ccf_id=source_id)
    assert outcome_for(admit_mapped(rig, mapped), mapped.records[0]["id"])["status"] == "admitted"
    replay = admit_mapped(rig, mapped)
    assert outcome_for(replay, mapped.records[0]["id"])["status"] == "existing"

    # Same native ID at the same revision with changed content conflicts.
    changed = dict(SESSION, status="open", metadata={"capture_mode": "auto"})
    remapped = session_submission(rig.producer, ctx, changed, source_ccf_id=source_id)
    conflict = admit_mapped(rig, remapped)
    outcome = outcome_for(conflict, remapped.records[0]["id"])
    assert outcome["status"] == "origin_revision_conflict"


def test_run_maps_to_stateful_process_run(rig, ctx, source_id):
    mapped = run_submission(rig.producer, ctx, RUN, source_ccf_id=source_id)
    record = mapped.records[0]
    assert record["type"] == "process.run"
    rig.producer.schemas.validate(
        "urn:ccf:schema:0.1.2-rc1:payload.process.run", record["payload"], what="process.run"
    )
    assert record["lineage"]["transition"] == "succeed"
    assert record["lineage"]["previous_head_id"] is None
    assert record["payload"]["status"] == "succeeded"
    # Naive/offset timestamps normalize to canonical millisecond Z form.
    assert record["payload"]["terminal_at"] == "2026-08-11T21:42:19.123Z"

    result = admit_mapped(rig, mapped)
    assert outcome_for(result, record["id"])["status"] == "admitted"
    semantic = compartment(rig, record["id"], "semantic")
    assert semantic["origin"]["native_id"] == RUN["run_id"]


@pytest.mark.parametrize(
    "status,transition",
    [("running", "start"), ("failed", "fail"), ("queued", "queue"), ("cancelled", "cancel")],
)
def test_run_status_transition_map(rig, ctx, source_id, status, transition):
    snapshot = dict(RUN, run_id=f"connector_run_{status}", status=status)
    if status in ("running", "queued"):
        snapshot["finished_at"] = None
    mapped = run_submission(rig.producer, ctx, snapshot, source_ccf_id=source_id)
    assert mapped.records[0]["lineage"]["transition"] == transition


def test_run_fails_closed_on_unknown_status(rig, ctx, source_id):
    with pytest.raises(ThothMapError, match="unmappable"):
        run_submission(rig.producer, ctx, dict(RUN, status="exploded"), source_ccf_id=source_id)
