"""Service-level tests for the classification review surface."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from core.artifact_classification import (
    Gate,
    Projection,
    RoutingPolicy,
    RoutingRule,
    policy_to_mapping,
)
from core.artifact_review_queue import (
    ArtifactReviewQueueError,
    ArtifactReviewQueueService,
)
from core.classification_review import (
    ClassificationReviewError,
    ClassificationReviewService,
)
from core.config import config as _runtime_config
from core.metadata_db import IngestionQueueEntry, MetadataDB
from core.path_layout import build_path_layout


def _configure_config(tmp_path: Path) -> None:
    _runtime_config.data = {}
    _runtime_config.set("paths.vault_dir", str(tmp_path / "vault"))
    _runtime_config.set("paths.system_dir", ".thoth_system")
    _runtime_config.set("paths.cache_dir", "graphql_cache")
    _runtime_config.set("paths.raw_dir", "raw")
    _runtime_config.set("paths.library_dir", "library")
    _runtime_config.set("paths.wiki_dir", "wiki")
    _runtime_config.set("paths.digests_dir", "_digests")
    _runtime_config.set("database.path", "meta.db")
    _runtime_config.set("classification.enabled", True)
    _runtime_config.set("classification.confidence_threshold", 0.85)
    _runtime_config.set("classification.min_support", 2)
    _runtime_config.set("classification.min_precision", 0.8)
    _runtime_config.set("classification.held_out_fraction", 0.2)
    _runtime_config.set(
        "classification.projections",
        [
            {"projection_id": "tweet_markdown", "name": "Tweets"},
            {"projection_id": "paper_library", "name": "Papers"},
            {
                "projection_id": "semantic_memory",
                "name": "Semantic memory",
                "gates": ["sensitive_semantic_promotion"],
            },
        ],
    )
    _runtime_config.set(
        "classification.rules",
        [
            {
                "rule_id": "tweet-twitter",
                "projection_id": "tweet_markdown",
                "pattern": {"artifact_type": "tweet", "source": "twitter"},
                "confidence": 0.95,
                "support_count": 10,
                "correct_count": 10,
            },
            {
                "rule_id": "paper-arxiv",
                "projection_id": "paper_library",
                "pattern": {"artifact_type": "paper", "source": "arxiv"},
                "confidence": 0.9,
                "support_count": 5,
                "correct_count": 5,
            },
        ],
    )


def _queue_entry(
    db: MetadataDB,
    artifact_id: str,
    artifact_type: str,
    source: str,
    payload: dict[str, Any] | None = None,
) -> IngestionQueueEntry:
    entry = IngestionQueueEntry(
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        source=source,
        payload_json=json.dumps(payload or {"id": artifact_id, "source_type": source}),
        created_at="2026-08-30T00:00:00Z",
    )
    db.upsert_ingestion_entry(entry)
    return db.get_ingestion_entry(artifact_id)


def _review_entry(
    db: MetadataDB,
    artifact_id: str,
    artifact_type: str,
    source: str,
    classification_event: dict[str, Any],
) -> IngestionQueueEntry:
    entry = _queue_entry(db, artifact_id, artifact_type, source)
    review_json = json.dumps(
        {
            "events": [
                {
                    "action": "review_required",
                    "actor": "system",
                    "at": "2026-08-30T00:00:00Z",
                    "category": "classification",
                    "metadata": classification_event,
                    "status": "needs_review",
                }
            ],
            "state": {"status": "needs_review", "category": "classification"},
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    db.mark_ingestion_review_required(
        artifact_id,
        category="classification",
        reason="classification review",
        actor="system",
        metadata={"classification": classification_event},
    )
    # Overwrite review_json directly so the event shape matches the surface parser.
    with db._get_connection() as conn:
        conn.execute(
            "UPDATE ingestion_queue SET review_json = ? WHERE artifact_id = ?",
            (review_json, artifact_id),
        )
    return db.get_ingestion_entry(artifact_id)


@pytest.fixture
def restore_config():
    original = deepcopy(_runtime_config.data)
    yield
    _runtime_config.data = original


@pytest.fixture
def svc(tmp_path: Path, monkeypatch, restore_config):
    monkeypatch.chdir(tmp_path)
    _configure_config(tmp_path)
    layout = build_path_layout(_runtime_config)
    db = MetadataDB(str(layout.database_path))
    return ClassificationReviewService(db, config=_runtime_config)


def test_service_seeds_default_policy_when_none_exists(svc: ClassificationReviewService):
    policy = svc.get_active_policy()
    assert policy.version == 1
    assert "tweet_markdown" in policy.projections
    assert policy.confidence_threshold == pytest.approx(0.85)


def test_review_surface_lists_only_classification_items(svc: ClassificationReviewService):
    db = svc.db
    _review_entry(
        db,
        "low-conf-1",
        "note",
        "manual",
        {
            "artifact_id": "low-conf-1",
            "artifact_type": "note",
            "source": "manual",
            "projection_id": None,
            "confidence": 0.0,
            "reasons": ["no matching routing rule"],
            "evidence": {"features": {"artifact_type": "note", "source": "manual", "tags": []}},
            "alternatives": [],
            "action": "review",
            "gated": False,
        },
    )
    _queue_entry(db, "security-1", "paper", "arxiv", {"id": "security-1"})
    db.mark_ingestion_review_required(
        "security-1",
        category="security_policy",
        reason="security review required",
        actor="system",
    )

    items = svc.list_review_items()
    assert len(items) == 1
    assert items[0]["artifact_id"] == "low-conf-1"
    assert items[0]["classification"]["category"] == "classification"


def test_approve_records_decision_and_resolves_routing(svc: ClassificationReviewService):
    db = svc.db
    _review_entry(
        db,
        "tweet-1",
        "tweet",
        "twitter",
        {
            "artifact_id": "tweet-1",
            "artifact_type": "tweet",
            "source": "twitter",
            "projection_id": "tweet_markdown",
            "confidence": 0.95,
            "reasons": ["rule match"],
            "evidence": {
                "features": {"artifact_type": "tweet", "source": "twitter", "tags": []},
                "matched_rules": [],
            },
            "alternatives": [],
            "action": "review",
            "gated": False,
        },
    )

    result = svc.approve("tweet-1", actor="operator", reason="looks right")
    assert result["status"] == "pending"
    assert result["projection_id"] == "tweet_markdown"

    decisions = svc.store.list_decisions(artifact_id="tweet-1")
    assert len(decisions) == 1
    assert decisions[0].action == "approve"
    assert decisions[0].actual_projection_id == "tweet_markdown"

    entry = db.get_ingestion_entry("tweet-1")
    assert "classification_resolved" not in entry.payload_json


def test_generic_review_actions_record_classification_decisions(
    svc: ClassificationReviewService,
):
    db = svc.db
    event = {
        "artifact_id": "tweet-generic",
        "artifact_type": "tweet",
        "source": "twitter",
        "projection_id": "tweet_markdown",
        "confidence": 0.5,
        "reasons": ["low confidence"],
        "evidence": {
            "features": {"artifact_type": "tweet", "source": "twitter", "tags": []}
        },
        "alternatives": [],
        "action": "review",
        "gated": False,
    }
    _review_entry(db, "tweet-generic", "tweet", "twitter", event)
    queue = ArtifactReviewQueueService(db, config=_runtime_config)

    assert queue.retry(
        "tweet-generic", actor="operator", reason="approve proposed route"
    ).status == "pending"
    assert svc.store.list_decisions(artifact_id="tweet-generic")[0].action == "approve"

    _review_entry(db, "tweet-reviewed", "tweet", "twitter", event | {
        "artifact_id": "tweet-reviewed"
    })
    with pytest.raises(ArtifactReviewQueueError, match="require approve"):
        queue.mark_reviewed(
            "tweet-reviewed", actor="operator", reason="just mark it done"
        )
    assert queue.reject(
        "tweet-reviewed", actor="operator", reason="reject route"
    ).status == "rejected"
    assert svc.store.list_decisions(artifact_id="tweet-reviewed")[0].action == "reject"


def test_reject_records_decision_and_terminal_status(svc: ClassificationReviewService):
    db = svc.db
    _review_entry(
        db,
        "noise-1",
        "note",
        "manual",
        {
            "artifact_id": "noise-1",
            "artifact_type": "note",
            "source": "manual",
            "projection_id": None,
            "confidence": 0.0,
            "reasons": ["no matching routing rule"],
            "evidence": {"features": {"artifact_type": "note", "source": "manual", "tags": []}},
            "alternatives": [],
            "action": "review",
            "gated": False,
        },
    )

    result = svc.reject("noise-1", actor="operator", reason="not useful")
    assert result["status"] == "rejected"
    assert result["projection_id"] is None

    decisions = svc.store.list_decisions(artifact_id="noise-1")
    assert decisions[0].action == "reject"
    assert decisions[0].actual_projection_id is None


def test_correct_records_server_owned_decision_without_mutating_source_payload(
    svc: ClassificationReviewService,
):
    db = svc.db
    _review_entry(
        db,
        "paper-1",
        "paper",
        "arxiv",
        {
            "artifact_id": "paper-1",
            "artifact_type": "paper",
            "source": "arxiv",
            "projection_id": "tweet_markdown",
            "confidence": 0.5,
            "reasons": ["low confidence"],
            "evidence": {"features": {"artifact_type": "paper", "source": "arxiv", "tags": []}},
            "alternatives": [
                {"projection_id": "paper_library", "confidence": 0.9, "rule_id": "paper-arxiv"}
            ],
            "action": "review",
            "gated": False,
        },
    )

    result = svc.correct(
        "paper-1",
        projection_id="paper_library",
        actor="operator",
        reason="should go to papers",
    )
    assert result["status"] == "pending"
    assert result["projection_id"] == "paper_library"

    decisions = svc.store.list_decisions(artifact_id="paper-1")
    assert decisions[0].action == "correct"
    assert decisions[0].actual_projection_id == "paper_library"

    entry = db.get_ingestion_entry("paper-1")
    assert "classification_resolved" not in entry.payload_json


def test_correct_unknown_projection_raises(svc: ClassificationReviewService):
    db = svc.db
    _review_entry(
        db,
        "bad-1",
        "note",
        "manual",
        {
            "artifact_id": "bad-1",
            "artifact_type": "note",
            "source": "manual",
            "projection_id": None,
            "confidence": 0.0,
            "reasons": ["no matching routing rule"],
            "evidence": {"features": {"artifact_type": "note", "source": "manual", "tags": []}},
            "alternatives": [],
            "action": "review",
            "gated": False,
        },
    )
    with pytest.raises(ClassificationReviewError, match="unknown projection"):
        svc.correct("bad-1", projection_id="no_such", actor="operator", reason="oops")


def test_figure_it_out_proposes_revision_after_repeated_corrections(
    svc: ClassificationReviewService,
):
    db = svc.db
    # Seed 5 identical corrections for a pattern not covered by existing rules.
    artifact_ids: list[str] = []
    for i in range(5):
        artifact_id = f"paper-manual-{i}"
        artifact_ids.append(artifact_id)
        _review_entry(
            db,
            artifact_id,
            "paper",
            "manual",
            {
                "artifact_id": artifact_id,
                "artifact_type": "paper",
                "source": "manual",
                "projection_id": None,
                "confidence": 0.0,
                "reasons": ["no matching routing rule"],
                "evidence": {
                    "features": {"artifact_type": "paper", "source": "manual", "tags": []}
                },
                "alternatives": [],
                "action": "review",
                "gated": False,
            },
        )
        svc.correct(
            artifact_id,
            projection_id="paper_library",
            actor="operator",
            reason="manual papers go to library",
        )

    # Force a deterministic held-out split so coverage/precision are measurable.
    held_out_ids = {artifact_ids[0], artifact_ids[3]}
    svc._evaluator.is_held_out = lambda artifact_id: artifact_id in held_out_ids

    proposal = svc.figure_it_out(actor="operator", reason="repeated manual paper corrections")
    assert proposal is not None
    assert proposal["rules_added"] == 1
    assert proposal["metrics"]["candidate"]["review_volume"] < proposal["metrics"]["baseline"]["review_volume"]
    assert proposal["metrics"]["candidate"]["precision"] == pytest.approx(1.0)


def test_revision_activation_requires_improvement(svc: ClassificationReviewService):
    db = svc.db
    policy = svc.get_active_policy()
    # Create a proposed revision manually in the store.
    worse = RoutingPolicy(
        revision_id="rev-worse",
        version=policy.version + 1,
        projections=policy.projections,
        rules=policy.rules,
        confidence_threshold=1.0,  # higher threshold -> lower coverage
        previous_revision_id=policy.revision_id,
    )
    svc.store.save_proposed_revision(
        worse,
        metrics={},
        actor="operator",
        reason="worse threshold",
    )
    with pytest.raises(ClassificationReviewError, match="does not reduce review volume"):
        svc.activate_revision("rev-worse", actor="operator", reason="should fail")


def test_activate_and_rollback_policy_revision(svc: ClassificationReviewService):
    db = svc.db
    policy = svc.get_active_policy()
    # Seed decisions for a pattern not covered by the active policy.
    artifact_ids = [f"paper-manual-{i}" for i in range(5)]
    held_out_ids = {artifact_ids[0], artifact_ids[3]}
    for artifact_id in artifact_ids:
        _review_entry(
            db,
            artifact_id,
            "paper",
            "manual",
            {
                "artifact_id": artifact_id,
                "artifact_type": "paper",
                "source": "manual",
                "projection_id": None,
                "confidence": 0.0,
                "reasons": ["no matching routing rule"],
                "evidence": {
                    "features": {"artifact_type": "paper", "source": "manual", "tags": []}
                },
                "alternatives": [],
                "action": "review",
                "gated": False,
            },
        )
        svc.correct(
            artifact_id,
            projection_id="paper_library",
            actor="operator",
            reason="manual papers go to library",
        )
    svc._evaluator.is_held_out = lambda artifact_id: artifact_id in held_out_ids

    # Create a strictly better proposed revision.
    better = RoutingPolicy(
        revision_id="rev-better",
        version=policy.version + 1,
        projections=policy.projections,
        rules=(
            RoutingRule(
                rule_id="paper-manual",
                projection_id="paper_library",
                pattern={"artifact_type": "paper", "source": "manual"},
                confidence=1.0,
                support_count=5,
                correct_count=5,
            ),
        ),
        confidence_threshold=0.5,
        previous_revision_id=policy.revision_id,
    )
    svc.store.save_proposed_revision(
        better,
        metrics={},
        actor="operator",
        reason="add manual paper rule",
    )

    activated = svc.activate_revision("rev-better", actor="operator", reason="improves coverage")
    assert activated["revision_id"] == "rev-better"
    assert svc.get_active_policy().revision_id == "rev-better"

    rolled = svc.rollback(actor="operator", reason="revert experiment")
    assert rolled["revision_id"] == policy.revision_id
    assert svc.get_active_policy().revision_id == policy.revision_id
    assert svc.store.next_policy_version() == better.version + 1
