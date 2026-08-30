"""Operator review surface for artifact classification routing decisions."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from .artifact_classification import (
    AlternativeProjection,
    ArtifactClassifier,
    ClassificationResult,
    PolicyEvaluator,
    PolicyRevisionError,
    RoutingAction,
    RoutingDecision,
    RoutingPolicy,
    policy_from_mapping,
    policy_to_mapping,
)
from .classification_store import ClassificationStore, ClassificationStoreError
from .config import config as _runtime_config
from .metadata_db import MetadataDB
from .time_utils import utc_now_iso as _now_iso


class ClassificationReviewError(RuntimeError):
    """Raised when a classification review action cannot be completed."""


class ClassificationReviewService:
    """List and resolve uncertain artifact routing decisions.

    Provides a single review surface for classification uncertainty.  Actions
    are durable, person-scoped, and feed the versioned policy-revision loop.
    """

    def __init__(
        self,
        db: MetadataDB | None = None,
        *,
        config=None,
        store: ClassificationStore | None = None,
    ):
        self.db = db or _metadata_db()
        self.config = config if config is not None else _runtime_config
        self.store = store or ClassificationStore(self.db)
        self._evaluator = PolicyEvaluator(
            held_out_fraction=self.config.get(
                "classification.held_out_fraction", 0.2
            )
        )

    def get_active_policy(self) -> RoutingPolicy:
        """Return the active routing policy, seeding a default if absent."""
        active = self.store.get_active_revision()
        if active is not None:
            return active
        default = _default_policy(self.config)
        self.store.seed_initial_policy(
            default,
            actor="system",
            reason="default classification policy seeded",
        )
        return default

    def list_review_items(
        self,
        *,
        status: str | None = "needs_review",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return uncertain/gated classification cases from the review queue."""
        entries = self.db.list_ingestion_review_entries(
            status=status,
            include_closed=(status is None),
            limit=limit,
        )
        items: list[dict[str, Any]] = []
        for entry in entries:
            classification = _extract_classification_event(entry.review_json)
            if classification is None:
                continue
            items.append(
                {
                    "artifact_id": entry.artifact_id,
                    "artifact_type": entry.artifact_type,
                    "source": entry.source,
                    "status": entry.status,
                    "classification": classification.to_review_event(),
                }
            )
        return items

    def is_review_item(self, artifact_id: str) -> bool:
        """Return whether an ingestion row carries a classification review."""
        entry = self.db.get_ingestion_entry(artifact_id)
        return bool(entry and _extract_classification_event(entry.review_json))

    def approve(
        self,
        artifact_id: str,
        *,
        actor: str,
        reason: str,
    ) -> dict[str, Any]:
        """Approve the classifier's proposed projection."""
        entry, classification = self._require_classification_review(artifact_id)
        policy = self.get_active_policy()
        proposed = classification.projection_id
        if not proposed:
            raise ClassificationReviewError(
                f"artifact {artifact_id} has no proposed projection to approve"
            )
        decision = self._build_decision(
            entry=entry,
            classification=classification,
            action="approve",
            actor=actor,
            reason=reason,
            actual_projection_id=proposed,
            revision_id=policy.revision_id,
        )
        updated = self.db.apply_classification_review_decision(
            decision,
            status="pending",
            transition_action="classification_approved",
            actor=actor,
            reason=reason,
        )
        return {"artifact_id": artifact_id, "status": updated.status, "projection_id": proposed}

    def reject(
        self,
        artifact_id: str,
        *,
        actor: str,
        reason: str,
    ) -> dict[str, Any]:
        """Reject routing for this artifact."""
        entry, classification = self._require_classification_review(artifact_id)
        policy = self.get_active_policy()
        decision = self._build_decision(
            entry=entry,
            classification=classification,
            action="reject",
            actor=actor,
            reason=reason,
            actual_projection_id=None,
            revision_id=policy.revision_id,
        )
        updated = self.db.apply_classification_review_decision(
            decision,
            status="rejected",
            transition_action="classification_rejected",
            actor=actor,
            reason=reason,
        )
        return {"artifact_id": artifact_id, "status": updated.status, "projection_id": None}

    def correct(
        self,
        artifact_id: str,
        *,
        projection_id: str,
        actor: str,
        reason: str,
    ) -> dict[str, Any]:
        """Override the classifier with an explicit projection choice."""
        entry, classification = self._require_classification_review(artifact_id)
        policy = self.get_active_policy()
        if projection_id not in policy.projections:
            raise ClassificationReviewError(
                f"unknown projection: {projection_id}"
            )
        decision = self._build_decision(
            entry=entry,
            classification=classification,
            action="correct",
            actor=actor,
            reason=reason,
            actual_projection_id=projection_id,
            revision_id=policy.revision_id,
        )
        updated = self.db.apply_classification_review_decision(
            decision,
            status="pending",
            transition_action="classification_corrected",
            actor=actor,
            reason=reason,
        )
        return {"artifact_id": artifact_id, "status": updated.status, "projection_id": projection_id}

    def figure_it_out(
        self,
        *,
        actor: str,
        reason: str,
    ) -> dict[str, Any] | None:
        """Propose a versioned routing-policy revision from recent decisions.

        Returns the proposed revision metadata, or None when no safe improvement
        is found.
        """
        policy = self.get_active_policy()
        decisions = self.store.list_decisions(limit=10000)
        revision_id = _new_decision_id()
        candidate = self._evaluator.propose_revision(
            policy,
            decisions,
            actor=actor,
            reason=reason,
            revision_id=revision_id,
        )
        if candidate is None:
            return None
        candidate = replace(candidate, version=self.store.next_policy_version())
        baseline_eval = self._evaluator.evaluate(policy, decisions)
        candidate_eval = self._evaluator.evaluate(candidate, decisions)
        saved = self.store.save_proposed_revision(
            candidate,
            metrics={
                "baseline": {
                    "precision": baseline_eval.precision,
                    "coverage": baseline_eval.coverage,
                    "review_volume": baseline_eval.review_volume,
                    "held_out_count": baseline_eval.held_out_count,
                },
                "candidate": {
                    "precision": candidate_eval.precision,
                    "coverage": candidate_eval.coverage,
                    "review_volume": candidate_eval.review_volume,
                    "held_out_count": candidate_eval.held_out_count,
                },
            },
            actor=actor,
            reason=reason,
        )
        return {
            "revision_id": saved.revision_id,
            "version": saved.version,
            "previous_revision_id": saved.previous_revision_id,
            "rules_added": max(0, len(saved.rules) - len(policy.rules)),
            "metrics": {
                "baseline": {
                    "precision": baseline_eval.precision,
                    "coverage": baseline_eval.coverage,
                    "review_volume": baseline_eval.review_volume,
                },
                "candidate": {
                    "precision": candidate_eval.precision,
                    "coverage": candidate_eval.coverage,
                    "review_volume": candidate_eval.review_volume,
                },
            },
        }

    def activate_revision(
        self,
        revision_id: str,
        *,
        actor: str,
        reason: str,
    ) -> dict[str, Any]:
        """Activate a proposed revision after re-evaluating on held-out data."""
        proposed = self.store.get_revision(revision_id)
        if proposed is None:
            raise ClassificationReviewError(
                f"proposed revision not found: {revision_id}"
            )
        decisions = self.store.list_decisions(limit=10000)
        candidate_eval = self._evaluator.evaluate(proposed, decisions)
        active = self.get_active_policy()
        baseline_eval = self._evaluator.evaluate(active, decisions)
        if candidate_eval.precision + 1e-9 < baseline_eval.precision:
            raise ClassificationReviewError(
                "proposed revision would reduce held-out precision"
            )
        if candidate_eval.review_volume + 1e-9 >= baseline_eval.review_volume:
            raise ClassificationReviewError(
                "proposed revision does not reduce review volume"
            )
        activated = self.store.activate_revision(
            revision_id,
            actor=actor,
            reason=reason,
            metrics={
                "precision": candidate_eval.precision,
                "coverage": candidate_eval.coverage,
                "review_volume": candidate_eval.review_volume,
                "held_out_count": candidate_eval.held_out_count,
            },
        )
        return {
            "revision_id": activated.revision_id,
            "version": activated.version,
            "previous_revision_id": activated.previous_revision_id,
            "metrics": {
                "precision": candidate_eval.precision,
                "coverage": candidate_eval.coverage,
                "review_volume": candidate_eval.review_volume,
            },
        }

    def rollback(self, *, actor: str, reason: str) -> dict[str, Any]:
        """Roll back the active revision to its predecessor."""
        restored = self.store.rollback_active_revision(actor=actor, reason=reason)
        return {
            "revision_id": restored.revision_id,
            "version": restored.version,
            "previous_revision_id": restored.previous_revision_id,
        }

    def evaluate_active_policy(self) -> dict[str, Any]:
        """Return held-out metrics for the currently active policy."""
        policy = self.get_active_policy()
        decisions = self.store.list_decisions(limit=10000)
        evaluation = self._evaluator.evaluate(policy, decisions)
        return {
            "revision_id": policy.revision_id,
            "version": policy.version,
            "held_out_count": evaluation.held_out_count,
            "routed_count": evaluation.routed_count,
            "correct_routed_count": evaluation.correct_routed_count,
            "incorrect_routed_count": evaluation.incorrect_routed_count,
            "precision": evaluation.precision,
            "coverage": evaluation.coverage,
            "review_volume": evaluation.review_volume,
        }

    def _require_classification_review(
        self, artifact_id: str
    ) -> tuple[Any, ClassificationResult]:
        entry = self.db.get_ingestion_entry(artifact_id)
        if entry is None:
            raise ClassificationReviewError(f"artifact not found: {artifact_id}")
        classification = _extract_classification_event(entry.review_json)
        if classification is None:
            raise ClassificationReviewError(
                f"artifact {artifact_id} is not a classification review item"
            )
        return entry, classification

    def operator_resolution(
        self,
        artifact_id: str,
        *,
        policy: RoutingPolicy,
    ) -> RoutingDecision | None:
        """Return the latest durable operator routing resolution, if any."""
        for decision in self.store.list_decisions(artifact_id=artifact_id, limit=20):
            if decision.action not in {"approve", "correct"}:
                continue
            projection_id = decision.actual_projection_id
            if not projection_id or projection_id not in policy.projections:
                raise ClassificationReviewError(
                    f"operator resolution for {artifact_id} references unknown "
                    f"projection {projection_id!r}"
                )
            return decision
        return None

    def record_auto_route(
        self,
        *,
        artifact_id: str,
        artifact_type: str,
        source: str,
        classification: ClassificationResult,
    ) -> RoutingDecision:
        """Persist a successful automatic route as evaluation evidence."""
        policy = self.get_active_policy()
        recent = self.store.list_decisions(artifact_id=artifact_id, limit=20)
        for decision in recent:
            if (
                decision.action == "auto_route"
                and decision.revision_id == policy.revision_id
                and decision.actual_projection_id == classification.projection_id
            ):
                return decision
        decision = RoutingDecision(
            decision_id=_new_decision_id(),
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            source=source,
            revision_id=policy.revision_id,
            proposed_projection_id=classification.projection_id,
            actual_projection_id=classification.projection_id,
            action="auto_route",
            actor="system",
            reason="high-confidence automatic route",
            features=classification.evidence.get("features", {}),
            alternatives=classification.alternatives,
            confidence=classification.confidence,
            created_at=_now_iso(),
        )
        return self.store.record_decision(decision)

    def _build_decision(
        self,
        *,
        entry: Any,
        classification: ClassificationResult,
        action: str,
        actor: str,
        reason: str,
        actual_projection_id: str | None,
        revision_id: str,
    ) -> RoutingDecision:
        decision = RoutingDecision(
            decision_id=_new_decision_id(),
            artifact_id=entry.artifact_id,
            artifact_type=entry.artifact_type,
            source=entry.source,
            revision_id=revision_id,
            proposed_projection_id=classification.projection_id,
            actual_projection_id=actual_projection_id,
            action=action,
            actor=actor,
            reason=reason,
            features=classification.evidence.get("features", {}),
            alternatives=classification.alternatives,
            confidence=classification.confidence,
            created_at=_now_iso(),
        )
        return decision


def _extract_classification_event(review_json: str | None) -> ClassificationResult | None:
    """Parse the most recent classification event from a review audit."""
    payload = _json_payload(review_json)
    events = payload.get("events") if isinstance(payload, Mapping) else None
    if not isinstance(events, list):
        return None
    for event in reversed(events):
        if not isinstance(event, Mapping):
            continue
        if event.get("category") == "classification" or (
            isinstance(event.get("metadata"), Mapping)
            and event["metadata"].get("category") == "classification"
        ):
            return _classification_from_event(event)
    return None


def _classification_from_event(event: Mapping[str, Any]) -> ClassificationResult | None:
    metadata = event.get("metadata") if isinstance(event.get("metadata"), Mapping) else event
    if not isinstance(metadata, Mapping):
        return None
    alternatives: list[AlternativeProjection] = []
    for alt in metadata.get("alternatives", []):
        if isinstance(alt, Mapping):
            alternatives.append(
                AlternativeProjection(
                    projection_id=alt.get("projection_id", ""),
                    confidence=alt.get("confidence", 0.0),
                    rule_id=alt.get("rule_id"),
                )
            )
    return ClassificationResult(
        artifact_id=str(metadata.get("artifact_id", event.get("artifact_id", ""))),
        artifact_type=str(metadata.get("artifact_type", event.get("artifact_type", ""))),
        source=str(metadata.get("source", event.get("source", ""))),
        projection_id=metadata.get("projection_id"),
        confidence=metadata.get("confidence", 0.0),
        reasons=tuple(metadata.get("reasons", [])),
        evidence=metadata.get("evidence", {}),
        alternatives=tuple(alternatives),
        action=metadata.get("action", RoutingAction.REVIEW.value),
        gated=bool(metadata.get("gated", False)),
        gate=metadata.get("gate"),
    )


def _default_policy(config) -> RoutingPolicy:
    """Build a default policy from configuration, or a minimal safe default."""
    cfg = config.get("classification", {}) or {}
    mapping = {
        "projections": cfg.get(
            "projections",
            [
                {"projection_id": "default", "name": "Default routing"},
            ],
        ),
        "rules": cfg.get("rules", []),
        "confidence_threshold": cfg.get("confidence_threshold", 0.85),
        "min_support": cfg.get("min_support", 3),
        "min_precision": cfg.get("min_precision", 0.8),
    }
    return policy_from_mapping(
        mapping,
        revision_id=_new_decision_id(),
        version=1,
        actor="system",
        reason="default policy",
    )


def _json_payload(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _new_decision_id() -> str:
    return str(uuid.uuid4())


def _metadata_db() -> MetadataDB:
    from .metadata_db import get_metadata_db

    return get_metadata_db()


__all__ = [
    "ClassificationReviewError",
    "ClassificationReviewService",
]
