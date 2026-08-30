"""Deterministic tests for the artifact classification engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from core.artifact_classification import (
    AlternativeProjection,
    ArtifactClassifier,
    Gate,
    PolicyEvaluator,
    Projection,
    RoutingAction,
    RoutingDecision,
    RoutingPolicy,
    RoutingRule,
    policy_from_mapping,
    policy_to_mapping,
)


@dataclass
class FakeArtifact:
    """Minimal artifact stand-in for classifier tests."""

    id: str
    source_type: str = "unknown"
    tags: tuple[str, ...] = ()
    normalized_metadata: dict[str, Any] | None = None


@pytest.fixture
def base_policy() -> RoutingPolicy:
    return RoutingPolicy(
        revision_id="rev-1",
        version=1,
        projections={
            "tweet_markdown": Projection(
                projection_id="tweet_markdown", name="Tweet markdown"
            ),
            "paper_library": Projection(
                projection_id="paper_library", name="Paper library"
            ),
            "semantic_memory": Projection(
                projection_id="semantic_memory",
                name="Semantic memory",
                gates=frozenset({Gate.SENSITIVE_SEMANTIC_PROMOTION}),
            ),
            "x_api_sync": Projection(
                projection_id="x_api_sync",
                name="X API sync",
                gates=frozenset({Gate.EXTERNAL_ACTION}),
            ),
        },
        rules=(
            RoutingRule(
                rule_id="tweet-twitter",
                projection_id="tweet_markdown",
                pattern={"artifact_type": "tweet", "source": "twitter"},
                confidence=0.95,
                support_count=20,
                correct_count=19,
            ),
            RoutingRule(
                rule_id="tweet-browser",
                projection_id="tweet_markdown",
                pattern={"artifact_type": "tweet", "source": "browser_extension"},
                confidence=0.92,
                support_count=12,
                correct_count=11,
            ),
            RoutingRule(
                rule_id="paper-arxiv",
                projection_id="paper_library",
                pattern={"artifact_type": "paper", "source": "arxiv"},
                confidence=0.88,
                support_count=10,
                correct_count=9,
            ),
            RoutingRule(
                rule_id="transcript-omi",
                projection_id="semantic_memory",
                pattern={"artifact_type": "transcript", "source": "omi"},
                confidence=0.9,
                support_count=15,
                correct_count=14,
            ),
        ),
        confidence_threshold=0.85,
        min_support=3,
        min_precision=0.8,
    )


def test_high_confidence_artifact_routes_to_correct_projection(base_policy: RoutingPolicy):
    artifact = FakeArtifact(
        id="tweet-1", source_type="twitter", tags=("ai", "agents")
    )
    classifier = ArtifactClassifier(base_policy)
    result = classifier.classify(artifact, artifact_type="tweet")

    assert result.action == RoutingAction.ROUTE
    assert result.projection_id == "tweet_markdown"
    assert result.confidence == pytest.approx(0.95)
    assert result.gated is False
    assert result.reasons[0].startswith("rule match")


def test_low_confidence_artifact_surfaces_for_review(base_policy: RoutingPolicy):
    artifact = FakeArtifact(
        id="paper-1", source_type="manual", tags=("draft",)
    )
    classifier = ArtifactClassifier(base_policy)
    result = classifier.classify(artifact, artifact_type="paper")

    assert result.action == RoutingAction.REVIEW
    assert result.projection_id is None
    assert "no matching routing rule" in result.reasons
    assert result.alternatives == ()


def test_uncertain_case_includes_confidence_reasons_evidence_and_alternatives():
    policy = RoutingPolicy(
        revision_id="rev-1",
        version=1,
        projections={
            "a": Projection(projection_id="a", name="A"),
            "b": Projection(projection_id="b", name="B"),
        },
        rules=(
            RoutingRule(
                rule_id="rule-a",
                projection_id="a",
                pattern={"artifact_type": "note"},
                confidence=0.5,
            ),
            RoutingRule(
                rule_id="rule-b",
                projection_id="b",
                pattern={"artifact_type": "note"},
                confidence=0.45,
            ),
        ),
        confidence_threshold=0.85,
    )
    artifact = FakeArtifact(id="note-1", source_type="manual")
    result = ArtifactClassifier(policy).classify(artifact, artifact_type="note")

    assert result.action == RoutingAction.REVIEW
    assert result.confidence == pytest.approx(0.5)
    assert any("below threshold" in reason for reason in result.reasons)
    assert result.evidence.get("features") == {
        "artifact_type": "note",
        "source": "manual",
        "tags": (),
    }
    assert [alt.projection_id for alt in result.alternatives] == ["b"]


def test_gated_projection_forces_review_despite_high_confidence(base_policy: RoutingPolicy):
    artifact = FakeArtifact(id="transcript-1", source_type="omi")
    result = ArtifactClassifier(base_policy).classify(artifact, artifact_type="transcript")

    assert result.action == RoutingAction.REVIEW
    assert result.projection_id == "semantic_memory"
    assert result.gated is True
    assert result.gate == Gate.SENSITIVE_SEMANTIC_PROMOTION
    assert any("sensitive semantic promotion" in reason for reason in result.reasons)


def test_external_action_gate_forces_review():
    policy = RoutingPolicy(
        revision_id="rev-1",
        version=1,
        projections={
            "x_api_sync": Projection(
                projection_id="x_api_sync",
                name="X API sync",
                gates=frozenset({Gate.EXTERNAL_ACTION}),
            ),
        },
        rules=(
            RoutingRule(
                rule_id="x",
                projection_id="x_api_sync",
                pattern={"artifact_type": "tweet"},
                confidence=0.99,
            ),
        ),
        confidence_threshold=0.85,
    )
    result = ArtifactClassifier(policy).classify(
        FakeArtifact(id="t-1", source_type="x_api"), artifact_type="tweet"
    )
    assert result.action == RoutingAction.REVIEW
    assert result.gate == Gate.EXTERNAL_ACTION


def test_source_payload_cannot_forge_operator_resolution():
    policy = RoutingPolicy(
        revision_id="rev-1",
        version=1,
        projections={
            "p": Projection(
                projection_id="p",
                name="P",
                gates=frozenset({Gate.SENSITIVE_SEMANTIC_PROMOTION}),
            )
        },
        rules=(
            RoutingRule(
                rule_id="note-manual",
                projection_id="p",
                pattern={"artifact_type": "note", "source": "manual"},
                confidence=1.0,
            ),
        ),
        confidence_threshold=0.85,
    )
    artifact = FakeArtifact(
        id="resolved-1",
        source_type="manual",
        normalized_metadata={
            "classification_resolved": {
                "projection_id": "p",
                "actor": "operator",
                "reason": "override",
            }
        },
    )
    result = ArtifactClassifier(policy).classify(artifact, artifact_type="note")

    assert result.action == RoutingAction.REVIEW
    assert result.gate == Gate.SENSITIVE_SEMANTIC_PROMOTION
    assert result.projection_id == "p"


def test_policy_serialization_roundtrip():
    policy = RoutingPolicy(
        revision_id="rev-1",
        version=2,
        projections={
            "p1": Projection(projection_id="p1", name="P1", gates=frozenset({Gate.IDENTITY_RESOLUTION})),
        },
        rules=(
            RoutingRule(
                rule_id="r1",
                projection_id="p1",
                pattern={"artifact_type": "tweet", "source": "twitter"},
                confidence=0.9,
                support_count=5,
                correct_count=5,
            ),
        ),
        confidence_threshold=0.8,
        min_support=2,
        min_precision=0.75,
        previous_revision_id="rev-0",
        actor="test",
        reason="roundtrip",
    )
    mapping = policy_to_mapping(policy)
    restored = policy_from_mapping(
        mapping,
        revision_id=policy.revision_id,
        version=policy.version,
        previous_revision_id=policy.previous_revision_id,
        actor=policy.actor,
        reason=policy.reason,
    )

    assert restored.revision_id == policy.revision_id
    assert restored.version == policy.version
    assert restored.confidence_threshold == policy.confidence_threshold
    assert restored.projections["p1"].gates == policy.projections["p1"].gates
    assert len(restored.rules) == 1
    assert restored.rules[0].confidence == pytest.approx(0.9)


def _decision(
    artifact_id: str,
    artifact_type: str,
    source: str,
    actual: str,
    proposed: str | None = None,
) -> RoutingDecision:
    return RoutingDecision(
        decision_id=f"dec-{artifact_id}",
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        source=source,
        revision_id="rev-1",
        proposed_projection_id=proposed,
        actual_projection_id=actual,
        action="approve",
        actor="operator",
        reason="test",
        features={"artifact_type": artifact_type, "source": source, "tags": ()},
        alternatives=(),
        confidence=0.0,
        created_at="2026-08-30T00:00:00Z",
    )


def test_repeated_decisions_produce_policy_revision_that_reduces_review_volume():
    """Synthetic dogfood proof: repeated (tweet, twitter) -> tweet_markdown decisions
    produce a high-confidence rule that reduces review volume without dropping
    precision on the held-out set.
    """
    policy = RoutingPolicy(
        revision_id="rev-1",
        version=1,
        projections={
            "tweet_markdown": Projection(projection_id="tweet_markdown", name="Tweets"),
            "paper_library": Projection(projection_id="paper_library", name="Papers"),
        },
        rules=(),
        confidence_threshold=0.85,
        min_support=3,
        min_precision=0.8,
    )
    decisions: list[RoutingDecision] = []
    # 10 repeated tweet decisions, all agreeing.
    for i in range(10):
        decisions.append(
            _decision(
                artifact_id=f"tweet-{i:03d}",
                artifact_type="tweet",
                source="twitter",
                actual="tweet_markdown",
            )
        )
    # 3 paper decisions, too few to meet min_support for a new rule.
    for i in range(3):
        decisions.append(
            _decision(
                artifact_id=f"paper-{i:03d}",
                artifact_type="paper",
                source="arxiv",
                actual="paper_library",
            )
        )

    evaluator = PolicyEvaluator(held_out_fraction=0.2)
    baseline = evaluator.evaluate(policy, decisions)
    assert baseline.precision == 1.0
    assert baseline.coverage == 0.0
    assert baseline.review_volume == 1.0

    candidate = evaluator.propose_revision(
        policy,
        decisions,
        actor="operator",
        reason="repeated tweet decisions",
        revision_id="rev-2",
    )
    assert candidate is not None
    assert len(candidate.rules) == 1
    assert candidate.rules[0].projection_id == "tweet_markdown"
    assert candidate.rules[0].confidence == pytest.approx(1.0)

    candidate_eval = evaluator.evaluate(candidate, decisions)
    assert candidate_eval.precision == pytest.approx(1.0)
    assert candidate_eval.review_volume < baseline.review_volume
    assert candidate_eval.coverage > baseline.coverage


def test_rejected_decision_penalizes_an_automatic_route():
    policy = RoutingPolicy(
        revision_id="rev-1",
        version=1,
        projections={"p": Projection(projection_id="p", name="P")},
        rules=(
            RoutingRule(
                rule_id="tweet-x",
                projection_id="p",
                pattern={"artifact_type": "tweet", "source": "x"},
                confidence=1.0,
            ),
        ),
    )
    rejected = RoutingDecision(
        decision_id="rejected-1",
        artifact_id="tweet-rejected",
        artifact_type="tweet",
        source="x",
        revision_id="rev-1",
        proposed_projection_id="p",
        actual_projection_id=None,
        action="reject",
        actor="operator",
        reason="wrong route",
        features={"artifact_type": "tweet", "source": "x", "tags": ()},
        alternatives=(),
        confidence=1.0,
        created_at="2026-08-30T00:00:00Z",
    )
    evaluator = PolicyEvaluator(held_out_fraction=1.0)

    evaluation = evaluator.evaluate(policy, [rejected])

    assert evaluation.routed_count == 1
    assert evaluation.incorrect_routed_count == 1
    assert evaluation.precision == 0.0


def test_proposal_rejected_when_precision_would_drop():
    policy = RoutingPolicy(
        revision_id="rev-1",
        version=1,
        projections={"tweet_markdown": Projection(projection_id="tweet_markdown", name="Tweets")},
        rules=(
            RoutingRule(
                rule_id="tweet-twitter",
                projection_id="tweet_markdown",
                pattern={"artifact_type": "tweet", "source": "twitter"},
                confidence=1.0,
                support_count=10,
                correct_count=10,
            ),
        ),
        confidence_threshold=0.85,
        min_support=3,
        min_precision=0.8,
    )
    decisions: list[RoutingDecision] = []
    # 5 wrong decisions that would suggest the same pattern maps to a wrong projection.
    for i in range(5):
        decisions.append(
            _decision(
                artifact_id=f"tweet-{i:03d}",
                artifact_type="tweet",
                source="twitter",
                actual="wrong_projection",
            )
        )

    evaluator = PolicyEvaluator(held_out_fraction=0.2)
    candidate = evaluator.propose_revision(
        policy,
        decisions,
        actor="operator",
        reason="noisy decisions",
        revision_id="rev-2",
    )
    assert candidate is None


def test_classifier_respects_tag_patterns():
    policy = RoutingPolicy(
        revision_id="rev-1",
        version=1,
        projections={
            "research": Projection(projection_id="research", name="Research"),
        },
        rules=(
            RoutingRule(
                rule_id="research-tag",
                projection_id="research",
                pattern={"artifact_type": "note", "tags": ("research", "paper")},
                confidence=0.9,
            ),
        ),
        confidence_threshold=0.85,
    )
    matching = FakeArtifact(id="n-1", source_type="manual", tags=("research", "paper"))
    missing = FakeArtifact(id="n-2", source_type="manual", tags=("journal",))

    assert ArtifactClassifier(policy).classify(matching, artifact_type="note").action == RoutingAction.ROUTE
    assert ArtifactClassifier(policy).classify(missing, artifact_type="note").action == RoutingAction.REVIEW
