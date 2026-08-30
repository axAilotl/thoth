"""Deterministic artifact routing classifier and policy revision engine.

The classifier maps ingested artifacts to configured projections.  High-confidence
matches are routed automatically; uncertain or gated cases are surfaced for
person review.  Repeated review decisions feed a versioned policy-revision loop
that is evaluated against held-out historical decisions before activation.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from .time_utils import utc_now_iso as _now_iso


class Gate(str, Enum):
    """Routing gates that always require explicit review."""

    IDENTITY_RESOLUTION = "identity_resolution"
    SENSITIVE_SEMANTIC_PROMOTION = "sensitive_semantic_promotion"
    EXTERNAL_ACTION = "external_action"


GATE_REASONS = {
    Gate.IDENTITY_RESOLUTION: "identity resolution requires explicit approval",
    Gate.SENSITIVE_SEMANTIC_PROMOTION: "sensitive semantic promotion requires explicit approval",
    Gate.EXTERNAL_ACTION: "external action requires explicit approval",
}


class RoutingAction(str, Enum):
    """Outcome posture for a classification result."""

    ROUTE = "route"
    REVIEW = "review"


@dataclass(frozen=True)
class Projection:
    """A destination projection that an artifact can be routed to."""

    projection_id: str
    name: str
    gates: frozenset[Gate] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        object.__setattr__(self, "projection_id", _clean_id(self.projection_id))
        object.__setattr__(self, "name", str(self.name or self.projection_id).strip())
        gates = self.gates
        if isinstance(gates, str):
            gates = {gates}
        object.__setattr__(
            self,
            "gates",
            frozenset(
                Gate(g.value if isinstance(g, Gate) else str(g).strip())
                for g in gates
                if (g.value if isinstance(g, Gate) else str(g).strip())
            ),
        )


@dataclass(frozen=True)
class RoutingRule:
    """One deterministic pattern -> projection mapping."""

    rule_id: str
    projection_id: str
    pattern: dict[str, Any]
    confidence: float
    support_count: int = 0
    correct_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", _clean_id(self.rule_id))
        object.__setattr__(self, "projection_id", _clean_id(self.projection_id))
        object.__setattr__(self, "pattern", _clean_pattern(self.pattern))
        object.__setattr__(self, "confidence", _clamp_confidence(self.confidence))
        object.__setattr__(self, "support_count", max(0, int(self.support_count)))
        object.__setattr__(self, "correct_count", max(0, int(self.correct_count)))


@dataclass(frozen=True)
class RoutingPolicy:
    """A versioned set of projections and rules."""

    revision_id: str
    version: int
    projections: dict[str, Projection]
    rules: tuple[RoutingRule, ...]
    confidence_threshold: float = 0.85
    min_support: int = 3
    min_precision: float = 0.8
    previous_revision_id: str | None = None
    actor: str | None = None
    reason: str | None = None
    created_at: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "revision_id", _clean_id(self.revision_id))
        object.__setattr__(self, "version", max(1, int(self.version)))
        projections = {
            p.projection_id: p
            for p in (
                self.projections.values()
                if isinstance(self.projections, Mapping)
                else self.projections
            )
            if isinstance(p, Projection)
        }
        object.__setattr__(self, "projections", projections)
        rules = tuple(self.rules) if isinstance(self.rules, (list, tuple)) else ()
        object.__setattr__(self, "rules", rules)
        object.__setattr__(self, "confidence_threshold", _clamp_confidence(self.confidence_threshold))
        object.__setattr__(self, "min_support", max(1, int(self.min_support)))
        object.__setattr__(self, "min_precision", _clamp_confidence(self.min_precision))
        object.__setattr__(
            self,
            "previous_revision_id",
            _clean_optional(self.previous_revision_id),
        )
        object.__setattr__(self, "actor", _clean_optional(self.actor))
        object.__setattr__(self, "reason", _clean_optional(self.reason))
        object.__setattr__(self, "created_at", _clean_optional(self.created_at) or _now_iso())


@dataclass(frozen=True)
class AlternativeProjection:
    """A runner-up projection and its confidence."""

    projection_id: str
    confidence: float
    rule_id: str | None = None


@dataclass(frozen=True)
class ClassificationResult:
    """Outcome of classifying one artifact."""

    artifact_id: str
    artifact_type: str
    source: str
    projection_id: str | None
    confidence: float
    reasons: tuple[str, ...]
    evidence: dict[str, Any]
    alternatives: tuple[AlternativeProjection, ...]
    action: RoutingAction
    gated: bool = False
    gate: Gate | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifact_id", _clean_id(self.artifact_id))
        object.__setattr__(self, "artifact_type", _clean_id(self.artifact_type))
        object.__setattr__(self, "source", _clean_id(self.source))
        object.__setattr__(
            self, "projection_id", _clean_optional(self.projection_id)
        )
        object.__setattr__(self, "confidence", _clamp_confidence(self.confidence))
        object.__setattr__(self, "reasons", tuple(str(r) for r in self.reasons))
        object.__setattr__(self, "evidence", dict(self.evidence or {}))
        alternatives = tuple(self.alternatives) if self.alternatives else ()
        object.__setattr__(self, "alternatives", alternatives)
        action = self.action
        if isinstance(action, str):
            action = RoutingAction(action)
        object.__setattr__(self, "action", action)
        gate = self.gate
        if isinstance(gate, str):
            gate = Gate(gate)
        object.__setattr__(self, "gate", gate)

    def to_review_event(self) -> dict[str, Any]:
        """Serialize as an ingestion review event payload."""
        return {
            "category": "classification",
            "artifact_id": self.artifact_id,
            "artifact_type": self.artifact_type,
            "source": self.source,
            "projection_id": self.projection_id,
            "confidence": self.confidence,
            "reasons": list(self.reasons),
            "evidence": self.evidence,
            "alternatives": [
                {
                    "projection_id": alt.projection_id,
                    "confidence": alt.confidence,
                    "rule_id": alt.rule_id,
                }
                for alt in self.alternatives
            ],
            "action": self.action.value,
            "gated": self.gated,
            "gate": self.gate.value if self.gate else None,
        }


@dataclass(frozen=True)
class PolicyEvaluation:
    """Metrics from evaluating a policy on a held-out decision set."""

    held_out_count: int
    routed_count: int
    correct_routed_count: int
    incorrect_routed_count: int
    precision: float
    coverage: float
    review_volume: float


@dataclass(frozen=True)
class RoutingDecision:
    """One durable person decision used to train/ evaluate policies."""

    decision_id: str
    artifact_id: str
    artifact_type: str
    source: str
    revision_id: str | None
    proposed_projection_id: str | None
    actual_projection_id: str | None
    action: str
    actor: str | None
    reason: str | None
    features: dict[str, Any]
    alternatives: tuple[AlternativeProjection, ...]
    confidence: float
    created_at: str


class ClassificationError(RuntimeError):
    """Raised when classification logic cannot proceed safely."""


class PolicyRevisionError(ClassificationError):
    """Raised when a proposed policy revision is invalid or unsafe."""


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_id(value: Any) -> str:
    text = _clean_optional(value)
    if not text:
        raise ValueError("identifier is required")
    return text


def _clamp_confidence(value: Any) -> float:
    try:
        confidence = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"confidence must be a number: {value!r}") from exc
    return max(0.0, min(1.0, confidence))


def _clean_pattern(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("rule pattern must be a mapping")
    pattern: dict[str, Any] = {}
    for key, val in value.items():
        key = str(key).strip()
        if not key:
            continue
        if isinstance(val, (list, tuple, set)):
            pattern[key] = tuple(str(v).strip() for v in val if str(v).strip())
        elif isinstance(val, str):
            pattern[key] = val.strip()
        else:
            pattern[key] = val
    return pattern


def _features_for_artifact(
    artifact: Any,
    *,
    artifact_type: str | None = None,
) -> dict[str, Any]:
    """Extract deterministic classification features from a KnowledgeArtifact."""
    artifact_type = str(
        artifact_type
        or getattr(artifact, "artifact_type", None)
        or getattr(artifact, "__class__", object).__name__.replace("Artifact", "").lower()
    ).strip()
    source_type = str(getattr(artifact, "source_type", "") or "").strip()
    tags = getattr(artifact, "tags", None)
    if isinstance(tags, str):
        tags = [tags]
    elif not isinstance(tags, (list, tuple)):
        tags = []
    normalized_tags = tuple(str(t).strip().lower() for t in tags if str(t).strip())
    features: dict[str, Any] = {
        "artifact_type": artifact_type,
        "source": source_type,
        "tags": normalized_tags,
    }
    return features


def _rule_matches(rule: RoutingRule, features: Mapping[str, Any]) -> bool:
    """A rule matches when every required feature value is satisfied."""
    for key, required in rule.pattern.items():
        actual = features.get(key)
        if key == "tags" and isinstance(actual, (list, tuple)):
            required_set = set(required) if isinstance(required, (list, tuple)) else {required}
            if not required_set.issubset(set(actual)):
                return False
        elif isinstance(required, (list, tuple)):
            if actual not in required:
                return False
        else:
            if str(actual or "").strip().lower() != str(required or "").strip().lower():
                return False
    return True


def _projection_for_result(
    policy: RoutingPolicy,
    features: Mapping[str, Any],
) -> tuple[str | None, float, dict[str, Any], list[tuple[str, float, str | None]]]:
    """Return top projection id, confidence, evidence, and alternatives."""
    matches: list[tuple[str, float, str | None]] = []
    for rule in policy.rules:
        if _rule_matches(rule, features):
            matches.append((rule.projection_id, rule.confidence, rule.rule_id))
    if not matches:
        return None, 0.0, {"matched_rules": []}, []

    # Sort by confidence descending; stable by rule order as tie-breaker.
    matches.sort(key=lambda item: item[1], reverse=True)
    top_projection_id, top_confidence, top_rule_id = matches[0]

    # Build alternatives from other distinct projections.
    seen = {top_projection_id}
    alternatives: list[tuple[str, float, str | None]] = []
    for projection_id, confidence, rule_id in matches:
        if projection_id in seen:
            continue
        seen.add(projection_id)
        alternatives.append((projection_id, confidence, rule_id))

    evidence = {
        "matched_rules": [
            {
                "rule_id": rule_id,
                "projection_id": projection_id,
                "confidence": confidence,
            }
            for projection_id, confidence, rule_id in matches
        ],
        "features": dict(features),
    }
    return top_projection_id, top_confidence, evidence, alternatives


class ArtifactClassifier:
    """Classify artifacts against a routing policy."""

    def __init__(self, policy: RoutingPolicy):
        self.policy = policy

    def classify(
        self,
        artifact: Any,
        *,
        artifact_type: str | None = None,
    ) -> ClassificationResult:
        """Classify one artifact and return a routing decision."""
        features = _features_for_artifact(artifact, artifact_type=artifact_type)
        artifact_id = str(getattr(artifact, "id", "") or "unknown")
        artifact_type = str(features.get("artifact_type") or "unknown")
        source = str(features.get("source") or "unknown")

        projection_id, confidence, evidence, alternatives = _projection_for_result(
            self.policy, features
        )
        alternatives_tuple = tuple(
            AlternativeProjection(
                projection_id=projection_id,
                confidence=confidence,
                rule_id=rule_id,
            )
            for projection_id, confidence, rule_id in alternatives
        )

        reasons: list[str] = []
        gated = False
        gate: Gate | None = None

        if projection_id is None:
            reasons.append("no matching routing rule")
            action = RoutingAction.REVIEW
        else:
            projection = self.policy.projections.get(projection_id)
            if projection and projection.gates:
                gated = True
                gate = sorted(projection.gates, key=lambda g: g.value)[0]
                reasons.append(GATE_REASONS.get(gate, f"gate {gate.value} requires review"))
                action = RoutingAction.REVIEW
            elif confidence < self.policy.confidence_threshold:
                reasons.append(
                    f"confidence {confidence:.2f} below threshold "
                    f"{self.policy.confidence_threshold:.2f}"
                )
                action = RoutingAction.REVIEW
            else:
                reasons.append(
                    f"rule match with confidence {confidence:.2f} >= threshold"
                )
                action = RoutingAction.ROUTE

        return ClassificationResult(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            source=source,
            projection_id=projection_id,
            confidence=confidence,
            reasons=tuple(reasons),
            evidence=evidence,
            alternatives=alternatives_tuple,
            action=action,
            gated=gated,
            gate=gate,
        )


class PolicyEvaluator:
    """Evaluate and evolve routing policies from historical decisions."""

    def __init__(self, held_out_fraction: float = 0.2):
        self.held_out_fraction = max(0.0, min(1.0, float(held_out_fraction)))

    def is_held_out(self, artifact_id: str) -> bool:
        """Deterministic split based on artifact id hash."""
        digest = hashlib.sha256(str(artifact_id).encode("utf-8")).hexdigest()
        bucket = int(digest[:8], 16) % 100
        return bucket < int(self.held_out_fraction * 100)

    def evaluate(
        self,
        policy: RoutingPolicy,
        decisions: list[RoutingDecision],
    ) -> PolicyEvaluation:
        """Evaluate a policy on the held-out subset of decisions."""
        held_out = [d for d in decisions if self.is_held_out(d.artifact_id)]
        routed = 0
        correct = 0
        incorrect = 0
        classifier = ArtifactClassifier(policy)
        for decision in held_out:
            actual = decision.actual_projection_id
            features = dict(decision.features)
            # Build a lightweight artifact-like object for the classifier.
            pseudo = _PseudoArtifact(
                id=decision.artifact_id,
                artifact_type=features.get("artifact_type", decision.artifact_type),
                source_type=features.get("source", decision.source),
                tags=features.get("tags", ()),
                normalized_metadata={},
            )
            result = classifier.classify(pseudo)
            if result.action == RoutingAction.ROUTE:
                routed += 1
                if actual is not None and result.projection_id == actual:
                    correct += 1
                else:
                    incorrect += 1

        precision = correct / routed if routed else 1.0
        coverage = routed / len(held_out) if held_out else 0.0
        return PolicyEvaluation(
            held_out_count=len(held_out),
            routed_count=routed,
            correct_routed_count=correct,
            incorrect_routed_count=incorrect,
            precision=precision,
            coverage=coverage,
            review_volume=1.0 - coverage,
        )

    def propose_revision(
        self,
        policy: RoutingPolicy,
        decisions: list[RoutingDecision],
        *,
        actor: str,
        reason: str,
        revision_id: str,
    ) -> RoutingPolicy | None:
        """Generate a candidate policy revision from recent person decisions.

        Returns None when the candidate would not improve coverage without
        reducing precision on the held-out set.
        """
        # Only decisions that resolved to an actual projection are useful for
        # learning rules.
        trainable = [
            d
            for d in decisions
            if d.actual_projection_id and not self.is_held_out(d.artifact_id)
        ]
        if not trainable:
            return None

        # Group by deterministic feature fingerprint -> projection.
        groups: dict[tuple[str, str], dict[str, Any]] = {}
        for decision in trainable:
            features = dict(decision.features)
            fingerprint = _feature_fingerprint(features)
            key = (fingerprint, decision.actual_projection_id)
            group = groups.setdefault(
                key,
                {
                    "features": features,
                    "projection_id": decision.actual_projection_id,
                    "total": 0,
                    "correct": 0,
                },
            )
            group["total"] += 1
            # "Correct" relative to the existing proposed projection, if any.
            if (
                decision.proposed_projection_id is None
                or decision.proposed_projection_id == decision.actual_projection_id
            ):
                group["correct"] += 1

        new_rules: list[RoutingRule] = []
        for (fingerprint, projection_id), group in groups.items():
            total = group["total"]
            if total < policy.min_support:
                continue
            accuracy = group["correct"] / total
            if accuracy < policy.min_precision:
                continue
            pattern = _simplify_features(group["features"])
            if any(
                rule.projection_id == projection_id
                and rule.pattern == pattern
                for rule in policy.rules
            ):
                continue
            rule_id = f"rule:{fingerprint}:{projection_id}"
            new_rules.append(
                RoutingRule(
                    rule_id=rule_id,
                    projection_id=projection_id,
                    pattern=pattern,
                    confidence=accuracy,
                    support_count=total,
                    correct_count=group["correct"],
                )
            )

        if not new_rules:
            return None

        candidate = RoutingPolicy(
            revision_id=revision_id,
            version=policy.version + 1,
            projections=policy.projections,
            rules=tuple(policy.rules) + tuple(new_rules),
            confidence_threshold=policy.confidence_threshold,
            min_support=policy.min_support,
            min_precision=policy.min_precision,
            previous_revision_id=policy.revision_id,
            actor=actor,
            reason=reason,
        )

        baseline_eval = self.evaluate(policy, decisions)
        candidate_eval = self.evaluate(candidate, decisions)

        # Require the candidate to maintain or improve precision and reduce
        # review volume.
        if candidate_eval.precision + 1e-9 < baseline_eval.precision:
            return None
        if candidate_eval.review_volume + 1e-9 >= baseline_eval.review_volume:
            return None
        return candidate


class _PseudoArtifact:
    """Minimal artifact stand-in for policy evaluation."""

    def __init__(
        self,
        *,
        id: str,
        artifact_type: str,
        source_type: str,
        tags: tuple[str, ...],
        normalized_metadata: dict[str, Any],
    ) -> None:
        self.id = id
        self.artifact_type = artifact_type
        self.source_type = source_type
        self.tags = list(tags)
        self.normalized_metadata = normalized_metadata


def _feature_fingerprint(features: Mapping[str, Any]) -> str:
    """Stable, deterministic fingerprint for a feature set."""
    simplified = _simplify_features(features)
    payload = json.dumps(simplified, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _simplify_features(features: Mapping[str, Any]) -> dict[str, Any]:
    """Drop volatile fields, keep only the rule-relevant shape."""
    simplified: dict[str, Any] = {}
    for key in ("artifact_type", "source"):
        value = features.get(key)
        if value:
            simplified[key] = str(value).strip().lower()
    tags = features.get("tags")
    if isinstance(tags, (list, tuple)) and tags:
        simplified["tags"] = sorted(set(str(t).strip().lower() for t in tags if str(t).strip()))
    return simplified


def policy_from_mapping(
    mapping: Mapping[str, Any],
    *,
    revision_id: str,
    version: int = 1,
    previous_revision_id: str | None = None,
    actor: str | None = None,
    reason: str | None = None,
) -> RoutingPolicy:
    """Build a RoutingPolicy from a JSON-friendly mapping."""
    projections: dict[str, Projection] = {}
    for item in mapping.get("projections", []):
        projection = Projection(
            projection_id=item["projection_id"],
            name=item.get("name", item["projection_id"]),
            gates=frozenset(item.get("gates", [])),
        )
        projections[projection.projection_id] = projection

    rules: list[RoutingRule] = []
    for item in mapping.get("rules", []):
        rules.append(
            RoutingRule(
                rule_id=item["rule_id"],
                projection_id=item["projection_id"],
                pattern=item.get("pattern", {}),
                confidence=item.get("confidence", 0.0),
                support_count=item.get("support_count", 0),
                correct_count=item.get("correct_count", 0),
            )
        )

    return RoutingPolicy(
        revision_id=revision_id,
        version=version,
        projections=projections,
        rules=tuple(rules),
        confidence_threshold=mapping.get("confidence_threshold", 0.85),
        min_support=mapping.get("min_support", 3),
        min_precision=mapping.get("min_precision", 0.8),
        previous_revision_id=previous_revision_id,
        actor=actor,
        reason=reason,
    )


def policy_to_mapping(policy: RoutingPolicy) -> dict[str, Any]:
    """Serialize a RoutingPolicy to a JSON-friendly mapping."""
    return {
        "revision_id": policy.revision_id,
        "version": policy.version,
        "confidence_threshold": policy.confidence_threshold,
        "min_support": policy.min_support,
        "min_precision": policy.min_precision,
        "previous_revision_id": policy.previous_revision_id,
        "actor": policy.actor,
        "reason": policy.reason,
        "created_at": policy.created_at,
        "projections": [
            {
                "projection_id": p.projection_id,
                "name": p.name,
                "gates": sorted(g.value for g in p.gates),
            }
            for p in policy.projections.values()
        ],
        "rules": [
            {
                "rule_id": r.rule_id,
                "projection_id": r.projection_id,
                "pattern": dict(r.pattern),
                "confidence": r.confidence,
                "support_count": r.support_count,
                "correct_count": r.correct_count,
            }
            for r in policy.rules
        ],
    }


__all__ = [
    "AlternativeProjection",
    "ArtifactClassifier",
    "ClassificationError",
    "ClassificationResult",
    "Gate",
    "PolicyEvaluator",
    "PolicyEvaluation",
    "PolicyRevisionError",
    "Projection",
    "RoutingAction",
    "RoutingDecision",
    "RoutingPolicy",
    "RoutingRule",
    "policy_from_mapping",
    "policy_to_mapping",
]
