"""Authorization decisions, bounded pending, and the local decision cache.

Decision documents match ``governance/authorization-decision.schema.json``;
pending documents match ``operational/policy-pending.schema.json`` (spec
section 9.6). Pending is bounded: it always carries the dirty sequence, a
dependency estimate, a retry hint, and a request ID, and it resolves to
allow or deny as soon as the dependency (an admitted policy head, a
resolved objection) lands — evaluation itself is synchronous against the
local head, so nothing stays pending forever.

The cache serves only terminal (allow/deny) decisions whose recorded
generation vector matches every current governance fence and whose
``valid_until`` has not passed (spec section 9.5). Any fence advance —
tightening, widening, or unknown-direction — forces recomputation instead
of serving a stale allow.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from psycopg.types.json import Jsonb

from ccf.governance.context import parse_timestamp

AUTHORIZATION_DECISION_SCHEMA = "urn:ccf:schema:0.1.2:governance.authorization-decision"
POLICY_PENDING_SCHEMA = "urn:ccf:schema:0.1.2:operational.policy-pending"

DEFAULT_RETRY_AFTER_MS = 250


@dataclass
class AuthorizationResult:
    """One authorization answer: a decision plus any pending entries."""

    decision: dict
    context: dict
    pending: list[dict] = field(default_factory=list)
    from_cache: bool = False


def build_decision_document(
    *,
    decision: str,
    reason_codes: list[str],
    obligations: list[dict],
    policy_closure_hash: str,
    decision_context_hash: str,
    evaluated_at_head: str,
    generation_vector: dict[str, str],
    evaluator_profile: str,
    evaluator_version: str,
    valid_until: str | None,
) -> dict:
    """Assemble an authorization-decision document (schema-checked by caller)."""
    return {
        "decision": decision,
        "reason_codes": sorted(set(reason_codes)),
        "obligations": obligations,
        "policy_closure_hash": policy_closure_hash,
        "decision_context_hash": decision_context_hash,
        "evaluated_at_head": str(evaluated_at_head),
        "generation_vector": dict(generation_vector),
        "evaluator_profile": evaluator_profile,
        "evaluator_version": evaluator_version,
        "valid_until": valid_until,
    }


def build_pending_document(
    *,
    object_id: str,
    head_sequence: str,
    dirty_since_sequence: int,
    remaining_dependencies_estimate: int,
    retry_after_ms: int = DEFAULT_RETRY_AFTER_MS,
    request_id: str | None = None,
) -> dict:
    """Assemble a ``policy_resolution_pending`` document (spec section 9.6)."""
    return {
        "status": "policy_resolution_pending",
        "object_id": object_id,
        "head_sequence": str(head_sequence),
        "dirty_since_sequence": str(max(0, int(dirty_since_sequence))),
        "remaining_dependencies_estimate": max(1, int(remaining_dependencies_estimate)),
        "retry_after_ms": max(1, int(retry_after_ms)),
        "request_id": request_id or f"req-{uuid.uuid4().hex}",
    }


# ---------------------------------------------------------------------------
# Local decision cache (spec section 9.5)
# ---------------------------------------------------------------------------


def cached_decision(
    conn,
    *,
    archive_id: str,
    decision_context_hash: str,
    current_generations: dict[str, str],
    now: str,
) -> dict | None:
    """Return a cached terminal decision iff every generation still matches."""
    row = conn.execute(
        """
        SELECT decision_json, generation_vector, valid_until
        FROM governance_decision
        WHERE archive_id = %s AND decision_context_hash = %s
        """,
        (archive_id, decision_context_hash),
    ).fetchone()
    if row is None:
        return None
    decision_json, generation_vector, valid_until = row
    recorded = {key: str(value) for key, value in (generation_vector or {}).items()}
    if recorded != {key: str(value) for key, value in current_generations.items()}:
        return None
    if valid_until is not None and parse_timestamp(now) >= parse_timestamp(valid_until):
        return None
    return decision_json


def cache_decision(
    conn,
    *,
    archive_id: str,
    decision_context_hash: str,
    decision: dict,
    now: str,
) -> None:
    """Cache a terminal decision; pending is never cached."""
    if decision["decision"] not in ("allow", "deny"):
        return
    conn.execute(
        """
        INSERT INTO governance_decision (
            decision_context_hash, archive_id, decision_json, generation_vector,
            head_sequence, valid_until, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (decision_context_hash) DO UPDATE SET
            archive_id = EXCLUDED.archive_id,
            decision_json = EXCLUDED.decision_json,
            generation_vector = EXCLUDED.generation_vector,
            head_sequence = EXCLUDED.head_sequence,
            valid_until = EXCLUDED.valid_until,
            created_at = EXCLUDED.created_at
        """,
        (
            decision_context_hash,
            archive_id,
            Jsonb(decision),
            Jsonb(decision["generation_vector"]),
            decision["evaluated_at_head"],
            decision["valid_until"],
            now,
        ),
    )
