"""Fenced external (egress) capabilities (spec section 9.7).

An egress capability binds the exact operation and purpose, the exact
object set, requester/recipient/runtime/destination, the archive head, the
generation vector the authorizing decision was computed against, the
objects' availability, a short expiry, and a use count. The egress
boundary consumes it — one atomic decrement — and rechecks every
generation and the objects' availability before the consequential action
may proceed. Any governance mutation after issuance invalidates the
capability; a second ordinary read is not a substitute for this check.
"""

from __future__ import annotations

import uuid

from psycopg.types.json import Jsonb

from ccf.governance.context import add_milliseconds, parse_timestamp
from ccf.governance.errors import CapabilityError
from ccf.governance.fences import snapshot_fences
from ccf.hashing import canonical_digest

DEFAULT_EGRESS_TTL_MS = 300_000  # five minutes
DEFAULT_EGRESS_USES = 1


def issue_capability(
    conn,
    *,
    archive_id: str,
    context: dict,
    decision: dict,
    availability: dict[str, str],
    now: str,
    ttl_ms: int = DEFAULT_EGRESS_TTL_MS,
    uses: int = DEFAULT_EGRESS_USES,
) -> dict:
    """Issue a fenced egress capability over a fresh allow decision."""
    if decision.get("decision") != "allow":
        raise CapabilityError("egress capabilities require an allow decision")
    if ttl_ms <= 0 or uses <= 0:
        raise CapabilityError("egress capability ttl and uses must be positive")
    capability = {
        "operation": context["operation"],
        "purpose": context["purpose"],
        "object_ids": list(context["object_ids"]),
        "requester": context["requester"],
        "recipient": context["recipient"],
        "runtime": context["runtime"],
        "destination": context["destination"],
        "head_sequence": str(decision["evaluated_at_head"]),
        "generation_vector": dict(decision["generation_vector"]),
        "availability": dict(availability),
        "decision_context_hash": decision["decision_context_hash"],
        "expires_at": add_milliseconds(now, ttl_ms),
        "remaining_uses": int(uses),
    }
    capability_id = f"cap-{uuid.uuid4().hex}"
    capability_hash = canonical_digest("ccf:egress-capability:v1", capability)
    conn.execute(
        """
        INSERT INTO egress_capability (
            capability_id, archive_id, capability_hash, decision_context_hash,
            generation_vector, head_sequence, object_ids, availability,
            remaining_uses, expires_at, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            capability_id,
            archive_id,
            capability_hash,
            capability["decision_context_hash"],
            Jsonb(capability["generation_vector"]),
            capability["head_sequence"],
            Jsonb(capability["object_ids"]),
            Jsonb(capability["availability"]),
            capability["remaining_uses"],
            capability["expires_at"],
            now,
            now,
        ),
    )
    return {"capability_id": capability_id, "capability_hash": capability_hash, **capability}


def consume_capability(
    conn,
    *,
    archive_id: str,
    capability_id: str,
    now: str,
) -> dict:
    """Consume one use at the egress boundary, rechecking every fence.

    Raises :class:`CapabilityError` (fail closed) when the capability is
    unknown, expired, exhausted, generation-stale, or an object's
    availability changed since issuance.
    """
    row = conn.execute(
        """
        SELECT decision_context_hash, generation_vector, object_ids,
               availability, remaining_uses, expires_at
        FROM egress_capability
        WHERE archive_id = %s AND capability_id = %s
        FOR UPDATE
        """,
        (archive_id, capability_id),
    ).fetchone()
    if row is None:
        raise CapabilityError(f"unknown egress capability: {capability_id}")
    (
        decision_context_hash,
        generation_vector,
        object_ids,
        availability,
        remaining_uses,
        expires_at,
    ) = row
    if parse_timestamp(now) >= parse_timestamp(expires_at):
        raise CapabilityError(f"egress capability {capability_id} expired")
    if remaining_uses <= 0:
        raise CapabilityError(f"egress capability {capability_id} exhausted")

    recorded = {key: str(value) for key, value in (generation_vector or {}).items()}
    current = snapshot_fences(conn, archive_id)
    if recorded != current:
        raise CapabilityError(
            f"egress capability {capability_id} is generation-stale: "
            "a governance mutation landed after issuance"
        )

    for object_id in object_ids:
        state = availability_of(conn, object_id)
        if state != availability.get(object_id):
            raise CapabilityError(
                f"object {object_id} availability changed: "
                f"{availability.get(object_id)} -> {state}"
            )
        if state != "plaintext":
            raise CapabilityError(f"object {object_id} is not available ({state})")

    conn.execute(
        """
        UPDATE egress_capability
        SET remaining_uses = remaining_uses - 1, updated_at = %s
        WHERE capability_id = %s
        """,
        (now, capability_id),
    )
    return {
        "capability_id": capability_id,
        "decision_context_hash": decision_context_hash,
        "object_ids": list(object_ids),
        "consumed_at": now,
        "remaining_uses": int(remaining_uses) - 1,
    }


def availability_of(conn, object_id: str) -> str:
    row = conn.execute(
        "SELECT object_kind FROM object_header WHERE id = %s", (object_id,)
    ).fetchone()
    if row is None:
        return "unknown"
    if row[0] == "blob":
        blob = conn.execute(
            "SELECT state FROM blob_content WHERE blob_id = %s", (object_id,)
        ).fetchone()
        return blob[0] if blob else "unknown"
    compartment = conn.execute(
        """
        SELECT state FROM compartment
        WHERE object_id = %s AND compartment = 'semantic'
        """,
        (object_id,),
    ).fetchone()
    return compartment[0] if compartment else "unknown"
