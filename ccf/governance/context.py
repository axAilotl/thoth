"""Decision contexts and canonical timestamp arithmetic (spec section 9.3).

A decision context is the complete, explicit input to an authorization
decision; there is no context-free "effective policy". Contexts are hashed
with the ``ccf:decision-context:v1`` domain separator so cached decisions
and egress capabilities can bind the exact request.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ccf.hashing import canonical_digest
from ccf.objects import validate_timestamp

DECISION_CONTEXT_SCHEMA = "urn:ccf:schema:0.1.1:governance.decision-context"

#: Destination marking local reads inside the archive control domain.
LOCAL_DESTINATION = "local"

_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def parse_timestamp(text: str) -> datetime:
    """Parse a canonical CCF timestamp into an aware UTC datetime."""
    validate_timestamp(text)
    return datetime.strptime(text, _TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)


def format_timestamp(value: datetime) -> str:
    """Format an aware datetime as a canonical CCF timestamp (milliseconds)."""
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def add_milliseconds(timestamp: str, milliseconds: int) -> str:
    """Canonical timestamp ``milliseconds`` after ``timestamp``."""
    return format_timestamp(
        parse_timestamp(timestamp) + timedelta(milliseconds=milliseconds)
    )


def build_decision_context(
    *,
    operation: str,
    purpose: str,
    requester: str,
    runtime: str,
    destination: str,
    object_ids: list[str],
    head_sequence: str,
    requested_at: str,
    recipient: str | None = None,
    jurisdiction: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    """Assemble a decision-context document (validated by the caller's engine)."""
    deduped = sorted(set(object_ids))
    return {
        "operation": operation,
        "purpose": purpose,
        "requester": requester,
        "recipient": recipient,
        "runtime": runtime,
        "destination": destination,
        "jurisdiction": dict(jurisdiction or {}),
        "requested_at": requested_at,
        "object_ids": deduped,
        "head_sequence": str(head_sequence),
        "extensions": dict(extensions or {}),
    }


def decision_context_hash(context: dict) -> str:
    """Canonical hash binding one exact decision context."""
    return canonical_digest("ccf:decision-context:v1", context)
