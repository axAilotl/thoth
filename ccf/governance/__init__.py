"""CCF 0.1.1 governance baseline (spec section 9, checklist phase 6).

Implements the pinned ``ccf-deny-overrides-v1`` evaluator over a
deterministic policy closure, decision contexts and results matching the
vendored governance schemas, ``governance.*`` generation fences advanced
atomically with admission, bounded ``policy_resolution_pending`` results,
short-expiry use-counted egress capabilities, and canonical consequential
receipts.

Everything in this package evaluates against the synchronized local head:
no network calls, no remote governance dependency for local reads
(spec section 9.4). Fresh authorization is required only at consequential
egress, exposed explicitly via ``GovernanceEngine.authorize_egress`` and
``consume_egress_capability``.
"""

from ccf.governance.context import (
    DECISION_CONTEXT_SCHEMA,
    LOCAL_DESTINATION,
    decision_context_hash,
)
from ccf.governance.decisions import (
    AUTHORIZATION_DECISION_SCHEMA,
    POLICY_PENDING_SCHEMA,
    AuthorizationResult,
)
from ccf.governance.engine import (
    CONSEQUENTIAL_OPERATIONS,
    GovernanceEngine,
)
from ccf.governance.errors import CapabilityError, GovernanceError
from ccf.governance.evaluator import EVALUATOR_PROFILE, DenyOverridesV1

__all__ = [
    "AUTHORIZATION_DECISION_SCHEMA",
    "AuthorizationResult",
    "CapabilityError",
    "CONSEQUENTIAL_OPERATIONS",
    "DECISION_CONTEXT_SCHEMA",
    "DenyOverridesV1",
    "EVALUATOR_PROFILE",
    "GovernanceEngine",
    "GovernanceError",
    "LOCAL_DESTINATION",
    "POLICY_PENDING_SCHEMA",
    "decision_context_hash",
]
