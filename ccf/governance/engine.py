"""The governance engine facade (spec sections 9.3-9.8).

``GovernanceEngine`` is the single entrypoint for contextual
authorization:

- :meth:`authorize` evaluates local reads against the synchronized local
  head — the local Postgres envelope only, never the network
  (spec section 9.4) — and serves cached terminal decisions while every
  governance generation matches (spec section 9.5);
- :meth:`authorize_egress` is the explicit consequential-egress boundary:
  it always evaluates fresh and, on allow, issues a short-expiry
  use-counted capability (spec section 9.7);
- :meth:`consume_egress_capability` is called by the key-unwrapping or
  egress boundary itself; it consumes one use and rechecks every fence;
- :meth:`record_consequential_receipt` admits the canonical receipt for a
  consequential disclosure or action (spec section 9.8).
"""

from __future__ import annotations

from ccf.db import open_ccf_connection
from ccf.governance import capabilities, decisions
from ccf.governance.closure import collect_policy_closure
from ccf.governance.context import (
    DECISION_CONTEXT_SCHEMA,
    LOCAL_DESTINATION,
    build_decision_context,
    decision_context_hash,
)
from ccf.governance.decisions import (
    AUTHORIZATION_DECISION_SCHEMA,
    POLICY_PENDING_SCHEMA,
    AuthorizationResult,
)
from ccf.governance.errors import GovernanceError
from ccf.governance.evaluator import EVALUATOR_PROFILE, DenyOverridesV1
from ccf.governance.fences import fence_last_change, snapshot_fences
from ccf.objects import now_timestamp

#: Operations that are consequential external actions (spec section 9.4):
#: they require fresh authorization and a fenced capability, never a cache
#: hit and never a local-read evaluation.
CONSEQUENTIAL_OPERATIONS: frozenset[str] = frozenset(
    {
        "disclose_external",
        "publish",
        "send_message",
        "spend",
        "destructive_remote",
        "model_training",
    }
)


class GovernanceEngine:
    """Contextual authorization over one archive's local canonical state."""

    def __init__(
        self,
        *,
        settings,
        archive_id: str,
        catalog,
        registries,
        schemas,
        clock=now_timestamp,
        archive=None,
    ) -> None:
        self._settings = settings
        self.archive_id = archive_id
        self.catalog = catalog
        self.registries = registries
        self.schemas = schemas
        self.clock = clock
        self._archive = archive
        # The evaluator profile and version are pinned by the semantic
        # catalog; constructing the evaluator fails closed otherwise.
        self._evaluator = DenyOverridesV1(
            registries.policy_evaluator(EVALUATOR_PROFILE)
        )

    @classmethod
    def from_archive(cls, archive) -> "GovernanceEngine":
        """Bind an engine to an :class:`ccf.archive.Archive` instance."""
        return cls(
            settings=archive.settings,
            archive_id=archive.archive_id,
            catalog=archive.catalog,
            registries=archive.registries,
            schemas=archive.schemas,
            clock=archive.clock,
            archive=archive,
        )

    # ------------------------------------------------------------------
    # Local authorization (spec sections 9.3-9.6)
    # ------------------------------------------------------------------

    def authorize(
        self,
        *,
        operation: str,
        purpose: str,
        requester: str,
        runtime: str,
        object_ids: list[str],
        destination: str = LOCAL_DESTINATION,
        recipient: str | None = None,
        jurisdiction: dict | None = None,
        requested_at: str | None = None,
        extensions: dict | None = None,
    ) -> AuthorizationResult:
        """Authorize a local operation against the synchronized local head."""
        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                context = self._build_context(
                    conn,
                    operation=operation,
                    purpose=purpose,
                    requester=requester,
                    runtime=runtime,
                    destination=destination,
                    recipient=recipient,
                    jurisdiction=jurisdiction,
                    requested_at=requested_at,
                    object_ids=object_ids,
                    extensions=extensions,
                )
                return self._evaluate(conn, context, use_cache=True)

    # ------------------------------------------------------------------
    # Consequential egress (spec sections 9.4 and 9.7)
    # ------------------------------------------------------------------

    def authorize_egress(
        self,
        *,
        operation: str,
        purpose: str,
        requester: str,
        runtime: str,
        object_ids: list[str],
        destination: str,
        recipient: str | None = None,
        jurisdiction: dict | None = None,
        requested_at: str | None = None,
        extensions: dict | None = None,
        ttl_ms: int = capabilities.DEFAULT_EGRESS_TTL_MS,
        uses: int = capabilities.DEFAULT_EGRESS_USES,
    ) -> tuple[AuthorizationResult, dict | None]:
        """Freshly authorize a consequential external action.

        On allow, issues the fenced egress capability in the same
        transaction, binding the exact decision context, head, generation
        vector, and availability the decision was computed against.
        """
        if operation not in CONSEQUENTIAL_OPERATIONS:
            raise GovernanceError(
                f"operation {operation!r} is not a consequential egress "
                f"operation {sorted(CONSEQUENTIAL_OPERATIONS)}"
            )
        if destination == LOCAL_DESTINATION:
            raise GovernanceError("egress authorization requires a non-local destination")
        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                context = self._build_context(
                    conn,
                    operation=operation,
                    purpose=purpose,
                    requester=requester,
                    runtime=runtime,
                    destination=destination,
                    recipient=recipient,
                    jurisdiction=jurisdiction,
                    requested_at=requested_at,
                    object_ids=object_ids,
                    extensions=extensions,
                )
                result = self._evaluate(conn, context, use_cache=False)
                capability = None
                if result.decision["decision"] == "allow":
                    capability = capabilities.issue_capability(
                        conn,
                        archive_id=self.archive_id,
                        context=context,
                        decision=result.decision,
                        availability=self._availability(conn, context["object_ids"]),
                        now=self.clock(),
                        ttl_ms=ttl_ms,
                        uses=uses,
                    )
                return result, capability

    def consume_egress_capability(self, capability_id: str) -> dict:
        """Consume one use at the egress boundary (fail closed on staleness)."""
        with open_ccf_connection(self._settings) as conn:
            with conn.transaction():
                return capabilities.consume_capability(
                    conn,
                    archive_id=self.archive_id,
                    capability_id=capability_id,
                    now=self.clock(),
                )

    # ------------------------------------------------------------------
    # Consequential receipts (spec section 9.8)
    # ------------------------------------------------------------------

    def record_consequential_receipt(
        self,
        *,
        context: dict,
        capability_id: str,
        consumption: dict,
        summary: str,
        status: str = "completed",
    ) -> str:
        """Admit the canonical receipt for a consumed consequential action."""
        if self._archive is None:
            raise GovernanceError(
                "consequential receipts require an engine bound to an Archive"
            )
        from ccf.governance.receipts import build_consequential_receipt

        spec = build_consequential_receipt(
            runtime_id=context["runtime"],
            recorded_at=self.clock(),
            context=context,
            capability_id=capability_id,
            consumption=consumption,
            summary=summary,
            status=status,
        )
        result = self._archive.admit_bootstrap([spec])
        return result["admitted"][0]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_context(self, conn, **kwargs) -> dict:
        head = conn.execute(
            "SELECT sequence FROM archive_head WHERE archive_id = %s",
            (self.archive_id,),
        ).fetchone()
        if head is None:
            raise GovernanceError(f"archive {self.archive_id} has no head")
        context = build_decision_context(
            head_sequence=str(int(head[0])),
            requested_at=kwargs.pop("requested_at", None) or self.clock(),
            **kwargs,
        )
        self.schemas.validate(
            DECISION_CONTEXT_SCHEMA, context, what="decision context"
        )
        return context

    def _evaluate(self, conn, context: dict, *, use_cache: bool) -> AuthorizationResult:
        context_hash = decision_context_hash(context)
        generation_vector = snapshot_fences(conn, self.archive_id)
        if use_cache:
            cached = decisions.cached_decision(
                conn,
                archive_id=self.archive_id,
                decision_context_hash=context_hash,
                current_generations=generation_vector,
                now=self.clock(),
            )
            if cached is not None:
                return AuthorizationResult(
                    decision=cached, context=context, from_cache=True
                )

        closure = collect_policy_closure(
            conn,
            archive_id=self.archive_id,
            registries=self.registries,
            object_ids=context["object_ids"],
            recipient=context["recipient"],
            generation_vector=generation_vector,
            head_sequence=context["head_sequence"],
        )
        evaluation = self._evaluator.evaluate(context, closure)
        decision = decisions.build_decision_document(
            decision=evaluation.decision,
            reason_codes=evaluation.reason_codes,
            obligations=evaluation.obligations,
            policy_closure_hash=closure.closure_hash,
            decision_context_hash=context_hash,
            evaluated_at_head=closure.head_sequence,
            generation_vector=generation_vector,
            evaluator_profile=self._evaluator.profile,
            evaluator_version=self._evaluator.version,
            valid_until=evaluation.valid_until,
        )
        self.schemas.validate(
            AUTHORIZATION_DECISION_SCHEMA, decision, what="authorization decision"
        )

        pending: list[dict] = []
        if evaluation.decision == "pending":
            head_sequence = int(closure.head_sequence)
            for verdict in evaluation.verdicts:
                if verdict.verdict != "pending":
                    continue
                dirty_since = head_sequence
                if verdict.pending_dependency:
                    changed = fence_last_change(
                        conn, self.archive_id, verdict.pending_dependency
                    )
                    if changed is not None:
                        dirty_since = changed
                document = decisions.build_pending_document(
                    object_id=verdict.object_id,
                    head_sequence=closure.head_sequence,
                    dirty_since_sequence=dirty_since,
                    remaining_dependencies_estimate=1,
                )
                self.schemas.validate(
                    POLICY_PENDING_SCHEMA, document, what="policy pending"
                )
                pending.append(document)
        else:
            decisions.cache_decision(
                conn,
                archive_id=self.archive_id,
                decision_context_hash=context_hash,
                decision=decision,
                now=self.clock(),
            )
        return AuthorizationResult(decision=decision, context=context, pending=pending)

    @staticmethod
    def _availability(conn, object_ids: list[str]) -> dict[str, str]:
        return {
            object_id: capabilities.availability_of(conn, object_id)
            for object_id in object_ids
        }
