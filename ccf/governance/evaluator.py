"""The pinned ``ccf-deny-overrides-v1`` baseline evaluator (spec section 9.1).

Combining algorithm, deterministically applied per object and then across
the object set (pending dominates deny, deny dominates allow):

- root default is deny: without an applicable allow from a base policy,
  the decision is deny;
- applicable deny rules override allows — from base policies and from
  destination overlays alike;
- mandatory obligations accumulate across every applicable allow and
  oblige rule (and an obligation that an active legal hold makes
  impossible turns the decision into deny);
- destination overlays may tighten (deny, oblige) but their allow rules
  never widen what the base closure allowed;
- unknown required context — an unresolved policy lineage, a rule
  condition referencing unavailable context, or a raised-but-unresolved
  objection — yields pending; structurally unknowable context (missing
  privacy block, incomplete subject coverage) yields deny;
- legal bases are alternatives: one covering active consent *or* one
  covering active legal-basis record suffices — never both at once.

The evaluator version is pinned by the semantic catalog through the
``ccf.policy-evaluators/0.1.2`` registry; construction fails closed when
the profile is not pinned.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ccf.governance.closure import GovernanceRecord, ObjectClosure, PolicyClosure
from ccf.governance.context import LOCAL_DESTINATION
from ccf.governance.errors import GovernanceError

EVALUATOR_PROFILE = "ccf-deny-overrides-v1"

#: Operations that destroy governed content; an active hold forbids them.
DESTRUCTIVE_OPERATIONS: frozenset[str] = frozenset({"erase", "delete", "purge"})

#: Obligations an active legal hold makes impossible to satisfy.
HOLD_INCOMPATIBLE_OBLIGATIONS: frozenset[str] = frozenset(
    {"erase", "delete", "destroy", "purge"}
)

#: Lineage states in which a governance record is currently in force.
_ACTIVE_STATES: frozenset[str] = frozenset({"give", "impose", "create", "supersede", "restore"})

_UNKNOWN = object()


@dataclass
class ObjectVerdict:
    """Evaluator outcome for one object."""

    object_id: str
    verdict: str  # allow | deny | pending
    reason_codes: list[str] = field(default_factory=list)
    obligations: list[dict] = field(default_factory=list)
    valid_until: str | None = None
    pending_dependency: str | None = None  # fence name behind a pending


@dataclass
class Evaluation:
    """Combined evaluator outcome for the whole object set."""

    verdicts: list[ObjectVerdict]

    @property
    def decision(self) -> str:
        if any(v.verdict == "pending" for v in self.verdicts):
            return "pending"
        if any(v.verdict == "deny" for v in self.verdicts):
            return "deny"
        return "allow"

    @property
    def reason_codes(self) -> list[str]:
        codes = {code for v in self.verdicts for code in v.reason_codes}
        return sorted(codes)

    @property
    def obligations(self) -> list[dict]:
        seen: set[str] = set()
        merged: list[dict] = []
        for verdict in self.verdicts:
            for obligation in verdict.obligations:
                key = obligation["obligation"]
                if key not in seen:
                    seen.add(key)
                    merged.append(obligation)
        return sorted(merged, key=lambda o: o["obligation"])

    @property
    def valid_until(self) -> str | None:
        if self.decision != "allow":
            return None
        expiries = [v.valid_until for v in self.verdicts if v.valid_until]
        return min(expiries) if expiries else None


class DenyOverridesV1:
    """The ``ccf-deny-overrides-v1`` evaluator, version pinned by the catalog."""

    def __init__(self, evaluator_entry: dict) -> None:
        if evaluator_entry.get("name") != EVALUATOR_PROFILE:
            raise GovernanceError(
                f"evaluator entry is {evaluator_entry.get('name')!r}, "
                f"expected {EVALUATOR_PROFILE!r}"
            )
        if evaluator_entry.get("combining_algorithm") != "deny_overrides_v1":
            raise GovernanceError(
                "evaluator combining algorithm mismatch: "
                f"{evaluator_entry.get('combining_algorithm')!r}"
            )
        self.profile = EVALUATOR_PROFILE
        self.version = str(evaluator_entry["version"])

    def evaluate(self, context: dict, closure: PolicyClosure) -> Evaluation:
        verdicts = [
            self._evaluate_object(context, obj, closure) for obj in closure.objects
        ]
        return Evaluation(verdicts=verdicts)

    # ------------------------------------------------------------------
    # Per-object evaluation
    # ------------------------------------------------------------------

    def _evaluate_object(
        self, context: dict, obj: ObjectClosure, closure: PolicyClosure
    ) -> ObjectVerdict:
        if not obj.available:
            return ObjectVerdict(obj.object_id, "deny", ["object_unavailable"])

        # Unknown required context: a policy we must apply but cannot read.
        if obj.unresolved_policy_lineages:
            return ObjectVerdict(
                obj.object_id,
                "pending",
                ["policy_lineage_unresolved"],
                pending_dependency="governance.policy",
            )
        if obj.structural_type == "sealed.record":
            return ObjectVerdict(obj.object_id, "deny", ["sealed_object"])

        rules = self._applicable_rules(context, obj, closure)
        if rules["unknown"]:
            return ObjectVerdict(
                obj.object_id,
                "pending",
                ["unknown_rule_context"],
                pending_dependency="governance.policy",
            )
        if rules["deny"]:
            codes = sorted(
                f"deny_rule:{rule['rule_id']}" for rule, _doc in rules["deny"]
            )
            return ObjectVerdict(obj.object_id, "deny", codes)

        base_allows = [
            (rule, doc) for rule, doc in rules["allow"] if not doc.overlay
        ]
        base_default_allow = any(
            not doc.overlay
            and doc.payload.get("default_effect") == "allow"
            and not self._any_rule_applicable(doc, rules)
            for doc in [*obj.policies]
        )
        if not base_allows and not base_default_allow:
            return ObjectVerdict(obj.object_id, "deny", ["default_deny"])

        obligations = self._obligations(rules, obj.policies)
        expiry = self._min_expiry(rules)

        governance_verdict = self._apply_subject_governance(
            context, obj, closure, obligations
        )
        if governance_verdict is not None:
            return governance_verdict

        # The legal basis that grounds the allow also bounds its validity.
        basis_expiry = self._basis_expiry(context, obj, closure)
        if basis_expiry is not None:
            expiry = min(expiry, basis_expiry) if expiry else basis_expiry
        return ObjectVerdict(
            obj.object_id,
            "allow",
            ["allowed"],
            obligations=obligations,
            valid_until=expiry,
        )

    # ------------------------------------------------------------------
    # Rule selection
    # ------------------------------------------------------------------

    def _applicable_rules(
        self, context: dict, obj: ObjectClosure, closure: PolicyClosure
    ) -> dict:
        """Partition every rule by effect; unknown-applicability rules aside.

        Selector semantics: an empty selector array matches everything; a
        non-empty selector requires the context value to be a member
        (``data_classes`` requires a non-empty intersection with the
        object's classes). Rule ``valid_from``/``expires_at`` are evaluated
        against the requested time, so backdated or expired rules do not
        apply.
        """
        result: dict[str, list] = {
            "allow": [],
            "deny": [],
            "oblige": [],
            "unknown": [],
        }
        docs = [*obj.policies, *closure.overlays]
        for doc in docs:
            for rule in doc.payload.get("rules") or []:
                status = self._rule_status(context, obj, rule)
                if status == "unknown":
                    result["unknown"].append((rule, doc))
                elif status == "applicable":
                    effect = rule["effect"]
                    if effect in ("allow", "deny", "oblige"):
                        result[effect].append((rule, doc))
        return result

    def _rule_status(self, context: dict, obj: ObjectClosure, rule: dict) -> str:
        selectors = (
            ("operations", context["operation"]),
            ("purposes", context["purpose"]),
            ("recipients", context["recipient"]),
            ("destinations", context["destination"]),
        )
        for key, value in selectors:
            selected = rule.get(key) or []
            if selected and value not in selected:
                return "inapplicable"
        classes = rule.get("data_classes") or []
        if classes and not set(classes) & set(obj.data_classes):
            return "inapplicable"
        requested_at = context["requested_at"]
        if rule.get("valid_from") and requested_at < rule["valid_from"]:
            return "inapplicable"
        if rule.get("expires_at") and requested_at >= rule["expires_at"]:
            return "inapplicable"
        saw_unknown = False
        for condition in rule.get("conditions") or []:
            outcome = self._condition(context, obj, condition)
            if outcome is _UNKNOWN:
                saw_unknown = True
            elif outcome is False:
                return "inapplicable"
        return "unknown" if saw_unknown else "applicable"

    def _condition(self, context: dict, obj: ObjectClosure, condition: dict):
        attribute = condition["attribute"]
        operator = condition["operator"]
        value = self._attribute(context, obj, attribute)
        if operator == "exists":
            return value is not _UNKNOWN
        if value is _UNKNOWN:
            return _UNKNOWN
        expected = condition.get("value")
        if operator == "equals":
            return value == expected
        if operator == "not_equals":
            return value != expected
        if operator == "in":
            return isinstance(expected, list) and value in expected
        if operator == "not_in":
            return isinstance(expected, list) and value not in expected
        if operator == "contains":
            if isinstance(value, list):
                return expected in value
            if isinstance(value, str) and isinstance(expected, str):
                return expected in value
            return False
        if operator == "before":
            return isinstance(value, str) and value < expected
        if operator == "after":
            return isinstance(value, str) and value > expected
        raise GovernanceError(f"unknown condition operator: {operator!r}")

    @staticmethod
    def _attribute(context: dict, obj: ObjectClosure, attribute: str):
        """Resolve a dotted condition attribute against the decision context."""
        if attribute == "object_id":
            return obj.object_id
        if attribute == "data_classes":
            return obj.data_classes
        if attribute == "subjects":
            return obj.subjects
        parts = attribute.split(".")
        value = context.get(parts[0], _UNKNOWN)
        for part in parts[1:]:
            if value is _UNKNOWN:
                return _UNKNOWN
            if not isinstance(value, dict):
                return _UNKNOWN
            value = value.get(part, _UNKNOWN)
        return value

    @staticmethod
    def _any_rule_applicable(doc: PolicyDoc, rules: dict) -> bool:
        return any(
            doc is rule_doc
            for effect in ("allow", "deny", "oblige")
            for _rule, rule_doc in rules[effect]
        )

    @staticmethod
    def _obligations(rules: dict, policies: list[PolicyDoc]) -> list[dict]:
        obligations: list[dict] = []
        for effect in ("allow", "oblige"):
            for rule, doc in rules[effect]:
                for name in rule.get("obligations") or []:
                    obligations.append(
                        {
                            "obligation": name,
                            "rule_id": rule["rule_id"],
                            "policy_lineage_id": doc.lineage_id,
                        }
                    )
        for doc in policies:
            if doc.payload.get("provenance_requirement") == "inspectable_source":
                obligations.append(
                    {
                        "obligation": "provenance:inspectable_source",
                        "rule_id": None,
                        "policy_lineage_id": doc.lineage_id,
                    }
                )
        return obligations

    @staticmethod
    def _min_expiry(rules: dict) -> str | None:
        expiries = [
            rule["expires_at"]
            for rule, _doc in rules["allow"]
            if rule.get("expires_at")
        ]
        return min(expiries) if expiries else None

    # ------------------------------------------------------------------
    # Data-subject governance (spec section 9.2 lineage inputs)
    # ------------------------------------------------------------------

    def _apply_subject_governance(
        self,
        context: dict,
        obj: ObjectClosure,
        closure: PolicyClosure,
        obligations: list[dict],
    ) -> ObjectVerdict | None:
        """Consent/restriction/objection/hold/basis overlays on a policy allow."""
        if not obj.data_classes and not obj.subjects:
            return None
        if obj.subject_coverage != "complete":
            # Structurally unknowable who the data concerns: fail closed.
            return ObjectVerdict(obj.object_id, "deny", ["subject_coverage_unknown"])

        subjects = set(obj.subjects)
        records = [
            rec for rec in closure.governance if rec.payload.get("subject_id") in subjects
        ]
        requested_at = context["requested_at"]

        holds = [
            rec
            for rec in records
            if rec.type == "governance.legal_hold"
            and rec.state in _ACTIVE_STATES
            and rec.payload.get("decision") == "impose"
            and rec.payload.get("effective_at", "") <= requested_at
        ]
        if holds and context["operation"] in DESTRUCTIVE_OPERATIONS:
            return ObjectVerdict(obj.object_id, "deny", ["legal_hold_active"])
        if holds:
            for hold in holds:
                obligations.append(
                    {
                        "obligation": "legal_hold:preserve",
                        "rule_id": None,
                        "policy_lineage_id": hold.lineage_id,
                    }
                )
            impossible = sorted(
                {o["obligation"] for o in obligations}
                & HOLD_INCOMPATIBLE_OBLIGATIONS
            )
            if impossible:
                return ObjectVerdict(
                    obj.object_id,
                    "deny",
                    [f"impossible_obligation:{name}" for name in impossible],
                )

        for rec in records:
            if rec.type != "governance.restriction":
                continue
            if rec.state not in _ACTIVE_STATES:
                continue
            if rec.payload.get("decision") != "impose":
                continue
            if rec.payload.get("effective_at", "") > requested_at:
                continue
            if self._scope_matches(rec.payload.get("scope"), context, obj):
                return ObjectVerdict(obj.object_id, "deny", ["restriction_active"])

        for rec in records:
            if rec.type != "governance.objection":
                continue
            if rec.payload.get("effective_at", "") > requested_at:
                continue
            if not self._scope_matches(rec.payload.get("scope"), context, obj):
                continue
            if rec.state == "raise":
                return ObjectVerdict(
                    obj.object_id,
                    "pending",
                    ["objection_pending"],
                    pending_dependency="governance.objection",
                )
            if rec.state == "accept":
                return ObjectVerdict(obj.object_id, "deny", ["objection_accepted"])

        if self._is_owner_local_read(context, obj):
            return None
        if obj.subjects and obj.data_classes:
            if self._covering_basis(context, obj, records) is None:
                return ObjectVerdict(obj.object_id, "deny", ["no_legal_basis"])
        return None

    @staticmethod
    def _is_owner_local_read(context: dict, obj: ObjectClosure) -> bool:
        return (
            context["destination"] == LOCAL_DESTINATION
            and context["requester"] in obj.subjects
        )

    def _scope_matches(
        self, scope: dict | None, context: dict, obj: ObjectClosure
    ) -> bool:
        """Consent-style scope match; empty/absent selectors match everything."""
        scope = scope or {}
        operations = scope.get("operations") or []
        if operations and context["operation"] not in operations:
            return False
        purposes = scope.get("purposes") or []
        if purposes and context["purpose"] not in purposes:
            return False
        classes = scope.get("data_classes") or []
        if classes and not set(classes) & set(obj.data_classes):
            return False
        return True

    def _covering_basis(
        self,
        context: dict,
        obj: ObjectClosure,
        records: list[GovernanceRecord],
    ) -> GovernanceRecord | None:
        """One covering consent *or* legal basis — alternatives, not cumulative."""
        for rec in records:
            if not self._valid_at(rec, context["requested_at"]):
                continue
            if rec.type == "governance.consent":
                if obj.consent_refs and rec.head_record_id not in obj.consent_refs:
                    continue
                if rec.payload.get("decision") != "given":
                    continue
                if self._payload_covers(rec.payload, context, obj):
                    return rec
            elif rec.type == "governance.legal_basis":
                if obj.legal_basis_refs and rec.head_record_id not in obj.legal_basis_refs:
                    continue
                if not self._jurisdiction_covers(
                    rec.payload.get("jurisdiction"), context["jurisdiction"]
                ):
                    continue
                if self._payload_covers(rec.payload, context, obj):
                    return rec
        return None

    @staticmethod
    def _valid_at(rec: GovernanceRecord, requested_at: str) -> bool:
        if rec.state not in _ACTIVE_STATES:
            return False
        payload = rec.payload
        if payload.get("valid_from") and requested_at < payload["valid_from"]:
            return False
        if payload.get("expires_at") and requested_at >= payload["expires_at"]:
            return False
        return True

    def _payload_covers(self, payload: dict, context: dict, obj: ObjectClosure) -> bool:
        purposes = payload.get("purposes") or []
        if purposes and context["purpose"] not in purposes:
            return False
        operations = payload.get("operations") or []
        if operations and context["operation"] not in operations:
            return False
        classes = payload.get("data_classes") or []
        if classes and not set(classes) & set(obj.data_classes):
            return False
        return True

    @staticmethod
    def _jurisdiction_covers(basis_jurisdiction, context_jurisdiction) -> bool:
        """A basis covers when every declared jurisdiction key matches."""
        basis_jurisdiction = basis_jurisdiction or {}
        context_jurisdiction = context_jurisdiction or {}
        for key, value in basis_jurisdiction.items():
            if context_jurisdiction.get(key) != value:
                return False
        return True

    def _basis_expiry(
        self, context: dict, obj: ObjectClosure, closure: PolicyClosure
    ) -> str | None:
        subjects = set(obj.subjects)
        records = [
            rec for rec in closure.governance if rec.payload.get("subject_id") in subjects
        ]
        basis = self._covering_basis(context, obj, records)
        if basis is not None:
            return basis.payload.get("expires_at")
        return None
