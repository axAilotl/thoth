"""Consequential receipts (spec section 9.8).

Every consequential disclosure or action — one that crossed the egress
boundary on a consumed capability — creates a canonical receipt Record.
Receipts use the pinned ``agent.receipt`` payload: ``details`` binds the
capability, decision context, and consumption coordinates;
``evidence_refs`` binds the exact disclosed objects. They are admitted
through the archive's operator path, so a receipt is itself a canonical,
journal-signed object.
"""

from __future__ import annotations

RECEIPT_TYPE = "agent.receipt"


def build_consequential_receipt(
    *,
    runtime_id: str,
    recorded_at: str,
    context: dict,
    capability_id: str,
    consumption: dict,
    summary: str,
    status: str = "completed",
) -> dict:
    """Build an ``admit_bootstrap`` record spec for a consequential receipt."""
    return {
        "type": RECEIPT_TYPE,
        "recorded_by": runtime_id,
        "recorded_at": recorded_at,
        "authority": {
            "basis": "deterministic_derivation",
            "asserted_by": runtime_id,
            "accepted_by": None,
        },
        "privacy": {
            "data_subjects": [],
            "data_classes": [],
            "consent_refs": [],
            "legal_basis_refs": [],
            "subject_coverage": "complete",
        },
        "payload": {
            "run_id": None,
            "summary": summary,
            "status": status,
            "details": {
                "capability_id": capability_id,
                "decision_context_hash": consumption["decision_context_hash"],
                "operation": context["operation"],
                "purpose": context["purpose"],
                "destination": context["destination"],
                "recipient": context["recipient"],
                "object_ids": list(context["object_ids"]),
                "consumed_at": consumption["consumed_at"],
            },
            "evidence_refs": list(context["object_ids"]),
            "extensions": {},
        },
        "extensions": {},
    }
