"""Security scans -> ``security.finding`` Records (checklist 4, row 5).

Maps a Thoth ``SecurityFinding`` (``core.capture_event_store.SecurityFinding``
/ ``security_findings`` table, populated from ``core.prompt_security``
scanner metadata) to one sealed ``security.finding`` Record (registry
default visibility for the type) on a ``ccf.state.reviewable-v1`` lineage.

Evidence is exact: the caller resolves the finding's ``event_id`` /
``raw_ref_id`` to the CCF URNs of the already-mapped capture objects and
passes them as ``evidence_ccf_ids``; each becomes a payload
``evidence_refs`` entry plus a structural ``ccf.evidence_for`` Link from
the evidence object to the finding. A finding without evidence is refused.

The origin native ID prefers the scanner ``fingerprint`` (stable across
re-scans of unchanged content) and falls back to the Thoth ``finding_id``.
"""

from __future__ import annotations

from ccf.ids import generate_id, parse_id
from ccf.producer import Producer

from ccf.thothmap.context import (
    MapContext,
    MappedSubmissions,
    ThothMapError,
    ccf_timestamp,
    claims,
    optional_str,
    origin,
    require_str,
    require_urn,
)

_SEVERITIES = {"info", "low", "medium", "high", "critical"}
_DISPOSITIONS = {"observe", "quarantine", "block", "release", "false_positive"}

# security_findings.status / prompt-security policy status -> CCF disposition
_STATUS_DISPOSITION = {
    "new": "observe",
    "open": "observe",
    "active": "observe",
    "triage": "observe",
    "allowed": "observe",
    "needs_review": "quarantine",
    "blocked": "block",
    "override_approved": "release",
    "closed": "release",
    "resolved": "release",
    "suppressed": "false_positive",
    "accepted": "false_positive",
}


def finding_submissions(
    producer: Producer,
    ctx: MapContext,
    snapshot: dict,
    *,
    source_ccf_id: str,
    evidence_ccf_ids: list[str],
    revision: str | int | None = "1",
    disposition: str | None = None,
    summary: str | None = None,
) -> MappedSubmissions:
    """Convert one ``SecurityFinding`` snapshot to a ``security.finding``.

    Snapshot keys: ``finding_id``, ``finding_type``, ``severity``,
    ``status``, ``scanner``, ``fingerprint``, ``detected_at``, ``details``.
    """
    require_urn(source_ccf_id, "record", field="source_ccf_id")
    if not evidence_ccf_ids:
        raise ThothMapError("security finding requires at least one evidence reference")
    # Evidence may be a Record or a Blob URN — validate as any CCF object URN.
    for evidence_id in evidence_ccf_ids:
        try:
            parse_id(evidence_id)
        except Exception as exc:
            raise ThothMapError(f"invalid evidence URN: {evidence_id!r}") from exc

    severity = (optional_str(snapshot, "severity") or "info").lower()
    if severity not in _SEVERITIES:
        raise ThothMapError(f"unknown finding severity {severity!r}")
    if disposition is None:
        status = (optional_str(snapshot, "status") or "open").lower()
        disposition = _STATUS_DISPOSITION.get(status)
        if disposition is None:
            raise ThothMapError(f"unmappable finding status {status!r}")
    if disposition not in _DISPOSITIONS:
        raise ThothMapError(f"unknown finding disposition {disposition!r}")

    finding_id = require_str(snapshot, "finding_id", what="security finding")
    native_id = optional_str(snapshot, "fingerprint") or finding_id
    details = snapshot.get("details") or {}
    if not isinstance(details, dict):
        raise ThothMapError("security finding 'details' must be an object")

    detected = snapshot.get("detected_at")
    valid_from = ccf_timestamp(detected, field="detected_at") if detected else None
    finding = producer.new_record(
        type="security.finding",
        type_visibility="sealed",
        claims=claims(ctx, basis="explicit_authorization", asserted_by=producer.producer_id),
        origin=origin(source_ccf_id, native_id, revision),
        lineage={
            "lineage_id": generate_id("lineage"),
            "previous_head_id": None,
            "transition": "create",
            "valid_from": valid_from or producer.clock(),
            "expires_at": None,
        },
        payload={
            "finding_kind": require_str(snapshot, "finding_type", what="security finding"),
            "severity": severity,
            "disposition": disposition,
            "scanner": optional_str(snapshot, "scanner") or "unknown",
            "summary": summary or str(details.get("source_label") or native_id),
            "evidence_refs": list(dict.fromkeys(evidence_ccf_ids)),
            "extensions": {
                "thoth_finding_id": finding_id,
                "thoth_status": snapshot.get("status"),
                "thoth_details": details,
            },
        },
    )

    links = [
        producer.new_link(
            type="ccf.evidence_for",
            from_id=evidence_id,
            to_id=finding["id"],
            claims=claims(ctx),
            selector={},
        )
        for evidence_id in dict.fromkeys(evidence_ccf_ids)
    ]
    return MappedSubmissions(records=[finding], links=links)
