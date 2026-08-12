"""Naming conventions shared by the dual-write service and the check harness.

Both sides must derive identical identifiers from the same legacy
inventory, or reconciliation is meaningless. Everything deterministic
lives here:

- bootstrap object IDs (person/runtime/policy/credential) — derived from
  the archive ID so a reopened archive can locate its founding Records;
- ``core.source`` Record IDs — the source is the origin root (no origin
  tuple), so the mirror derives a stable CCF URN from the Thoth
  ``capture_sources.source_id``; the harness recomputes the same URN;
- origin-tuple conventions for sessions, runs, media, and findings
  (native ID + revision), and the prompt-security metadata parsing both
  sides apply to legacy ``normalized_metadata``.

Deterministic derivation is used ONLY where CCF offers no idempotency key
(bootstrap and origin-root sources). Every origin-bearing object keeps a
freshly generated CCF URN; re-mirror idempotence comes from the
archive's origin index, not from reused IDs.
"""

from __future__ import annotations

import json
import uuid
from typing import Mapping

from core.prompt_security import (
    PROMPT_SECURITY_SCANNER,
    THOTH_SECURITY_FINDINGS_KEY,
)

DUALWRITE_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "thoth.ccf.dualwrite")

MEDIA_REVISION = "content-sha256"  # marker: media origin revision IS the sha256
SESSION_REVISION = "1"
FINDING_REVISION = "1"


def deterministic_id(kind: str, archive_id: str, *parts: str) -> str:
    """Deterministic CCF URN of ``kind`` namespaced to one archive.

    Derived from a UUIDv5 digest with the version/variant bits forced to
    the UUIDv4/RFC-4122 layout CCF requires — deterministic content with a
    spec-legal shape (``ccf.ids.parse_id`` rejects non-v4 URNs).
    """
    material = json.dumps(
        [archive_id, kind, *parts], ensure_ascii=False, separators=(",", ":")
    )
    digest = bytearray(uuid.uuid5(DUALWRITE_ID_NAMESPACE, material).bytes)
    digest[6] = (digest[6] & 0x0F) | 0x40  # version 4
    digest[8] = (digest[8] & 0x3F) | 0x80  # RFC 4122 variant
    return f"urn:ccf:{kind}:{uuid.UUID(bytes=bytes(digest))}"


def source_record_id(archive_id: str, thoth_source_id: str) -> str:
    """CCF ``core.source`` Record URN mirroring one Thoth capture source."""
    return deterministic_id("record", archive_id, "core.source", thoth_source_id)


def bootstrap_ids(archive_id: str) -> dict[str, str]:
    """The deterministic founding-object IDs of one dual-write archive."""
    return {
        "policy_record_id": deterministic_id("record", archive_id, "bootstrap", "policy"),
        "policy_lineage_id": deterministic_id("lineage", archive_id, "bootstrap", "policy"),
        "person_id": deterministic_id("record", archive_id, "bootstrap", "person"),
        "runtime_id": deterministic_id("record", archive_id, "bootstrap", "runtime"),
        "credential_record_id": deterministic_id(
            "record", archive_id, "bootstrap", "credential"
        ),
        "credential_lineage_id": deterministic_id(
            "lineage", archive_id, "bootstrap", "credential"
        ),
        "credential_id": deterministic_id("credential", archive_id, "bootstrap", "device"),
        "device_key_id": deterministic_id("key", archive_id, "bootstrap", "device"),
        "archive_key_id": deterministic_id("key", archive_id, "bootstrap", "archive"),
    }


def run_native_id(session_id: str) -> str:
    """Origin native ID of the ``process.run`` paired to a capture session."""
    return f"{session_id}:run"


def raw_ref_id_for(source_id: str, sha256: str, path: str) -> str:
    """Recompute the legacy ``raw_ref_id`` the capture lifecycle assigned.

    The lifecycle derives it with ``core.capture_lifecycle._stable_id``
    over exactly these fields; the harness recomputes it to find the
    mirrored artifact/blob origin tuples.
    """
    from core.capture_lifecycle import _stable_id

    return _stable_id(
        "raw-ref", {"source_id": source_id, "sha256": sha256, "path": path}
    )


def findings_from_metadata(metadata: Mapping) -> list[dict]:
    """Parse prompt-security findings out of legacy normalized metadata.

    Mirrors ``CaptureEventStore.upsert_security_findings_from_metadata``:
    entries without a ``pattern_id`` are scanner noise and are skipped;
    the origin-native fingerprint defaults to ``<scanner>:<pattern_id>``.
    """
    raw = metadata.get(THOTH_SECURITY_FINDINGS_KEY)
    if not isinstance(raw, list):
        return []
    findings: list[dict] = []
    for entry in raw:
        if not isinstance(entry, Mapping):
            continue
        pattern_id = entry.get("pattern_id")
        if not pattern_id:
            continue
        fingerprint = str(
            entry.get("fingerprint") or f"{PROMPT_SECURITY_SCANNER}:{pattern_id}"
        )
        findings.append(
            {
                "finding_id": f"dualwrite:{fingerprint}",
                "finding_type": str(entry.get("finding_type") or "prompt_security"),
                "severity": str(entry.get("severity") or "info"),
                "status": str(entry.get("status") or "open"),
                "scanner": str(entry.get("scanner") or PROMPT_SECURITY_SCANNER),
                "fingerprint": fingerprint,
                "detected_at": entry.get("detected_at"),
                "details": dict(entry),
            }
        )
    return findings


def finding_origin_native_id(finding: Mapping) -> str:
    """Origin native ID of a mirrored finding (scanner fingerprint first)."""
    return str(finding.get("fingerprint") or finding["finding_id"])
