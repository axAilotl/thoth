"""CCF 0.2.0 Capsule interoperability helpers for Thoth.

Small, focused primitives for protocol identity negotiation and honest
interoperability reporting. This module does not perform canonical admission
and does not synthesize producer evidence.
"""

from __future__ import annotations

from ccf.capsule import CapsuleError, load_capsule
from ccf.exchange import build_pending_uplift
from ccf.objects import now_timestamp

#: Cissa's pre-reconciliation 0.1.2 semantic-catalog root.
CISSA_LEGACY_ROOT = (
    "sha256:447aa218156d0b33861090c5931bee78bc4a59300e94feacbcf89eb9d35dbc10"
)

#: Thoth's transient 0.1.2 semantic-catalog root.
THOTH_TRANSIENT_ROOT = (
    "sha256:34a285bb6e0c3713e89ca6c4c59df5abdd4b1bb3498abd1391d44674f035a5f7"
)

#: Known legacy 0.1.2 semantic-catalog roots as identity facts only.
#: Dispositions are caller-selected; these records carry no fixed policy.
KNOWN_LEGACY_ROOTS = {
    CISSA_LEGACY_ROOT: {
        "name": "cissa-0.1.2-pre-reconciliation",
        "note": (
            "Cissa's pre-reconciliation 0.1.2 semantic catalog. Cissa must "
            "migrate to the reconciled baseline or the operator must commit a "
            "catalog-transition Record before admission."
        ),
    },
    THOTH_TRANSIENT_ROOT: {
        "name": "thoth-0.1.2-transient",
        "note": (
            "Thoth's intermediate 0.1.2 semantic catalog. Objects admitted "
            "under this root remain readable only through explicit legacy policy."
        ),
    },
}

#: Cross-repo Cissa conformance gaps, reported only by explicit status surfaces.
CISSA_CROSS_REPO_BLOCKERS = [
    {
        "type": "cissa-root-mismatch",
        "category": "authority",
        "description": (
            "Cissa's checked-in 0.1.2 semantic-catalog root is legacy and does "
            "not match the reconciled baseline pinned by Thoth"
        ),
        "external_contract": "cissa-99d.2",
    },
    {
        "type": "missing-authoritative-fixture",
        "category": "fixture",
        "description": (
            "Cissa has no committed CCF 0.2 fixture produced from the "
            "reconciled baseline; a true bidirectional cross-repo pass is blocked"
        ),
        "external_contract": "cissa-99d.2",
    },
    {
        "type": "missing-carrier-contract",
        "category": "transport",
        "description": (
            "External Blob carrier mapping and verified ranges belong to the "
            "open cissa-99d.3 delivery/session/receipt contract"
        ),
        "external_contract": "cissa-99d.3",
    },
]


class InteropError(RuntimeError):
    """Raised when interoperability checks fail closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InteropError(message)


def is_known_legacy_root(catalog_root: str) -> bool:
    return catalog_root in KNOWN_LEGACY_ROOTS


def negotiate_identity(
    capsule_manifest: dict,
    archive_declaration: dict,
) -> dict:
    """Return protocol identity facts for a Capsule.

    The result contains no disposition; the caller selects policy. The returned
    ``root`` is ``current`` when the Capsule's semantic-catalog root matches one
    of the archive's current roots, ``legacy`` when it matches a known legacy
    root, and ``unknown`` otherwise.
    """
    _require(
        capsule_manifest.get("format") == "ccf.capsule/0.2.0",
        f"unsupported capsule format: {capsule_manifest.get('format')!r}",
    )
    _require(
        "ccf/0.1.2" in archive_declaration.get("portable_formats", []),
        "archive declaration does not support portable format ccf/0.1.2",
    )

    catalog_root: str | None = None
    for dep in capsule_manifest.get("catalog_dependencies", []):
        if dep.get("kind") == "semantic_catalog" and dep.get("required"):
            catalog_root = dep.get("digest")
            break
    _require(
        catalog_root is not None,
        "capsule manifest has no required semantic_catalog dependency",
    )

    level = capsule_manifest.get("level")
    _require(level is not None, "capsule manifest has no level")

    current_roots = archive_declaration.get("semantic_catalog_roots", [])
    if catalog_root in current_roots:
        root = "current"
    elif is_known_legacy_root(catalog_root):
        root = "legacy"
    else:
        root = "unknown"

    return {
        "portable_format": "ccf/0.1.2",
        "envelope_version": "0.2.0",
        "root": root,
        "known_legacy": root == "legacy",
        "catalog_root": catalog_root,
        "level": level,
        "recipient_level": archive_declaration["level"],
        "capabilities": list(capsule_manifest.get("capabilities", [])),
        "recipient_capabilities": list(archive_declaration.get("capabilities", [])),
        "current_catalog_roots": list(current_roots),
        "legacy_identity": KNOWN_LEGACY_ROOTS.get(catalog_root),
    }


def load_capsule_integrity(capsule_dir) -> object:
    """Load a Capsule verifying only stream digests and containment.

    Does not validate submissions against any archive's current schemas, so it
    is safe to call for legacy-root Capsules.
    """
    try:
        return load_capsule(capsule_dir, schemas=None)
    except CapsuleError as exc:
        raise InteropError(str(exc)) from exc


def external_carrier_dependencies(capsule_manifest: dict) -> list[dict]:
    """Return Capsule dependencies that require an external carrier.

    Matches the existing manifest schema: ``availability`` is ``external`` and
    a ``locator`` is present.
    """
    return [
        dep
        for dep in capsule_manifest.get("dependencies", [])
        if dep.get("availability") == "external" and dep.get("locator")
    ]


def build_pending_uplift_receipt(capsule, archive_id: str, level: str) -> dict:
    """Build a pending uplift receipt with producer_authentication absent."""
    return build_pending_uplift(
        capsule,
        destination_level=level,
        destination_archive_id=archive_id,
        created_at=now_timestamp(),
    )


def cross_repo_conformance_status() -> dict:
    """Explicit cross-repo/status report for Cissa conformance gaps.

    This is not a per-Capsule result; it documents why a true bidirectional
    Cissa↔Thoth pass remains blocked regardless of any local Capsule.
    """
    return {
        "pass": False,
        "blockers": list(CISSA_CROSS_REPO_BLOCKERS),
        "note": (
            "True bidirectional Cissa↔Thoth interoperability is blocked by "
            "open external contracts. Per-Capsule compatibility may still pass "
            "for local current-root Capsules."
        ),
    }
