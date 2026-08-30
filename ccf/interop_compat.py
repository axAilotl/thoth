"""CCF 0.2.0 Capsule interoperability compatibility harness and status.

This module is report-only: it never mutates the archive and never raises for
a malformed Capsule input; it returns structured blockers instead.
"""

from __future__ import annotations

import json

from ccf.capsule import CapsuleError, load_capsule
from ccf.interop import CISSA_LEGACY_ROOT, InteropContext, InteropError
from ccf.layered import LayeredError
from ccf.schemas import CcfSchemaError

#: Canonical blocker definitions reused by compatibility and cross-repo status.
CISSA_ROOT_MISMATCH_BLOCKER = {
    "type": "cissa-root-mismatch",
    "category": "authority",
    "description": "Capsule uses Cissa's legacy pre-reconciliation root",
    "external_contract": "cissa-99d.2",
}

MISSING_CARRIER_CONTRACT_BLOCKER = {
    "type": "missing-carrier-contract",
    "category": "transport",
    "description": (
        "Capsule declares external dependencies with locators; the "
        "cissa-99d.3 carrier contract is required to fetch and verify them"
    ),
    "external_contract": "cissa-99d.3",
}

MISSING_AUTHORITATIVE_FIXTURE_BLOCKER = {
    "type": "missing-authoritative-fixture",
    "category": "fixture",
    "description": (
        "Cissa has no committed CCF 0.2 fixture produced from the "
        "reconciled baseline; a true bidirectional cross-repo pass is blocked"
    ),
    "external_contract": "cissa-99d.2",
}


def external_carrier_dependencies(capsule_manifest: dict) -> list[dict]:
    """Return Capsule dependencies that require an external carrier.

    Matches the existing manifest schema: ``availability`` is ``external`` and
    a ``locator`` is present. Validates dependency entry shapes so malformed
    entries produce a domain error rather than AttributeError.
    """
    dependencies = capsule_manifest.get("dependencies", [])
    if not isinstance(dependencies, (list, tuple)):
        raise InteropError("capsule manifest dependencies must be a list")
    result: list[dict] = []
    for dep in dependencies:
        if not isinstance(dep, dict):
            raise InteropError("capsule dependency entry must be an object")
        availability = dep.get("availability")
        locator = dep.get("locator")
        if availability == "external" and isinstance(locator, str) and locator:
            result.append(dep)
    return result


def _verify_current_capsule(
    ctx: InteropContext, capsule_dir
) -> list[dict]:
    """Verify a current-root Capsule and return any conformance blockers."""
    from ccf.capsule import verify_capsule

    try:
        capsule = load_capsule(capsule_dir, schemas=ctx.schemas)
        verify_capsule(
            capsule,
            layered=ctx.layered,
            schemas=ctx.schemas,
            recipient_level=ctx.declaration["level"],
            recipient_capabilities=ctx.declaration["capabilities"],
        )
    except (
        CapsuleError,
        CcfSchemaError,
        LayeredError,
        InteropError,
        json.JSONDecodeError,
        UnicodeDecodeError,
        OSError,
        KeyError,
        TypeError,
        AttributeError,
    ) as exc:
        return [
            {
                "type": "capsule-verification-failed",
                "category": "conformance",
                "message": str(exc),
                "note": "Capsule does not verify against this archive's declaration",
            }
        ]
    return []


def _load_failure_blocker(exc: BaseException) -> dict:
    return {
        "type": "capsule-load-failed",
        "category": "integrity",
        "message": str(exc),
        "note": "Capsule could not be loaded; possible corruption",
    }


def _identity_failure_blocker(exc: BaseException) -> dict:
    return {
        "type": "capsule-identity-failed",
        "category": "authority",
        "message": str(exc),
        "note": "Capsule manifest does not yield a valid protocol identity",
    }


def evaluate_compatibility(ctx: InteropContext, capsule_dir) -> dict:
    """Return a structured compatibility report for a Capsule.

    Reports only blockers that actually apply to this Capsule. A valid
    current-root local Capsule with no external dependencies can pass. This
    function never raises for corrupted Capsule input.
    """
    from ccf.interop import negotiate_identity

    # Load/integrity failures are report-only.
    try:
        capsule = load_capsule(capsule_dir, schemas=None)
    except (
        CapsuleError,
        CcfSchemaError,
        json.JSONDecodeError,
        UnicodeDecodeError,
        OSError,
        KeyError,
        TypeError,
        AttributeError,
    ) as exc:
        return {
            "identity": None,
            "blockers": [_load_failure_blocker(exc)],
            "pass": False,
            "archive_id": ctx.archive_id,
        }

    # Identity negotiation failures are report-only.
    try:
        identity = negotiate_identity(capsule.manifest, ctx.declaration)
    except InteropError as exc:
        return {
            "identity": None,
            "blockers": [_identity_failure_blocker(exc)],
            "pass": False,
            "archive_id": ctx.archive_id,
        }

    blockers: list[dict] = []

    # External carrier dependency detection failures are report-only.
    try:
        has_external_carriers = bool(external_carrier_dependencies(capsule.manifest))
    except InteropError as exc:
        has_external_carriers = False
        blockers.append(
            {
                "type": "carrier-detection-failed",
                "category": "transport",
                "message": str(exc),
                "note": "Capsule dependency entries are malformed",
            }
        )

    if has_external_carriers:
        blockers.append(dict(MISSING_CARRIER_CONTRACT_BLOCKER))

    if identity["root"] == "current":
        blockers.extend(_verify_current_capsule(ctx, capsule_dir))
    elif identity["root"] == "legacy":
        if identity["catalog_root"] == CISSA_LEGACY_ROOT:
            blockers.append(dict(CISSA_ROOT_MISMATCH_BLOCKER))
        blockers.append(
            {
                "type": "root-not-current",
                "category": "authority",
                "catalog_root": identity["catalog_root"],
                "note": "Capsule root is a known legacy root, not the archive's current root",
            }
        )
    else:
        blockers.append(
            {
                "type": "unknown-root",
                "category": "authority",
                "catalog_root": identity["catalog_root"],
                "note": "Capsule root is neither current nor a known legacy root",
            }
        )

    return {
        "identity": identity,
        "blockers": blockers,
        "pass": len(blockers) == 0,
        "archive_id": ctx.archive_id,
    }


def cross_repo_conformance_status() -> dict:
    """Explicit cross-repo/status report for Cissa conformance gaps.

    This is not a per-Capsule result; it documents why a true bidirectional
    Cissa↔Thoth pass remains blocked regardless of any local Capsule.
    """
    return {
        "pass": False,
        "blockers": [
            dict(CISSA_ROOT_MISMATCH_BLOCKER),
            dict(MISSING_AUTHORITATIVE_FIXTURE_BLOCKER),
            dict(MISSING_CARRIER_CONTRACT_BLOCKER),
        ],
        "note": (
            "True bidirectional Cissa↔Thoth interoperability is blocked by "
            "open external contracts. Per-Capsule compatibility may still pass "
            "for local current-root Capsules."
        ),
    }
