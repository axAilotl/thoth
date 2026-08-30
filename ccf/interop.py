"""CCF 0.2.0 Capsule interoperability core for Thoth.

Focused primitives for protocol identity negotiation, policy dispatch, and
honest preview/uplift receipts. This module does not perform canonical
admission and does not synthesize producer evidence.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable

from ccf.capsule import Capsule, CapsuleError, load_capsule, verify_capsule
from ccf.exchange import ExchangeError, build_pending_uplift, verify_uplift_receipt
from ccf.layered import LayeredError, LayeredRegistries
from ccf.schemas import CcfSchemaError, SchemaSet

#: Cissa's pre-reconciliation 0.1.2 semantic-catalog root.
CISSA_LEGACY_ROOT = (
    "sha256:447aa218156d0b33861090c5931bee78bc4a59300e94feacbcf89eb9d35dbc10"
)

#: Thoth's first/original 0.1.2 semantic-catalog root, later superseded by the
#: reconciled baseline.
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
        "name": "thoth-0.1.2-superseded",
        "note": (
            "Thoth's first/original 0.1.2 semantic-catalog root, later "
            "superseded by the reconciled baseline."
        ),
    },
}


class InteropError(RuntimeError):
    """Raised when interoperability checks fail closed."""


@dataclass(frozen=True, slots=True)
class InteropContext:
    """Typed resources shared by interoperability operations on one archive."""

    declaration: dict[str, object]
    layered: LayeredRegistries
    schemas: SchemaSet
    archive_id: str
    clock: Callable[[], str]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InteropError(message)


def is_known_legacy_root(catalog_root: str) -> bool:
    return catalog_root in KNOWN_LEGACY_ROOTS


def load_capsule_integrity(capsule_dir) -> Capsule:
    """Load a Capsule verifying only stream digests and containment.

    Does not validate submissions against any archive's current schemas, so it
    is safe to call for legacy-root Capsules. Translates known Capsule parsing,
    IO, schema, and shape failures into InteropError.
    """
    try:
        return load_capsule(capsule_dir, schemas=None)
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
        raise InteropError(str(exc)) from exc


def _is_string(value: object) -> bool:
    return isinstance(value, str)


def _is_sequence(value: object) -> bool:
    return isinstance(value, (list, tuple)) and not isinstance(value, str)


def _require_dict(value: object, name: str) -> dict:
    if not isinstance(value, dict):
        raise InteropError(f"{name} must be an object")
    return value


def _require_sequence(value: object, name: str) -> list:
    if not _is_sequence(value):
        raise InteropError(f"{name} must be a list")
    return list(value)


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
    capsule_manifest = _require_dict(capsule_manifest, "capsule manifest")
    archive_declaration = _require_dict(archive_declaration, "archive declaration")

    fmt = capsule_manifest.get("format")
    _require(
        _is_string(fmt) and fmt == "ccf.capsule/0.2.0",
        f"unsupported capsule format: {fmt!r}",
    )

    portable_formats = archive_declaration.get("portable_formats", [])
    _require(
        _is_sequence(portable_formats) and "ccf/0.1.2" in portable_formats,
        "archive declaration does not support portable format ccf/0.1.2",
    )

    catalog_dependencies = _require_sequence(
        capsule_manifest.get("catalog_dependencies", []),
        "capsule manifest catalog_dependencies",
    )
    required_catalogs = []
    for dep in catalog_dependencies:
        dep = _require_dict(dep, "catalog dependency entry")
        if (
            dep.get("kind") == "semantic_catalog"
            and dep.get("required") is True
        ):
            digest = dep.get("digest")
            _require(
                _is_string(digest) and digest.strip() != "",
                "required semantic_catalog dependency must have a nonblank string digest",
            )
            required_catalogs.append(digest)
    _require(
        len(required_catalogs) == 1,
        "capsule manifest must have exactly one required semantic_catalog dependency",
    )
    catalog_root = required_catalogs[0]

    level = capsule_manifest.get("level")
    _require(_is_string(level), "capsule manifest level must be a string")

    current_roots = _require_sequence(
        archive_declaration.get("semantic_catalog_roots", []),
        "archive declaration semantic_catalog_roots",
    )
    _require(
        all(_is_string(r) for r in current_roots),
        "archive declaration semantic_catalog_roots must be strings",
    )

    if catalog_root in current_roots:
        root = "current"
    elif is_known_legacy_root(catalog_root):
        root = "legacy"
    else:
        root = "unknown"

    roles = _require_sequence(archive_declaration.get("roles", []), "roles")
    capabilities = _require_sequence(
        capsule_manifest.get("capabilities", []), "capsule capabilities"
    )
    recipient_capabilities = _require_sequence(
        archive_declaration.get("capabilities", []), "recipient capabilities"
    )
    recipient_level = archive_declaration.get("level")
    _require(
        _is_string(recipient_level),
        "archive declaration level must be a string",
    )
    _require(all(_is_string(role) for role in roles), "roles must be strings")
    _require(
        all(_is_string(capability) for capability in capabilities),
        "capsule capabilities must be strings",
    )
    _require(
        all(_is_string(capability) for capability in recipient_capabilities),
        "recipient capabilities must be strings",
    )

    legacy_identity = KNOWN_LEGACY_ROOTS.get(catalog_root)

    return {
        "portable_format": "ccf/0.1.2",
        "envelope_version": "0.2.0",
        "root": root,
        "known_legacy": root == "legacy",
        "catalog_root": catalog_root,
        "level": level,
        "recipient_level": recipient_level,
        "roles": list(roles),
        "capabilities": list(capabilities),
        "recipient_capabilities": list(recipient_capabilities),
        "current_catalog_roots": list(current_roots),
        "legacy_identity": dict(legacy_identity) if legacy_identity else None,
    }


def preview_capsule(ctx: InteropContext, capsule_dir) -> dict:
    """Validate a Capsule against the archive declaration and emit pending uplift.

    This is the default-refuse public preview path: it negotiates catalog
    identity using the same logic as :func:`inspect_capsule`, then verifies
    current-root Capsules and emits a verified pending uplift receipt.
    Legacy and unknown roots fail closed with :class:`InteropError` and no
    receipt is built.
    """
    try:
        capsule = load_capsule_integrity(capsule_dir)
        result = inspect_capsule(ctx, capsule, policy="refuse")
    except InteropError:
        raise
    except (
        CapsuleError,
        CcfSchemaError,
        ExchangeError,
        LayeredError,
        json.JSONDecodeError,
        UnicodeDecodeError,
        OSError,
        KeyError,
        TypeError,
        AttributeError,
    ) as exc:
        raise InteropError(str(exc)) from exc
    return {
        "declaration": ctx.declaration,
        "capsule": result["capsule"],
        "uplift": result["uplift"],
    }


def _build_pending_uplift(
    capsule: Capsule, ctx: InteropContext
) -> dict:
    """Build and verify a pending uplift receipt using the archive clock."""
    try:
        receipt = build_pending_uplift(
            capsule,
            destination_level=ctx.declaration["level"],
            destination_archive_id=ctx.archive_id,
            created_at=ctx.clock(),
        )
        verify_uplift_receipt(
            receipt, capsule=capsule, layered=ctx.layered, schemas=ctx.schemas
        )
    except (
        CapsuleError,
        CcfSchemaError,
        ExchangeError,
        LayeredError,
        KeyError,
        TypeError,
        AttributeError,
    ) as exc:
        raise InteropError(str(exc)) from exc
    return receipt


def inspect_capsule(
    ctx: InteropContext,
    capsule: Capsule,
    *,
    policy: str,
) -> dict:
    """Inspect a Capsule and return a negotiation/preview or pending-uplift result.

    ``policy`` controls behavior for known legacy roots:

    - ``refuse``: fail closed.
    - ``read``: integrity-verified inert preview; no archive mutation, no
      current-layered-schema verification, and no uplift receipt.
    - ``uplift``: pending verified uplift receipt only; preserves IDs and marks
      producer authentication absent.

    This function does **not** admit Capsule submissions into the archive.
    """
    _require(
        policy in {"refuse", "read", "uplift"},
        f"unknown Capsule inspect policy: {policy!r}",
    )

    identity = negotiate_identity(capsule.manifest, ctx.declaration)

    if identity["root"] == "current":
        # Current-root Capsules are verified against the archive's declaration.
        try:
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
            KeyError,
            TypeError,
            AttributeError,
        ) as exc:
            raise InteropError(str(exc)) from exc
        receipt = _build_pending_uplift(capsule, ctx)
        return {
            "status": "preview",
            "disposition": "current",
            "identity": identity,
            "uplift": receipt,
            "capsule": capsule.manifest,
        }

    if identity["root"] == "legacy":
        if policy == "refuse":
            raise InteropError(
                f"capsule catalog root {identity['catalog_root']} is a known "
                "legacy root; policy is refuse"
            )
        if policy == "read":
            # Inert: integrity is already verified by the caller; no receipt.
            return {
                "status": "preview",
                "disposition": "legacy_read",
                "identity": identity,
                "capsule": capsule.manifest,
            }
        # policy == "uplift"
        receipt = _build_pending_uplift(capsule, ctx)
        return {
            "status": "pending_uplift",
            "disposition": "legacy_uplift",
            "identity": identity,
            "uplift": receipt,
            "capsule": capsule.manifest,
        }

    raise InteropError(
        f"capsule catalog root {identity['catalog_root']} is not recognized; "
        "refusing unknown root"
    )
