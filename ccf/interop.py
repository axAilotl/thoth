"""Thoth↔Cissa CCF 0.2.0 interoperability boundary.

This module implements only the Thoth-owned portion of the Cissa delivery
contract tracked by cissa-99d.3:

* exact protocol identity negotiation using the existing 0.2 declaration and
  Capsule schemas;
* explicit ``current`` / ``read`` / ``refuse`` / ``uplift`` disposition for
  both legacy semantic-catalog roots without rewriting identifiers;
* a fail-closed compatibility/conformance harness that reports known external
  blockers in a structured form.

It does **not** admit foreign Capsule submissions into a Thoth archive. A
Capsule is not a signed producer batch, and unsigned Capsule material has
``producer_authentication: absent`` unless a signed producer proof suite
verifies it. Canonical admission of foreign objects therefore remains blocked
until a verified producer/consumer path exists or the operator performs a
catalog-transition uplift Record.

The module reuses existing primitives: ``Capsule``, ``preview_capsule``,
``build_thoth_declaration``, ``build_pending_uplift``, and
``verify_uplift_receipt``. It does not introduce a parallel artifact envelope
or manifest.
"""

from __future__ import annotations

from ccf.capsule import CapsuleError, load_capsule, verify_capsule
from ccf.catalog import LayeredCatalog
from ccf.declaration import build_thoth_declaration
from ccf.layered import LayeredRegistries
from ccf.schemas import SchemaSet

#: Cissa's pre-reconciliation 0.1.2 semantic-catalog root.
CISSA_LEGACY_ROOT = (
    "sha256:447aa218156d0b33861090c5931bee78bc4a59300e94feacbcf89eb9d35dbc10"
)

#: Thoth's transient 0.1.2 semantic-catalog root.
THOTH_TRANSIENT_ROOT = (
    "sha256:34a285bb6e0c3713e89ca6c4c59df5abdd4b1bb3498abd1391d44674f035a5f7"
)

#: Recognized legacy 0.1.2 semantic-catalog roots and their default disposition.
#: The current reconciled baseline (``sha256:9924…``) is deliberately absent;
#: it is read from the archive declaration at runtime.
KNOWN_LEGACY_ROOTS = {
    # Cissa's pre-reconciliation 0.1.2 baseline.
    CISSA_LEGACY_ROOT: {
        "name": "cissa-0.1.2-pre-reconciliation",
        "disposition": "legacy_refuse",
        "note": (
            "Cissa's older 0.1.2 semantic catalog; adoption requires Cissa to "
            "migrate to the reconciled baseline or the operator to commit a "
            "catalog-transition Record"
        ),
    },
    # Thoth's transient 0.1.2 baseline between the Cissa-aligned root and the
    # final reconciled root.
    THOTH_TRANSIENT_ROOT: {
        "name": "thoth-0.1.2-transient",
        "disposition": "legacy_read",
        "note": (
            "Thoth's intermediate 0.1.2 semantic catalog; objects admitted "
            "under this root remain readable only through explicit legacy policy"
        ),
    },
}

#: Open external contracts that block a full bidirectional pass.
KNOWN_EXTERNAL_BLOCKERS = {
    "cissa-root-mismatch": {
        "type": "cissa-root-mismatch",
        "category": "authority",
        "description": (
            "Cissa's checked-in 0.1.2 semantic-catalog root is legacy and does "
            "not match the reconciled baseline pinned by Thoth"
        ),
        "external_contract": "cissa-99d.2",
    },
    "missing-authoritative-fixture": {
        "type": "missing-authoritative-fixture",
        "category": "fixture",
        "description": (
            "Cissa has no committed CCF 0.2 fixture produced from the "
            "reconciled baseline; a true bidirectional cross-repo pass is "
            "blocked"
        ),
        "external_contract": "cissa-99d.2",
    },
    "missing-carrier-contract": {
        "type": "missing-carrier-contract",
        "category": "transport",
        "description": (
            "External Blob carrier mapping and verified ranges belong to the "
            "open cissa-99d.3 delivery/session/receipt contract"
        ),
        "external_contract": "cissa-99d.3",
    },
}


class InteropError(RuntimeError):
    """Raised when a Capsule delivery cannot proceed safely."""


class InteropCompatibilityError(InteropError):
    """Raised when a compatibility/conformance check reports blockers."""

    def __init__(self, message: str, blockers: list[dict]) -> None:
        super().__init__(message)
        self.blockers = list(blockers)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InteropError(message)


def _load_archive_declaration(archive) -> tuple[LayeredCatalog, dict]:
    """Load the 0.2.0 layered catalogs and build Thoth's declaration."""
    if archive.package_root is None:
        raise InteropError("archive has no package_root; cannot negotiate Capsule identity")
    draft = archive.draft_root()
    catalog = LayeredCatalog.load(draft, archive.package_root)
    if catalog.base_root != archive.semantic_catalog_root:
        raise InteropError(
            "archive package base catalog root mismatch: "
            f"loaded {catalog.base_root} != pinned {archive.semantic_catalog_root}"
        )
    layered = LayeredRegistries.load(draft)
    schemas = SchemaSet.load_layered(archive.package_root, draft)
    declaration = build_thoth_declaration(
        layered=layered,
        catalog_roots=(catalog.base_root, catalog.root),
        profiles=archive.active_profiles,
        schemas=schemas,
    )
    return catalog, declaration


def negotiate_identity(
    capsule_manifest: dict,
    archive_declaration: dict,
) -> dict:
    """Return the negotiated protocol identity for a Capsule.

    Fails closed if required identity fields are missing or incompatible. The
    returned ``root_disposition`` is one of ``current``, ``legacy_read``,
    ``legacy_refuse``, or ``legacy_uplift``.
    """
    if capsule_manifest.get("format") != "ccf.capsule/0.2.0":
        raise InteropError(
            f"unsupported capsule format: {capsule_manifest.get('format')!r}"
        )

    portable_formats = archive_declaration.get("portable_formats", [])
    if "ccf/0.1.2" not in portable_formats:
        raise InteropError(
            "archive declaration does not support portable format ccf/0.1.2"
        )

    catalog_root: str | None = None
    for dep in capsule_manifest.get("catalog_dependencies", []):
        if dep.get("kind") == "semantic_catalog" and dep.get("required"):
            catalog_root = dep.get("digest")
            break
    if catalog_root is None:
        raise InteropError("capsule manifest has no required semantic_catalog dependency")

    level = capsule_manifest.get("level")
    if level is None:
        raise InteropError("capsule manifest has no level")

    current_roots = archive_declaration.get("semantic_catalog_roots", [])
    if catalog_root in current_roots:
        disposition = "current"
    else:
        legacy = KNOWN_LEGACY_ROOTS.get(catalog_root)
        disposition = legacy["disposition"] if legacy else "legacy_refuse"

    return {
        "portable_format": "ccf/0.1.2",
        "catalog_root": catalog_root,
        "envelope_version": "0.2.0",
        "level": level,
        "recipient_level": archive_declaration["level"],
        "roles": list(archive_declaration.get("roles", [])),
        "capabilities": list(capsule_manifest.get("capabilities", [])),
        "recipient_capabilities": list(archive_declaration.get("capabilities", [])),
        "root_disposition": disposition,
        "current_catalog_roots": list(current_roots),
    }


def preview_capsule_as_thoth(archive, capsule_dir) -> dict:
    """Load and verify a Capsule, then build a pending uplift receipt.

    This is the honest Thoth-owned read path: it validates the Capsule against
    the archive's declaration and returns a *pending* uplift receipt where
    every object has ``producer_authentication: absent``. It does not mutate
    the archive.
    """
    from ccf.archive import ArchiveError

    try:
        return archive.preview_capsule(capsule_dir)
    except ArchiveError as exc:
        raise InteropError(str(exc)) from exc


def apply_disposition(
    identity: dict,
    preview: dict,
    *,
    legacy_root_policy: str,
    archive_id: str,
) -> dict:
    """Classify a verified Capsule preview into a delivery result.

    ``legacy_root_policy`` may be ``refuse`` (default), ``read``, or
    ``uplift``. Only ``refuse`` and ``read`` are permitted; ``uplift`` is
    rejected because cross-catalog-root uplift changes canonical commitments
    and requires an explicit catalog-transition Record.

    The returned document never claims admission. For the ``current`` and
    ``legacy_read`` dispositions it returns the pending uplift receipt and
    the verified Capsule manifest; for ``legacy_refuse`` and ``legacy_uplift``
    it raises.
    """
    _require(
        legacy_root_policy in {"refuse", "read", "uplift"},
        f"unknown legacy_root_policy: {legacy_root_policy!r}",
    )

    disposition = identity["root_disposition"]

    if disposition == "current":
        return {
            "status": "preview",
            "disposition": "current",
            "identity": identity,
            "uplift": preview["uplift"],
            "capsule": preview["capsule"],
            "archive_id": archive_id,
            "admitted": [],
            "note": (
                "Capsule matches a current semantic-catalog root. Canonical "
                "admission is not performed here; use a verified producer "
                "batch or a catalog-transition uplift Record."
            ),
        }

    if disposition == "legacy_read":
        if legacy_root_policy == "refuse":
            raise InteropError(
                f"capsule catalog root {identity['catalog_root']} is a known "
                "legacy root; legacy_root_policy is refuse"
            )
        if legacy_root_policy == "read":
            return {
                "status": "preview",
                "disposition": "legacy_read",
                "identity": identity,
                "uplift": preview["uplift"],
                "capsule": preview["capsule"],
                "archive_id": archive_id,
                "admitted": [],
                "note": (
                    "Capsule matches a known legacy root; only read/preview is "
                    "permitted. Canonical admission is not performed."
                ),
            }
        # legacy_root_policy == "uplift"
        raise InteropError(
            "cross-catalog-root uplift is not supported without a "
            "catalog-transition Record signed by the archive"
        )

    if disposition == "legacy_refuse":
        raise InteropCompatibilityError(
            f"capsule catalog root {identity['catalog_root']} is not recognized; "
            "treating as legacy_refuse",
            blockers=[
                {
                    "type": "unknown-catalog-root",
                    "category": "authority",
                    "catalog_root": identity["catalog_root"],
                    "note": (
                        "Root is neither the archive's current root nor a "
                        "known legacy root"
                    ),
                },
                KNOWN_EXTERNAL_BLOCKERS["cissa-root-mismatch"],
                KNOWN_EXTERNAL_BLOCKERS["missing-authoritative-fixture"],
            ],
        )

    if disposition == "legacy_uplift":
        raise InteropError(
            "capsule catalog root requires uplift; cross-catalog-root uplift "
            "is not supported without a catalog-transition Record"
        )

    raise InteropError(f"unhandled disposition: {disposition!r}")


def evaluate_compatibility(archive, capsule_dir) -> dict:
    """Return a structured compatibility report for a Capsule.

    The report always includes the negotiated identity and a list of blockers.
    Because the full Cissa↔Thoth pass is externally blocked, the report will
    contain at least one blocker for any foreign Capsule:

    * ``cissa-root-mismatch`` when the root is Cissa's legacy root;
    * ``missing-authoritative-fixture`` for every foreign Capsule, because no
      committed authoritative CCF 0.2 fixture exists yet;
    * ``missing-carrier-contract`` when the Capsule declares external Blob
      carriers (``blob_transfers`` references or opaque Blob streams).

    This function does not mutate the archive.
    """
    _, declaration = _load_archive_declaration(archive)

    blockers: list[dict] = []

    # Any foreign Capsule is currently blocked by the missing authoritative
    # fixture and, when Blobs are carried externally, by the missing carrier
    # contract.
    blockers.append(KNOWN_EXTERNAL_BLOCKERS["missing-authoritative-fixture"])

    try:
        capsule = load_capsule(capsule_dir, schemas=None)
    except CapsuleError as exc:
        blockers.append(
            {
                "type": "capsule-load-failed",
                "category": "integrity",
                "message": str(exc),
                "note": "Capsule could not be loaded; possible corruption",
            }
        )
        return {
            "identity": None,
            "blockers": blockers,
            "pass": False,
            "archive_id": archive.archive_id,
        }

    identity = negotiate_identity(capsule.manifest, declaration)

    if identity["catalog_root"] == CISSA_LEGACY_ROOT:
        blockers.append(KNOWN_EXTERNAL_BLOCKERS["cissa-root-mismatch"])

    if identity["catalog_root"] not in identity["current_catalog_roots"]:
        blockers.append(
            {
                "type": "root-not-current",
                "category": "authority",
                "catalog_root": identity["catalog_root"],
                "current_roots": identity["current_catalog_roots"],
                "note": "Capsule root is not the archive's current root",
            }
        )

    # Detect external Blob carrier references. Without cissa-99d.3 we cannot
    # map, verify ranges, or load those bytes safely.
    manifest = capsule.manifest
    has_external_blobs = any(
        stream.spec.get("content_role") == "blob_carrier"
        for stream in capsule.streams
    )
    has_blob_transfers = bool(manifest.get("extensions", {}).get("blob_transfers"))
    if has_external_blobs or has_blob_transfers:
        blockers.append(KNOWN_EXTERNAL_BLOCKERS["missing-carrier-contract"])

    # Downgrade/capability mismatch is also a blocker.
    try:
        verify_capsule(
            capsule,
            layered=LayeredRegistries.load(archive.draft_root()),
            schemas=SchemaSet.load_layered(archive.package_root, archive.draft_root()),
            recipient_level=declaration["level"],
            recipient_capabilities=declaration["capabilities"],
        )
    except CapsuleError as exc:
        blockers.append(
            {
                "type": "capsule-verification-failed",
                "category": "conformance",
                "message": str(exc),
                "note": "Capsule does not verify against this archive's declaration",
            }
        )

    return {
        "identity": identity,
        "blockers": blockers,
        "pass": len(blockers) == 0,
        "archive_id": archive.archive_id,
    }


def import_capsule(
    archive,
    capsule_dir,
    *,
    legacy_root_policy: str = "refuse",
    importer_tag: str | None = None,
) -> dict:
    """Evaluate a Capsule against this archive and return a disposition.

    This function does **not** admit Capsule submissions into the archive. It
    verifies the Capsule, negotiates protocol identity, and returns a
    structured result for one of the four dispositions:

    * ``current``: the Capsule root is a current root; a pending uplift receipt
      is returned.
    * ``legacy_read``: the Capsule root is a known legacy root and
      ``legacy_root_policy`` is ``read``; a pending uplift receipt is returned.
    * ``legacy_refuse``: the root is unrecognized or ``legacy_root_policy`` is
      ``refuse``; raises ``InteropError``.
    * ``legacy_uplift``: cross-root uplift is requested; raises
      ``InteropError`` because a catalog-transition Record is required.

    The ``importer_tag`` is accepted for API compatibility but is ignored:
      tags do not affect canonical identity or validation.
    """
    if archive.package_root is None:
        raise InteropError("archive has no package_root; cannot evaluate a Capsule")

    # Reuse the existing preview path, which loads the Capsule, validates it
    # against the archive's declaration, and builds a verified pending uplift
    # receipt without mutating the archive.
    preview = preview_capsule_as_thoth(archive, capsule_dir)
    declaration = preview["declaration"]
    identity = negotiate_identity(preview["capsule"], declaration)

    result = apply_disposition(
        identity,
        preview,
        legacy_root_policy=legacy_root_policy,
        archive_id=archive.archive_id,
    )
    if importer_tag is not None:
        result["importer_tag"] = importer_tag
    return result
