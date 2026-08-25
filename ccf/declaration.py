"""CCF 0.2.0 implementation declarations.

Migration changes declarations and packaging, not existing object bytes.
Thoth remains a Governed Archive over portable format ``ccf/0.1.2``.
"""

from __future__ import annotations

from pathlib import Path

from ccf.layered import LayeredError, LayeredRegistries
from ccf.schemas import SchemaSet

SCHEMA_IMPLEMENTATION = "urn:ccf:schema:0.2.0:declarations.implementation"

THOTH_IMPLEMENTATION = "Thoth reference archive"
THOTH_IMPLEMENTATION_VERSION = "0.2.0"

THOTH_ROLES = (
    "preserver",
    "producer",
    "processor",
    "importer-exporter",
    "archive",
    "policy-evaluator",
)


def build_declaration(
    *,
    implementation: str,
    version: str,
    level: str,
    roles: list[str] | tuple[str, ...],
    capabilities: list[str] | tuple[str, ...],
    portable_formats: list[str] | tuple[str, ...] = ("ccf/0.1.2",),
    semantic_catalog_roots: list[str] | tuple[str, ...],
    extensions: dict | None = None,
) -> dict:
    """Build a schema-shaped implementation declaration."""
    return {
        "format": "ccf.implementation-declaration/0.2.0",
        "implementation": implementation,
        "version": version,
        "level": level,
        "roles": list(roles),
        "capabilities": list(capabilities),
        "portable_formats": list(portable_formats),
        "semantic_catalog_roots": list(semantic_catalog_roots),
        "extensions": {} if extensions is None else dict(extensions),
    }


def build_thoth_declaration(
    *,
    layered: LayeredRegistries,
    catalog_roots: list[str] | tuple[str, ...],
    profiles: list[str] | None = None,
    schemas: SchemaSet | None = None,
) -> dict:
    """Declare Thoth as a Governed Archive from its 0.1.2 active profiles.

    Only capabilities and semantic packs implied by ``profiles`` are claimed.
    Encryption, cryptographic object-erasure, witnessing, and succession stay
    undeclared — they are unimplemented.
    """
    from ccf.archive import DEFAULT_ACTIVE_PROFILES

    mapped = layered.map_legacy_profiles(list(profiles or DEFAULT_ACTIVE_PROFILES))
    declaration = build_declaration(
        implementation=THOTH_IMPLEMENTATION,
        version=THOTH_IMPLEMENTATION_VERSION,
        level=mapped.level,
        roles=THOTH_ROLES,
        capabilities=mapped.declared_features,
        semantic_catalog_roots=catalog_roots,
        extensions={
            "thoth.legacy_profiles": list(profiles or DEFAULT_ACTIVE_PROFILES),
        },
    )
    validate_declaration(declaration, layered=layered, schemas=schemas)
    return declaration


def validate_declaration(
    document: dict,
    *,
    layered: LayeredRegistries,
    schemas: SchemaSet | None = None,
) -> None:
    """Fail closed if the declaration is schema-invalid or over-claims."""
    if schemas is not None:
        schemas.validate(
            SCHEMA_IMPLEMENTATION, document, what="implementation declaration"
        )
    if document.get("format") != "ccf.implementation-declaration/0.2.0":
        raise LayeredError("declaration format must be ccf.implementation-declaration/0.2.0")
    layered.level(document["level"])
    for role in document["roles"]:
        layered.role(role)
    unknown = set(document["capabilities"]) - layered.known_feature_ids()
    if unknown:
        raise LayeredError(f"declaration names unknown features: {sorted(unknown)}")
    layered.features_fit_level(document["level"], document["capabilities"])
    if "ccf/0.1.2" not in document["portable_formats"]:
        raise LayeredError("declaration must include portable format ccf/0.1.2")


def load_thoth_declaration(
    *,
    base_root: str | Path,
    draft_root: str | Path,
    profiles: list[str] | None = None,
) -> dict:
    """Load pinned catalogs and emit Thoth's 0.2.0 declaration."""
    from ccf.catalog import LayeredCatalog

    catalog = LayeredCatalog.load(draft_root, base_root)
    layered = LayeredRegistries.load(draft_root)
    schemas = SchemaSet.load_layered(base_root, draft_root)
    return build_thoth_declaration(
        layered=layered,
        catalog_roots=(catalog.base_root, catalog.root),
        profiles=profiles,
        schemas=schemas,
    )
