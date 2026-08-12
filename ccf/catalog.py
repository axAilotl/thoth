"""Semantic catalog pinning (spec section 4.10).

Loads ``semantic-catalog.json`` from the vendored package, verifies its
root digest over the catalog-without-root, and verifies every pinned schema
and registry entry digest against the artifact on disk. Any mismatch fails
closed — an implementation must not silently substitute local semantics
while claiming replay equivalence.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ccf.hashing import (
    parse_digest,
    registry_artifact_digest,
    schema_artifact_digest,
    semantic_catalog_root,
)


class CatalogError(ValueError):
    """Raised when a semantic catalog or pinned artifact fails verification."""


@dataclass(frozen=True)
class CatalogEntry:
    """One pinned catalog entry (schema or registry)."""

    name: str
    path: str
    digest: str


def compute_catalog_root(catalog: dict) -> str:
    """Root digest of a full catalog document (its ``root`` field stripped)."""
    without_root = {key: value for key, value in catalog.items() if key != "root"}
    return semantic_catalog_root(without_root)


class SemanticCatalog:
    """A verified, pinned semantic catalog."""

    def __init__(self, document: dict, entries_verified: bool) -> None:
        self._document = document
        self.root: str = document["root"]
        self.format: str = document["format"]
        self.version: str = document["version"]
        self._schemas = {
            entry["id"]: CatalogEntry(entry["id"], entry["path"], entry["digest"])
            for entry in document["schemas"]
        }
        self._registries = {
            entry["name"]: CatalogEntry(entry["name"], entry["path"], entry["digest"])
            for entry in document["registries"]
        }
        self.entries_verified = entries_verified

    @classmethod
    def load(cls, package_root: str | Path) -> "SemanticCatalog":
        """Load and fully verify the catalog under a CCF package root.

        Verifies (fail closed, in order): catalog shape, embedded root
        digest, and every pinned schema/registry artifact digest against the
        file on disk.
        """
        package_root = Path(package_root)
        catalog_path = package_root / "semantic-catalog.json"
        document = json.loads(catalog_path.read_text(encoding="utf-8"))
        return cls.from_document(document, package_root=package_root)

    @classmethod
    def from_document(
        cls, document: dict, package_root: str | Path | None = None
    ) -> "SemanticCatalog":
        """Verify a catalog document; optionally verify artifacts on disk."""
        for field in ("format", "version", "schemas", "registries", "root"):
            if field not in document:
                raise CatalogError(f"semantic catalog missing field: {field}")
        parse_digest(document["root"])
        computed = compute_catalog_root(document)
        if computed != document["root"]:
            raise CatalogError(
                f"semantic catalog root mismatch: computed {computed}, "
                f"declared {document['root']}"
            )
        catalog = cls(document, entries_verified=False)
        if package_root is not None:
            catalog.verify_artifacts(Path(package_root))
        return catalog

    def verify_artifacts(self, package_root: Path) -> None:
        """Verify every pinned entry digest against the artifact on disk."""
        if not package_root.is_dir():
            raise CatalogError(f"package root not found: {package_root}")
        root_real = package_root.resolve()
        checks = [
            (entry, schema_artifact_digest) for entry in self._schemas.values()
        ] + [
            (entry, registry_artifact_digest) for entry in self._registries.values()
        ]
        for entry, digest_fn in checks:
            artifact_path = (package_root / entry.path).resolve()
            if root_real not in artifact_path.parents:
                raise CatalogError(
                    f"catalog entry path escapes package root: {entry.path!r}"
                )
            if not artifact_path.is_file():
                raise CatalogError(f"catalog artifact missing: {entry.path}")
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
            computed = digest_fn(artifact)
            if computed != entry.digest:
                raise CatalogError(
                    f"catalog artifact digest mismatch for {entry.name}: "
                    f"computed {computed}, pinned {entry.digest}"
                )
        self.entries_verified = True

    def schema_digest(self, schema_id: str) -> str:
        """Pinned digest for a schema ID; raises KeyError if unknown."""
        return self._schemas[schema_id].digest

    def registry_digest(self, registry_name: str) -> str:
        """Pinned digest for a registry name; raises KeyError if unknown."""
        return self._registries[registry_name].digest

    def schema_entry(self, schema_id: str) -> CatalogEntry:
        return self._schemas[schema_id]

    def registry_entry(self, registry_name: str) -> CatalogEntry:
        return self._registries[registry_name]

    @property
    def schemas(self) -> dict[str, CatalogEntry]:
        return dict(self._schemas)

    @property
    def registries(self) -> dict[str, CatalogEntry]:
        return dict(self._registries)
