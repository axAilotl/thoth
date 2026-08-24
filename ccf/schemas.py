"""Schema validation against the vendored, catalog-pinned CCF schemas.

The spec requires schema validation to succeed before canonicalization and
hashing (spec section 4.1) and requires payload validation for the resolved
exact type at admission (spec section 5.5). All schemas are loaded from the
vendored package and bound by their ``$id`` so ``urn:ccf:schema:...``
references resolve locally — never against the network.
"""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource


class CcfSchemaError(ValueError):
    """Raised when a document fails validation against a pinned CCF schema."""


class SchemaSet:
    """All ``schemas/**/*.json`` of a CCF package, indexed by ``$id``."""

    def __init__(self, schemas: dict[str, dict]) -> None:
        if not schemas:
            raise CcfSchemaError("no CCF schemas loaded")
        self._schemas = dict(schemas)
        self._registry = Registry().with_resources(
            (schema_id, Resource.from_contents(document))
            for schema_id, document in self._schemas.items()
        )
        self._validators: dict[str, Draft202012Validator] = {}

    @classmethod
    def load(cls, package_root: str | Path) -> "SchemaSet":
        """Load every schema with an ``$id`` under ``<package_root>/schemas``."""
        schemas_dir = Path(package_root) / "schemas"
        if not schemas_dir.is_dir():
            raise CcfSchemaError(f"schemas directory not found: {schemas_dir}")
        schemas: dict[str, dict] = {}
        for path in sorted(schemas_dir.rglob("*.json")):
            document = json.loads(path.read_text(encoding="utf-8"))
            schema_id = document.get("$id")
            if not schema_id:
                # Non-schema index documents (e.g. schemas/catalog.json).
                if document.get("format", "").startswith("ccf."):
                    continue
                raise CcfSchemaError(f"schema without $id: {path}")
            if schema_id in schemas:
                raise CcfSchemaError(f"duplicate schema $id {schema_id!r}: {path}")
            schemas[schema_id] = document
        return cls(schemas)

    def _validator(self, schema_id: str) -> Draft202012Validator:
        validator = self._validators.get(schema_id)
        if validator is None:
            document = self._schemas.get(schema_id)
            if document is None:
                raise CcfSchemaError(f"unknown CCF schema: {schema_id!r}")
            validator = Draft202012Validator(document, registry=self._registry)
            self._validators[schema_id] = validator
        return validator

    def has_schema(self, schema_id: str) -> bool:
        return schema_id in self._schemas

    def validate(self, schema_id: str, instance: object, *, what: str = "document") -> None:
        """Validate ``instance``; raise CcfSchemaError with details on failure."""
        validator = self._validator(schema_id)
        errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.path))
        if errors:
            details = "; ".join(
                f"{'/'.join(str(p) for p in error.path) or '<root>'}: {error.message}"
                for error in errors[:5]
            )
            raise CcfSchemaError(
                f"{what} fails schema {schema_id}: {details}"
            )
