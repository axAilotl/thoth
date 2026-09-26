"""Validation with the five pinned local CCF 0.3.0 JSON schemas."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from ccf import PACKAGE_ROOT, schema_id
from ccf.jcs import canonicalize


class CcfSchemaError(ValueError):
    """A document does not match the pinned schema."""


class SchemaSet:
    """Schema resources resolved locally by their published identifiers."""

    def __init__(self, schemas: dict[str, dict]) -> None:
        if not schemas:
            raise CcfSchemaError("no CCF schemas loaded")
        registry = Registry().with_resources(
            (key, Resource.from_contents(value)) for key, value in schemas.items()
        )
        self._validators = {}
        for key, value in schemas.items():
            Draft202012Validator.check_schema(value)
            self._validators[key] = Draft202012Validator(value, registry=registry)

    @classmethod
    def load(cls, package_root: str | Path = PACKAGE_ROOT) -> "SchemaSet":
        schemas = {}
        directory = Path(package_root) / "schemas"
        if not directory.is_dir():
            raise CcfSchemaError(f"schemas directory not found: {directory}")
        for path in sorted(directory.glob("*.schema.json")):
            document = json.loads(path.read_text(encoding="utf-8"))
            identifier = document.get("$id")
            if not identifier or identifier in schemas:
                raise CcfSchemaError(f"missing or duplicate schema $id: {path}")
            schemas[identifier] = document
        return cls(schemas)

    def has_schema(self, identifier: str) -> bool:
        return identifier in self._validators

    def validate(self, identifier: str, instance: object, *, what="document") -> None:
        canonicalize(instance)
        validator = self._validators.get(identifier)
        if validator is None:
            raise CcfSchemaError(f"unknown CCF schema: {identifier!r}")
        errors = list(validator.iter_errors(instance))
        if errors:
            details = "; ".join(
                f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}"
                for error in errors[:5]
            )
            raise CcfSchemaError(f"{what} fails schema: {details}")


@lru_cache(maxsize=1)
def pinned_schemas() -> SchemaSet:
    return SchemaSet.load()


def validate_manifest(manifest: object) -> None:
    """Validate an export manifest including integer and calendar rules."""
    from ccf.objects import validate_timestamp

    pinned_schemas().validate(schema_id("manifest"), manifest, what="manifest")
    validate_timestamp(manifest["exported_at"])
