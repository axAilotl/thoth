"""Schema validation against the vendored, catalog-pinned CCF schemas.

The spec requires schema validation to succeed before canonicalization and
hashing (spec section 4.1) and requires payload validation for the resolved
exact type at admission (spec section 5.5). All schemas are loaded from the
vendored package and bound by their ``$id`` so ``urn:ccf:schema:...``
references resolve locally — never against the network.

CCF 0.2.0 additionally requires the inherited ``ccf-uint64`` format to be
enforced (inclusive ``0`` through ``18446744073709551615``). Treating that
custom format as an unknown annotation is not CCF-conformant.
"""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

_UINT64_MAX = "18446744073709551615"


class CcfSchemaError(ValueError):
    """Raised when a document fails validation against a pinned CCF schema."""


def is_ccf_uint64(value: object) -> bool:
    """Return True iff ``value`` is a canonical decimal uint64 string.

    Non-strings are left to type checks (the format applies to strings).
    """
    if not isinstance(value, str):
        return True
    if value != "0" and (
        not value.isascii() or not value.isdigit() or value.startswith("0")
    ):
        return False
    return value == "0" or len(value) < 20 or (
        len(value) == 20 and value <= _UINT64_MAX
    )


def ccf_format_checker() -> FormatChecker:
    """FormatChecker that asserts the inherited ``ccf-uint64`` format."""
    checker = FormatChecker(formats=())
    checker.checks("ccf-uint64")(is_ccf_uint64)
    return checker


CCF_FORMAT_CHECKER = ccf_format_checker()


def _load_schema_documents(package_root: Path) -> dict[str, dict]:
    schemas_dir = package_root / "schemas"
    if not schemas_dir.is_dir():
        raise CcfSchemaError(f"schemas directory not found: {schemas_dir}")
    schemas: dict[str, dict] = {}
    for path in sorted(schemas_dir.rglob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        schema_id = document.get("$id")
        if not schema_id:
            # Non-schema index documents (e.g. schemas/catalog.json).
            if str(document.get("format", "")).startswith("ccf."):
                continue
            raise CcfSchemaError(f"schema without $id: {path}")
        if schema_id in schemas:
            raise CcfSchemaError(f"duplicate schema $id {schema_id!r}: {path}")
        schemas[schema_id] = document
    return schemas


class SchemaSet:
    """All ``schemas/**/*.json`` of one or more CCF packages, indexed by ``$id``."""

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
        return cls(_load_schema_documents(Path(package_root)))

    @classmethod
    def load_layered(
        cls, base_root: str | Path, draft_root: str | Path
    ) -> "SchemaSet":
        """Load frozen 0.1.2 schemas plus the 0.2.0 declaration overlay."""
        schemas = _load_schema_documents(Path(base_root))
        for schema_id, document in _load_schema_documents(Path(draft_root)).items():
            if schema_id in schemas:
                raise CcfSchemaError(
                    f"draft schema collides with base $id {schema_id!r}"
                )
            schemas[schema_id] = document
        return cls(schemas)

    def _validator(self, schema_id: str) -> Draft202012Validator:
        validator = self._validators.get(schema_id)
        if validator is None:
            document = self._schemas.get(schema_id)
            if document is None:
                raise CcfSchemaError(f"unknown CCF schema: {schema_id!r}")
            validator = Draft202012Validator(
                document,
                registry=self._registry,
                format_checker=CCF_FORMAT_CHECKER,
            )
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
