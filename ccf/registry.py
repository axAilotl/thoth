"""Pinned type/Link/state-machine registry access (spec section 5.5).

Loads the vendored registries through the verified semantic catalog: every
registry artifact digest is checked against the pinned catalog entry before
any entry is usable. Registry lookups drive admission decisions — retention
profiles, ``endpoints_location``, lineage modes, and state-machine rules —
so they must fail closed on unknown names rather than fall back to local
assumptions (spec section 7.6).
"""

from __future__ import annotations

import json
from pathlib import Path

from ccf.catalog import SemanticCatalog
from ccf.hashing import registry_entry_digest


class RegistryError(ValueError):
    """Raised when a registry lookup fails or a registry fails verification."""


class PinnedRegistries:
    """Verified views over the type, Link, blob, and state-machine registries."""

    def __init__(
        self,
        *,
        types: dict,
        links: dict,
        blobs: dict,
        state_machines: dict,
    ) -> None:
        self._types = types
        self._links = links
        self._blobs = blobs
        self._state_machines = state_machines
        self._type_entries = {
            (entry["name"], entry["version"]): entry for entry in types["entries"]
        }
        self._link_entries = {
            (entry["name"], entry["version"]): entry for entry in links["entries"]
        }
        self._machines = {entry["id"]: entry for entry in state_machines["entries"]}
        blob_entries = blobs.get("entries") or []
        if len(blob_entries) != 1:
            raise RegistryError(
                f"blobs registry must declare exactly one entry, got {len(blob_entries)}"
            )
        self._blob_entry = blob_entries[0]

    @classmethod
    def load(
        cls, package_root: str | Path, catalog: SemanticCatalog | None = None
    ) -> "PinnedRegistries":
        """Load registries from a CCF package root, verified via the catalog."""
        package_root = Path(package_root)
        if catalog is None:
            catalog = SemanticCatalog.load(package_root)
        elif not catalog.entries_verified:
            catalog.verify_artifacts(package_root)

        def _load_verified(name: str) -> dict:
            entry = catalog.registry_entry(name)
            artifact_path = package_root / entry.path
            document = json.loads(artifact_path.read_text(encoding="utf-8"))
            # verify_artifacts already pinned this exact digest; parsing the
            # same file here keeps registry bytes and catalog binding in sync.
            return document

        return cls(
            types=_load_verified("ccf.types/0.1.1"),
            links=_load_verified("ccf.links/0.1.1"),
            blobs=_load_verified("ccf.blobs/0.1.1"),
            state_machines=_load_verified("ccf.state-machines/0.1.1"),
        )

    def type_entry(self, name: str, version: int = 1) -> dict:
        """Type registry entry for ``name@version``; fail closed if unknown."""
        entry = self._type_entries.get((name, version))
        if entry is None:
            raise RegistryError(f"unknown type registry entry: {name}@{version}")
        return entry

    def link_entry(self, name: str, version: int = 1) -> dict:
        """Link registry entry for ``name@version``; fail closed if unknown."""
        entry = self._link_entries.get((name, version))
        if entry is None:
            raise RegistryError(f"unknown Link registry entry: {name}@{version}")
        return entry

    @property
    def blob_entry(self) -> dict:
        """The single blob manifest registry entry."""
        return self._blob_entry

    def state_machine(self, machine_id: str) -> dict:
        """State-machine declaration; fail closed if unknown."""
        machine = self._machines.get(machine_id)
        if machine is None:
            raise RegistryError(f"unknown state machine: {machine_id!r}")
        return machine

    def acyclic_link_types(self) -> frozenset[str]:
        """Link types whose active edges must remain acyclic (spec 8.6)."""
        return frozenset(
            entry["name"] for entry in self._links["entries"] if entry.get("acyclic")
        )

    @staticmethod
    def entry_digest(entry: dict) -> str:
        """``ccf:registry-entry:v1`` digest bound into structural compartments."""
        return registry_entry_digest(entry)
