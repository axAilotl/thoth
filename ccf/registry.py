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
        policy_evaluators: dict,
        data_classes: dict,
        authority_bases: dict,
        authority_classes: dict,
        suppression_profiles: dict,
    ) -> None:
        self._types = types
        self._links = links
        self._blobs = blobs
        self._state_machines = state_machines
        self._policy_evaluators = {
            entry["name"]: entry for entry in policy_evaluators["entries"]
        }
        self._data_classes = {
            entry["name"] for entry in data_classes["entries"]
        }
        self._authority_bases = {
            entry["name"] for entry in authority_bases["entries"]
        }
        self._authority_classes = {
            entry["class"]: entry for entry in authority_classes["entries"]
        }
        self._suppression_profiles = {
            entry["name"]: entry for entry in suppression_profiles["entries"]
        }
        self._type_entries = {
            (entry["name"], entry["version"]): entry for entry in types["entries"]
        }
        self._link_entries = {
            (entry["name"], entry["version"]): entry for entry in links["entries"]
        }
        self._machines = {entry["id"]: entry for entry in state_machines["entries"]}
        blob_entries = blobs.get("entries") or []
        self._blob_entries = {entry["name"]: entry for entry in blob_entries}
        if "blob.manifest" not in self._blob_entries:
            raise RegistryError("blobs registry must declare a blob.manifest entry")

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
            types=_load_verified("ccf.types/0.1.2-rc1"),
            links=_load_verified("ccf.links/0.1.2-rc1"),
            blobs=_load_verified("ccf.blobs/0.1.2-rc1"),
            state_machines=_load_verified("ccf.state-machines/0.1.2-rc1"),
            policy_evaluators=_load_verified("ccf.policy-evaluators/0.1.2-rc1"),
            data_classes=_load_verified("ccf.data-classes/0.1.2-rc1"),
            authority_bases=_load_verified("ccf.authority-bases/0.1.2-rc1"),
            authority_classes=_load_verified("ccf.admission-authority-classes/0.1.2-rc1"),
            suppression_profiles=_load_verified("ccf.suppression-profiles/0.1.2-rc1"),
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

    def link_entries(self) -> list[dict]:
        """All pinned Link registry entries."""
        return list(self._links["entries"])

    @property
    def blob_entry(self) -> dict:
        """The default blob manifest registry entry (``blob.manifest``)."""
        return self._blob_entries["blob.manifest"]

    def blob_type_entry(self, name: str) -> dict:
        """Named blob registry entry (e.g. ``blob.suppression_set``)."""
        entry = self._blob_entries.get(name)
        if entry is None:
            raise RegistryError(f"unknown blob registry entry: {name!r}")
        return entry

    def state_machine(self, machine_id: str) -> dict:
        """State-machine declaration; fail closed if unknown."""
        machine = self._machines.get(machine_id)
        if machine is None:
            raise RegistryError(f"unknown state machine: {machine_id!r}")
        return machine

    def policy_evaluator(self, name: str) -> dict:
        """Policy-evaluator registry entry; fail closed if unknown."""
        entry = self._policy_evaluators.get(name)
        if entry is None:
            raise RegistryError(f"unknown policy evaluator: {name!r}")
        return entry

    def data_class_names(self) -> frozenset[str]:
        """Pinned data-class names (ccf.data-classes registry)."""
        return frozenset(self._data_classes)

    def authority_basis_names(self) -> frozenset[str]:
        """Pinned authority-basis names (ccf.authority-bases registry)."""
        return frozenset(self._authority_bases)

    def authority_class(self, name: str) -> dict:
        """Admission authority-class entry; fail closed if unknown.

        The pinned ``ccf.admission-authority-classes`` registry (0.1.2-rc1)
        declares the normative ``failure_reason`` for each class; rejection
        reasons must emit it verbatim.
        """
        entry = self._authority_classes.get(name)
        if entry is None:
            raise RegistryError(f"unknown admission authority class: {name!r}")
        return entry

    def authority_class_names(self) -> frozenset[str]:
        """Pinned admission authority-class names."""
        return frozenset(self._authority_classes)

    def suppression_profile(self, name: str) -> dict:
        """Suppression-profile registry entry; fail closed if unknown."""
        entry = self._suppression_profiles.get(name)
        if entry is None:
            raise RegistryError(f"unknown suppression profile: {name!r}")
        return entry

    def acyclic_link_types(self) -> frozenset[str]:
        """Link types whose active edges must remain acyclic (spec 8.6)."""
        return frozenset(
            entry["name"] for entry in self._links["entries"] if entry.get("acyclic")
        )

    @staticmethod
    def entry_digest(entry: dict) -> str:
        """``ccf:registry-entry:v1`` digest bound into structural compartments."""
        return registry_entry_digest(entry)
