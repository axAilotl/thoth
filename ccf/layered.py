"""CCF 0.2.0 layered-conformance registries.

Levels, roles, capabilities, semantic packs, legacy profile mappings, and
the semantic-requirements overlay live here. Portable object algebra stays
on the frozen 0.1.2 package.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path



class LayeredError(ValueError):
    """Raised when a 0.2.0 registry lookup or declaration check fails."""


LEVEL_IDS = (
    "ccf-exchange-v1",
    "ccf-canonical-store-v1",
    "ccf-verified-archive-v1",
    "ccf-governed-archive-v1",
)

_REGISTRY_FILES = {
    "levels": "levels.registry.json",
    "roles": "roles.registry.json",
    "capabilities": "capabilities.registry.json",
    "semantic_packs": "semantic-packs.registry.json",
    "legacy_mappings": "legacy-profile-mappings.registry.json",
    "compatibility_rules": "compatibility-rules.registry.json",
    "semantic_requirements": "semantic-requirements.registry.json",
}


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def raw_digest(data: bytes) -> str:
    """Raw SHA-256 digest of bytes (Capsule streams and inventories)."""
    import hashlib

    return "sha256:" + hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class MappedProfiles:
    """0.2.0 declaration axes derived from 0.1.2 active profiles."""

    level: str
    capabilities: tuple[str, ...]
    semantic_packs: tuple[str, ...]

    @property
    def declared_features(self) -> tuple[str, ...]:
        return self.capabilities + self.semantic_packs


class LayeredRegistries:
    """Verified views over the 0.2.0 declaration registries."""

    def __init__(
        self,
        *,
        levels: dict,
        roles: dict,
        capabilities: dict,
        semantic_packs: dict,
        legacy_mappings: dict,
        compatibility_rules: dict,
        semantic_requirements: dict,
    ) -> None:
        self._levels = {entry["id"]: entry for entry in levels["entries"]}
        if list(self._levels) != list(LEVEL_IDS):
            raise LayeredError("guarantee levels must be the four published identifiers")
        self._roles = {entry["id"]: entry for entry in roles["entries"]}
        self._capabilities = {
            entry["id"]: entry for entry in capabilities["entries"]
        }
        self._semantic_packs = {
            entry["id"]: entry for entry in semantic_packs["entries"]
        }
        self._legacy = {
            entry["legacy_profile"]: entry for entry in legacy_mappings["entries"]
        }
        self._compatibility = list(compatibility_rules["entries"])
        self._requirements = {
            (entry["resource_kind"], entry["name"], entry["version"]): entry
            for entry in semantic_requirements["entries"]
        }
        if len(self._requirements) != len(semantic_requirements["entries"]):
            raise LayeredError("duplicate semantic requirement entry")

    @classmethod
    def load(cls, draft_root: str | Path) -> "LayeredRegistries":
        root = Path(draft_root)
        loaded = {}
        for key, filename in _REGISTRY_FILES.items():
            path = root / "registries" / filename
            if not path.is_file():
                raise LayeredError(f"missing 0.2.0 registry: {path}")
            loaded[key] = _load_json(path)
        return cls(**loaded)

    def level(self, level_id: str) -> dict:
        try:
            return self._levels[level_id]
        except KeyError as exc:
            raise LayeredError(f"unknown guarantee level: {level_id!r}") from exc

    def level_rank(self, level_id: str) -> int:
        return int(self.level(level_id)["rank"])

    def accepts_level(self, declared_level: str, incoming_level: str) -> bool:
        return incoming_level in self.level(declared_level)["accepts_levels"]

    def role(self, role_id: str) -> dict:
        try:
            return self._roles[role_id]
        except KeyError as exc:
            raise LayeredError(f"unknown implementation role: {role_id!r}") from exc

    def capability(self, capability_id: str) -> dict:
        if capability_id in self._capabilities:
            return self._capabilities[capability_id]
        if capability_id in self._semantic_packs:
            return self._semantic_packs[capability_id]
        raise LayeredError(f"unknown capability or semantic pack: {capability_id!r}")

    def known_feature_ids(self) -> frozenset[str]:
        return frozenset(self._capabilities) | frozenset(self._semantic_packs)

    def map_legacy_profiles(self, profiles: list[str]) -> MappedProfiles:
        """Map 0.1.2 active profiles onto one level plus declared features."""
        level = None
        capabilities: list[str] = []
        packs: list[str] = []
        for name in profiles:
            try:
                mapping = self._legacy[name]
            except KeyError as exc:
                raise LayeredError(f"unmapped 0.1.2 profile: {name!r}") from exc
            if mapping["level"] is not None:
                if level is not None and level != mapping["level"]:
                    raise LayeredError(
                        f"profiles map to conflicting levels: {level} vs {mapping['level']}"
                    )
                level = mapping["level"]
            if mapping["capability"] is not None:
                capabilities.append(mapping["capability"])
            if mapping["semantic_pack"] is not None:
                packs.append(mapping["semantic_pack"])
        if level is None:
            raise LayeredError("active profiles do not map to a guarantee level")
        return MappedProfiles(
            level=level,
            capabilities=tuple(dict.fromkeys(capabilities)),
            semantic_packs=tuple(dict.fromkeys(packs)),
        )

    def requirement_for(
        self, resource_kind: str, name: str, version: int = 1
    ) -> dict:
        try:
            return self._requirements[(resource_kind, name, version)]
        except KeyError as exc:
            raise LayeredError(
                f"unregistered semantics: {resource_kind} {name}@{version}"
            ) from exc

    def requirement_for_submission(self, submission: dict) -> dict:
        kind = submission.get("submission_kind")
        resource_kind = {
            "record": "record_type",
            "link": "link_type",
            "blob": "blob_type",
        }.get(kind)
        if resource_kind is None:
            raise LayeredError(f"submission missing kind: {kind!r}")
        return self.requirement_for(resource_kind, submission["type"], 1)

    def features_fit_level(self, level_id: str, feature_ids: list[str] | tuple[str, ...]) -> None:
        rank = self.level_rank(level_id)
        for feature_id in feature_ids:
            entry = self.capability(feature_id)
            minimum = entry["minimum_level"]
            if self.level_rank(minimum) > rank:
                raise LayeredError(
                    f"{feature_id} requires {minimum}, above declared {level_id}"
                )

    def can_activate(
        self,
        requirement: dict,
        *,
        level_id: str,
        capabilities: list[str] | tuple[str, ...],
    ) -> bool:
        """True iff the declared implementation may activate the resource."""
        if self.level_rank(level_id) < self.level_rank(requirement["minimum_level"]):
            return False
        needed = set(requirement.get("required_capabilities") or [])
        pack = requirement.get("semantic_pack")
        if pack:
            needed.add(pack)
        return needed <= set(capabilities)

    def stream_may_activate(
        self,
        stream: dict,
        *,
        level_id: str,
        capabilities: list[str] | tuple[str, ...],
    ) -> bool:
        requirements = stream["activation_requirements"]
        if self.level_rank(level_id) < self.level_rank(requirements["minimum_level"]):
            return False
        return set(requirements.get("capabilities") or []) <= set(capabilities)


def load_layered(draft_root: str | Path) -> LayeredRegistries:
    return LayeredRegistries.load(draft_root)

