"""Promotion policy for semantic memory candidates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .config import config as runtime_config


JsonObject = dict[str, Any]
SEMANTIC_MEMORY_PROMOTION_METADATA_KEY = "semantic_memory_promotion_gate"
SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_ARTIFACT_TYPES = (
    "calendar_event",
    "contact_card",
    "manual_preference",
    "manual_profile",
    "project_manifest",
    "structured_note",
    "task_record",
)
SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_METADATA_KEYS = (
    "semantic_memory_trusted_structured",
    "trusted_structured_input",
)


class SemanticMemoryPromotionConfigError(ValueError):
    """Raised when semantic memory promotion policy config is invalid."""


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _positive_int_config(
    value: Any,
    *,
    key: str,
    default: int,
) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise SemanticMemoryPromotionConfigError(
            f"semantic_memory.promotion.{key} must be a positive integer"
        )
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SemanticMemoryPromotionConfigError(
            f"semantic_memory.promotion.{key} must be a positive integer"
        ) from exc
    if parsed < 1:
        raise SemanticMemoryPromotionConfigError(
            f"semantic_memory.promotion.{key} must be a positive integer"
        )
    return parsed


def _string_tuple_config(
    value: Any,
    *,
    key: str,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    if value is None:
        return default
    if not isinstance(value, (list, tuple)):
        raise SemanticMemoryPromotionConfigError(
            f"semantic_memory.promotion.{key} must be a list of strings"
        )
    cleaned: list[str] = []
    for item in value:
        text = _clean_optional(item)
        if not text:
            raise SemanticMemoryPromotionConfigError(
                f"semantic_memory.promotion.{key} must be a list of non-empty strings"
            )
        cleaned.append(text)
    return tuple(dict.fromkeys(cleaned))


def semantic_text_fingerprint(value: Any) -> str:
    """Normalize candidate text for comparing rejected candidate reappearances."""
    return " ".join(str(value or "").casefold().split())


def metadata_flag_enabled(metadata: Mapping[str, Any], keys: tuple[str, ...]) -> bool:
    """Return true only for explicit trusted-structured metadata flags."""
    for key in keys:
        value = metadata.get(key)
        if value is True:
            return True
        if isinstance(value, str) and value.strip().casefold() == "true":
            return True
    return False


@dataclass(frozen=True)
class SemanticMemoryPromotionPolicy:
    """Configurable evidence gate for promoting semantic memory candidates."""

    min_evidence_count: int = 2
    min_distinct_sources: int = 2
    trusted_structured_artifact_types: tuple[str, ...] = (
        SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_ARTIFACT_TYPES
    )
    trusted_structured_metadata_keys: tuple[str, ...] = (
        SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_METADATA_KEYS
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "min_evidence_count",
            _positive_int_config(
                self.min_evidence_count,
                key="min_evidence_count",
                default=2,
            ),
        )
        object.__setattr__(
            self,
            "min_distinct_sources",
            _positive_int_config(
                self.min_distinct_sources,
                key="min_distinct_sources",
                default=2,
            ),
        )
        object.__setattr__(
            self,
            "trusted_structured_artifact_types",
            _string_tuple_config(
                self.trusted_structured_artifact_types,
                key="trusted_structured_artifact_types",
                default=SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_ARTIFACT_TYPES,
            ),
        )
        object.__setattr__(
            self,
            "trusted_structured_metadata_keys",
            _string_tuple_config(
                self.trusted_structured_metadata_keys,
                key="trusted_structured_metadata_keys",
                default=SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_METADATA_KEYS,
            ),
        )

    @classmethod
    def from_config(cls, config_obj: Any | None = None) -> "SemanticMemoryPromotionPolicy":
        """Build a promotion policy from semantic_memory.promotion config."""
        config_obj = config_obj or runtime_config
        raw = config_obj.get("semantic_memory.promotion", {})
        return cls.from_mapping(raw)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any] | None,
    ) -> "SemanticMemoryPromotionPolicy":
        if value is None:
            value = {}
        if not isinstance(value, Mapping):
            raise SemanticMemoryPromotionConfigError(
                "semantic_memory.promotion must be a JSON object"
            )
        return cls(
            min_evidence_count=value.get("min_evidence_count", 2),
            min_distinct_sources=value.get("min_distinct_sources", 2),
            trusted_structured_artifact_types=_string_tuple_config(
                value.get("trusted_structured_artifact_types"),
                key="trusted_structured_artifact_types",
                default=SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_ARTIFACT_TYPES,
            ),
            trusted_structured_metadata_keys=_string_tuple_config(
                value.get("trusted_structured_metadata_keys"),
                key="trusted_structured_metadata_keys",
                default=SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_METADATA_KEYS,
            ),
        )


@dataclass(frozen=True)
class SemanticMemoryPromotionDecision:
    """Auditable promotion gate result."""

    allowed: bool
    reason: str
    candidate_status: str
    evidence_count: int
    distinct_source_count: int
    min_evidence_count: int
    min_distinct_sources: int
    explicitly_confirmed: bool = False
    trusted_structured_input: bool = False

    def to_metadata(self) -> JsonObject:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "candidate_status": self.candidate_status,
            "evidence_count": self.evidence_count,
            "distinct_source_count": self.distinct_source_count,
            "thresholds": {
                "min_evidence_count": self.min_evidence_count,
                "min_distinct_sources": self.min_distinct_sources,
            },
            "explicitly_confirmed": self.explicitly_confirmed,
            "trusted_structured_input": self.trusted_structured_input,
        }


__all__ = [
    "SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_ARTIFACT_TYPES",
    "SEMANTIC_MEMORY_DEFAULT_TRUSTED_STRUCTURED_METADATA_KEYS",
    "SEMANTIC_MEMORY_PROMOTION_METADATA_KEY",
    "SemanticMemoryPromotionConfigError",
    "SemanticMemoryPromotionDecision",
    "SemanticMemoryPromotionPolicy",
    "metadata_flag_enabled",
    "semantic_text_fingerprint",
]
