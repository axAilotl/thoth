"""Processor identity for transcript enrichment."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


_PROCESSOR_IDENTITY_KEYS = {
    "processor_name",
    "processor_version",
    "prompt_version",
    "config_version",
    "model",
    "provider",
}


@dataclass(frozen=True)
class ProcessorIdentity:
    """Explicit identity for a processor that produces transcript derivatives.

    Every processor must declare its name, implementation version, prompt
    version, configuration version, model, and provider. These values form part
    of the derivative cache key so that a changed processor, prompt, model, or
    provider never silently reuses stale output.

    For expensive processors (summarizer, classifier) all fields must be
    present and non-blank. ``model`` and ``provider`` may be the explicit
    sentinel ``"none"`` for a local normalizer, but they may not be empty.
    """

    processor_name: str
    processor_version: str
    prompt_version: str
    config_version: str
    model: str
    provider: str

    def __post_init__(self) -> None:
        for field_name in (
            "processor_name",
            "processor_version",
            "prompt_version",
            "config_version",
            "model",
            "provider",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise TranscriptIdentityError(
                    f"ProcessorIdentity.{field_name} is required and must be non-blank, "
                    f"got {value!r}"
                )

    def cache_material(self) -> str:
        return "|".join(
            [
                self.processor_name,
                self.processor_version,
                self.prompt_version,
                self.config_version,
                self.model,
                self.provider,
            ]
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "processor_name": self.processor_name,
            "processor_version": self.processor_version,
            "prompt_version": self.prompt_version,
            "config_version": self.config_version,
            "model": self.model,
            "provider": self.provider,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ProcessorIdentity":
        if not isinstance(value, Mapping):
            raise TranscriptIdentityError("processor identity must be a mapping")
        keys = set(value.keys())
        if keys != _PROCESSOR_IDENTITY_KEYS:
            raise TranscriptIdentityError(
                "processor identity must contain exactly "
                + ", ".join(sorted(_PROCESSOR_IDENTITY_KEYS))
            )
        for field_name in sorted(_PROCESSOR_IDENTITY_KEYS):
            if not isinstance(value[field_name], str):
                raise TranscriptIdentityError(
                    f"ProcessorIdentity.{field_name} must be a string, "
                    f"got {type(value[field_name]).__name__}"
                )
        return cls(**{key: value[key] for key in _PROCESSOR_IDENTITY_KEYS})


class TranscriptIdentityError(ValueError):
    """Raised when a processor identity is invalid."""


_SOURCE_PROVIDED_NAME = "thoth.source_provided"


def source_provided_summary_identity() -> ProcessorIdentity:
    """Honest identity for a summary that the source artifact already supplied."""
    return ProcessorIdentity(
        processor_name=_SOURCE_PROVIDED_NAME,
        processor_version="artifact",
        prompt_version="source-supplied-summary",
        config_version="source-supplied-summary",
        model="none",
        provider="none",
    )


def source_provided_classification_identity() -> ProcessorIdentity:
    """Honest identity for classification tags that the source artifact already supplied."""
    return ProcessorIdentity(
        processor_name=_SOURCE_PROVIDED_NAME,
        processor_version="artifact",
        prompt_version="source-supplied-tags",
        config_version="source-supplied-tags",
        model="none",
        provider="none",
    )


def is_source_provided_identity(identity: ProcessorIdentity) -> bool:
    return identity.processor_name == _SOURCE_PROVIDED_NAME
