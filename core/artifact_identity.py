"""Shared artifact identity helpers for queue review and capture lifecycle."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


DEFAULT_NATIVE_ID_KEYS = ("id", "artifact_id")
NATIVE_ID_KEYS_BY_ARTIFACT_TYPE: dict[str, tuple[str, ...]] = {
    "tweet": ("tweet_id", "id", "artifact_id"),
    "paper": ("id", "arxiv_id", "doi", "artifact_id"),
    "repository": ("id", "repo_name", "full_name", "artifact_id"),
    "web_clipper": (
        "id",
        "artifact_id",
        "source_relative_path",
        "source_path",
    ),
    "markdown": (
        "id",
        "artifact_id",
        "source_relative_path",
        "source_path",
    ),
    "video": ("video_id", "native_id", "id", "artifact_id"),
    "transcript": ("transcript_id", "id", "artifact_id", "video_id"),
}


def native_id_keys_for_artifact_type(artifact_type: str) -> tuple[str, ...]:
    """Return candidate payload keys that identify one artifact type."""
    normalized = str(artifact_type or "").strip().lower()
    return NATIVE_ID_KEYS_BY_ARTIFACT_TYPE.get(normalized, DEFAULT_NATIVE_ID_KEYS)


def native_id_candidates_for_artifact_type(
    artifact_type: str,
    payload: Mapping[str, Any],
) -> tuple[Any, ...]:
    """Return raw native-ID candidate values in canonical priority order."""
    keys = native_id_keys_for_artifact_type(artifact_type)
    return tuple(payload.get(key) for key in keys)


def native_id_from_payload(
    artifact_type: str,
    payload: Mapping[str, Any],
) -> str | None:
    """Return the first non-empty native ID for an artifact payload."""
    for value in native_id_candidates_for_artifact_type(artifact_type, payload):
        text = str(value).strip() if value is not None else ""
        if text:
            return text
    return None
