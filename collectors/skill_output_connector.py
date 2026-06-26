"""Connector for external skill output envelopes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from core.config import Config, config
from core.metadata_db import IngestionQueueEntry, MetadataDB, get_metadata_db
from core.path_layout import PathLayout, build_path_layout


DEFAULT_FILE_PATTERNS = ("*.json", "*.jsonl")
SUPPORTED_ARTIFACT_TYPES = {
    "paper",
    "repository",
    "transcript",
    "tweet",
    "video",
    "web_clipper",
}
FORBIDDEN_WIKI_WRITE_KEYS = {
    "compiled_wiki_path",
    "page_path",
    "thoth_slug",
    "wiki_output_path",
    "wiki_path",
}
ENVELOPE_KEYS = {
    "artifact_id",
    "artifact_type",
    "capabilities",
    "payload",
    "priority",
    "source",
    "source_name",
    "type",
}


@dataclass(frozen=True)
class SkillOutputRecord:
    """Queued artifact produced from one skill output envelope."""

    artifact_id: str
    artifact_type: str
    source_name: str
    raw_output_path: Path
    queued: bool = True


@dataclass(frozen=True)
class SkillOutputResult:
    """Summary of one skill output connector run."""

    records: tuple[SkillOutputRecord, ...] = field(default_factory=tuple)
    output_paths: tuple[str, ...] = field(default_factory=tuple)
    output_dirs: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [
                {
                    "artifact_id": record.artifact_id,
                    "artifact_type": record.artifact_type,
                    "source_name": record.source_name,
                    "raw_output_path": str(record.raw_output_path),
                    "queued": record.queued,
                }
                for record in self.records
            ],
            "queued_count": sum(1 for record in self.records if record.queued),
            "output_paths": list(self.output_paths),
            "output_dirs": list(self.output_dirs),
        }


@dataclass(frozen=True)
class _PreparedEnvelope:
    artifact_id: str
    artifact_type: str
    source_name: str
    priority: int
    payload: dict[str, Any]
    capabilities: tuple[str, ...]


class SkillOutputConnector:
    """Ingest external skill outputs through the shared artifact queue."""

    def __init__(
        self,
        runtime_config: Config | None = None,
        *,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
    ):
        self.config = runtime_config or config
        self.layout = layout or build_path_layout(self.config)
        self.layout.ensure_directories()
        self.db = db or get_metadata_db()

    async def collect(
        self,
        *,
        output_paths: Iterable[str | Path] | None = None,
        output_dirs: Iterable[str | Path] | None = None,
        file_patterns: Iterable[str] | None = None,
        source_name: str | None = None,
        limit: int | None = None,
    ) -> SkillOutputResult:
        """Collect skill output envelope files and queue declared artifacts."""
        configured = self.config.get("sources.skill_outputs", {}) or {}
        if not isinstance(configured, Mapping):
            configured = {}

        resolved_source = _clean_string(
            source_name or configured.get("source_name")
        ) or "external_skill"
        resolved_paths = _path_list(output_paths) or _path_list(
            configured.get("output_paths") or configured.get("output_path")
        )
        resolved_dirs = _path_list(output_dirs) or _path_list(
            configured.get("output_dirs") or configured.get("output_dir")
        )
        resolved_patterns = tuple(
            _string_list(file_patterns)
            or _string_list(configured.get("file_patterns"))
            or DEFAULT_FILE_PATTERNS
        )
        if not resolved_paths and not resolved_dirs:
            raise ValueError("Skill output ingestion requires output_paths or output_dirs")

        input_files = self._resolve_input_files(
            output_paths=resolved_paths,
            output_dirs=resolved_dirs,
            file_patterns=resolved_patterns,
        )
        if limit is not None:
            input_files = input_files[: max(0, int(limit))]

        records: list[SkillOutputRecord] = []
        for output_path in input_files:
            envelope_payloads = await asyncio.to_thread(
                self._load_envelope_payloads,
                output_path,
            )
            for envelope_payload in envelope_payloads:
                envelope_source_name = _source_name_from_envelope(
                    envelope_payload,
                    default_source_name=resolved_source,
                )
                raw_output_path = await asyncio.to_thread(
                    self._preserve_raw_output,
                    output_path,
                    envelope_source_name,
                )
                envelope = self._prepare_envelope(
                    envelope_payload,
                    default_source_name=resolved_source,
                    raw_output_path=raw_output_path,
                )
                self._queue_envelope(envelope)
                records.append(
                    SkillOutputRecord(
                        artifact_id=envelope.artifact_id,
                        artifact_type=envelope.artifact_type,
                        source_name=envelope.source_name,
                        raw_output_path=raw_output_path,
                    )
                )

        return SkillOutputResult(
            records=tuple(records),
            output_paths=tuple(str(path) for path in resolved_paths),
            output_dirs=tuple(str(path) for path in resolved_dirs),
        )

    def _resolve_input_files(
        self,
        *,
        output_paths: Iterable[Path],
        output_dirs: Iterable[Path],
        file_patterns: Iterable[str],
    ) -> list[Path]:
        candidates: list[Path] = []
        for path in output_paths:
            resolved = path.expanduser()
            if not resolved.exists():
                raise FileNotFoundError(f"Skill output path does not exist: {resolved}")
            if resolved.is_dir():
                candidates.extend(_files_in_dir(resolved, file_patterns))
            elif resolved.is_file():
                candidates.append(resolved)
            else:
                raise ValueError(f"Skill output path is not a file: {resolved}")

        for directory in output_dirs:
            resolved = directory.expanduser()
            if not resolved.exists():
                raise FileNotFoundError(
                    f"Skill output directory does not exist: {resolved}"
                )
            if not resolved.is_dir():
                raise ValueError(
                    f"Skill output directory is not a directory: {resolved}"
                )
            candidates.extend(_files_in_dir(resolved, file_patterns))

        return _dedupe_paths(candidates)

    def _preserve_raw_output(self, output_path: Path, source_name: str) -> Path:
        raw_root = self.layout.raw_root / "skill_outputs" / _safe_slug(source_name)
        raw_root.mkdir(parents=True, exist_ok=True)
        payload = output_path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()[:12]
        raw_path = raw_root / (
            f"{_safe_slug(output_path.stem)}-{digest}{output_path.suffix.lower()}"
        )
        try:
            if output_path.resolve() == raw_path.resolve():
                return raw_path
        except FileNotFoundError:
            pass
        if not raw_path.exists():
            shutil.copy2(output_path, raw_path)
        return raw_path

    def _load_envelope_payloads(
        self,
        output_path: Path,
    ) -> list[Any]:
        suffix = output_path.suffix.lower()
        text = output_path.read_text(encoding="utf-8")
        if suffix == ".jsonl":
            payloads = [
                json.loads(line)
                for line in text.splitlines()
                if line.strip()
            ]
        elif suffix == ".json":
            parsed = json.loads(text)
            payloads = _envelope_payloads_from_json(parsed)
        else:
            raise ValueError(f"Unsupported skill output file type: {output_path}")
        return payloads

    def _prepare_envelope(
        self,
        envelope: Any,
        *,
        default_source_name: str,
        raw_output_path: Path,
    ) -> _PreparedEnvelope:
        if not isinstance(envelope, Mapping):
            raise ValueError("Skill output envelope must be an object")

        source_name = _clean_string(
            envelope.get("source_name") or envelope.get("source")
        ) or default_source_name
        artifact_type = _clean_string(
            envelope.get("artifact_type") or envelope.get("type")
        )
        if not artifact_type:
            raise ValueError("Skill output envelope missing artifact_type")
        artifact_type = artifact_type.lower()
        if artifact_type not in SUPPORTED_ARTIFACT_TYPES:
            raise ValueError(f"Unsupported skill output artifact type: {artifact_type}")

        payload_value = envelope.get("payload")
        if payload_value is None:
            payload = {
                str(key): value
                for key, value in envelope.items()
                if key not in ENVELOPE_KEYS
            }
        elif isinstance(payload_value, Mapping):
            payload = dict(payload_value)
        else:
            raise ValueError("Skill output envelope payload must be an object")

        reject_direct_wiki_write_claims(envelope, wiki_root=self.layout.wiki_root)
        reject_direct_wiki_write_claims(payload, wiki_root=self.layout.wiki_root)

        raw_output_ref = self._relative_to_vault(raw_output_path)
        artifact_id = _clean_string(envelope.get("artifact_id") or payload.get("id"))
        if not artifact_id:
            digest = hashlib.sha256(
                json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:12]
            artifact_id = f"{_safe_slug(source_name)}_{artifact_type}_{digest}"

        payload.setdefault("id", artifact_id)
        payload.setdefault("source_type", source_name)
        if artifact_type == "transcript":
            payload.setdefault("transcript_id", artifact_id)

        custom_metadata = payload.get("custom_metadata")
        if not isinstance(custom_metadata, Mapping):
            custom_metadata = {}
        payload["custom_metadata"] = {
            **dict(custom_metadata),
            "raw_payload_path": raw_output_ref,
            "skill_output_path": raw_output_ref,
            "skill_source_name": source_name,
        }

        normalized_metadata = payload.get("normalized_metadata")
        if not isinstance(normalized_metadata, Mapping):
            normalized_metadata = {}
        payload["normalized_metadata"] = {
            **dict(normalized_metadata),
            "artifact_id": artifact_id,
            "source_type": source_name,
            "skill_output_source": source_name,
        }

        capabilities = tuple(_string_list(envelope.get("capabilities")))
        priority = _optional_int(envelope.get("priority"), default=0)
        return _PreparedEnvelope(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            source_name=source_name,
            priority=priority,
            payload=payload,
            capabilities=capabilities,
        )

    def _queue_envelope(self, envelope: _PreparedEnvelope) -> None:
        entry = IngestionQueueEntry(
            artifact_id=envelope.artifact_id,
            artifact_type=envelope.artifact_type,
            source=envelope.source_name,
            payload_json=json.dumps(envelope.payload, ensure_ascii=False),
            created_at=datetime.now().isoformat(),
            capabilities_json=json.dumps(list(envelope.capabilities)),
            priority=envelope.priority,
        )
        if not self.db.upsert_ingestion_entry(entry):
            raise RuntimeError(f"Failed to queue skill output artifact: {envelope.artifact_id}")

    def _relative_to_vault(self, path: Path | None) -> str | None:
        if path is None:
            return None
        try:
            return path.relative_to(self.layout.vault_root).as_posix()
        except ValueError:
            return str(path)


def _envelope_payloads_from_json(payload: Any) -> list[Any]:
    if isinstance(payload, Mapping):
        artifacts = payload.get("artifacts") or payload.get("items")
        if isinstance(artifacts, list):
            return artifacts
        return [payload]
    if isinstance(payload, list):
        return payload
    raise ValueError("Skill output JSON must contain an object or array")


def reject_direct_wiki_write_claims(
    value: Any,
    *,
    wiki_root: Path | None = None,
) -> None:
    """Reject envelope claims that try to steer output into compiled wiki paths."""
    if isinstance(value, Mapping):
        forbidden = sorted(FORBIDDEN_WIKI_WRITE_KEYS.intersection(value.keys()))
        if forbidden:
            raise ValueError(
                "Skill outputs cannot declare direct wiki write fields: "
                + ", ".join(forbidden)
            )
        for nested in value.values():
            reject_direct_wiki_write_claims(nested, wiki_root=wiki_root)
    elif isinstance(value, list):
        for item in value:
            reject_direct_wiki_write_claims(item, wiki_root=wiki_root)
    elif isinstance(value, str) and _looks_like_direct_wiki_path(
        value,
        wiki_root=wiki_root,
    ):
        raise ValueError(
            "Skill outputs cannot target direct wiki paths: " + value.strip()
        )


def _looks_like_direct_wiki_path(value: str, *, wiki_root: Path | None) -> bool:
    text = value.strip().strip("'\"")
    if not text:
        return False
    lowered = text.lower().replace("\\", "/")
    if lowered.startswith(("http://", "https://")):
        return False
    for prefix in ("local_file:", "file://", "file:", "path:", "output:", "target:"):
        if lowered.startswith(prefix):
            lowered = lowered[len(prefix) :].lstrip()
            break
    if lowered.startswith(("wiki/", "./wiki/", "../wiki/")):
        return True
    if "/wiki/" in lowered:
        return True
    if wiki_root is None:
        return False
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        return False
    try:
        candidate.resolve().relative_to(wiki_root.resolve())
    except ValueError:
        return False
    return True


def _source_name_from_envelope(
    envelope: Any,
    *,
    default_source_name: str,
) -> str:
    if not isinstance(envelope, Mapping):
        return default_source_name
    return _clean_string(envelope.get("source_name") or envelope.get("source")) or default_source_name


def _files_in_dir(directory: Path, file_patterns: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in file_patterns:
        files.extend(path for path in directory.rglob(pattern) if path.is_file())
    return sorted(files)


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    deduped: dict[Path, Path] = {}
    for path in paths:
        deduped[path.resolve()] = path
    return list(deduped.values())


def _path_list(value: Any) -> list[Path]:
    return [Path(item).expanduser() for item in _string_list(value)]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    chars = [char if char.isalnum() else "-" for char in text]
    slug = "-".join(part for part in "".join(chars).split("-") if part)
    return slug or "skill"


def _optional_int(value: Any, *, default: int) -> int:
    if value in (None, ""):
        return default
    return int(value)
