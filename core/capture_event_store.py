"""Typed Postgres repository for Thoth capture events and raw artifacts."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from .postgres_migrations import DEFAULT_CAPTURE_SCHEMA, quote_identifier
from .prompt_security import (
    PROMPT_SECURITY_SCANNER,
    THOTH_REDACTION_METADATA_KEY,
    THOTH_SECURITY_FINDINGS_KEY,
    prompt_security_metadata_for_text,
)


JsonObject = dict[str, Any]


class CaptureEventStoreError(RuntimeError):
    """Raised when capture event records fail closed before persistence."""


def _new_id() -> str:
    return str(uuid.uuid4())


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_required(value: Any, field_name: str) -> str:
    text = _clean_optional(value)
    if not text:
        raise ValueError(f"{field_name} is required")
    return text


def _json_object(value: Mapping[str, Any] | None) -> JsonObject:
    return dict(value) if isinstance(value, Mapping) else {}


def _json_param(value: Mapping[str, Any] | None) -> str:
    return json.dumps(_json_object(value), ensure_ascii=False, sort_keys=True)


def _row_value(row: Any, index: int, name: str) -> Any:
    if isinstance(row, Mapping):
        return row[name]
    return row[index]


def _read_json(value: Any) -> JsonObject:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        payload = json.loads(value)
        return dict(payload) if isinstance(payload, Mapping) else {}
    return {}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_raw_roots(raw_roots: Sequence[str | Path]) -> tuple[Path, ...]:
    roots: list[Path] = []
    for raw_root in raw_roots:
        root = Path(raw_root).expanduser().resolve(strict=True)
        if not root.is_dir():
            raise CaptureEventStoreError(f"raw root is not a directory: {raw_root}")
        roots.append(root)
    return tuple(roots)


def _path_is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _validate_raw_file(
    path: str | Path,
    *,
    raw_roots: Sequence[str | Path] = (),
) -> tuple[Path, str | None]:
    candidate = Path(path).expanduser()
    if candidate.is_symlink():
        raise CaptureEventStoreError(f"raw artifact path must not be a symlink: {path}")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise CaptureEventStoreError(f"raw artifact path does not exist: {path}") from exc
    if not resolved.is_file():
        raise CaptureEventStoreError(f"raw artifact path is not a file: {path}")

    roots = _resolve_raw_roots(raw_roots)
    matching_root = next(
        (root for root in roots if _path_is_relative_to(resolved, root)),
        None,
    )
    if roots and matching_root is None:
        raise CaptureEventStoreError(
            f"raw artifact path is outside configured raw roots: {resolved}"
        )
    return resolved, str(matching_root) if matching_root else None


def _relation(schema: str, table_name: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table_name)}"


@dataclass(frozen=True)
class CaptureSource:
    """Upstream capture source such as X bookmarks, web clipper, or Omi."""

    source_name: str
    source_type: str
    source_id: str = field(default_factory=_new_id)
    collector: str | None = None
    account: str | None = None
    native_source_id: str | None = None
    base_uri: str | None = None
    status: str = "active"
    config: JsonObject = field(default_factory=dict)
    metadata: JsonObject = field(default_factory=dict)
    created_at: Any = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _clean_required(self.source_id, "source_id"))
        object.__setattr__(self, "source_name", _clean_required(self.source_name, "source_name"))
        object.__setattr__(self, "source_type", _clean_required(self.source_type, "source_type"))
        object.__setattr__(self, "status", _clean_required(self.status, "status"))
        object.__setattr__(self, "collector", _clean_optional(self.collector))
        object.__setattr__(self, "account", _clean_optional(self.account))
        object.__setattr__(self, "native_source_id", _clean_optional(self.native_source_id))
        object.__setattr__(self, "base_uri", _clean_optional(self.base_uri))
        object.__setattr__(self, "config", _json_object(self.config))
        object.__setattr__(self, "metadata", _json_object(self.metadata))


@dataclass(frozen=True)
class CaptureSession:
    """A bounded capture run from a source."""

    source_id: str
    session_type: str
    session_id: str = field(default_factory=_new_id)
    native_session_id: str | None = None
    status: str = "open"
    started_at: Any = None
    ended_at: Any = None
    metadata: JsonObject = field(default_factory=dict)
    provenance: JsonObject = field(default_factory=dict)
    created_at: Any = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "session_id", _clean_required(self.session_id, "session_id"))
        object.__setattr__(self, "source_id", _clean_required(self.source_id, "source_id"))
        object.__setattr__(self, "session_type", _clean_required(self.session_type, "session_type"))
        object.__setattr__(self, "native_session_id", _clean_optional(self.native_session_id))
        object.__setattr__(self, "status", _clean_required(self.status, "status"))
        object.__setattr__(self, "metadata", _json_object(self.metadata))
        object.__setattr__(self, "provenance", _json_object(self.provenance))


@dataclass(frozen=True)
class CaptureEvent:
    """One durable captured event before downstream artifact processing."""

    source_id: str
    event_type: str
    event_id: str = field(default_factory=_new_id)
    session_id: str | None = None
    native_event_id: str | None = None
    status: str = "captured"
    occurred_at: Any = None
    captured_at: Any = None
    event_hash: str | None = None
    payload: JsonObject = field(default_factory=dict)
    privacy: JsonObject = field(default_factory=dict)
    retention: JsonObject = field(default_factory=dict)
    provenance: JsonObject = field(default_factory=dict)
    created_at: Any = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _clean_required(self.event_id, "event_id"))
        object.__setattr__(self, "source_id", _clean_required(self.source_id, "source_id"))
        object.__setattr__(self, "event_type", _clean_required(self.event_type, "event_type"))
        object.__setattr__(self, "session_id", _clean_optional(self.session_id))
        object.__setattr__(self, "native_event_id", _clean_optional(self.native_event_id))
        object.__setattr__(self, "status", _clean_required(self.status, "status"))
        object.__setattr__(self, "event_hash", _clean_optional(self.event_hash))
        object.__setattr__(self, "payload", _json_object(self.payload))
        object.__setattr__(self, "privacy", _json_object(self.privacy))
        object.__setattr__(self, "retention", _json_object(self.retention))
        object.__setattr__(self, "provenance", _json_object(self.provenance))


@dataclass(frozen=True)
class RawArtifactRef:
    """Immutable raw local file reference backing capture events."""

    source_id: str
    path: str
    raw_ref_id: str = field(default_factory=_new_id)
    event_id: str | None = None
    session_id: str | None = None
    raw_root: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None
    mime_type: str | None = None
    immutable: bool = True
    metadata: JsonObject = field(default_factory=dict)
    created_at: Any = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "raw_ref_id", _clean_required(self.raw_ref_id, "raw_ref_id"))
        object.__setattr__(self, "source_id", _clean_required(self.source_id, "source_id"))
        object.__setattr__(self, "path", _clean_required(self.path, "path"))
        object.__setattr__(self, "event_id", _clean_optional(self.event_id))
        object.__setattr__(self, "session_id", _clean_optional(self.session_id))
        object.__setattr__(self, "raw_root", _clean_optional(self.raw_root))
        object.__setattr__(self, "sha256", _clean_optional(self.sha256))
        object.__setattr__(self, "mime_type", _clean_optional(self.mime_type))
        object.__setattr__(self, "metadata", _json_object(self.metadata))
        if self.size_bytes is not None and int(self.size_bytes) < 0:
            raise ValueError("size_bytes must be non-negative")
        if not self.immutable:
            raise ValueError("raw artifact refs must be immutable")

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        source_id: str,
        event_id: str | None = None,
        session_id: str | None = None,
        raw_roots: Sequence[str | Path] = (),
        metadata: Mapping[str, Any] | None = None,
    ) -> "RawArtifactRef":
        resolved, raw_root = _validate_raw_file(path, raw_roots=raw_roots)
        guessed_mime, _ = mimetypes.guess_type(str(resolved))
        return cls(
            source_id=source_id,
            event_id=event_id,
            session_id=session_id,
            raw_root=raw_root,
            path=str(resolved),
            sha256=_sha256_file(resolved),
            size_bytes=resolved.stat().st_size,
            mime_type=guessed_mime,
            immutable=True,
            metadata=_json_object(metadata),
        )


@dataclass(frozen=True)
class ArtifactLink:
    """Link from a capture event to a downstream artifact record."""

    event_id: str
    artifact_id: str
    artifact_type: str
    artifact_link_id: str = field(default_factory=_new_id)
    raw_ref_id: str | None = None
    link_type: str = "source"
    metadata: JsonObject = field(default_factory=dict)
    created_at: Any = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifact_link_id", _clean_required(self.artifact_link_id, "artifact_link_id"))
        object.__setattr__(self, "event_id", _clean_required(self.event_id, "event_id"))
        object.__setattr__(self, "artifact_id", _clean_required(self.artifact_id, "artifact_id"))
        object.__setattr__(self, "artifact_type", _clean_required(self.artifact_type, "artifact_type"))
        object.__setattr__(self, "raw_ref_id", _clean_optional(self.raw_ref_id))
        object.__setattr__(self, "link_type", _clean_required(self.link_type, "link_type"))
        object.__setattr__(self, "metadata", _json_object(self.metadata))


@dataclass(frozen=True)
class SecurityFinding:
    """Security scanner finding attached to an event or raw ref."""

    finding_type: str
    finding_id: str = field(default_factory=_new_id)
    event_id: str | None = None
    raw_ref_id: str | None = None
    severity: str = "info"
    status: str = "open"
    scanner: str | None = None
    fingerprint: str | None = None
    detected_at: Any = None
    details: JsonObject = field(default_factory=dict)
    created_at: Any = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "finding_id", _clean_required(self.finding_id, "finding_id"))
        object.__setattr__(self, "event_id", _clean_optional(self.event_id))
        object.__setattr__(self, "raw_ref_id", _clean_optional(self.raw_ref_id))
        if not self.event_id and not self.raw_ref_id:
            raise ValueError("security findings require event_id or raw_ref_id")
        object.__setattr__(self, "finding_type", _clean_required(self.finding_type, "finding_type"))
        object.__setattr__(self, "severity", _clean_required(self.severity, "severity"))
        object.__setattr__(self, "status", _clean_required(self.status, "status"))
        object.__setattr__(self, "scanner", _clean_optional(self.scanner))
        object.__setattr__(self, "fingerprint", _clean_optional(self.fingerprint))
        object.__setattr__(self, "details", _json_object(self.details))


@dataclass(frozen=True)
class PrivacyAnnotation:
    """Privacy label or handling requirement for a captured event."""

    event_id: str
    classification: str
    privacy_id: str = field(default_factory=_new_id)
    raw_ref_id: str | None = None
    scope: str = "event"
    policy: str | None = None
    subject_ref: str = ""
    metadata: JsonObject = field(default_factory=dict)
    created_at: Any = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "privacy_id", _clean_required(self.privacy_id, "privacy_id"))
        object.__setattr__(self, "event_id", _clean_required(self.event_id, "event_id"))
        object.__setattr__(self, "raw_ref_id", _clean_optional(self.raw_ref_id))
        object.__setattr__(self, "scope", _clean_required(self.scope, "scope"))
        object.__setattr__(self, "classification", _clean_required(self.classification, "classification"))
        object.__setattr__(self, "policy", _clean_optional(self.policy))
        object.__setattr__(self, "subject_ref", _clean_optional(self.subject_ref) or "")
        object.__setattr__(self, "metadata", _json_object(self.metadata))


@dataclass(frozen=True)
class RetentionPolicy:
    """Retention policy attached to an event, raw ref, or artifact link."""

    target_type: str
    target_id: str
    policy_name: str
    retention_id: str = field(default_factory=_new_id)
    action: str = "retain"
    retain_until: Any = None
    delete_after: Any = None
    legal_hold: bool = False
    metadata: JsonObject = field(default_factory=dict)
    created_at: Any = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "retention_id", _clean_required(self.retention_id, "retention_id"))
        object.__setattr__(self, "target_type", _clean_required(self.target_type, "target_type"))
        object.__setattr__(self, "target_id", _clean_required(self.target_id, "target_id"))
        object.__setattr__(self, "policy_name", _clean_required(self.policy_name, "policy_name"))
        object.__setattr__(self, "action", _clean_required(self.action, "action"))
        object.__setattr__(self, "legal_hold", bool(self.legal_hold))
        object.__setattr__(self, "metadata", _json_object(self.metadata))


@dataclass(frozen=True)
class ProvenanceRecord:
    """Audit/provenance fact for a capture event-store target."""

    target_type: str
    target_id: str
    operation: str
    provenance_id: str = field(default_factory=_new_id)
    actor: str | None = None
    tool: str | None = None
    fingerprint: str | None = None
    occurred_at: Any = None
    metadata: JsonObject = field(default_factory=dict)
    created_at: Any = None
    updated_at: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "provenance_id", _clean_required(self.provenance_id, "provenance_id"))
        object.__setattr__(self, "target_type", _clean_required(self.target_type, "target_type"))
        object.__setattr__(self, "target_id", _clean_required(self.target_id, "target_id"))
        object.__setattr__(self, "operation", _clean_required(self.operation, "operation"))
        object.__setattr__(self, "actor", _clean_optional(self.actor))
        object.__setattr__(self, "tool", _clean_optional(self.tool))
        object.__setattr__(self, "fingerprint", _clean_optional(self.fingerprint))
        object.__setattr__(self, "metadata", _json_object(self.metadata))


class CaptureEventStore:
    """Repository methods for the Postgres-backed capture event contract."""

    def __init__(
        self,
        conn,
        *,
        schema: str = DEFAULT_CAPTURE_SCHEMA,
        raw_roots: Sequence[str | Path] = (),
    ) -> None:
        self.conn = conn
        self.schema = schema
        self.raw_roots = tuple(raw_roots)

    def upsert_source(self, source: CaptureSource) -> CaptureSource:
        row = self.conn.execute(
            f"""
            INSERT INTO {_relation(self.schema, "capture_sources")} (
                source_id, source_name, source_type, collector, account,
                native_source_id, base_uri, status, config, metadata, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, COALESCE(%s, now()), now())
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = excluded.source_type,
                collector = excluded.collector,
                account = excluded.account,
                native_source_id = excluded.native_source_id,
                base_uri = excluded.base_uri,
                status = excluded.status,
                config = excluded.config,
                metadata = excluded.metadata,
                updated_at = now()
            RETURNING source_id, source_name, source_type, collector, account,
                native_source_id, base_uri, status, config, metadata, created_at, updated_at
            """,
            (
                source.source_id,
                source.source_name,
                source.source_type,
                source.collector,
                source.account,
                source.native_source_id,
                source.base_uri,
                source.status,
                _json_param(source.config),
                _json_param(source.metadata),
                source.created_at,
            ),
        ).fetchone()
        return _source_from_row(row)

    def get_source(self, source_id: str) -> CaptureSource | None:
        return self._fetch_one_source("source_id = %s", (_clean_required(source_id, "source_id"),))

    def get_source_by_name(self, source_name: str) -> CaptureSource | None:
        return self._fetch_one_source("source_name = %s", (_clean_required(source_name, "source_name"),))

    def list_sources(self) -> tuple[CaptureSource, ...]:
        rows = self.conn.execute(
            f"""
            SELECT source_id, source_name, source_type, collector, account,
                native_source_id, base_uri, status, config, metadata, created_at, updated_at
            FROM {_relation(self.schema, "capture_sources")}
            ORDER BY source_name
            """
        ).fetchall()
        return tuple(_source_from_row(row) for row in rows)

    def upsert_session(self, session: CaptureSession) -> CaptureSession:
        conflict = (
            "ON CONFLICT (source_id, native_session_id) WHERE native_session_id IS NOT NULL"
            if session.native_session_id
            else "ON CONFLICT (session_id)"
        )
        row = self.conn.execute(
            f"""
            INSERT INTO {_relation(self.schema, "capture_sessions")} (
                session_id, source_id, native_session_id, session_type, status,
                started_at, ended_at, metadata, provenance, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, COALESCE(%s, now()), %s, %s::jsonb, %s::jsonb, COALESCE(%s, now()), now())
            {conflict} DO UPDATE SET
                session_type = excluded.session_type,
                status = excluded.status,
                ended_at = excluded.ended_at,
                metadata = excluded.metadata,
                provenance = excluded.provenance,
                updated_at = now()
            RETURNING session_id, source_id, native_session_id, session_type, status,
                started_at, ended_at, metadata, provenance, created_at, updated_at
            """,
            (
                session.session_id,
                session.source_id,
                session.native_session_id,
                session.session_type,
                session.status,
                session.started_at,
                session.ended_at,
                _json_param(session.metadata),
                _json_param(session.provenance),
                session.created_at,
            ),
        ).fetchone()
        return _session_from_row(row)

    def get_session(self, session_id: str) -> CaptureSession | None:
        return self._fetch_one_session("session_id = %s", (_clean_required(session_id, "session_id"),))

    def list_sessions(self, *, source_id: str | None = None) -> tuple[CaptureSession, ...]:
        where = ""
        params: tuple[Any, ...] = ()
        if source_id:
            where = "WHERE source_id = %s"
            params = (_clean_required(source_id, "source_id"),)
        rows = self.conn.execute(
            f"""
            SELECT session_id, source_id, native_session_id, session_type, status,
                started_at, ended_at, metadata, provenance, created_at, updated_at
            FROM {_relation(self.schema, "capture_sessions")}
            {where}
            ORDER BY started_at, session_id
            """,
            params,
        ).fetchall()
        return tuple(_session_from_row(row) for row in rows)

    def upsert_event(self, event: CaptureEvent) -> CaptureEvent:
        if event.native_event_id:
            conflict = "ON CONFLICT (source_id, native_event_id) WHERE native_event_id IS NOT NULL"
        elif event.event_hash:
            conflict = "ON CONFLICT (source_id, event_hash) WHERE event_hash IS NOT NULL"
        else:
            conflict = "ON CONFLICT (event_id)"
        row = self.conn.execute(
            f"""
            INSERT INTO {_relation(self.schema, "capture_events")} (
                event_id, source_id, session_id, native_event_id, event_type, status,
                occurred_at, captured_at, event_hash, payload, privacy, retention,
                provenance, created_at, updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()), %s,
                %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, COALESCE(%s, now()), now()
            )
            {conflict} DO UPDATE SET
                session_id = excluded.session_id,
                event_type = excluded.event_type,
                status = excluded.status,
                occurred_at = excluded.occurred_at,
                captured_at = excluded.captured_at,
                payload = excluded.payload,
                privacy = excluded.privacy,
                retention = excluded.retention,
                provenance = excluded.provenance,
                updated_at = now()
            RETURNING event_id, source_id, session_id, native_event_id, event_type, status,
                occurred_at, captured_at, event_hash, payload, privacy, retention,
                provenance, created_at, updated_at
            """,
            (
                event.event_id,
                event.source_id,
                event.session_id,
                event.native_event_id,
                event.event_type,
                event.status,
                event.occurred_at,
                event.captured_at,
                event.event_hash,
                _json_param(event.payload),
                _json_param(event.privacy),
                _json_param(event.retention),
                _json_param(event.provenance),
                event.created_at,
            ),
        ).fetchone()
        return _event_from_row(row)

    def get_event(self, event_id: str) -> CaptureEvent | None:
        return self._fetch_one_event("event_id = %s", (_clean_required(event_id, "event_id"),))

    def list_events(
        self,
        *,
        source_id: str | None = None,
        session_id: str | None = None,
    ) -> tuple[CaptureEvent, ...]:
        filters: list[str] = []
        params: list[Any] = []
        if source_id:
            filters.append("source_id = %s")
            params.append(_clean_required(source_id, "source_id"))
        if session_id:
            filters.append("session_id = %s")
            params.append(_clean_required(session_id, "session_id"))
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        rows = self.conn.execute(
            f"""
            SELECT event_id, source_id, session_id, native_event_id, event_type, status,
                occurred_at, captured_at, event_hash, payload, privacy, retention,
                provenance, created_at, updated_at
            FROM {_relation(self.schema, "capture_events")}
            {where}
            ORDER BY captured_at, event_id
            """,
            tuple(params),
        ).fetchall()
        return tuple(_event_from_row(row) for row in rows)

    def upsert_raw_ref(self, raw_ref: RawArtifactRef) -> RawArtifactRef:
        normalized = self._normalize_raw_ref(raw_ref)
        conflict = (
            "ON CONFLICT (sha256) WHERE sha256 IS NOT NULL"
            if normalized.sha256
            else "ON CONFLICT (path)"
        )
        row = self.conn.execute(
            f"""
            INSERT INTO {_relation(self.schema, "raw_artifact_refs")} AS existing (
                raw_ref_id, event_id, source_id, session_id, raw_root, path, sha256,
                size_bytes, mime_type, immutable, metadata, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s::jsonb, COALESCE(%s, now()), now())
            {conflict} DO UPDATE SET
                event_id = COALESCE(excluded.event_id, existing.event_id),
                source_id = excluded.source_id,
                session_id = COALESCE(excluded.session_id, existing.session_id),
                raw_root = excluded.raw_root,
                path = excluded.path,
                size_bytes = excluded.size_bytes,
                mime_type = excluded.mime_type,
                metadata = excluded.metadata,
                updated_at = now()
            RETURNING raw_ref_id, event_id, source_id, session_id, raw_root, path, sha256,
                size_bytes, mime_type, immutable, metadata, created_at, updated_at
            """,
            (
                normalized.raw_ref_id,
                normalized.event_id,
                normalized.source_id,
                normalized.session_id,
                normalized.raw_root,
                normalized.path,
                normalized.sha256,
                normalized.size_bytes,
                normalized.mime_type,
                _json_param(normalized.metadata),
                normalized.created_at,
            ),
        ).fetchone()
        return _raw_ref_from_row(row)

    def get_raw_ref(self, raw_ref_id: str) -> RawArtifactRef | None:
        return self._fetch_one_raw_ref("raw_ref_id = %s", (_clean_required(raw_ref_id, "raw_ref_id"),))

    def list_raw_refs(self, *, event_id: str | None = None) -> tuple[RawArtifactRef, ...]:
        where = ""
        params: tuple[Any, ...] = ()
        if event_id:
            where = "WHERE event_id = %s"
            params = (_clean_required(event_id, "event_id"),)
        rows = self.conn.execute(
            f"""
            SELECT raw_ref_id, event_id, source_id, session_id, raw_root, path, sha256,
                size_bytes, mime_type, immutable, metadata, created_at, updated_at
            FROM {_relation(self.schema, "raw_artifact_refs")}
            {where}
            ORDER BY created_at, raw_ref_id
            """,
            params,
        ).fetchall()
        return tuple(_raw_ref_from_row(row) for row in rows)

    def upsert_artifact_link(self, link: ArtifactLink) -> ArtifactLink:
        row = self.conn.execute(
            f"""
            INSERT INTO {_relation(self.schema, "artifact_links")} (
                artifact_link_id, event_id, raw_ref_id, artifact_id, artifact_type,
                link_type, metadata, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, COALESCE(%s, now()), now())
            ON CONFLICT (event_id, artifact_id, artifact_type, link_type) DO UPDATE SET
                raw_ref_id = excluded.raw_ref_id,
                metadata = excluded.metadata,
                updated_at = now()
            RETURNING artifact_link_id, event_id, raw_ref_id, artifact_id, artifact_type,
                link_type, metadata, created_at, updated_at
            """,
            (
                link.artifact_link_id,
                link.event_id,
                link.raw_ref_id,
                link.artifact_id,
                link.artifact_type,
                link.link_type,
                _json_param(link.metadata),
                link.created_at,
            ),
        ).fetchone()
        return _artifact_link_from_row(row)

    def list_artifact_links(self, *, event_id: str | None = None) -> tuple[ArtifactLink, ...]:
        where = ""
        params: tuple[Any, ...] = ()
        if event_id:
            where = "WHERE event_id = %s"
            params = (_clean_required(event_id, "event_id"),)
        rows = self.conn.execute(
            f"""
            SELECT artifact_link_id, event_id, raw_ref_id, artifact_id, artifact_type,
                link_type, metadata, created_at, updated_at
            FROM {_relation(self.schema, "artifact_links")}
            {where}
            ORDER BY created_at, artifact_link_id
            """,
            params,
        ).fetchall()
        return tuple(_artifact_link_from_row(row) for row in rows)

    def get_artifact_link(self, artifact_link_id: str) -> ArtifactLink | None:
        return self._fetch_one_artifact_link(
            "artifact_link_id = %s",
            (_clean_required(artifact_link_id, "artifact_link_id"),),
        )

    def upsert_security_finding(self, finding: SecurityFinding) -> SecurityFinding:
        if finding.event_id and finding.fingerprint:
            conflict = "ON CONFLICT (event_id, fingerprint) WHERE event_id IS NOT NULL AND fingerprint IS NOT NULL"
        elif finding.raw_ref_id and finding.fingerprint:
            conflict = "ON CONFLICT (raw_ref_id, fingerprint) WHERE raw_ref_id IS NOT NULL AND fingerprint IS NOT NULL"
        else:
            conflict = "ON CONFLICT (finding_id)"
        row = self.conn.execute(
            f"""
            INSERT INTO {_relation(self.schema, "security_findings")} (
                finding_id, event_id, raw_ref_id, finding_type, severity, status,
                scanner, fingerprint, detected_at, details, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()), %s::jsonb, COALESCE(%s, now()), now())
            {conflict} DO UPDATE SET
                raw_ref_id = excluded.raw_ref_id,
                finding_type = excluded.finding_type,
                severity = excluded.severity,
                status = excluded.status,
                scanner = excluded.scanner,
                details = excluded.details,
                updated_at = now()
            RETURNING finding_id, event_id, raw_ref_id, finding_type, severity, status,
                scanner, fingerprint, detected_at, details, created_at, updated_at
            """,
            (
                finding.finding_id,
                finding.event_id,
                finding.raw_ref_id,
                finding.finding_type,
                finding.severity,
                finding.status,
                finding.scanner,
                finding.fingerprint,
                finding.detected_at,
                _json_param(finding.details),
                finding.created_at,
            ),
        ).fetchone()
        return _security_finding_from_row(row)

    def upsert_prompt_security_findings(
        self,
        *,
        content: str,
        source_label: str,
        event_id: str | None = None,
        raw_ref_id: str | None = None,
    ) -> tuple[SecurityFinding, ...]:
        """Scan source content and persist prompt-security findings."""
        metadata = prompt_security_metadata_for_text(
            content,
            source_label=source_label,
            scope="context",
        )
        return self.upsert_security_findings_from_metadata(
            metadata,
            event_id=event_id,
            raw_ref_id=raw_ref_id,
        )

    def upsert_security_findings_from_metadata(
        self,
        metadata: Mapping[str, Any],
        *,
        event_id: str | None = None,
        raw_ref_id: str | None = None,
    ) -> tuple[SecurityFinding, ...]:
        """Persist serialized prompt-security metadata as capture findings."""
        if not event_id and not raw_ref_id:
            raise ValueError("security findings require event_id or raw_ref_id")
        findings = metadata.get(THOTH_SECURITY_FINDINGS_KEY)
        if not isinstance(findings, list):
            return ()
        redaction_metadata = metadata.get(THOTH_REDACTION_METADATA_KEY)
        persisted: list[SecurityFinding] = []
        for finding in findings:
            if not isinstance(finding, Mapping):
                continue
            pattern_id = _clean_optional(finding.get("pattern_id"))
            if not pattern_id:
                continue
            details = dict(finding)
            if isinstance(redaction_metadata, Mapping):
                details[THOTH_REDACTION_METADATA_KEY] = dict(redaction_metadata)
            persisted.append(
                self.upsert_security_finding(
                    SecurityFinding(
                        event_id=event_id,
                        raw_ref_id=raw_ref_id,
                        finding_type=str(
                            finding.get("finding_type") or "prompt_security"
                        ),
                        severity=str(finding.get("severity") or "info"),
                        status=str(finding.get("status") or "open"),
                        scanner=str(
                            finding.get("scanner") or PROMPT_SECURITY_SCANNER
                        ),
                        fingerprint=str(
                            finding.get("fingerprint")
                            or f"{PROMPT_SECURITY_SCANNER}:{pattern_id}"
                        ),
                        details=details,
                    )
                )
            )
        return tuple(persisted)

    def list_security_findings(self, *, event_id: str | None = None) -> tuple[SecurityFinding, ...]:
        where = ""
        params: tuple[Any, ...] = ()
        if event_id:
            where = "WHERE event_id = %s"
            params = (_clean_required(event_id, "event_id"),)
        rows = self.conn.execute(
            f"""
            SELECT finding_id, event_id, raw_ref_id, finding_type, severity, status,
                scanner, fingerprint, detected_at, details, created_at, updated_at
            FROM {_relation(self.schema, "security_findings")}
            {where}
            ORDER BY detected_at, finding_id
            """,
            params,
        ).fetchall()
        return tuple(_security_finding_from_row(row) for row in rows)

    def get_security_finding(self, finding_id: str) -> SecurityFinding | None:
        return self._fetch_one_security_finding(
            "finding_id = %s",
            (_clean_required(finding_id, "finding_id"),),
        )

    def upsert_privacy_annotation(self, privacy: PrivacyAnnotation) -> PrivacyAnnotation:
        row = self.conn.execute(
            f"""
            INSERT INTO {_relation(self.schema, "privacy_annotations")} (
                privacy_id, event_id, raw_ref_id, scope, classification, policy,
                subject_ref, metadata, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, COALESCE(%s, now()), now())
            ON CONFLICT (event_id, scope, classification, subject_ref) DO UPDATE SET
                raw_ref_id = excluded.raw_ref_id,
                policy = excluded.policy,
                metadata = excluded.metadata,
                updated_at = now()
            RETURNING privacy_id, event_id, raw_ref_id, scope, classification, policy,
                subject_ref, metadata, created_at, updated_at
            """,
            (
                privacy.privacy_id,
                privacy.event_id,
                privacy.raw_ref_id,
                privacy.scope,
                privacy.classification,
                privacy.policy,
                privacy.subject_ref,
                _json_param(privacy.metadata),
                privacy.created_at,
            ),
        ).fetchone()
        return _privacy_annotation_from_row(row)

    def list_privacy_annotations(self, *, event_id: str | None = None) -> tuple[PrivacyAnnotation, ...]:
        where = ""
        params: tuple[Any, ...] = ()
        if event_id:
            where = "WHERE event_id = %s"
            params = (_clean_required(event_id, "event_id"),)
        rows = self.conn.execute(
            f"""
            SELECT privacy_id, event_id, raw_ref_id, scope, classification, policy,
                subject_ref, metadata, created_at, updated_at
            FROM {_relation(self.schema, "privacy_annotations")}
            {where}
            ORDER BY created_at, privacy_id
            """,
            params,
        ).fetchall()
        return tuple(_privacy_annotation_from_row(row) for row in rows)

    def get_privacy_annotation(self, privacy_id: str) -> PrivacyAnnotation | None:
        return self._fetch_one_privacy_annotation(
            "privacy_id = %s",
            (_clean_required(privacy_id, "privacy_id"),),
        )

    def upsert_retention_policy(self, policy: RetentionPolicy) -> RetentionPolicy:
        row = self.conn.execute(
            f"""
            INSERT INTO {_relation(self.schema, "retention_policies")} (
                retention_id, target_type, target_id, policy_name, action,
                retain_until, delete_after, legal_hold, metadata, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, COALESCE(%s, now()), now())
            ON CONFLICT (target_type, target_id, policy_name) DO UPDATE SET
                action = excluded.action,
                retain_until = excluded.retain_until,
                delete_after = excluded.delete_after,
                legal_hold = excluded.legal_hold,
                metadata = excluded.metadata,
                updated_at = now()
            RETURNING retention_id, target_type, target_id, policy_name, action,
                retain_until, delete_after, legal_hold, metadata, created_at, updated_at
            """,
            (
                policy.retention_id,
                policy.target_type,
                policy.target_id,
                policy.policy_name,
                policy.action,
                policy.retain_until,
                policy.delete_after,
                policy.legal_hold,
                _json_param(policy.metadata),
                policy.created_at,
            ),
        ).fetchone()
        return _retention_policy_from_row(row)

    def list_retention_policies(
        self,
        *,
        target_type: str | None = None,
        target_id: str | None = None,
    ) -> tuple[RetentionPolicy, ...]:
        filters: list[str] = []
        params: list[Any] = []
        if target_type:
            filters.append("target_type = %s")
            params.append(_clean_required(target_type, "target_type"))
        if target_id:
            filters.append("target_id = %s")
            params.append(_clean_required(target_id, "target_id"))
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        rows = self.conn.execute(
            f"""
            SELECT retention_id, target_type, target_id, policy_name, action,
                retain_until, delete_after, legal_hold, metadata, created_at, updated_at
            FROM {_relation(self.schema, "retention_policies")}
            {where}
            ORDER BY created_at, retention_id
            """,
            tuple(params),
        ).fetchall()
        return tuple(_retention_policy_from_row(row) for row in rows)

    def get_retention_policy(self, retention_id: str) -> RetentionPolicy | None:
        return self._fetch_one_retention_policy(
            "retention_id = %s",
            (_clean_required(retention_id, "retention_id"),),
        )

    def upsert_provenance_record(self, provenance: ProvenanceRecord) -> ProvenanceRecord:
        conflict = (
            "ON CONFLICT (target_type, target_id, operation, fingerprint) WHERE fingerprint IS NOT NULL"
            if provenance.fingerprint
            else "ON CONFLICT (provenance_id)"
        )
        row = self.conn.execute(
            f"""
            INSERT INTO {_relation(self.schema, "provenance_records")} (
                provenance_id, target_type, target_id, operation, actor, tool,
                fingerprint, occurred_at, metadata, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()), %s::jsonb, COALESCE(%s, now()), now())
            {conflict} DO UPDATE SET
                actor = excluded.actor,
                tool = excluded.tool,
                metadata = excluded.metadata,
                updated_at = now()
            RETURNING provenance_id, target_type, target_id, operation, actor, tool,
                fingerprint, occurred_at, metadata, created_at, updated_at
            """,
            (
                provenance.provenance_id,
                provenance.target_type,
                provenance.target_id,
                provenance.operation,
                provenance.actor,
                provenance.tool,
                provenance.fingerprint,
                provenance.occurred_at,
                _json_param(provenance.metadata),
                provenance.created_at,
            ),
        ).fetchone()
        return _provenance_record_from_row(row)

    def list_provenance_records(
        self,
        *,
        target_type: str | None = None,
        target_id: str | None = None,
    ) -> tuple[ProvenanceRecord, ...]:
        filters: list[str] = []
        params: list[Any] = []
        if target_type:
            filters.append("target_type = %s")
            params.append(_clean_required(target_type, "target_type"))
        if target_id:
            filters.append("target_id = %s")
            params.append(_clean_required(target_id, "target_id"))
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        rows = self.conn.execute(
            f"""
            SELECT provenance_id, target_type, target_id, operation, actor, tool,
                fingerprint, occurred_at, metadata, created_at, updated_at
            FROM {_relation(self.schema, "provenance_records")}
            {where}
            ORDER BY occurred_at, provenance_id
            """,
            tuple(params),
        ).fetchall()
        return tuple(_provenance_record_from_row(row) for row in rows)

    def get_provenance_record(self, provenance_id: str) -> ProvenanceRecord | None:
        return self._fetch_one_provenance_record(
            "provenance_id = %s",
            (_clean_required(provenance_id, "provenance_id"),),
        )

    def _normalize_raw_ref(self, raw_ref: RawArtifactRef) -> RawArtifactRef:
        resolved, raw_root = _validate_raw_file(raw_ref.path, raw_roots=self.raw_roots)
        size_bytes = raw_ref.size_bytes
        if size_bytes is None:
            size_bytes = resolved.stat().st_size
        sha256 = raw_ref.sha256 or _sha256_file(resolved)
        mime_type = raw_ref.mime_type
        if mime_type is None:
            mime_type, _ = mimetypes.guess_type(str(resolved))
        return replace(
            raw_ref,
            path=str(resolved),
            raw_root=raw_ref.raw_root or raw_root,
            sha256=sha256,
            size_bytes=size_bytes,
            mime_type=mime_type,
            immutable=True,
        )

    def _fetch_one_source(self, where: str, params: tuple[Any, ...]) -> CaptureSource | None:
        row = self.conn.execute(
            f"""
            SELECT source_id, source_name, source_type, collector, account,
                native_source_id, base_uri, status, config, metadata, created_at, updated_at
            FROM {_relation(self.schema, "capture_sources")}
            WHERE {where}
            """,
            params,
        ).fetchone()
        return _source_from_row(row) if row else None

    def _fetch_one_session(self, where: str, params: tuple[Any, ...]) -> CaptureSession | None:
        row = self.conn.execute(
            f"""
            SELECT session_id, source_id, native_session_id, session_type, status,
                started_at, ended_at, metadata, provenance, created_at, updated_at
            FROM {_relation(self.schema, "capture_sessions")}
            WHERE {where}
            """,
            params,
        ).fetchone()
        return _session_from_row(row) if row else None

    def _fetch_one_event(self, where: str, params: tuple[Any, ...]) -> CaptureEvent | None:
        row = self.conn.execute(
            f"""
            SELECT event_id, source_id, session_id, native_event_id, event_type, status,
                occurred_at, captured_at, event_hash, payload, privacy, retention,
                provenance, created_at, updated_at
            FROM {_relation(self.schema, "capture_events")}
            WHERE {where}
            """,
            params,
        ).fetchone()
        return _event_from_row(row) if row else None

    def _fetch_one_raw_ref(self, where: str, params: tuple[Any, ...]) -> RawArtifactRef | None:
        row = self.conn.execute(
            f"""
            SELECT raw_ref_id, event_id, source_id, session_id, raw_root, path, sha256,
                size_bytes, mime_type, immutable, metadata, created_at, updated_at
            FROM {_relation(self.schema, "raw_artifact_refs")}
            WHERE {where}
            """,
            params,
        ).fetchone()
        return _raw_ref_from_row(row) if row else None

    def _fetch_one_artifact_link(self, where: str, params: tuple[Any, ...]) -> ArtifactLink | None:
        row = self.conn.execute(
            f"""
            SELECT artifact_link_id, event_id, raw_ref_id, artifact_id, artifact_type,
                link_type, metadata, created_at, updated_at
            FROM {_relation(self.schema, "artifact_links")}
            WHERE {where}
            """,
            params,
        ).fetchone()
        return _artifact_link_from_row(row) if row else None

    def _fetch_one_security_finding(self, where: str, params: tuple[Any, ...]) -> SecurityFinding | None:
        row = self.conn.execute(
            f"""
            SELECT finding_id, event_id, raw_ref_id, finding_type, severity, status,
                scanner, fingerprint, detected_at, details, created_at, updated_at
            FROM {_relation(self.schema, "security_findings")}
            WHERE {where}
            """,
            params,
        ).fetchone()
        return _security_finding_from_row(row) if row else None

    def _fetch_one_privacy_annotation(self, where: str, params: tuple[Any, ...]) -> PrivacyAnnotation | None:
        row = self.conn.execute(
            f"""
            SELECT privacy_id, event_id, raw_ref_id, scope, classification, policy,
                subject_ref, metadata, created_at, updated_at
            FROM {_relation(self.schema, "privacy_annotations")}
            WHERE {where}
            """,
            params,
        ).fetchone()
        return _privacy_annotation_from_row(row) if row else None

    def _fetch_one_retention_policy(self, where: str, params: tuple[Any, ...]) -> RetentionPolicy | None:
        row = self.conn.execute(
            f"""
            SELECT retention_id, target_type, target_id, policy_name, action,
                retain_until, delete_after, legal_hold, metadata, created_at, updated_at
            FROM {_relation(self.schema, "retention_policies")}
            WHERE {where}
            """,
            params,
        ).fetchone()
        return _retention_policy_from_row(row) if row else None

    def _fetch_one_provenance_record(self, where: str, params: tuple[Any, ...]) -> ProvenanceRecord | None:
        row = self.conn.execute(
            f"""
            SELECT provenance_id, target_type, target_id, operation, actor, tool,
                fingerprint, occurred_at, metadata, created_at, updated_at
            FROM {_relation(self.schema, "provenance_records")}
            WHERE {where}
            """,
            params,
        ).fetchone()
        return _provenance_record_from_row(row) if row else None


def _source_from_row(row: Any) -> CaptureSource:
    return CaptureSource(
        source_id=_row_value(row, 0, "source_id"),
        source_name=_row_value(row, 1, "source_name"),
        source_type=_row_value(row, 2, "source_type"),
        collector=_row_value(row, 3, "collector"),
        account=_row_value(row, 4, "account"),
        native_source_id=_row_value(row, 5, "native_source_id"),
        base_uri=_row_value(row, 6, "base_uri"),
        status=_row_value(row, 7, "status"),
        config=_read_json(_row_value(row, 8, "config")),
        metadata=_read_json(_row_value(row, 9, "metadata")),
        created_at=_row_value(row, 10, "created_at"),
        updated_at=_row_value(row, 11, "updated_at"),
    )


def _session_from_row(row: Any) -> CaptureSession:
    return CaptureSession(
        session_id=_row_value(row, 0, "session_id"),
        source_id=_row_value(row, 1, "source_id"),
        native_session_id=_row_value(row, 2, "native_session_id"),
        session_type=_row_value(row, 3, "session_type"),
        status=_row_value(row, 4, "status"),
        started_at=_row_value(row, 5, "started_at"),
        ended_at=_row_value(row, 6, "ended_at"),
        metadata=_read_json(_row_value(row, 7, "metadata")),
        provenance=_read_json(_row_value(row, 8, "provenance")),
        created_at=_row_value(row, 9, "created_at"),
        updated_at=_row_value(row, 10, "updated_at"),
    )


def _event_from_row(row: Any) -> CaptureEvent:
    return CaptureEvent(
        event_id=_row_value(row, 0, "event_id"),
        source_id=_row_value(row, 1, "source_id"),
        session_id=_row_value(row, 2, "session_id"),
        native_event_id=_row_value(row, 3, "native_event_id"),
        event_type=_row_value(row, 4, "event_type"),
        status=_row_value(row, 5, "status"),
        occurred_at=_row_value(row, 6, "occurred_at"),
        captured_at=_row_value(row, 7, "captured_at"),
        event_hash=_row_value(row, 8, "event_hash"),
        payload=_read_json(_row_value(row, 9, "payload")),
        privacy=_read_json(_row_value(row, 10, "privacy")),
        retention=_read_json(_row_value(row, 11, "retention")),
        provenance=_read_json(_row_value(row, 12, "provenance")),
        created_at=_row_value(row, 13, "created_at"),
        updated_at=_row_value(row, 14, "updated_at"),
    )


def _raw_ref_from_row(row: Any) -> RawArtifactRef:
    return RawArtifactRef(
        raw_ref_id=_row_value(row, 0, "raw_ref_id"),
        event_id=_row_value(row, 1, "event_id"),
        source_id=_row_value(row, 2, "source_id"),
        session_id=_row_value(row, 3, "session_id"),
        raw_root=_row_value(row, 4, "raw_root"),
        path=_row_value(row, 5, "path"),
        sha256=_row_value(row, 6, "sha256"),
        size_bytes=_row_value(row, 7, "size_bytes"),
        mime_type=_row_value(row, 8, "mime_type"),
        immutable=_row_value(row, 9, "immutable"),
        metadata=_read_json(_row_value(row, 10, "metadata")),
        created_at=_row_value(row, 11, "created_at"),
        updated_at=_row_value(row, 12, "updated_at"),
    )


def _artifact_link_from_row(row: Any) -> ArtifactLink:
    return ArtifactLink(
        artifact_link_id=_row_value(row, 0, "artifact_link_id"),
        event_id=_row_value(row, 1, "event_id"),
        raw_ref_id=_row_value(row, 2, "raw_ref_id"),
        artifact_id=_row_value(row, 3, "artifact_id"),
        artifact_type=_row_value(row, 4, "artifact_type"),
        link_type=_row_value(row, 5, "link_type"),
        metadata=_read_json(_row_value(row, 6, "metadata")),
        created_at=_row_value(row, 7, "created_at"),
        updated_at=_row_value(row, 8, "updated_at"),
    )


def _security_finding_from_row(row: Any) -> SecurityFinding:
    return SecurityFinding(
        finding_id=_row_value(row, 0, "finding_id"),
        event_id=_row_value(row, 1, "event_id"),
        raw_ref_id=_row_value(row, 2, "raw_ref_id"),
        finding_type=_row_value(row, 3, "finding_type"),
        severity=_row_value(row, 4, "severity"),
        status=_row_value(row, 5, "status"),
        scanner=_row_value(row, 6, "scanner"),
        fingerprint=_row_value(row, 7, "fingerprint"),
        detected_at=_row_value(row, 8, "detected_at"),
        details=_read_json(_row_value(row, 9, "details")),
        created_at=_row_value(row, 10, "created_at"),
        updated_at=_row_value(row, 11, "updated_at"),
    )


def _privacy_annotation_from_row(row: Any) -> PrivacyAnnotation:
    return PrivacyAnnotation(
        privacy_id=_row_value(row, 0, "privacy_id"),
        event_id=_row_value(row, 1, "event_id"),
        raw_ref_id=_row_value(row, 2, "raw_ref_id"),
        scope=_row_value(row, 3, "scope"),
        classification=_row_value(row, 4, "classification"),
        policy=_row_value(row, 5, "policy"),
        subject_ref=_row_value(row, 6, "subject_ref"),
        metadata=_read_json(_row_value(row, 7, "metadata")),
        created_at=_row_value(row, 8, "created_at"),
        updated_at=_row_value(row, 9, "updated_at"),
    )


def _retention_policy_from_row(row: Any) -> RetentionPolicy:
    return RetentionPolicy(
        retention_id=_row_value(row, 0, "retention_id"),
        target_type=_row_value(row, 1, "target_type"),
        target_id=_row_value(row, 2, "target_id"),
        policy_name=_row_value(row, 3, "policy_name"),
        action=_row_value(row, 4, "action"),
        retain_until=_row_value(row, 5, "retain_until"),
        delete_after=_row_value(row, 6, "delete_after"),
        legal_hold=_row_value(row, 7, "legal_hold"),
        metadata=_read_json(_row_value(row, 8, "metadata")),
        created_at=_row_value(row, 9, "created_at"),
        updated_at=_row_value(row, 10, "updated_at"),
    )


def _provenance_record_from_row(row: Any) -> ProvenanceRecord:
    return ProvenanceRecord(
        provenance_id=_row_value(row, 0, "provenance_id"),
        target_type=_row_value(row, 1, "target_type"),
        target_id=_row_value(row, 2, "target_id"),
        operation=_row_value(row, 3, "operation"),
        actor=_row_value(row, 4, "actor"),
        tool=_row_value(row, 5, "tool"),
        fingerprint=_row_value(row, 6, "fingerprint"),
        occurred_at=_row_value(row, 7, "occurred_at"),
        metadata=_read_json(_row_value(row, 8, "metadata")),
        created_at=_row_value(row, 9, "created_at"),
        updated_at=_row_value(row, 10, "updated_at"),
    )
