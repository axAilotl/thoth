"""Shared CLI/API surface for capture event inspection and manual ingest."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .capture_event_store import (
    ArtifactLink,
    CaptureEvent,
    CaptureEventStore,
    CaptureSource,
    PrivacyAnnotation,
    ProvenanceRecord,
    RawArtifactRef,
    RetentionPolicy,
    SecurityFinding,
)
from .capture_lifecycle import CaptureLifecycleService
from .config import Config, config
from .metadata_db import MetadataDB
from .path_layout import PathLayout, build_path_layout
from .postgres import (
    PostgresConfigError,
    open_postgres_connection,
    resolve_postgres_settings,
)
from .retention_service import CaptureRetentionService, RetentionServiceError
from .wiki_updater import CompiledWikiUpdater


class CaptureSurfaceError(RuntimeError):
    """Raised when a capture CLI/API operation cannot complete."""


class CaptureSurfaceConfigError(CaptureSurfaceError):
    """Raised when capture surfaces are requested without a usable store."""


class CaptureSurfaceNotFoundError(CaptureSurfaceError):
    """Raised when a requested capture event-store record is missing."""


class CaptureSurfaceService:
    """Read and ingest capture events through the shared store/lifecycle layer."""

    def __init__(
        self,
        event_store: CaptureEventStore,
        *,
        lifecycle_service: CaptureLifecycleService | None = None,
        layout: PathLayout | None = None,
        db: MetadataDB | None = None,
    ) -> None:
        self.event_store = event_store
        self.lifecycle_service = lifecycle_service
        self.layout = layout
        self.db = db

    def list_sources(self) -> dict[str, Any]:
        """Return configured capture sources."""
        sources = [_source_payload(source) for source in self.event_store.list_sources()]
        return {"sources": sources, "total": len(sources)}

    def list_events(
        self,
        *,
        source_id: str | None = None,
        session_id: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Return capture events with their security and retention state."""
        events = list(
            self.event_store.list_events(source_id=source_id, session_id=session_id)
        )
        if limit is not None:
            parsed_limit = int(limit)
            if parsed_limit <= 0:
                raise ValueError("limit must be positive")
            events = events[:parsed_limit]
        payloads = [self._event_payload(event, include_payload=False) for event in events]
        return {"events": payloads, "total": len(payloads)}

    def get_event(self, event_id: str) -> dict[str, Any]:
        """Return one capture event with all attached capture metadata."""
        event = self.event_store.get_event(event_id)
        if event is None:
            raise CaptureSurfaceNotFoundError(f"capture event not found: {event_id}")
        return self._event_payload(event, include_payload=True)

    def ingest_manual(
        self,
        *,
        artifact_type: str,
        payload: Mapping[str, Any],
        source: Mapping[str, Any] | str,
        session: Mapping[str, Any] | None = None,
        event: Mapping[str, Any] | None = None,
        raw_path: str | Path | None = None,
        queue_artifact_id: str | None = None,
        priority: int = 0,
        capabilities: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        """Capture a manual artifact through CaptureLifecycleService."""
        if self.lifecycle_service is None:
            raise CaptureSurfaceConfigError(
                "manual ingest requires an initialized CaptureLifecycleService"
            )

        result = self.lifecycle_service.capture(
            artifact_type=artifact_type,
            payload=payload,
            source=source,
            session=session,
            event=event,
            raw_path=raw_path,
            queue_artifact_id=queue_artifact_id,
            priority=priority,
            capabilities=capabilities,
        )
        response = result.to_dict()
        response["event"] = self.get_event(result.event_id)
        return _json_safe(response)

    def compile_wiki_pages(
        self,
        updater: CompiledWikiUpdater,
        *,
        source_id: str | None = None,
        session_id: str | None = None,
        include_restricted_events: bool = False,
        audit_reason: str | None = None,
    ) -> dict[str, Any]:
        """Compile event-backed wiki pages through the shared wiki updater."""
        results = updater.update_from_capture_events(
            self.event_store,
            source_id=source_id,
            session_id=session_id,
            include_restricted_events=include_restricted_events,
            audit_reason=audit_reason,
        )
        pages = [
            {
                "slug": result.slug,
                "page_path": result.page_path,
                "source_paths": result.source_paths,
                "action": result.action,
            }
            for result in results
        ]
        return _json_safe({"pages": pages, "total": len(pages)})

    def inspect_retention(
        self,
        *,
        event_id: str | None = None,
        source_id: str | None = None,
        session_id: str | None = None,
        as_of: Any = None,
    ) -> dict[str, Any]:
        """Inspect retention classes and expiry eligibility for capture data."""
        try:
            return _json_safe(
                self._retention_service().inspect(
                    event_id=event_id,
                    source_id=source_id,
                    session_id=session_id,
                    as_of=as_of,
                )
            )
        except RetentionServiceError as exc:
            raise CaptureSurfaceError(str(exc)) from exc

    def expire_retention(
        self,
        *,
        event_id: str,
        delete_raw: bool = False,
        delete_distilled: bool = False,
        dry_run: bool = True,
        reason: str | None = None,
        actor: str | None = None,
        as_of: Any = None,
    ) -> dict[str, Any]:
        """Expire eligible raw or distilled capture data with audit records."""
        try:
            return _json_safe(
                self._retention_service().expire(
                    event_id=event_id,
                    delete_raw=delete_raw,
                    delete_distilled=delete_distilled,
                    dry_run=dry_run,
                    reason=reason,
                    actor=actor,
                    as_of=as_of,
                )
            )
        except RetentionServiceError as exc:
            raise CaptureSurfaceError(str(exc)) from exc

    def _retention_service(self) -> CaptureRetentionService:
        if self.layout is None:
            raise CaptureSurfaceConfigError(
                "retention operations require an initialized PathLayout"
            )
        return CaptureRetentionService(
            self.event_store,
            layout=self.layout,
            db=self.db,
        )

    def _event_payload(
        self,
        event: CaptureEvent,
        *,
        include_payload: bool,
    ) -> dict[str, Any]:
        raw_refs = self.event_store.list_raw_refs(event_id=event.event_id)
        artifact_links = self.event_store.list_artifact_links(event_id=event.event_id)
        privacy_annotations = self.event_store.list_privacy_annotations(
            event_id=event.event_id
        )
        retention_policies = self._retention_policies(event, raw_refs, artifact_links)
        provenance_records = self._provenance_records(event, raw_refs, artifact_links)
        security_findings = self._security_findings(event, raw_refs)
        source = self.event_store.get_source(event.source_id)
        session = (
            self.event_store.get_session(event.session_id)
            if event.session_id
            else None
        )
        security_state = _security_state(security_findings)

        payload: dict[str, Any] = {
            "event_id": event.event_id,
            "source_id": event.source_id,
            "session_id": event.session_id,
            "native_event_id": event.native_event_id,
            "event_type": event.event_type,
            "status": event.status,
            "occurred_at": event.occurred_at,
            "captured_at": event.captured_at,
            "event_hash": event.event_hash,
            "source": _source_payload(source) if source else None,
            "session": _session_payload(session) if session else None,
            "privacy": dict(event.privacy),
            "privacy_class": _privacy_class(event.privacy, privacy_annotations),
            "privacy_annotations": [
                _privacy_payload(item) for item in privacy_annotations
            ],
            "retention": dict(event.retention),
            "retention_class": _retention_class(event.retention, retention_policies),
            "retention_policies": [
                _retention_payload(item) for item in retention_policies
            ],
            "provenance": dict(event.provenance),
            "provenance_records": [
                _provenance_payload(item) for item in provenance_records
            ],
            "raw_ref_ids": [item.raw_ref_id for item in raw_refs],
            "raw_refs": [_raw_ref_payload(item) for item in raw_refs],
            "artifact_ids": [item.artifact_id for item in artifact_links],
            "artifacts": [_artifact_link_payload(item) for item in artifact_links],
            "security_state": security_state,
            "security_findings": [
                _security_finding_payload(item) for item in security_findings
            ],
            "created_at": event.created_at,
            "updated_at": event.updated_at,
        }
        if include_payload:
            payload["payload"] = dict(event.payload)
        return _json_safe(payload)

    def _retention_policies(
        self,
        event: CaptureEvent,
        raw_refs: tuple[RawArtifactRef, ...],
        artifact_links: tuple[ArtifactLink, ...],
    ) -> tuple[RetentionPolicy, ...]:
        policies: list[RetentionPolicy] = []
        policies.extend(
            self.event_store.list_retention_policies(
                target_type="event",
                target_id=event.event_id,
            )
        )
        for raw_ref in raw_refs:
            policies.extend(
                self.event_store.list_retention_policies(
                    target_type="raw_ref",
                    target_id=raw_ref.raw_ref_id,
                )
            )
        for link in artifact_links:
            policies.extend(
                self.event_store.list_retention_policies(
                    target_type="artifact_link",
                    target_id=link.artifact_link_id,
                )
            )
        return tuple(_dedupe_by_attr(policies, "retention_id"))

    def _provenance_records(
        self,
        event: CaptureEvent,
        raw_refs: tuple[RawArtifactRef, ...],
        artifact_links: tuple[ArtifactLink, ...],
    ) -> tuple[ProvenanceRecord, ...]:
        records: list[ProvenanceRecord] = []
        records.extend(
            self.event_store.list_provenance_records(
                target_type="event",
                target_id=event.event_id,
            )
        )
        for raw_ref in raw_refs:
            records.extend(
                self.event_store.list_provenance_records(
                    target_type="raw_ref",
                    target_id=raw_ref.raw_ref_id,
                )
            )
        for link in artifact_links:
            records.extend(
                self.event_store.list_provenance_records(
                    target_type="artifact_link",
                    target_id=link.artifact_link_id,
                )
            )
        return tuple(_dedupe_by_attr(records, "provenance_id"))

    def _security_findings(
        self,
        event: CaptureEvent,
        raw_refs: tuple[RawArtifactRef, ...],
    ) -> tuple[SecurityFinding, ...]:
        findings: list[SecurityFinding] = list(
            self.event_store.list_security_findings(event_id=event.event_id)
        )
        for raw_ref in raw_refs:
            findings.extend(
                self.event_store.list_security_findings(raw_ref_id=raw_ref.raw_ref_id)
            )
        return tuple(_dedupe_by_attr(findings, "finding_id"))


@contextmanager
def open_capture_surface(
    runtime_config: Config | None = None,
    *,
    layout: PathLayout | None = None,
    db: MetadataDB | None = None,
):
    """Open a configured capture surface backed by Postgres."""
    config_obj = runtime_config or config
    try:
        settings = resolve_postgres_settings(config_obj)
    except PostgresConfigError as exc:
        raise CaptureSurfaceConfigError(str(exc)) from exc
    if not settings.enabled:
        raise CaptureSurfaceConfigError(
            "database.capture_event_store.enabled must be true to use capture surfaces"
        )

    surface_layout = layout or build_path_layout(config_obj)
    surface_layout.ensure_directories()
    surface_db = db or MetadataDB(str(surface_layout.database_path))

    try:
        with open_postgres_connection(settings) as conn:
            event_store = CaptureEventStore(
                conn,
                schema=settings.schema,
                raw_roots=[surface_layout.raw_root],
            )
            lifecycle = CaptureLifecycleService(
                config_obj,
                layout=surface_layout,
                db=surface_db,
                capture_event_store=event_store,
            )
            yield CaptureSurfaceService(
                event_store,
                lifecycle_service=lifecycle,
                layout=surface_layout,
                db=surface_db,
            )
    except PostgresConfigError as exc:
        raise CaptureSurfaceConfigError(str(exc)) from exc


def _source_payload(source: CaptureSource) -> dict[str, Any]:
    return _json_safe(
        {
            "source_id": source.source_id,
            "source_name": source.source_name,
            "source_type": source.source_type,
            "collector": source.collector,
            "account": source.account,
            "native_source_id": source.native_source_id,
            "base_uri": source.base_uri,
            "status": source.status,
            "config": dict(source.config),
            "metadata": dict(source.metadata),
            "created_at": source.created_at,
            "updated_at": source.updated_at,
        }
    )


def _session_payload(session) -> dict[str, Any]:
    return _json_safe(
        {
            "session_id": session.session_id,
            "source_id": session.source_id,
            "native_session_id": session.native_session_id,
            "session_type": session.session_type,
            "status": session.status,
            "started_at": session.started_at,
            "ended_at": session.ended_at,
            "metadata": dict(session.metadata),
            "provenance": dict(session.provenance),
            "created_at": session.created_at,
            "updated_at": session.updated_at,
        }
    )


def _raw_ref_payload(raw_ref: RawArtifactRef) -> dict[str, Any]:
    return _json_safe(
        {
            "raw_ref_id": raw_ref.raw_ref_id,
            "event_id": raw_ref.event_id,
            "source_id": raw_ref.source_id,
            "session_id": raw_ref.session_id,
            "raw_root": raw_ref.raw_root,
            "path": raw_ref.path,
            "sha256": raw_ref.sha256,
            "size_bytes": raw_ref.size_bytes,
            "mime_type": raw_ref.mime_type,
            "immutable": raw_ref.immutable,
            "metadata": dict(raw_ref.metadata),
            "created_at": raw_ref.created_at,
            "updated_at": raw_ref.updated_at,
        }
    )


def _artifact_link_payload(link: ArtifactLink) -> dict[str, Any]:
    return _json_safe(
        {
            "artifact_link_id": link.artifact_link_id,
            "event_id": link.event_id,
            "raw_ref_id": link.raw_ref_id,
            "artifact_id": link.artifact_id,
            "artifact_type": link.artifact_type,
            "link_type": link.link_type,
            "metadata": dict(link.metadata),
            "created_at": link.created_at,
            "updated_at": link.updated_at,
        }
    )


def _security_finding_payload(finding: SecurityFinding) -> dict[str, Any]:
    return _json_safe(
        {
            "finding_id": finding.finding_id,
            "event_id": finding.event_id,
            "raw_ref_id": finding.raw_ref_id,
            "finding_type": finding.finding_type,
            "severity": finding.severity,
            "status": finding.status,
            "scanner": finding.scanner,
            "fingerprint": finding.fingerprint,
            "detected_at": finding.detected_at,
            "details": dict(finding.details),
            "created_at": finding.created_at,
            "updated_at": finding.updated_at,
        }
    )


def _privacy_payload(privacy: PrivacyAnnotation) -> dict[str, Any]:
    return _json_safe(
        {
            "privacy_id": privacy.privacy_id,
            "event_id": privacy.event_id,
            "raw_ref_id": privacy.raw_ref_id,
            "scope": privacy.scope,
            "classification": privacy.classification,
            "policy": privacy.policy,
            "subject_ref": privacy.subject_ref,
            "metadata": dict(privacy.metadata),
            "created_at": privacy.created_at,
            "updated_at": privacy.updated_at,
        }
    )


def _retention_payload(policy: RetentionPolicy) -> dict[str, Any]:
    return _json_safe(
        {
            "retention_id": policy.retention_id,
            "target_type": policy.target_type,
            "target_id": policy.target_id,
            "policy_name": policy.policy_name,
            "action": policy.action,
            "retain_until": policy.retain_until,
            "delete_after": policy.delete_after,
            "legal_hold": policy.legal_hold,
            "metadata": dict(policy.metadata),
            "created_at": policy.created_at,
            "updated_at": policy.updated_at,
        }
    )


def _provenance_payload(record: ProvenanceRecord) -> dict[str, Any]:
    return _json_safe(
        {
            "provenance_id": record.provenance_id,
            "target_type": record.target_type,
            "target_id": record.target_id,
            "operation": record.operation,
            "actor": record.actor,
            "tool": record.tool,
            "fingerprint": record.fingerprint,
            "occurred_at": record.occurred_at,
            "metadata": dict(record.metadata),
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }
    )


def _privacy_class(
    privacy: Mapping[str, Any],
    annotations: tuple[PrivacyAnnotation, ...],
) -> str | None:
    for key in ("privacy_class", "classification", "class"):
        value = _clean_string(privacy.get(key))
        if value:
            return value
    if annotations:
        return annotations[0].classification
    return None


def _retention_class(
    retention: Mapping[str, Any],
    policies: tuple[RetentionPolicy, ...],
) -> str | None:
    for key in ("retention_class", "policy_name", "policy", "class"):
        value = _clean_string(retention.get(key))
        if value:
            return value
    if policies:
        return policies[0].policy_name
    return None


def _security_state(findings: tuple[SecurityFinding, ...]) -> dict[str, Any]:
    open_statuses = {"new", "open", "active", "triage"}
    closed_statuses = {"closed", "resolved", "suppressed", "accepted"}
    open_findings = [
        finding
        for finding in findings
        if finding.status.lower() in open_statuses
        or finding.status.lower() not in closed_statuses
    ]
    return {
        "state": "open" if open_findings else ("clear" if not findings else "closed"),
        "finding_count": len(findings),
        "open_finding_count": len(open_findings),
        "max_severity": _max_severity(findings),
    }


def _max_severity(findings: tuple[SecurityFinding, ...]) -> str | None:
    order = {"info": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
    max_finding = None
    max_score = -1
    for finding in findings:
        severity = finding.severity.lower()
        score = order.get(severity, 0)
        if score > max_score:
            max_finding = finding.severity
            max_score = score
    return max_finding


def _dedupe_by_attr(items: Iterable[Any], attr_name: str) -> list[Any]:
    seen: set[Any] = set()
    deduped: list[Any] = []
    for item in items:
        key = getattr(item, attr_name)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value, key=str)]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
