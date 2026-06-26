"""Event-backed capture rollup compiler for the Thoth wiki."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import os
from pathlib import Path
from typing import Any, Mapping

from .capture_event_store import (
    ArtifactLink,
    CaptureEvent,
    CaptureEventStore,
    CaptureSession,
    CaptureSource,
    PrivacyAnnotation,
    RawArtifactRef,
    SecurityFinding,
)
from .path_layout import PathLayout
from .prompt_security import prompt_security_requires_review
from .wiki_contract import WikiContract, WikiPageSpec, normalize_wiki_slug
from .wiki_io import atomic_write_text, read_frontmatter, render_frontmatter


_RESTRICTED_PRIVACY_CLASSES = {
    "confidential",
    "personal",
    "phi",
    "pii",
    "private",
    "restricted",
    "secret",
    "sensitive",
}
_QUARANTINED_EVENT_STATUSES = {
    "blocked",
    "needs_review",
    "quarantine",
    "quarantined",
    "redacted",
    "security_review",
}
_CLOSED_SECURITY_STATUSES = {"accepted", "closed", "resolved", "suppressed"}
_REVIEW_SECURITY_SEVERITIES = {"critical", "high"}
_PERSON_METADATA_KEYS = ("people", "persons", "participants")
_PERSON_SINGLE_METADATA_KEYS = ("person", "speaker")
_PROJECT_METADATA_KEYS = ("projects", "repositories")
_PROJECT_SINGLE_METADATA_KEYS = ("project", "repository", "repo_name")
_RAW_CONTENT_KEYS = {
    "body",
    "content",
    "html",
    "processed_transcript",
    "raw_content",
    "raw_text",
    "raw_transcript",
    "segments",
    "text",
    "transcript",
    "transcript_segments",
}


@dataclass(frozen=True)
class CaptureWikiPageResult:
    """Summary of one event-backed wiki page write."""

    slug: str
    page_path: Path
    source_paths: tuple[str, ...]
    action: str


@dataclass(frozen=True)
class _CaptureEventRecord:
    event: CaptureEvent
    source: CaptureSource | None
    session: CaptureSession | None
    raw_refs: tuple[RawArtifactRef, ...]
    artifact_links: tuple[ArtifactLink, ...]
    privacy_annotations: tuple[PrivacyAnnotation, ...]
    security_findings: tuple[SecurityFinding, ...]


@dataclass(frozen=True)
class _CaptureEntityRef:
    label: str
    slug: str


@dataclass(frozen=True)
class _CapturePageGroup:
    page_type: str
    key: str
    title: str
    slug: str
    kind: str
    records: tuple[_CaptureEventRecord, ...]
    resource: str | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_sequence(value: Any) -> tuple[Any, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Mapping):
        return (value,)
    if isinstance(value, (list, tuple, set)):
        return tuple(value)
    return (value,)


def _parse_capture_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    elif value is None:
        return None
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _capture_timestamp(record: _CaptureEventRecord) -> datetime | None:
    event = record.event
    for value in (
        event.occurred_at,
        event.captured_at,
        event.created_at,
        event.updated_at,
    ):
        parsed = _parse_capture_datetime(value)
        if parsed is not None:
            return parsed
    return None


def _capture_timestamp_text(record: _CaptureEventRecord) -> str:
    timestamp = _capture_timestamp(record)
    if timestamp is not None:
        return timestamp.isoformat().replace("+00:00", "Z")
    return "unknown"


def _capture_date_key(record: _CaptureEventRecord) -> str:
    timestamp = _capture_timestamp(record)
    return timestamp.date().isoformat() if timestamp is not None else "unknown-date"


def _capture_week_key(record: _CaptureEventRecord) -> str:
    timestamp = _capture_timestamp(record)
    if timestamp is None:
        return "unknown-week"
    year, week, _weekday = timestamp.date().isocalendar()
    return f"{year}-w{week:02d}"


def _capture_event_sort_key(record: _CaptureEventRecord) -> tuple[str, str]:
    return (_capture_timestamp_text(record), record.event.event_id)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalized_event_metadata(record: _CaptureEventRecord) -> Mapping[str, Any]:
    metadata = record.event.payload.get("normalized_metadata")
    return metadata if isinstance(metadata, Mapping) else {}


def _capture_record_security_requires_review(record: _CaptureEventRecord) -> bool:
    for metadata in (_normalized_event_metadata(record), record.event.payload):
        if prompt_security_requires_review(metadata):
            return True

    for finding in record.security_findings:
        status = finding.status.lower()
        if status in _CLOSED_SECURITY_STATUSES:
            continue
        severity = finding.severity.lower()
        finding_type = finding.finding_type.lower()
        if severity in _REVIEW_SECURITY_SEVERITIES:
            return True
        if finding_type in {"prompt_injection", "prompt_security"}:
            return True
    return False


def _privacy_classes_for_capture_record(record: _CaptureEventRecord) -> tuple[str, ...]:
    classes: list[str] = []
    for key in ("privacy_class", "classification", "class"):
        value = _clean_text(record.event.privacy.get(key))
        if value:
            classes.append(value.lower())
    for annotation in record.privacy_annotations:
        value = _clean_text(annotation.classification)
        if value:
            classes.append(value.lower())
    return tuple(dict.fromkeys(classes))


def _capture_record_filter_reasons(record: _CaptureEventRecord) -> tuple[str, ...]:
    reasons: list[str] = []
    status = record.event.status.lower()
    if status in _QUARANTINED_EVENT_STATUSES:
        reasons.append(f"status:{status}")

    restricted_classes = sorted(
        set(_privacy_classes_for_capture_record(record)) & _RESTRICTED_PRIVACY_CLASSES
    )
    if restricted_classes:
        reasons.append("privacy:" + ",".join(restricted_classes))

    if _capture_record_security_requires_review(record):
        reasons.append("security_review")
    return tuple(reasons)


def _security_findings_for_capture_records(
    records: tuple[_CaptureEventRecord, ...],
) -> tuple[Mapping[str, Any], ...]:
    entries: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for record in records:
        for finding in record.security_findings:
            key = finding.finding_id
            if key in seen:
                continue
            seen.add(key)
            entry: dict[str, Any] = {
                "finding_id": finding.finding_id,
                "event_id": finding.event_id,
                "raw_ref_id": finding.raw_ref_id,
                "finding_type": finding.finding_type,
                "severity": finding.severity,
                "status": finding.status,
                "scanner": finding.scanner,
                "fingerprint": finding.fingerprint,
            }
            for detail_key in (
                "category",
                "finding_type",
                "pattern_id",
                "rule_id",
                "scope",
                "source",
                "source_label",
            ):
                if detail_key in finding.details:
                    entry[detail_key] = finding.details[detail_key]
            entries.append(
                {key: value for key, value in entry.items() if value is not None}
            )
    return tuple(entries)


def _entity_value_refs(value: Any) -> tuple[_CaptureEntityRef, ...]:
    refs: list[_CaptureEntityRef] = []
    for item in _as_sequence(value):
        label = None
        slug_seed = None
        if isinstance(item, Mapping):
            for key in ("name", "display_name", "title", "label", "id", "slug"):
                label = _clean_text(item.get(key))
                if label:
                    break
            for key in ("slug", "id", "name", "display_name", "title", "label"):
                slug_seed = _clean_text(item.get(key))
                if slug_seed:
                    break
        else:
            label = _clean_text(item)
            slug_seed = label
        if not label or not slug_seed:
            continue
        try:
            slug = normalize_wiki_slug(slug_seed)
        except ValueError:
            continue
        refs.append(_CaptureEntityRef(label=label, slug=slug))

    deduped: dict[str, _CaptureEntityRef] = {}
    for ref in refs:
        deduped.setdefault(ref.slug, ref)
    return tuple(deduped[key] for key in sorted(deduped))


def _entity_refs_from_metadata(
    metadata: Mapping[str, Any],
    *,
    collection_keys: tuple[str, ...],
    single_keys: tuple[str, ...],
) -> tuple[_CaptureEntityRef, ...]:
    refs: list[_CaptureEntityRef] = []
    for key in collection_keys:
        if key in metadata:
            refs.extend(_entity_value_refs(metadata[key]))
    for key in single_keys:
        if key in metadata:
            refs.extend(_entity_value_refs(metadata[key]))

    deduped: dict[str, _CaptureEntityRef] = {}
    for ref in refs:
        deduped.setdefault(ref.slug, ref)
    return tuple(deduped[key] for key in sorted(deduped))


def _capture_event_label(record: _CaptureEventRecord) -> str:
    metadata = _normalized_event_metadata(record)
    for mapping in (metadata, record.event.payload):
        for key in (
            "title",
            "name",
            "artifact_id",
            "queue_artifact_id",
            "id",
        ):
            if key in _RAW_CONTENT_KEYS:
                continue
            value = _clean_text(mapping.get(key))
            if value:
                return value
    return (
        _clean_text(record.event.native_event_id)
        or _clean_text(record.event.event_hash)
        or record.event.event_id
    )


def _slug_component(value: str, fallback: str) -> str:
    try:
        return normalize_wiki_slug(value)
    except ValueError:
        try:
            return normalize_wiki_slug(fallback)
        except ValueError:
            return "unknown"


class CaptureWikiCompiler:
    """Render event-backed capture rollup pages into a wiki contract."""

    def __init__(
        self,
        *,
        layout: PathLayout,
        contract: WikiContract,
    ) -> None:
        self.layout = layout
        self.contract = contract

    def compile(
        self,
        event_store: CaptureEventStore,
        *,
        source_id: str | None = None,
        session_id: str | None = None,
        include_restricted_events: bool = False,
        audit_reason: str | None = None,
    ) -> tuple[CaptureWikiPageResult, ...]:
        """Compile deterministic capture rollup pages from event-store records."""
        if include_restricted_events and not _clean_text(audit_reason):
            raise ValueError(
                "audit_reason is required when include_restricted_events is true"
            )

        records = self._capture_records_from_store(
            event_store,
            source_id=source_id,
            session_id=session_id,
        )
        if not include_restricted_events:
            records = tuple(
                record
                for record in records
                if not _capture_record_filter_reasons(record)
            )
        records = tuple(sorted(records, key=_capture_event_sort_key))
        return tuple(
            self._update_capture_page(
                group,
                include_restricted_events=include_restricted_events,
                audit_reason=audit_reason,
            )
            for group in self._capture_page_groups(records)
        )

    def _capture_records_from_store(
        self,
        event_store: CaptureEventStore,
        *,
        source_id: str | None,
        session_id: str | None,
    ) -> tuple[_CaptureEventRecord, ...]:
        source_cache: dict[str, CaptureSource | None] = {}
        session_cache: dict[str, CaptureSession | None] = {}
        records: list[_CaptureEventRecord] = []
        for event in event_store.list_events(source_id=source_id, session_id=session_id):
            if event.source_id not in source_cache:
                source_cache[event.source_id] = event_store.get_source(event.source_id)
            session = None
            if event.session_id:
                if event.session_id not in session_cache:
                    session_cache[event.session_id] = event_store.get_session(
                        event.session_id
                    )
                session = session_cache[event.session_id]

            raw_refs = event_store.list_raw_refs(event_id=event.event_id)
            security_findings: list[SecurityFinding] = list(
                event_store.list_security_findings(event_id=event.event_id)
            )
            for raw_ref in raw_refs:
                security_findings.extend(
                    event_store.list_security_findings(raw_ref_id=raw_ref.raw_ref_id)
                )

            deduped_findings: dict[str, SecurityFinding] = {}
            for finding in security_findings:
                deduped_findings.setdefault(finding.finding_id, finding)

            records.append(
                _CaptureEventRecord(
                    event=event,
                    source=source_cache[event.source_id],
                    session=session,
                    raw_refs=raw_refs,
                    artifact_links=event_store.list_artifact_links(
                        event_id=event.event_id
                    ),
                    privacy_annotations=event_store.list_privacy_annotations(
                        event_id=event.event_id
                    ),
                    security_findings=tuple(
                        deduped_findings[key] for key in sorted(deduped_findings)
                    ),
                )
            )
        return tuple(records)

    def _capture_page_groups(
        self,
        records: tuple[_CaptureEventRecord, ...],
    ) -> tuple[_CapturePageGroup, ...]:
        groups: dict[tuple[str, str], dict[str, Any]] = {}

        def add_group(
            *,
            page_type: str,
            key: str,
            title: str,
            slug: str,
            kind: str,
            record: _CaptureEventRecord,
            resource: str | None = None,
        ) -> None:
            bucket = groups.setdefault(
                (page_type, key),
                {
                    "page_type": page_type,
                    "key": key,
                    "title": title,
                    "slug": slug,
                    "kind": kind,
                    "resource": resource,
                    "records": [],
                },
            )
            bucket["records"].append(record)

        for record in records:
            day_key = _capture_date_key(record)
            day_title = (
                f"Capture Day: {day_key}"
                if day_key != "unknown-date"
                else "Capture Day: Unknown Date"
            )
            add_group(
                page_type="daily",
                key=day_key,
                title=day_title,
                slug=f"capture-daily-{normalize_wiki_slug(day_key)}",
                kind="topic",
                record=record,
            )

            week_key = _capture_week_key(record)
            week_title = (
                f"Capture Week: {week_key.replace('-w', ' W')}"
                if week_key != "unknown-week"
                else "Capture Week: Unknown Week"
            )
            add_group(
                page_type="weekly",
                key=week_key,
                title=week_title,
                slug=f"capture-weekly-{normalize_wiki_slug(week_key)}",
                kind="topic",
                record=record,
            )

            source_label = (
                record.source.source_name if record.source else record.event.source_id
            )
            add_group(
                page_type="source",
                key=record.event.source_id,
                title=f"Capture Source: {source_label}",
                slug=(
                    f"capture-source-"
                    f"{_slug_component(source_label, record.event.source_id)}"
                ),
                kind="entity",
                record=record,
                resource=record.source.base_uri if record.source else None,
            )

            if record.event.session_id:
                session_label = (
                    record.session.native_session_id
                    if record.session and record.session.native_session_id
                    else record.event.session_id
                )
                add_group(
                    page_type="session",
                    key=record.event.session_id,
                    title=f"Capture Session: {session_label}",
                    slug=(
                        "capture-session-"
                        + _slug_component(record.event.session_id, record.event.event_id)
                    ),
                    kind="topic",
                    record=record,
                )

            metadata = _normalized_event_metadata(record)
            for person in _entity_refs_from_metadata(
                metadata,
                collection_keys=_PERSON_METADATA_KEYS,
                single_keys=_PERSON_SINGLE_METADATA_KEYS,
            ):
                add_group(
                    page_type="person",
                    key=person.slug,
                    title=f"Person: {person.label}",
                    slug=f"person-{person.slug}",
                    kind="entity",
                    record=record,
                )
            for project in _entity_refs_from_metadata(
                metadata,
                collection_keys=_PROJECT_METADATA_KEYS,
                single_keys=_PROJECT_SINGLE_METADATA_KEYS,
            ):
                add_group(
                    page_type="project",
                    key=project.slug,
                    title=f"Project: {project.label}",
                    slug=f"project-{project.slug}",
                    kind="entity",
                    record=record,
                )

        order = {
            "daily": 0,
            "weekly": 1,
            "source": 2,
            "session": 3,
            "person": 4,
            "project": 5,
        }
        compiled_groups: list[_CapturePageGroup] = []
        for (_page_type, _key), bucket in sorted(
            groups.items(),
            key=lambda item: (order.get(item[0][0], 99), item[0][1]),
        ):
            event_records: dict[str, _CaptureEventRecord] = {}
            for record in bucket["records"]:
                event_records.setdefault(record.event.event_id, record)
            compiled_groups.append(
                _CapturePageGroup(
                    page_type=bucket["page_type"],
                    key=bucket["key"],
                    title=bucket["title"],
                    slug=bucket["slug"],
                    kind=bucket["kind"],
                    records=tuple(
                        sorted(event_records.values(), key=_capture_event_sort_key)
                    ),
                    resource=bucket["resource"],
                )
            )
        return tuple(compiled_groups)

    def _update_capture_page(
        self,
        group: _CapturePageGroup,
        *,
        include_restricted_events: bool,
        audit_reason: str | None,
    ) -> CaptureWikiPageResult:
        source_paths = self._source_paths_for_capture_records(group.records)
        event_ids = tuple(record.event.event_id for record in group.records)
        source_ids = tuple(record.event.source_id for record in group.records)
        session_ids = tuple(
            record.event.session_id
            for record in group.records
            if record.event.session_id
        )
        summary = (
            f"Compiled {len(group.records)} capture event(s) for "
            f"{group.page_type} `{group.key}`."
        )
        audit_metadata = None
        if include_restricted_events:
            audit_metadata = {
                "include_restricted_events": True,
                "reason": _clean_text(audit_reason),
                "compiled_at": _now_iso(),
            }
        spec = WikiPageSpec(
            title=group.title,
            slug=group.slug,
            kind=group.kind,
            summary=summary,
            source_paths=source_paths,
            created_at=_now_iso(),
            updated_at=_now_iso(),
            resource=group.resource,
            event_ids=event_ids,
            source_ids=source_ids,
            session_ids=session_ids,
            capture_page_type=group.page_type,
            capture_page_key=group.key,
            capture_event_count=len(group.records),
            capture_audit=audit_metadata,
            security_findings=_security_findings_for_capture_records(group.records),
        )
        page_path = self.contract.page_path_for(spec)
        existing = read_frontmatter(page_path) if page_path.exists() else {}
        created_at = str(existing.get("created_at") or spec.created_at or _now_iso())
        updated_spec = WikiPageSpec(
            title=spec.title,
            slug=spec.slug,
            kind=spec.kind,
            summary=spec.summary,
            aliases=spec.aliases,
            source_paths=spec.source_paths,
            related_slugs=spec.related_slugs,
            language=spec.language,
            translated_from=spec.translated_from,
            created_at=created_at,
            updated_at=_now_iso(),
            resource=spec.resource,
            event_ids=spec.event_ids,
            source_ids=spec.source_ids,
            session_ids=spec.session_ids,
            capture_page_type=spec.capture_page_type,
            capture_page_key=spec.capture_page_key,
            capture_event_count=spec.capture_event_count,
            capture_audit=spec.capture_audit,
            security_findings=spec.security_findings,
        )
        content = self._render_capture_page(updated_spec, group)
        action = "updated" if page_path.exists() else "created"
        atomic_write_text(page_path, content)
        return CaptureWikiPageResult(
            slug=updated_spec.slug,
            page_path=page_path,
            source_paths=updated_spec.source_paths,
            action=action,
        )

    def _source_paths_for_capture_records(
        self,
        records: tuple[_CaptureEventRecord, ...],
    ) -> tuple[str, ...]:
        vault_root = self.layout.vault_root.resolve(strict=False)
        paths: list[str] = []
        for record in records:
            for raw_ref in record.raw_refs:
                raw_path = Path(raw_ref.path)
                candidate = raw_path if raw_path.is_absolute() else vault_root / raw_path
                try:
                    source_path = candidate.resolve(strict=False).relative_to(vault_root)
                except ValueError:
                    continue
                paths.append(source_path.as_posix())
        return tuple(sorted(set(paths)))

    def _render_capture_page(
        self,
        spec: WikiPageSpec,
        group: _CapturePageGroup,
    ) -> str:
        frontmatter = self.contract.frontmatter_for(spec)
        first_seen, last_seen = self._capture_group_time_range(group.records)
        lines = [
            render_frontmatter(frontmatter).rstrip(),
            "",
            f"# {spec.title}",
            "",
            spec.summary,
            "",
            "## Capture Metadata",
            "",
            f"- Page Type: `{group.page_type}`",
            f"- Page Key: `{group.key}`",
            f"- Event Count: `{len(group.records)}`",
            f"- First Event: `{first_seen}`",
            f"- Last Event: `{last_seen}`",
            f"- Generated At: `{spec.updated_at}`",
        ]
        if spec.capture_audit:
            reason = _clean_text(spec.capture_audit.get("reason")) or "unspecified"
            lines.append("- Restricted Events Included: `true`")
            lines.append(f"- Audit Reason: `{reason}`")
        lines.append("")

        lines.extend(["## Events", ""])
        for record in group.records:
            lines.append(self._capture_event_line(record))
        lines.append("")

        lines.extend(["## Sources", ""])
        if spec.resource:
            lines.append(f"- [Canonical resource]({spec.resource})")
        if spec.source_paths:
            for source_path in spec.source_paths:
                lines.append(f"- [{source_path}]({self._source_link(source_path)})")
        if not spec.resource and not spec.source_paths:
            lines.append("- No raw artifact references recorded for this group.")
        lines.append("")

        citation_lines = self._capture_citation_lines(spec)
        if citation_lines:
            lines.extend(["# Citations", ""])
            lines.extend(citation_lines)
            lines.append("")

        return "\n".join(lines) + "\n"

    def _capture_group_time_range(
        self,
        records: tuple[_CaptureEventRecord, ...],
    ) -> tuple[str, str]:
        timestamps = [
            timestamp
            for timestamp in (_capture_timestamp(record) for record in records)
            if timestamp is not None
        ]
        if not timestamps:
            return ("unknown", "unknown")
        return (
            min(timestamps).isoformat().replace("+00:00", "Z"),
            max(timestamps).isoformat().replace("+00:00", "Z"),
        )

    def _capture_event_line(self, record: _CaptureEventRecord) -> str:
        event = record.event
        label = _capture_event_label(record)
        source_label = record.source.source_name if record.source else event.source_id
        anchor = self._capture_event_anchor(event.event_id)
        line = (
            f'- <a id="{anchor}"></a>`{event.event_id}` - `{event.event_type}`'
            f"; timestamp `{_capture_timestamp_text(record)}`"
            f"; source `{source_label}`"
        )
        if label and label != event.event_id:
            line += f"; label `{label}`"
        if record.session:
            session_label = record.session.native_session_id or record.session.session_id
            line += f"; session `{session_label}`"
        elif event.session_id:
            line += f"; session `{event.session_id}`"
        artifact_ids = tuple(
            sorted(
                {link.artifact_id for link in record.artifact_links if link.artifact_id}
            )
        )
        if artifact_ids:
            line += "; artifacts " + ", ".join(
                f"`{artifact_id}`" for artifact_id in artifact_ids
            )
        raw_ref_ids = tuple(
            sorted(
                {raw_ref.raw_ref_id for raw_ref in record.raw_refs if raw_ref.raw_ref_id}
            )
        )
        if raw_ref_ids:
            line += "; raw refs " + ", ".join(
                f"`{raw_ref_id}`" for raw_ref_id in raw_ref_ids
            )
        privacy_classes = _privacy_classes_for_capture_record(record)
        if privacy_classes:
            line += "; privacy " + ", ".join(f"`{item}`" for item in privacy_classes)
        security_label = self._capture_event_security_label(record)
        if security_label:
            line += f"; security {security_label}"
        return line

    def _capture_event_security_label(
        self,
        record: _CaptureEventRecord,
    ) -> str | None:
        if not record.security_findings:
            return None
        severity_order = {"info": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
        max_finding = max(
            record.security_findings,
            key=lambda finding: severity_order.get(finding.severity.lower(), 0),
        )
        return (
            f"`{len(record.security_findings)}` finding(s), "
            f"max severity `{max_finding.severity}`"
        )

    def _capture_citation_lines(self, spec: WikiPageSpec) -> list[str]:
        citations: list[str] = []
        if spec.resource:
            citations.append(
                f"[{len(citations) + 1}] [Canonical resource]({spec.resource})"
            )
        for source_path in spec.source_paths:
            citations.append(
                f"[{len(citations) + 1}] "
                f"[{source_path}]({self._source_link(source_path)})"
            )
        for event_id in spec.event_ids:
            citations.append(
                f"[{len(citations) + 1}] [Capture event {event_id}]"
                f"(#{self._capture_event_anchor(event_id)})"
            )
        return citations

    def _capture_event_anchor(self, event_id: str) -> str:
        return f"event-{_slug_component(event_id, 'capture-event')}"

    def _source_link(self, source_path: str) -> str:
        absolute_source = self.layout.vault_root / source_path
        return os.path.relpath(absolute_source, self.contract.pages_dir)
