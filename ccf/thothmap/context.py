"""Shared context and helpers for the Thoth-to-CCF mapping layer.

Every converter in this package consumes a plain ``dict`` snapshot of an
existing Thoth record (a ``dataclasses.asdict`` result, a DB row, or a
fixture with the same field names as the Thoth source dataclass) and emits
CCF producer submissions built through :class:`ccf.producer.Producer`.
Converters only set producer-controlled fields (spec section 5.2); the
archive resolves registry digests, privacy classification, and policy
references at admission.

Thoth-native identifiers (``capture_sources.source_id``,
``connector_runs.run_id``, ``raw_artifact_refs.raw_ref_id``,
``security_findings.finding_id``, ``canonical_entities.canonical_id``,
``semantic_memory_candidates.candidate_id``, transcript artifact IDs, ...)
are carried as source-native IDs inside origin tuples (spec section 2.1);
portable CCF IDs are always freshly generated URNs and never derived from
or rewritten onto Thoth IDs.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from ccf.ids import parse_id


class ThothMapError(ValueError):
    """Raised when a Thoth snapshot cannot be mapped safely (fail closed)."""


@dataclass(frozen=True)
class MapContext:
    """Producer-side identity context shared by all converters.

    ``person_id`` is the archive principal (the Thoth operator's
    ``core.person`` Record); ``perspective_id`` defaults to it.
    ``policy_hint`` is the policy lineage URN every bootstrap uses.
    """

    person_id: str
    policy_hint: str | None = None
    perspective_id: str | None = None

    def __post_init__(self) -> None:
        require_urn(self.person_id, "record", field="person_id")
        if self.perspective_id is not None:
            require_urn(self.perspective_id, "record", field="perspective_id")
        if self.policy_hint is not None:
            require_urn(self.policy_hint, "lineage", field="policy_hint")

    @property
    def perspective(self) -> str:
        return self.perspective_id or self.person_id


@dataclass
class MappedSubmissions:
    """Accumulator for the submissions one converter call produces.

    ``blob_data`` carries the bytes for each Blob submission (keyed by Blob
    ID) so the caller can hand them to ``Archive.admit_batch`` as
    ``blob_bytes`` together with the signed batch.
    """

    records: list[dict] = field(default_factory=list)
    links: list[dict] = field(default_factory=list)
    blobs: list[dict] = field(default_factory=list)
    blob_data: dict[str, bytes] = field(default_factory=dict)

    def extend(self, other: "MappedSubmissions") -> "MappedSubmissions":
        self.records.extend(other.records)
        self.links.extend(other.links)
        self.blobs.extend(other.blobs)
        overlap = set(self.blob_data) & set(other.blob_data)
        if overlap:
            raise ThothMapError(f"duplicate blob IDs across converters: {sorted(overlap)}")
        self.blob_data.update(other.blob_data)
        return self

    @property
    def primary_id(self) -> str:
        """ID of the first produced object (the converter's main subject)."""
        for group in (self.records, self.links, self.blobs):
            if group:
                return group[0]["id"]
        raise ThothMapError("no submissions produced")


def combine(*parts: MappedSubmissions) -> MappedSubmissions:
    combined = MappedSubmissions()
    for part in parts:
        combined.extend(part)
    return combined


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def require_urn(value: str, kind: str, *, field: str) -> str:
    if not isinstance(value, str):
        raise ThothMapError(f"{field} must be a URN string, got {type(value).__name__}")
    try:
        parsed = parse_id(value)
    except Exception as exc:
        raise ThothMapError(f"{field} is not a CCF URN: {value!r}") from exc
    if parsed.kind != kind:
        raise ThothMapError(f"{field} must be a {kind} URN, got {value!r}")
    return value


def require_str(snapshot: dict, key: str, *, what: str) -> str:
    value = snapshot.get(key)
    if not isinstance(value, str) or not value:
        raise ThothMapError(f"{what} snapshot requires non-empty string field {key!r}")
    return value


def optional_str(snapshot: dict, key: str) -> str | None:
    value = snapshot.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ThothMapError(f"snapshot field {key!r} must be a string or null")
    return value


# ---------------------------------------------------------------------------
# Timestamps (canonical ccf-timestamp: strict millisecond Z form)
# ---------------------------------------------------------------------------

_CANONICAL_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def ccf_timestamp(value, *, field: str = "timestamp") -> str:
    """Normalize a Thoth timestamp to the canonical CCF form.

    Accepts canonical strings, any ISO-8601 string (naive values are UTC —
    Thoth legacy code writes naive ``datetime.now().isoformat()`` stamps in
    local-naive form but Thoth's canonical writer ``core.time_utils`` is
    UTC), and ``datetime`` objects. Fails closed on anything else.
    """
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        if _CANONICAL_TS.match(value):
            return value
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            moment = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ThothMapError(f"{field} is not ISO-8601: {value!r}") from exc
    else:
        raise ThothMapError(f"{field} must be str or datetime, got {type(value).__name__}")
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    moment = moment.astimezone(timezone.utc)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def occurred_at(
    start, end=None, *, precision: str = "second", clock_uncertainty_ms: int | None = None
) -> dict:
    block = {"start": ccf_timestamp(start, field="occurred_at.start"), "precision": precision}
    if end is not None:
        block["end"] = ccf_timestamp(end, field="occurred_at.end")
    if clock_uncertainty_ms is not None:
        block["clock_uncertainty_ms"] = clock_uncertainty_ms
    return block


# ---------------------------------------------------------------------------
# Origin tuples (spec section 2.1): Thoth-native IDs, never rewritten
# ---------------------------------------------------------------------------


def origin(source_ccf_id: str, native_id: str, revision: str | int | None = "1") -> dict:
    require_urn(source_ccf_id, "record", field="origin.source_id")
    if not isinstance(native_id, str) or not native_id:
        raise ThothMapError("origin native_id must be a non-empty string")
    if len(native_id) > 2048:
        raise ThothMapError("origin native_id exceeds 2048 characters")
    rev = "1" if revision is None else str(revision)
    if not rev:
        raise ThothMapError("origin revision must be a non-empty string")
    return {"source_id": source_ccf_id, "native_id": native_id, "revision": rev}


# ---------------------------------------------------------------------------
# Claims (producer-controlled claim block, spec section 5.2)
# ---------------------------------------------------------------------------


def data_subject(person_id: str, role: str, *, identity_state: str = "unknown") -> dict:
    require_urn(person_id, "record", field="data_subject.person_id")
    return {
        "person_id": person_id,
        "role": role,
        "identity_state_at_write": identity_state,
    }


def claims(
    ctx: MapContext,
    *,
    basis: str = "runtime_import",
    asserted_by: str | None = None,
    accepted_by: str | None = None,
    data_classes: list[str] | None = None,
    subjects: list[dict] | None = None,
    subject_coverage: str | None = None,
) -> dict:
    """Producer claim block; ``asserted_by`` defaults to the principal."""
    subjects = list(subjects or [])
    if subject_coverage is None:
        subject_coverage = "complete" if subjects else "unknown"
    return {
        "person_id": ctx.person_id,
        "perspective_id": ctx.perspective,
        "privacy": {
            "data_subjects": subjects,
            "data_classes": list(data_classes or []),
            "consent_refs": [],
            "legal_basis_refs": [],
            "subject_coverage": subject_coverage,
        },
        "authority": {
            "basis": basis,
            "asserted_by": asserted_by or ctx.person_id,
            "accepted_by": accepted_by,
        },
        "policy_hint": ctx.policy_hint,
        "extensions": {},
    }


def inherit_subjects(
    explicit: list[dict] | None, *parents: list[dict] | None
) -> tuple[list[dict], str]:
    """Conservative subject propagation (spec section 3.9).

    Derived content inherits source subjects unless the caller explicitly
    narrows the set via a reviewed transformation. Returns
    ``(subjects, subject_coverage)``: explicit subjects win; otherwise the
    union of parent subjects (first non-``None`` list wins, since Thoth
    tracks subjects per source/session); otherwise unknown.
    """
    if explicit is not None:
        return list(explicit), "complete" if explicit else "unknown"
    for parent in parents:
        if parent is not None:
            return list(parent), "complete" if parent else "unknown"
    return [], "unknown"


# ---------------------------------------------------------------------------
# Data classes and media helpers
# ---------------------------------------------------------------------------


def data_classes_for_media_type(media_type: str) -> list[str]:
    """Registered CCF data classes for a media MIME type (empty if benign)."""
    top = media_type.split("/", 1)[0].strip().lower() if media_type else ""
    if top == "audio":
        return ["voice_recording"]
    if top == "image":
        return ["image"]
    if top == "video":
        return ["video"]
    if media_type in ("text/markdown", "text/plain", "text/html", "application/pdf"):
        return ["document_content"]
    return []


def normalize_predicate(raw: str, *, candidate_type: str = "memory") -> str:
    """Map a free-form Thoth predicate to a valid CCF predicate name.

    Thoth ``semantic_memory_candidates.predicate`` is arbitrary text; CCF
    requires ``^[a-z][a-z0-9]*(\\\\.[a-z][a-z0-9_]*){2,}$``. The mapped name
    lives under the ``thoth.<candidate_type>.`` namespace; the original
    predicate text is preserved in the assertion's qualifiers/extensions.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ThothMapError("predicate must be a non-empty string")
    slug = re.sub(r"[^a-z0-9_]+", "_", raw.strip().lower()).strip("_")
    slug = re.sub(r"_+", "_", slug)
    if not slug or not slug[0].isalpha():
        slug = f"p_{slug}" if slug else "unspecified"
    kind = re.sub(r"[^a-z0-9]+", "", candidate_type.lower()) or "memory"
    return f"thoth.{kind}.{slug}"


def literal(value, *, datatype: str = "string", language: str | None = None) -> dict:
    block = {"value": value, "datatype": datatype}
    if language is not None:
        block["language"] = language
    return block
