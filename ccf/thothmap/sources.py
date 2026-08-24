"""Capture source -> ``core.source`` (checklist section 4, row 1).

Maps a Thoth ``CaptureSource`` (``core.capture_event_store.CaptureSource`` /
``capture_sources`` table) plus its ``ConnectorManifest``
(``core.connector_registry``) to one ``core.source`` Record. The source is
the root of every origin tuple its captures produce, so it carries no
origin of its own (same shape as the spec's thoth-capture example).

Snapshot keys (CaptureSource fields): ``source_id``, ``source_name``,
``source_type``, ``collector``, ``account``, ``native_source_id``,
``base_uri``, ``status``. Optional manifest keys: ``display_name``,
``name``.
"""

from __future__ import annotations

import uuid

from ccf.ids import derive_id
from ccf.producer import Producer

from ccf.thothmap.context import (
    MapContext,
    MappedSubmissions,
    ThothMapError,
    ccf_timestamp,
    claims,
    optional_str,
    require_str,
)

_SOURCE_TRUST_CLASSES = {"trusted", "authenticated", "untrusted", "hostile", "unknown"}

#: UUIDv5 namespace salt for importer origin-root source IDs. Distinct
#: from the dual-write namespace: a dual-write mirror and an import pass
#: never share an archive, and their IDs must never be interchangeable.
IMPORT_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "thoth.ccf.import")


def stable_source_object_id(archive_id: str, native_source_id: str) -> str:
    """Deterministic ``core.source`` URN for one importer origin root.

    Sources carry no origin tuple, so the origin index cannot dedupe
    them; the importer derives the URN from the archive ID plus the
    source's stable native identity (e.g. vault path + segment) instead.
    Two importer instances pointed at the same source derive the same
    URN; distinct sources and distinct archives cannot collide.
    """
    return derive_id(
        IMPORT_ID_NAMESPACE, "record", [archive_id, "core.source", native_source_id]
    )


def source_submission(
    producer: Producer,
    ctx: MapContext,
    snapshot: dict,
    *,
    trust_class: str = "unknown",
    revision: str | int | None = None,
    object_id: str | None = None,
) -> MappedSubmissions:
    """Convert one Thoth capture source snapshot to a ``core.source`` Record.

    ``trust_class`` is producer-controlled and must be chosen by the caller
    from the registry vocabulary; the conservative default is ``unknown``.
    ``revision`` is accepted for interface symmetry but unused: the source
    Record is the origin root and carries no origin tuple. ``object_id``
    pins the Record URN (dual-write uses a deterministic archive-derived
    URN so the origin root stays unique across restarts); when omitted a
    fresh URN is generated.
    """
    if trust_class not in _SOURCE_TRUST_CLASSES:
        raise ThothMapError(f"unknown trust_class {trust_class!r}")
    source_name = require_str(snapshot, "source_name", what="capture source")
    source_type = require_str(snapshot, "source_type", what="capture source")
    native_identity = (
        optional_str(snapshot, "native_source_id")
        or optional_str(snapshot, "account")
        or source_name
    )
    connector = (
        optional_str(snapshot, "collector")
        or optional_str(snapshot, "name")  # ConnectorManifest.name
        or f"thoth.{source_type}"
    )
    display_name = optional_str(snapshot, "display_name") or source_name

    extensions: dict = {}
    for key in ("account", "base_uri", "status"):
        value = snapshot.get(key)
        if value is not None:
            extensions[f"thoth_{key}"] = value

    record = producer.new_record(
        type="core.source",
        claims=claims(ctx, data_classes=["identity_data"]),
        object_id=object_id,
        payload={
            "kind": source_type,
            "name": display_name,
            "connector": connector,
            "native_identity": native_identity,
            "trust_class": trust_class,
            "producer_key_id": None,
            "extensions": extensions,
        },
    )
    return MappedSubmissions(records=[record])
