"""Config predicate for the config-gated CCF dual-write mirror.

This module intentionally does NOT import the ``ccf`` package: when
``database.ccf_archive.dual_write`` is off, legacy capture paths must not
import or touch CCF code at all. The heavy resolution (DSN, key paths,
package root) lives in ``ccf.dualwrite.config`` and only runs once this
predicate says the operator asked for the mirror.
"""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from .path_layout import build_path_layout


def dual_write_requested(config_obj) -> bool:
    """True when config asks for the CCF dual-write mirror.

    Reads only ``database.ccf_archive.dual_write``; contradiction handling
    (``dual_write`` true while ``enabled`` is false) is the resolver's job
    and fails closed there, not here.
    """
    store = config_obj.get("database.ccf_archive", {})
    if store is None:
        return False
    if not isinstance(store, Mapping):
        return False
    return bool(store.get("dual_write", False))


def open_dual_write_service(config_obj):
    """Open the CCF dual-write service when requested, else ``None``.

    Used by mirror call sites outside the capture lifecycle (review
    services, wiki compilers). The ``ccf`` package is imported lazily so
    legacy paths never touch CCF code while the mirror is off.
    Contradictory or incomplete config raises from the resolver — fail
    closed before anything is mirrored.
    """
    if not dual_write_requested(config_obj):
        return None
    from ccf.dualwrite import CcfDualWriteService, resolve_dual_write_settings

    return CcfDualWriteService.create_or_open(resolve_dual_write_settings(config_obj))


def mirrored_queue_artifact(service, config_obj, payload: Mapping):
    """Resolve a queue entry payload to its mirrored media artifact.

    Returns ``(source_ccf_id, artifact_ccf_id)`` when the entry's raw
    payload was mirrored into the archive, else ``None``. Vault-relative
    raw paths are resolved against the configured vault root, matching the
    absolute-path convention the raw_ref_id derivation requires.
    """
    metadata = payload.get("normalized_metadata")
    if not isinstance(metadata, Mapping):
        return None
    source_id = metadata.get("capture_source_id")
    raw = payload.get("raw_payload")
    if not isinstance(raw, Mapping):
        return None
    sha256 = raw.get("sha256")
    path = raw.get("path")
    if not source_id or not sha256 or not path:
        return None
    raw_path = Path(str(path))
    if not raw_path.is_absolute():
        raw_path = build_path_layout(config_obj).vault_root / raw_path
    from ccf.dualwrite import families
    from ccf.dualwrite.conventions import source_record_id

    artifact_ccf_id = families.mirrored_media_artifact(
        service,
        source_id=str(source_id),
        sha256=str(sha256),
        path=str(raw_path.resolve()),
    )
    if artifact_ccf_id is None:
        return None
    return (
        source_record_id(service.archive.archive_id, str(source_id)),
        artifact_ccf_id,
    )
