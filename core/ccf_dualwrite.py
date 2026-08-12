"""Config predicate for the config-gated CCF dual-write mirror.

This module intentionally does NOT import the ``ccf`` package: when
``database.ccf_archive.dual_write`` is off, legacy capture paths must not
import or touch CCF code at all. The heavy resolution (DSN, key paths,
package root) lives in ``ccf.dualwrite.config`` and only runs once this
predicate says the operator asked for the mirror.
"""

from __future__ import annotations

from typing import Mapping


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
