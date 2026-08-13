"""Config resolution for the CCF dual-write mirror.

Resolution fails closed: ``dual_write: true`` requires
``database.ccf_archive.enabled: true``, a reachable DSN (via the
configured ``dsn_env``), explicit device/archive key paths (config or
``THOTH_CCF_DEVICE_KEY`` / ``THOTH_CCF_ARCHIVE_KEY``), and an existing CCF
spec package root. There are no silent defaults for any of these except
the vendored in-repo spec package, which is the canonical pinned artifact
for this codebase.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from ccf.db import (
    CcfConfigError,
    CcfPostgresSettings,
    resolve_ccf_postgres_settings,
)

DEFAULT_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2] / "spec" / "ccf" / "0.1.2-rc1"
)
DEFAULT_PACKAGE_ROOT_ENV = "THOTH_CCF_PACKAGE_ROOT"
DEFAULT_ERROR_LOG_ENV = "THOTH_CCF_DUALWRITE_ERROR_LOG"
DEFAULT_ERROR_LOG_PATH = ".thoth_system/ccf_dualwrite_errors.jsonl"


@dataclass(frozen=True)
class DualWriteSettings:
    """Resolved dual-write settings; ``enabled=False`` means untouched.

    The ``mirror_*`` flags gate the phase-2 converter families
    (``ccf.dualwrite.families``) individually so operators can roll the
    mirror out family by family. Like ``dual_write`` itself they default
    off; they are meaningless (and unresolved) unless ``dual_write`` is on.
    """

    enabled: bool
    store: CcfPostgresSettings | None = None
    package_root: Path | None = None
    error_log_path: Path | None = None
    mirror_transcripts: bool = False
    mirror_semantic: bool = False
    mirror_review: bool = False
    mirror_wiki: bool = False


def resolve_dual_write_settings(
    config_obj,
    *,
    environ: Mapping[str, str] | None = None,
) -> DualWriteSettings:
    """Resolve dual-write settings from Thoth config and environment.

    Returns ``DualWriteSettings(enabled=False)`` when the operator did not
    ask for the mirror; raises :class:`CcfConfigError` on contradictory or
    incomplete configuration (fail closed).
    """
    env = os.environ if environ is None else environ

    store_cfg = config_obj.get("database.ccf_archive", {})
    if store_cfg is None:
        store_cfg = {}
    if not isinstance(store_cfg, dict):
        raise CcfConfigError("database.ccf_archive must be an object")
    if not bool(store_cfg.get("dual_write", False)):
        return DualWriteSettings(enabled=False)

    store = resolve_ccf_postgres_settings(config_obj, environ=env)
    if not store.enabled:
        raise CcfConfigError(
            "database.ccf_archive.dual_write is true but enabled is false; "
            "enable the store or drop the dual_write flag"
        )
    if not store.device_key_path or not store.archive_key_path:
        raise CcfConfigError(
            "database.ccf_archive.dual_write requires device_key_path and "
            "archive_key_path (config keys or THOTH_CCF_DEVICE_KEY / "
            "THOTH_CCF_ARCHIVE_KEY)"
        )

    package_root = Path(
        store_cfg.get("package_root")
        or env.get(DEFAULT_PACKAGE_ROOT_ENV)
        or DEFAULT_PACKAGE_ROOT
    )
    if not package_root.is_dir():
        raise CcfConfigError(
            f"CCF spec package root not found: {package_root}"
        )

    error_log_path = Path(
        store_cfg.get("error_log_path")
        or env.get(DEFAULT_ERROR_LOG_ENV)
        or DEFAULT_ERROR_LOG_PATH
    )

    return DualWriteSettings(
        enabled=True,
        store=store,
        package_root=package_root,
        error_log_path=error_log_path,
        mirror_transcripts=bool(store_cfg.get("mirror_transcripts", False)),
        mirror_semantic=bool(store_cfg.get("mirror_semantic", False)),
        mirror_review=bool(store_cfg.get("mirror_review", False)),
        mirror_wiki=bool(store_cfg.get("mirror_wiki", False)),
    )
