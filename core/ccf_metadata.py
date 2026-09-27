"""Optional CCF metadata: failures are visible and never block native work."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path

from .path_layout import build_path_layout

logger = logging.getLogger(__name__)


def _settings(config_obj):
    value = config_obj.get('database.ccf', {})
    if not isinstance(value, Mapping):
        raise ValueError('database.ccf must be an object')
    enabled = value.get('enabled', False)
    if not isinstance(enabled, bool):
        raise ValueError('database.ccf.enabled must be boolean')
    return value, enabled


def metadata_enabled(config_obj) -> bool:
    try:
        return _settings(config_obj)[1]
    except Exception:
        logger.exception('CCF metadata configuration failed')
        return False


def open_metadata_service(config_obj, *, layout=None):
    """Open the local store, or report disabled/unavailable metadata as None.

    Paths relative to database.ccf.path resolve under paths.system_dir using
    the same layout as the native database. No signing keys or network store
    are involved. Explicit malformed configuration is reported, not replaced.
    """
    try:
        settings, enabled = _settings(config_obj)
        if not enabled:
            return None
        path = settings.get('path', 'ccf/metadata.sqlite3')
        device = settings.get('device', 'thoth')
        if not isinstance(path, str) or not path.strip():
            raise ValueError('database.ccf.path must be a non-empty string')
        if not isinstance(device, str) or not device.strip():
            raise ValueError('database.ccf.device must be a non-empty string')
        layout = layout or build_path_layout(config_obj)
        path = Path(path)
        if not path.is_absolute():
            path = layout.system_root / path
        from .connector_registry import load_connector_registry
        from ccf.metadata import CcfMetadataService
        from ccf.store import ObjectStore
        return CcfMetadataService(ObjectStore(path, device=device), vault_root=layout.vault_root,
                                  connector_registry=load_connector_registry(config_obj))
    except Exception:
        logger.exception('CCF metadata store could not be opened')
        return None
