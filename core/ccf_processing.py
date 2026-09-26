"""Publish metadata only after native processing has successfully committed."""

import logging
from pathlib import Path

from .ccf_metadata import open_metadata_service

logger = logging.getLogger(__name__)


def record_processed_artifact(config, layout, entry, artifact):
    """Metadata errors remain visible without changing a processed queue row."""
    service = open_metadata_service(config, layout=layout)
    if service is None:
        return
    try:
        record = service.capture_queue(artifact_id=entry.artifact_id,
                                       artifact_type=entry.artifact_type,
                                       payload=artifact.to_dict())
    except Exception:
        logger.exception("CCF processed metadata failed for %s", entry.artifact_id)
        return
    for kind, name in artifact.output_paths.items():
        try:
            path = Path(name)
            if not path.is_absolute():
                path = layout.vault_root / path
            output = service.record_file(path, artifact_type=kind)
            if kind in {"summary", "translation", "transcript", "markdown"}:
                service.link(output, "derived_from", record, inferred=True)
            else:
                service.link(record, "derived_from", output)
        except Exception:
            logger.exception("CCF output metadata failed for %s output %s", entry.artifact_id, name)
