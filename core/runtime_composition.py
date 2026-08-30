"""Explicit application composition root for the Thoth runtime.

This module owns construction and injection of long-lived services that are
shared across API, CLI, and worker entry points. Importing it must not create
runtime state; callers must invoke ``resolve_runtime_database`` at application
startup.
"""

from __future__ import annotations

from pathlib import Path

from .config import Config
from .metadata_db import MetadataDB, get_metadata_db, set_metadata_db
from .path_layout import PathLayout, build_path_layout


def resolve_runtime_database(
    config_obj: Config,
    *,
    layout: PathLayout | None = None,
    project_root: Path | None = None,
) -> MetadataDB:
    """Build and register the singleton metadata database for this process.

    API, CLI, and worker entry points call this once at startup. If a database
    is already registered, it must resolve to the same canonical path; a
    mismatch is a hard failure so runtime components never silently use a
    different database than the one requested by the composition root.
    """
    requested_layout = layout or build_path_layout(
        config_obj, project_root=project_root
    )
    requested_path = Path(requested_layout.database_path).resolve()

    try:
        existing = get_metadata_db()
    except RuntimeError:
        existing = None

    if existing is not None:
        existing_path = Path(existing.db_path).resolve()
        if existing_path != requested_path:
            raise RuntimeError(
                f"Refusing to reuse metadata database at {existing.db_path} "
                f"for requested path {requested_layout.database_path}"
            )
        return existing

    db = MetadataDB(str(requested_layout.database_path))
    set_metadata_db(db)
    return db


def teardown_runtime_services() -> None:
    """Tear down the process-global runtime services.

    This is the public composition-root lifecycle counterpart to
    ``resolve_runtime_database``.  Production entry points call it during
    graceful shutdown so that the registered metadata database and shared
    knowledge runtime are released before the process exits.
    """
    from .ingestion_runtime import clear_knowledge_artifact_runtime
    from .metadata_db import clear_metadata_db

    clear_metadata_db()
    clear_knowledge_artifact_runtime()


def reset_runtime_database() -> None:
    """Clear the registered runtime database and shared runtime.

    Provided for isolated tests that need to reset process-global state
    explicitly; production code should prefer ``teardown_runtime_services``.
    """
    teardown_runtime_services()
