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


class RuntimeCompositionError(RuntimeError):
    """Raised when runtime composition invariants are violated."""


def validate_metadata_db_matches_layout(db: MetadataDB, layout: PathLayout) -> None:
    """Prove that ``db`` points at the database path declared by ``layout``.

    The comparison uses resolved canonical paths so symlinks or relative
    segments cannot hide a mismatch. This validation must run before any
    ``ensure_directories`` or other filesystem mutation.
    """
    if not isinstance(db, MetadataDB):
        return
    db_path = Path(db.db_path).resolve()
    layout_path = Path(layout.database_path).resolve()
    if db_path != layout_path:
        raise RuntimeCompositionError(
            f"Metadata database path {db_path} does not match "
            f"runtime layout database path {layout_path}"
        )


def _legacy_doubled_database_path(layout: PathLayout) -> Path:
    """Return the known legacy doubled-path location for the metadata DB.

    Older defaults accidentally nested the system directory, producing paths
    such as ``.thoth_system/.thoth_system/meta.db``. This is only a diagnostic
    helper; Thoth never creates or migrates the legacy location automatically.
    """
    return (layout.system_root / layout.system_root.name / layout.database_path.name).resolve()


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
            raise RuntimeCompositionError(
                f"Refusing to reuse metadata database at {existing.db_path} "
                f"for requested path {requested_layout.database_path}"
            )
        return existing

    legacy_path = _legacy_doubled_database_path(requested_layout)
    if not requested_path.exists() and legacy_path.exists():
        raise RuntimeCompositionError(
            f"Legacy metadata database found at {legacy_path}, but the configured "
            f"database is {requested_path}. Move or rename the legacy file "
            f"(or update database.path to point at it) before starting Thoth."
        )

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
