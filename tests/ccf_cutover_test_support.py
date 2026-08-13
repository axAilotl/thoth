"""Focused helpers shared by the CCF cutover-gate test modules."""

from __future__ import annotations

import asyncio
from pathlib import Path

from ccf.db import CcfPostgresSettings, open_ccf_connection
from ccf.projections.schema import CCF_PROJECTION_MIGRATION
from ccf_helpers import make_rig


PROJECTION_TABLES = (
    "projection_link_state",
    "projection_derivation_closure",
    "projection_entity_cluster",
    "projection_full_text",
    "projection_embedding",
    "projection_checkpoint",
    "projection_invalidation",
    "generation_fence",
)


def make_cutover_rig(ccf_settings, tmp_path, ccf_package_root):
    return make_rig(ccf_settings, tmp_path, ccf_package_root)


def drop_all_projections(settings: CcfPostgresSettings) -> None:
    with open_ccf_connection(settings) as conn:
        with conn.transaction():
            for table in PROJECTION_TABLES:
                conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")


def reprovision_projection_tables(settings: CcfPostgresSettings) -> None:
    """Recreate projection tables after an intentional destructive drill."""
    with open_ccf_connection(settings) as conn:
        with conn.transaction():
            for statement in CCF_PROJECTION_MIGRATION.statements:
                conn.execute(statement)


def object_counts(settings: CcfPostgresSettings) -> dict[str, int]:
    with open_ccf_connection(settings) as conn:
        return {
            kind: int(count)
            for kind, count in conn.execute(
                "SELECT object_kind, COUNT(*) FROM object_header GROUP BY 1"
            ).fetchall()
        }


def dualwrite_config(
    tmp_path: Path,
    schema: str,
    *,
    dual_write: bool = True,
    enabled: bool = True,
):
    from core.config import Config

    cfg = Config()
    cfg.data = {
        "paths": {
            "vault_dir": str(tmp_path / "knowledge_vault"),
            "system_dir": str(tmp_path / ".thoth_system"),
            "cache_dir": str(tmp_path / ".thoth_system" / "cache"),
        },
        "database": {
            "enabled": True,
            "path": str(tmp_path / ".thoth_system" / "meta.db"),
            "ccf_archive": {
                "enabled": enabled,
                "dual_write": dual_write,
                "backend": "postgres",
                "dsn_env": "THOTH_CCF_POSTGRES_DSN",
                "schema": schema,
                "device_key_path": str(tmp_path / "ccf" / "device.pem"),
                "archive_key_path": str(tmp_path / "ccf" / "archive.pem"),
                "error_log_path": str(tmp_path / "errors.jsonl"),
            },
        },
    }
    return cfg


def collect_imported_markdown(
    cfg,
    import_dir: Path,
    tmp_path: Path,
    *,
    source_name: str,
):
    from collectors.imported_markdown_connector import ImportedMarkdownConnector
    from core.metadata_db import MetadataDB
    from core.path_layout import build_path_layout

    layout = build_path_layout(cfg)
    db = MetadataDB(str(tmp_path / ".thoth_system" / "meta.db"))
    connector = ImportedMarkdownConnector(cfg, layout=layout, db=db)
    return asyncio.run(
        connector.collect(import_dirs=[import_dir], source_name=source_name)
    )
