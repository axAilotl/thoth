"""Dual-write corpus driver (checklist section 10a).

Imports a representative corpus through the REAL legacy entrypoint —
``collectors.imported_markdown_connector.ImportedMarkdownConnector`` —
with the CCF dual-write mirror enabled against a scratch workspace
(knowledge_vault layout + SQLite metadata DB + CCF schema + key material
all under ``--workspace``). The corpus itself is only read.

After the import, run ``scripts/ccf_dualwrite_check.py`` against the same
workspace to reconcile to zero mismatches:

    python scripts/ccf_dualwrite_corpus_import.py \
        --corpus /home/ada/thoth/.CCF/_vault_share \
        --workspace /tmp/ccf-dualwrite-corpus \
        --dsn "$THOTH_CCF_POSTGRES_DSN"
    python scripts/ccf_dualwrite_check.py \
        --metadata-db /tmp/ccf-dualwrite-corpus/.thoth_system/meta.db \
        --dsn "$THOTH_CCF_POSTGRES_DSN" --schema <printed schema> \
        --error-log /tmp/ccf-dualwrite-corpus/.thoth_system/ccf_dualwrite_errors.jsonl \
        --out /tmp/ccf-dualwrite-corpus/report.json
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config import Config  # noqa: E402
from core.metadata_db import MetadataDB  # noqa: E402
from core.path_layout import build_path_layout  # noqa: E402


def build_config(workspace: Path, *, schema: str) -> Config:
    """Scratch Thoth config: legacy stores + gated CCF dual-write mirror."""
    cfg = Config()
    cfg.data = {
        "paths": {
            "vault_dir": str(workspace / "knowledge_vault"),
            "system_dir": str(workspace / ".thoth_system"),
            "cache_dir": str(workspace / ".thoth_system" / "cache"),
        },
        "connectors": {
            "budgets": {
                "defaults": {
                    "max_input_tokens_per_run": 100_000_000,
                    "max_files_per_run": 1_000_000,
                }
            }
        },
        "database": {
            "enabled": True,
            "path": str(workspace / ".thoth_system" / "meta.db"),
            "ccf_archive": {
                "enabled": True,
                "dual_write": True,
                "backend": "postgres",
                "dsn_env": "THOTH_CCF_POSTGRES_DSN",
                "schema": schema,
                "device_key_path": str(workspace / ".thoth_system" / "ccf" / "device-ed25519.pem"),
                "archive_key_path": str(workspace / ".thoth_system" / "ccf" / "archive-ed25519.pem"),
                "error_log_path": str(workspace / ".thoth_system" / "ccf_dualwrite_errors.jsonl"),
            },
        },
    }
    return cfg


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--corpus", required=True, help="corpus directory (read-only)")
    parser.add_argument("--workspace", required=True, help="scratch workspace (created)")
    parser.add_argument("--dsn", default=None, help="CCF Postgres DSN")
    parser.add_argument("--schema", default=None, help="CCF schema name (default: fresh)")
    parser.add_argument("--limit", type=int, default=None, help="cap imported files")
    parser.add_argument(
        "--include-dir",
        action="append",
        default=None,
        help="corpus subdirectory to import (repeatable; default: all)",
    )
    args = parser.parse_args(argv)

    dsn = args.dsn or os.environ.get("THOTH_CCF_POSTGRES_DSN")
    if not dsn:
        print("error: pass --dsn or set THOTH_CCF_POSTGRES_DSN", file=sys.stderr)
        return 2
    os.environ["THOTH_CCF_POSTGRES_DSN"] = dsn

    corpus = Path(args.corpus)
    if not corpus.is_dir():
        print(f"error: corpus not found: {corpus}", file=sys.stderr)
        return 2
    workspace = Path(args.workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    schema = args.schema or f"ccf_dw_{uuid.uuid4().hex[:10]}"

    cfg = build_config(workspace, schema=schema)
    layout = build_path_layout(cfg)
    db = MetadataDB(str(workspace / ".thoth_system" / "meta.db"))

    from collectors.imported_markdown_connector import ImportedMarkdownConnector

    connector = ImportedMarkdownConnector(cfg, layout=layout, db=db)
    selected = set(args.include_dir or [])
    import_dirs = sorted(
        str(path)
        for path in corpus.iterdir()
        if path.is_dir() and (not selected or path.name in selected)
    )
    if not import_dirs:
        print("error: no corpus directories selected", file=sys.stderr)
        return 2
    result = asyncio.run(
        connector.collect(
            import_dirs=import_dirs,
            source_name="obsidian_corpus",
            limit=args.limit,
        )
    )
    summary = result.to_dict()
    print(f"schema: {schema}")
    print(f"metadata_db: {workspace / '.thoth_system' / 'meta.db'}")
    print(f"queued: {summary['queued_count']} artifacts from {len(result.records)} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
