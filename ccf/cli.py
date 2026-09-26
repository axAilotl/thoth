"""CCF commands kept separate from Thoth's general command dispatcher."""

import json
from pathlib import Path
import sys


def register_parser(subparsers):
    parser = subparsers.add_parser("ccf", help="CCF 0.3.0 metadata, portable exchange and backfill")
    parser.add_argument("--store", type=Path, help="Explicit isolated CCF database instead of configured store")
    parser.add_argument("--device", help="Device name for a new store or explicit export source")
    actions = parser.add_subparsers(dest="ccf_action", required=True)
    actions.add_parser("status", help="Show metadata counts and CCF version")
    actions.add_parser("backfill", help="Index native Thoth state without modifying sources or running processors")
    export = actions.add_parser("export", help="Export metadata to a new directory or .zip")
    export.add_argument("destination", type=Path)
    export.add_argument("--include-files", action="store_true", help="Include content only from configured local roots")
    ingest = actions.add_parser("import", help="Merge a 0.3.0 directory or ZIP, preserving IDs and locations")
    ingest.add_argument("source", type=Path)
    changes = actions.add_parser("changes", help="Read changes not yet acknowledged by this peer")
    changes.add_argument("peer")
    changes.add_argument("--limit", type=int, default=1000)
    ack = actions.add_parser("ack", help="Record an acknowledgement received from this peer")
    ack.add_argument("peer")
    ack.add_argument("cursor", type=int)


def run(args, config):
    from core.path_layout import build_path_layout
    from core.ccf_metadata import open_metadata_service
    from .metadata import CcfMetadataService
    from .store import ObjectStore

    layout = build_path_layout(config)
    if args.store is not None:
        device = args.device or config.get("database.ccf.device", "thoth")
        store_path = args.store.resolve()
        service = CcfMetadataService(ObjectStore(store_path, device=device), vault_root=layout.vault_root)
    else:
        service = open_metadata_service(config, layout=layout)
        if service is None:
            raise ValueError("CCF is disabled or could not open; configure database.ccf or pass --store")
        device = args.device or config.get("database.ccf.device", "thoth")
        store_path = Path(config.get("database.ccf.path", "ccf/metadata.sqlite3"))
        if not store_path.is_absolute():
            store_path = layout.system_root / store_path
    store = service.store
    content_dir = store_path.parent / (store_path.name + ".files")
    action = args.ccf_action
    if action == "status":
        report = {"version": "0.3.0", "store": str(store_path), "counts": store.stats()}
    elif action == "backfill":
        from .backfill import backfill
        report = backfill(service, layout.database_path, wiki_root=layout.wiki_root)
    elif action == "export":
        from .exchange import export_objects
        roots = [root.base_path for root in layout.content_roots]
        report = export_objects(store.objects(), args.destination, source={"app": "thoth", "device": device},
                                include_files=args.include_files, vault_root=layout.vault_root,
                                allowed_roots=roots, content_dir=content_dir)
    elif action == "import":
        from .exchange import import_objects
        report = import_objects(store, args.source, content_dir=content_dir)
    elif action == "changes":
        report = store.changes(args.peer, limit=args.limit)
    else:
        store.acknowledge(args.peer, args.cursor)
        report = {"peer": args.peer, "acknowledged": args.cursor}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report.get("errors") else 0


def dispatch(args, config):
    try:
        return run(args, config)
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}), file=sys.stderr)
        return 1
