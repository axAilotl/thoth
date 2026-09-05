"""Operator/agent inspection and explicit adoption of generated wiki baselines."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path

from .config import config
from .metadata_db import MetadataDB
from .path_layout import build_path_layout
from .wiki_publication import WikiPublicationStore


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("inspect", "adopt", "feedback-status"))
    parser.add_argument("page", help="Markdown path relative to the configured wiki root")
    parser.add_argument("--expected-hash", help="Required for adoption; inspect first")
    parser.add_argument("--feedback-id")
    parser.add_argument("--status", choices=("pending", "included", "addressed", "needs_clarification"))
    args = parser.parse_args(argv)
    layout = build_path_layout(config)
    store = WikiPublicationStore(MetadataDB(str(layout.database_path)), layout.wiki_root)
    if Path(args.page).is_absolute():
        parser.error("page must be relative to the configured wiki root")
    page = layout.wiki_root / args.page
    if args.action == "adopt":
        if not args.expected_hash:
            parser.error("adopt requires --expected-hash from an inspected generated-only page")
        store.adopt_baseline(page, expected_hash=args.expected_hash)
    elif args.action == "feedback-status":
        if not args.feedback_id or not args.status:
            parser.error("feedback-status requires --feedback-id and --status")
        store.set_feedback_status(page, args.feedback_id, args.status)
    print(json.dumps({"publication": asdict(store.inspect(page)), "feedback": store.feedback_records(page)}, indent=2))


if __name__ == "__main__":
    main()
