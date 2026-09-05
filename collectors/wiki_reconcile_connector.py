"""Scheduled observation of owned wiki pages; never a wiki writer."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from core.archivist_topics import load_archivist_topic_registry
from core.connector_budgets import start_connector_budget_run
from core.metadata_db import get_metadata_db
from core.path_layout import build_path_layout
from core.wiki_publication import WikiPublicationStore


class WikiReconcileConnector:
    def __init__(self, config, *, layout=None, db=None):
        self.config = config
        self.layout = layout or build_path_layout(config)
        self.db = db or get_metadata_db()

    async def collect(self):
        store = WikiPublicationStore(self.db, self.layout.wiki_root)
        registry = load_archivist_topic_registry(self.config)
        paths = set(store.known_paths()) | {
            topic.output_path_for_root(self.layout.wiki_root) for topic in registry.topics
        }
        budget = start_connector_budget_run(self.config, "wiki_reconcile")
        for path in sorted(paths):
            store._path(path)  # Validate containment before budget preflight reads it.
            if path.exists():
                budget.add_file(path, count_input_tokens=False)
        counts = Counter()
        blocked = []
        for path in sorted(paths):
            snapshot = store.inspect(path)
            counts[snapshot.status] += 1
            if not snapshot.publishable:
                blocked.append({"page": snapshot.page_key, "reason": snapshot.status})
        result = {
            "pages": len(paths), "statuses": dict(counts), "blocked": blocked,
            "budget": budget.summary(), "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        self.db.upsert_automation_state("wiki_reconcile:last_result", result)
        return result
