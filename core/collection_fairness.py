"""Fair bounded samples across selected sources, independent of window cursors."""
from __future__ import annotations

import hashlib
import json


class FairCollectionBudget:
    def __init__(self, config, db, connector, sources, *, max_items, slots=None):
        sources = list(dict.fromkeys(sources))
        scope = [connector, config.get("managed.integration_id"), sorted(sources)]
        key = "collection_rotation:" + hashlib.sha256(json.dumps(scope).encode()).hexdigest()
        state = db.get_automation_state(key) or {}
        offset = int(state.get("offset", 0)) % len(sources) if sources else 0
        self.sources = sources[offset:] + sources[:offset]
        self.used = dict.fromkeys(sources, 0)
        self.quotas = {}
        active = [s for s in self.sources if not _completed(config, s)]
        if active:
            share, extra = divmod(int(max_items), len(active))
            self.quotas = {s: share + (i < extra) for i, s in enumerate(active)}
        self.admitted = set(active[:slots] if slots is not None else active)
        # Reserve the NEXT run's turn before I/O. A killed worker must not keep
        # starting at the same slow source. Exact managed replays bypass the
        # collector through the existing durable receipt ledger.
        if sources:
            advance = min(slots, len(sources)) if slots is not None and slots < len(sources) else 1
            db.upsert_automation_state(key, {"offset": (offset + advance) % len(sources)})

    def remaining(self, source):
        return max(0, self.quotas.get(source, 0) - self.used.get(source, 0)) if source in self.admitted else 0

    def observe(self, source):
        if not self.remaining(source):
            return False
        self.used[source] += 1
        return True


def _completed(config, source):
    cursors = json.loads(config.get("managed.cursor") or "{}")
    return bool((cursors.get(source) or {}).get("completed"))
