"""Dual-write error ledger: loud, durable records of mirror failures.

A failed mirror must never corrupt the authoritative legacy write, but it
must never disappear either. Every failure is appended here as one JSON
line and read back by ``scripts/ccf_dualwrite_check.py``, which reports
unresolved entries as mismatches. The ledger is an operational tripwire,
not a queue: after the underlying failure is fixed, re-running the
capture (or the corpus import) heals the inventory and the operator
truncates the ledger.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping


def append_error(path: str | Path, entry: Mapping) -> None:
    """Append one failure record to the JSONL ledger (creates parents)."""
    record = dict(entry)
    record.setdefault(
        "at", datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    )
    ledger = Path(path)
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with ledger.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(record, ensure_ascii=False, sort_keys=True, default=str) + "\n"
        )


def read_errors(path: str | Path) -> list[dict]:
    """Read every ledger entry; an absent ledger means no failures."""
    ledger = Path(path)
    if not ledger.is_file():
        return []
    entries: list[dict] = []
    with ledger.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                entries.append(json.loads(text))
            except json.JSONDecodeError as exc:
                entries.append(
                    {
                        "at": None,
                        "kind": "ledger_corrupt",
                        "error": f"{ledger}:{line_number}: {exc}",
                    }
                )
    return entries
