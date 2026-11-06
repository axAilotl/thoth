"""Coverage receipts for bounded raw collection, separate from synthesis."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import json
from functools import lru_cache
from pathlib import Path


class CollectionCoverage:
    def __init__(self, config):
        self.config = config
        self.sources: dict[str, dict[str, Any]] = {}

    def source(self, source: str, *, time_basis: str = "source_time") -> dict[str, Any]:
        return self.sources.setdefault(source, {
            "source": source, "status": "completed", "observed_count": 0,
            "queued_count": 0, "errors": [], "cursor": None,
            "time_basis": time_basis,
        })

    def fail(self, source: str, code: str, *, partial: bool = False) -> None:
        entry = self.source(source)
        entry["status"] = "partial" if partial or entry["queued_count"] else "failed"
        entry["errors"].append({"code": code})

    def limited(self, source: str, cursor: Any = None) -> None:
        entry = self.source(source)
        entry["status"] = "partial"
        entry["cursor"] = cursor
        entry["errors"].append({"code": "limit_exhausted"})

    def in_window(self, source: str, timestamp: str | None) -> bool:
        start = self.config.get("managed.window_start")
        end = self.config.get("managed.window_end")
        if start is None:
            return True
        if not timestamp:
            self.fail(source, "missing_source_time", partial=True)
            return False
        try:
            value = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if value.tzinfo is None:
                raise ValueError("naive source timestamp")
            return datetime.fromisoformat(start.replace("Z", "+00:00")) <= value < datetime.fromisoformat(end.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            self.fail(source, "invalid_source_time", partial=True)
            return False

    def receipt(self) -> dict[str, Any]:
        statuses = [source["status"] for source in self.sources.values()]
        status = "completed" if statuses and all(s == "completed" for s in statuses) else (
            "failed" if not statuses or all(s == "failed" for s in statuses) else "partial"
        )
        end = self.config.get("managed.window_end")
        coverage = list(self.sources.values())
        for entry in coverage:
            entry["covered_through"] = end if entry["status"] == "completed" else None
        return {"status": status, "coverage": coverage,
                "window_start": self.config.get("managed.window_start"), "window_end": end,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "checkpoint": {"sources": coverage},
                "resume_token": None if status == "completed" else json.dumps({entry["source"]: entry["cursor"] for entry in coverage if entry["cursor"] is not None})}


def collection_limit(config, key: str) -> float:
    value = config.get(f"collection.{key}", _defaults().get(key))
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < float("inf"):
        raise ValueError(f"collection.{key} must be configured as a finite positive number")
    return value


@lru_cache(maxsize=1)
def _defaults() -> dict[str, Any]:
    return json.loads((Path(__file__).resolve().parent.parent / "config.example.json").read_text())["collection"]
