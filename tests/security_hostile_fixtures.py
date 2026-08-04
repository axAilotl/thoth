"""Shared loader for hostile security evaluation fixtures."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "security_hostile" / "payloads.json"


@lru_cache(maxsize=1)
def hostile_fixture_corpus() -> tuple[dict[str, Any], ...]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    fixtures = payload.get("fixtures")
    if not isinstance(fixtures, list):
        raise ValueError(
            f"Hostile fixture corpus must contain a fixtures list: {FIXTURE_PATH}"
        )
    return tuple(dict(item) for item in fixtures if isinstance(item, dict))


def hostile_fixture(fixture_id: str) -> dict[str, Any]:
    for fixture in hostile_fixture_corpus():
        if fixture.get("id") == fixture_id:
            return dict(fixture)
    raise KeyError(f"Unknown hostile security fixture: {fixture_id}")


def hostile_text(fixture_id: str) -> str:
    text = hostile_fixture(fixture_id).get("text")
    if not isinstance(text, str):
        raise ValueError(f"Hostile fixture {fixture_id!r} is missing text")
    return text
