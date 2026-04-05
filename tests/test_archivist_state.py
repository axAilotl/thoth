from datetime import datetime
from pathlib import Path

from core.archivist_selection import ArchivistCandidate
from core.archivist_state import (
    clear_archivist_topic_force,
    evaluate_archivist_dirty_check,
    load_archivist_topic_state,
    record_archivist_topic_run,
    request_archivist_topic_force,
)
from core.archivist_topics import ArchivistTopicDefinition
from core.metadata_db import MetadataDB


def make_topic(**overrides) -> ArchivistTopicDefinition:
    values = {
        "id": "companion-ai-research",
        "title": "Companion AI Research",
        "output_path": "pages/topic-companion-ai-research.md",
        "include_roots": ("tweets",),
        "cadence_hours": 12.0,
    }
    values.update(overrides)
    return ArchivistTopicDefinition(**values)


def make_candidate(path: str, source_hash: str) -> ArchivistCandidate:
    return ArchivistCandidate(
        candidate_key=f"vault:{path}",
        path=Path("/tmp") / path,
        scope="vault",
        scope_relative_path=path,
        root_spec="tweets",
        source_type="tweet",
        file_type="markdown",
        title="Companion AI",
        tags=("companion_ai",),
        content_text="Notes about introspection.",
        source_hash=source_hash,
        size_bytes=42,
        updated_at="2026-04-04T00:00:00",
        source_id="tweet-1",
    )


def test_archivist_state_skips_unchanged_topics_until_due(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    topic = make_topic()
    candidates = [make_candidate("tweets/one.md", "hash-a")]
    route = ("openrouter", "openrouter/cheap", {"max_tokens": 1200})

    initial = evaluate_archivist_dirty_check(topic, candidates, route=route, db=db)
    assert initial.should_run is True
    assert initial.reason == "initial_run"

    record_archivist_topic_run(
        topic,
        candidates,
        route=route,
        db=db,
        run_at="2026-04-04T00:00:00",
    )

    unchanged = evaluate_archivist_dirty_check(
        topic,
        candidates,
        route=route,
        db=db,
        now=datetime.fromisoformat("2026-04-04T06:00:00"),
    )

    assert unchanged.should_run is False
    assert unchanged.reason == "up_to_date"
    assert unchanged.next_due_at == "2026-04-04T12:00:00"


def test_archivist_state_marks_topics_dirty_on_source_or_route_change(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    topic = make_topic()
    route = ("openrouter", "openrouter/cheap", {"max_tokens": 1200})
    original_candidates = [make_candidate("tweets/one.md", "hash-a")]
    record_archivist_topic_run(
        topic,
        original_candidates,
        route=route,
        db=db,
        run_at="2026-04-04T00:00:00",
    )

    changed_sources = evaluate_archivist_dirty_check(
        topic,
        [make_candidate("tweets/one.md", "hash-b")],
        route=route,
        db=db,
        now=datetime.fromisoformat("2026-04-04T01:00:00"),
    )
    assert changed_sources.should_run is True
    assert changed_sources.reason == "sources_changed"
    assert changed_sources.dirty is True

    changed_route = evaluate_archivist_dirty_check(
        topic,
        original_candidates,
        route=("anthropic", "claude-sonnet", {"max_tokens": 4000}),
        db=db,
        now=datetime.fromisoformat("2026-04-04T01:00:00"),
    )
    assert changed_route.should_run is True
    assert changed_route.reason == "route_changed"
    assert changed_route.dirty is True


def test_archivist_state_supports_manual_force_and_clearing(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    topic = make_topic()
    candidates = [make_candidate("tweets/one.md", "hash-a")]
    route = ("openrouter", "openrouter/cheap", {"max_tokens": 1200})
    record_archivist_topic_run(
        topic,
        candidates,
        route=route,
        db=db,
        run_at="2026-04-04T00:00:00",
    )

    requested = request_archivist_topic_force(
        topic,
        db=db,
        requested_at="2026-04-04T02:00:00",
        reason="operator requested rebuild",
    )
    assert requested.force_requested_at == "2026-04-04T02:00:00"

    forced = evaluate_archivist_dirty_check(
        topic,
        candidates,
        route=route,
        db=db,
        now=datetime.fromisoformat("2026-04-04T02:30:00"),
    )
    assert forced.should_run is True
    assert forced.reason == "manual_force"
    assert forced.forced is True

    cleared = clear_archivist_topic_force(topic.id, db=db)
    assert cleared.force_requested_at is None
    assert load_archivist_topic_state(topic.id, db=db).force_requested_at is None


def test_archivist_state_runs_again_when_topic_is_due(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    topic = make_topic(cadence_hours=6.0)
    candidates = [make_candidate("tweets/one.md", "hash-a")]
    route = ("openrouter", "openrouter/cheap", {"max_tokens": 1200})
    record_archivist_topic_run(
        topic,
        candidates,
        route=route,
        db=db,
        run_at="2026-04-04T00:00:00",
    )

    due = evaluate_archivist_dirty_check(
        topic,
        candidates,
        route=route,
        db=db,
        now=datetime.fromisoformat("2026-04-04T07:00:00"),
    )

    assert due.should_run is True
    assert due.reason == "cadence_due"
    assert due.due is True
