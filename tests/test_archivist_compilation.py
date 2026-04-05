from pathlib import Path

from core.archivist_compilation.planning import build_stage_planning_result, extract_cited_candidate_keys
from core.archivist_compilation.models import ArchivistTopicSourceUsage
from core.archivist_selection import ArchivistCandidate
from core.archivist_topics import ArchivistTopicDefinition


def _make_topic(**overrides) -> ArchivistTopicDefinition:
    values = {
        "id": "companion-ai-research",
        "title": "Companion AI Research",
        "output_path": "pages/topic-companion-ai-research.md",
        "include_roots": ("tweets", "stars"),
        "source_types": ("tweet", "repository"),
        "cadence_hours": 12.0,
    }
    values.update(overrides)
    return ArchivistTopicDefinition(**values)


def _make_candidate(path: str, *, source_type: str, score: float, source_hash: str) -> ArchivistCandidate:
    return ArchivistCandidate(
        candidate_key=f"vault:{path}",
        path=Path("/tmp") / path,
        scope="vault",
        scope_relative_path=path,
        root_spec=path.split("/", 1)[0],
        source_type=source_type,
        file_type="markdown",
        title=path,
        tags=("companion_ai",),
        content_text="Companion AI source text.",
        source_hash=source_hash,
        size_bytes=42,
        updated_at="2026-04-04T00:00:00",
        source_id=path,
        retrieval_score=score,
    )


def test_stage_planning_skips_unchanged_never_used_sources_and_keeps_changed_and_carryover():
    topic = _make_topic()
    changed_tweet = _make_candidate(
        "tweets/new.md",
        source_type="tweet",
        score=0.9,
        source_hash="hash-new",
    )
    carryover_repo = _make_candidate(
        "stars/repo.md",
        source_type="repository",
        score=0.7,
        source_hash="hash-repo",
    )
    skipped_tweet = _make_candidate(
        "tweets/old-unused.md",
        source_type="tweet",
        score=0.5,
        source_hash="hash-old",
    )
    usage_by_key = {
        carryover_repo.candidate_key: ArchivistTopicSourceUsage(
            topic_id=topic.id,
            candidate_key=carryover_repo.candidate_key,
            source_type="repository",
            source_hash="hash-repo",
            retrieval_score=0.7,
            last_polled_at="2026-04-04T00:00:00",
            last_selected_at="2026-04-04T00:00:00",
            last_read_at="2026-04-04T00:00:00",
            last_source_used_at="2026-04-04T00:00:00",
            last_final_used_at="2026-04-04T00:00:00",
            selected_count=1,
            read_count=1,
            source_used_count=1,
            final_used_count=1,
            last_decision="final_used",
            last_reason="final_citation",
            updated_at="2026-04-04T00:00:00",
        ),
        skipped_tweet.candidate_key: ArchivistTopicSourceUsage(
            topic_id=topic.id,
            candidate_key=skipped_tweet.candidate_key,
            source_type="tweet",
            source_hash="hash-old",
            retrieval_score=0.5,
            last_polled_at="2026-04-04T00:00:00",
            last_selected_at="2026-04-04T00:00:00",
            last_read_at="2026-04-04T00:00:00",
            last_source_used_at=None,
            last_final_used_at=None,
            selected_count=1,
            read_count=1,
            source_used_count=0,
            final_used_count=0,
            last_decision="read_not_used",
            last_reason="stage_not_cited",
            updated_at="2026-04-04T00:00:00",
        ),
    }

    result = build_stage_planning_result(
        topic,
        [changed_tweet, carryover_repo, skipped_tweet],
        usage_by_key=usage_by_key,
        force=False,
    )

    assert result.any_source_delta is True
    assert len(result.stage_plans) == 2
    assert changed_tweet.candidate_key in result.selected_candidate_keys
    assert carryover_repo.candidate_key in result.selected_candidate_keys
    assert skipped_tweet.candidate_key in result.skipped_unchanged_candidate_keys


def test_extract_cited_candidate_keys_preserves_first_use_order():
    first = _make_candidate("tweets/one.md", source_type="tweet", score=0.9, source_hash="hash-a")
    second = _make_candidate("stars/two.md", source_type="repository", score=0.8, source_hash="hash-b")

    cited = extract_cited_candidate_keys(
        "Signal [S2], then [S1], then [S2] again.",
        [first, second],
    )

    assert cited == (second.candidate_key, first.candidate_key)
