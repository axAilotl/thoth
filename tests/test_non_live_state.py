from datetime import datetime, timedelta, timezone

import pytest

from core.metadata_db import MetadataDB
from core.non_live_state import (
    MIN_NON_LIVE_INTERVAL_HOURS,
    get_known_readme_filename,
    get_non_live_next_run_at,
    mark_non_live_run_finished,
    mark_non_live_run_started,
    record_readme_probe_outcome,
    should_skip_readme_probe,
    validate_non_live_interval_hours,
)


def test_validate_non_live_interval_hours_rejects_too_frequent_values():
    assert (
        validate_non_live_interval_hours(
            MIN_NON_LIVE_INTERVAL_HOURS,
            field_name="automation.social_sync.interval_hours",
        )
        == MIN_NON_LIVE_INTERVAL_HOURS
    )

    with pytest.raises(ValueError):
        validate_non_live_interval_hours(
            MIN_NON_LIVE_INTERVAL_HOURS - 1,
            field_name="automation.social_sync.interval_hours",
        )


def test_non_live_scheduler_state_delays_restart_runs(tmp_path):
    db = MetadataDB(db_path=str(tmp_path / "meta.db"))
    now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

    assert get_non_live_next_run_at(
        db,
        job_name="social_sync",
        interval_hours=MIN_NON_LIVE_INTERVAL_HOURS,
        run_on_startup=True,
        now=now,
    ) == now

    mark_non_live_run_started(
        db,
        job_name="social_sync",
        interval_hours=MIN_NON_LIVE_INTERVAL_HOURS,
        now=now,
    )
    mark_non_live_run_finished(
        db,
        job_name="social_sync",
        success=True,
        now=now + timedelta(minutes=1),
    )

    next_run_at = get_non_live_next_run_at(
        db,
        job_name="social_sync",
        interval_hours=MIN_NON_LIVE_INTERVAL_HOURS,
        run_on_startup=True,
        now=now + timedelta(minutes=5),
    )

    assert next_run_at == now + timedelta(hours=MIN_NON_LIVE_INTERVAL_HOURS)


def test_readme_probe_state_skips_recent_missing_and_reuses_known_filename(tmp_path):
    db = MetadataDB(db_path=str(tmp_path / "meta.db"))
    now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

    record_readme_probe_outcome(
        db,
        provider="huggingface",
        repo_name="org/repo",
        repo_revision="2026-04-04T00:00:00+00:00",
        found=False,
        now=now,
    )

    assert should_skip_readme_probe(
        db,
        provider="huggingface",
        repo_name="org/repo",
        repo_revision="2026-04-04T00:00:00+00:00",
        now=now + timedelta(hours=1),
    )
    assert not should_skip_readme_probe(
        db,
        provider="huggingface",
        repo_name="org/repo",
        repo_revision="2026-04-05T00:00:00+00:00",
        now=now + timedelta(hours=1),
    )

    record_readme_probe_outcome(
        db,
        provider="huggingface",
        repo_name="org/repo",
        repo_revision="2026-04-05T00:00:00+00:00",
        found=True,
        filename="README.md",
        now=now + timedelta(hours=2),
    )

    assert (
        get_known_readme_filename(
            db,
            provider="huggingface",
            repo_name="org/repo",
            repo_revision="2026-04-05T00:00:00+00:00",
        )
        == "README.md"
    )
