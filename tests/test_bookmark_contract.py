from datetime import datetime

import pytest

from core.bookmark_contract import (
    BOOKMARK_PAYLOAD_FIELDS,
    bookmark_contract_summary,
    build_graphql_cache_filename,
    normalize_bookmark_payload,
    normalize_source,
    normalize_timestamp,
    validate_tweet_id,
)


def test_validate_tweet_id_accepts_numeric_strings():
    assert validate_tweet_id("1234567890") == "1234567890"


@pytest.mark.parametrize("tweet_id", [None, "", "abc", "12a34"])
def test_validate_tweet_id_rejects_invalid_values(tweet_id):
    with pytest.raises(ValueError):
        validate_tweet_id(tweet_id)


def test_normalize_bookmark_payload_keeps_canonical_fields():
    payload = normalize_bookmark_payload(
        {
            "tweet_id": 123,
            "tweet_data": {"text": "hello"},
            "graphql_response": {"data": {}},
            "force": 1,
        },
        default_source="userscript_fetch",
        default_timestamp=datetime(2026, 4, 4, 12, 30, 45),
    )

    assert payload["tweet_id"] == "123"
    assert payload["source"] == "userscript_fetch"
    assert payload["timestamp"] == "2026-04-04T12:30:45"
    assert payload["force"] is True
    assert payload["tweet_data"] == {"text": "hello"}
    assert payload["graphql_response"] == {"data": {}}


def test_build_graphql_cache_filename_is_timestamped():
    assert (
        build_graphql_cache_filename("42", timestamp=datetime(2026, 4, 4, 12, 30, 45))
        == "tweet_42_20260404_123045.json"
    )


def test_normalize_source_and_timestamp_require_values():
    assert normalize_source("  x_api_backfill  ") == "x_api_backfill"
    assert normalize_timestamp("2026-04-04T12:30:45") == "2026-04-04T12:30:45"


def test_contract_summary_mentions_canonical_queue_payload():
    summary = bookmark_contract_summary()
    assert "tweet_id" in summary
    assert "GraphQL payloads are optional" in summary
    assert "queue" in summary
    assert BOOKMARK_PAYLOAD_FIELDS == (
        "tweet_id",
        "tweet_data",
        "graphql_response",
        "graphql_cache_file",
        "timestamp",
        "source",
        "force",
    )
