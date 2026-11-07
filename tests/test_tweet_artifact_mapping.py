"""Tweet artifact mapping from X API bookmark payloads.

The X API backfill emits a payload shape that carries the tweet's own
created_at, public_metrics, expanded media objects and t.co expansions.  The
artifact must keep all of it: capture time is not publication time, and paper
or repository detection runs on expanded URLs.
"""

from core.artifacts.tweet import TweetArtifact


def _payload() -> dict:
    return {
        "tweet_id": "2107909069369487577",
        "source": "x_api_backfill",
        "timestamp": "2026-11-03T01:21:18.756785+00:00",
        "tweet_data": {
            "id": "2107909069369487577",
            "text": "new work https://t.co/abc123",
            "full_text": "new work https://t.co/abc123",
            "created_at": "2025-12-01T09:30:00.000Z",
            "author_username": "sahilmdkr",
            "author_name": "Sahil",
            "author": "sahilmdkr",
            "public_metrics": {
                "like_count": 485,
                "retweet_count": 29,
                "reply_count": 5,
                "bookmark_count": 656,
            },
            "entities": {
                "urls": [
                    {
                        "url": "https://t.co/abc123",
                        "expanded_url": "https://arxiv.org/abs/2610.06817",
                        "display_url": "arxiv.org/abs/2610.06817",
                    }
                ]
            },
            "attachments": {"media_keys": ["3_1"]},
            "media": [
                {
                    "media_key": "3_1",
                    "type": "photo",
                    "url": "https://pbs.twimg.com/media/example.jpg",
                    "alt_text": "a diagram",
                }
            ],
        },
    }


def test_created_at_prefers_tweet_publication_time():
    artifact = TweetArtifact.from_bookmark_payload(_payload())
    assert artifact.created_at == "2025-12-01T09:30:00.000Z"
    assert artifact.ingested_at == "2026-11-03T01:21:18.756785+00:00"


def test_engagement_comes_from_public_metrics():
    artifact = TweetArtifact.from_bookmark_payload(_payload())
    assert artifact.engagement == {
        "favorite_count": 485,
        "retweet_count": 29,
        "reply_count": 5,
    }
    tweet = artifact.to_tweet_model()
    assert tweet.favorite_count == 485
    assert tweet.retweet_count == 29
    assert tweet.reply_count == 5


def test_media_and_expanded_links_are_preserved():
    artifact = TweetArtifact.from_bookmark_payload(_payload())
    assert [item.media_url for item in artifact.media_items] == [
        "https://pbs.twimg.com/media/example.jpg"
    ]
    assert artifact.media_items[0].media_type == "photo"
    assert artifact.media_items[0].alt_text == "a diagram"
    assert [(m.short_url, m.expanded_url) for m in artifact.url_mappings] == [
        ("https://t.co/abc123", "https://arxiv.org/abs/2610.06817")
    ]
    assert artifact.extracted_urls == ["https://arxiv.org/abs/2610.06817"]

    tweet = artifact.to_tweet_model()
    assert [item.media_url for item in tweet.media_items] == [
        "https://pbs.twimg.com/media/example.jpg"
    ]
    assert tweet.extracted_urls == ["https://arxiv.org/abs/2610.06817"]
    assert tweet.url_mappings[0].expanded_url == "https://arxiv.org/abs/2610.06817"


def test_payload_without_metrics_or_entities_still_builds():
    payload = _payload()
    payload["tweet_data"] = {"text": "bare", "author": "someone"}
    artifact = TweetArtifact.from_bookmark_payload(payload)
    assert artifact.created_at == "2026-11-03T01:21:18.756785+00:00"
    assert artifact.engagement["favorite_count"] == 0
    assert artifact.media_items == []
    assert artifact.url_mappings == []
    assert artifact.to_tweet_model().favorite_count == 0
