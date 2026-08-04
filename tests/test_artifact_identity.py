import json

from core.artifact_identity import native_id_from_payload
from core.artifact_review_policy import structural_review_for_ingestion


def test_native_id_keys_are_shared_by_review_and_capture_identity():
    cases = {
        "tweet": ({"tweet_id": "tweet-1"}, "tweet-1"),
        "paper": ({"doi": "10.1000/example"}, "10.1000/example"),
        "repository": ({"full_name": "owner/repo"}, "owner/repo"),
        "web_clipper": ({"source_relative_path": "clips/page.md"}, "clips/page.md"),
        "markdown": ({"source_relative_path": "notes/imported.md"}, "notes/imported.md"),
        "video": ({"native_id": "video-native"}, "video-native"),
        "transcript": ({"video_id": "video-1"}, "video-1"),
    }

    for artifact_type, (payload, expected_id) in cases.items():
        assert native_id_from_payload(artifact_type, payload) == expected_id
        assert (
            structural_review_for_ingestion(
                artifact_type=artifact_type,
                payload_json=json.dumps(payload),
            )
            is None
        )


def test_native_id_review_still_fails_closed_when_all_candidates_are_missing():
    review = structural_review_for_ingestion(
        artifact_type="markdown",
        payload_json=json.dumps({"title": "Missing source identity"}),
    )

    assert review is not None
    assert review["category"] == "incomplete_payload"
