import asyncio

import pytest

from collectors.omi_api_client import (
    OmiApiError,
    OmiConversationQuery,
    fetch_omi_conversations,
    normalize_categories,
)


def test_omi_conversation_query_requires_developer_key():
    with pytest.raises(OmiApiError, match="must start with omi_dev_"):
        OmiConversationQuery(api_key="not-an-omi-key")


def test_normalize_categories_dedupes_csv_and_lists():
    assert normalize_categories("work, personal,work") == ("work", "personal")
    assert normalize_categories(["ideas", "ideas", "research"]) == ("ideas", "research")


def test_fetch_omi_conversations_pages_with_filters(monkeypatch: pytest.MonkeyPatch):
    calls = []
    responses = [
        _FakeResponse([{"id": "conv_1"}, {"id": "conv_2"}]),
        _FakeResponse([{"id": "conv_3"}]),
    ]

    class FakeAsyncClient:
        def __init__(self, *, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url, *, headers, params):
            calls.append({"url": url, "headers": headers, "params": dict(params)})
            return responses.pop(0)

    monkeypatch.setattr("collectors.omi_api_client.httpx.AsyncClient", FakeAsyncClient)

    query = OmiConversationQuery(
        api_key="omi_dev_test-key",
        limit=3,
        page_size=2,
        include_transcript=True,
        categories=("work", "personal"),
        starred=True,
    )
    result = asyncio.run(fetch_omi_conversations(query))

    assert [item["id"] for item in result] == ["conv_1", "conv_2", "conv_3"]
    assert calls[0]["url"] == "https://api.omi.me/v1/dev/user/conversations"
    assert calls[0]["headers"]["Authorization"] == "Bearer omi_dev_test-key"
    assert calls[0]["params"]["limit"] == 2
    assert calls[0]["params"]["offset"] == 0
    assert calls[0]["params"]["include_transcript"] == "true"
    assert calls[0]["params"]["categories"] == "work,personal"
    assert calls[0]["params"]["starred"] == "true"
    assert calls[1]["params"]["limit"] == 1
    assert calls[1]["params"]["offset"] == 2


class _FakeResponse:
    def __init__(self, payload, *, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None
