import asyncio
from types import SimpleNamespace

import pytest

from core.bounded_workers import map_bounded
from core.data_models import Tweet
from processors.document_factory import DocumentFactory


def test_map_bounded_limits_concurrent_workers():
    async def run():
        active = 0
        max_active = 0

        async def worker(item: int) -> int:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return item * 2

        result = await map_bounded(list(range(6)), worker, concurrency=2)
        return result, max_active

    result, max_active = asyncio.run(run())

    assert result == [0, 2, 4, 6, 8, 10]
    assert max_active == 2


def test_map_bounded_stops_starting_work_when_cancel_event_is_set():
    async def run():
        started = []
        cancel_event = asyncio.Event()
        release = asyncio.Event()

        async def worker(item: int) -> int:
            started.append(item)
            if len(started) == 2:
                cancel_event.set()
            await release.wait()
            return item

        task = asyncio.create_task(
            map_bounded(
                [0, 1, 2, 3],
                worker,
                concurrency=2,
                cancel_event=cancel_event,
            )
        )
        while len(started) < 2:
            await asyncio.sleep(0)
        release.set()
        return await task, started

    result, started = asyncio.run(run())

    assert result == [0, 1]
    assert started == [0, 1]


def test_map_bounded_continues_sibling_items_after_worker_exception():
    async def run():
        started = []

        async def worker(item: int) -> int:
            started.append(item)
            await asyncio.sleep(0)
            if item == 1:
                raise RuntimeError("item failed")
            return item

        with pytest.raises(RuntimeError, match="item failed"):
            await map_bounded([0, 1, 2, 3], worker, concurrency=2)
        return started

    started = asyncio.run(run())

    assert sorted(started) == [0, 1, 2, 3]


def test_document_factory_async_downloads_use_bounded_workers(monkeypatch):
    factory = object.__new__(DocumentFactory)
    factory.concurrent_workers = 2
    factory.arxiv_processor = SimpleNamespace(
        extract_urls_from_tweet=lambda tweet: [],
    )
    factory.pdf_processor = SimpleNamespace(
        extract_urls_from_tweet=lambda tweet: [
            "https://example.test/a.pdf",
            "https://example.test/b.pdf",
            "https://example.test/c.pdf",
        ],
    )
    factory.readme_processor = SimpleNamespace(
        extract_urls_from_tweet=lambda tweet: [],
    )

    async def run():
        active = 0
        max_active = 0

        async def fake_download(processor, url, tweet_id, resume):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return SimpleNamespace(url=url)

        monkeypatch.setattr(factory, "_download_document_async", fake_download)
        tweet = Tweet(
            id="tweet-1",
            full_text="pdfs",
            created_at="2026-04-04T00:00:00Z",
            screen_name="alice",
            name="Alice",
        )
        results = await factory.process_single_tweet_async(tweet)
        return results, max_active, tweet

    results, max_active, tweet = asyncio.run(run())

    assert [item.url for item in results["pdf"]] == [
        "https://example.test/a.pdf",
        "https://example.test/b.pdf",
        "https://example.test/c.pdf",
    ]
    assert [item.url for item in tweet.pdf_links] == [
        "https://example.test/a.pdf",
        "https://example.test/b.pdf",
        "https://example.test/c.pdf",
    ]
    assert max_active == 2
