"""Bounded async worker helpers for background processing."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")

_MISSING = object()


def resolve_worker_concurrency(
    value: object,
    *,
    default: int = 1,
    setting_name: str = "worker_concurrency",
) -> int:
    """Resolve and validate a positive worker concurrency setting."""
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError(f"{setting_name} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{setting_name} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{setting_name} must be a positive integer")
    return parsed


async def map_bounded(
    items: Sequence[T],
    worker: Callable[[T], Awaitable[R]],
    *,
    concurrency: int,
    cancel_event: asyncio.Event | None = None,
) -> list[R]:
    """Run an async worker over items with bounded concurrency.

    If ``cancel_event`` is set while a batch is running, active work is allowed
    to finish and no additional items are started. If the parent task is
    cancelled, active worker tasks are cancelled before the cancellation is
    propagated.
    """
    resolved_concurrency = resolve_worker_concurrency(
        concurrency,
        setting_name="concurrency",
    )
    if not items:
        return []

    queue: asyncio.Queue[tuple[int, T]] = asyncio.Queue()
    for index, item in enumerate(items):
        queue.put_nowait((index, item))

    results: list[object] = [_MISSING] * len(items)
    errors: list[tuple[int, Exception]] = []

    async def run_worker() -> None:
        while True:
            if cancel_event is not None and cancel_event.is_set():
                return
            try:
                index, item = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                if cancel_event is not None and cancel_event.is_set():
                    return
                results[index] = await worker(item)
            except Exception as exc:
                errors.append((index, exc))
            finally:
                queue.task_done()

    worker_count = min(resolved_concurrency, len(items))
    tasks = [asyncio.create_task(run_worker()) for _ in range(worker_count)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    if errors:
        errors.sort(key=lambda item: item[0])
        raise errors[0][1]

    return [item for item in results if item is not _MISSING]
