import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

import httpx
from prisma import Prisma

from .retry import compute_delay

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10.0
MAX_CONCURRENCY = 20

_semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
_http_client: httpx.AsyncClient | None = None


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT)
    return _http_client


async def close_http_client() -> None:
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None


def reset_http_client(client: httpx.AsyncClient | None = None) -> None:
    """For testing: inject a mock-friendly client."""
    global _http_client
    _http_client = client


async def deliver(task_id: int, db: Prisma) -> None:
    async with _semaphore:
        task = await db.notificationtask.find_unique(where={"id": task_id})
        if task is None or task.status == "DELIVERED":
            return

        client = get_http_client()
        headers = json.loads(task.headers) if task.headers else {}
        status_code: int | None = None
        error: str | None = None

        try:
            response = await client.post(
                task.targetUrl,
                headers=headers,
                content=task.body,
            )
            status_code = response.status_code
        except Exception as e:
            error = str(e)[:500]

        new_attempt_count = task.attemptCount + 1

        if status_code == 200:
            await db.notificationtask.update(
                where={"id": task_id},
                data={
                    "status": "DELIVERED",
                    "attemptCount": new_attempt_count,
                    "lastStatusCode": status_code,
                    "lastError": None,
                },
            )
            logger.info("Task %d delivered successfully", task_id)
            return

        if error is None:
            error = f"HTTP {status_code}"

        if new_attempt_count >= task.maxAttempts:
            await db.notificationtask.update(
                where={"id": task_id},
                data={
                    "status": "FAILED",
                    "attemptCount": new_attempt_count,
                    "lastStatusCode": status_code,
                    "lastError": error,
                },
            )
            logger.warning("Task %d failed after %d attempts", task_id, new_attempt_count)
            return

        delay = compute_delay(new_attempt_count)
        next_attempt = datetime.now(timezone.utc) + timedelta(seconds=delay)

        await db.notificationtask.update(
            where={"id": task_id},
            data={
                "status": "RETRYABLE",
                "attemptCount": new_attempt_count,
                "lastStatusCode": status_code,
                "lastError": error,
                "nextAttemptAt": next_attempt,
            },
        )
        logger.info("Task %d retry #%d in %.1fs", task_id, new_attempt_count, delay)

        loop = asyncio.get_event_loop()
        loop.call_later(delay, lambda: asyncio.ensure_future(deliver(task_id, db)))
