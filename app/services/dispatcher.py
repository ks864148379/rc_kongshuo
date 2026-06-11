import asyncio
import logging
from datetime import datetime, timezone

from prisma import Prisma

from .delivery import deliver

logger = logging.getLogger(__name__)


def schedule_task(task_id: int, db: Prisma, delay: float = 0) -> None:
    if delay <= 0:
        asyncio.ensure_future(deliver(task_id, db))
    else:
        loop = asyncio.get_event_loop()
        loop.call_later(delay, lambda: asyncio.ensure_future(deliver(task_id, db)))


async def recover_pending_tasks(db: Prisma) -> int:
    tasks = await db.notificationtask.find_many(
        where={"status": {"in": ["PENDING", "RETRYABLE"]}}
    )
    now = datetime.now(timezone.utc)
    count = 0
    for task in tasks:
        next_at = task.nextAttemptAt.replace(tzinfo=timezone.utc) if task.nextAttemptAt.tzinfo is None else task.nextAttemptAt
        delay = max(0, (next_at - now).total_seconds())
        schedule_task(task.id, db, delay)
        count += 1
    logger.info("Recovered %d pending tasks", count)
    return count
