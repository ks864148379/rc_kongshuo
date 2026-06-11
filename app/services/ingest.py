import json

from prisma import Prisma

from ..models.schemas import NotificationRequest, NotificationResponse
from .dispatcher import schedule_task


async def submit_notification(
    request: NotificationRequest, db: Prisma
) -> tuple[NotificationResponse, bool]:
    """Returns (response, is_new). is_new=False means idempotent duplicate."""
    existing = await db.notificationtask.find_unique(
        where={"idempotencyKey": request.idempotency_key}
    )
    if existing:
        return NotificationResponse(
            id=existing.id,
            idempotency_key=existing.idempotencyKey,
            status=existing.status,
        ), False

    task = await db.notificationtask.create(
        data={
            "idempotencyKey": request.idempotency_key,
            "targetUrl": request.target_url,
            "headers": json.dumps(request.headers),
            "body": request.body,
            "maxAttempts": request.max_attempts,
        }
    )

    schedule_task(task.id, db, delay=0)

    return NotificationResponse(
        id=task.id,
        idempotency_key=task.idempotencyKey,
        status=task.status,
    ), True
