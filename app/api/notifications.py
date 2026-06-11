from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from ..models.schemas import NotificationRequest, NotificationResponse
from ..services.ingest import submit_notification

router = APIRouter()


@router.post("/api/v1/notifications", response_model=NotificationResponse, status_code=202)
async def create_notification(payload: NotificationRequest, request: Request):
    db = request.app.state.db
    result, is_new = await submit_notification(payload, db)
    if not is_new:
        return JSONResponse(status_code=409, content=result.model_dump())
    return result


@router.get("/api/v1/notifications/{idempotency_key}")
async def get_notification(idempotency_key: str, request: Request):
    db = request.app.state.db
    task = await db.notificationtask.find_unique(
        where={"idempotencyKey": idempotency_key}
    )
    if task is None:
        return JSONResponse(status_code=404, content={"error": "not found"})
    return {
        "id": task.id,
        "idempotency_key": task.idempotencyKey,
        "status": task.status,
        "attempt_count": task.attemptCount,
        "last_status_code": task.lastStatusCode,
        "created_at": task.createdAt.isoformat(),
    }
