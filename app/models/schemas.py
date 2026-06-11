from pydantic import BaseModel


class NotificationRequest(BaseModel):
    idempotency_key: str
    target_url: str
    headers: dict[str, str] = {}
    body: str | None = None
    max_attempts: int = 5


class NotificationResponse(BaseModel):
    id: int
    idempotency_key: str
    status: str
