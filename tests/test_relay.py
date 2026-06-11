"""
End-to-end tests for the notification relay service.

Run with: python3 -m pytest tests/test_relay.py -v
"""

import asyncio
import json

import httpx
import pytest
import pytest_asyncio
import respx
from httpx import ASGITransport, AsyncClient, Response
from prisma import Prisma

from app.main import app
from app.services.delivery import reset_http_client


@pytest_asyncio.fixture
async def db():
    prisma = Prisma()
    await prisma.connect()
    await prisma.notificationtask.delete_many()
    yield prisma
    await prisma.notificationtask.delete_many()
    await prisma.disconnect()


@pytest_asyncio.fixture
async def client(db: Prisma):
    app.state.db = db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def mock_delivery():
    """
    Creates a dedicated respx router for the delivery HTTP client only.
    Prisma uses its own internal httpx client which is NOT affected.
    """
    router = respx.Router(assert_all_called=False)
    mock_transport = httpx.MockTransport(router.async_handler)
    mock_client = httpx.AsyncClient(transport=mock_transport)
    reset_http_client(mock_client)
    yield router
    await mock_client.aclose()
    reset_http_client(None)


# ============================================================
# 1. 基本接入测试
# ============================================================


async def test_submit_returns_202(client: AsyncClient):
    """提交通知应返回 202 + PENDING 状态"""
    resp = await client.post("/api/v1/notifications", json={
        "idempotency_key": "test-submit-001",
        "target_url": "https://example.com/callback",
        "headers": {"X-Token": "abc"},
        "body": '{"event": "test"}',
    })
    assert resp.status_code == 202
    data = resp.json()
    assert data["idempotency_key"] == "test-submit-001"
    assert data["status"] == "PENDING"
    assert "id" in data


async def test_idempotency_duplicate_returns_409(client: AsyncClient):
    """重复提交同一个幂等键应返回 409"""
    payload = {
        "idempotency_key": "test-idem-001",
        "target_url": "https://example.com/callback",
    }
    resp1 = await client.post("/api/v1/notifications", json=payload)
    assert resp1.status_code == 202

    resp2 = await client.post("/api/v1/notifications", json=payload)
    assert resp2.status_code == 409


async def test_query_existing_task(client: AsyncClient):
    """查询已提交的通知"""
    await client.post("/api/v1/notifications", json={
        "idempotency_key": "test-query-001",
        "target_url": "https://example.com/callback",
    })

    resp = await client.get("/api/v1/notifications/test-query-001")
    assert resp.status_code == 200
    data = resp.json()
    assert data["idempotency_key"] == "test-query-001"
    assert "status" in data


async def test_query_not_found(client: AsyncClient):
    """查询不存在的通知应返回 404"""
    resp = await client.get("/api/v1/notifications/nonexistent-key")
    assert resp.status_code == 404


# ============================================================
# 2. 投递成功测试
# ============================================================


async def test_delivery_success(client: AsyncClient, db: Prisma, mock_delivery):
    """目标返回 200 → 任务标记 DELIVERED"""
    mock_delivery.post("https://vendor.example.com/hook").mock(return_value=Response(200))

    resp = await client.post("/api/v1/notifications", json={
        "idempotency_key": "test-success-001",
        "target_url": "https://vendor.example.com/hook",
        "headers": {"Authorization": "Bearer token123"},
        "body": '{"userId": "u001"}',
    })
    assert resp.status_code == 202

    await asyncio.sleep(1)

    task = await db.notificationtask.find_unique(
        where={"idempotencyKey": "test-success-001"}
    )
    assert task is not None
    assert task.status == "DELIVERED"
    assert task.attemptCount == 1
    assert task.lastStatusCode == 200


async def test_delivery_sends_correct_headers_and_body(client: AsyncClient, db: Prisma, mock_delivery):
    """验证转发的 headers 和 body 与提交时一致"""
    captured_requests = []

    def capture(request):
        captured_requests.append(request)
        return Response(200)

    mock_delivery.post("https://vendor.example.com/capture").mock(side_effect=capture)

    await client.post("/api/v1/notifications", json={
        "idempotency_key": "test-capture-001",
        "target_url": "https://vendor.example.com/capture",
        "headers": {"X-Custom": "my-value", "Authorization": "Bearer secret"},
        "body": '{"order_id": "ORD-999"}',
    })

    await asyncio.sleep(1)

    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert req.headers["X-Custom"] == "my-value"
    assert req.headers["Authorization"] == "Bearer secret"
    assert req.content == b'{"order_id": "ORD-999"}'


# ============================================================
# 3. 重试测试
# ============================================================


async def test_retry_on_500(client: AsyncClient, db: Prisma, mock_delivery):
    """目标返回 500 → 自动重试，状态变为 RETRYABLE"""
    mock_delivery.post("https://vendor.example.com/fail").mock(return_value=Response(500))

    await client.post("/api/v1/notifications", json={
        "idempotency_key": "test-retry-500",
        "target_url": "https://vendor.example.com/fail",
        "max_attempts": 3,
    })

    await asyncio.sleep(0.5)

    task = await db.notificationtask.find_unique(
        where={"idempotencyKey": "test-retry-500"}
    )
    assert task is not None
    assert task.status == "RETRYABLE"
    assert task.attemptCount == 1
    assert task.lastStatusCode == 500


async def test_retry_on_network_error(client: AsyncClient, db: Prisma, mock_delivery):
    """网络异常 → 自动重试"""
    mock_delivery.post("https://vendor.example.com/down").mock(
        side_effect=httpx.ConnectError("Connection refused")
    )

    await client.post("/api/v1/notifications", json={
        "idempotency_key": "test-retry-network",
        "target_url": "https://vendor.example.com/down",
        "max_attempts": 3,
    })

    await asyncio.sleep(0.5)

    task = await db.notificationtask.find_unique(
        where={"idempotencyKey": "test-retry-network"}
    )
    assert task is not None
    assert task.status == "RETRYABLE"
    assert task.attemptCount == 1
    assert task.lastError is not None


async def test_max_attempts_then_failed(client: AsyncClient, db: Prisma, mock_delivery):
    """达到最大重试次数 → 标记 FAILED"""
    mock_delivery.post("https://vendor.example.com/always-fail").mock(
        return_value=Response(503)
    )

    await client.post("/api/v1/notifications", json={
        "idempotency_key": "test-maxretry-001",
        "target_url": "https://vendor.example.com/always-fail",
        "max_attempts": 2,
    })

    # attempt 1 立即 + attempt 2 约 1s 退避
    await asyncio.sleep(3)

    task = await db.notificationtask.find_unique(
        where={"idempotencyKey": "test-maxretry-001"}
    )
    assert task is not None
    assert task.status == "FAILED"
    assert task.attemptCount == 2
    assert task.lastStatusCode == 503


async def test_retry_then_success(client: AsyncClient, db: Prisma, mock_delivery):
    """先失败后成功：第一次 500，第二次 200"""
    route = mock_delivery.post("https://vendor.example.com/flaky")
    route.side_effect = [Response(500), Response(200)]

    await client.post("/api/v1/notifications", json={
        "idempotency_key": "test-flaky-001",
        "target_url": "https://vendor.example.com/flaky",
        "max_attempts": 3,
    })

    # 第一次失败 + 退避 ~1s + 第二次成功
    await asyncio.sleep(3)

    task = await db.notificationtask.find_unique(
        where={"idempotencyKey": "test-flaky-001"}
    )
    assert task is not None
    assert task.status == "DELIVERED"
    assert task.attemptCount == 2


# ============================================================
# 4. 持久化 + 恢复测试
# ============================================================


async def test_task_persisted_to_db(client: AsyncClient, db: Prisma):
    """提交后任务应立即持久化到数据库"""
    await client.post("/api/v1/notifications", json={
        "idempotency_key": "test-persist-001",
        "target_url": "https://example.com/hook",
        "body": '{"data": 1}',
        "max_attempts": 10,
    })

    task = await db.notificationtask.find_unique(
        where={"idempotencyKey": "test-persist-001"}
    )
    assert task is not None
    assert task.targetUrl == "https://example.com/hook"
    assert task.body == '{"data": 1}'
    assert task.maxAttempts == 10


async def test_recovery_loads_pending_tasks(db: Prisma):
    """启动恢复器应加载所有 PENDING/RETRYABLE 任务"""
    from app.services.dispatcher import recover_pending_tasks

    await db.notificationtask.create(data={
        "idempotencyKey": "test-recovery-001",
        "targetUrl": "https://example.com/hook",
        "status": "RETRYABLE",
        "attemptCount": 2,
        "maxAttempts": 5,
    })

    await db.notificationtask.create(data={
        "idempotencyKey": "test-recovery-002",
        "targetUrl": "https://example.com/hook",
        "status": "DELIVERED",
        "attemptCount": 1,
        "maxAttempts": 5,
    })

    count = await recover_pending_tasks(db)
    assert count == 1
