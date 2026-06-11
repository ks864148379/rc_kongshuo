import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prisma import Prisma

from .api.notifications import router
from .services.delivery import close_http_client
from .services.dispatcher import recover_pending_tasks

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Prisma()
    await db.connect()
    app.state.db = db

    await recover_pending_tasks(db)

    yield

    await close_http_client()
    await db.disconnect()


app = FastAPI(title="Notification Relay", lifespan=lifespan)
app.include_router(router)
