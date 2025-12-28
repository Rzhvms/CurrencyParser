from typing import Any, cast
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import SQLModel
from app.api import router
from app.config import NATS_SERVERS
from app.db.database import engine
from app.nats.nats_pub import NatsPublisher
from app.nats.nats_sub import NatsSubscriber
from app.tasks.task_poller import Poller

app = FastAPI(title="parser", version="1.0")

# Подключаем роуты
app.include_router(router.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

app_state = cast(Any, getattr(app, "state"))


# Startup
@app.on_event("startup")
async def on_startup():
    # Создаём таблицы
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    # NATS Publisher
    publisher = NatsPublisher(servers=NATS_SERVERS)
    await publisher.connect()
    app_state.nats_publisher = publisher

    # NATS Subscriber
    subscriber = NatsSubscriber(servers=NATS_SERVERS)
    await subscriber.connect()
    app_state.nats_subscriber = subscriber

    # Poller
    poller = Poller(nats=publisher)
    poller.start()
    app_state.poller = poller


# Shutdown
@app.on_event("shutdown")
async def on_shutdown():
    # Graceful stop Poller
    poller: Poller | None = getattr(app_state, "poller", None)
    if poller is not None:
        await poller.stop()

    # Закрываем NATS Publisher
    publisher: NatsPublisher | None = getattr(app_state, "nats_publisher", None)
    if publisher is not None:
        await publisher.close()