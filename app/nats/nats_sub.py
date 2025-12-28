import asyncio
import json
from typing import Any, Dict, Optional

from nats.aio.client import Client as NATS
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import AsyncSessionLocal
from app.models.item_model import ItemModel
from app.ws.ws_handler import manager


class NatsSubscriber:
    """
    NATS subscriber для обработки событий items.updates.

    - Получает события из NATS
    - Синхронизирует данные с БД
    - Рассылает события по WebSocket
    """

    def __init__(self, servers: list[str]) -> None:
        self._servers = servers
        self._client: Optional[NATS] = None
        self._lock = asyncio.Lock()


    # Lifecycle
    async def connect(self) -> None:
        async with self._lock:
            if self._client and self._client.is_connected:
                return

            client = NATS()
            await client.connect(servers=self._servers)
            await client.subscribe("items.updates", cb=self._on_message)
            self._client = client

            print("NATS subscriber подключен", flush=True)

    async def close(self) -> None:
        async with self._lock:
            if not self._client:
                return

            try:
                await self._client.drain()
            except Exception as exc:
                print(f"Ошибка drain subscriber: {exc}", flush=True)

            try:
                await self._client.close()
            except Exception as exc:
                print(f"Ошибка close subscriber: {exc}", flush=True)

            self._client = None
            print("NATS subscriber отключен", flush=True)


    # Message handler
    async def _on_message(self, msg) -> None:
        """Обработчик сообщений из NATS."""
        try:
            data: Dict[str, Any] = json.loads(msg.data.decode("utf-8"))
        except Exception as exc:
            print(f"Ошибка парсинга NATS сообщения: {exc}", flush=True)
            return

        item_data = data.get("item")
        if not isinstance(item_data, dict):
            return

        currency = item_data.get("currency")
        if not currency:
            return

        async with AsyncSessionLocal() as db:
            await self._upsert_item(db, item_data)

        # Проброс события дальше по WS
        try:
            await manager.broadcast(data)
        except Exception as exc:
            print(f"WS broadcast error: {exc}", flush=True)


    # DB logic
    async def _upsert_item(self, db: AsyncSession, item: Dict[str, Any]) -> None:
        """Создание или обновление Item по currency."""
        result = await db.execute(
            select(ItemModel).where(ItemModel.currency == item["currency"])
        )
        model = result.scalar_one_or_none()

        if model is None:
            model = ItemModel(
                currency=item["currency"],
                rate=item.get("rate", 0),
                amount=item.get("amount", 1),
                platform=item.get("platform", ""),
                crypto_currency=item.get("crypto_currency", False),
            )
            db.add(model)
            action = "created"
        else:
            model.rate = item.get("rate", model.rate)
            model.amount = item.get("amount", model.amount)
            model.platform = item.get("platform", model.platform)
            model.crypto_currency = item.get("crypto_currency", model.crypto_currency)
            action = "updated"

        await db.commit()
        await db.refresh(model)

        print(f"Item {action} from NATS: currency={model.currency}", flush=True)