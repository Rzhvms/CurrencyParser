import asyncio
import json
from typing import Any, Dict, List, Optional

from nats.aio.client import Client as NATS


class NatsPublisher:
    """
    Безопасный async-publisher для NATS.
    """

    def __init__(self, servers: List[str]) -> None:
        self._servers = servers
        self._client: Optional[NATS] = None
        self._lock = asyncio.Lock()


    # Connection lifecycle
    async def connect(self) -> None:
        """Подключение к NATS (idempotent)."""
        async with self._lock:
            if self._client and self._client.is_connected:
                return

            client = NATS()
            await client.connect(servers=self._servers)
            self._client = client

            print(
                f"NATS publisher подключен: servers={self._servers}",
                flush=True,
            )

    async def close(self) -> None:
        """Корректное закрытие соединения с NATS."""
        async with self._lock:
            if not self._client:
                return

            try:
                if self._client.is_connected:
                    await self._client.drain()
            except Exception as exc:
                print(f"Ошибка drain NATS: {exc}", flush=True)

            try:
                await self._client.close()
            except Exception as exc:
                print(f"Ошибка close NATS: {exc}", flush=True)

            self._client = None
            print("NATS publisher отключен", flush=True)

    # Publish
    async def publish(self, subject: str, message: Dict[str, Any]) -> None:
        """
        Публикация сообщения в NATS.

        :param subject: topic / subject
        :param message: JSON-сериализуемый dict
        """
        if not self._client or not self._client.is_connected:
            await self.connect()

        assert self._client is not None

        payload = json.dumps(message, ensure_ascii=False).encode("utf-8")

        try:
            await self._client.publish(subject, payload)
            # flush не обязателен, но полезен при low-latency событиях
            await self._client.flush(timeout=1)
        except Exception as exc:
            print(
                f"Ошибка публикации в NATS: subject={subject}, error={exc}",
                flush=True,
            )
            raise