from typing import Set
from fastapi import WebSocket
from fastapi.encoders import jsonable_encoder
import asyncio


class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()
        self._has_clients = asyncio.Event()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)
        self._has_clients.set()

        client_info = getattr(ws, "client", None)
        print(
            f"WebSocket подключен: client={client_info}, total={len(self.active)}, manager_id={id(self)}",
            flush=True
        )

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)
        print(
            f"WebSocket отключен: client={getattr(ws, 'client', None)}, total={len(self.active)}, manager_id={id(self)}",
            flush=True
        )
        if not self.active:
            self._has_clients.clear()

    async def broadcast(self, message: dict):
        if not self.active:
            return

        data = jsonable_encoder(message)
        bad_connections = []

        for ws in list(self.active):
            try:
                await ws.send_json(data)
            except Exception:
                bad_connections.append(ws)

        for ws in bad_connections:
            self.disconnect(ws)
            print(f"Удалён неработающий WebSocket: client={getattr(ws, 'client', None)}", flush=True)

    async def wait_for_clients(self, timeout: float | None = None) -> bool:
        """
        Опционально можно ждать пока хотя бы один клиент подключится
        """
        try:
            await asyncio.wait_for(self._has_clients.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False


# Singleton
manager = ConnectionManager()