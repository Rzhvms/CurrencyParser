import asyncio
from datetime import datetime
from typing import Dict, Tuple

import httpx
from sqlalchemy import select

from app.config import (
    CBR_URL,
    BINANCE_URL,
    USER_AGENT,
    CURRENCY,
    CBR_PLATFORM,
    POLL_INTERVAL_SECONDS,
    CRYPTO_CODES,
    CRYPTO_PLATFORM,
    DEFAULT_TIMEOUT,
)
from app.db.database import AsyncSessionLocal
from app.models.item_model import ItemModel
from app.nats.nats_pub import NatsPublisher
from app.services.parser import parse_cbr_xml
from app.ws.ws_handler import manager


# HTTP

async def fetch_cbr(client: httpx.AsyncClient, wanted: Tuple[str, ...]) -> Dict[str, Dict]:
    try:
        resp = await client.get(CBR_URL)
        resp.raise_for_status()
    except Exception as exc:
        print(f"Ошибка ЦБ: {exc}", flush=True)
        return {
            "RUB": {"rate": 1.0, "amount": 1, "platform": CBR_PLATFORM}
        }

    rates = {
        "RUB": {"rate": 1.0, "amount": 1, "platform": CBR_PLATFORM}
    }
    rates.update(parse_cbr_xml(resp.content, wanted))
    return rates


async def fetch_binance(
    client: httpx.AsyncClient,
    symbols: Tuple[str, ...],
    usd_rub: float,
) -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}

    for sym in symbols:
        pair = f"{sym}USDT"
        try:
            resp = await client.get(BINANCE_URL, params={"symbol": pair})
            resp.raise_for_status()
            price = float(resp.json()["price"])
        except Exception as exc:
            print(f"Binance {pair}: {exc}", flush=True)
            continue

        result[sym] = {
            "rate": price * usd_rub,
            "amount": 1,
            "platform": CRYPTO_PLATFORM or "Binance",
        }

    return result


# EVENTS

async def emit_event(event_type: str, item: ItemModel, nats: NatsPublisher):
    payload = {
        "type": event_type,
        "item": {
            "currency": item.currency,
            "rate": item.rate,
            "amount": item.amount,
            "platform": item.platform,
            "crypto_currency": item.crypto_currency,
        },
    }

    try:
        await manager.broadcast(payload)
    except Exception:
        print("WS broadcast failed", flush=True)

    try:
        await nats.publish("items.updates", payload)
    except Exception:
        print("NATS publish failed", flush=True)


# DB

async def upsert_item(db, currency: str, data: Dict, nats: NatsPublisher):
    result = await db.execute(
        select(ItemModel).where(ItemModel.currency == currency)
    )
    item = result.scalar_one_or_none()

    crypto_currency = currency in CRYPTO_CODES

    if item is None:
        item = ItemModel(
            currency=currency,
            rate=data["rate"],
            amount=data.get("amount", 1),
            platform=data.get("platform", ""),
            crypto_currency=crypto_currency,
        )
        db.add(item)
        await db.commit()
        await db.refresh(item)
        await emit_event("created", item, nats)
        return

    if abs(item.rate - data["rate"]) < 1e-9:
        return

    item.rate = data["rate"]
    item.amount = data.get("amount", item.amount)
    item.platform = data.get("platform", item.platform)
    item.crypto_currency = crypto_currency
    item.last_updated_time = datetime.utcnow()

    await db.commit()
    await db.refresh(item)
    await emit_event("updated", item, nats)


# POLLER

class Poller:
    def __init__(self, nats: NatsPublisher):
        self.nats = nats
        self._task: asyncio.Task | None = None
        self._client = httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            timeout=DEFAULT_TIMEOUT,
        )

    async def run_once(self):
        rates = await fetch_cbr(self._client, CURRENCY)
        usd = rates.get("USD", {}).get("rate", 80.0)

        crypto = await fetch_binance(self._client, CRYPTO_CODES, usd)
        combined = {**rates, **crypto}

        async with AsyncSessionLocal() as db:
            for currency, data in combined.items():
                await upsert_item(db, currency, data, self.nats)

    async def _loop(self):
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"Poller error: {exc}", flush=True)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            await self._client.aclose()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None