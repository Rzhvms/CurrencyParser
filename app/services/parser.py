import asyncio
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date
from typing import Dict, Tuple

from sqlalchemy import select

from app.config import (
    CBR_URL,
    USER_AGENT,
    CURRENCY,
    CBR_PLATFORM,
    POLL_INTERVAL_SECONDS,
    CRYPTO_CODES,
    CRYPTO_PLATFORM,
    BINANCE_URL,
    DEFAULT_TIMEOUT,
)
from app.models.item_model import ItemModel
from app.ws.ws_handler import manager


# In-memory cache: date -> rates
_rates_cache: Dict[str, Dict[str, Dict]] = {}


# Utils

def format_cbr_date(dt: date) -> str:
    """Формат даты для Центробанк"""
    return dt.strftime("%d/%m/%Y")


# CBR

def parse_cbr_xml(xml: bytes, allowed_codes: Tuple[str, ...]) -> Dict[str, Dict]:
    """
    Парсит XML ЦБ и возвращает словарь курсов:
    { "USD": {rate, amount, platform}, ... }
    """
    result: Dict[str, Dict] = {}

    try:
        root = ET.fromstring(xml)
    except Exception:
        return result

    for valute in root.findall(".//Valute"):
        code_node = valute.find("CharCode")
        if code_node is None or not code_node.text:
            continue

        currency = code_node.text.strip()
        if currency not in allowed_codes:
            continue

        value_node = valute.find("Value") or valute.find("VunitRate")
        amount_node = valute.find("amount")

        try:
            value = float(value_node.text.replace(",", "."))
            amount = int(amount_node.text) if amount_node is not None else 1
        except Exception:
            continue

        result[currency] = {
            "rate": value / amount,
            "amount": amount,
            "platform": CBR_PLATFORM,
        }

    return result


def fetch_cbr_rates_sync(dt: date) -> Dict[str, Dict]:
    """
    Синхронно запрашивает курсы ЦБ (используется через asyncio.to_thread)
    """
    date_key = format_cbr_date(dt)
    if date_key in _rates_cache:
        return _rates_cache[date_key]

    url = CBR_URL.format(date=date_key)
    headers = {"User-Agent": USER_AGENT}

    try:
        resp = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        print(f"Ошибка запроса ЦБ: {exc}", flush=True)
        fallback = {
            "RUB": {"rate": 80.0, "amount": 1, "platform": CBR_PLATFORM}
        }
        _rates_cache[date_key] = fallback
        return fallback

    rates = {
        "RUB": {"rate": 1.0, "amount": 1, "platform": CBR_PLATFORM}
    }
    rates.update(parse_cbr_xml(resp.content, CURRENCY))

    _rates_cache[date_key] = rates
    return rates


# Crypto
def fetch_crypto_rates_sync(
    symbols: Tuple[str, ...],
    usd_rub_rate: float,
) -> Dict[str, Dict]:
    """
    Получает курсы крипты с Binance
    """
    result: Dict[str, Dict] = {}
    headers = {"User-Agent": USER_AGENT}

    for symbol in symbols:
        pair = f"{symbol}USDT"

        try:
            resp = requests.get(
                BINANCE_URL,
                headers=headers,
                params={"symbol": pair},
                timeout=DEFAULT_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            price_usdt = float(data["price"])
        except Exception as exc:
            print(f"Ошибка Binance {pair}: {exc}", flush=True)
            continue

        result[symbol] = {
            "rate": price_usdt * usd_rub_rate,
            "amount": 1,
            "platform": CRYPTO_PLATFORM or "binance",
        }

    return result


# DB
async def upsert_item(db, currency: str, data: Dict):
    """
    Создаёт или обновляет ItemModel
    """
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

        await broadcast_event("created", item)
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

    await broadcast_event("updated", item)


# WS
async def broadcast_event(event_type: str, item: ItemModel):
    try:
        await manager.broadcast({
            "type": event_type,
            "item": {
                "currency": item.currency,
                "rate": item.rate,
                "amount": item.amount,
                "platform": item.platform,
                "crypto_currency": item.crypto_currency,
            }
        })
    except Exception:
        print(f"WS broadcast failed ({event_type})", flush=True)


# Poll loop

async def poll_loop(app):
    """
    Основной polling-цикл
    """
    while True:
        try:
            today = datetime.utcnow().date()

            cbr_rates = await asyncio.to_thread(
                fetch_cbr_rates_sync,
                today,
            )

            usd_rate = cbr_rates.get("USD", {}).get("rate", 80.0)

            crypto_rates = await asyncio.to_thread(
                fetch_crypto_rates_sync,
                CRYPTO_CODES,
                usd_rate,
            )

            all_rates = {**cbr_rates, **crypto_rates}

            async with app.state.db() as db:
                for currency, data in all_rates.items():
                    await upsert_item(db, currency, data)

            await asyncio.sleep(POLL_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"Ошибка poll_loop: {exc}", flush=True)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)