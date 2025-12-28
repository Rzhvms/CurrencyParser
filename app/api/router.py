from typing import List, Optional

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from app.db.database import get_db
from app.models.item_model import ItemModel
from app.models.pydantic_item_dto import Item, ItemCreate, ItemUpdate
from app.tasks.task_poller import Poller
from app.ws.ws_handler import manager

router = APIRouter()


# CRUD helpers
def _validate_item_create(data: ItemCreate) -> None:
    """Базовая бизнес-валидация при создании Item."""
    currency = data.currency.strip()
    if not currency:
        raise HTTPException(status_code=400, detail="Код не должен быть пустым")
    if not currency.isalpha():
        raise HTTPException(status_code=400, detail="Код может содержать только буквы")

    if data.rate < 0:
        raise HTTPException(status_code=400, detail="Цена не может быть отрицательной")

    if data.amount < 1:
        raise HTTPException(status_code=400, detail="Номинал должен быть >= 1")


async def _get_item_or_404(db: AsyncSession, item_id: int) -> ItemModel:
    """Получить Item или выбросить 404."""
    item = await db.get(ItemModel, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Элемент не найден")
    return item


async def _broadcast(event_type: str, payload: dict) -> None:
    """Единая точка отправки WS-событий."""
    try:
        await manager.broadcast({"type": event_type, **payload})
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Ошибка при отправке сообщения по WebSocket",
        )


# REST endpoints
@router.get("/items", response_model=List[Item])
async def list_items(db: AsyncSession = Depends(get_db)):
    """Вернуть список всех элементов."""
    result = await db.execute(select(ItemModel))
    return result.scalars().all()


@router.get("/items/{item_id}", response_model=Item)
async def retrieve_item(item_id: int, db: AsyncSession = Depends(get_db)):
    """Вернуть элемент по id."""
    return await _get_item_or_404(db, item_id)


@router.post("/items", response_model=Item, status_code=status.HTTP_201_CREATED)
async def create_item(item: ItemCreate, db: AsyncSession = Depends(get_db)):
    # Валидация кода
    currency = item.currency.strip().upper()
    if not currency.isalpha():
        raise HTTPException(status_code=400, detail="Код может содержать только буквы")

    # Проверка в базе перед добавлением
    existing = await db.execute(select(ItemModel).where(ItemModel.currency == currency))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Элемент с таким currency уже существует")

    model = ItemModel(
        currency=currency,
        rate=item.rate,
        amount=item.amount,
        platform=item.platform,
        crypto_currency=item.crypto_currency
    )

    db.add(model)
    await db.commit()
    await db.refresh(model)

    return model


@router.patch("/items/{item_id}", response_model=Item)
async def update_item_partial(
    item_id: int,
    patch: ItemUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Частичное обновление элемента."""
    item = await _get_item_or_404(db, item_id)

    updated_fields = patch.model_dump(exclude_unset=True)
    if not updated_fields:
        return item

    for field, value in updated_fields.items():
        setattr(item, field, value)

    await db.commit()
    await db.refresh(item)

    await _broadcast("updated", {"item": jsonable_encoder(item)})
    return item


@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_item(item_id: int, db: AsyncSession = Depends(get_db)):
    """Удаление элемента."""
    item = await _get_item_or_404(db, item_id)

    await db.delete(item)
    await db.commit()

    await _broadcast("deleted", {"id": item_id})


# WebSocket
@router.websocket("/ws/items")
async def items_ws(ws: WebSocket):
    """WebSocket для пуш-обновлений items."""
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()  # keep-alive / ping
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print("WS error:", exc, flush=True)
    finally:
        manager.disconnect(ws)


# Background tasks
@router.post("/tasks/run", status_code=status.HTTP_202_ACCEPTED)
async def run_poller_once(request: Request):
    """Ручной запуск poller-задачи."""
    poller: Optional[Poller] = getattr(request.app.state, "poller", None)

    if poller is None:
        raise HTTPException(500, "Фоновая задача не запущена")

    try:
        await poller.run_once()
    except Exception:
        raise HTTPException(500, "Ошибка при выполнении задач")

    return {"status": "ok"}