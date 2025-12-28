from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field
from sqlalchemy import Column, String, DateTime


class ItemModel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    currency: str = Field(
        sa_column=Column(String, nullable=False, unique=True)
    )
    rate: float
    amount: int
    platform: str
    crypto_currency: bool = False
    last_updated_time: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    )