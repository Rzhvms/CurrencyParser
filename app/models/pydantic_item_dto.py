from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, ConfigDict


# Base schemas
class ItemBase(BaseModel):
    """Общие поля Item, используемые в response и create."""

    currency: str = Field(..., description="Код элемента (USD, BTC и т.д.)")
    rate: float = Field(..., ge=0, description="Цена / курс (>= 0)")
    amount: int = Field(1, ge=1, description="Номинал (>= 1)")
    platform: str = Field(..., description="Источник данных")
    crypto_currency: bool = Field(False, description="Является ли криптовалютой")


    # Validators
    @field_validator("currency")
    @classmethod
    def validate_code(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("currency не должен быть пустым")
        if not v.isalpha():
            raise ValueError("currency может содержать только буквы")
        return v

    @field_validator("platform")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Поле не должно быть пустым")
        return v


# Create
class ItemCreate(ItemBase):
    """Schema для создания Item."""
    pass


# Response
class Item(ItemBase):
    """Schema ответа Item."""

    id: int = Field(..., description="ID элемента")
    last_updated_time: datetime = Field(..., description="Дата последнего обновления")

    model_config = ConfigDict(from_attributes=True)


# Update
class ItemUpdate(BaseModel):
    """Schema для частичного обновления Item."""
    rate: Optional[float] = Field(None, ge=0, description="Цена / курс")
    amount: Optional[int] = Field(None, ge=1, description="Номинал")
    platform: Optional[str] = Field(None, description="Источник")
    crypto_currency: Optional[bool] = Field(None, description="Криптовалюта")

    @field_validator("platform")
    @classmethod
    def validate_optional_non_empty(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if not v:
            raise ValueError("Поле не должно быть пустым")
        return v