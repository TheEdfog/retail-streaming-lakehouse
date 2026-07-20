from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderStatus(StrEnum):
    CREATED = "created"
    PAID = "paid"
    CANCELLED = "cancelled"


class OrderEvent(BaseModel):
    """Versioned event accepted by the public Kafka topic."""

    model_config = ConfigDict(str_strip_whitespace=True)

    event_id: UUID
    event_time: datetime
    order_id: UUID
    customer_id: UUID
    store_id: int = Field(gt=0)
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    status: OrderStatus
    schema_version: int = Field(default=1, ge=1)

    @field_validator("event_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event_time must include a timezone")
        return value.astimezone(UTC)
