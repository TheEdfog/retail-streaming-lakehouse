from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from retail_streaming.models import OrderEvent, OrderStatus


def event_data() -> dict:
    return {
        "event_id": uuid4(),
        "event_time": datetime.now(UTC),
        "order_id": uuid4(),
        "customer_id": uuid4(),
        "store_id": 3,
        "amount": Decimal("1250.50"),
        "currency": "RUB",
        "status": OrderStatus.PAID,
    }


def test_valid_event_is_normalized_to_utc() -> None:
    event = OrderEvent(**event_data())
    assert event.event_time.tzinfo == UTC
    assert event.schema_version == 1


@pytest.mark.parametrize("field,value", [("amount", 0), ("currency", "rub"), ("store_id", 0)])
def test_invalid_business_fields_are_rejected(field: str, value: object) -> None:
    payload = event_data()
    payload[field] = value
    with pytest.raises(ValidationError):
        OrderEvent(**payload)
