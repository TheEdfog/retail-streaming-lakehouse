import random
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from retail_streaming.models import OrderEvent, OrderStatus


def make_order_event(rng: random.Random | None = None) -> OrderEvent:
    rng = rng or random.Random()
    return OrderEvent(
        event_id=uuid4(),
        event_time=datetime.now(UTC),
        order_id=uuid4(),
        customer_id=uuid4(),
        store_id=rng.randint(1, 25),
        amount=Decimal(rng.randint(100, 50_000)) / 100,
        currency="RUB",
        status=rng.choices(
            [OrderStatus.CREATED, OrderStatus.PAID, OrderStatus.CANCELLED],
            weights=[15, 80, 5],
        )[0],
    )
