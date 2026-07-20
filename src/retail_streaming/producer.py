import argparse
import time

from confluent_kafka import Producer

from retail_streaming.generator import make_order_event


def delivery_report(error, message) -> None:
    if error:
        raise RuntimeError(f"Kafka delivery failed: {error}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish sample retail order events")
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="retail.orders.v1")
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--interval", type=float, default=0.05)
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
        }
    )
    for _ in range(args.events):
        event = make_order_event()
        producer.produce(
            args.topic,
            key=str(event.order_id),
            value=event.model_dump_json(),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        time.sleep(args.interval)
    producer.flush(10)


if __name__ == "__main__":
    main()
