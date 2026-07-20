# Retail Streaming Lakehouse

This project is a compact example of an event pipeline that can be replayed and inspected. Retail order events enter Kafka, Spark Structured Streaming validates and deduplicates them, and accepted rows are appended to an Iceberg table in S3-compatible storage.

The repository focuses on the parts that tend to cause incidents in real pipelines: duplicate delivery, late events, malformed payloads, checkpoint recovery and traceability back to a Kafka offset.

## Flow

```text
order producer -> Redpanda/Kafka -> Spark Structured Streaming
                                      |            |
                                      |            +-> quarantine JSON
                                      +-> Iceberg on MinIO -> Trino SQL
                                             |
                                             +-> Nessie catalog
```

The event model uses explicit identifiers and event time. The streaming job keeps Kafka topic, partition and offset in the table, applies a ten-minute watermark and deduplicates by `event_id`. Invalid JSON is written to a separate replayable path rather than silently discarded.

## Run locally

Start the small infrastructure layer:

```bash
docker compose up -d
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,spark]"
python -m retail_streaming.producer --events 1000
```

Run Spark with the Kafka, Iceberg, Nessie and S3 connector packages that match your Spark distribution. The job entry point is:

```bash
python -m retail_streaming.stream_job
```

Connector versions are intentionally not hidden inside the Python package because Spark and cluster images must use a compatible set. In a deployed environment they belong in the image build or `spark-submit --packages` configuration.

The Redpanda console is available at <http://localhost:18080>, MinIO at <http://localhost:19001>, the Nessie API at <http://localhost:19120>, and Trino at <http://localhost:18082>.

## Design decisions

- Idempotent Kafka production and stable order keys preserve per-order ordering.
- Event time is timezone-aware and separate from Kafka ingestion time.
- Watermarking bounds state while allowing normal late arrival.
- `event_id` handles at-least-once delivery without treating legitimate order updates as duplicates.
- Source offsets remain in Iceberg for investigation and targeted replay.
- The local password in Compose is a development-only credential.

## Tests

```bash
pip install -e ".[dev]"
ruff check .
pytest -q
docker compose config --quiet
```

Tests cover the event contract without requiring Kafka or downloading Spark images. A full integration run is deliberately separate because it pulls several large JVM and container dependencies.

## Scope

This is a local engineering case study, not a production cluster definition. Production work would add a durable Nessie database, encrypted secrets, Kafka ACLs, metrics, table maintenance, retention policies and a tested Spark image.

The architecture was informed by the public Iceberg, Spark, Trino, Nessie and Redpanda documentation and by open examples such as `danthelion/trino-minio-iceberg-example`. No source code was copied from those projects.
