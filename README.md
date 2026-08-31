# Real-Time Building Sensor Pipeline

[![CI](https://github.com/MuhammadAliacc/Realtime-Building-Sensor-Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/MuhammadAliacc/Realtime-Building-Sensor-Pipeline/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A streaming pipeline for building sensor telemetry (temperature, humidity, power draw). It ingests readings off a message bus, scores every reading for anomalies as it arrives, publishes alerts to a dedicated topic, keeps recent state in Postgres for querying, and archives the raw stream to S3-compatible object storage partitioned by date. A Streamlit dashboard gives a live operational view.

The repo ships with a load generator that simulates a sensor fleet, so `docker compose up` gives you a full working pipeline with data flowing through it in under a minute. Point the processor at a real broker and the load generator becomes unnecessary — the ingestion, scoring, storage, and alerting paths are unchanged.

## Architecture

```mermaid
flowchart LR
    P[Load generator<br/>simulated fleet] -->|Kafka topic<br/>building-sensor-readings| C[Stream Processor]
    C -->|rolling z-score<br/>per sensor+metric| C
    C -->|batched inserts<br/>at-least-once| PG[(Postgres<br/>readings + alerts)]
    C -->|raw JSON, date-partitioned| S3[(MinIO<br/>S3-compatible archive)]
    C -->|anomaly events| A[Kafka topic<br/>sensor-alerts]
    PG --> D[Streamlit Dashboard]
```

Kafka decouples ingestion from processing, so a slow or restarting consumer never drops readings. The processor scores each reading against a per-sensor, per-metric rolling window and only flags an anomaly once it has enough history to judge against — a cold sensor is never flagged on its first reading. Anomalies are published to their own topic so downstream consumers (paging, ticketing, a second analytics job) can react without touching this codebase, and are also written to Postgres for the dashboard. Every reading, anomalous or not, is archived to object storage as newline-delimited JSON under `readings/YYYY/MM/DD/` — the layout a Glue/Athena or Spark job would expect.

## Operational behaviour

- **Delivery semantics:** at-least-once. Kafka offsets are committed only after the corresponding batch is durably written to Postgres and object storage, so a crash mid-batch replays those readings rather than losing them. Writes are idempotent enough for a monitoring workload; the `readings` table is append-only.
- **Backpressure:** readings are buffered and flushed on whichever comes first — `BATCH_SIZE` rows or `BATCH_FLUSH_SECONDS`. Under load the pipeline trades latency for throughput via larger effective batches; when idle, the time trigger bounds staleness.
- **Startup:** the processor retries broker, database, and object-store connections with backoff, so `docker compose up` ordering and transient restarts don't require manual intervention. Compose still gates the app services on health checks.
- **Shutdown:** `SIGTERM`/`SIGINT` drains the in-flight batch, commits offsets, and closes clients before exiting, so `docker compose down` and orchestrator rollouts are clean.
- **Fault isolation:** a single malformed or unexpected message is logged and skipped; it does not stall the consumer.
- **Logging:** structured, level-controlled via `LOG_LEVEL`, one line per lifecycle event and per alert.

## Stack

Kafka in KRaft mode (no ZooKeeper) for the bus, Python (`kafka-python`) for the load generator and processor, Postgres for queryable recent state, MinIO for S3-compatible archival, Streamlit for the dashboard, Docker Compose to wire it together. Swapping MinIO for S3 and Kafka for MSK is configuration, not a rewrite.

## Running it

```
docker compose up --build
```

Brings up Kafka, Postgres, MinIO, the load generator, the processor, and the dashboard. Kafka's health check takes 20–30s before the app services start.

| Service | URL | Credentials |
|---|---|---|
| Dashboard | http://localhost:8501 | — |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Postgres | `localhost:5432` | `pipeline` / `pipeline` |

The load generator emits readings for 3 buildings × 2 sensors × 3 metrics every second and injects a spike about 2% of the time, so alerts appear in the dashboard within a few minutes.

## Configuration

All services read their configuration from the environment; `docker-compose.yml` sets working defaults. Copy `.env.example` to `.env` to override.

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | generator, processor |
| `SENSOR_TOPIC` | `building-sensor-readings` | generator, processor |
| `ALERT_TOPIC` | `sensor-alerts` | processor |
| `DATABASE_URL` | `postgresql://pipeline:pipeline@localhost:5432/sensor_pipeline` | processor, dashboard |
| `S3_ENDPOINT` / `S3_BUCKET` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` | MinIO local defaults | processor |
| `BATCH_SIZE` / `BATCH_FLUSH_SECONDS` | `50` / `5` | processor |
| `ANOMALY_WINDOW` / `ANOMALY_THRESHOLD` / `ANOMALY_MIN_SAMPLES` | `30` / `3.0` / `10` | processor |
| `EMIT_INTERVAL_SECONDS` / `ANOMALY_PROBABILITY` | `1.0` / `0.02` | generator |
| `LOG_LEVEL` | `INFO` | all |

## Anomaly detection

`RollingZScoreDetector` keeps a fixed-size window per `sensor:metric` key and flags a reading when its z-score against the *prior* readings in the window exceeds the threshold. The reading being scored is never part of its own baseline, and nothing is flagged until `min_samples` history exists. It holds no external state and does one pass over a bounded deque per message, so cost is constant per reading. This is the piece with real branching logic, so it carries the test suite.

```
pip install -r requirements.txt
pytest -q
```

The broker/database/object-store wiring is integration behaviour, exercised by running the stack rather than by mocking three services in a unit test.

## Continuous integration

`.github/workflows/ci.yml` runs the test suite and a byte-compile check of every module on push and pull request against `main`.

## Project layout

```
producer/sensor_producer.py     load generator: simulated sensor fleet -> Kafka
processor/anomaly_detector.py   rolling z-score detector (unit tested)
processor/stream_processor.py   consume -> score -> Postgres / object store / alerts topic
processor/db.py                 Postgres schema + access layer
dashboard/app.py                Streamlit operational view over Postgres
tests/                          detector unit tests
docker-compose.yml              Kafka, Postgres, MinIO, and the three app services
.github/workflows/ci.yml        test + compile check
```

## Scope

The sensor feed is simulated and the stack runs locally via Compose; it has not been pointed at a physical building's infrastructure. The architecture and the design decisions behind it — bounded rolling windows, offset commits tied to durable writes, raw archive kept separate from queryable state, alerts on their own topic — are the substance, and they carry over unchanged to a real broker and real object storage.

## License

MIT — see [LICENSE](LICENSE).
