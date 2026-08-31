"""
Consumes raw sensor readings from Kafka, scores each one with the rolling
z-score detector, batches results into Postgres, publishes anomalies to a
separate 'alerts' topic, and archives raw readings to object storage as
newline-delimited JSON partitioned by date.

Kafka offsets are committed only after a batch has been durably written to
Postgres and object storage, so the pipeline is at-least-once: a crash
mid-batch replays those readings rather than losing them.
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from io import BytesIO

import boto3
from botocore.client import Config
from kafka import KafkaConsumer, KafkaProducer

from anomaly_detector import RollingZScoreDetector
import db

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("stream_processor")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
SENSOR_TOPIC = os.environ.get("SENSOR_TOPIC", "building-sensor-readings")
ALERT_TOPIC = os.environ.get("ALERT_TOPIC", "sensor-alerts")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
BATCH_FLUSH_SECONDS = float(os.environ.get("BATCH_FLUSH_SECONDS", "5"))

ANOMALY_WINDOW = int(os.environ.get("ANOMALY_WINDOW", "30"))
ANOMALY_THRESHOLD = float(os.environ.get("ANOMALY_THRESHOLD", "3.0"))
ANOMALY_MIN_SAMPLES = int(os.environ.get("ANOMALY_MIN_SAMPLES", "10"))

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.environ.get("S3_BUCKET", "sensor-archive")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")

CONNECT_MAX_BACKOFF = 30.0


class _Shutdown:
    """Flag flipped by SIGTERM/SIGINT so the main loop can drain and exit."""

    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum, _frame) -> None:
        log.info("received signal %s, will drain and shut down", signum)
        self.requested = True


def _retry(description: str, fn):
    """Call fn() until it succeeds, backing off between attempts.

    Used for anything that talks to an external service so container start
    order and transient restarts don't need manual intervention.
    """
    backoff = 2.0
    while True:
        try:
            return fn()
        except Exception as exc:  # retry on anything transient
            log.warning("%s failed (%s); retrying in %.0fs", description, exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, CONNECT_MAX_BACKOFF)


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
        region_name="us-east-1",
    )


def ensure_bucket(s3) -> None:
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if S3_BUCKET not in existing:
        s3.create_bucket(Bucket=S3_BUCKET)
        log.info("created archive bucket %s", S3_BUCKET)


def archive_to_s3(s3, batch: list[dict]) -> None:
    if not batch:
        return
    date_prefix = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    key = f"readings/{date_prefix}/{int(time.time() * 1000)}.jsonl"
    body = "\n".join(json.dumps(r) for r in batch).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=BytesIO(body))
    log.debug("archived %d readings to s3://%s/%s", len(batch), S3_BUCKET, key)


def parse_reading(message):
    """Pull the fields we need off a Kafka message, or None if it's malformed.

    One bad message is logged and skipped; it must not stall the consumer.
    """
    try:
        r = message.value
        return {
            "sensor_id": str(r["sensor_id"]),
            "metric": str(r["metric"]),
            "value": float(r["value"]),
            "timestamp": str(r["timestamp"]),
            "raw": r,
        }
    except (KeyError, TypeError, ValueError) as exc:
        log.warning("skipping malformed message at offset %s: %s",
                    getattr(message, "offset", "?"), exc)
        return None


def main() -> None:
    shutdown = _Shutdown()

    _retry("schema init", db.init_schema)
    s3 = make_s3_client()
    _retry("archive bucket check", lambda: ensure_bucket(s3))

    consumer = _retry(
        "kafka consumer connect",
        lambda: KafkaConsumer(
            SENSOR_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            group_id="stream-processor",
            consumer_timeout_ms=1000,
        ),
    )
    alert_producer = _retry(
        "kafka producer connect",
        lambda: KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        ),
    )

    detector = RollingZScoreDetector(
        window_size=ANOMALY_WINDOW,
        threshold=ANOMALY_THRESHOLD,
        min_samples=ANOMALY_MIN_SAMPLES,
    )

    db_batch: list[tuple] = []
    archive_batch: list[dict] = []
    consumed_since_commit = 0
    last_flush = time.time()

    def flush() -> None:
        nonlocal db_batch, archive_batch, consumed_since_commit, last_flush
        if db_batch:
            _retry("postgres write", lambda: db.insert_readings(db_batch))
            _retry("s3 archive", lambda: archive_to_s3(s3, archive_batch))
            log.info("flushed %d readings", len(db_batch))
            db_batch, archive_batch = [], []
        alert_producer.flush()
        if consumed_since_commit:
            consumer.commit()  # offsets advance only once the batch is durable
            consumed_since_commit = 0
        last_flush = time.time()

    log.info("consuming '%s' on %s", SENSOR_TOPIC, KAFKA_BROKER)

    while not shutdown.requested:
        # consumer_timeout_ms ends this loop after ~1s of no messages, so the
        # time-based flush and the shutdown check still run when the stream is idle.
        for message in consumer:
            if shutdown.requested:
                break

            consumed_since_commit += 1
            parsed = parse_reading(message)
            if parsed is None:
                continue

            # key on sensor+metric: temperature and power draw for one sensor
            # have completely different baselines.
            key = f"{parsed['sensor_id']}:{parsed['metric']}"
            result = detector.update(key, parsed["value"])

            db_batch.append((
                parsed["sensor_id"], parsed["metric"], parsed["value"],
                parsed["timestamp"], result.z_score, result.is_anomaly,
            ))
            archive_batch.append(parsed["raw"])

            if result.is_anomaly:
                alert = {
                    "sensor_id": parsed["sensor_id"],
                    "metric": parsed["metric"],
                    "value": parsed["value"],
                    "z_score": round(result.z_score, 2),
                    "timestamp": parsed["timestamp"],
                }
                alert_producer.send(ALERT_TOPIC, value=alert)
                _retry("alert insert", lambda p=parsed, r=result: db.insert_alert(
                    p["sensor_id"], p["metric"], p["value"], r.z_score, p["timestamp"],
                ))
                log.warning("ALERT sensor=%s metric=%s value=%s z=%.2f",
                            parsed["sensor_id"], parsed["metric"],
                            parsed["value"], result.z_score)

            if len(db_batch) >= BATCH_SIZE:
                flush()

        if (time.time() - last_flush) >= BATCH_FLUSH_SECONDS:
            flush()

    log.info("draining %d buffered readings before exit", len(db_batch))
    flush()
    consumer.close()
    alert_producer.close()
    log.info("shutdown complete")


if __name__ == "__main__":
    main()
