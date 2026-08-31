"""
Load generator: simulates a fleet of building sensors (temperature, humidity,
power draw) publishing readings to a Kafka topic in real time.

Each sensor walks a slightly randomized baseline (so the stream isn't just
flat noise) and, with a configurable probability, injects a spike so the
downstream anomaly detector has something real to catch. Point real sensors
at the same topic and this process is no longer needed -- everything
downstream is unchanged.
"""

import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("sensor_producer")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = os.environ.get("SENSOR_TOPIC", "building-sensor-readings")
NUM_BUILDINGS = int(os.environ.get("NUM_BUILDINGS", "3"))
SENSORS_PER_BUILDING = int(os.environ.get("SENSORS_PER_BUILDING", "2"))
EMIT_INTERVAL_SECONDS = float(os.environ.get("EMIT_INTERVAL_SECONDS", "1.0"))
ANOMALY_PROBABILITY = float(os.environ.get("ANOMALY_PROBABILITY", "0.02"))

METRICS = {
    "temperature_c": {"base": 21.0, "noise": 0.3, "spike": 15.0},
    "humidity_pct": {"base": 45.0, "noise": 1.5, "spike": 30.0},
    "power_kw": {"base": 12.0, "noise": 0.8, "spike": 40.0},
}

_running = {"flag": True}


def _handle_signal(signum, _frame) -> None:
    log.info("received signal %s, stopping after current tick", signum)
    _running["flag"] = False


def build_sensor_ids() -> list[str]:
    return [
        f"building-{b}-sensor-{s}"
        for b in range(1, NUM_BUILDINGS + 1)
        for s in range(1, SENSORS_PER_BUILDING + 1)
    ]


def make_reading(sensor_id: str, metric: str, state: dict) -> dict:
    cfg = METRICS[metric]
    drift = random.gauss(0, cfg["noise"])
    state[metric] = state.get(metric, cfg["base"]) * 0.95 + (cfg["base"] + drift) * 0.05

    value = state[metric] + random.gauss(0, cfg["noise"])
    is_injected_anomaly = random.random() < ANOMALY_PROBABILITY
    if is_injected_anomaly:
        value += cfg["spike"] * random.choice([-1, 1])

    return {
        "sensor_id": sensor_id,
        "metric": metric,
        "value": round(value, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "injected_anomaly": is_injected_anomaly,  # ground truth, for evaluating the detector offline
    }


def connect() -> KafkaProducer:
    """Connect to Kafka, retrying with backoff until the broker is reachable."""
    backoff = 2.0
    while _running["flag"]:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
            )
        except Exception as exc:  # retry on anything transient
            log.warning("kafka connect failed (%s); retrying in %.0fs", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
    raise SystemExit(0)


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    producer = connect()
    sensor_ids = build_sensor_ids()
    state: dict[str, dict] = {sid: {} for sid in sensor_ids}

    log.info("producing to '%s' on %s for %d sensors", TOPIC, KAFKA_BROKER, len(sensor_ids))

    try:
        while _running["flag"]:
            for sensor_id in sensor_ids:
                for metric in METRICS:
                    reading = make_reading(sensor_id, metric, state[sensor_id])
                    producer.send(TOPIC, key=sensor_id, value=reading)
            producer.flush()
            time.sleep(EMIT_INTERVAL_SECONDS)
    finally:
        producer.close()
        log.info("producer stopped")


if __name__ == "__main__":
    main()
