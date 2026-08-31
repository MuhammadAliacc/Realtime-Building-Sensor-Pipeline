"""Thin Postgres access layer for the stream processor and the dashboard.

Kept deliberately small: two tables (readings, alerts), plain SQL via
psycopg2, no ORM. For a project this size an ORM would be more ceremony
than it's worth; the schema is simple enough to hand-write and reason about.
"""

import logging
import os
import time

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values

log = logging.getLogger("db")

DB_DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://pipeline:pipeline@localhost:5432/sensor_pipeline",
)

CONNECT_RETRIES = int(os.environ.get("DB_CONNECT_RETRIES", "10"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id BIGSERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    z_score DOUBLE PRECISION,
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_readings_sensor_ts ON readings (sensor_id, ts DESC);

CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    z_score DOUBLE PRECISION NOT NULL,
    ts TIMESTAMPTZ NOT NULL
);
"""


def get_connection():
    """Open a Postgres connection, retrying with backoff while the server is
    still coming up (container start order, restarts)."""
    backoff = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            return psycopg2.connect(DB_DSN)
        except OperationalError as exc:
            last_exc = exc
            log.warning("db connect attempt %d/%d failed: %s", attempt, CONNECT_RETRIES, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 15.0)
    raise last_exc


def init_schema() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
        conn.commit()


def insert_readings(rows: list[tuple]) -> None:
    """rows: (sensor_id, metric, value, ts, z_score, is_anomaly)"""
    if not rows:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO readings (sensor_id, metric, value, ts, z_score, is_anomaly)
                   VALUES %s""",
                rows,
            )
        conn.commit()


def insert_alert(sensor_id: str, metric: str, value: float, z_score: float, ts: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO alerts (sensor_id, metric, value, z_score, ts)
                   VALUES (%s, %s, %s, %s, %s)""",
                (sensor_id, metric, value, z_score, ts),
            )
        conn.commit()


def recent_alerts(limit: int = 50) -> list[tuple]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sensor_id, metric, value, z_score, ts FROM alerts ORDER BY ts DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()


def recent_readings(sensor_id: str, metric: str, limit: int = 200) -> list[tuple]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT value, ts, is_anomaly FROM readings
                   WHERE sensor_id = %s AND metric = %s
                   ORDER BY ts DESC LIMIT %s""",
                (sensor_id, metric, limit),
            )
            return cur.fetchall()
