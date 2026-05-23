import json
import os

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from kafka import KafkaProducer
from psycopg2.extras import RealDictCursor

from incident_commands import build_incident_command


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
INCIDENT_TOPIC = os.getenv("INCIDENT_COMMANDS_TOPIC", "incident_commands")
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "rtm")
PG_USER = os.getenv("PG_USER", "rtm")
PG_PASSWORD = os.getenv("PG_PASSWORD", "rtm")

app = FastAPI(title="RTM-AI Demo API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

producer = None


def get_db_connection():
    return psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def get_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            request_timeout_ms=30000,
            api_version_auto_timeout_ms=30000,
        )
    return producer


def query_rows(sql, params):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]
    except (psycopg2.errors.UndefinedColumn, psycopg2.errors.UndefinedTable):
        return []


@app.get("/api/health")
def health():
    postgres_ok = False
    kafka_ok = False

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        postgres_ok = True
    except Exception:
        postgres_ok = False

    try:
        kafka_ok = bool(get_producer().bootstrap_connected())
    except Exception:
        kafka_ok = False

    return {
        "status": "ok" if postgres_ok and kafka_ok else "degraded",
        "postgres": postgres_ok,
        "kafka": kafka_ok,
    }


@app.get("/api/metrics/latest")
def latest_metrics(limit: int = 50):
    limit = max(1, min(limit, 500))
    return query_rows(
        """
        SELECT event_id, time, host, ts, cpu, mem, disk, net_in, net_out, scenario, source
        FROM metrics_raw
        WHERE event_id IS NOT NULL AND time IS NOT NULL
        ORDER BY time DESC NULLS LAST
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/anomalies/latest")
def latest_anomalies(limit: int = 50):
    limit = max(1, min(limit, 500))
    return query_rows(
        """
        SELECT event_id, time, host, ts, metric, value, baseline, std, score, severity, scenario
        FROM metrics_anomalies
        WHERE event_id IS NOT NULL AND time IS NOT NULL
        ORDER BY time DESC NULLS LAST
        LIMIT %s
        """,
        (limit,),
    )


@app.post("/api/incidents")
def create_incident(payload: dict):
    try:
        command = build_incident_command(payload)
        get_producer().send(INCIDENT_TOPIC, command).get(timeout=10)
        return command
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to publish incident: {e}") from e


app.mount("/", StaticFiles(directory="static", html=True), name="static")
