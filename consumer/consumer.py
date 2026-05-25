import json
import os
import time

import psycopg2
from kafka import KafkaConsumer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC = os.getenv("METRICS_RAW_TOPIC", "metrics_raw")
ANOMALY_TOPIC = os.getenv("METRICS_ANOMALY_TOPIC", "metrics_anomalies")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "rtm")
PG_USER = os.getenv("PG_USER", "rtm")
PG_PASSWORD = os.getenv("PG_PASSWORD", "rtm")

RAW_FIELDS = (
    "event_id",
    "ts",
    "host",
    "cpu",
    "mem",
    "disk",
    "net_in",
    "net_out",
    "scenario",
    "source",
)
ANOMALY_FIELDS = (
    "event_id",
    "ts",
    "host",
    "metric",
    "value",
    "baseline",
    "std",
    "score",
    "severity",
    "scenario",
)


def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                database=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
            )
            conn.autocommit = True
            print("Connected to PostgreSQL/TimescaleDB")
            return conn
        except Exception as e:
            print("PostgreSQL/TimescaleDB not ready yet, retrying...", str(e))
            time.sleep(2)


def ensure_schema(conn):
    conn, timescale_enabled = enable_timescale(conn)
    table_statements = [
        """
        CREATE TABLE IF NOT EXISTS metrics_raw (
            event_id TEXT,
            time TIMESTAMPTZ,
            host TEXT,
            ts BIGINT,
            cpu DOUBLE PRECISION,
            mem DOUBLE PRECISION,
            disk DOUBLE PRECISION,
            net_in DOUBLE PRECISION,
            net_out DOUBLE PRECISION,
            scenario TEXT,
            source TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS metrics_anomalies (
            event_id TEXT,
            time TIMESTAMPTZ,
            host TEXT,
            ts BIGINT,
            metric TEXT,
            value DOUBLE PRECISION,
            baseline DOUBLE PRECISION,
            std DOUBLE PRECISION,
            score DOUBLE PRECISION,
            severity TEXT,
            scenario TEXT
        );
        """,
    ]
    migrations = [
        "ALTER TABLE metrics_raw ADD COLUMN IF NOT EXISTS event_id TEXT;",
        "ALTER TABLE metrics_raw ADD COLUMN IF NOT EXISTS time TIMESTAMPTZ;",
        "ALTER TABLE metrics_raw ADD COLUMN IF NOT EXISTS disk DOUBLE PRECISION;",
        "ALTER TABLE metrics_raw ADD COLUMN IF NOT EXISTS net_in DOUBLE PRECISION;",
        "ALTER TABLE metrics_raw ADD COLUMN IF NOT EXISTS net_out DOUBLE PRECISION;",
        "ALTER TABLE metrics_raw ADD COLUMN IF NOT EXISTS scenario TEXT;",
        "ALTER TABLE metrics_raw ADD COLUMN IF NOT EXISTS source TEXT;",
        "ALTER TABLE metrics_anomalies ADD COLUMN IF NOT EXISTS event_id TEXT;",
        "ALTER TABLE metrics_anomalies ADD COLUMN IF NOT EXISTS time TIMESTAMPTZ;",
        "ALTER TABLE metrics_anomalies ADD COLUMN IF NOT EXISTS metric TEXT;",
        "ALTER TABLE metrics_anomalies ADD COLUMN IF NOT EXISTS value DOUBLE PRECISION;",
        "ALTER TABLE metrics_anomalies ADD COLUMN IF NOT EXISTS baseline DOUBLE PRECISION;",
        "ALTER TABLE metrics_anomalies ADD COLUMN IF NOT EXISTS severity TEXT;",
        "ALTER TABLE metrics_anomalies ADD COLUMN IF NOT EXISTS scenario TEXT;",
        "UPDATE metrics_raw SET time = to_timestamp(ts) WHERE time IS NULL AND ts IS NOT NULL;",
        "UPDATE metrics_anomalies SET time = to_timestamp(ts) WHERE time IS NULL AND ts IS NOT NULL;",
        "ALTER TABLE metrics_raw DROP CONSTRAINT IF EXISTS metrics_raw_pkey;",
        "ALTER TABLE metrics_anomalies DROP CONSTRAINT IF EXISTS metrics_anomalies_pkey;",
    ]
    indexes = [
        "CREATE INDEX IF NOT EXISTS metrics_raw_host_time_idx ON metrics_raw (host, time DESC);",
        "CREATE INDEX IF NOT EXISTS metrics_raw_scenario_time_idx ON metrics_raw (scenario, time DESC);",
        "CREATE INDEX IF NOT EXISTS metrics_anomalies_host_metric_time_idx ON metrics_anomalies (host, metric, time DESC);",
        "CREATE INDEX IF NOT EXISTS metrics_anomalies_scenario_time_idx ON metrics_anomalies (scenario, time DESC);",
    ]
    hypertables = ("metrics_raw", "metrics_anomalies")

    with conn.cursor() as cur:
        for statement in table_statements:
            cur.execute(statement)
        for migration in migrations:
            cur.execute(migration)
        for index in indexes:
            cur.execute(index)
        if timescale_enabled:
            for table_name in hypertables:
                ensure_hypertable(conn, cur, table_name)

    print("Ensured metric tables exist")
    return conn


def ensure_hypertable(conn, cur, table_name):
    try:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM timescaledb_information.hypertables
                WHERE hypertable_schema = current_schema()
                  AND hypertable_name = %s
            );
            """,
            (table_name,),
        )
        if cur.fetchone()[0]:
            print(f"Timescale hypertable ready: {table_name}")
            return

        cur.execute(
            """
            SELECT create_hypertable(
                %s::regclass,
                'time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
            """,
            (table_name,),
        )
        print(f"Timescale hypertable created: {table_name}")
    except psycopg2.Error as e:
        print(f"Timescale hypertable migration failed for {table_name}:", str(e))
        conn.rollback()


def enable_timescale(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        print("TimescaleDB extension enabled")
        return conn, True
    except psycopg2.Error as e:
        print("TimescaleDB extension unavailable; continuing with PostgreSQL tables:", str(e))
        if conn.closed:
            conn = connect_db()
    return conn, False


def is_valid_event(data, required_fields, event_type):
    missing = [field for field in required_fields if field not in data]
    if missing:
        print(f"Skipping invalid {event_type} event; missing fields:", missing, data)
        return False
    return True


def create_consumer(topic, group_id):
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=group_id,
                api_version_auto_timeout_ms=30000,
            )
            print(f"Connected to Kafka topic {topic}")
            return consumer
        except Exception as e:
            print(f"Kafka topic {topic} not ready yet, retrying...", str(e))
            time.sleep(3)


def insert_raw(conn, data):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metrics_raw (
                event_id, time, host, ts, cpu, mem, disk, net_in, net_out, scenario, source
            )
            VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                data["event_id"],
                data["ts"],
                data["host"],
                data["ts"],
                data["cpu"],
                data["mem"],
                data["disk"],
                data["net_in"],
                data["net_out"],
                data["scenario"],
                data["source"],
            ),
        )


def insert_anomaly(conn, data):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metrics_anomalies (
                event_id, time, host, ts, metric, value, baseline, std, score, severity, scenario
            )
            VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                data["event_id"],
                data["ts"],
                data["host"],
                data["ts"],
                data["metric"],
                data["value"],
                data["baseline"],
                data["std"],
                data["score"],
                data["severity"],
                data["scenario"],
            ),
        )


def poll_consumer(consumer, topic, group_id):
    try:
        return consumer, consumer.poll(timeout_ms=1000)
    except Exception as e:
        print(f"{topic} consumer failed, reconnecting...", str(e))
        return create_consumer(topic, group_id), {}


def insert_with_recovery(conn, insert_func, data, event_type):
    try:
        insert_func(conn, data)
        return conn
    except psycopg2.Error as e:
        print(f"{event_type} database insert failed, reconnecting...", str(e))
        try:
            conn.close()
        except Exception:
            pass
        conn = connect_db()
        return ensure_schema(conn)


def main():
    conn = connect_db()
    conn = ensure_schema(conn)

    raw_consumer = create_consumer(RAW_TOPIC, "rtm-consumer-raw")
    anomaly_consumer = create_consumer(ANOMALY_TOPIC, "rtm-consumer-anomaly")

    print("Consumer started. Listening for advanced metric events...")

    while True:
        raw_consumer, raw_messages = poll_consumer(raw_consumer, RAW_TOPIC, "rtm-consumer-raw")
        for batch in raw_messages.values():
            for record in batch:
                data = record.value
                print("RAW:", data)
                if not is_valid_event(data, RAW_FIELDS, "raw metric"):
                    continue
                conn = insert_with_recovery(conn, insert_raw, data, "Raw metric")

        anomaly_consumer, anomaly_messages = poll_consumer(
            anomaly_consumer,
            ANOMALY_TOPIC,
            "rtm-consumer-anomaly",
        )
        for batch in anomaly_messages.values():
            for record in batch:
                data = record.value
                print("ANOMALY:", data)
                if not is_valid_event(data, ANOMALY_FIELDS, "anomaly"):
                    continue
                conn = insert_with_recovery(conn, insert_anomaly, data, "Anomaly")


if __name__ == "__main__":
    main()
