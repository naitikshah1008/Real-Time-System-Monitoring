import json
import os
import time
import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# Topics
RAW_TOPIC = os.getenv("METRICS_RAW_TOPIC", "metrics_raw")
ANOMALY_TOPIC = os.getenv("METRICS_ANOMALY_TOPIC", "metrics_anomalies")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "rtm")
PG_USER = os.getenv("PG_USER", "rtm")
PG_PASSWORD = os.getenv("PG_PASSWORD", "rtm")

def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                database=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD
            )
            conn.autocommit = True
            print("Connected to Postgres")
            return conn
        except Exception as e:
            print("Postgres not ready yet, retrying...", str(e))
            time.sleep(2)

def create_tables(conn):
    raw_sql = """
    CREATE TABLE IF NOT EXISTS metrics_raw (
        id SERIAL PRIMARY KEY,
        host TEXT,
        cpu FLOAT,
        mem FLOAT,
        ts BIGINT
    );
    """

    anomaly_sql = """
    CREATE TABLE IF NOT EXISTS metrics_anomalies (
        id SERIAL PRIMARY KEY,
        host TEXT,
        cpu FLOAT,
        mem FLOAT,
        ts BIGINT,
        ewma FLOAT,
        std FLOAT,
        score FLOAT,
        type TEXT
    );
    """

    migrations = [
        "ALTER TABLE metrics_anomalies ADD COLUMN IF NOT EXISTS mem FLOAT;",
        "ALTER TABLE metrics_anomalies ADD COLUMN IF NOT EXISTS score FLOAT;",
    ]

    with conn.cursor() as cur:
        cur.execute(raw_sql)
        cur.execute(anomaly_sql)
        for migration in migrations:
            cur.execute(migration)

    print("Ensured tables exist")

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

def main():
    conn = connect_db()
    create_tables(conn)

    raw_consumer = create_consumer(RAW_TOPIC, "rtm-consumer-raw")
    anomaly_consumer = create_consumer(ANOMALY_TOPIC, "rtm-consumer-anomaly")

    print("Consumer started. Listening for messages...")

    while True:
        try:
            raw_messages = raw_consumer.poll(timeout_ms=1000)
        except Exception as e:
            print("Raw Kafka consumer failed, reconnecting...", str(e))
            raw_consumer = create_consumer(RAW_TOPIC, "rtm-consumer-raw")
            raw_messages = {}

        for msg in raw_messages.values():
            for record in msg:
                data = record.value
                print("RAW:", data)
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO metrics_raw (host, cpu, mem, ts) VALUES (%s, %s, %s, %s)",
                        (data["host"], data["cpu"], data["mem"], data["ts"])
                    )

        try:
            anomaly_messages = anomaly_consumer.poll(timeout_ms=1000)
        except Exception as e:
            print("Anomaly Kafka consumer failed, reconnecting...", str(e))
            anomaly_consumer = create_consumer(ANOMALY_TOPIC, "rtm-consumer-anomaly")
            anomaly_messages = {}

        for msg in anomaly_messages.values():
            for record in msg:
                data = record.value
                print("ANOMALY:", data)
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO metrics_anomalies (host, cpu, mem, ts, ewma, std, score, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            data["host"],
                            data["cpu"],
                            data.get("mem"),
                            data["ts"],
                            data["ewma"],
                            data["std"],
                            data.get("score"),
                            data["type"],
                        )
                    )

if __name__ == "__main__":
    main()
