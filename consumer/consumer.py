import json
import time
import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER = "kafka:9092"

# Topics
RAW_TOPIC = "metrics_raw"
ANOMALY_TOPIC = "metrics_anomalies"

PG_HOST = "postgres"
PG_DB = "rtm"
PG_USER = "rtm"
PG_PASSWORD = "rtm"

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
        ts BIGINT,
        ewma FLOAT,
        std FLOAT,
        type TEXT
    );
    """

    with conn.cursor() as cur:
        cur.execute(raw_sql)
        cur.execute(anomaly_sql)

    print("Ensured tables exist")

def main():
    conn = connect_db()
    create_tables(conn)

    # Consumer for raw metrics
    raw_consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="rtm-consumer-raw"
    )

    # Consumer for anomaly messages
    anomaly_consumer = KafkaConsumer(
        ANOMALY_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="rtm-consumer-anomaly"
    )

    print("Consumer started. Listening for messages...")

    while True:
        # Process RAW metrics
        for msg in raw_consumer.poll(timeout_ms=1000).values():
            for record in msg:
                data = record.value
                print("RAW:", data)
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO metrics_raw (host, cpu, mem, ts) VALUES (%s, %s, %s, %s)",
                        (data["host"], data["cpu"], data["mem"], data["ts"])
                    )

        # Process ANOMALY metrics
        for msg in anomaly_consumer.poll(timeout_ms=1000).values():
            for record in msg:
                data = record.value
                print("ANOMALY:", data)
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO metrics_anomalies (host, cpu, ts, ewma, std, type) VALUES (%s, %s, %s, %s, %s, %s)",
                        (data["host"], data["cpu"], data["ts"], data["ewma"], data["std"], data["type"])
                    )

if __name__ == "__main__":
    main()