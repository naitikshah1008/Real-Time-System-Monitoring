import json
import os
import time
import socket
import psutil
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("METRICS_RAW_TOPIC", "metrics_raw")

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
            )
            print("Connected to Kafka producer")
            return producer
        except Exception as e:
            print("Kafka not ready yet, retrying...", str(e))
            time.sleep(3)

def get_metrics():
    return {
        "host": socket.gethostname(),
        "cpu": psutil.cpu_percent(interval=1),
        "mem": psutil.virtual_memory().percent,
        "ts": int(time.time())
    }

def main():
    print("Starting producer...")
    producer = create_producer()

    while True:
        msg = get_metrics()
        try:
            producer.send(TOPIC, msg).get(timeout=10)
            print("Sent:", msg)
        except Exception as e:
            print("Failed to send metric, reconnecting...", str(e))
            try:
                producer.close()
            except Exception:
                pass
            producer = create_producer()
        time.sleep(1)

if __name__ == "__main__":
    main()
