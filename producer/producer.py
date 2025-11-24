import json
import time
import socket
import psutil
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC = "metrics_raw"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

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
        producer.send(TOPIC, msg)
        print("Sent:", msg)
        time.sleep(1)

if __name__ == "__main__":
    main()
