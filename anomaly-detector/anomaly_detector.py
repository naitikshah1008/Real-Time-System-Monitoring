import json
import time
import math
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "metrics_raw"
OUTPUT_TOPIC = "metrics_anomalies"

ALPHA = 0.2
state = {}   # host -> (ewma, ewmsq)

def detect_anomaly(event):
    host = event["host"]
    value = float(event["cpu"])
    ts = event["ts"]

    prev = state.get(host)
    if prev is None:
        state[host] = (value, value * value)
        return None

    ewma, ewmsq = prev

    ewma_new = ALPHA * value + (1 - ALPHA) * ewma
    ewmsq_new = ALPHA * (value * value) + (1 - ALPHA) * ewmsq

    state[host] = (ewma_new, ewmsq_new)

    var_est = max(ewmsq_new - ewma_new * ewma_new, 0.0)
    std = math.sqrt(var_est)

    if std > 0 and abs(value - ewma_new) > 3 * std:
        return {
            "host": host,
            "cpu": value,
            "ts": ts,
            "ewma": ewma_new,
            "std": std,
            "type": "ANOMALY"
        }

    return None


def main():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="rtm-anomaly-group"
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Anomaly detector started")

    for msg in consumer:
        data = msg.value
        result = detect_anomaly(data)

        if result:
            print("ANOMALY:", result)
            producer.send(OUTPUT_TOPIC, result)


if __name__ == "__main__":
    main()
