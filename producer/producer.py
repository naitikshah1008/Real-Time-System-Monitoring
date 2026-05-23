import json
import math
import os
import socket
import time
import uuid

import psutil
from kafka import KafkaConsumer, KafkaProducer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC = os.getenv("METRICS_RAW_TOPIC", "metrics_raw")
INCIDENT_TOPIC = os.getenv("INCIDENT_COMMANDS_TOPIC", "incident_commands")
HOST_ID = os.getenv("HOST_ID", socket.gethostname())
SOURCE = os.getenv("METRICS_SOURCE", "simulator")

INCIDENT_TYPES = {"cpu_spike", "memory_leak", "disk_pressure", "network_burst"}


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
            print("Kafka producer not ready yet, retrying...", str(e))
            time.sleep(3)


def create_incident_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                INCIDENT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id=f"rtm-producer-incidents-{HOST_ID}",
                api_version_auto_timeout_ms=30000,
            )
            print("Connected to incident command topic")
            return consumer
        except Exception as e:
            print("Incident command topic not ready yet, retrying...", str(e))
            time.sleep(3)


def clamp_percent(value):
    return round(max(0.0, min(100.0, float(value))), 2)


def collect_base_metrics(net_state):
    now = time.time()
    counters = psutil.net_io_counters()
    previous = net_state.get("previous")
    previous_time = net_state.get("time")
    elapsed = max(now - previous_time, 1.0) if previous_time else 1.0

    if previous:
        net_in = (counters.bytes_recv - previous.bytes_recv) / elapsed / 1024
        net_out = (counters.bytes_sent - previous.bytes_sent) / elapsed / 1024
    else:
        net_in = 0.0
        net_out = 0.0

    net_state["previous"] = counters
    net_state["time"] = now

    return {
        "cpu": clamp_percent(psutil.cpu_percent(interval=1)),
        "mem": clamp_percent(psutil.virtual_memory().percent),
        "disk": clamp_percent(psutil.disk_usage("/").percent),
        "net_in": round(max(net_in, 0.0), 2),
        "net_out": round(max(net_out, 0.0), 2),
    }


def should_apply(command):
    host = command.get("host", "*")
    return host in {"*", HOST_ID}


def consume_incident_commands(consumer, active_incidents):
    try:
        messages = consumer.poll(timeout_ms=10)
    except Exception as e:
        print("Incident command consumer failed, reconnecting...", str(e))
        return create_incident_consumer()

    now = int(time.time())
    for batch in messages.values():
        for record in batch:
            command = record.value
            try:
                incident_type = command.get("type")
                if incident_type not in INCIDENT_TYPES or not should_apply(command):
                    continue

                incident_id = command.get("incident_id") or str(uuid.uuid4())
                duration = int(command.get("duration_seconds", 120))
                intensity = float(command.get("intensity", 1.0))
                created_at = int(command.get("created_at", now))
            except (TypeError, ValueError, AttributeError) as e:
                print("Skipping invalid incident command:", e, command)
                continue

            active_incidents[incident_id] = {
                "type": incident_type,
                "host": command.get("host", "*"),
                "intensity": intensity,
                "created_at": created_at,
                "expires_at": now + duration,
            }
            print("Activated incident:", active_incidents[incident_id])

    return consumer


def apply_incidents(metrics, active_incidents):
    now = int(time.time())
    expired = [
        incident_id
        for incident_id, incident in active_incidents.items()
        if incident["expires_at"] <= now
    ]
    for incident_id in expired:
        active_incidents.pop(incident_id, None)

    scenario_types = []
    for incident in active_incidents.values():
        intensity = max(0.1, min(1.0, incident["intensity"]))
        elapsed = max(now - incident["created_at"], 0)
        duration = max(incident["expires_at"] - incident["created_at"], 1)
        progress = min(elapsed / duration, 1.0)
        wave = (math.sin(now / 3) + 1) / 2
        incident_type = incident["type"]
        scenario_types.append(incident_type)

        if incident_type == "cpu_spike":
            metrics["cpu"] = clamp_percent(max(metrics["cpu"], 65 + 35 * intensity * wave))
        elif incident_type == "memory_leak":
            metrics["mem"] = clamp_percent(max(metrics["mem"], 45 + 55 * intensity * progress))
        elif incident_type == "disk_pressure":
            metrics["disk"] = clamp_percent(max(metrics["disk"], 55 + 45 * intensity))
        elif incident_type == "network_burst":
            burst = 2500 * intensity * (0.5 + wave)
            metrics["net_in"] = round(max(metrics["net_in"], burst), 2)
            metrics["net_out"] = round(max(metrics["net_out"], burst * 0.7), 2)

    return "+".join(sorted(set(scenario_types))) if scenario_types else "normal"


def build_metric_event(net_state, active_incidents):
    metrics = collect_base_metrics(net_state)
    scenario = apply_incidents(metrics, active_incidents)
    return {
        "event_id": str(uuid.uuid4()),
        "host": HOST_ID,
        "ts": int(time.time()),
        "cpu": metrics["cpu"],
        "mem": metrics["mem"],
        "disk": metrics["disk"],
        "net_in": metrics["net_in"],
        "net_out": metrics["net_out"],
        "scenario": scenario,
        "source": SOURCE,
    }


def main():
    print("Starting advanced metrics producer...")
    producer = create_producer()
    incident_consumer = create_incident_consumer()
    active_incidents = {}
    net_state = {}

    while True:
        incident_consumer = consume_incident_commands(incident_consumer, active_incidents)
        metric = build_metric_event(net_state, active_incidents)
        try:
            producer.send(RAW_TOPIC, metric).get(timeout=10)
            print("Sent:", metric)
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
