import json
import os
import time
from pathlib import Path

import requests


SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_DIR = Path(os.getenv("SCHEMA_DIR", "/schemas"))
SUBJECTS = {
    "metrics_raw-value": "raw_metric.schema.json",
    "metrics_anomalies-value": "anomaly_event.schema.json",
    "incident_commands-value": "incident_command.schema.json",
}


def wait_for_registry():
    for _ in range(60):
        try:
            response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
            if response.status_code < 500:
                return
        except requests.RequestException:
            pass
        print("Schema Registry not ready yet, retrying...")
        time.sleep(2)
    raise RuntimeError("Schema Registry did not become ready")


def register_subject(subject, schema_path):
    schema = json.loads(schema_path.read_text())
    payload = {
        "schemaType": "JSON",
        "schema": json.dumps(schema),
    }
    response = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps(payload),
        timeout=10,
    )
    response.raise_for_status()
    print(f"Registered {subject}: {response.json()}")


def main():
    wait_for_registry()
    for subject, filename in SUBJECTS.items():
        register_subject(subject, SCHEMA_DIR / filename)


if __name__ == "__main__":
    main()
