import json
from pathlib import Path

from jsonschema import Draft7Validator, validate


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"


def load_schema(filename):
    return json.loads((SCHEMA_DIR / filename).read_text())


def test_checked_in_json_schemas_are_valid():
    for schema_path in SCHEMA_DIR.glob("*.schema.json"):
        Draft7Validator.check_schema(json.loads(schema_path.read_text()))


def test_raw_metric_schema_accepts_public_interface_example():
    schema = load_schema("raw_metric.schema.json")
    validate(
        {
            "event_id": "evt-1",
            "host": "demo-host-1",
            "ts": 1779349000,
            "cpu": 12.5,
            "mem": 42.0,
            "disk": 61.2,
            "net_in": 15.0,
            "net_out": 9.5,
            "scenario": "normal",
            "source": "simulator",
        },
        schema,
    )


def test_anomaly_schema_accepts_public_interface_example():
    schema = load_schema("anomaly_event.schema.json")
    validate(
        {
            "event_id": "evt-1-cpu",
            "host": "demo-host-1",
            "ts": 1779349000,
            "metric": "cpu",
            "value": 95.0,
            "baseline": 10.0,
            "std": 1.2,
            "score": 70.83,
            "severity": "critical",
            "scenario": "cpu_spike",
        },
        schema,
    )


def test_incident_schema_accepts_public_interface_example():
    schema = load_schema("incident_command.schema.json")
    validate(
        {
            "incident_id": "incident-1",
            "type": "network_burst",
            "host": "*",
            "duration_seconds": 120,
            "intensity": 1.0,
            "created_at": 1779349000,
        },
        schema,
    )
