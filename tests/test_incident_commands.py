import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_incidents_module():
    path = ROOT / "demo-api" / "incident_commands.py"
    spec = importlib.util.spec_from_file_location("incident_commands", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_incident_command_defaults_to_all_hosts():
    incidents = load_incidents_module()

    command = incidents.build_incident_command({"type": "cpu_spike"})

    assert command["type"] == "cpu_spike"
    assert command["host"] == "*"
    assert command["duration_seconds"] == 120
    assert command["intensity"] == 1.0
    assert command["incident_id"]


def test_build_incident_command_rejects_invalid_type():
    incidents = load_incidents_module()

    with pytest.raises(ValueError):
        incidents.build_incident_command({"type": "unknown"})
