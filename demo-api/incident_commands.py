import time
import uuid


INCIDENT_TYPES = {"cpu_spike", "memory_leak", "disk_pressure", "network_burst"}


def build_incident_command(payload):
    incident_type = payload.get("type")
    if incident_type not in INCIDENT_TYPES:
        raise ValueError(f"Unsupported incident type: {incident_type}")

    duration = int(payload.get("duration_seconds", 120))
    if duration < 5 or duration > 3600:
        raise ValueError("duration_seconds must be between 5 and 3600")

    intensity = float(payload.get("intensity", 1.0))
    if intensity < 0.1 or intensity > 1.0:
        raise ValueError("intensity must be between 0.1 and 1.0")

    host = payload.get("host", "*") or "*"
    now = int(time.time())
    return {
        "incident_id": payload.get("incident_id") or str(uuid.uuid4()),
        "type": incident_type,
        "host": host,
        "duration_seconds": duration,
        "intensity": intensity,
        "created_at": int(payload.get("created_at", now)),
    }
