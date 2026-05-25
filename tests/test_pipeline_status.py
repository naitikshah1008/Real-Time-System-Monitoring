import importlib.util
from datetime import datetime
from datetime import timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_pipeline_status_module():
    path = ROOT / "demo-api" / "pipeline_status.py"
    spec = importlib.util.spec_from_file_location("pipeline_status", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_status_rollup_degrades_when_a_component_is_degraded():
    pipeline_status = load_pipeline_status_module()

    components = {
        "api": pipeline_status.component("API", "ok", "serving requests"),
        "kafka": pipeline_status.component("Kafka", "degraded", "missing topic"),
    }

    assert pipeline_status.status_rollup(components) == "degraded"


def test_age_helpers_format_recent_and_missing_events():
    pipeline_status = load_pipeline_status_module()
    now = datetime(2026, 5, 25, 12, 0, 30, tzinfo=timezone.utc)
    event_time = datetime(2026, 5, 25, 12, 0, 0)

    assert pipeline_status.age_seconds(event_time, now) == 30
    assert pipeline_status.describe_age(30) == "30s ago"
    assert pipeline_status.describe_age(None) == "no events yet"
    assert pipeline_status.serialize_datetime(event_time) == "2026-05-25T12:00:00+00:00"
