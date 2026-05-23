import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ewma_detector_emits_metric_specific_anomaly():
    anomaly = load_module("stream_anomaly", ROOT / "stream-processor" / "anomaly.py")
    detector = anomaly.EWMADetector()

    normal = {
        "event_id": "baseline",
        "host": "demo-host-1",
        "ts": 100,
        "cpu": 10.0,
        "mem": 20.0,
        "disk": 30.0,
        "net_in": 1.0,
        "net_out": 1.0,
        "scenario": "normal",
        "source": "test",
    }
    for index in range(5):
        detector.detect({**normal, "event_id": f"baseline-{index}", "ts": 100 + index})

    spike = {**normal, "event_id": "spike", "ts": 200, "cpu": 95.0, "scenario": "cpu_spike"}
    events = detector.detect(spike)

    assert len(events) == 1
    assert events[0]["metric"] == "cpu"
    assert events[0]["severity"] == "critical"
    assert events[0]["scenario"] == "cpu_spike"


def test_ewma_detector_tracks_all_configured_metrics():
    anomaly = load_module("stream_anomaly_metrics", ROOT / "stream-processor" / "anomaly.py")

    assert anomaly.TRACKED_METRICS == ("cpu", "mem", "disk", "net_in", "net_out")
