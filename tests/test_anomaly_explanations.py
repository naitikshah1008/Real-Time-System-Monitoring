import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_explanations_module():
    path = ROOT / "demo-api" / "anomaly_explanations.py"
    spec = importlib.util.spec_from_file_location("anomaly_explanations", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_explain_anomaly_adds_plain_english_context():
    explanations = load_explanations_module()

    anomaly = explanations.explain_anomaly(
        {
            "event_id": "evt-1-cpu",
            "host": "demo-host-1",
            "metric": "cpu",
            "value": 95.0,
            "baseline": 20.0,
            "score": 12.5,
            "severity": "critical",
            "scenario": "cpu_spike",
        }
    )

    assert anomaly["headline"] == "Critical CPU anomaly on demo-host-1"
    assert "95.0% vs 20.0%" in anomaly["explanation"]
    assert anomaly["scenario_label"] == "CPU Spike"
    assert anomaly["recommendation"]


def test_summarize_current_metric_formats_combined_scenario():
    explanations = load_explanations_module()

    summary = explanations.summarize_current_metric(
        {
            "host": "demo-host-1",
            "scenario": "cpu_spike+network_burst",
        }
    )

    assert summary["scenario_label"] == "CPU Spike + Network Burst"
    assert summary["headline"] == "demo-host-1 is reporting CPU Spike + Network Burst"
