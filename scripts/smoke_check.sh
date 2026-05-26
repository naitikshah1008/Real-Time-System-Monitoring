#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Checking Docker Compose config..."
docker compose config --quiet

echo "Checking Python syntax..."
python3 -m py_compile \
  producer/producer.py \
  consumer/consumer.py \
  anomaly-detector/anomaly_detector.py \
  stream-processor/anomaly.py \
  stream-processor/processor.py \
  demo-api/app.py \
  demo-api/incident_commands.py \
  demo-api/anomaly_explanations.py \
  demo-api/pipeline_status.py \
  schema-registry/register_schemas.py

echo "Checking Grafana dashboard JSON..."
python3 - <<'PY'
import json
from pathlib import Path

json.loads(Path("grafana/dashboards/rtm-dashboard.json").read_text())
PY

echo "Running tests..."
python3 -m pytest

if [[ "${1:-}" != "--runtime" ]]; then
  echo "Static smoke checks passed."
  echo "Run scripts/smoke_check.sh --runtime after docker compose up --build to check live services."
  exit 0
fi

echo "Checking running demo API..."
python3 - <<'PY'
import json
import os
from urllib.request import urlopen

base_url = os.environ.get("DEMO_API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")


def get_json(path):
    with urlopen(f"{base_url}{path}", timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


health = get_json("/api/health")
if health.get("status") != "ok":
    raise SystemExit(f"/api/health is not ok: {health}")

pipeline = get_json("/api/pipeline/status")
pipeline_status = pipeline.get("status") or pipeline.get("overall_status")
if pipeline_status != "ok":
    raise SystemExit(f"/api/pipeline/status is not ok: {pipeline}")

summary = get_json("/api/summary")
if "current" not in summary or "latest_anomalies" not in summary:
    raise SystemExit(f"/api/summary has unexpected shape: {summary}")

print("Runtime smoke checks passed.")
PY
