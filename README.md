# RTM-AI Advanced Local Monitoring

RTM-AI is a local real-time observability demo that streams simulated system metrics through Kafka, registers event contracts in Schema Registry, scores anomalies with a PyFlink stream processor, stores time-series data in TimescaleDB, and visualizes the system through Grafana and a simple browser control panel.

## Architecture

```text
Demo API -> Kafka incident_commands -> Producer / Simulator
                                      |
                                      v
Producer -> Kafka metrics_raw -> PyFlink Processor -> Kafka metrics_anomalies
                                      |
                                      v
Consumer -----------------------> TimescaleDB -> Grafana + Demo API
```

## Tech Stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| Stream transport | Kafka, Zookeeper | Metric and incident event topics |
| Schema management | Confluent Schema Registry | JSON schema registration for public event contracts |
| Metric generation | Python, psutil | Host metrics plus simulated incidents |
| Stream processing | PyFlink | Stateful EWMA anomaly scoring |
| Storage | TimescaleDB | Hypertables for raw metrics and anomaly events |
| Dashboarding | Grafana | Time-series and anomaly dashboards |
| Demo UI/API | FastAPI | Non-technical incident controls and latest-event APIs |
| Local runtime | Docker Compose | One-command local stack |

## Quick Start

Prerequisites:

- Docker
- Docker Compose

Start the advanced local stack:

```bash
docker compose up --build
```

Open:

- Demo control panel: http://localhost:8000
- Grafana: http://localhost:3000
- Schema Registry: http://localhost:8081

Grafana login:

- Username: `admin`
- Password: `admin`

## Demo Flow

1. Open the demo control panel at http://localhost:8000.
2. Click an incident button such as `CPU Spike`, `Memory Leak`, `Disk Pressure`, or `Network Burst`.
3. The API publishes an `IncidentCommand` to Kafka.
4. The producer applies the scenario and emits richer `RawMetric` events.
5. PyFlink scores anomalies per host and metric.
6. The consumer stores raw and anomaly events in TimescaleDB.
7. Grafana and the demo API show the latest metrics and anomaly events.

## Public Event Contracts

The checked-in JSON schemas live in `schemas/`.

`RawMetric`:

```text
event_id, host, ts, cpu, mem, disk, net_in, net_out, scenario, source
```

`AnomalyEvent`:

```text
event_id, host, ts, metric, value, baseline, std, score, severity, scenario
```

`IncidentCommand`:

```text
incident_id, type, host, duration_seconds, intensity, created_at
```

Kafka topics:

- `metrics_raw`
- `metrics_anomalies`
- `incident_commands`

## API

```bash
curl http://localhost:8000/api/health
curl http://localhost:8000/api/pipeline/status
curl http://localhost:8000/api/summary
curl http://localhost:8000/api/metrics/latest
curl http://localhost:8000/api/anomalies/latest
```

`/api/pipeline/status` reports API, Kafka topic readiness, Schema Registry subjects, Flink processor health, Timescale hypertables, telemetry freshness, and Grafana health for the demo status strip.

Create an incident:

```bash
curl -X POST http://localhost:8000/api/incidents \
  -H "Content-Type: application/json" \
  -d '{"type":"cpu_spike","host":"*","duration_seconds":120,"intensity":1.0}'
```

Supported incident types:

- `cpu_spike`
- `memory_leak`
- `disk_pressure`
- `network_burst`

## Database

Connect to TimescaleDB:

```bash
docker exec -it rtm-postgres psql -U rtm -d rtm
```

Useful queries:

```sql
SELECT * FROM metrics_raw ORDER BY time DESC LIMIT 5;
SELECT * FROM metrics_anomalies ORDER BY time DESC LIMIT 5;
```

## Tests

Install local test dependencies:

```bash
python3 -m pip install -r requirements-dev.txt
```

Run tests:

```bash
python3 -m pytest
```

Run config checks:

```bash
docker compose config --quiet
python3 -c 'import json; json.load(open("grafana/dashboards/rtm-dashboard.json"))'
```

## Expected Containers

Most services are long-running. Two setup containers are expected to exit successfully:

- `rtm-kafka-init` creates the Kafka topics.
- `rtm-schema-registrar` registers the checked-in event schemas.

Optional developer tools run behind profiles so the default demo stays smaller:

```bash
docker compose --profile debug up -d kafka-ui
docker compose --profile legacy up -d anomaly-detector
```

Open Kafka UI only when the debug profile is running:

- Kafka UI: http://localhost:8080

## Legacy Detector

The original lightweight Python anomaly detector is kept as a fallback profile:

```bash
docker compose --profile legacy up --build anomaly-detector
```
