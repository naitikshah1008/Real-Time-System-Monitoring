# Real-Time System Monitoring with AI Prediction

A Docker Compose based real-time monitoring pipeline that collects CPU and memory metrics, streams them through Kafka, detects CPU anomalies with an EWMA + 3 sigma model, stores results in Postgres, and visualizes the data in Grafana.

## Architecture

```text
producer -> Kafka metrics_raw -> anomaly-detector -> Kafka metrics_anomalies
                                      |
                                      v
consumer ------------------------> Postgres -> Grafana
```

## Tech Stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| Metrics collection | Python, psutil | Collect CPU and memory usage |
| Streaming | Apache Kafka, Zookeeper | Transport metric events |
| Processing | Python anomaly detector | EWMA + 3 sigma CPU anomaly detection |
| Storage | Postgres | Store raw metrics and anomaly events |
| Visualization | Grafana | Dashboard for CPU, memory, and anomaly score |
| Kafka inspection | Kafka UI | View topics and messages locally |
| Runtime | Docker Compose | Local multi-container orchestration |

## Repository Structure

```text
.
├── anomaly-detector/
│   ├── anomaly_detector.py
│   ├── Dockerfile
│   └── requirements.txt
├── consumer/
│   ├── consumer.py
│   ├── Dockerfile
│   └── requirements.txt
├── producer/
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
├── grafana/
│   ├── dashboards/rtm-dashboard.json
│   └── provisioning/
├── docker-compose.yml
└── README.md
```

## Quick Start

Prerequisites:

- Docker
- Docker Compose

Start the full pipeline:

```bash
docker compose up --build
```

Useful local URLs:

- Grafana: http://localhost:3000
- Kafka UI: http://localhost:8080

Default Grafana login:

- Username: `admin`
- Password: `admin`

Grafana provisions the Postgres datasource and dashboard automatically.

## Data Flow

1. `producer/producer.py` publishes metrics to Kafka topic `metrics_raw`.
2. `anomaly-detector/anomaly_detector.py` consumes `metrics_raw`, maintains per-host EWMA state, and publishes CPU anomaly events to `metrics_anomalies`.
3. `consumer/consumer.py` stores both raw metrics and anomaly events in Postgres.
4. Grafana reads Postgres and renders the dashboard from `grafana/dashboards/rtm-dashboard.json`.

## Kafka Topics

- `metrics_raw`
- `metrics_anomalies`

List topics:

```bash
docker exec -it rtm-kafka kafka-topics --bootstrap-server kafka:9092 --list
```

## Database

Connect to Postgres:

```bash
docker exec -it rtm-postgres psql -U rtm -d rtm
```

Tables created by the consumer:

- `metrics_raw`
- `metrics_anomalies`

Example queries:

```sql
SELECT * FROM metrics_raw ORDER BY ts DESC LIMIT 5;
SELECT * FROM metrics_anomalies ORDER BY ts DESC LIMIT 5;
```

## Anomaly Detection

The anomaly detector uses an exponentially weighted moving average per host:

```python
ewma_new = ALPHA * value + (1 - ALPHA) * ewma
ewmsq_new = ALPHA * (value**2) + (1 - ALPHA) * ewmsq
variance = max(ewmsq_new - ewma_new**2, 0)
std = sqrt(variance)
score = abs(value - ewma_new) / std
```

Events with `score >= 3` are stored in `metrics_anomalies`.

## Configuration

The Python services use environment variables with Docker-friendly defaults:

| Variable | Default | Used by |
| --- | --- | --- |
| `KAFKA_BROKER` | `kafka:9092` | producer, consumer, anomaly detector |
| `METRICS_RAW_TOPIC` | `metrics_raw` | producer, consumer, anomaly detector |
| `METRICS_ANOMALY_TOPIC` | `metrics_anomalies` | consumer, anomaly detector |
| `PG_HOST` | `postgres` | consumer |
| `PG_DB` | `rtm` | consumer |
| `PG_USER` | `rtm` | consumer |
| `PG_PASSWORD` | `rtm` | consumer |

## Troubleshooting

Check service status:

```bash
docker compose ps
```

Follow logs:

```bash
docker compose logs -f producer anomaly-detector consumer
```

Restart from a clean local database and Grafana volume:

```bash
docker compose down -v
docker compose up --build
```
