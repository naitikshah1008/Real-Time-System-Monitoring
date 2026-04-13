# Real-Time System Monitoring

A full-stack data engineering and AI-driven observability project that monitors live system metrics (CPU, memory, etc.), streams them through **Apache Kafka**, processes them in **Apache Flink (PyFlink)** for anomaly detection, and stores results in **TimescaleDB (PostgreSQL)** for real-time visualization in **Grafana** dashboards.

---

## Overview

Modern distributed systems generate massive amounts of telemetry data every second.  
This project builds a **real-time monitoring pipeline** that can:
- Stream system metrics in real time.
- Detect anomalies using adaptive statistical models (EWMA + 3σ).
- Store raw and processed data efficiently.
- Visualize insights dynamically in Grafana.
- Extend to AI-based forecasting (ARIMA / LSTM).

It demonstrates hands-on skills in **streaming data pipelines**, **distributed systems**, **AI for observability**, and **real-time analytics**—key areas for cloud and SRE roles at Amazon, Google, or Microsoft.

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-------------|----------|
| **Ingestion** |  **Apache Kafka** | Message broker for streaming metrics |
| **Coordination** |  **Zookeeper** | Kafka cluster coordination |
| **Schema Management** |  **Confluent Schema Registry (Avro)** | Enforce message consistency |
| **Processing** |  **Apache PyFlink** | Real-time anomaly detection |
| **Storage** |  **TimescaleDB (PostgreSQL)** | Time-series database for metrics |
| **Visualization** |  **Grafana** | Dashboard for real-time monitoring |
| **Alerting (future)** |  **Slack / PagerDuty** | Incident alerts for detected anomalies |
| **Containerization** |  **Docker & Docker Compose** | Local multi-service orchestration |

---

## System Architecture

```
┌─────────────┐ ┌────────────┐ ┌───────────────┐ ┌───────────────┐ ┌──────────────┐
│ Metrics     │ →→→ │ Kafka      │ →→→ │ PyFlink Stream │ →→→ │ TimescaleDB   │ →→→ │ Grafana   │
│ Producer(s) │     │ (Broker)   │     │ Processing     │     │ (Postgres)    │     │ Dashboard │
└─────────────┘ └────────────┘ └───────────────┘ └───────────────┘ └──────────────┘
```

**Flow summary:**
1. Python producers generate and publish live CPU/memory metrics to Kafka topics.
2. PyFlink consumes the stream, applies an **EWMA (Exponentially Weighted Moving Average)** anomaly detector using the 3σ rule.
3. Anomalies and metrics are inserted into TimescaleDB.
4. Grafana visualizes real-time metrics and anomaly scores.

![Grafana Dashboard](./images/image.png)

---

## AI & Anomaly Detection

**EWMA (Exponentially Weighted Moving Average) Model**

```python
ewma_new  = ALPHA * value + (1 - ALPHA) * ewma
ewmsq_new = ALPHA * (value**2) + (1 - ALPHA) * ewmsq
variance  = max(ewmsq_new - ewma_new**2, 0)
std_dev   = sqrt(variance)
score     = abs(value - ewma_new) / std_dev   # 3σ rule
```

Each host maintains its own EWMA state.
A score ≥ 3 is treated as an anomaly.
Future extensions: integrate ARIMA / LSTM for predictive forecasting.

---

## Repository Structure

Real-Time-System-Monitoring-with-AI-Prediction/
│
├── docker-compose.yml          # Multi-container setup (Kafka, Zookeeper, Postgres, Schema Registry, Grafana)
├── metrics_producer.py         # Streams random CPU & memory metrics to Kafka
├── db_consumer.py              # Consumes from Kafka and inserts into TimescaleDB
├── flink_job.py (or anomaly_flink.py)
│                              # PyFlink anomaly detection pipeline
├── sql/
│   └── init.sql                # Creates metrics_raw and metrics_anomalies tables
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation

---

## Quick Start

```bash
docker compose up -d
python metrics_producer.py
python db_consumer.py
python flink_job.py
```

---

## Setup & Installation

1️⃣ Prerequisites
Install the following tools:

```bash
Docker & Docker Compose
Python 3.10+
pip install -r requirements.txt
```

2️⃣ Launch Docker Services

```bash
docker compose up -d
```

This spins up:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Schema Registry (8081)
- Postgres/TimescaleDB (5432)
- Grafana (3000)
- Kafdrop UI (19000)

Check container status:

```bash
docker compose ps
```

3️⃣ Verify Kafka Topics

```bash
docker exec -it rtm-ai-kafka-1 \
  kafka-topics --bootstrap-server kafka:29092 --list
```

If not present, create:

```bash
docker exec -it rtm-ai-kafka-1 \
  kafka-topics --bootstrap-server kafka:29092 --create --topic metrics --partitions 1 --replication-factor 1
```

---

## Running the Pipeline

Step 1. Start the Producer

```bash
python metrics_producer.py
```

Sends random CPU/memory values (per host) to Kafka every few seconds.

Step 2. Start the Consumer

```bash
python db_consumer.py
```

Reads messages from Kafka → Inserts into metrics_raw table.

Step 3. Run PyFlink Job

```bash
python flink_job.py
```

Applies EWMA anomaly detection and writes results into metrics_anomalies.

Step 4. View Data in Postgres

```bash
docker exec -it rtm-ai-postgres-1 psql -U postgres -d metrics
\dt
SELECT * FROM metrics_raw LIMIT 5;
SELECT * FROM metrics_anomalies LIMIT 5;
```

Step 5. Visualize in Grafana

Navigate to: http://localhost:3000

Add PostgreSQL data source (host: postgres:5432)

Create dashboards using SQL panels:
- CPU Panel  
- Memory Panel  
- Anomaly Score Panel

---

 Example Grafana Queries

CPU Panel

```sql
SELECT to_timestamp(ts) AS "time", host, cpu
FROM metrics_anomalies
WHERE $__timeFilter(to_timestamp(ts))
ORDER BY ts;
```

Memory Panel

```sql
SELECT to_timestamp(ts) AS "time", host, memory
FROM metrics_anomalies
WHERE $__timeFilter(to_timestamp(ts))
ORDER BY ts;
```

Anomaly Score Panel

```sql
SELECT to_timestamp(ts) AS "time", host, score
FROM metrics_anomalies
WHERE score >= 3
  AND $__timeFilter(to_timestamp(ts))
ORDER BY ts;
```

---

## Future Enhancements
- ✅ AI Forecasting (ARIMA / LSTM via River / PyTorch)
- ✅ Real-time Alerting via Slack / PagerDuty
- ✅ Auto-scaling metrics ingestion using Kubernetes
- ✅ Integration with Prometheus / OpenTelemetry
- ✅ Web dashboard for anomaly reports

---

## Author
- Naitik Shah
- Master of Engineering in Computer Science
- Oregon State University
- 🔗 [LinkedIn](https://www.linkedin.com/in/naitik1008)  
- 🔗 [GitHub](https://github.com/naitikshah1008)

---

## License

This project is licensed under the MIT License.

---

## ⭐ Acknowledgments
- Apache Flink & Kafka documentation

- TimescaleDB community

- Grafana Labs tutorials

- Confluent Schema Registry samples

- “Building reliable systems means understanding the data flowing through them.
This project bridges the gap between monitoring, prediction, and intelligent automation.”
