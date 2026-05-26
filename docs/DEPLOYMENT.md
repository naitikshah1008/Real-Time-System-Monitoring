# Deployment Notes

This project is ready to publish as a source repository. A public live demo should expose only the demo web UI/API and, optionally, a read-only Grafana dashboard. Keep Kafka, Zookeeper, Schema Registry, TimescaleDB/Postgres, and Flink private.

## Recommended First Live Demo

Use a small VPS or single Docker host first. It matches the current Compose architecture and avoids prematurely splitting Kafka, Flink, and TimescaleDB across multiple hosted products.

1. Provision a host with Docker and Docker Compose.
2. Clone the repository.
3. Copy `.env.example` to `.env`.
4. Change `POSTGRES_PASSWORD` and `GRAFANA_ADMIN_PASSWORD`.
5. Keep `LOCAL_BIND_ADDRESS=127.0.0.1`.
6. Start the stack with `docker compose up -d --build`.
7. Run `scripts/smoke_check.sh --runtime`.
8. Put Caddy, Nginx, or a managed reverse proxy in front of `127.0.0.1:8000`.
9. Only expose Grafana if it is protected or configured for read-only sharing.

## Public Surface

Expose:

- Demo UI/API: `demo-api` on internal host port `${DEMO_API_PORT:-8000}`
- Optional read-only dashboard: Grafana on internal host port `${GRAFANA_PORT:-3000}`

Do not expose:

- Kafka: `9092`, `29092`
- Zookeeper: `2181`
- Schema Registry: `8081`
- TimescaleDB/Postgres: `5432`
- Flink health endpoint: `8090`
- Kafka UI: `8080`

## VPS Reverse Proxy Shape

```text
https://demo.example.com
        |
        v
Reverse proxy on host
        |
        v
127.0.0.1:8000 -> demo-api -> Kafka/Timescale/Grafana inside Docker network
```

Grafana can either stay private behind your own login or be shared through Grafana's [externally shared dashboard](https://grafana.com/docs/grafana/latest/visualizations/dashboards/share-dashboards-panels/shared-dashboards/) feature. Grafana documents that externally shared dashboards are read-only and accessible to anyone with the link, but they can increase datasource query load.

## Managed Platform Path

Use this after the VPS demo is already working and you want a stronger production story.

- Managed Kafka or Confluent Cloud for `metrics_raw`, `metrics_anomalies`, and `incident_commands`.
- Managed Postgres/TimescaleDB for storage.
- Hosted Grafana or Grafana Cloud for dashboards.
- A hosted Docker web service for the FastAPI demo API.
- Separate worker services for producer, consumer, and Flink.

Notes from current platform docs:

- [Render](https://render.com/docs/docker) supports Docker-based services built from a repository Dockerfile.
- [Fly.io](https://fly.io/docs/launch/deploy/) deploys from `fly.toml` and a Dockerfile with `fly deploy`.
- [Railway](https://docs.railway.com/guides/docker-compose) does not run `docker-compose.yml` directly; each Compose service maps to a separate Railway service.

This is a better long-term architecture, but it is more work than the first public demo needs.

## Pre-Publish Checklist

- `cp .env.example .env`
- Change default passwords before any shared environment.
- `docker compose config --quiet`
- `python3 -m pytest`
- `scripts/smoke_check.sh`
- `docker compose up -d --build`
- Wait for `rtm-kafka-init` and `rtm-schema-registrar` to exit with code 0.
- `scripts/smoke_check.sh --runtime`
- Open the demo UI and trigger a short incident.
- Confirm `/api/pipeline/status` reports `status: ok`.
- Confirm Grafana dashboard panels load.
