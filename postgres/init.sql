CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS metrics_raw (
    event_id TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    host TEXT NOT NULL,
    ts BIGINT NOT NULL,
    cpu DOUBLE PRECISION NOT NULL,
    mem DOUBLE PRECISION NOT NULL,
    disk DOUBLE PRECISION NOT NULL,
    net_in DOUBLE PRECISION NOT NULL,
    net_out DOUBLE PRECISION NOT NULL,
    scenario TEXT NOT NULL,
    source TEXT NOT NULL,
    UNIQUE (event_id, time)
);

CREATE TABLE IF NOT EXISTS metrics_anomalies (
    event_id TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    host TEXT NOT NULL,
    ts BIGINT NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    baseline DOUBLE PRECISION NOT NULL,
    std DOUBLE PRECISION NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    severity TEXT NOT NULL,
    scenario TEXT NOT NULL,
    UNIQUE (event_id, time)
);

SELECT create_hypertable('metrics_raw', 'time', if_not_exists => TRUE);
SELECT create_hypertable('metrics_anomalies', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS metrics_raw_host_time_idx
    ON metrics_raw (host, time DESC);

CREATE INDEX IF NOT EXISTS metrics_raw_scenario_time_idx
    ON metrics_raw (scenario, time DESC);

CREATE INDEX IF NOT EXISTS metrics_anomalies_host_metric_time_idx
    ON metrics_anomalies (host, metric, time DESC);

CREATE INDEX IF NOT EXISTS metrics_anomalies_scenario_time_idx
    ON metrics_anomalies (scenario, time DESC);
