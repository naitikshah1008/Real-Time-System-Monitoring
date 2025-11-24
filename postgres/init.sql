-- ===========================================
-- RAW METRICS TABLE (written by db_consumer.py)
-- ===========================================
CREATE TABLE IF NOT EXISTS metrics_raw (
    id SERIAL PRIMARY KEY,
    host TEXT NOT NULL,
    cpu DOUBLE PRECISION,
    memory DOUBLE PRECISION,
    ts TIMESTAMPTZ NOT NULL,
    load1 DOUBLE PRECISION,
    load5 DOUBLE PRECISION,
    load15 DOUBLE PRECISION,
    disk_used_gb DOUBLE PRECISION,
    disk_used_pct DOUBLE PRECISION,
    net_rx_kbps DOUBLE PRECISION,
    net_tx_kbps DOUBLE PRECISION,
    os TEXT,
    arch TEXT
);

-- Indexes for faster Grafana queries
CREATE INDEX IF NOT EXISTS idx_metrics_raw_ts ON metrics_raw(ts DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_raw_host_ts ON metrics_raw(host, ts DESC);

-- ===========================================
-- ANOMALIES TABLE (written by FLINK)
-- ===========================================
CREATE TABLE IF NOT EXISTS metrics_anomalies (
    id SERIAL PRIMARY KEY,
    host TEXT NOT NULL,
    ts BIGINT NOT NULL,
    cpu DOUBLE PRECISION,
    memory DOUBLE PRECISION,
    metric TEXT,
    score DOUBLE PRECISION,
    rule TEXT
);

CREATE INDEX IF NOT EXISTS idx_metrics_anomalies_ts ON metrics_anomalies(ts DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_anomalies_host_ts ON metrics_anomalies(host, ts DESC);
