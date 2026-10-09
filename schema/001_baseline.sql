-- Singularity Platform baseline schema.
-- The v0.1.0 service writes JSONL; this schema is the contract for the
-- ClickHouse-backed processing milestone.

CREATE DATABASE IF NOT EXISTS singularity;

CREATE TABLE IF NOT EXISTS singularity.match_salts
(
    match_id UInt64,
    cluster_id Nullable(UInt32),
    metadata_salt Nullable(UInt32),
    replay_salt Nullable(UInt32),
    username Nullable(String),
    collector_id Nullable(String),
    received_at DateTime DEFAULT now(),
    source String DEFAULT 'cache-collector'
)
ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(received_at)
ORDER BY (match_id, cluster_id, metadata_salt, replay_salt);
