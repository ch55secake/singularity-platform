-- Authenticated requests from the read API to force a replay resource refresh.

CREATE TABLE IF NOT EXISTS singularity.replay_reparse_requests
(
    match_id UInt64,
    request_id UInt64,
    requested_at UInt64,
    status LowCardinality(String),
    error_code LowCardinality(String),
    model_version UInt64
)
ENGINE = ReplacingMergeTree(model_version)
ORDER BY (match_id, request_id);
