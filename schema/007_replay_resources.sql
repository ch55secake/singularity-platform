-- Replay-time presence transitions for neutral camps and Sinner's Sacrifice.

CREATE TABLE IF NOT EXISTS singularity.replay_resources
(
    match_id UInt64,
    entity_index Int32,
    event_index UInt32,
    tick Int32,
    time_s Float32,
    resource_kind LowCardinality(String),
    active UInt8,
    x Float32,
    y Float32,
    z Float32,
    parser_version String,
    model_version UInt64
)
ENGINE = ReplacingMergeTree(model_version)
ORDER BY (match_id, resource_kind, entity_index, tick, event_index);
