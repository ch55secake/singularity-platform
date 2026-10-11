-- Haste-derived replay rows. The parser version and model version make a
-- replay retry deterministic and allow a future parser to replace v1 rows.

CREATE TABLE IF NOT EXISTS singularity.replay_matches
(
    match_id UInt64,
    replay_job_id String,
    parser_version String,
    parsed_at UInt64,
    position_count UInt32,
    model_version UInt64
)
ENGINE = ReplacingMergeTree(model_version)
ORDER BY match_id;

CREATE TABLE IF NOT EXISTS singularity.replay_positions
(
    match_id UInt64,
    entity_index Int32,
    tick Int32,
    time_s Float32,
    x Float32,
    y Float32,
    z Float32,
    parser_version String,
    model_version UInt64
)
ENGINE = ReplacingMergeTree(model_version)
ORDER BY (match_id, entity_index, tick);
