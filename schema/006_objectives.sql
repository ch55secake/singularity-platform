-- Objective destruction and mid-boss claim events from match metadata.

CREATE TABLE IF NOT EXISTS singularity.match_objectives
(
    match_id UInt64,
    event_index UInt32,
    objective_id UInt32,
    team Int32,
    game_time_s UInt32,
    metadata_version UInt64
)
ENGINE = ReplacingMergeTree(metadata_version)
ORDER BY (match_id, event_index);
