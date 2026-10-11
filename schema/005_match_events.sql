-- Durable processed-match notifications. The platform read API exposes these
-- rows to optional consumers such as the Discord bot.

CREATE TABLE IF NOT EXISTS singularity.match_processed_events
(
    match_id UInt64,
    public_id String,
    start_time UInt32,
    processed_at UInt64,
    metadata_version UInt64
)
ENGINE = ReplacingMergeTree(metadata_version)
ORDER BY match_id;
