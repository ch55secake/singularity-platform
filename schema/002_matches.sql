-- Parsed metadata tables. The parser supplies a deterministic metadata_version
-- so re-parsing the same file is idempotent under ReplacingMergeTree.

CREATE TABLE IF NOT EXISTS singularity.matches
(
    match_id UInt64,
    start_time Nullable(UInt32),
    duration_s Nullable(UInt32),
    winning_team Nullable(Int32),
    game_mode Nullable(Int32),
    match_mode Nullable(Int32),
    average_badge_team0 Nullable(UInt32),
    average_badge_team1 Nullable(UInt32),
    metadata_version UInt64
)
ENGINE = ReplacingMergeTree(metadata_version)
ORDER BY match_id;

CREATE TABLE IF NOT EXISTS singularity.match_players
(
    match_id UInt64,
    account_id UInt32,
    player_slot UInt32,
    team Nullable(Int32),
    hero_id Nullable(UInt32),
    kills Nullable(UInt32),
    deaths Nullable(UInt32),
    assists Nullable(UInt32),
    net_worth Nullable(UInt32),
    last_hits Nullable(UInt32),
    denies Nullable(UInt32),
    level Nullable(UInt32),
    assigned_lane Nullable(UInt32),
    abandon_match_time_s Nullable(UInt32),
    mvp_rank Nullable(UInt32),
    metadata_version UInt64
)
ENGINE = ReplacingMergeTree(metadata_version)
ORDER BY (match_id, player_slot, account_id);

CREATE TABLE IF NOT EXISTS singularity.match_player_stats
(
    match_id UInt64,
    account_id UInt32,
    player_slot UInt32,
    snapshot_time_s UInt32,
    net_worth Nullable(UInt32),
    gold_player Nullable(UInt32),
    gold_player_orbs Nullable(UInt32),
    gold_lane_creep_orbs Nullable(UInt32),
    gold_neutral_creep_orbs Nullable(UInt32),
    gold_boss Nullable(UInt32),
    gold_boss_orb Nullable(UInt32),
    gold_treasure Nullable(UInt32),
    gold_denied Nullable(UInt32),
    gold_death_loss Nullable(UInt32),
    gold_lane_creep Nullable(UInt32),
    gold_neutral_creep Nullable(UInt32),
    kills Nullable(UInt32),
    deaths Nullable(UInt32),
    assists Nullable(UInt32),
    creep_kills Nullable(UInt32),
    neutral_kills Nullable(UInt32),
    denies Nullable(UInt32),
    player_damage Nullable(UInt32),
    player_healing Nullable(UInt32),
    self_healing Nullable(UInt32),
    level Nullable(UInt32),
    metadata_version UInt64
)
ENGINE = ReplacingMergeTree(metadata_version)
ORDER BY (match_id, player_slot, account_id, snapshot_time_s);

CREATE VIEW IF NOT EXISTS singularity.match_players_with_derived AS
SELECT
    *,
    toFloat64(ifNull(kills, 0)) / greatest(toFloat64(ifNull(deaths, 0)), 1.0) AS kdr,
    (toFloat64(ifNull(kills, 0)) + toFloat64(ifNull(assists, 0)))
        / greatest(toFloat64(ifNull(deaths, 0)), 1.0) AS kda
FROM singularity.match_players;
