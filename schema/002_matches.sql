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

CREATE TABLE IF NOT EXISTS singularity.match_player_items
(
    match_id UInt64,
    player_slot UInt32,
    game_time_s UInt32,
    item_id UInt32,
    upgrade_id UInt32,
    sold_time_s UInt32,
    flags UInt32,
    imbued_ability_id UInt32,
    upgrade_info UInt32,
    upgrade_unlocked Bool,
    upgrade_tier UInt32,
    metadata_version UInt64
)
ENGINE = ReplacingMergeTree(metadata_version)
ORDER BY (match_id, player_slot, game_time_s, item_id, upgrade_id, upgrade_info);

CREATE TABLE IF NOT EXISTS singularity.match_damage
(
    match_id UInt64,
    dealer_slot UInt32,
    target_slot UInt32,
    source_details_index UInt32,
    stat_type Int32,
    source_name String,
    sample_time_s UInt32,
    raw_cumulative_damage UInt32,
    cumulative_damage Float64,
    metadata_version UInt64
)
ENGINE = ReplacingMergeTree(metadata_version)
ORDER BY (
    match_id,
    dealer_slot,
    target_slot,
    source_details_index,
    stat_type,
    sample_time_s
);

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
    shots_hit Nullable(UInt32),
    shots_missed Nullable(UInt32),
    hero_bullets_hit Nullable(UInt32),
    hero_bullets_hit_crit Nullable(UInt32),
    bullet_kills Nullable(UInt32),
    ability_kills Nullable(UInt32),
    melee_kills Nullable(UInt32),
    headshot_kills Nullable(UInt32),
    player_damage_taken Nullable(UInt32),
    damage_mitigated Nullable(UInt32),
    damage_absorbed Nullable(UInt32),
    self_damage Nullable(UInt32),
    heal_prevented Nullable(UInt32),
    heal_lost Nullable(UInt32),
    teammate_healing Nullable(UInt32),
    teammate_barriering Nullable(UInt32),
    weapon_power Nullable(UInt32),
    tech_power Nullable(UInt32),
    max_health Nullable(UInt32),
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

CREATE VIEW IF NOT EXISTS singularity.match_damage_totals AS
SELECT
    match_id,
    dealer_slot,
    target_slot,
    source_details_index,
    stat_type,
    source_name,
    argMax(cumulative_damage, sample_time_s) AS total_damage
FROM singularity.match_damage
GROUP BY
    match_id,
    dealer_slot,
    target_slot,
    source_details_index,
    stat_type,
    source_name;

CREATE VIEW IF NOT EXISTS singularity.match_damage_player_reconciliation AS
WITH matrix_totals AS
(
    SELECT
        damage.match_id,
        damage.dealer_slot,
        sum(damage.total_damage) AS matrix_player_damage
    FROM singularity.match_damage_totals AS damage
    INNER JOIN singularity.match_players AS targets
        ON targets.match_id = damage.match_id
        AND targets.player_slot = damage.target_slot
    WHERE damage.stat_type = 0
    GROUP BY damage.match_id, damage.dealer_slot
),
timeline_totals AS
(
    SELECT
        match_id,
        player_slot,
        argMax(ifNull(player_damage, 0), snapshot_time_s) AS timeline_player_damage
    FROM singularity.match_player_stats
    GROUP BY match_id, player_slot
)
SELECT
    matrix.match_id,
    matrix.dealer_slot,
    matrix.matrix_player_damage,
    toFloat64(timeline.timeline_player_damage) AS timeline_player_damage,
    matrix.matrix_player_damage - toFloat64(timeline.timeline_player_damage)
        AS damage_difference,
    abs(damage_difference) < 0.000001 AS matches_player_damage
FROM matrix_totals AS matrix
INNER JOIN timeline_totals AS timeline
    ON timeline.match_id = matrix.match_id
    AND timeline.player_slot = matrix.dealer_slot;

CREATE VIEW IF NOT EXISTS singularity.match_player_stats_with_derived AS
SELECT
    stats.*,
    matches.duration_s AS match_duration_s,
    if(
        ifNull(shots_hit, 0) + ifNull(shots_missed, 0) = 0,
        0.0,
        toFloat64(ifNull(shots_hit, 0))
            / (ifNull(shots_hit, 0) + ifNull(shots_missed, 0))
    ) AS accuracy,
    if(
        ifNull(hero_bullets_hit, 0) = 0,
        0.0,
        toFloat64(ifNull(hero_bullets_hit_crit, 0)) / ifNull(hero_bullets_hit, 0)
    ) AS hero_crit_rate,
    ifNull(bullet_kills, 0) + ifNull(ability_kills, 0) + ifNull(melee_kills, 0)
        AS total_kills_by_type,
    if(
        ifNull(bullet_kills, 0) + ifNull(ability_kills, 0)
            + ifNull(melee_kills, 0) = 0,
        0.0,
        toFloat64(ifNull(bullet_kills, 0))
            / (ifNull(bullet_kills, 0) + ifNull(ability_kills, 0)
                + ifNull(melee_kills, 0))
    ) AS bullet_kill_share,
    if(
        ifNull(bullet_kills, 0) + ifNull(ability_kills, 0)
            + ifNull(melee_kills, 0) = 0,
        0.0,
        toFloat64(ifNull(ability_kills, 0))
            / (ifNull(bullet_kills, 0) + ifNull(ability_kills, 0)
                + ifNull(melee_kills, 0))
    ) AS ability_kill_share,
    if(
        ifNull(bullet_kills, 0) + ifNull(ability_kills, 0)
            + ifNull(melee_kills, 0) = 0,
        0.0,
        toFloat64(ifNull(melee_kills, 0))
            / (ifNull(bullet_kills, 0) + ifNull(ability_kills, 0)
                + ifNull(melee_kills, 0))
    ) AS melee_kill_share,
    if(
        ifNull(bullet_kills, 0) + ifNull(ability_kills, 0)
            + ifNull(melee_kills, 0) = 0,
        0.0,
        toFloat64(ifNull(headshot_kills, 0))
            / (ifNull(bullet_kills, 0) + ifNull(ability_kills, 0)
                + ifNull(melee_kills, 0))
    ) AS headshot_kill_share,
    toFloat64(ifNull(player_damage_taken, 0)) * 60.0
        / greatest(toFloat64(ifNull(matches.duration_s, snapshot_time_s)), 60.0)
        AS player_damage_taken_per_minute
FROM singularity.match_player_stats AS stats
LEFT JOIN singularity.matches AS matches ON matches.match_id = stats.match_id;

CREATE VIEW IF NOT EXISTS singularity.player_stat_averages AS
SELECT
    account_id,
    avg(accuracy) AS avg_accuracy,
    avg(hero_crit_rate) AS avg_hero_crit_rate,
    avg(bullet_kill_share) AS avg_bullet_kill_share,
    avg(ability_kill_share) AS avg_ability_kill_share,
    avg(melee_kill_share) AS avg_melee_kill_share,
    avg(headshot_kill_share) AS avg_headshot_kill_share,
    avg(player_damage_taken_per_minute) AS avg_damage_taken_per_minute,
    avg(damage_mitigated) AS avg_damage_mitigated,
    avg(damage_absorbed) AS avg_damage_absorbed,
    avg(self_damage) AS avg_self_damage,
    avg(heal_prevented) AS avg_heal_prevented,
    avg(heal_lost) AS avg_heal_lost,
    avg(teammate_healing) AS avg_teammate_healing,
    avg(teammate_barriering) AS avg_teammate_barriering,
    count() AS matches
FROM
(
    SELECT
        derived.match_id,
        derived.account_id,
        derived.player_slot,
        derived.accuracy,
        derived.hero_crit_rate,
        derived.bullet_kill_share,
        derived.ability_kill_share,
        derived.melee_kill_share,
        derived.headshot_kill_share,
        derived.player_damage_taken_per_minute,
        derived.damage_mitigated,
        derived.damage_absorbed,
        derived.self_damage,
        derived.heal_prevented,
        derived.heal_lost,
        derived.teammate_healing,
        derived.teammate_barriering,
        row_number() OVER (
            PARTITION BY derived.match_id, derived.account_id, derived.player_slot
            ORDER BY derived.snapshot_time_s DESC
        ) AS snapshot_rank
    FROM singularity.match_player_stats_with_derived AS derived
)
WHERE snapshot_rank = 1
GROUP BY account_id;
