use std::{
    collections::HashSet,
    env,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use getrandom::fill as fill_random;
use serde::{Deserialize, Serialize};
use tokio::fs;

const DEFAULT_OUTPUT_DIR: &str = "data/metadata";
const DEFAULT_PARSE_STATE: &str = "data/metadata-parse-state.json";
const DEFAULT_CLICKHOUSE_DATABASE: &str = "singularity";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

mod clickhouse;
mod match_events;
mod metadata;
mod player_rows;
mod state;

use clickhouse::ClickHouseClient;
use state::{ParseState, should_skip_duplicate_match};

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MatchRow {
    match_id: u64,
    public_id: String,
    start_time: Option<u32>,
    duration_s: Option<u32>,
    winning_team: Option<i32>,
    game_mode: Option<i32>,
    match_mode: Option<i32>,
    average_badge_team0: Option<u32>,
    average_badge_team1: Option<u32>,
    metadata_version: u64,
}

#[derive(Debug, Clone, Serialize)]
struct MatchProcessedEventRow {
    match_id: u64,
    public_id: String,
    start_time: u32,
    processed_at: u64,
    metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlayerRow {
    match_id: u64,
    account_id: u32,
    player_slot: u32,
    team: Option<i32>,
    hero_id: Option<u32>,
    kills: Option<u32>,
    deaths: Option<u32>,
    assists: Option<u32>,
    net_worth: Option<u32>,
    last_hits: Option<u32>,
    denies: Option<u32>,
    level: Option<u32>,
    assigned_lane: Option<u32>,
    abandon_match_time_s: Option<u32>,
    mvp_rank: Option<u32>,
    metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlayerStatRow {
    match_id: u64,
    account_id: u32,
    player_slot: u32,
    snapshot_time_s: u32,
    net_worth: Option<u32>,
    gold_player: Option<u32>,
    gold_player_orbs: Option<u32>,
    gold_lane_creep_orbs: Option<u32>,
    gold_neutral_creep_orbs: Option<u32>,
    gold_boss: Option<u32>,
    gold_boss_orb: Option<u32>,
    gold_treasure: Option<u32>,
    gold_denied: Option<u32>,
    gold_death_loss: Option<u32>,
    gold_lane_creep: Option<u32>,
    gold_neutral_creep: Option<u32>,
    kills: Option<u32>,
    deaths: Option<u32>,
    assists: Option<u32>,
    creep_kills: Option<u32>,
    neutral_kills: Option<u32>,
    denies: Option<u32>,
    player_damage: Option<u32>,
    player_healing: Option<u32>,
    self_healing: Option<u32>,
    shots_hit: Option<u32>,
    shots_missed: Option<u32>,
    hero_bullets_hit: Option<u32>,
    hero_bullets_hit_crit: Option<u32>,
    bullet_kills: Option<u32>,
    ability_kills: Option<u32>,
    melee_kills: Option<u32>,
    headshot_kills: Option<u32>,
    player_damage_taken: Option<u32>,
    damage_mitigated: Option<u32>,
    damage_absorbed: Option<u32>,
    self_damage: Option<u32>,
    heal_prevented: Option<u32>,
    heal_lost: Option<u32>,
    teammate_healing: Option<u32>,
    teammate_barriering: Option<u32>,
    weapon_power: Option<u32>,
    tech_power: Option<u32>,
    max_health: Option<u32>,
    level: Option<u32>,
    metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlayerItemRow {
    match_id: u64,
    player_slot: u32,
    game_time_s: u32,
    item_id: u32,
    upgrade_id: u32,
    sold_time_s: u32,
    flags: u32,
    imbued_ability_id: u32,
    upgrade_info: u32,
    upgrade_unlocked: bool,
    upgrade_tier: u32,
    metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct DamageRow {
    match_id: u64,
    dealer_slot: u32,
    target_slot: u32,
    source_details_index: u32,
    stat_type: i32,
    source_name: String,
    sample_time_s: u32,
    raw_cumulative_damage: u32,
    cumulative_damage: f64,
    metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct DeathRow {
    match_id: u64,
    victim_slot: u32,
    death_index: u32,
    killer_slot: u32,
    game_time_s: u32,
    time_to_kill_s: Option<f32>,
    victim_x: Option<f32>,
    victim_y: Option<f32>,
    victim_z: Option<f32>,
    killer_x: Option<f32>,
    killer_y: Option<f32>,
    killer_z: Option<f32>,
    death_duration_s: u32,
    metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlayerPositionRow {
    match_id: u64,
    player_slot: u32,
    sample_index: u32,
    time_s: f32,
    x: Option<f32>,
    y: Option<f32>,
    health: Option<u32>,
    combat_type: Option<i32>,
    move_type: Option<i32>,
    metadata_version: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ObjectiveRow {
    match_id: u64,
    event_index: u32,
    objective_id: u32,
    team: i32,
    game_time_s: u32,
    metadata_version: u64,
}

#[derive(Debug)]
struct ParsedMatch {
    match_row: MatchRow,
    player_rows: Vec<PlayerRow>,
    stat_rows: Vec<PlayerStatRow>,
    item_rows: Vec<PlayerItemRow>,
    damage_rows: Vec<DamageRow>,
    death_rows: Vec<DeathRow>,
    position_rows: Vec<PlayerPositionRow>,
    objective_rows: Vec<ObjectiveRow>,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = PathBuf::from(
        env::var("SINGULARITY_METADATA_OUTPUT_DIR")
            .unwrap_or_else(|_| DEFAULT_OUTPUT_DIR.to_string()),
    );
    let state_path = PathBuf::from(
        env::var("SINGULARITY_METADATA_PARSE_STATE_PATH")
            .unwrap_or_else(|_| DEFAULT_PARSE_STATE.to_string()),
    );
    let clickhouse = ClickHouseClient::from_env()?;
    let mut state = ParseState::load(&state_path).await?;
    let mut entries = fs::read_dir(&output_dir).await?;
    let mut parsed_count = 0;
    let mut duplicate_count = 0;
    let mut processed_match_ids = HashSet::new();

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("pb") {
            continue;
        }
        let Some(job_id) = path.file_stem().and_then(|name| name.to_str()) else {
            continue;
        };
        if state.processed.contains_key(job_id) {
            continue;
        }
        let bytes = fs::read(&path).await?;
        let contents = metadata::decode_metadata(&bytes)?;
        let match_id = metadata::metadata_match_id(&contents)?;
        let data_version = metadata::metadata_version(&bytes);
        let check_clickhouse = !processed_match_ids.contains(&match_id)
            && !state.reprocessed_matches.contains(&match_id)
            && !state.reprocess_existing_matches;
        let exists_in_clickhouse = if check_clickhouse {
            clickhouse.match_is_processed(match_id).await?
        } else {
            false
        };
        let already_processed = should_skip_duplicate_match(
            match_id,
            &processed_match_ids,
            &state.reprocessed_matches,
            exists_in_clickhouse,
            state.reprocess_existing_matches,
        );
        if already_processed {
            state.processed.insert(job_id.to_string(), data_version);
            state.save().await?;
            processed_match_ids.insert(match_id);
            duplicate_count += 1;
            continue;
        }
        let mut parsed = metadata::parse_decoded_metadata(contents, data_version)?;
        let public_id = match state.public_ids.entry(parsed.match_row.match_id) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let public_id = generate_public_id()?;
                entry.insert(public_id.clone());
                public_id
            }
        };
        parsed.match_row.public_id = public_id;
        state.save().await?;
        clickhouse
            .insert(
                "matches",
                MATCH_COLUMNS,
                std::slice::from_ref(&parsed.match_row),
            )
            .await?;
        clickhouse
            .insert("match_players", PLAYER_COLUMNS, &parsed.player_rows)
            .await?;
        clickhouse
            .insert("match_player_stats", STAT_COLUMNS, &parsed.stat_rows)
            .await?;
        clickhouse
            .insert("match_player_items", ITEM_COLUMNS, &parsed.item_rows)
            .await?;
        clickhouse
            .insert("match_damage", DAMAGE_COLUMNS, &parsed.damage_rows)
            .await?;
        clickhouse
            .insert("match_deaths", DEATH_COLUMNS, &parsed.death_rows)
            .await?;
        clickhouse
            .insert(
                "match_player_positions",
                POSITION_COLUMNS,
                &parsed.position_rows,
            )
            .await?;
        clickhouse
            .insert(
                "match_objectives",
                OBJECTIVE_COLUMNS,
                &parsed.objective_rows,
            )
            .await?;
        clickhouse
            .insert(
                "match_processed_events",
                EVENT_COLUMNS,
                &[MatchProcessedEventRow {
                    match_id: parsed.match_row.match_id,
                    public_id: parsed.match_row.public_id.clone(),
                    start_time: parsed.match_row.start_time.unwrap_or_default(),
                    processed_at: now_seconds(),
                    metadata_version: parsed.match_row.metadata_version,
                }],
            )
            .await?;
        state
            .processed
            .insert(job_id.to_string(), parsed.match_row.metadata_version);
        processed_match_ids.insert(match_id);
        if state.reprocess_existing_matches {
            state.reprocessed_matches.insert(match_id);
        }
        state.save().await?;
        parsed_count += 1;
    }

    state.reprocess_existing_matches = false;
    state.reprocessed_matches.clear();
    state.save().await?;
    println!("metadata parser: processed={parsed_count} duplicates={duplicate_count}");
    Ok(())
}

const MATCH_COLUMNS: &str = "match_id, public_id, start_time, duration_s, winning_team, game_mode, match_mode, average_badge_team0, average_badge_team1, metadata_version";
const PLAYER_COLUMNS: &str = "match_id, account_id, player_slot, team, hero_id, kills, deaths, assists, net_worth, last_hits, denies, level, assigned_lane, abandon_match_time_s, mvp_rank, metadata_version";
const STAT_COLUMNS: &str = "match_id, account_id, player_slot, snapshot_time_s, net_worth, gold_player, gold_player_orbs, gold_lane_creep_orbs, gold_neutral_creep_orbs, gold_boss, gold_boss_orb, gold_treasure, gold_denied, gold_death_loss, gold_lane_creep, gold_neutral_creep, kills, deaths, assists, creep_kills, neutral_kills, denies, player_damage, player_healing, self_healing, shots_hit, shots_missed, hero_bullets_hit, hero_bullets_hit_crit, bullet_kills, ability_kills, melee_kills, headshot_kills, player_damage_taken, damage_mitigated, damage_absorbed, self_damage, heal_prevented, heal_lost, teammate_healing, teammate_barriering, weapon_power, tech_power, max_health, level, metadata_version";
const ITEM_COLUMNS: &str = "match_id, player_slot, game_time_s, item_id, upgrade_id, sold_time_s, flags, imbued_ability_id, upgrade_info, upgrade_unlocked, upgrade_tier, metadata_version";
const DAMAGE_COLUMNS: &str = "match_id, dealer_slot, target_slot, source_details_index, stat_type, source_name, sample_time_s, raw_cumulative_damage, cumulative_damage, metadata_version";
const DEATH_COLUMNS: &str = "match_id, victim_slot, death_index, killer_slot, game_time_s, time_to_kill_s, victim_x, victim_y, victim_z, killer_x, killer_y, killer_z, death_duration_s, metadata_version";
const POSITION_COLUMNS: &str = "match_id, player_slot, sample_index, time_s, x, y, health, combat_type, move_type, metadata_version";
const OBJECTIVE_COLUMNS: &str =
    "match_id, event_index, objective_id, team, game_time_s, metadata_version";
const EVENT_COLUMNS: &str = "match_id, public_id, start_time, processed_at, metadata_version";

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_public_id() -> Result<String, std::io::Error> {
    let mut bytes = [0_u8; 16];
    fill_random(&mut bytes).map_err(std::io::Error::other)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(format!(
        "{}-{}-{}-{}-{}",
        hex::encode(&bytes[0..4]),
        hex::encode(&bytes[4..6]),
        hex::encode(&bytes[6..8]),
        hex::encode(&bytes[8..10]),
        hex::encode(&bytes[10..16]),
    ))
}
