use std::{
    collections::HashSet,
    env,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use getrandom::fill as fill_random;
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs;
use valveprotos::deadlock::{CMsgMatchMetaData, CMsgMatchMetaDataContents};

const DEFAULT_OUTPUT_DIR: &str = "data/metadata";
const DEFAULT_PARSE_STATE: &str = "data/metadata-parse-state.json";
const DEFAULT_CLICKHOUSE_DATABASE: &str = "singularity";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

mod clickhouse;
mod match_events;
mod player_rows;
mod state;

use clickhouse::ClickHouseClient;
use player_rows::{player_item_row, player_row, player_stat_row};
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
        let contents = decode_metadata(&bytes)?;
        let match_id = metadata_match_id(&contents)?;
        let data_version = metadata_version(&bytes);
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
        let mut parsed = parse_decoded_metadata(contents, data_version)?;
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

#[cfg(test)]
fn parse_metadata(bytes: &[u8]) -> Result<ParsedMatch, std::io::Error> {
    parse_decoded_metadata(decode_metadata(bytes)?, metadata_version(bytes))
}

fn decode_metadata(bytes: &[u8]) -> Result<CMsgMatchMetaDataContents, std::io::Error> {
    decode_contents(bytes).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid metadata protobuf")
    })
}

fn metadata_match_id(contents: &CMsgMatchMetaDataContents) -> Result<u64, std::io::Error> {
    contents
        .match_info
        .as_ref()
        .and_then(|info| info.match_id)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "metadata has no match id")
        })
}

fn parse_decoded_metadata(
    contents: CMsgMatchMetaDataContents,
    metadata_version: u64,
) -> Result<ParsedMatch, std::io::Error> {
    let info = contents.match_info.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "metadata has no match info",
        )
    })?;
    let match_id = info.match_id.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "metadata has no match id")
    })?;
    let match_row = MatchRow {
        match_id,
        public_id: String::new(),
        start_time: info.start_time,
        duration_s: info.duration_s,
        winning_team: info.winning_team,
        game_mode: info.game_mode,
        match_mode: info.match_mode,
        average_badge_team0: info.average_badge_team0,
        average_badge_team1: info.average_badge_team1,
        metadata_version,
    };
    let mut player_rows = Vec::with_capacity(info.players.len());
    let mut stat_rows = Vec::new();
    let mut item_rows = Vec::new();
    let damage_rows =
        match_events::damage_rows(match_id, info.damage_matrix.as_ref(), metadata_version);
    let death_rows = match_events::death_rows(match_id, &info.players, metadata_version);
    let position_rows =
        match_events::position_rows(match_id, info.match_paths.as_ref(), metadata_version);
    let objective_rows =
        match_events::objective_rows(match_id, &info.objectives, &info.mid_boss, metadata_version);
    for player in info.players {
        player_rows.push(player_row(match_id, &player, metadata_version));
        item_rows.extend(
            player
                .items
                .iter()
                .map(|item| player_item_row(match_id, &player, item, metadata_version)),
        );
        stat_rows.extend(
            player
                .stats
                .iter()
                .map(|stats| player_stat_row(match_id, &player, stats, metadata_version)),
        );
    }
    Ok(ParsedMatch {
        match_row,
        player_rows,
        stat_rows,
        item_rows,
        damage_rows,
        death_rows,
        position_rows,
        objective_rows,
    })
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Valve's `.meta` files hold a `CMsgMatchMetaData` wrapper (version, match ID)
/// whose `match_details` bytes are the `CMsgMatchMetaDataContents`. Bare
/// contents are still accepted.
fn decode_contents(bytes: &[u8]) -> Option<CMsgMatchMetaDataContents> {
    if let Ok(outer) = CMsgMatchMetaData::decode(bytes)
        && let Some(details) = outer.match_details
        && let Ok(contents) = CMsgMatchMetaDataContents::decode(details.as_slice())
        && contents.match_info.is_some()
    {
        return Some(contents);
    }
    CMsgMatchMetaDataContents::decode(bytes).ok()
}

fn metadata_version(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_le_bytes(digest[..8].try_into().expect("digest has eight bytes"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use valveprotos::deadlock::c_msg_match_meta_data_contents::{
        Deaths, Items, MatchInfo, MidBoss, Objective, PlayerStats, Players, Position,
    };
    use valveprotos::deadlock::c_msg_match_player_damage_matrix::{
        DamageDealer, DamageSource, DamageToPlayer, SourceDetails,
    };
    use valveprotos::deadlock::c_msg_match_player_paths_data::Path as PlayerPath;

    #[test]
    fn falls_back_to_legacy_team_objective_ids() {
        let objective = Objective {
            legacy_objective_id: Some(17),
            ..Default::default()
        };

        assert_eq!(match_events::objective_identity(&objective), Some((1, 1)));
    }

    #[test]
    fn decodes_match_players_and_timeline_rows() {
        let message = CMsgMatchMetaDataContents {
            match_info: Some(MatchInfo {
                match_id: Some(42),
                duration_s: Some(1_800),
                match_paths: Some(valveprotos::deadlock::CMsgMatchPlayerPathsData {
                    version: Some(1),
                    interval_s: Some(1.0),
                    x_resolution: Some(100),
                    y_resolution: Some(200),
                    paths: vec![PlayerPath {
                        player_slot: Some(3),
                        x_min: Some(-10.0),
                        y_min: Some(-20.0),
                        x_max: Some(10.0),
                        y_max: Some(20.0),
                        x_pos: vec![0, 50],
                        y_pos: vec![100, 200],
                        health: vec![100, 90],
                        combat_type: vec![1, 2],
                        move_type: vec![0, 3],
                    }],
                }),
                damage_matrix: Some(valveprotos::deadlock::CMsgMatchPlayerDamageMatrix {
                    damage_dealers: vec![DamageDealer {
                        dealer_player_slot: Some(3),
                        damage_sources: vec![DamageSource {
                            source_details_index: Some(0),
                            damage_to_players: vec![DamageToPlayer {
                                target_player_slot: Some(4),
                                damage: vec![20, 41],
                            }],
                        }],
                    }],
                    sample_time_s: vec![60, 120],
                    source_details: Some(SourceDetails {
                        stat_type: vec![0],
                        source_name: vec!["ability_test".to_string()],
                    }),
                }),
                objectives: vec![
                    Objective {
                        team_objective_id: Some(1),
                        team: Some(1),
                        destroyed_time_s: Some(240),
                        ..Default::default()
                    },
                    Objective {
                        team_objective_id: Some(5),
                        team: Some(0),
                        destroyed_time_s: None,
                        ..Default::default()
                    },
                ],
                mid_boss: vec![MidBoss {
                    team_claimed: Some(0),
                    destroyed_time_s: Some(900),
                    ..Default::default()
                }],
                winning_team: Some(2),
                players: vec![Players {
                    account_id: Some(7),
                    player_slot: Some(3),
                    hero_id: Some(10),
                    death_details: vec![Deaths {
                        game_time_s: Some(300),
                        time_to_kill_s: Some(2.5),
                        killer_player_slot: Some(4),
                        death_pos: Some(Position {
                            x: Some(10.0),
                            y: Some(20.0),
                            z: Some(30.0),
                        }),
                        killer_pos: Some(Position {
                            x: Some(40.0),
                            y: Some(50.0),
                            z: Some(60.0),
                        }),
                        death_duration_s: Some(4),
                    }],
                    items: vec![Items {
                        game_time_s: Some(95),
                        item_id: Some(1_841_661_392),
                        upgrade_id: Some(0),
                        upgrade_info: Some(196_608),
                        ..Default::default()
                    }],
                    kills: Some(8),
                    deaths: Some(2),
                    assists: Some(11),
                    stats: vec![PlayerStats {
                        time_stamp_s: Some(180),
                        net_worth: Some(4_200),
                        player_damage: Some(9_000),
                        shots_hit: Some(1_701),
                        shots_missed: Some(1_503),
                        hero_bullets_hit: Some(425),
                        hero_bullets_hit_crit: Some(49),
                        bullet_kills: Some(5),
                        ability_kills: Some(2),
                        melee_kills: Some(1),
                        headshot_kills: Some(3),
                        player_damage_taken: Some(7_500),
                        damage_mitigated: Some(1_200),
                        damage_absorbed: Some(350),
                        self_damage: Some(25),
                        heal_prevented: Some(100),
                        heal_lost: Some(50),
                        teammate_healing: Some(400),
                        teammate_barriering: Some(200),
                        weapon_power: Some(12),
                        tech_power: Some(8),
                        max_health: Some(850),
                        level: Some(6),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        };
        let bytes = message.encode_to_vec();

        let parsed = parse_metadata(&bytes).expect("metadata parses");

        assert_eq!(parsed.match_row.match_id, 42);
        assert_eq!(parsed.match_row.winning_team, Some(2));
        assert_eq!(parsed.player_rows[0].kills, Some(8));
        assert_eq!(parsed.item_rows.len(), 1);
        assert_eq!(parsed.item_rows[0].item_id, 1_841_661_392);
        assert!(parsed.item_rows[0].upgrade_unlocked);
        assert_eq!(parsed.item_rows[0].upgrade_tier, 1);
        assert_eq!(parsed.death_rows.len(), 1);
        assert_eq!(parsed.death_rows[0].victim_slot, 3);
        assert_eq!(parsed.death_rows[0].killer_slot, 4);
        assert_eq!(parsed.death_rows[0].victim_x, Some(10.0));
        assert_eq!(parsed.death_rows[0].killer_z, Some(60.0));
        assert_eq!(parsed.position_rows.len(), 2);
        assert_eq!(parsed.position_rows[0].x, Some(-10.0));
        assert_eq!(parsed.position_rows[0].y, Some(0.0));
        assert_eq!(parsed.position_rows[1].x, Some(0.0));
        assert_eq!(parsed.position_rows[1].y, Some(20.0));
        assert_eq!(parsed.position_rows[1].combat_type, Some(2));
        assert_eq!(parsed.position_rows[1].move_type, Some(3));
        assert_eq!(parsed.objective_rows.len(), 2);
        assert_eq!(parsed.objective_rows[0].objective_id, 1);
        assert_eq!(parsed.objective_rows[0].team, 1);
        assert_eq!(parsed.objective_rows[0].game_time_s, 240);
        assert_eq!(parsed.objective_rows[1].objective_id, 16);
        assert_eq!(parsed.objective_rows[1].team, 0);
        assert_eq!(parsed.objective_rows[1].game_time_s, 900);
        assert_eq!(parsed.damage_rows.len(), 2);
        assert_eq!(parsed.damage_rows[1].source_name, "ability_test");
        assert_eq!(parsed.damage_rows[1].sample_time_s, 120);
        assert_eq!(parsed.damage_rows[1].raw_cumulative_damage, 41);
        assert_eq!(parsed.damage_rows[1].cumulative_damage, 20.5);
        assert_eq!(parsed.stat_rows[0].snapshot_time_s, 180);
        assert_eq!(parsed.stat_rows[0].player_damage, Some(9_000));
        assert_eq!(parsed.stat_rows[0].shots_hit, Some(1_701));
        assert_eq!(parsed.stat_rows[0].hero_bullets_hit_crit, Some(49));
        assert_eq!(parsed.stat_rows[0].headshot_kills, Some(3));
        assert_eq!(parsed.stat_rows[0].player_damage_taken, Some(7_500));
        assert_eq!(parsed.match_row.metadata_version, metadata_version(&bytes));
    }

    #[test]
    fn decodes_contents_inside_valve_wrapper() {
        let contents = CMsgMatchMetaDataContents {
            match_info: Some(MatchInfo {
                match_id: Some(42),
                duration_s: Some(1_800),
                ..Default::default()
            }),
        };
        let wrapped = CMsgMatchMetaData {
            version: Some(1),
            match_details: Some(contents.encode_to_vec()),
            match_id: Some(42),
        }
        .encode_to_vec();

        let parsed = parse_metadata(&wrapped).expect("wrapped metadata parses");

        assert_eq!(parsed.match_row.match_id, 42);
        assert_eq!(parsed.match_row.duration_s, Some(1_800));
    }

    #[test]
    fn parses_sample_metadata_files() {
        // Decompressed .meta files can be dropped in tests/fixtures/ to check
        // real Valve data; the directory is optional.
        let Ok(entries) = std::fs::read_dir("tests/fixtures") else {
            return;
        };
        for entry in entries.flatten() {
            let bytes = std::fs::read(entry.path()).expect("fixture reads");
            let parsed = parse_metadata(&bytes).expect("sample metadata parses");
            assert!(parsed.match_row.match_id > 0);
        }
    }

    #[test]
    fn rejects_metadata_without_match_info() {
        let error = parse_metadata(&CMsgMatchMetaDataContents::default().encode_to_vec())
            .expect_err("missing match info is invalid");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
