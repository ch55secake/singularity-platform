use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use prost::Message;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};
use valveprotos::deadlock::CMsgMatchMetaDataContents;

use valveprotos::deadlock::c_msg_match_meta_data_contents::{Items, Players};

const DEFAULT_OUTPUT_DIR: &str = "data/metadata";
const DEFAULT_PARSE_STATE: &str = "data/metadata-parse-state.json";
const DEFAULT_CLICKHOUSE_DATABASE: &str = "singularity";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MatchRow {
    match_id: u64,
    start_time: Option<u32>,
    duration_s: Option<u32>,
    winning_team: Option<i32>,
    game_mode: Option<i32>,
    match_mode: Option<i32>,
    average_badge_team0: Option<u32>,
    average_badge_team1: Option<u32>,
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

#[derive(Debug)]
struct ParsedMatch {
    match_row: MatchRow,
    player_rows: Vec<PlayerRow>,
    stat_rows: Vec<PlayerStatRow>,
    item_rows: Vec<PlayerItemRow>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ParseStateFile {
    version: u32,
    #[serde(default)]
    processed: BTreeMap<String, u64>,
}

struct ParseState {
    path: PathBuf,
    processed: BTreeMap<String, u64>,
}

struct ClickHouseConfig {
    url: String,
    database: String,
    user: Option<String>,
    password: Option<String>,
    timeout: Duration,
}

struct ClickHouseClient {
    http: Client,
    config: ClickHouseConfig,
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
        let parsed = parse_metadata(&bytes)?;
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
        state
            .processed
            .insert(job_id.to_string(), parsed.match_row.metadata_version);
        state.save().await?;
        parsed_count += 1;
    }

    state.save().await?;
    println!("metadata parser: processed={parsed_count}");
    Ok(())
}

const MATCH_COLUMNS: &str = "match_id, start_time, duration_s, winning_team, game_mode, match_mode, average_badge_team0, average_badge_team1, metadata_version";
const PLAYER_COLUMNS: &str = "match_id, account_id, player_slot, team, hero_id, kills, deaths, assists, net_worth, last_hits, denies, level, assigned_lane, abandon_match_time_s, mvp_rank, metadata_version";
const STAT_COLUMNS: &str = "match_id, account_id, player_slot, snapshot_time_s, net_worth, gold_player, gold_player_orbs, gold_lane_creep_orbs, gold_neutral_creep_orbs, gold_boss, gold_boss_orb, gold_treasure, gold_denied, gold_death_loss, gold_lane_creep, gold_neutral_creep, kills, deaths, assists, creep_kills, neutral_kills, denies, player_damage, player_healing, self_healing, shots_hit, shots_missed, hero_bullets_hit, hero_bullets_hit_crit, bullet_kills, ability_kills, melee_kills, headshot_kills, player_damage_taken, damage_mitigated, damage_absorbed, self_damage, heal_prevented, heal_lost, teammate_healing, teammate_barriering, weapon_power, tech_power, max_health, level, metadata_version";
const ITEM_COLUMNS: &str = "match_id, player_slot, game_time_s, item_id, upgrade_id, sold_time_s, flags, imbued_ability_id, upgrade_info, upgrade_unlocked, upgrade_tier, metadata_version";

impl ClickHouseClient {
    fn from_env() -> Result<Self, std::io::Error> {
        let url = env::var("SINGULARITY_CLICKHOUSE_URL").map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SINGULARITY_CLICKHOUSE_URL is required for metadata-parser",
            )
        })?;
        let timeout = env::var("SINGULARITY_METADATA_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| DEFAULT_TIMEOUT_SECONDS.to_string())
            .parse::<u64>()
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid parser timeout")
            })?;
        if timeout == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "parser timeout must be greater than zero",
            ));
        }
        let config = ClickHouseConfig {
            url,
            database: env::var("SINGULARITY_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| DEFAULT_CLICKHOUSE_DATABASE.to_string()),
            user: env::var("SINGULARITY_CLICKHOUSE_USER").ok(),
            password: env::var("SINGULARITY_CLICKHOUSE_PASSWORD").ok(),
            timeout: Duration::from_secs(timeout),
        };
        let http = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "invalid ClickHouse client",
                )
            })?;
        Ok(Self { http, config })
    }

    async fn insert<T: Serialize>(
        &self,
        table: &str,
        columns: &str,
        rows: &[T],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut body, row)?;
            body.push(b'\n');
        }
        let query = format!(
            "INSERT INTO {}.{} ({}) FORMAT JSONEachRow",
            self.config.database, table, columns
        );
        let mut request = self
            .http
            .post(&self.config.url)
            .query(&[("query", query)])
            .header("content-type", "application/json")
            .body(body);
        if let Some(user) = &self.config.user {
            request = request.basic_auth(user, self.config.password.as_deref());
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(std::io::Error::other("ClickHouse insert failed").into());
        }
        Ok(())
    }
}

impl ParseState {
    async fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = match fs::read(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if contents.is_empty() {
            return Ok(Self {
                path: path.to_path_buf(),
                processed: BTreeMap::new(),
            });
        }
        #[cfg(unix)]
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
        let file: ParseStateFile = serde_json::from_slice(&contents)?;
        if file.version != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported parser state version",
            )
            .into());
        }
        Ok(Self {
            path: path.to_path_buf(),
            processed: file.processed,
        })
    }

    async fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).await?;
        }
        let contents = serde_json::to_vec_pretty(&ParseStateFile {
            version: 1,
            processed: self.processed.clone(),
        })?;
        let temp_path = self.path.with_file_name(format!(
            ".{}.tmp-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("metadata-parse-state.json"),
            std::process::id()
        ));
        let mut temp = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)
            .await?;
        temp.write_all(&contents).await?;
        temp.flush().await?;
        temp.sync_data().await?;
        drop(temp);
        #[cfg(unix)]
        fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(0o600)).await?;
        fs::rename(temp_path, &self.path).await?;
        Ok(())
    }
}

fn parse_metadata(bytes: &[u8]) -> Result<ParsedMatch, std::io::Error> {
    let contents = CMsgMatchMetaDataContents::decode(bytes).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid metadata protobuf")
    })?;
    let info = contents.match_info.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "metadata has no match info",
        )
    })?;
    let match_id = info.match_id.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "metadata has no match id")
    })?;
    let metadata_version = metadata_version(bytes);
    let match_row = MatchRow {
        match_id,
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
    })
}

fn player_row(match_id: u64, player: &Players, metadata_version: u64) -> PlayerRow {
    PlayerRow {
        match_id,
        account_id: player.account_id.unwrap_or_default(),
        player_slot: player.player_slot.unwrap_or_default(),
        team: player.team,
        hero_id: player.hero_id,
        kills: player.kills,
        deaths: player.deaths,
        assists: player.assists,
        net_worth: player.net_worth,
        last_hits: player.last_hits,
        denies: player.denies,
        level: player.level,
        assigned_lane: player.assigned_lane,
        abandon_match_time_s: player.abandon_match_time_s,
        mvp_rank: player.mvp_rank,
        metadata_version,
    }
}

fn player_stat_row(
    match_id: u64,
    player: &Players,
    stats: &valveprotos::deadlock::c_msg_match_meta_data_contents::PlayerStats,
    metadata_version: u64,
) -> PlayerStatRow {
    PlayerStatRow {
        match_id,
        account_id: player.account_id.unwrap_or_default(),
        player_slot: player.player_slot.unwrap_or_default(),
        snapshot_time_s: stats.time_stamp_s.unwrap_or_default(),
        net_worth: stats.net_worth,
        gold_player: stats.gold_player,
        gold_player_orbs: stats.gold_player_orbs,
        gold_lane_creep_orbs: stats.gold_lane_creep_orbs,
        gold_neutral_creep_orbs: stats.gold_neutral_creep_orbs,
        gold_boss: stats.gold_boss,
        gold_boss_orb: stats.gold_boss_orb,
        gold_treasure: stats.gold_treasure,
        gold_denied: stats.gold_denied,
        gold_death_loss: stats.gold_death_loss,
        gold_lane_creep: stats.gold_lane_creep,
        gold_neutral_creep: stats.gold_neutral_creep,
        kills: stats.kills,
        deaths: stats.deaths,
        assists: stats.assists,
        creep_kills: stats.creep_kills,
        neutral_kills: stats.neutral_kills,
        denies: stats.denies,
        player_damage: stats.player_damage,
        player_healing: stats.player_healing,
        self_healing: stats.self_healing,
        shots_hit: stats.shots_hit,
        shots_missed: stats.shots_missed,
        hero_bullets_hit: stats.hero_bullets_hit,
        hero_bullets_hit_crit: stats.hero_bullets_hit_crit,
        bullet_kills: stats.bullet_kills,
        ability_kills: stats.ability_kills,
        melee_kills: stats.melee_kills,
        headshot_kills: stats.headshot_kills,
        player_damage_taken: stats.player_damage_taken,
        damage_mitigated: stats.damage_mitigated,
        damage_absorbed: stats.damage_absorbed,
        self_damage: stats.self_damage,
        heal_prevented: stats.heal_prevented,
        heal_lost: stats.heal_lost,
        teammate_healing: stats.teammate_healing,
        teammate_barriering: stats.teammate_barriering,
        weapon_power: stats.weapon_power,
        tech_power: stats.tech_power,
        max_health: stats.max_health,
        level: stats.level,
        metadata_version,
    }
}

fn player_item_row(
    match_id: u64,
    player: &Players,
    item: &Items,
    metadata_version: u64,
) -> PlayerItemRow {
    let (upgrade_unlocked, upgrade_tier) = decode_upgrade_info(item.upgrade_info);
    PlayerItemRow {
        match_id,
        player_slot: player.player_slot.unwrap_or_default(),
        game_time_s: item.game_time_s.unwrap_or_default(),
        item_id: item.item_id.unwrap_or_default(),
        upgrade_id: item.upgrade_id.unwrap_or_default(),
        sold_time_s: item.sold_time_s.unwrap_or_default(),
        flags: item.flags.unwrap_or_default(),
        imbued_ability_id: item.imbued_ability_id.unwrap_or_default(),
        upgrade_info: item.upgrade_info.unwrap_or_default(),
        upgrade_unlocked,
        upgrade_tier,
        metadata_version,
    }
}

fn decode_upgrade_info(upgrade_info: Option<u32>) -> (bool, u32) {
    upgrade_info
        .map(|value| (value & (1 << 16) != 0, ((value >> 17) & 0xf).count_ones()))
        .unwrap_or_default()
}

fn metadata_version(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_le_bytes(digest[..8].try_into().expect("digest has eight bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use valveprotos::deadlock::c_msg_match_meta_data_contents::{Items, MatchInfo, PlayerStats};

    #[test]
    fn decodes_match_players_and_timeline_rows() {
        let message = CMsgMatchMetaDataContents {
            match_info: Some(MatchInfo {
                match_id: Some(42),
                duration_s: Some(1_800),
                winning_team: Some(2),
                players: vec![Players {
                    account_id: Some(7),
                    player_slot: Some(3),
                    hero_id: Some(10),
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
        assert_eq!(parsed.stat_rows[0].snapshot_time_s, 180);
        assert_eq!(parsed.stat_rows[0].player_damage, Some(9_000));
        assert_eq!(parsed.stat_rows[0].shots_hit, Some(1_701));
        assert_eq!(parsed.stat_rows[0].hero_bullets_hit_crit, Some(49));
        assert_eq!(parsed.stat_rows[0].headshot_kills, Some(3));
        assert_eq!(parsed.stat_rows[0].player_damage_taken, Some(7_500));
        assert_eq!(parsed.match_row.metadata_version, metadata_version(&bytes));
    }

    #[test]
    fn rejects_metadata_without_match_info() {
        let error = parse_metadata(&CMsgMatchMetaDataContents::default().encode_to_vec())
            .expect_err("missing match info is invalid");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
