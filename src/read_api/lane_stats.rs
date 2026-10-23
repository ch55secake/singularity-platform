use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use serde::Serialize;
use serde_json::Value;

use crate::{AppState, ErrorBody};

use super::matches::CorePlayer;
use super::{
    Hero, ReadApiState, api_error, authorize, load_core, match_selector, number_u32, optional_u32,
    query_rows,
};

pub(super) const LANE_PHASE_END_S: u32 = 600;

#[derive(Debug, Serialize)]
pub(crate) struct LaneStats {
    uuid: String,
    match_id: u64,
    lane_phase_end_s: u32,
    players: Vec<LanePlayer>,
    deaths: Vec<LaneDeath>,
}

#[derive(Debug, Serialize)]
struct LanePlayer {
    slot: u32,
    team: i32,
    hero: Hero,
    snapshot_time_s: Option<u32>,
    kills: Option<u32>,
    deaths: Option<u32>,
    assists: Option<u32>,
    creep_kills: Option<u32>,
    neutral_kills: Option<u32>,
    denies: Option<u32>,
    hero_damage: Option<u32>,
    souls: LaneSouls,
}

#[derive(Debug, Serialize)]
struct LaneSouls {
    net_worth: Option<u32>,
    player: Option<u32>,
    player_orbs: Option<u32>,
    lane_creeps: Option<u32>,
    lane_creep_orbs: Option<u32>,
    neutral_creeps: Option<u32>,
    neutral_creep_orbs: Option<u32>,
    boss: Option<u32>,
    boss_orbs: Option<u32>,
    treasure: Option<u32>,
    denied: Option<u32>,
    lost_on_death: Option<u32>,
}

#[derive(Debug, Serialize)]
struct LaneDeath {
    t: u32,
    victim: u32,
    killer: u32,
}

pub(crate) async fn get_lane_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
) -> Result<Json<LaneStats>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let selector = match_selector(&identifier)?;
    let Some(core) = load_core(&state.read_api, &selector).await? else {
        return Err(api_error(StatusCode::NOT_FOUND, "match not found"));
    };

    let snapshots = load_lane_snapshots(&state.read_api, core.match_id).await?;
    let deaths = load_lane_deaths(&state.read_api, core.match_id).await?;
    let players = core
        .players
        .iter()
        .map(|player| lane_player(player, snapshots.get(&player.slot)))
        .collect();

    Ok(Json(LaneStats {
        uuid: core.uuid,
        match_id: core.match_id,
        lane_phase_end_s: LANE_PHASE_END_S,
        players,
        deaths,
    }))
}

fn lane_player(player: &CorePlayer, row: Option<&Value>) -> LanePlayer {
    LanePlayer {
        slot: player.slot,
        team: player.team,
        hero: player.hero.clone(),
        snapshot_time_s: row.and_then(|row| number_u32(row, "latest_snapshot_time_s")),
        kills: metric(row, "kills"),
        deaths: metric(row, "deaths"),
        assists: metric(row, "assists"),
        creep_kills: metric(row, "creep_kills"),
        neutral_kills: metric(row, "neutral_kills"),
        denies: metric(row, "denies"),
        hero_damage: metric(row, "hero_damage"),
        souls: LaneSouls {
            net_worth: metric(row, "net_worth"),
            player: metric(row, "player_souls"),
            player_orbs: metric(row, "player_orb_souls"),
            lane_creeps: metric(row, "lane_creep_souls"),
            lane_creep_orbs: metric(row, "lane_creep_orb_souls"),
            neutral_creeps: metric(row, "neutral_creep_souls"),
            neutral_creep_orbs: metric(row, "neutral_creep_orb_souls"),
            boss: metric(row, "boss_souls"),
            boss_orbs: metric(row, "boss_orb_souls"),
            treasure: metric(row, "treasure_souls"),
            denied: metric(row, "denied_souls"),
            lost_on_death: metric(row, "death_loss_souls"),
        },
    }
}

async fn load_lane_snapshots(
    state: &ReadApiState,
    match_id: u64,
) -> Result<HashMap<u32, Value>, (StatusCode, Json<ErrorBody>)> {
    let sql = lane_snapshot_query(&state.database, match_id);
    Ok(query_rows(state, &sql)
        .await?
        .into_iter()
        .filter_map(|row| Some((number_u32(&row, "player_slot")?, row)))
        .collect())
}

fn lane_snapshot_query(database: &str, match_id: u64) -> String {
    format!(
        "SELECT player_slot, max(snapshot_time_s) AS latest_snapshot_time_s, \
         argMax(net_worth, snapshot_time_s) AS net_worth, \
         argMax(kills, snapshot_time_s) AS kills, argMax(deaths, snapshot_time_s) AS deaths, \
         argMax(assists, snapshot_time_s) AS assists, \
         argMax(creep_kills, snapshot_time_s) AS creep_kills, \
         argMax(neutral_kills, snapshot_time_s) AS neutral_kills, \
         argMax(denies, snapshot_time_s) AS denies, \
         argMax(player_damage, snapshot_time_s) AS hero_damage, \
         argMax(gold_player, snapshot_time_s) AS player_souls, \
         argMax(gold_player_orbs, snapshot_time_s) AS player_orb_souls, \
         argMax(gold_lane_creep, snapshot_time_s) AS lane_creep_souls, \
         argMax(gold_lane_creep_orbs, snapshot_time_s) AS lane_creep_orb_souls, \
         argMax(gold_neutral_creep, snapshot_time_s) AS neutral_creep_souls, \
         argMax(gold_neutral_creep_orbs, snapshot_time_s) AS neutral_creep_orb_souls, \
         argMax(gold_boss, snapshot_time_s) AS boss_souls, \
         argMax(gold_boss_orb, snapshot_time_s) AS boss_orb_souls, \
         argMax(gold_treasure, snapshot_time_s) AS treasure_souls, \
         argMax(gold_denied, snapshot_time_s) AS denied_souls, \
         argMax(gold_death_loss, snapshot_time_s) AS death_loss_souls \
         FROM {database}.match_player_stats FINAL \
         WHERE match_id = {match_id} AND snapshot_time_s <= {lane_end_s} \
         GROUP BY player_slot ORDER BY player_slot",
        lane_end_s = LANE_PHASE_END_S,
    )
}

async fn load_lane_deaths(
    state: &ReadApiState,
    match_id: u64,
) -> Result<Vec<LaneDeath>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT game_time_s, victim_slot, killer_slot FROM {db}.match_deaths FINAL WHERE match_id = {match_id} AND game_time_s <= {lane_end_s} ORDER BY game_time_s, victim_slot, death_index",
        db = state.database,
        lane_end_s = LANE_PHASE_END_S,
    );
    Ok(query_rows(state, &sql)
        .await?
        .into_iter()
        .map(|row| LaneDeath {
            t: number_u32(&row, "game_time_s").unwrap_or_default(),
            victim: number_u32(&row, "victim_slot").unwrap_or_default(),
            killer: number_u32(&row, "killer_slot").unwrap_or_default(),
        })
        .collect())
}

fn metric(row: Option<&Value>, name: &str) -> Option<u32> {
    row.and_then(|row| optional_u32(row, name))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn lane_player_serializes_snapshot_and_soul_sources() {
        let player = CorePlayer {
            account_id: 7,
            slot: 3,
            team: 0,
            hero: Hero {
                id: 10,
                name: "Hero".to_string(),
                icon: None,
                card: None,
            },
            kills: 0,
            deaths: 0,
            assists: 0,
        };
        let row = json!({
            "latest_snapshot_time_s": 540,
            "kills": 2,
            "deaths": 1,
            "assists": null,
            "creep_kills": 24,
            "hero_damage": 3200,
            "net_worth": 4100,
            "lane_creep_souls": 1700,
            "lane_creep_orb_souls": 350
        });

        let serialized = serde_json::to_value(lane_player(&player, Some(&row))).unwrap();
        assert_eq!(serialized["slot"], 3);
        assert_eq!(serialized["snapshot_time_s"], 540);
        assert_eq!(serialized["kills"], 2);
        assert_eq!(serialized["assists"], serde_json::Value::Null);
        assert_eq!(serialized["souls"]["net_worth"], 4100);
        assert_eq!(serialized["souls"]["lane_creeps"], 1700);
        assert_eq!(serialized["souls"]["lane_creep_orbs"], 350);
    }

    #[test]
    fn players_without_lane_snapshots_keep_metrics_unknown() {
        let player = CorePlayer {
            account_id: 7,
            slot: 3,
            team: 0,
            hero: Hero {
                id: 10,
                name: "Hero".to_string(),
                icon: None,
                card: None,
            },
            kills: 0,
            deaths: 0,
            assists: 0,
        };

        let serialized = serde_json::to_value(lane_player(&player, None)).unwrap();
        assert_eq!(serialized["snapshot_time_s"], serde_json::Value::Null);
        assert_eq!(serialized["kills"], serde_json::Value::Null);
        assert_eq!(serialized["souls"]["net_worth"], serde_json::Value::Null);
    }

    #[test]
    fn lane_phase_window_is_first_ten_minutes_inclusive() {
        assert_eq!(LANE_PHASE_END_S, 10 * 60);
    }

    #[test]
    fn lane_snapshot_query_does_not_alias_the_argmax_weight_column() {
        let sql = lane_snapshot_query("singularity", 42);

        assert!(sql.contains("max(snapshot_time_s) AS latest_snapshot_time_s"));
        assert!(sql.contains("argMax(net_worth, snapshot_time_s)"));
        assert!(!sql.contains("AS snapshot_time_s"));
    }
}
