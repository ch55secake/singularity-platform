use std::collections::{BTreeMap, HashMap};

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use serde::Serialize;

use crate::{AppState, ErrorBody};

use super::{
    ReadApiState, api_error, authorize, load_core, match_selector, number_f32, number_f64,
    number_i32, number_u32, query_rows, string_value,
};

#[derive(Debug, Serialize)]
pub(crate) struct Death {
    t: u32,
    victim: u32,
    killer: u32,
    victim_pos: [f32; 2],
    killer_pos: [f32; 2],
    time_to_kill_s: f32,
}

#[derive(Debug, Serialize)]
struct PathData {
    x: Vec<f32>,
    y: Vec<f32>,
    health: Vec<u32>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Paths {
    interval_s: f32,
    players: BTreeMap<String, PathData>,
}

#[derive(Debug, Serialize)]
pub(super) struct ObjectiveEvent {
    t: u32,
    team: i32,
    kind: String,
    position_key: Option<String>,
    position: Option<[f64; 2]>,
}

#[derive(Debug, Serialize)]
pub(super) struct MapData {
    radius: f64,
    image: String,
    objective_positions: BTreeMap<String, [f64; 2]>,
}

pub(crate) async fn get_positions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
) -> Result<Json<Paths>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let selector = match_selector(&identifier)?;
    let Some(core) = load_core(&state.read_api, &selector).await? else {
        return Err(api_error(StatusCode::NOT_FOUND, "match not found"));
    };
    Ok(Json(load_paths(&state.read_api, core.match_id).await?))
}

pub(crate) async fn get_deaths(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
) -> Result<Json<Vec<Death>>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let selector = match_selector(&identifier)?;
    let Some(core) = load_core(&state.read_api, &selector).await? else {
        return Err(api_error(StatusCode::NOT_FOUND, "match not found"));
    };
    Ok(Json(load_deaths(&state.read_api, core.match_id).await?))
}

pub(super) async fn load_deaths(
    state: &ReadApiState,
    match_id: u64,
) -> Result<Vec<Death>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT game_time_s, victim_slot, killer_slot, victim_x, victim_y, killer_x, killer_y, time_to_kill_s FROM {db}.match_deaths WHERE match_id = {match_id} ORDER BY game_time_s, victim_slot, death_index",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    Ok(rows
        .into_iter()
        .map(|row| Death {
            t: number_u32(&row, "game_time_s").unwrap_or_default(),
            victim: number_u32(&row, "victim_slot").unwrap_or_default(),
            killer: number_u32(&row, "killer_slot").unwrap_or_default(),
            victim_pos: [
                number_f32(&row, "victim_x").unwrap_or_default(),
                number_f32(&row, "victim_y").unwrap_or_default(),
            ],
            killer_pos: [
                number_f32(&row, "killer_x").unwrap_or_default(),
                number_f32(&row, "killer_y").unwrap_or_default(),
            ],
            time_to_kill_s: number_f32(&row, "time_to_kill_s").unwrap_or_default(),
        })
        .collect())
}

pub(super) async fn load_paths(
    state: &ReadApiState,
    match_id: u64,
) -> Result<Paths, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT player_slot, time_s, x, y, health FROM {db}.match_player_positions WHERE match_id = {match_id} ORDER BY player_slot, sample_index",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    let mut players = BTreeMap::new();
    let mut interval_s = 1.0;
    let mut previous_time: HashMap<u32, f32> = HashMap::new();
    for row in rows {
        let Some(slot) = number_u32(&row, "player_slot") else {
            continue;
        };
        let time = number_f32(&row, "time_s").unwrap_or_default();
        if let Some(previous) = previous_time.insert(slot, time) {
            let delta = time - previous;
            if delta > 0.0 {
                interval_s = delta;
            }
        }
        let path = players.entry(slot.to_string()).or_insert_with(|| PathData {
            x: Vec::new(),
            y: Vec::new(),
            health: Vec::new(),
        });
        path.x.push(number_f32(&row, "x").unwrap_or_default());
        path.y.push(number_f32(&row, "y").unwrap_or_default());
        path.health
            .push(number_u32(&row, "health").unwrap_or_default());
    }
    Ok(Paths {
        interval_s,
        players,
    })
}

pub(super) async fn load_map(
    state: &ReadApiState,
) -> Result<MapData, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT radius, minimap_url, payload_json FROM {db}.map_assets ORDER BY snapshot_version DESC LIMIT 1",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    let Some(row) = rows.first() else {
        return Ok(MapData {
            radius: 0.0,
            image: String::new(),
            objective_positions: BTreeMap::new(),
        });
    };
    let radius = number_f64(row, "radius");
    let mut objective_positions: BTreeMap<String, [f64; 2]> = string_value(row, "payload_json")
        .and_then(|payload| serde_json::from_str::<serde_json::Value>(&payload).ok())
        .and_then(|payload| payload.get("objective_positions").cloned())
        .and_then(|positions| positions.as_object().cloned())
        .map(|positions| {
            positions
                .into_iter()
                .filter_map(|(name, position)| {
                    let left = position.get("left_relative")?.as_f64()?;
                    let top = position.get("top_relative")?.as_f64()?;
                    let world_position = objective_position_to_world(&name, left, top, radius);
                    Some((name, world_position))
                })
                .collect()
        })
        .unwrap_or_default();
    if radius > 0.0 {
        objective_positions.insert("mid_boss".to_string(), [0.0, 0.0]);
    }
    Ok(MapData {
        radius,
        image: string_value(row, "minimap_url").unwrap_or_default(),
        objective_positions,
    })
}

pub(super) async fn load_objectives(
    state: &ReadApiState,
    match_id: u64,
    map: &MapData,
) -> Result<Vec<ObjectiveEvent>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT event_index, argMax(objective_id, metadata_version) AS objective_id, argMax(team, metadata_version) AS team, argMax(game_time_s, metadata_version) AS game_time_s FROM {db}.match_objectives WHERE match_id = {match_id} GROUP BY event_index ORDER BY game_time_s, event_index",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let objective_id = number_u32(&row, "objective_id")?;
            let team = number_i32(&row, "team")?;
            let kind = objective_kind(objective_id)?.to_string();
            let position_key = if objective_id == 16 {
                Some("mid_boss".to_string())
            } else {
                objective_position_key(objective_id, team)
            };
            let position = position_key
                .as_ref()
                .and_then(|key| map.objective_positions.get(key).copied());
            Some(ObjectiveEvent {
                t: number_u32(&row, "game_time_s")?,
                team,
                kind,
                position_key,
                position,
            })
        })
        .collect())
}

fn objective_kind(objective_id: u32) -> Option<&'static str> {
    match objective_id {
        0 => Some("patron_core"),
        1..=4 => Some("guardian"),
        5..=8 => Some("walker"),
        9 => Some("patron"),
        10..=11 => Some("shrine"),
        12..=15 => Some("base_guardian"),
        16 => Some("mid_boss"),
        _ => None,
    }
}

fn objective_position_key(objective_id: u32, team: i32) -> Option<String> {
    if !(0..=1).contains(&team) {
        return None;
    }
    match objective_id {
        0 => Some(format!("team{team}_core")),
        1..=4 => Some(format!("team{team}_tier1_{objective_id}")),
        5..=8 => Some(format!("team{team}_tier2_{}", objective_id - 4)),
        9 => Some(format!("team{team}_titan")),
        _ => None,
    }
}

fn map_relative_to_world(left: f64, top: f64, radius: f64) -> [f64; 2] {
    [(left * 2.0 - 1.0) * radius, (1.0 - top * 2.0) * radius]
}

fn objective_position_to_world(key: &str, left: f64, top: f64, radius: f64) -> [f64; 2] {
    // The upstream map asset records CSS margins (top-left anchors), not marker
    // centers. Regular objective icons are 10% square; Core markers are 30% x 8%.
    let (width, height) = if key.ends_with("_core") {
        (0.30, 0.08)
    } else {
        (0.10, 0.10)
    };
    map_relative_to_world(left + width / 2.0, top + height / 2.0, radius)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn maps_objective_ids_to_labels_and_asset_positions() {
        assert_eq!(objective_kind(1), Some("guardian"));
        assert_eq!(objective_kind(7), Some("walker"));
        assert_eq!(objective_kind(9), Some("patron"));
        assert_eq!(objective_kind(16), Some("mid_boss"));
        assert_eq!(objective_kind(99), None);
        assert_eq!(
            objective_position_key(1, 1).as_deref(),
            Some("team1_tier1_1")
        );
        assert_eq!(
            objective_position_key(7, 0).as_deref(),
            Some("team0_tier2_3")
        );
        assert_eq!(objective_position_key(16, 0), None);
        assert_eq!(objective_position_key(1, 2), None);
        let guardian = objective_position_to_world("team1_tier1_1", 0.21, 0.39, 10_752.0);
        assert!((guardian[0] + 5_160.96).abs() < 1e-9);
        assert!((guardian[1] - 1_290.24).abs() < 1e-9);

        let core = objective_position_to_world("team0_core", 0.35, 0.93, 10_752.0);
        assert!(core[0].abs() < 1e-9);
        assert!((core[1] + 10_106.88).abs() < 1e-9);
    }

    #[test]
    fn exposes_map_asset_positions_for_objective_markers() {
        let map = MapData {
            radius: 10_752.0,
            image: "https://assets.example/map.png".to_string(),
            objective_positions: BTreeMap::from([
                ("mid_boss".to_string(), [0.0, 0.0]),
                ("team0_core".to_string(), [100.0, 200.0]),
            ]),
        };

        let json = serde_json::to_value(map).unwrap();

        assert_eq!(json["radius"], 10_752.0);
        assert_eq!(
            json["objective_positions"]["team0_core"],
            serde_json::json!([100.0, 200.0])
        );
        assert_eq!(
            json["objective_positions"]["mid_boss"],
            serde_json::json!([0.0, 0.0])
        );
    }

    #[test]
    fn objective_events_include_the_map_position_key() {
        let event = ObjectiveEvent {
            t: 120,
            team: 1,
            kind: "guardian".to_string(),
            position_key: Some("team1_tier1_1".to_string()),
            position: Some([-6_236.0, 2_365.0]),
        };

        let json = serde_json::to_value(event).unwrap();

        assert_eq!(json["position_key"], "team1_tier1_1");
    }
}
