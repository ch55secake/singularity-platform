use std::collections::HashMap;

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::Serialize;
use serde_json::Value;

use crate::{AppState, ErrorBody};

use super::{
    EventQuery, Hero, ListQuery, Member, ReadApiState, api_error, authorize, mode_name,
    non_empty_string, number_i32, number_u32, number_u64, optional_u32, read_backend_error,
};

#[derive(Debug, Serialize, Clone)]
struct PlayerSummary {
    slot: u32,
    team: i32,
    member: Option<Member>,
    hero: Hero,
    kills: u32,
    deaths: u32,
    assists: u32,
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct MatchSummary {
    uuid: String,
    match_id: u64,
    start_time: u32,
    duration_s: u32,
    mode: String,
    winning_team: i32,
    players: Vec<PlayerSummary>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MatchProcessedEvent {
    uuid: String,
    match_id: u64,
    start_time: u32,
}

#[derive(Debug, Clone)]
pub(super) struct CorePlayer {
    pub(super) account_id: u32,
    pub(super) slot: u32,
    pub(super) team: i32,
    pub(super) hero: Hero,
    pub(super) kills: u32,
    pub(super) deaths: u32,
    pub(super) assists: u32,
}

#[derive(Debug)]
pub(super) struct CoreMatch {
    pub(super) uuid: String,
    pub(super) match_id: u64,
    pub(super) start_time: u32,
    pub(super) duration_s: u32,
    pub(super) winning_team: i32,
    pub(super) average_badge: [Option<u32>; 2],
    pub(super) players: Vec<CorePlayer>,
}

pub(crate) async fn list_matches(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<MatchSummary>>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let limit = query.limit.unwrap_or(super::DEFAULT_LIMIT);
    let offset = query.offset.unwrap_or_default();
    if limit == 0 || limit > super::MAX_LIMIT || offset > super::MAX_OFFSET {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid pagination"));
    }
    let sql = format!(
        "SELECT m.public_id AS uuid, m.match_id, m.start_time, m.duration_s, m.winning_team, m.average_badge_team0, m.average_badge_team1, p.account_id, p.player_slot, p.team, p.hero_id, p.kills, p.deaths, p.assists, h.name AS hero_name, h.image_url AS hero_image, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_sm'), ''), nullIf(h.image_url, '')) AS hero_icon, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero_card'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_card'), ''), nullIf(h.image_url, '')) AS hero_card FROM (SELECT public_id, match_id, start_time, duration_s, winning_team, average_badge_team0, average_badge_team1 FROM {db}.matches FINAL ORDER BY start_time DESC, match_id DESC LIMIT {limit} OFFSET {offset}) AS m LEFT JOIN {db}.match_players AS p FINAL ON p.match_id = m.match_id LEFT JOIN {db}.heroes AS h FINAL ON h.id = p.hero_id ORDER BY m.start_time DESC, m.match_id DESC, p.player_slot",
        db = state.read_api.database,
    );
    let rows = state
        .read_api
        .query(&sql)
        .await
        .map_err(read_backend_error)?;
    Ok(Json(group_core_rows(rows, &state.read_api.members)))
}

pub(crate) async fn list_processed_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<MatchProcessedEvent>>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let limit = query.limit.unwrap_or(super::DEFAULT_LIMIT);
    if limit == 0 || limit > super::MAX_LIMIT {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid pagination"));
    }
    let after_start_time = query.after_start_time.unwrap_or_default();
    let after_match_id = query.after_match_id.unwrap_or_default();
    let sql = format!(
        "SELECT public_id AS uuid, match_id, start_time FROM {db}.match_processed_events FINAL WHERE start_time > {after_start_time} OR (start_time = {after_start_time} AND match_id > {after_match_id}) ORDER BY start_time ASC, match_id ASC LIMIT {limit}",
        db = state.read_api.database,
    );
    let rows = state
        .read_api
        .query(&sql)
        .await
        .map_err(read_backend_error)?;
    Ok(Json(
        rows.into_iter()
            .filter_map(|row| {
                Some(MatchProcessedEvent {
                    uuid: non_empty_string(&row, "uuid")?,
                    match_id: number_u64(&row, "match_id")?,
                    start_time: number_u32(&row, "start_time").unwrap_or_default(),
                })
            })
            .collect(),
    ))
}

pub(super) fn match_selector(identifier: &str) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    if is_public_id(identifier) {
        return Ok(format!("m.public_id = '{identifier}'"));
    }
    let match_id = identifier
        .parse::<u64>()
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "invalid match id"))?;
    if match_id == 0 {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid match id"));
    }
    Ok(format!("m.match_id = {match_id}"))
}

fn is_public_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

pub(super) async fn load_core(
    state: &ReadApiState,
    selector: &str,
) -> Result<Option<CoreMatch>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT m.public_id AS uuid, m.match_id, m.start_time, m.duration_s, m.winning_team, m.average_badge_team0, m.average_badge_team1, p.account_id, p.player_slot, p.team, p.hero_id, p.kills, p.deaths, p.assists, h.name AS hero_name, h.image_url AS hero_image, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_sm'), ''), nullIf(h.image_url, '')) AS hero_icon, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero_card'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_card'), ''), nullIf(h.image_url, '')) AS hero_card FROM {db}.matches AS m FINAL LEFT JOIN {db}.match_players AS p FINAL ON p.match_id = m.match_id LEFT JOIN {db}.heroes AS h FINAL ON h.id = p.hero_id WHERE {selector} ORDER BY p.player_slot",
        db = state.database,
    );
    let rows = state.query(&sql).await.map_err(read_backend_error)?;
    Ok(group_core(rows))
}

fn group_core_rows(rows: Vec<Value>, members: &HashMap<u32, String>) -> Vec<MatchSummary> {
    let mut indexes = HashMap::new();
    let mut grouped = Vec::new();
    for row in rows {
        let Some((match_data, player)) = core_row(row) else {
            continue;
        };
        let index = match indexes.entry(match_data.match_id) {
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let index = grouped.len();
                entry.insert(index);
                grouped.push(match_data);
                index
            }
        };
        if let Some(player) = player {
            push_unique_core_player(&mut grouped[index].players, player);
        }
    }
    grouped
        .into_iter()
        .map(|core| MatchSummary {
            uuid: core.uuid,
            match_id: core.match_id,
            start_time: core.start_time,
            duration_s: core.duration_s,
            mode: mode_name(core.players.len()),
            winning_team: core.winning_team,
            players: core
                .players
                .iter()
                .map(|player| summary_player(player, members))
                .collect(),
        })
        .collect()
}

fn group_core(rows: Vec<Value>) -> Option<CoreMatch> {
    let mut grouped: Option<CoreMatch> = None;
    for row in rows {
        let Some((match_data, player)) = core_row(row) else {
            continue;
        };
        let core = grouped.get_or_insert(match_data);
        if let Some(player) = player {
            push_unique_core_player(&mut core.players, player);
        }
    }
    grouped
}

fn push_unique_core_player(players: &mut Vec<CorePlayer>, player: CorePlayer) {
    if !players.iter().any(|existing| existing.slot == player.slot) {
        players.push(player);
    }
}

fn core_row(row: Value) -> Option<(CoreMatch, Option<CorePlayer>)> {
    let match_id = number_u64(&row, "match_id")?;
    let player = number_u32(&row, "player_slot").map(|slot| CorePlayer {
        account_id: number_u32(&row, "account_id").unwrap_or_default(),
        slot,
        team: number_i32(&row, "team").unwrap_or_default(),
        hero: Hero {
            id: number_u32(&row, "hero_id").unwrap_or_default(),
            name: non_empty_string(&row, "hero_name").unwrap_or_else(|| "Unknown".to_string()),
            icon: non_empty_string(&row, "hero_icon")
                .or_else(|| non_empty_string(&row, "hero_image")),
            card: non_empty_string(&row, "hero_card"),
        },
        kills: number_u32(&row, "kills").unwrap_or_default(),
        deaths: number_u32(&row, "deaths").unwrap_or_default(),
        assists: number_u32(&row, "assists").unwrap_or_default(),
    });
    Some((
        CoreMatch {
            uuid: non_empty_string(&row, "uuid").unwrap_or_else(|| match_id.to_string()),
            match_id,
            start_time: number_u32(&row, "start_time").unwrap_or_default(),
            duration_s: number_u32(&row, "duration_s").unwrap_or_default(),
            winning_team: number_i32(&row, "winning_team").unwrap_or_default(),
            average_badge: [
                optional_u32(&row, "average_badge_team0"),
                optional_u32(&row, "average_badge_team1"),
            ],
            players: Vec::new(),
        },
        player,
    ))
}

fn summary_player(player: &CorePlayer, members: &HashMap<u32, String>) -> PlayerSummary {
    PlayerSummary {
        slot: player.slot,
        team: player.team,
        member: members
            .get(&player.account_id)
            .map(|name| Member { name: name.clone() }),
        hero: player.hero.clone(),
        kills: player.kills,
        deaths: player.deaths,
        assists: player.assists,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_uuid_shaped_public_ids() {
        assert!(is_public_id("37bb05cc-27d5-5ae8-8c28-129dd11cd85c"));
        assert!(!is_public_id("37bb05cc-27d5-5ae8-8c28-129dd11cd85"));
        assert!(!is_public_id("' OR 1=1 --"));
    }

    #[test]
    fn keeps_query_order_and_resolves_configured_members() {
        let rows = vec![
            serde_json::json!({
                "uuid": "new",
                "match_id": 2,
                "start_time": 20,
                "player_slot": 1,
                "account_id": 99,
                "team": 0,
                "hero_id": 72,
                "hero_name": "Billy",
                "kills": 3,
                "deaths": 1,
                "assists": 4
            }),
            serde_json::json!({
                "uuid": "old",
                "match_id": 1,
                "start_time": 10,
                "player_slot": 1,
                "account_id": 100,
                "team": 1,
                "hero_id": 76,
                "hero_name": "Graves",
                "kills": 1,
                "deaths": 2,
                "assists": 3
            }),
        ];
        let members = HashMap::from([(99, "oscar".to_string())]);
        let matches = group_core_rows(rows, &members);

        assert_eq!(matches[0].uuid, "new");
        assert_eq!(matches[0].players[0].member.as_ref().unwrap().name, "oscar");
        assert_eq!(matches[1].uuid, "old");
        assert!(matches[1].players[0].member.is_none());
    }

    #[test]
    fn ignores_duplicate_player_slots_when_building_match_summaries() {
        let unique_players = (1..=12)
            .map(|player_slot| {
                serde_json::json!({
                    "uuid": "match",
                    "match_id": 7,
                    "start_time": 10,
                    "player_slot": player_slot,
                    "account_id": player_slot + 100,
                    "team": if player_slot <= 6 { 0 } else { 1 },
                    "hero_id": 72,
                    "hero_name": "Billy",
                    "kills": 3,
                    "deaths": 1,
                    "assists": 4
                })
            })
            .collect::<Vec<_>>();
        let rows = unique_players
            .iter()
            .flat_map(|player| [player.clone(), player.clone()])
            .collect();

        let matches = group_core_rows(rows, &HashMap::new());

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].players.len(), 12);
        assert_eq!(matches[0].mode, "6v6");
    }
}
