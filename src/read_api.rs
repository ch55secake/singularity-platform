use std::{
    collections::{BTreeMap, HashMap},
    env,
    time::Duration,
};

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::{AppState, ErrorBody, bearer_token, constant_time_eq, non_empty_env};

const DEFAULT_DATABASE: &str = "singularity";
const DEFAULT_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 100;
const MAX_OFFSET: u32 = 10_000;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const MAX_PLAYER_MATCHES: u32 = 100;

#[derive(Clone)]
pub(crate) struct ReadApiState {
    client: Client,
    url: Option<String>,
    database: String,
    user: Option<String>,
    password: Option<String>,
    token: Option<String>,
    members: HashMap<u32, String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListQuery {
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct EventQuery {
    limit: Option<u32>,
    after_start_time: Option<u32>,
    after_match_id: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct PlayerQuery {
    last: Option<u32>,
}

#[derive(Debug, Serialize, Clone)]
struct Hero {
    id: u32,
    name: String,
    icon: Option<String>,
    card: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
struct Member {
    name: String,
}

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

#[derive(Debug, Serialize)]
struct Aim {
    shots_hit: u32,
    shots_missed: u32,
    hero_hits: u32,
    hero_crits: u32,
}

#[derive(Debug, Serialize)]
struct KillTypes {
    bullet: u32,
    ability: u32,
    melee: u32,
    headshot: u32,
}

#[derive(Debug, Serialize, Clone, Default)]
struct Timeline {
    t: Vec<u32>,
    souls: Vec<u32>,
    hero_damage: Vec<u32>,
}

#[derive(Debug, Serialize, Clone)]
struct DamageSource {
    source: String,
    name: String,
    kind: String,
    icon: Option<String>,
    total: f64,
    by_target: BTreeMap<String, f64>,
}

#[derive(Debug, Serialize, Clone)]
struct BuildEntry {
    t: u32,
    id: u32,
    name: String,
    kind: String,
    tier: Option<u32>,
    icon: Option<String>,
    sold_t: Option<u32>,
}

#[derive(Debug, Serialize)]
struct Player {
    slot: u32,
    team: i32,
    member: Option<Member>,
    hero: Hero,
    kills: u32,
    deaths: u32,
    assists: u32,
    souls: u32,
    last_hits: u32,
    denies: u32,
    level: u32,
    hero_damage: u32,
    healing: u32,
    damage_taken: u32,
    damage_mitigated: u32,
    aim: Aim,
    kill_types: KillTypes,
    timeline: Timeline,
    damage_by_source: Vec<DamageSource>,
    build: Vec<BuildEntry>,
}

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
struct MapData {
    radius: f64,
    image: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MatchDetail {
    uuid: String,
    match_id: u64,
    start_time: u32,
    duration_s: u32,
    mode: String,
    winning_team: i32,
    average_badge: [Option<u32>; 2],
    players: Vec<Player>,
    deaths: Vec<Death>,
    paths: Paths,
    map: MapData,
}

#[derive(Debug, Serialize)]
pub(crate) struct PlayerAverage {
    name: String,
    matches: u64,
    kdr: f64,
    kda: f64,
    accuracy: f64,
    crit_rate: f64,
    win_rate: f64,
    damage_per_minute: f64,
    heroes: Vec<HeroAverage>,
    recent: Vec<RecentMatch>,
}

#[derive(Debug, Serialize)]
struct HeroAverage {
    id: u32,
    name: String,
    matches: u64,
}

#[derive(Debug, Serialize)]
struct RecentMatch {
    uuid: String,
    match_id: u64,
    start_time: u32,
    duration_s: u32,
    winning_team: i32,
    team: i32,
    hero: Hero,
    kills: u32,
    deaths: u32,
    assists: u32,
}

#[derive(Debug, Clone)]
struct CorePlayer {
    account_id: u32,
    slot: u32,
    team: i32,
    hero: Hero,
    kills: u32,
    deaths: u32,
    assists: u32,
}

#[derive(Debug)]
struct CoreMatch {
    uuid: String,
    match_id: u64,
    start_time: u32,
    duration_s: u32,
    winning_team: i32,
    average_badge: [Option<u32>; 2],
    players: Vec<CorePlayer>,
}

#[derive(Debug, Clone, Default)]
struct FinalStats {
    souls: u32,
    last_hits: u32,
    denies: u32,
    level: u32,
    hero_damage: u32,
    healing: u32,
    damage_taken: u32,
    damage_mitigated: u32,
    shots_hit: u32,
    shots_missed: u32,
    hero_hits: u32,
    hero_crits: u32,
    bullet_kills: u32,
    ability_kills: u32,
    melee_kills: u32,
    headshot_kills: u32,
}

impl ReadApiState {
    pub(crate) fn from_env() -> Result<Self, std::io::Error> {
        let timeout = env::var("SINGULARITY_READ_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| DEFAULT_TIMEOUT_SECONDS.to_string())
            .parse::<u64>()
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid read timeout")
            })?;
        if timeout == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "read timeout must be greater than zero",
            ));
        }
        let members = read_members()?;
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout))
            .user_agent("singularity-platform-read-api/0.1")
            .build()
            .map_err(std::io::Error::other)?;
        Ok(Self {
            client,
            url: non_empty_env("SINGULARITY_CLICKHOUSE_URL"),
            database: env::var("SINGULARITY_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| DEFAULT_DATABASE.to_string()),
            user: non_empty_env("SINGULARITY_CLICKHOUSE_USER"),
            password: non_empty_env("SINGULARITY_CLICKHOUSE_PASSWORD"),
            token: non_empty_env("SINGULARITY_READ_TOKEN"),
            members,
        })
    }

    async fn query(&self, sql: &str) -> Result<Vec<Value>, ()> {
        let Some(url) = &self.url else {
            return Err(());
        };
        let mut request = self
            .client
            .post(url)
            .query(&[("query", format!("{sql} FORMAT JSONEachRow"))])
            .header("content-type", "text/plain");
        if let Some(user) = &self.user {
            request = request.basic_auth(user, self.password.as_deref());
        }
        let response = request.send().await.map_err(|error| {
            warn!(%error, "read API ClickHouse request failed");
        })?;
        if !response.status().is_success() {
            warn!(status = %response.status(), "read API ClickHouse request returned an error");
            return Err(());
        }
        let body = response.bytes().await.map_err(|error| {
            warn!(%error, "read API ClickHouse response could not be read");
        })?;
        if body.len() > MAX_RESPONSE_BYTES {
            warn!(
                bytes = body.len(),
                "read API ClickHouse response exceeded the limit"
            );
            return Err(());
        }
        let body = String::from_utf8(body.to_vec()).map_err(|_| ())?;
        body.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line).map_err(|error| {
                    warn!(%error, "read API ClickHouse row was invalid JSON");
                })
            })
            .collect()
    }
}

pub(crate) async fn list_matches(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<MatchSummary>>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    let offset = query.offset.unwrap_or_default();
    if limit == 0 || limit > MAX_LIMIT || offset > MAX_OFFSET {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid pagination"));
    }
    let sql = format!(
        "SELECT m.public_id AS uuid, m.match_id, m.start_time, m.duration_s, m.winning_team, m.average_badge_team0, m.average_badge_team1, p.account_id, p.player_slot, p.team, p.hero_id, p.kills, p.deaths, p.assists, h.name AS hero_name, h.image_url AS hero_image, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_sm'), ''), nullIf(h.image_url, '')) AS hero_icon, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero_card'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_card'), ''), nullIf(h.image_url, '')) AS hero_card FROM (SELECT public_id, match_id, start_time, duration_s, winning_team, average_badge_team0, average_badge_team1 FROM {db}.matches ORDER BY start_time DESC, match_id DESC LIMIT {limit} OFFSET {offset}) AS m LEFT JOIN {db}.match_players AS p ON p.match_id = m.match_id LEFT JOIN {db}.heroes AS h ON h.id = p.hero_id ORDER BY m.start_time DESC, m.match_id DESC, p.player_slot",
        db = state.read_api.database,
    );
    let rows = state
        .read_api
        .query(&sql)
        .await
        .map_err(|_| api_error(StatusCode::SERVICE_UNAVAILABLE, "read backend unavailable"))?;
    Ok(Json(group_core_rows(rows, &state.read_api.members)))
}

pub(crate) async fn list_processed_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<MatchProcessedEvent>>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 || limit > MAX_LIMIT {
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
        .map_err(|_| api_error(StatusCode::SERVICE_UNAVAILABLE, "read backend unavailable"))?;
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

pub(crate) async fn get_match(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
) -> Result<Json<MatchDetail>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let selector = match_selector(&identifier)?;
    let Some(core) = load_core(&state.read_api, &selector).await? else {
        return Err(api_error(StatusCode::NOT_FOUND, "match not found"));
    };
    let final_stats = load_final_stats(&state.read_api, core.match_id).await?;
    let timelines = load_timelines(&state.read_api, core.match_id).await?;
    let damage = load_damage(&state.read_api, core.match_id).await?;
    let builds = load_builds(&state.read_api, core.match_id).await?;
    let deaths = load_deaths(&state.read_api, core.match_id).await?;
    let paths = load_paths(&state.read_api, core.match_id).await?;
    let map = load_map(&state.read_api).await?;
    let players = core
        .players
        .iter()
        .map(|player| {
            build_player(
                player,
                &state.read_api.members,
                &final_stats,
                &timelines,
                &damage,
                &builds,
            )
        })
        .collect();
    Ok(Json(MatchDetail {
        uuid: core.uuid,
        match_id: core.match_id,
        start_time: core.start_time,
        duration_s: core.duration_s,
        mode: mode_name(core.players.len()),
        winning_team: core.winning_team,
        average_badge: core.average_badge,
        players,
        deaths,
        paths,
        map,
    }))
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

pub(crate) async fn get_player(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(account_id): Path<u32>,
    Query(query): Query<PlayerQuery>,
) -> Result<Json<PlayerAverage>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let last_matches = validate_player_limit(query.last)?;
    let summary =
        query_player_average(&state.read_api, account_id, String::new(), last_matches).await?;
    Ok(Json(summary))
}

pub(crate) async fn get_player_by_name(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(query): Query<PlayerQuery>,
) -> Result<Json<PlayerAverage>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let Some((account_id, canonical_name)) = state
        .read_api
        .members
        .iter()
        .find(|(_, member_name)| member_name.eq_ignore_ascii_case(&name))
        .map(|(account_id, member_name)| (*account_id, member_name.clone()))
    else {
        return Err(api_error(StatusCode::NOT_FOUND, "player not found"));
    };
    let last_matches = validate_player_limit(query.last)?;
    let summary =
        query_player_average(&state.read_api, account_id, canonical_name, last_matches).await?;
    Ok(Json(summary))
}

fn read_members() -> Result<HashMap<u32, String>, std::io::Error> {
    let Some(value) = non_empty_env("SINGULARITY_MEMBER_NAMES_JSON") else {
        return Ok(HashMap::new());
    };
    let names: HashMap<String, String> = serde_json::from_str(&value).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "SINGULARITY_MEMBER_NAMES_JSON must be an object of account IDs to names",
        )
    })?;
    names
        .into_iter()
        .map(|(account_id, name)| {
            let account_id = account_id.parse::<u32>().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "member account IDs must be unsigned integers",
                )
            })?;
            if name.trim().is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "member names must not be empty",
                ));
            }
            Ok((account_id, name))
        })
        .collect()
}

fn authorize(
    headers: &HeaderMap,
    state: &ReadApiState,
) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    let Some(expected) = &state.token else {
        return Ok(());
    };
    let Some(actual) = bearer_token(headers) else {
        return Err(api_error(StatusCode::UNAUTHORIZED, "unauthorized"));
    };
    if constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(api_error(StatusCode::UNAUTHORIZED, "unauthorized"))
    }
}

fn api_error(status: StatusCode, error: &'static str) -> (StatusCode, Json<ErrorBody>) {
    (status, Json(ErrorBody { error }))
}

fn validate_player_limit(last: Option<u32>) -> Result<Option<u32>, (StatusCode, Json<ErrorBody>)> {
    if last.is_some_and(|limit| limit == 0 || limit > MAX_PLAYER_MATCHES) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "invalid player match limit",
        ));
    }
    Ok(last)
}

fn match_selector(identifier: &str) -> Result<String, (StatusCode, Json<ErrorBody>)> {
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

async fn load_core(
    state: &ReadApiState,
    selector: &str,
) -> Result<Option<CoreMatch>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT m.public_id AS uuid, m.match_id, m.start_time, m.duration_s, m.winning_team, m.average_badge_team0, m.average_badge_team1, p.account_id, p.player_slot, p.team, p.hero_id, p.kills, p.deaths, p.assists, h.name AS hero_name, h.image_url AS hero_image, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_sm'), ''), nullIf(h.image_url, '')) AS hero_icon, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero_card'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_card'), ''), nullIf(h.image_url, '')) AS hero_card FROM {db}.matches AS m LEFT JOIN {db}.match_players AS p ON p.match_id = m.match_id LEFT JOIN {db}.heroes AS h ON h.id = p.hero_id WHERE {selector} ORDER BY p.player_slot",
        db = state.database,
    );
    let rows = state
        .query(&sql)
        .await
        .map_err(|_| api_error(StatusCode::SERVICE_UNAVAILABLE, "read backend unavailable"))?;
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
            grouped[index].players.push(player);
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
            core.players.push(player);
        }
    }
    grouped
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
        member: member_for(members, player.account_id),
        hero: player.hero.clone(),
        kills: player.kills,
        deaths: player.deaths,
        assists: player.assists,
    }
}

fn member_for(members: &HashMap<u32, String>, account_id: u32) -> Option<Member> {
    members
        .get(&account_id)
        .map(|name| Member { name: name.clone() })
}

fn mode_name(player_count: usize) -> String {
    if player_count > 0 && player_count.is_multiple_of(2) {
        format!("{}v{}", player_count / 2, player_count / 2)
    } else {
        "unknown".to_string()
    }
}

fn number_u64(row: &Value, key: &str) -> Option<u64> {
    row.get(key).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
            .or_else(|| value.as_str()?.parse().ok())
    })
}

fn number_u32(row: &Value, key: &str) -> Option<u32> {
    number_u64(row, key).and_then(|value| u32::try_from(value).ok())
}

fn number_i32(row: &Value, key: &str) -> Option<i32> {
    row.get(key).and_then(|value| {
        value
            .as_i64()
            .and_then(|number| i32::try_from(number).ok())
            .or_else(|| value.as_u64().and_then(|number| i32::try_from(number).ok()))
            .or_else(|| value.as_str()?.parse().ok())
    })
}

fn number_f64(row: &Value, key: &str) -> f64 {
    row.get(key)
        .and_then(|value| value.as_f64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default()
}

fn number_f32(row: &Value, key: &str) -> Option<f32> {
    row.get(key).and_then(|value| {
        value
            .as_f64()
            .map(|number| number as f32)
            .or_else(|| value.as_str()?.parse().ok())
    })
}

fn optional_u32(row: &Value, key: &str) -> Option<u32> {
    row.get(key)
        .filter(|value| !value.is_null())
        .and_then(|_| number_u32(row, key))
}

fn string_value(row: &Value, key: &str) -> Option<String> {
    row.get(key).and_then(Value::as_str).map(ToOwned::to_owned)
}

fn non_empty_string(row: &Value, key: &str) -> Option<String> {
    string_value(row, key).filter(|value| !value.is_empty())
}

async fn query_rows(
    state: &ReadApiState,
    sql: &str,
) -> Result<Vec<Value>, (StatusCode, Json<ErrorBody>)> {
    state
        .query(sql)
        .await
        .map_err(|_| api_error(StatusCode::SERVICE_UNAVAILABLE, "read backend unavailable"))
}

async fn load_final_stats(
    state: &ReadApiState,
    match_id: u64,
) -> Result<HashMap<u32, FinalStats>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT players.player_slot, players.net_worth AS souls, players.last_hits, players.denies, players.level, argMax(ifNull(stats.player_damage, 0), stats.snapshot_time_s) AS hero_damage, argMax(ifNull(stats.player_healing, 0), stats.snapshot_time_s) AS healing, argMax(ifNull(stats.player_damage_taken, 0), stats.snapshot_time_s) AS damage_taken, argMax(ifNull(stats.damage_mitigated, 0), stats.snapshot_time_s) AS damage_mitigated, argMax(ifNull(stats.shots_hit, 0), stats.snapshot_time_s) AS shots_hit, argMax(ifNull(stats.shots_missed, 0), stats.snapshot_time_s) AS shots_missed, argMax(ifNull(stats.hero_bullets_hit, 0), stats.snapshot_time_s) AS hero_hits, argMax(ifNull(stats.hero_bullets_hit_crit, 0), stats.snapshot_time_s) AS hero_crits, argMax(ifNull(stats.bullet_kills, 0), stats.snapshot_time_s) AS bullet_kills, argMax(ifNull(stats.ability_kills, 0), stats.snapshot_time_s) AS ability_kills, argMax(ifNull(stats.melee_kills, 0), stats.snapshot_time_s) AS melee_kills, argMax(ifNull(stats.headshot_kills, 0), stats.snapshot_time_s) AS headshot_kills FROM {db}.match_players AS players LEFT JOIN {db}.match_player_stats AS stats ON stats.match_id = players.match_id AND stats.player_slot = players.player_slot WHERE players.match_id = {match_id} GROUP BY players.player_slot, players.net_worth, players.last_hits, players.denies, players.level",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            Some((
                number_u32(&row, "player_slot")?,
                FinalStats {
                    souls: number_u32(&row, "souls").unwrap_or_default(),
                    last_hits: number_u32(&row, "last_hits").unwrap_or_default(),
                    denies: number_u32(&row, "denies").unwrap_or_default(),
                    level: number_u32(&row, "level").unwrap_or_default(),
                    hero_damage: number_u32(&row, "hero_damage").unwrap_or_default(),
                    healing: number_u32(&row, "healing").unwrap_or_default(),
                    damage_taken: number_u32(&row, "damage_taken").unwrap_or_default(),
                    damage_mitigated: number_u32(&row, "damage_mitigated").unwrap_or_default(),
                    shots_hit: number_u32(&row, "shots_hit").unwrap_or_default(),
                    shots_missed: number_u32(&row, "shots_missed").unwrap_or_default(),
                    hero_hits: number_u32(&row, "hero_hits").unwrap_or_default(),
                    hero_crits: number_u32(&row, "hero_crits").unwrap_or_default(),
                    bullet_kills: number_u32(&row, "bullet_kills").unwrap_or_default(),
                    ability_kills: number_u32(&row, "ability_kills").unwrap_or_default(),
                    melee_kills: number_u32(&row, "melee_kills").unwrap_or_default(),
                    headshot_kills: number_u32(&row, "headshot_kills").unwrap_or_default(),
                },
            ))
        })
        .collect())
}

async fn load_timelines(
    state: &ReadApiState,
    match_id: u64,
) -> Result<HashMap<u32, Timeline>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT player_slot, snapshot_time_s, ifNull(net_worth, 0) AS souls, ifNull(player_damage, 0) AS hero_damage FROM {db}.match_player_stats WHERE match_id = {match_id} ORDER BY player_slot, snapshot_time_s",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    let mut timelines: HashMap<u32, Timeline> = HashMap::new();
    for row in rows {
        let Some(slot) = number_u32(&row, "player_slot") else {
            continue;
        };
        let timeline = timelines.entry(slot).or_default();
        timeline
            .t
            .push(number_u32(&row, "snapshot_time_s").unwrap_or_default());
        timeline
            .souls
            .push(number_u32(&row, "souls").unwrap_or_default());
        timeline
            .hero_damage
            .push(number_u32(&row, "hero_damage").unwrap_or_default());
    }
    Ok(timelines)
}

async fn load_damage(
    state: &ReadApiState,
    match_id: u64,
) -> Result<HashMap<u32, Vec<DamageSource>>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT d.dealer_slot, d.target_slot, d.source_name, d.total_damage, a.asset_name, a.asset_kind, a.image_url FROM {db}.match_damage_totals AS d INNER JOIN {db}.match_players AS dealer ON dealer.match_id = d.match_id AND dealer.player_slot = d.dealer_slot INNER JOIN {db}.match_players AS target ON target.match_id = d.match_id AND target.player_slot = d.target_slot LEFT JOIN (SELECT class_name, name AS asset_name, asset_type AS asset_kind, image_url FROM {db}.items UNION ALL SELECT class_name, name AS asset_name, asset_type AS asset_kind, image_url FROM {db}.abilities) AS a ON a.class_name = d.source_name WHERE d.match_id = {match_id} AND d.stat_type = 0 AND ifNull(dealer.team, -1) != ifNull(target.team, -2) AND d.source_name NOT IN ('Bullet', 'Melee', 'Ability', 'Misc')",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    let mut grouped: HashMap<(u32, String), DamageSource> = HashMap::new();
    for row in rows {
        let Some(dealer) = number_u32(&row, "dealer_slot") else {
            continue;
        };
        let source_name = string_value(&row, "source_name").unwrap_or_default();
        let (source, name, kind) = normalize_source(
            &source_name,
            non_empty_string(&row, "asset_name"),
            non_empty_string(&row, "asset_kind"),
        );
        let key = (dealer, source.clone());
        let damage = number_f64(&row, "total_damage");
        let entry = grouped.entry(key).or_insert_with(|| DamageSource {
            source,
            name,
            kind,
            icon: non_empty_string(&row, "image_url"),
            total: 0.0,
            by_target: BTreeMap::new(),
        });
        entry.total += damage;
        *entry
            .by_target
            .entry(
                number_u32(&row, "target_slot")
                    .unwrap_or_default()
                    .to_string(),
            )
            .or_default() += damage;
    }
    let mut result: HashMap<u32, Vec<DamageSource>> = HashMap::new();
    for ((slot, _), source) in grouped {
        result.entry(slot).or_default().push(source);
    }
    for sources in result.values_mut() {
        sources.sort_by(|left, right| left.source.cmp(&right.source));
    }
    Ok(result)
}

fn normalize_source(
    source: &str,
    asset_name: Option<String>,
    asset_kind: Option<String>,
) -> (String, String, String) {
    if source.starts_with("citadel_weapon_") || source == "weapon" {
        return (
            "weapon".to_string(),
            "Gun".to_string(),
            "weapon".to_string(),
        );
    }
    if source.starts_with("ability_melee_") || source == "melee" {
        return (
            "melee".to_string(),
            "Melee".to_string(),
            "melee".to_string(),
        );
    }
    let kind = if source.starts_with("ability_") {
        "ability".to_string()
    } else if source.starts_with("upgrade_") {
        "upgrade".to_string()
    } else {
        asset_kind.unwrap_or_else(|| "other".to_string())
    };
    let name = asset_name.unwrap_or_else(|| source.to_string());
    (source.to_string(), name, kind)
}

async fn load_builds(
    state: &ReadApiState,
    match_id: u64,
) -> Result<HashMap<u32, Vec<BuildEntry>>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT player_slot, game_time_s, item_id, upgrade_id, sold_time_s, upgrade_tier, asset_name, asset_kind, asset_image_url FROM {db}.match_player_items_with_assets WHERE match_id = {match_id} AND (item_id != 0 OR upgrade_id != 0) ORDER BY player_slot, game_time_s, item_id, upgrade_id",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    let mut builds: HashMap<u32, Vec<BuildEntry>> = HashMap::new();
    for row in rows {
        let Some(slot) = number_u32(&row, "player_slot") else {
            continue;
        };
        let item_id = number_u32(&row, "item_id").unwrap_or_default();
        let upgrade_id = number_u32(&row, "upgrade_id").unwrap_or_default();
        let id = if item_id != 0 { item_id } else { upgrade_id };
        let kind = if non_empty_string(&row, "asset_kind").as_deref() == Some("ability") {
            "ability"
        } else {
            "item"
        };
        builds.entry(slot).or_default().push(BuildEntry {
            t: number_u32(&row, "game_time_s").unwrap_or_default(),
            id,
            name: non_empty_string(&row, "asset_name").unwrap_or_else(|| id.to_string()),
            kind: kind.to_string(),
            tier: number_u32(&row, "upgrade_tier").filter(|tier| *tier != 0),
            icon: non_empty_string(&row, "asset_image_url"),
            sold_t: number_u32(&row, "sold_time_s").filter(|time| *time != 0),
        });
    }
    Ok(builds)
}

async fn load_deaths(
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

async fn load_paths(
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

async fn load_map(state: &ReadApiState) -> Result<MapData, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT radius, minimap_url FROM {db}.map_assets ORDER BY snapshot_version DESC LIMIT 1",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    Ok(rows
        .first()
        .map(|row| MapData {
            radius: number_f64(row, "radius"),
            image: string_value(row, "minimap_url").unwrap_or_default(),
        })
        .unwrap_or_else(|| MapData {
            radius: 0.0,
            image: String::new(),
        }))
}

fn build_player(
    player: &CorePlayer,
    members: &HashMap<u32, String>,
    final_stats: &HashMap<u32, FinalStats>,
    timelines: &HashMap<u32, Timeline>,
    damage: &HashMap<u32, Vec<DamageSource>>,
    builds: &HashMap<u32, Vec<BuildEntry>>,
) -> Player {
    let stats = final_stats.get(&player.slot).cloned().unwrap_or_default();
    Player {
        slot: player.slot,
        team: player.team,
        member: member_for(members, player.account_id),
        hero: player.hero.clone(),
        kills: player.kills,
        deaths: player.deaths,
        assists: player.assists,
        souls: stats.souls,
        last_hits: stats.last_hits,
        denies: stats.denies,
        level: stats.level,
        hero_damage: stats.hero_damage,
        healing: stats.healing,
        damage_taken: stats.damage_taken,
        damage_mitigated: stats.damage_mitigated,
        aim: Aim {
            shots_hit: stats.shots_hit,
            shots_missed: stats.shots_missed,
            hero_hits: stats.hero_hits,
            hero_crits: stats.hero_crits,
        },
        kill_types: KillTypes {
            bullet: stats.bullet_kills,
            ability: stats.ability_kills,
            melee: stats.melee_kills,
            headshot: stats.headshot_kills,
        },
        timeline: timelines.get(&player.slot).cloned().unwrap_or_default(),
        damage_by_source: damage.get(&player.slot).cloned().unwrap_or_default(),
        build: builds.get(&player.slot).cloned().unwrap_or_default(),
    }
}

async fn query_player_average(
    state: &ReadApiState,
    account_id: u32,
    name: String,
    last_matches: Option<u32>,
) -> Result<PlayerAverage, (StatusCode, Json<ErrorBody>)> {
    let stats_filter = player_match_filter(&state.database, account_id, last_matches, "stats");
    let players_filter = player_match_filter(&state.database, account_id, last_matches, "players");
    let sql = format!(
        "SELECT count() AS matches, sum(kills) AS kills, sum(deaths) AS deaths, sum(assists) AS assists, sum(shots_hit) AS shots_hit, sum(shots_missed) AS shots_missed, sum(hero_hits) AS hero_hits, sum(hero_crits) AS hero_crits, avg(toFloat64(player_damage) * 60.0 / greatest(toFloat64(ifNull(match_rows.duration_s, 60)), 60.0)) AS damage_per_minute, sum(if(match_rows.winning_team = players.team, 1, 0)) AS wins FROM (SELECT stats.match_id, stats.player_slot, argMax(ifNull(stats.kills, 0), stats.snapshot_time_s) AS kills, argMax(ifNull(stats.deaths, 0), stats.snapshot_time_s) AS deaths, argMax(ifNull(stats.assists, 0), stats.snapshot_time_s) AS assists, argMax(ifNull(stats.shots_hit, 0), stats.snapshot_time_s) AS shots_hit, argMax(ifNull(stats.shots_missed, 0), stats.snapshot_time_s) AS shots_missed, argMax(ifNull(stats.hero_bullets_hit, 0), stats.snapshot_time_s) AS hero_hits, argMax(ifNull(stats.hero_bullets_hit_crit, 0), stats.snapshot_time_s) AS hero_crits, argMax(ifNull(stats.player_damage, 0), stats.snapshot_time_s) AS player_damage FROM {db}.match_player_stats AS stats WHERE stats.account_id = {account_id}{stats_filter} GROUP BY stats.match_id, stats.player_slot) AS stats LEFT JOIN (SELECT players.match_id, players.player_slot, any(players.team) AS team FROM {db}.match_players AS players WHERE players.account_id = {account_id}{players_filter} GROUP BY players.match_id, players.player_slot) AS players ON players.match_id = stats.match_id AND players.player_slot = stats.player_slot INNER JOIN {db}.matches AS match_rows ON match_rows.match_id = stats.match_id",
        db = state.database,
        stats_filter = stats_filter,
        players_filter = players_filter,
    );
    let rows = query_rows(state, &sql).await?;
    let Some(row) = rows.first() else {
        return Err(api_error(StatusCode::NOT_FOUND, "player not found"));
    };
    let matches = number_u64(row, "matches").unwrap_or_default();
    if matches == 0 {
        return Err(api_error(StatusCode::NOT_FOUND, "player not found"));
    }
    let kills = number_f64(row, "kills");
    let deaths = number_f64(row, "deaths");
    let assists = number_f64(row, "assists");
    let shots_hit = number_f64(row, "shots_hit");
    let shots_missed = number_f64(row, "shots_missed");
    let hero_hits = number_f64(row, "hero_hits");
    let hero_crits = number_f64(row, "hero_crits");
    let damage_per_minute = number_f64(row, "damage_per_minute");
    let wins = number_f64(row, "wins");
    let heroes = query_player_heroes(state, account_id, last_matches).await?;
    let recent = query_player_recent(state, account_id, last_matches).await?;
    Ok(PlayerAverage {
        name,
        matches,
        kdr: kills / deaths.max(1.0),
        kda: (kills + assists) / deaths.max(1.0),
        accuracy: shots_hit / (shots_hit + shots_missed).max(1.0),
        crit_rate: hero_crits / hero_hits.max(1.0),
        win_rate: wins / matches as f64,
        damage_per_minute,
        heroes,
        recent,
    })
}

async fn query_player_heroes(
    state: &ReadApiState,
    account_id: u32,
    last_matches: Option<u32>,
) -> Result<Vec<HeroAverage>, (StatusCode, Json<ErrorBody>)> {
    let match_filter = player_match_filter(&state.database, account_id, last_matches, "p");
    let sql = format!(
        "SELECT p.hero_id, h.name AS hero_name, count() AS matches FROM {db}.match_players AS p LEFT JOIN {db}.heroes AS h ON h.id = p.hero_id WHERE p.account_id = {account_id} AND p.hero_id != 0{match_filter} GROUP BY p.hero_id, h.name ORDER BY matches DESC, p.hero_id LIMIT 20",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            Some(HeroAverage {
                id: number_u32(&row, "hero_id")?,
                name: non_empty_string(&row, "hero_name").unwrap_or_else(|| "Unknown".to_string()),
                matches: number_u64(&row, "matches").unwrap_or_default(),
            })
        })
        .collect())
}

async fn query_player_recent(
    state: &ReadApiState,
    account_id: u32,
    last_matches: Option<u32>,
) -> Result<Vec<RecentMatch>, (StatusCode, Json<ErrorBody>)> {
    let match_filter = player_match_filter(&state.database, account_id, last_matches, "p");
    let sql = format!(
        "SELECT m.public_id AS uuid, m.match_id, m.start_time, m.duration_s, m.winning_team, p.team, p.hero_id, p.kills, p.deaths, p.assists, h.name AS hero_name, h.image_url AS hero_image, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_sm'), ''), nullIf(h.image_url, '')) AS hero_icon, coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero_card'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_card'), ''), nullIf(h.image_url, '')) AS hero_card FROM {db}.matches AS m INNER JOIN {db}.match_players AS p ON p.match_id = m.match_id LEFT JOIN {db}.heroes AS h ON h.id = p.hero_id WHERE p.account_id = {account_id}{match_filter} ORDER BY m.start_time DESC, m.match_id DESC LIMIT 20",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let match_id = number_u64(&row, "match_id")?;
            Some(RecentMatch {
                uuid: non_empty_string(&row, "uuid").unwrap_or_else(|| match_id.to_string()),
                match_id,
                start_time: number_u32(&row, "start_time").unwrap_or_default(),
                duration_s: number_u32(&row, "duration_s").unwrap_or_default(),
                winning_team: number_i32(&row, "winning_team").unwrap_or_default(),
                team: number_i32(&row, "team").unwrap_or_default(),
                hero: Hero {
                    id: number_u32(&row, "hero_id").unwrap_or_default(),
                    name: non_empty_string(&row, "hero_name")
                        .unwrap_or_else(|| "Unknown".to_string()),
                    icon: non_empty_string(&row, "hero_icon")
                        .or_else(|| non_empty_string(&row, "hero_image")),
                    card: non_empty_string(&row, "hero_card"),
                },
                kills: number_u32(&row, "kills").unwrap_or_default(),
                deaths: number_u32(&row, "deaths").unwrap_or_default(),
                assists: number_u32(&row, "assists").unwrap_or_default(),
            })
        })
        .collect())
}

fn player_match_filter(
    database: &str,
    account_id: u32,
    last_matches: Option<u32>,
    alias: &str,
) -> String {
    let Some(last_matches) = last_matches else {
        return String::new();
    };
    format!(
        " AND {alias}.match_id IN (SELECT selected.match_id FROM {database}.matches AS selected INNER JOIN {database}.match_players AS selected_players ON selected_players.match_id = selected.match_id WHERE selected_players.account_id = {account_id} GROUP BY selected.match_id, selected.start_time ORDER BY selected.start_time DESC, selected.match_id DESC LIMIT {last_matches})"
    )
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
    fn bounds_player_match_windows_without_interpolating_user_input() {
        assert!(validate_player_limit(Some(1)).is_ok());
        assert!(validate_player_limit(Some(100)).is_ok());
        assert!(validate_player_limit(Some(0)).is_err());
        assert!(validate_player_limit(Some(101)).is_err());
        assert_eq!(player_match_filter("singularity", 42, None, "p"), "");
        assert!(player_match_filter("singularity", 42, Some(10), "p").contains("LIMIT 10"));
    }

    #[test]
    fn folds_weapon_and_melee_damage_sources() {
        assert_eq!(
            normalize_source("citadel_weapon_punkgoat_set", None, None),
            (
                "weapon".to_string(),
                "Gun".to_string(),
                "weapon".to_string()
            )
        );
        assert_eq!(
            normalize_source("ability_melee_punkgoat", None, None),
            (
                "melee".to_string(),
                "Melee".to_string(),
                "melee".to_string()
            )
        );
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
    fn member_name_lookup_is_case_insensitive() {
        let members = HashMap::from([(42_u32, "Cheesecake".to_string())]);
        let found = members
            .iter()
            .find(|(_, name)| name.eq_ignore_ascii_case("cheesecake"))
            .map(|(account_id, name)| (*account_id, name.clone()));
        assert_eq!(found, Some((42, "Cheesecake".to_string())));
    }
}
