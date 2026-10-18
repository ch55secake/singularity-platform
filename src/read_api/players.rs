use std::collections::{BTreeMap, HashMap};

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};

use crate::{AppState, ErrorBody};

use super::{
    Hero, IMPACT_SCORE_VERSION, ImpactMetrics, ReadApiState, api_error, authorize,
    calculate_impact_scores, non_empty_string, number_f64, number_i32, number_u32, number_u64,
    query_rows, round_score,
};

const MAX_PLAYER_MATCHES: u32 = 100;

#[derive(Debug, Deserialize, Default)]
pub(crate) struct PlayerQuery {
    last: Option<u32>,
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
    average_impact_score: f64,
    impact_score_matches: u64,
    impact_score_version: u32,
    heroes: Vec<HeroAverage>,
    recent: Vec<RecentMatch>,
}

#[derive(Debug, Serialize)]
struct HeroAverage {
    id: u32,
    name: String,
    matches: u64,
    icon: Option<String>,
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
    impact_score: f64,
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
    let Some((account_id, canonical_name)) = member_by_name(&state.read_api.members, &name) else {
        return Err(api_error(StatusCode::NOT_FOUND, "player not found"));
    };
    let last_matches = validate_player_limit(query.last)?;
    let summary =
        query_player_average(&state.read_api, account_id, canonical_name, last_matches).await?;
    Ok(Json(summary))
}

fn member_by_name(members: &HashMap<u32, String>, name: &str) -> Option<(u32, String)> {
    members
        .iter()
        .find(|(_, member_name)| member_name.eq_ignore_ascii_case(name))
        .map(|(account_id, member_name)| (*account_id, member_name.clone()))
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

async fn query_player_impact_scores(
    state: &ReadApiState,
    account_id: u32,
    last_matches: Option<u32>,
) -> Result<BTreeMap<u64, f64>, (StatusCode, Json<ErrorBody>)> {
    let limit = last_matches
        .map(|count| format!("LIMIT {count}"))
        .unwrap_or_default();
    let sql = format!(
        "WITH target_matches AS (SELECT match_rows.match_id, match_rows.start_time, match_rows.duration_s FROM {db}.matches AS match_rows INNER JOIN {db}.match_players AS target ON target.match_id = match_rows.match_id WHERE target.account_id = {account_id} GROUP BY match_rows.match_id, match_rows.start_time, match_rows.duration_s ORDER BY match_rows.start_time DESC, match_rows.match_id DESC {limit}), latest_stats AS (SELECT stats.match_id, stats.player_slot, argMax(ifNull(stats.player_damage, 0), stats.snapshot_time_s) AS hero_damage, argMax(ifNull(stats.player_healing, 0), stats.snapshot_time_s) AS healing, argMax(ifNull(stats.damage_mitigated, 0), stats.snapshot_time_s) AS damage_mitigated FROM {db}.match_player_stats AS stats WHERE stats.match_id IN (SELECT match_id FROM target_matches) GROUP BY stats.match_id, stats.player_slot) SELECT target_matches.match_id, target_matches.duration_s, players.account_id, players.player_slot, players.kills, players.deaths, players.assists, players.net_worth AS souls, players.last_hits, players.denies, ifNull(latest_stats.hero_damage, 0) AS hero_damage, ifNull(latest_stats.healing, 0) AS healing, ifNull(latest_stats.damage_mitigated, 0) AS damage_mitigated FROM target_matches INNER JOIN {db}.match_players AS players ON players.match_id = target_matches.match_id LEFT JOIN latest_stats ON latest_stats.match_id = players.match_id AND latest_stats.player_slot = players.player_slot ORDER BY target_matches.start_time DESC, target_matches.match_id DESC, players.player_slot",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    let mut grouped: BTreeMap<u64, (u32, Vec<ImpactMetrics>, Vec<u32>)> = BTreeMap::new();
    for row in rows {
        let (Some(match_id), Some(slot)) = (
            number_u64(&row, "match_id"),
            number_u32(&row, "player_slot"),
        ) else {
            continue;
        };
        let account = number_u32(&row, "account_id").unwrap_or_default();
        let entry = grouped.entry(match_id).or_insert_with(|| {
            (
                number_u32(&row, "duration_s").unwrap_or_default(),
                Vec::new(),
                Vec::new(),
            )
        });
        entry.1.push(ImpactMetrics {
            slot,
            hero_damage: number_f64(&row, "hero_damage"),
            kills: f64::from(number_u32(&row, "kills").unwrap_or_default()),
            deaths: f64::from(number_u32(&row, "deaths").unwrap_or_default()),
            assists: f64::from(number_u32(&row, "assists").unwrap_or_default()),
            souls: number_f64(&row, "souls"),
            last_hits: number_f64(&row, "last_hits"),
            denies: number_f64(&row, "denies"),
            healing: number_f64(&row, "healing"),
            damage_mitigated: number_f64(&row, "damage_mitigated"),
        });
        if account == account_id {
            entry.2.push(slot);
        }
    }

    let mut scores = BTreeMap::new();
    for (match_id, (duration_s, players, target_slots)) in grouped {
        if target_slots.is_empty() {
            continue;
        }
        let match_scores = calculate_impact_scores(duration_s, &players);
        let target_scores = target_slots
            .iter()
            .filter_map(|slot| match_scores.get(slot).map(|score| score.score))
            .collect::<Vec<_>>();
        if !target_scores.is_empty() {
            scores.insert(
                match_id,
                round_score(target_scores.iter().sum::<f64>() / target_scores.len() as f64),
            );
        }
    }
    Ok(scores)
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
    let impact_scores = query_player_impact_scores(state, account_id, last_matches).await?;
    let average_impact_score = if impact_scores.is_empty() {
        0.0
    } else {
        round_score(impact_scores.values().sum::<f64>() / impact_scores.len() as f64)
    };
    let mut recent = query_player_recent(state, account_id, last_matches).await?;
    for match_data in &mut recent {
        match_data.impact_score = impact_scores
            .get(&match_data.match_id)
            .copied()
            .unwrap_or_default();
    }
    Ok(PlayerAverage {
        name,
        matches,
        kdr: kills / deaths.max(1.0),
        kda: (kills + assists) / deaths.max(1.0),
        accuracy: shots_hit / (shots_hit + shots_missed).max(1.0),
        crit_rate: hero_crits / hero_hits.max(1.0),
        win_rate: wins / matches as f64,
        damage_per_minute,
        average_impact_score,
        impact_score_matches: impact_scores.len() as u64,
        impact_score_version: IMPACT_SCORE_VERSION,
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
        "SELECT p.hero_id, h.name AS hero_name, count() AS matches, any(coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_image_small_webp'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero'), ''), nullIf(JSONExtractString(h.payload_json, 'images', 'hero_sm'), ''), nullIf(h.image_url, ''))) AS hero_icon FROM {db}.match_players AS p LEFT JOIN {db}.heroes AS h ON h.id = p.hero_id WHERE p.account_id = {account_id} AND p.hero_id != 0{match_filter} GROUP BY p.hero_id, h.name ORDER BY matches DESC, p.hero_id LIMIT 20",
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
                icon: non_empty_string(&row, "hero_icon"),
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
                impact_score: 0.0,
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
    use std::collections::HashMap;

    use super::*;

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
    fn includes_hero_icon_urls_in_player_hero_averages() {
        let hero = HeroAverage {
            id: 72,
            name: "Billy".to_string(),
            matches: 4,
            icon: Some("https://assets.example/billy.webp".to_string()),
        };
        let json = serde_json::to_value(hero).unwrap();

        assert_eq!(json["icon"], "https://assets.example/billy.webp");
    }

    #[test]
    fn member_name_lookup_is_case_insensitive() {
        let members = HashMap::from([(42_u32, "Cheesecake".to_string())]);

        assert_eq!(
            member_by_name(&members, "cheesecake"),
            Some((42, "Cheesecake".to_string()))
        );
    }
}
