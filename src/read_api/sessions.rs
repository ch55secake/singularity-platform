use std::collections::HashMap;

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde::Serialize;
use serde_json::Value;

use crate::{AppState, ErrorBody};

use super::{
    ImpactMetrics, authorize, calculate_impact_scores, non_empty_string, number_f64, number_i32,
    number_u32, number_u64, query_rows, round_score,
};

const SESSION_GAP_SECONDS: u32 = 90 * 60;

#[derive(Debug, Serialize)]
pub(crate) struct SessionSummary {
    start_time: u32,
    end_time: u32,
    duration_s: u32,
    matches: Vec<SessionMatchSummary>,
    members: Vec<String>,
    wins: u32,
    losses: u32,
    splits: u32,
    win_rate: f64,
    impact_score: f64,
}

#[derive(Debug, Serialize)]
struct SessionMatchSummary {
    uuid: String,
    match_id: u64,
    start_time: u32,
    duration_s: u32,
    result: SessionMatchResult,
    members: Vec<String>,
    impact_score: f64,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SessionMatchResult {
    Win,
    Loss,
    Split,
}

#[derive(Debug)]
struct SessionMatchData {
    uuid: String,
    match_id: u64,
    start_time: u32,
    duration_s: u32,
    winning_team: i32,
    players: Vec<ImpactMetrics>,
    tracked_players: Vec<(String, i32, u32)>,
}

pub(crate) async fn list_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<SessionSummary>>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    if state.read_api.members.is_empty() {
        return Ok(Json(Vec::new()));
    }

    let member_ids = state
        .read_api
        .members
        .keys()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "WITH target_matches AS (SELECT public_id AS uuid, match_id, start_time, duration_s, winning_team FROM {db}.matches FINAL WHERE match_id IN (SELECT DISTINCT match_id FROM {db}.match_players FINAL WHERE account_id IN ({member_ids}))), latest_stats AS (SELECT stats.match_id, stats.player_slot, argMax(ifNull(stats.player_damage, 0), stats.snapshot_time_s) AS hero_damage, argMax(ifNull(stats.player_healing, 0), stats.snapshot_time_s) AS healing, argMax(ifNull(stats.damage_mitigated, 0), stats.snapshot_time_s) AS damage_mitigated FROM {db}.match_player_stats AS stats WHERE stats.match_id IN (SELECT match_id FROM target_matches) GROUP BY stats.match_id, stats.player_slot) SELECT target_matches.uuid, target_matches.match_id, target_matches.start_time, target_matches.duration_s, target_matches.winning_team, players.account_id, players.player_slot, players.team, players.kills, players.deaths, players.assists, players.net_worth AS souls, players.last_hits, players.denies, ifNull(latest_stats.hero_damage, 0) AS hero_damage, ifNull(latest_stats.healing, 0) AS healing, ifNull(latest_stats.damage_mitigated, 0) AS damage_mitigated FROM target_matches INNER JOIN {db}.match_players AS players FINAL ON players.match_id = target_matches.match_id LEFT JOIN latest_stats ON latest_stats.match_id = players.match_id AND latest_stats.player_slot = players.player_slot ORDER BY target_matches.start_time, target_matches.match_id, players.player_slot",
        db = state.read_api.database,
    );
    let rows = query_rows(&state.read_api, &sql).await?;
    Ok(Json(sessions_from_rows(rows, &state.read_api.members)))
}

fn sessions_from_rows(rows: Vec<Value>, members: &HashMap<u32, String>) -> Vec<SessionSummary> {
    let mut grouped: HashMap<u64, SessionMatchData> = HashMap::new();
    for row in rows {
        let (Some(match_id), Some(slot)) = (
            number_u64(&row, "match_id"),
            number_u32(&row, "player_slot"),
        ) else {
            continue;
        };
        let account_id = number_u32(&row, "account_id").unwrap_or_default();
        let start_time = number_u32(&row, "start_time").unwrap_or_default();
        let duration_s = number_u32(&row, "duration_s").unwrap_or_default();
        let winning_team = number_i32(&row, "winning_team").unwrap_or_default();
        let entry = grouped.entry(match_id).or_insert_with(|| SessionMatchData {
            uuid: non_empty_string(&row, "uuid").unwrap_or_else(|| match_id.to_string()),
            match_id,
            start_time,
            duration_s,
            winning_team,
            players: Vec::new(),
            tracked_players: Vec::new(),
        });
        entry.players.push(ImpactMetrics {
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
        if let Some(name) = members.get(&account_id) {
            entry.tracked_players.push((
                name.clone(),
                number_i32(&row, "team").unwrap_or_default(),
                slot,
            ));
        }
    }

    let mut matches = grouped.into_values().collect::<Vec<_>>();
    matches.sort_by_key(|match_data| (match_data.start_time, match_data.match_id));
    group_sessions(matches)
}

fn group_sessions(matches: Vec<SessionMatchData>) -> Vec<SessionSummary> {
    let mut sessions: Vec<SessionSummary> = Vec::new();
    let mut previous_match_end = None;

    for match_data in matches {
        let match_summary = session_match_summary(match_data);
        if match_summary.members.is_empty() {
            continue;
        }

        let new_session = previous_match_end.is_some_and(|previous_end: u32| {
            match_summary.start_time > previous_end
                && match_summary.start_time - previous_end >= SESSION_GAP_SECONDS
        });
        if sessions.is_empty() || new_session {
            sessions.push(SessionSummary {
                start_time: match_summary.start_time,
                end_time: match_summary
                    .start_time
                    .saturating_add(match_summary.duration_s),
                duration_s: match_summary.duration_s,
                matches: Vec::new(),
                members: Vec::new(),
                wins: 0,
                losses: 0,
                splits: 0,
                win_rate: 0.0,
                impact_score: 0.0,
            });
        }

        let session = sessions.last_mut().expect("session is initialized");
        let match_end = match_summary
            .start_time
            .saturating_add(match_summary.duration_s);
        session.end_time = session.end_time.max(match_end);
        session
            .members
            .extend(match_summary.members.iter().cloned());
        match match_summary.result {
            SessionMatchResult::Win => session.wins += 1,
            SessionMatchResult::Loss => session.losses += 1,
            SessionMatchResult::Split => session.splits += 1,
        }
        session.matches.push(match_summary);
        previous_match_end = Some(match_end);
    }

    for session in &mut sessions {
        session.duration_s = session.end_time.saturating_sub(session.start_time);
        session.members.sort();
        session.members.dedup();
        let match_count = session.matches.len() as f64;
        session.win_rate = if match_count == 0.0 {
            0.0
        } else {
            session.wins as f64 / match_count
        };
        session.impact_score = if match_count == 0.0 {
            0.0
        } else {
            round_score(
                session
                    .matches
                    .iter()
                    .map(|match_data| match_data.impact_score)
                    .sum::<f64>()
                    / match_count,
            )
        };
    }
    sessions.reverse();
    sessions
}

fn session_match_summary(match_data: SessionMatchData) -> SessionMatchSummary {
    let impact_scores = calculate_impact_scores(match_data.duration_s, &match_data.players);
    let member_scores = match_data
        .tracked_players
        .iter()
        .filter_map(|(_, _, slot)| impact_scores.get(slot).map(|score| score.score))
        .collect::<Vec<_>>();
    let wins = match_data
        .tracked_players
        .iter()
        .filter(|(_, team, _)| *team == match_data.winning_team)
        .count();
    let result = if wins == match_data.tracked_players.len() {
        SessionMatchResult::Win
    } else if wins == 0 {
        SessionMatchResult::Loss
    } else {
        SessionMatchResult::Split
    };
    let mut members = match_data
        .tracked_players
        .iter()
        .map(|(name, _, _)| name.clone())
        .collect::<Vec<_>>();
    members.sort();
    members.dedup();

    SessionMatchSummary {
        uuid: match_data.uuid,
        match_id: match_data.match_id,
        start_time: match_data.start_time,
        duration_s: match_data.duration_s,
        result,
        members,
        impact_score: if member_scores.is_empty() {
            0.0
        } else {
            round_score(member_scores.iter().sum::<f64>() / member_scores.len() as f64)
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_match(
        match_id: u64,
        start_time: u32,
        duration_s: u32,
        members: &[(&str, i32)],
    ) -> SessionMatchData {
        SessionMatchData {
            uuid: format!("00000000-0000-0000-0000-{match_id:012x}"),
            match_id,
            start_time,
            duration_s,
            winning_team: 0,
            players: Vec::new(),
            tracked_players: members
                .iter()
                .enumerate()
                .map(|(index, (name, team))| ((*name).to_string(), *team, index as u32))
                .collect(),
        }
    }

    #[test]
    fn session_boundary_uses_90_minutes_since_previous_match_end() {
        let sessions = group_sessions(vec![
            session_match(1, 1_000, 600, &[("Oscar", 0)]),
            session_match(2, 6_999, 600, &[("Max", 1)]),
            session_match(3, 12_999, 600, &[("Oscar", 0), ("Max", 1)]),
        ]);

        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].matches.len(), 1);
        assert_eq!(sessions[0].matches[0].result, SessionMatchResult::Split);
        assert_eq!(sessions[1].matches.len(), 2);
        assert_eq!(sessions[1].wins, 1);
        assert_eq!(sessions[1].losses, 1);
        assert_eq!(sessions[1].win_rate, 0.5);
    }

    #[test]
    fn session_impact_uses_tracked_members_impact_scores() {
        let mut match_data = session_match(1, 1_000, 600, &[("Oscar", 0)]);
        match_data.players = vec![
            ImpactMetrics {
                slot: 0,
                hero_damage: 10_000.0,
                kills: 20.0,
                deaths: 0.0,
                assists: 20.0,
                souls: 100_000.0,
                last_hits: 100.0,
                denies: 20.0,
                healing: 10_000.0,
                damage_mitigated: 10_000.0,
            },
            ImpactMetrics {
                slot: 1,
                hero_damage: 0.0,
                kills: 0.0,
                deaths: 30.0,
                assists: 0.0,
                souls: 0.0,
                last_hits: 0.0,
                denies: 0.0,
                healing: 0.0,
                damage_mitigated: 0.0,
            },
        ];
        let expected = calculate_impact_scores(600, &match_data.players)[&0].score;

        let sessions = group_sessions(vec![match_data]);

        assert!(expected > 0.0);
        assert_eq!(sessions[0].matches[0].impact_score, expected);
        assert_eq!(sessions[0].impact_score, expected);
    }
}
