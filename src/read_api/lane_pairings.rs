use std::collections::{BTreeMap, HashMap};

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde::Serialize;
use serde_json::Value;

use crate::{AppState, ErrorBody};

use super::{
    authorize, lane_stats::LANE_PHASE_END_S, number_i32, number_u32, number_u64, optional_u32,
    query_rows,
};

#[derive(Debug, Serialize)]
pub(crate) struct LanePairings {
    lane_phase_end_s: u32,
    pairings: Vec<LanePairing>,
}

#[derive(Debug, Serialize)]
struct LanePairing {
    members: [String; 2],
    matches: u32,
    result_matches: u32,
    wins: u32,
    match_win_rate: Option<f64>,
    lane_samples: u32,
    lane_leads: u32,
    lane_ties: u32,
    lane_trails: u32,
    lane_lead_rate: Option<f64>,
    average_net_worth_lead: Option<f64>,
}

#[derive(Debug)]
struct LanePlayer {
    slot: u32,
    member: Option<String>,
    net_worth: Option<u32>,
}

#[derive(Debug)]
struct LaneLineup {
    match_id: u64,
    winning_team: Option<i32>,
    team: i32,
    assigned_lane: u32,
    players: Vec<LanePlayer>,
}

#[derive(Debug, Default)]
struct PairingTotals {
    matches: u32,
    result_matches: u32,
    wins: u32,
    lane_samples: u32,
    lane_leads: u32,
    lane_ties: u32,
    lane_trails: u32,
    net_worth_lead_total: i64,
}

pub(crate) async fn get_lane_pairings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<LanePairings>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let accounts = state.read_api.members.keys().copied().collect::<Vec<_>>();
    if accounts.len() < 2 {
        return Ok(Json(LanePairings {
            lane_phase_end_s: LANE_PHASE_END_S,
            pairings: Vec::new(),
        }));
    }
    let mut accounts = accounts;
    accounts.sort_unstable();

    let rows = query_rows(
        &state.read_api,
        &lane_pairings_query(&state.read_api.database, &accounts),
    )
    .await?;
    Ok(Json(LanePairings {
        lane_phase_end_s: LANE_PHASE_END_S,
        pairings: aggregate_pairings(rows, &state.read_api.members),
    }))
}

fn lane_pairings_query(database: &str, accounts: &[u32]) -> String {
    let accounts = accounts
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "WITH duo_lanes AS (\
           SELECT match_id, assigned_lane FROM {database}.match_players FINAL \
           WHERE account_id IN ({accounts}) AND assigned_lane IS NOT NULL \
           GROUP BY match_id, assigned_lane HAVING uniqExact(account_id) >= 2\
         ), lane_snapshots AS (\
           SELECT match_id, player_slot, argMax(net_worth, snapshot_time_s) AS lane_net_worth \
           FROM {database}.match_player_stats FINAL \
           WHERE snapshot_time_s <= {lane_phase_end_s} \
             AND match_id IN (SELECT match_id FROM duo_lanes) \
           GROUP BY match_id, player_slot\
         ) \
         SELECT m.match_id, m.winning_team, p.team, p.assigned_lane, \
                 p.player_slot, p.account_id, lane.lane_net_worth \
         FROM duo_lanes AS duo \
         INNER JOIN {database}.match_players AS p FINAL \
           ON p.match_id = duo.match_id AND p.assigned_lane = duo.assigned_lane \
         INNER JOIN {database}.matches AS m FINAL ON m.match_id = p.match_id \
         LEFT JOIN lane_snapshots AS lane \
           ON lane.match_id = p.match_id AND lane.player_slot = p.player_slot \
         ORDER BY m.start_time DESC, m.match_id DESC, p.assigned_lane, p.team, p.player_slot",
        lane_phase_end_s = LANE_PHASE_END_S,
    )
}

fn aggregate_pairings(rows: Vec<Value>, members: &HashMap<u32, String>) -> Vec<LanePairing> {
    let mut lineups: BTreeMap<(u64, u32, i32), LaneLineup> = BTreeMap::new();
    for row in rows {
        let (Some(match_id), Some(assigned_lane), Some(team), Some(slot)) = (
            number_u64(&row, "match_id"),
            number_u32(&row, "assigned_lane"),
            number_i32(&row, "team"),
            number_u32(&row, "player_slot"),
        ) else {
            continue;
        };
        let lineup = lineups
            .entry((match_id, assigned_lane, team))
            .or_insert_with(|| LaneLineup {
                match_id,
                winning_team: row
                    .get("winning_team")
                    .filter(|value| !value.is_null())
                    .and_then(|_| number_i32(&row, "winning_team")),
                team,
                assigned_lane,
                players: Vec::new(),
            });
        if lineup.players.iter().any(|player| player.slot == slot) {
            continue;
        }
        let member =
            number_u32(&row, "account_id").and_then(|account| members.get(&account).cloned());
        lineup.players.push(LanePlayer {
            slot,
            member,
            net_worth: optional_u32(&row, "lane_net_worth"),
        });
    }

    let mut totals: BTreeMap<[String; 2], PairingTotals> = BTreeMap::new();
    for lineup in lineups.values() {
        let mut tracked = lineup
            .players
            .iter()
            .filter_map(|player| player.member.clone())
            .collect::<Vec<_>>();
        tracked.sort();
        tracked.dedup();
        if tracked.len() != 2 {
            continue;
        }
        let pair: [String; 2] = [tracked.remove(0), tracked.remove(0)];
        let total = totals.entry(pair).or_default();
        total.matches += 1;
        if let Some(winner) = lineup.winning_team {
            total.result_matches += 1;
            if winner == lineup.team {
                total.wins += 1;
            }
        }

        let opponent_team = match lineup.team {
            0 => 1,
            1 => 0,
            _ => continue,
        };
        let Some(opponent) = lineups.get(&(lineup.match_id, lineup.assigned_lane, opponent_team))
        else {
            continue;
        };
        if lineup.players.len() != 2 || opponent.players.len() != 2 {
            continue;
        }
        let (Some(ours), Some(theirs)) = (
            net_worth_total(&lineup.players),
            net_worth_total(&opponent.players),
        ) else {
            continue;
        };
        let lead = i64::from(ours) - i64::from(theirs);
        total.lane_samples += 1;
        total.net_worth_lead_total += lead;
        if lead > 0 {
            total.lane_leads += 1;
        } else if lead < 0 {
            total.lane_trails += 1;
        } else {
            total.lane_ties += 1;
        }
    }

    let mut pairings = totals
        .into_iter()
        .map(|(members, total)| {
            let matches = total.matches;
            let result_matches = total.result_matches;
            let lane_samples = total.lane_samples;
            LanePairing {
                members,
                matches,
                result_matches,
                wins: total.wins,
                match_win_rate: (result_matches > 0)
                    .then_some(f64::from(total.wins) / f64::from(result_matches)),
                lane_samples,
                lane_leads: total.lane_leads,
                lane_ties: total.lane_ties,
                lane_trails: total.lane_trails,
                lane_lead_rate: (lane_samples > 0)
                    .then_some(f64::from(total.lane_leads) / f64::from(lane_samples)),
                average_net_worth_lead: (lane_samples > 0)
                    .then_some(total.net_worth_lead_total as f64 / f64::from(lane_samples)),
            }
        })
        .collect::<Vec<_>>();
    pairings.sort_by(|a, b| {
        b.average_net_worth_lead
            .partial_cmp(&a.average_net_worth_lead)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.lane_samples.cmp(&a.lane_samples))
            .then_with(|| a.members.cmp(&b.members))
    });
    pairings
}

fn net_worth_total(players: &[LanePlayer]) -> Option<u32> {
    players
        .iter()
        .try_fold(0_u32, |total, player| total.checked_add(player.net_worth?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;

    fn row(
        slot: u32,
        account: u32,
        team: u32,
        assigned_lane: u32,
        net_worth: Option<u32>,
        winning_team: u32,
    ) -> Value {
        json!({
            "uuid": "match-a",
            "match_id": 42,
            "start_time": 100,
            "winning_team": winning_team,
            "team": team,
            "assigned_lane": assigned_lane,
            "player_slot": slot,
            "account_id": account,
            "lane_net_worth": net_worth,
        })
    }

    #[test]
    fn aggregates_tracked_duos_by_lane_lead_and_match_result_without_exposing_ids() {
        let members = HashMap::from([(11, "alice".to_string()), (22, "bob".to_string())]);
        let rows = vec![
            row(1, 11, 0, 2, Some(8000), 0),
            row(2, 22, 0, 2, Some(7000), 0),
            row(7, 33, 1, 2, Some(6000), 0),
            row(8, 44, 1, 2, Some(7000), 0),
        ];

        let pairings = aggregate_pairings(rows, &members);
        assert_eq!(pairings.len(), 1);
        let pairing = &pairings[0];
        assert_eq!(pairing.members, ["alice", "bob"]);
        assert_eq!(pairing.matches, 1);
        assert_eq!(pairing.wins, 1);
        assert_eq!(pairing.match_win_rate, Some(1.0));
        assert_eq!(pairing.lane_samples, 1);
        assert_eq!(pairing.lane_leads, 1);
        assert_eq!(pairing.average_net_worth_lead, Some(2000.0));

        let serialized = serde_json::to_value(pairing).unwrap();
        assert!(serialized.get("account_id").is_none());
        assert!(serialized.get("members").is_some());
    }

    #[test]
    fn missing_opponent_snapshot_keeps_match_result_but_skips_lane_comparison() {
        let members = HashMap::from([(11, "alice".to_string()), (22, "bob".to_string())]);
        let rows = vec![
            row(1, 11, 0, 2, Some(8000), 1),
            row(2, 22, 0, 2, Some(7000), 1),
            row(7, 33, 1, 2, None, 1),
            row(8, 44, 1, 2, Some(7000), 1),
        ];

        let pairings = aggregate_pairings(rows, &members);
        assert_eq!(pairings[0].matches, 1);
        assert_eq!(pairings[0].wins, 0);
        assert_eq!(pairings[0].lane_samples, 0);
        assert_eq!(pairings[0].average_net_worth_lead, None);
    }

    #[test]
    fn query_uses_recorded_assignments_and_inclusive_lane_window() {
        let sql = lane_pairings_query("singularity", &[11, 22]);

        assert!(sql.contains("assigned_lane IS NOT NULL"));
        assert!(sql.contains("uniqExact(account_id) >= 2"));
        assert!(sql.contains("snapshot_time_s <= 600"));
        assert!(sql.contains("AS lane_net_worth"));
        assert!(sql.contains("IN (11,22)"));
    }
}
