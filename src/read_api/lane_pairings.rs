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
    Hero, authorize, lane_stats::LANE_PHASE_END_S, non_empty_string, number_i32, number_u32,
    number_u64, optional_u32, query_rows,
};

const GAME_DETAIL_LIMIT: usize = 100;

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
    stats_samples: u32,
    average_kills: Option<f64>,
    average_deaths: Option<f64>,
    average_assists: Option<f64>,
    economy_samples: u32,
    average_creep_kills: Option<f64>,
    average_denies: Option<f64>,
    damage_samples: u32,
    average_hero_damage: Option<f64>,
    hero_combinations: Vec<LaneHeroCombination>,
    games: Vec<LanePairingGame>,
    games_truncated: bool,
}

#[derive(Debug, Serialize)]
struct LaneHeroCombination {
    heroes: [Hero; 2],
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

#[derive(Debug, Serialize)]
struct LanePairingGame {
    uuid: String,
    match_id: u64,
    start_time: u32,
    heroes: [Hero; 2],
    match_result: Option<&'static str>,
    lane_net_worth_lead: Option<i64>,
    stats: LaneDuoStats,
}

#[derive(Debug, Serialize, Clone, Copy)]
struct LaneDuoStats {
    kills: Option<u32>,
    deaths: Option<u32>,
    assists: Option<u32>,
    creep_kills: Option<u32>,
    denies: Option<u32>,
    hero_damage: Option<u32>,
}

#[derive(Debug)]
struct LanePlayer {
    slot: u32,
    member: Option<String>,
    hero: Hero,
    net_worth: Option<u32>,
    kills: Option<u32>,
    deaths: Option<u32>,
    assists: Option<u32>,
    creep_kills: Option<u32>,
    denies: Option<u32>,
    hero_damage: Option<u32>,
}

#[derive(Debug)]
struct LaneLineup {
    uuid: String,
    match_id: u64,
    start_time: u32,
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
    stats_samples: u32,
    kills_total: u64,
    deaths_total: u64,
    assists_total: u64,
    economy_samples: u32,
    creep_kills_total: u64,
    denies_total: u64,
    damage_samples: u32,
    hero_damage_total: u64,
    hero_combinations: BTreeMap<[LaneHeroKey; 2], HeroCombinationTotals>,
    games: Vec<LanePairingGame>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LaneHeroKey {
    id: u32,
    name: String,
    icon: Option<String>,
}

#[derive(Debug, Default)]
struct HeroCombinationTotals {
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
           SELECT match_id, player_slot, \
                  argMax(net_worth, snapshot_time_s) AS lane_net_worth, \
                  argMax(kills, snapshot_time_s) AS lane_kills, \
                  argMax(deaths, snapshot_time_s) AS lane_deaths, \
                  argMax(assists, snapshot_time_s) AS lane_assists, \
                  argMax(creep_kills, snapshot_time_s) AS lane_creep_kills, \
                  argMax(denies, snapshot_time_s) AS lane_denies, \
                  argMax(player_damage, snapshot_time_s) AS lane_hero_damage \
           FROM {database}.match_player_stats FINAL \
           WHERE snapshot_time_s <= {lane_phase_end_s} \
             AND match_id IN (SELECT match_id FROM duo_lanes) \
           GROUP BY match_id, player_slot\
         ) \
         SELECT m.public_id AS uuid, m.match_id, m.start_time, m.winning_team, \
                p.team, p.assigned_lane, p.player_slot, p.account_id, p.hero_id, \
                h.name AS hero_name, h.image_url AS hero_image, \
                coalesce(nullIf(JSONExtractString(h.payload_json, 'images', 'icon_hero'), ''), \
                         nullIf(JSONExtractString(h.payload_json, 'images', 'hero_sm'), ''), \
                         nullIf(h.image_url, '')) AS hero_icon, \
                lane.lane_net_worth, lane.lane_kills, lane.lane_deaths, \
                lane.lane_assists, lane.lane_creep_kills, lane.lane_denies, \
                lane.lane_hero_damage \
         FROM duo_lanes AS duo \
         INNER JOIN {database}.match_players AS p FINAL \
           ON p.match_id = duo.match_id AND p.assigned_lane = duo.assigned_lane \
         INNER JOIN {database}.matches AS m FINAL ON m.match_id = p.match_id \
         LEFT JOIN {database}.heroes AS h FINAL ON h.id = p.hero_id \
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
                uuid: non_empty_string(&row, "uuid").unwrap_or_else(|| match_id.to_string()),
                match_id,
                start_time: number_u32(&row, "start_time").unwrap_or_default(),
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
            hero: Hero {
                id: number_u32(&row, "hero_id").unwrap_or_default(),
                name: non_empty_string(&row, "hero_name").unwrap_or_else(|| "Unknown".to_string()),
                icon: non_empty_string(&row, "hero_icon")
                    .or_else(|| non_empty_string(&row, "hero_image")),
                card: None,
            },
            net_worth: optional_u32(&row, "lane_net_worth"),
            kills: optional_u32(&row, "lane_kills"),
            deaths: optional_u32(&row, "lane_deaths"),
            assists: optional_u32(&row, "lane_assists"),
            creep_kills: optional_u32(&row, "lane_creep_kills"),
            denies: optional_u32(&row, "lane_denies"),
            hero_damage: optional_u32(&row, "lane_hero_damage"),
        });
    }

    let mut totals: BTreeMap<[String; 2], PairingTotals> = BTreeMap::new();
    for lineup in lineups.values() {
        let mut duo_players = lineup
            .players
            .iter()
            .filter(|player| player.member.is_some())
            .collect::<Vec<_>>();
        duo_players.sort_by(|a, b| a.member.cmp(&b.member));
        if duo_players.len() != 2 || duo_players[0].member == duo_players[1].member {
            continue;
        }
        let pair = [
            duo_players[0].member.clone().unwrap(),
            duo_players[1].member.clone().unwrap(),
        ];
        let heroes = [duo_players[0].hero.clone(), duo_players[1].hero.clone()];
        let hero_key = [lane_hero_key(&heroes[0]), lane_hero_key(&heroes[1])];
        let stats = LaneDuoStats {
            kills: sum_pair(duo_players[0].kills, duo_players[1].kills),
            deaths: sum_pair(duo_players[0].deaths, duo_players[1].deaths),
            assists: sum_pair(duo_players[0].assists, duo_players[1].assists),
            creep_kills: sum_pair(duo_players[0].creep_kills, duo_players[1].creep_kills),
            denies: sum_pair(duo_players[0].denies, duo_players[1].denies),
            hero_damage: sum_pair(duo_players[0].hero_damage, duo_players[1].hero_damage),
        };
        let match_result = lineup
            .winning_team
            .map(|winner| if winner == lineup.team { "win" } else { "loss" });

        let lane_lead = opponent_lineup(lineup, &lineups).and_then(|opponent| {
            if lineup.players.len() != 2 || opponent.players.len() != 2 {
                return None;
            }
            let ours = net_worth_total(&lineup.players)?;
            let theirs = net_worth_total(&opponent.players)?;
            Some(i64::from(ours) - i64::from(theirs))
        });

        let total = totals.entry(pair).or_default();
        total.matches += 1;
        if match_result.is_some() {
            total.result_matches += 1;
            if match_result == Some("win") {
                total.wins += 1;
            }
        }

        if let (Some(kills), Some(deaths), Some(assists)) =
            (stats.kills, stats.deaths, stats.assists)
        {
            total.stats_samples += 1;
            total.kills_total += u64::from(kills);
            total.deaths_total += u64::from(deaths);
            total.assists_total += u64::from(assists);
        }
        if let (Some(creep_kills), Some(denies)) = (stats.creep_kills, stats.denies) {
            total.economy_samples += 1;
            total.creep_kills_total += u64::from(creep_kills);
            total.denies_total += u64::from(denies);
        }
        if let Some(hero_damage) = stats.hero_damage {
            total.damage_samples += 1;
            total.hero_damage_total += u64::from(hero_damage);
        }
        record_lane_lead(total, lane_lead);

        let combination = total.hero_combinations.entry(hero_key).or_default();
        combination.matches += 1;
        if match_result.is_some() {
            combination.result_matches += 1;
            if match_result == Some("win") {
                combination.wins += 1;
            }
        }
        record_combination_lane_lead(combination, lane_lead);
        total.games.push(LanePairingGame {
            uuid: lineup.uuid.clone(),
            match_id: lineup.match_id,
            start_time: lineup.start_time,
            heroes,
            match_result,
            lane_net_worth_lead: lane_lead,
            stats,
        });
    }

    let mut pairings = totals
        .into_iter()
        .map(|(members, total)| {
            let matches = total.matches;
            let result_matches = total.result_matches;
            let lane_samples = total.lane_samples;
            let mut games = total.games;
            games.sort_by(|a, b| {
                b.start_time
                    .cmp(&a.start_time)
                    .then_with(|| b.match_id.cmp(&a.match_id))
            });
            let games_truncated = games.len() > GAME_DETAIL_LIMIT;
            games.truncate(GAME_DETAIL_LIMIT);
            let mut hero_combinations = total
                .hero_combinations
                .into_iter()
                .map(|(keys, totals)| make_hero_combination(keys.map(hero_from_key), totals))
                .collect::<Vec<_>>();
            hero_combinations.sort_by(|a, b| {
                b.average_net_worth_lead
                    .partial_cmp(&a.average_net_worth_lead)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.matches.cmp(&a.matches))
                    .then_with(|| a.heroes[0].name.cmp(&b.heroes[0].name))
                    .then_with(|| a.heroes[1].name.cmp(&b.heroes[1].name))
            });
            LanePairing {
                members,
                matches,
                result_matches,
                wins: total.wins,
                match_win_rate: rate(total.wins, result_matches),
                lane_samples,
                lane_leads: total.lane_leads,
                lane_ties: total.lane_ties,
                lane_trails: total.lane_trails,
                lane_lead_rate: rate(total.lane_leads, lane_samples),
                average_net_worth_lead: average_signed(total.net_worth_lead_total, lane_samples),
                stats_samples: total.stats_samples,
                average_kills: average(total.kills_total, total.stats_samples),
                average_deaths: average(total.deaths_total, total.stats_samples),
                average_assists: average(total.assists_total, total.stats_samples),
                economy_samples: total.economy_samples,
                average_creep_kills: average(total.creep_kills_total, total.economy_samples),
                average_denies: average(total.denies_total, total.economy_samples),
                damage_samples: total.damage_samples,
                average_hero_damage: average(total.hero_damage_total, total.damage_samples),
                hero_combinations,
                games,
                games_truncated,
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

fn opponent_lineup<'a>(
    lineup: &LaneLineup,
    lineups: &'a BTreeMap<(u64, u32, i32), LaneLineup>,
) -> Option<&'a LaneLineup> {
    let opponent_team = match lineup.team {
        0 => 1,
        1 => 0,
        _ => return None,
    };
    lineups.get(&(lineup.match_id, lineup.assigned_lane, opponent_team))
}

fn sum_pair(first: Option<u32>, second: Option<u32>) -> Option<u32> {
    first?.checked_add(second?)
}

fn average(total: u64, samples: u32) -> Option<f64> {
    (samples > 0).then(|| total as f64 / f64::from(samples))
}

fn average_signed(total: i64, samples: u32) -> Option<f64> {
    (samples > 0).then(|| total as f64 / f64::from(samples))
}

fn rate(successes: u32, samples: u32) -> Option<f64> {
    (samples > 0).then(|| f64::from(successes) / f64::from(samples))
}

fn record_lane_lead(total: &mut PairingTotals, lead: Option<i64>) {
    let Some(lead) = lead else {
        return;
    };
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

fn record_combination_lane_lead(total: &mut HeroCombinationTotals, lead: Option<i64>) {
    let Some(lead) = lead else {
        return;
    };
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

fn lane_hero_key(hero: &Hero) -> LaneHeroKey {
    LaneHeroKey {
        id: hero.id,
        name: hero.name.clone(),
        icon: hero.icon.clone(),
    }
}

fn hero_from_key(key: LaneHeroKey) -> Hero {
    Hero {
        id: key.id,
        name: key.name,
        icon: key.icon,
        card: None,
    }
}

fn make_hero_combination(heroes: [Hero; 2], total: HeroCombinationTotals) -> LaneHeroCombination {
    LaneHeroCombination {
        heroes,
        matches: total.matches,
        result_matches: total.result_matches,
        wins: total.wins,
        match_win_rate: rate(total.wins, total.result_matches),
        lane_samples: total.lane_samples,
        lane_leads: total.lane_leads,
        lane_ties: total.lane_ties,
        lane_trails: total.lane_trails,
        lane_lead_rate: rate(total.lane_leads, total.lane_samples),
        average_net_worth_lead: average_signed(total.net_worth_lead_total, total.lane_samples),
    }
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
            "hero_id": 100 + slot,
            "hero_name": format!("Hero {slot}"),
            "hero_image": format!("https://example.test/hero-{slot}.webp"),
            "hero_icon": format!("https://example.test/hero-{slot}-icon.webp"),
            "lane_net_worth": net_worth,
            "lane_kills": slot,
            "lane_deaths": 1,
            "lane_assists": 2,
            "lane_creep_kills": slot + 10,
            "lane_denies": 1,
            "lane_hero_damage": slot * 100,
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
        assert_eq!(pairing.stats_samples, 1);
        assert_eq!(pairing.average_kills, Some(3.0));
        assert_eq!(pairing.average_deaths, Some(2.0));
        assert_eq!(pairing.average_assists, Some(4.0));
        assert_eq!(pairing.average_creep_kills, Some(23.0));
        assert_eq!(pairing.average_denies, Some(2.0));
        assert_eq!(pairing.average_hero_damage, Some(300.0));
        assert_eq!(pairing.hero_combinations.len(), 1);
        assert_eq!(pairing.hero_combinations[0].matches, 1);
        assert_eq!(
            pairing.hero_combinations[0].average_net_worth_lead,
            Some(2000.0)
        );
        assert_eq!(pairing.games.len(), 1);
        assert_eq!(pairing.games[0].heroes[0].name, "Hero 1");
        assert_eq!(pairing.games[0].match_result, Some("win"));
        assert_eq!(pairing.games[0].stats.kills, Some(3));
        assert_eq!(pairing.games[0].lane_net_worth_lead, Some(2000));

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
        assert!(sql.contains("AS lane_kills"));
        assert!(sql.contains("AS lane_hero_damage"));
        assert!(sql.contains("hero_icon"));
        assert!(sql.contains("IN (11,22)"));
    }
}
