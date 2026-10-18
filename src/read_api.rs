use std::{collections::HashMap, env, time::Duration};

use axum::{
    Json,
    http::{HeaderMap, StatusCode},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::{ErrorBody, bearer_token, constant_time_eq, non_empty_env};

mod match_context;
mod match_detail;
mod matches;
pub(crate) mod players;
mod sessions;

pub(crate) use match_context::{get_deaths, get_positions};
pub(crate) use match_detail::get_match;
pub(crate) use matches::{list_matches, list_processed_events};
use matches::{load_core, match_selector};
pub(crate) use players::{get_player, get_player_by_name};
pub(crate) use sessions::list_sessions;

const DEFAULT_DATABASE: &str = "singularity";
const DEFAULT_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 100;
const MAX_OFFSET: u32 = 10_000;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const IMPACT_SCORE_VERSION: u32 = 1;

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

#[derive(Debug, Serialize, Clone, Copy, Default)]
struct ImpactScore {
    score: f64,
    combat: f64,
    economy: f64,
    utility: f64,
    survival: f64,
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
            // Return UInt64/Int64 as JSON numbers, not quoted strings.
            .query(&[("output_format_json_quote_64bit_integers", "0")])
            // The SQL goes in the body: ClickHouse rejects a POST without a
            // body (411 Length Required), and long queries stay out of the URL.
            .header("content-type", "text/plain")
            .body(format!("{sql} FORMAT JSONEachRow"));
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
                serde_json::from_str(line)
                    .map(unqualify_columns)
                    .map_err(|error| {
                        warn!(%error, "read API ClickHouse row was invalid JSON");
                    })
            })
            .collect()
    }
}

/// ClickHouse's analyzer names a selected column `m.match_id` when the bare
/// name is ambiguous across joined tables. Rows are read by bare names, so
/// drop the table prefix unless that would overwrite another column.
fn unqualify_columns(row: Value) -> Value {
    let Value::Object(map) = row else {
        return row;
    };
    let mut out = serde_json::Map::with_capacity(map.len());
    let mut qualified = Vec::new();
    for (key, value) in map {
        match key.rsplit_once('.') {
            Some((_, bare)) if !bare.is_empty() => qualified.push((bare.to_string(), key, value)),
            _ => {
                out.insert(key, value);
            }
        }
    }
    for (bare, key, value) in qualified {
        if out.contains_key(&bare) {
            out.insert(key, value);
        } else {
            out.insert(bare, value);
        }
    }
    Value::Object(out)
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

pub(crate) fn authorize(
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

fn mode_name(player_count: usize) -> String {
    if player_count > 0 && player_count.is_multiple_of(2) {
        format!("{}v{}", player_count / 2, player_count / 2)
    } else {
        "unknown".to_string()
    }
}

fn member_for(members: &HashMap<u32, String>, account_id: u32) -> Option<Member> {
    members
        .get(&account_id)
        .map(|name| Member { name: name.clone() })
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

#[derive(Debug, Clone, Copy)]
struct ImpactMetrics {
    slot: u32,
    hero_damage: f64,
    kills: f64,
    deaths: f64,
    assists: f64,
    souls: f64,
    last_hits: f64,
    denies: f64,
    healing: f64,
    damage_mitigated: f64,
}

fn calculate_impact_scores(
    duration_s: u32,
    players: &[ImpactMetrics],
) -> HashMap<u32, ImpactScore> {
    if players.is_empty() {
        return HashMap::new();
    }

    let minutes = f64::from(duration_s.max(60)) / 60.0;
    let rates = players
        .iter()
        .map(|player| {
            [
                player.hero_damage / minutes,
                (player.kills + player.assists) / minutes,
                player.souls / minutes,
                (player.last_hits + player.denies) / minutes,
                player.healing / minutes + player.damage_mitigated / minutes,
                player.deaths / minutes,
            ]
        })
        .collect::<Vec<_>>();
    let z_scores = (0..6)
        .map(|metric| standard_scores(&rates.iter().map(|row| row[metric]).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    players
        .iter()
        .enumerate()
        .map(|(index, player)| {
            let combat = component_score((z_scores[0][index] + z_scores[1][index]) / 2.0);
            let economy = component_score((z_scores[2][index] + z_scores[3][index]) / 2.0);
            let utility = component_score(z_scores[4][index]);
            let survival = component_score(-z_scores[5][index]);
            let score =
                round_score(combat * 0.40 + economy * 0.25 + utility * 0.20 + survival * 0.15);
            (
                player.slot,
                ImpactScore {
                    score,
                    combat,
                    economy,
                    utility,
                    survival,
                },
            )
        })
        .collect()
}

fn standard_scores(values: &[f64]) -> Vec<f64> {
    if values.len() < 2 {
        return vec![0.0; values.len()];
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let deviation = (values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64)
        .sqrt();
    if deviation <= f64::EPSILON {
        return vec![0.0; values.len()];
    }
    values
        .iter()
        .map(|value| ((value - mean) / deviation).clamp(-2.5, 2.5))
        .collect()
}

fn component_score(value: f64) -> f64 {
    round_score(value.clamp(-2.5, 2.5) * 2.0)
}

fn round_score(value: f64) -> f64 {
    (value.clamp(-5.0, 5.0) * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn impact_scores_are_neutral_for_identical_lobby_stats() {
        let baseline = ImpactMetrics {
            slot: 1,
            hero_damage: 1000.0,
            kills: 2.0,
            deaths: 2.0,
            assists: 4.0,
            souls: 12000.0,
            last_hits: 80.0,
            denies: 4.0,
            healing: 100.0,
            damage_mitigated: 200.0,
        };
        let players = [
            baseline,
            ImpactMetrics {
                slot: 2,
                ..baseline
            },
        ];

        let scores = calculate_impact_scores(600, &players);

        assert_eq!(scores[&1].score, 0.0);
        assert_eq!(scores[&2].score, 0.0);
    }

    #[test]
    fn impact_score_rewards_broad_contribution_and_stays_bounded() {
        let baseline = ImpactMetrics {
            slot: 1,
            hero_damage: 1000.0,
            kills: 2.0,
            deaths: 2.0,
            assists: 4.0,
            souls: 12000.0,
            last_hits: 80.0,
            denies: 4.0,
            healing: 100.0,
            damage_mitigated: 200.0,
        };
        let standout = ImpactMetrics {
            slot: 3,
            hero_damage: 6000.0,
            kills: 9.0,
            deaths: 1.0,
            assists: 12.0,
            souls: 25000.0,
            last_hits: 170.0,
            denies: 12.0,
            healing: 1600.0,
            damage_mitigated: 1800.0,
        };

        let scores = calculate_impact_scores(
            1200,
            &[
                baseline,
                ImpactMetrics {
                    slot: 2,
                    ..baseline
                },
                standout,
            ],
        );
        let strong = scores[&3];

        assert!(strong.score > 0.0);
        assert!(scores[&1].score < 0.0);
        for score in scores.values() {
            assert!((-5.0..=5.0).contains(&score.score));
            assert!((-5.0..=5.0).contains(&score.combat));
            assert!((-5.0..=5.0).contains(&score.economy));
            assert!((-5.0..=5.0).contains(&score.utility));
            assert!((-5.0..=5.0).contains(&score.survival));
        }
    }

    #[test]
    fn unqualifies_ambiguous_clickhouse_columns() {
        let row = serde_json::json!({"m.match_id": 7, "start_time": 1, "p.team": 0, "team": 1});
        let row = unqualify_columns(row);

        assert_eq!(row["match_id"], 7);
        assert_eq!(row["start_time"], 1);
        assert_eq!(row["team"], 1);
        assert_eq!(row["p.team"], 0);
    }
}
