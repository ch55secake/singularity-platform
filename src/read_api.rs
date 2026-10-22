use std::{collections::HashMap, env, time::Duration};

use axum::{
    Json,
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ErrorBody, auth::bearer_token, constant_time_eq, server_config::non_empty_env};

mod clickhouse;
mod impact;
mod lane_stats;
mod match_context;
mod match_detail;
mod matches;
pub(crate) mod players;
mod sessions;

use clickhouse::ClickHouseClient;
use impact::{
    IMPACT_SCORE_VERSION, ImpactMetrics, ImpactScore, calculate_impact_scores, round_score,
};
pub(crate) use lane_stats::get_lane_stats;
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

#[derive(Clone)]
pub(crate) struct ReadApiState {
    clickhouse: ClickHouseClient,
    database: String,
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
        let clickhouse = ClickHouseClient::new(Duration::from_secs(timeout))?;
        Ok(Self {
            clickhouse,
            database: env::var("SINGULARITY_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| DEFAULT_DATABASE.to_string()),
            token: non_empty_env("SINGULARITY_READ_TOKEN"),
            members,
        })
    }

    async fn query(&self, sql: &str) -> Result<Vec<Value>, ()> {
        self.clickhouse.query(sql).await
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

#[cfg(test)]
mod tests {
    use super::*;

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
