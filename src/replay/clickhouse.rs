use std::env;
use std::time::Duration;

use reqwest::Client;
use serde::Serialize;
use serde_json::Value;

pub(super) struct ClickHouseClient {
    pub(super) http: Client,
    pub(super) url: String,
    pub(super) database: String,
    pub(super) user: Option<String>,
    pub(super) password: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ReplayMatchRow {
    pub(super) match_id: u64,
    pub(super) replay_job_id: String,
    pub(super) parser_version: String,
    pub(super) parsed_at: u64,
    pub(super) position_count: u32,
    pub(super) model_version: u64,
}

pub(super) const REPLAY_MATCH_COLUMNS: &str =
    "match_id, replay_job_id, parser_version, parsed_at, position_count, model_version";
pub(super) const REPLAY_POSITION_COLUMNS: &str =
    "match_id, entity_index, tick, time_s, x, y, z, parser_version, model_version";
pub(super) const REPLAY_RESOURCE_COLUMNS: &str = "match_id, entity_index, event_index, tick, time_s, resource_kind, active, x, y, z, parser_version, model_version";
pub(super) const REPLAY_REPARSE_REQUEST_COLUMNS: &str =
    "match_id, request_id, requested_at, status, error_code, model_version";

impl ClickHouseClient {
    pub(super) fn from_env(timeout: &Duration) -> Result<Self, std::io::Error> {
        let url = env::var("SINGULARITY_CLICKHOUSE_URL").map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SINGULARITY_CLICKHOUSE_URL is required for replay-worker",
            )
        })?;
        let http = Client::builder()
            .timeout(*timeout)
            .build()
            .map_err(std::io::Error::other)?;
        Ok(Self {
            http,
            url,
            database: env::var("SINGULARITY_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| "singularity".to_string()),
            user: env::var("SINGULARITY_CLICKHOUSE_USER").ok(),
            password: env::var("SINGULARITY_CLICKHOUSE_PASSWORD").ok(),
        })
    }

    pub(super) async fn insert<T: Serialize>(
        &self,
        table: &str,
        columns: &str,
        rows: &[T],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut body, row)?;
            body.push(b'\n');
        }
        let query = format!(
            "INSERT INTO {}.{} ({}) FORMAT JSONEachRow",
            self.database, table, columns
        );
        let mut request = self
            .http
            .post(&self.url)
            .query(&[("query", query)])
            .header("content-type", "application/json")
            .body(body);
        if let Some(user) = &self.user {
            request = request.basic_auth(user, self.password.as_deref());
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(std::io::Error::other("ClickHouse insert failed").into());
        }
        Ok(())
    }

    pub(super) async fn query_rows(
        &self,
        sql: &str,
    ) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        let mut request = self
            .http
            .post(&self.url)
            .query(&[("output_format_json_quote_64bit_integers", "0")])
            .header("content-type", "text/plain")
            .body(format!("{sql} FORMAT JSONEachRow"));
        if let Some(user) = &self.user {
            request = request.basic_auth(user, self.password.as_deref());
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(std::io::Error::other("ClickHouse query failed").into());
        }
        let body = response.text().await?;
        body.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    }
}
