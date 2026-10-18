use std::{env, time::Duration};

use reqwest::Client;
use serde::Serialize;

use super::{DEFAULT_CLICKHOUSE_DATABASE, DEFAULT_TIMEOUT_SECONDS};

struct ClickHouseConfig {
    url: String,
    database: String,
    user: Option<String>,
    password: Option<String>,
    timeout: Duration,
}

pub(super) struct ClickHouseClient {
    http: Client,
    config: ClickHouseConfig,
}

impl ClickHouseClient {
    pub(super) fn from_env() -> Result<Self, std::io::Error> {
        let url = env::var("SINGULARITY_CLICKHOUSE_URL").map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SINGULARITY_CLICKHOUSE_URL is required for metadata-parser",
            )
        })?;
        let timeout = env::var("SINGULARITY_METADATA_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| DEFAULT_TIMEOUT_SECONDS.to_string())
            .parse::<u64>()
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid parser timeout")
            })?;
        if timeout == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "parser timeout must be greater than zero",
            ));
        }
        let config = ClickHouseConfig {
            url,
            database: env::var("SINGULARITY_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| DEFAULT_CLICKHOUSE_DATABASE.to_string()),
            user: env::var("SINGULARITY_CLICKHOUSE_USER").ok(),
            password: env::var("SINGULARITY_CLICKHOUSE_PASSWORD").ok(),
            timeout: Duration::from_secs(timeout),
        };
        let http = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "invalid ClickHouse client",
                )
            })?;
        Ok(Self { http, config })
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
            self.config.database, table, columns
        );
        let mut request = self
            .http
            .post(&self.config.url)
            .query(&[("query", query)])
            .header("content-type", "application/json")
            .body(body);
        if let Some(user) = &self.config.user {
            request = request.basic_auth(user, self.config.password.as_deref());
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(std::io::Error::other("ClickHouse insert failed").into());
        }
        Ok(())
    }

    pub(super) async fn match_is_processed(
        &self,
        match_id: u64,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let query = format!(
            "SELECT count() FROM {}.match_processed_events FINAL WHERE match_id = {}",
            self.config.database, match_id
        );
        let mut request = self.http.post(&self.config.url).body(query);
        if let Some(user) = &self.config.user {
            request = request.basic_auth(user, self.config.password.as_deref());
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(std::io::Error::other("ClickHouse duplicate check failed").into());
        }
        let count = response.text().await?.trim().parse::<u64>()?;
        Ok(count > 0)
    }
}
