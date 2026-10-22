use std::time::Duration;

use reqwest::Client;
use serde_json::Value;
use tracing::warn;

use super::unqualify_columns;

const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone)]
pub(super) struct ClickHouseClient {
    client: Client,
    url: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl ClickHouseClient {
    pub(super) fn new(timeout: Duration) -> Result<Self, std::io::Error> {
        let client = Client::builder()
            .timeout(timeout)
            .user_agent("singularity-platform-read-api/0.1")
            .build()
            .map_err(std::io::Error::other)?;
        Ok(Self {
            client,
            url: super::non_empty_env("SINGULARITY_CLICKHOUSE_URL"),
            user: super::non_empty_env("SINGULARITY_CLICKHOUSE_USER"),
            password: super::non_empty_env("SINGULARITY_CLICKHOUSE_PASSWORD"),
        })
    }

    pub(super) async fn query(&self, sql: &str) -> Result<Vec<Value>, ()> {
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
