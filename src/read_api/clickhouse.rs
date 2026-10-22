use std::time::Duration;

use reqwest::Client;
use serde_json::Value;
use tracing::warn;

use super::unqualify_columns;

const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClickHouseError {
    Unavailable,
    QueryRejected,
    InvalidResponse,
}

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

    pub(super) async fn query(&self, sql: &str) -> Result<Vec<Value>, ClickHouseError> {
        let Some(url) = &self.url else {
            return Err(ClickHouseError::Unavailable);
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
            ClickHouseError::Unavailable
        })?;
        if !response.status().is_success() {
            warn!(status = %response.status(), "read API ClickHouse request returned an error");
            return Err(ClickHouseError::QueryRejected);
        }
        if let Some(bytes) = response
            .content_length()
            .filter(|&bytes| bytes > MAX_RESPONSE_BYTES as u64)
        {
            warn!(bytes, "read API ClickHouse response exceeded the limit");
            return Err(ClickHouseError::InvalidResponse);
        }
        let mut response = response;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            warn!(%error, "read API ClickHouse response could not be read");
            ClickHouseError::Unavailable
        })? {
            if let Err(bytes) = append_bounded_chunk(&mut body, &chunk, MAX_RESPONSE_BYTES) {
                warn!(bytes, "read API ClickHouse response exceeded the limit");
                return Err(ClickHouseError::InvalidResponse);
            }
        }
        let body = String::from_utf8(body).map_err(|error| {
            warn!(%error, "read API ClickHouse response was not UTF-8");
            ClickHouseError::InvalidResponse
        })?;
        body.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .map(unqualify_columns)
                    .map_err(|error| {
                        warn!(%error, "read API ClickHouse row was invalid JSON");
                        ClickHouseError::InvalidResponse
                    })
            })
            .collect()
    }
}

fn append_bounded_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> Result<(), usize> {
    let bytes = body.len().saturating_add(chunk.len());
    if bytes > max_bytes {
        return Err(bytes);
    }
    body.extend_from_slice(chunk);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::append_bounded_chunk;

    #[test]
    fn response_body_limit_rejects_before_appending_overflow() {
        let mut body = Vec::new();

        assert_eq!(append_bounded_chunk(&mut body, b"123", 3), Ok(()));
        assert_eq!(append_bounded_chunk(&mut body, b"4", 3), Err(4));
        assert_eq!(&body, b"123");
    }
}
