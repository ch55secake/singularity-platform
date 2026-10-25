use std::time::Duration;

use reqwest::Client;
use serde::Serialize;
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

    pub(super) async fn insert<T: Serialize>(
        &self,
        sql: &str,
        rows: &[T],
    ) -> Result<(), ClickHouseError> {
        let Some(url) = &self.url else {
            return Err(ClickHouseError::Unavailable);
        };
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut body, row).map_err(|error| {
                warn!(%error, "read API ClickHouse insert row could not be serialized");
                ClickHouseError::InvalidResponse
            })?;
            body.push(b'\n');
        }
        let mut request = self
            .client
            .post(url)
            .query(&[
                ("query", sql),
                ("output_format_json_quote_64bit_integers", "0"),
            ])
            .header("content-type", "application/json")
            .body(body);
        if let Some(user) = &self.user {
            request = request.basic_auth(user, self.password.as_deref());
        }
        let response = request.send().await.map_err(|error| {
            warn!(%error, "read API ClickHouse insert request failed");
            ClickHouseError::Unavailable
        })?;
        if !response.status().is_success() {
            warn!(status = %response.status(), "read API ClickHouse insert was rejected");
            return Err(ClickHouseError::QueryRejected);
        }
        Ok(())
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
    use std::time::Duration;

    use serde::Serialize;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{ClickHouseClient, append_bounded_chunk};

    #[test]
    fn response_body_limit_rejects_before_appending_overflow() {
        let mut body = Vec::new();

        assert_eq!(append_bounded_chunk(&mut body, b"123", 3), Ok(()));
        assert_eq!(append_bounded_chunk(&mut body, b"4", 3), Err(4));
        assert_eq!(&body, b"123");
    }

    #[tokio::test]
    async fn insert_sends_sql_and_json_each_row_body() {
        #[derive(Serialize)]
        struct Row {
            match_id: u64,
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener binds");
        let address = listener.local_addr().expect("listener address reads");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("request accepts");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            loop {
                let count = stream.read(&mut chunk).await.expect("request reads");
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..count]);
                let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let body_len = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|length| length.trim().parse::<usize>().ok())
                    })
                    .expect("content length exists");
                if request.len() >= header_end + 4 + body_len {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&request);
            assert!(request.contains("replay_reparse_requests"));
            assert!(request.contains("{\"match_id\":123}"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("response writes");
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("HTTP client builds");
        let clickhouse = ClickHouseClient {
            client,
            url: Some(format!("http://{address}")),
            user: None,
            password: None,
        };

        clickhouse
            .insert(
                "INSERT INTO singularity.replay_reparse_requests (match_id) FORMAT JSONEachRow",
                &[Row { match_id: 123 }],
            )
            .await
            .expect("insert succeeds");
        server.await.expect("server completes");
    }
}
