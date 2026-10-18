use std::env;

use reqwest::{Client, StatusCode};
use serde::Deserialize;

use crate::salt_spool::SaltRecord;

const DEFAULT_DEADLOCK_API_URL: &str = "https://api.deadlock-api.com";
const SALTS_COLLECTOR_ID: &str = "community-api";
/// Salts that the community API must fetch from Steam are limited to 10 per
/// 30 minutes per IP, so requests are spaced out across all matches.
pub(super) const MIN_REQUEST_GAP_SECONDS: u64 = 180;

#[derive(Debug, Deserialize)]
struct SaltsResponse {
    match_id: u64,
    #[serde(default)]
    cluster_id: Option<u32>,
    #[serde(default)]
    metadata_salt: Option<u32>,
    #[serde(default)]
    replay_salt: Option<u32>,
}

pub(super) enum RequestError {
    /// The API asked us to wait this many seconds.
    Busy(u64),
    Other(String),
}

pub(super) fn configured_base_url() -> String {
    env::var("SINGULARITY_DEADLOCK_API_URL")
        .unwrap_or_else(|_| DEFAULT_DEADLOCK_API_URL.to_string())
        .trim_end_matches('/')
        .to_string()
}

pub(super) async fn request_salts(
    http: &Client,
    base_url: &str,
    match_id: u64,
) -> Result<Option<SaltRecord>, RequestError> {
    let response = http
        .get(format!("{base_url}/v1/matches/{match_id}/salts"))
        .send()
        .await
        .map_err(|error| RequestError::Other(error.to_string()))?;
    let status = response.status();
    if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
        let wait = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(MIN_REQUEST_GAP_SECONDS);
        return Err(RequestError::Busy(wait));
    }
    if status == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !status.is_success() {
        return Err(RequestError::Other(format!("HTTP {status}")));
    }
    let body = response
        .bytes()
        .await
        .map_err(|error| RequestError::Other(error.to_string()))?;
    let salts = serde_json::from_slice::<SaltsResponse>(&body)
        .map_err(|error| RequestError::Other(error.to_string()))?;
    Ok(salts_record(salts, match_id))
}

fn salts_record(salts: SaltsResponse, match_id: u64) -> Option<SaltRecord> {
    if salts.match_id != match_id || salts.metadata_salt.is_none() {
        return None;
    }
    Some(SaltRecord {
        match_id,
        cluster_id: salts.cluster_id,
        metadata_salt: salts.metadata_salt,
        replay_salt: salts.replay_salt,
        username: None,
        collector_id: Some(SALTS_COLLECTOR_ID.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_complete_salts_become_records() {
        let salts = |match_id, metadata_salt| SaltsResponse {
            match_id,
            cluster_id: Some(390),
            metadata_salt,
            replay_salt: Some(9),
        };
        let record = salts_record(salts(5, Some(7)), 5).unwrap();
        assert_eq!(record.metadata_salt, Some(7));
        assert_eq!(record.collector_id.as_deref(), Some(SALTS_COLLECTOR_ID));
        assert!(salts_record(salts(5, None), 5).is_none());
        assert!(salts_record(salts(6, Some(7)), 5).is_none());
    }
}
