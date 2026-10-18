use std::{collections::HashSet, path::Path};

use axum::{Json, http::StatusCode};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::warn;

use crate::{AppState, ErrorBody, internal_error};

pub(super) const MAX_MATCH_ID: u64 = 10_000_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct SaltRecord {
    pub(super) match_id: u64,
    pub(super) cluster_id: Option<u32>,
    pub(super) metadata_salt: Option<u32>,
    pub(super) replay_salt: Option<u32>,
    pub(super) username: Option<String>,
    #[serde(default)]
    pub(super) collector_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SaltKey {
    pub(super) match_id: u64,
    pub(super) cluster_id: Option<u32>,
    pub(super) metadata_salt: Option<u32>,
    pub(super) replay_salt: Option<u32>,
}

impl From<&SaltRecord> for SaltKey {
    fn from(record: &SaltRecord) -> Self {
        Self {
            match_id: record.match_id,
            cluster_id: record.cluster_id,
            metadata_salt: record.metadata_salt,
            replay_salt: record.replay_salt,
        }
    }
}

/// Appends new salt records to the spool. Returns (accepted, duplicates).
pub(super) async fn store_records(
    state: &AppState,
    records: Vec<SaltRecord>,
) -> Result<(usize, usize), (StatusCode, Json<ErrorBody>)> {
    let mut seen = state.seen.lock().await;
    let mut spool = state.spool.lock().await;
    let mut accepted = 0;
    let mut duplicates = 0;
    for record in records {
        let key = SaltKey::from(&record);
        if seen.contains(&key) {
            duplicates += 1;
            continue;
        }
        let line = serde_json::to_string(&record).map_err(|_| internal_error())?;
        spool
            .write_all(line.as_bytes())
            .await
            .map_err(|_| internal_error())?;
        spool.write_all(b"\n").await.map_err(|_| internal_error())?;
        seen.insert(key);
        accepted += 1;
    }
    spool.flush().await.map_err(|_| internal_error())?;
    spool.sync_data().await.map_err(|_| internal_error())?;
    Ok((accepted, duplicates))
}

pub(super) async fn load_seen(path: &Path) -> Result<HashSet<SaltKey>, std::io::Error> {
    let contents = match tokio::fs::read_to_string(path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error),
    };
    Ok(recover_seen(&contents))
}

fn recover_seen(contents: &str) -> HashSet<SaltKey> {
    let mut seen = HashSet::new();
    let mut lines = contents.split('\n').peekable();
    let mut line_number = 0;

    while let Some(line) = lines.next() {
        line_number += 1;
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<SaltRecord>(line) {
            Ok(record) if valid_record(&record) => {
                seen.insert(SaltKey::from(&record));
            }
            Ok(_) => {
                warn!(line = line_number, "ignoring invalid record in salt spool");
            }
            Err(error) if lines.peek().is_none() => {
                warn!(
                    line = line_number,
                    %error,
                    "ignoring incomplete final line in salt spool"
                );
            }
            Err(error) => {
                warn!(line = line_number, %error, "ignoring malformed record in salt spool");
            }
        }
    }

    seen
}

pub(super) fn valid_record(record: &SaltRecord) -> bool {
    record.match_id > 0
        && record.match_id <= MAX_MATCH_ID
        && (record.metadata_salt.is_some() || record.replay_salt.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> SaltRecord {
        SaltRecord {
            match_id: 42,
            cluster_id: Some(1),
            metadata_salt: Some(2),
            replay_salt: None,
            username: None,
            collector_id: None,
        }
    }

    #[test]
    fn accepts_metadata_or_replay_salts() {
        assert!(valid_record(&sample_record()));
        assert!(valid_record(&SaltRecord {
            match_id: 42,
            cluster_id: Some(1),
            metadata_salt: None,
            replay_salt: Some(3),
            username: None,
            collector_id: None,
        }));
    }

    #[test]
    fn rejects_records_without_salts() {
        assert!(!valid_record(&SaltRecord {
            match_id: 42,
            cluster_id: Some(1),
            metadata_salt: None,
            replay_salt: None,
            username: None,
            collector_id: None,
        }));
    }

    #[test]
    fn recovers_records_from_spool_and_ignores_truncated_final_line() {
        let record = sample_record();
        let complete = serde_json::to_string(&record).expect("sample record serializes");
        let contents = format!("{complete}\n{{\"match_id\":42");

        let seen = recover_seen(&contents);

        assert_eq!(seen.len(), 1);
        assert!(seen.contains(&SaltKey::from(&record)));
    }

    #[test]
    fn recovers_only_valid_records() {
        let record = sample_record();
        let complete = serde_json::to_string(&record).expect("sample record serializes");
        let contents = format!("{complete}\n{{\"match_id\":0,\"metadata_salt\":null}}\n");

        let seen = recover_seen(&contents);

        assert_eq!(seen.len(), 1);
        assert!(seen.contains(&SaltKey::from(&record)));
    }
}
