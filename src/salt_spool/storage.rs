use std::{collections::HashSet, path::Path};

use axum::{Json, http::StatusCode};
use tokio::io::AsyncWriteExt;
use tracing::warn;

use crate::{AppState, ErrorBody, internal_error};

use super::{SaltKey, SaltRecord, valid_record};

/// Appends new salt records to the spool. Returns (accepted, duplicates).
pub(crate) async fn store_records(
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

pub(crate) async fn load_seen(path: &Path) -> Result<HashSet<SaltKey>, std::io::Error> {
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
