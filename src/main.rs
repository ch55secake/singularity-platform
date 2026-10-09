#![forbid(unsafe_code)]
#![deny(clippy::all)]

use std::{
    collections::HashSet,
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    net::TcpListener,
    sync::Mutex,
};
use tracing::{info, warn};

const DEFAULT_BIND: &str = "0.0.0.0:8080";
const DEFAULT_SPOOL: &str = "data/salts.jsonl";
const MAX_BATCH_SIZE: usize = 1_000;
const MAX_MATCH_ID: u64 = 10_000_000_000;

#[derive(Clone)]
struct AppState {
    token: Option<String>,
    spool: Arc<Mutex<tokio::fs::File>>,
    seen: Arc<Mutex<HashSet<SaltKey>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct SaltRecord {
    match_id: u64,
    cluster_id: Option<u32>,
    metadata_salt: Option<u32>,
    replay_salt: Option<u32>,
    username: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SaltKey {
    match_id: u64,
    cluster_id: Option<u32>,
    metadata_salt: Option<u32>,
    replay_salt: Option<u32>,
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

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: &'static str,
}

#[derive(Debug, Serialize)]
struct IngestBody {
    accepted: usize,
    duplicates: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "singularity_platform=info".into()),
        )
        .init();

    let spool_path = PathBuf::from(
        env::var("SINGULARITY_SPOOL_PATH").unwrap_or_else(|_| DEFAULT_SPOOL.to_string()),
    );
    if let Some(parent) = spool_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let seen = load_seen(&spool_path).await?;
    let recovered_records = seen.len();
    let spool = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&spool_path)
        .await?;

    let token = env::var("SINGULARITY_INGEST_TOKEN").ok();
    if token.is_none() {
        warn!("SINGULARITY_INGEST_TOKEN is unset; ingestion authentication is disabled");
    }

    let state = AppState {
        token,
        spool: Arc::new(Mutex::new(spool)),
        seen: Arc::new(Mutex::new(seen)),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/ingest/salts", post(ingest_salts))
        .with_state(state);

    let bind = env::var("SINGULARITY_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string());
    let address: SocketAddr = bind.parse()?;
    let listener = TcpListener::bind(address).await?;
    info!(
        %address,
        spool_path = %spool_path.display(),
        records = recovered_records,
        "Singularity platform listening"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "service": "singularity-platform",
        "status": "ok",
        "baseline": "v0.1.0",
    }))
}

async fn ingest_salts(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(records): Json<Vec<SaltRecord>>,
) -> Result<Json<IngestBody>, (StatusCode, Json<ErrorBody>)> {
    if !authorized(&headers, state.token.as_deref()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorBody {
                error: "unauthorized",
            }),
        ));
    }
    if records.is_empty() || records.len() > MAX_BATCH_SIZE {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: "batch must contain between 1 and 1000 records",
            }),
        ));
    }

    for record in &records {
        if !valid_record(record) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: "invalid match salt record",
                }),
            ));
        }
    }

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

    Ok(Json(IngestBody {
        accepted,
        duplicates,
    }))
}

async fn load_seen(path: &Path) -> Result<HashSet<SaltKey>, std::io::Error> {
    let contents = match fs::read_to_string(path).await {
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

fn authorized(headers: &HeaderMap, expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    value == format!("Bearer {expected}")
}

fn valid_record(record: &SaltRecord) -> bool {
    record.match_id > 0
        && record.match_id <= MAX_MATCH_ID
        && (record.metadata_salt.is_some() || record.replay_salt.is_some())
}

fn internal_error() -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: "storage failure",
        }),
    )
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
