#![forbid(unsafe_code)]
#![deny(clippy::all)]

mod assets;
mod commands;
mod credentials;
mod discord;
mod discord_format;
mod discord_live;
mod live;
mod metadata;
mod parser;
mod rate_limit;
mod read_api;
mod replay;
mod salt_spool;

use commands::{Command, parse_command};
use salt_spool::{SaltKey, SaltRecord};

use std::{
    collections::HashSet,
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    routing::{get, post},
};
use serde::Serialize;
use subtle::ConstantTimeEq;
use tokio::{
    fs::{self, OpenOptions},
    net::TcpListener,
    sync::Mutex,
};
use tracing::{info, warn};

const DEFAULT_BIND: &str = "0.0.0.0:8080";
const DEFAULT_SPOOL: &str = "data/salts.jsonl";
const DEFAULT_CREDENTIALS: &str = "data/collectors.json";
const MAX_BATCH_SIZE: usize = 1_000;
const MAX_REQUEST_BODY_BYTES: usize = 512 * 1024;
const LEGACY_COLLECTOR_ID: &str = "legacy-global-token";
const ANONYMOUS_COLLECTOR_ID: &str = "anonymous-development";

#[derive(Clone)]
struct AppState {
    legacy_token: Option<String>,
    credentials: Arc<Mutex<credentials::CredentialStore>>,
    spool: Arc<Mutex<tokio::fs::File>>,
    seen: Arc<Mutex<HashSet<SaltKey>>>,
    rate_windows: Arc<Mutex<rate_limit::Windows>>,
    rate_limit: rate_limit::RateLimit,
    max_batch_size: usize,
    authentication_required: bool,
    read_api: read_api::ReadApiState,
    live: Arc<Mutex<live::LiveStore>>,
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
    // Before subcommands too, so the workers and the Discord bot log.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "singularity_platform=info".into()),
        )
        .init();

    let command = parse_command(env::args().skip(1))
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    if let Some(command) = command {
        return match command {
            Command::Credentials(command) => credentials::run_command(command).await,
            Command::MetadataWorker => metadata::run().await,
            Command::MetadataParser => parser::run().await,
            Command::ReplayWorker => replay::run().await,
            Command::AssetsSync => assets::run().await,
            Command::DiscordBot => discord::run().await,
        };
    }

    let spool_path = PathBuf::from(
        env::var("SINGULARITY_SPOOL_PATH").unwrap_or_else(|_| DEFAULT_SPOOL.to_string()),
    );
    if let Some(parent) = spool_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let seen = salt_spool::load_seen(&spool_path).await?;
    let recovered_records = seen.len();
    let spool = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&spool_path)
        .await?;

    let credentials_path = PathBuf::from(
        env::var("SINGULARITY_CREDENTIALS_PATH")
            .unwrap_or_else(|_| DEFAULT_CREDENTIALS.to_string()),
    );
    let credential_store = credentials::CredentialStore::load(&credentials_path).await?;
    let credential_count = credential_store.len();
    let legacy_token = non_empty_env("SINGULARITY_INGEST_TOKEN");
    let authentication_required = legacy_token.is_some() || credential_count > 0;
    if !authentication_required {
        warn!("no ingestion credentials configured; authentication is disabled for development");
    }
    let max_batch_size = configured_batch_size()?;
    let rate_limit = rate_limit::configured_rate_limit()?;
    let read_api = read_api::ReadApiState::from_env()?;
    let live_path = live::store_path(&spool_path);
    let live_store = live::LiveStore::load(&live_path).await?;

    let state = AppState {
        legacy_token,
        credentials: Arc::new(Mutex::new(credential_store)),
        spool: Arc::new(Mutex::new(spool)),
        seen: Arc::new(Mutex::new(seen)),
        rate_windows: Arc::new(Mutex::new(rate_limit::Windows::default())),
        rate_limit,
        max_batch_size,
        authentication_required,
        read_api,
        live: Arc::new(Mutex::new(live_store)),
    };
    tokio::spawn(live::fetch_salts(state.clone()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/ingest/salts", post(ingest_salts))
        .route("/ingest/live", post(live::report))
        // Not under /api: the web app proxies /api to browsers, and this list
        // carries Steam account IDs for the Discord bot.
        .route("/live", get(live::list))
        .route("/matches", get(read_api::list_matches))
        .route("/sessions", get(read_api::list_sessions))
        .route("/events/matches", get(read_api::list_processed_events))
        .route("/matches/{identifier}", get(read_api::get_match))
        .route(
            "/matches/{identifier}/positions",
            get(read_api::get_positions),
        )
        .route("/matches/{identifier}/deaths", get(read_api::get_deaths))
        .route("/api/matches", get(read_api::list_matches))
        .route("/api/sessions", get(read_api::list_sessions))
        .route("/api/events/matches", get(read_api::list_processed_events))
        .route("/api/matches/{identifier}", get(read_api::get_match))
        .route(
            "/api/matches/{identifier}/positions",
            get(read_api::get_positions),
        )
        .route(
            "/api/matches/{identifier}/deaths",
            get(read_api::get_deaths),
        )
        .route("/players/{account_id}", get(read_api::get_player))
        .route("/api/players/{account_id}", get(read_api::get_player))
        .route("/players/by-name/{name}", get(read_api::get_player_by_name))
        .route(
            "/api/players/by-name/{name}",
            get(read_api::get_player_by_name),
        )
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state);

    let bind = env::var("SINGULARITY_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string());
    let address: SocketAddr = bind.parse()?;
    let listener = TcpListener::bind(address).await?;
    info!(
        %address,
        spool_path = %spool_path.display(),
        credentials = credential_count,
        requests_per_window = rate_limit.max_requests,
        rate_window_seconds = rate_limit.window.as_secs(),
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
    Json(mut records): Json<Vec<SaltRecord>>,
) -> Result<Json<IngestBody>, (StatusCode, Json<ErrorBody>)> {
    let Some(collector_id) = authenticate(&headers, &state).await else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorBody {
                error: "unauthorized",
            }),
        ));
    };
    if !allow_request(&state, &collector_id).await {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorBody {
                error: "collector request rate limit exceeded",
            }),
        ));
    }
    if records.is_empty() || records.len() > state.max_batch_size {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: "batch exceeds the configured collector limit",
            }),
        ));
    }

    for record in &records {
        if !salt_spool::valid_record(record) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: "invalid match salt record",
                }),
            ));
        }
    }

    for record in &mut records {
        record.collector_id = Some(collector_id.clone());
    }

    let (accepted, duplicates) = salt_spool::store_records(&state, records).await?;
    Ok(Json(IngestBody {
        accepted,
        duplicates,
    }))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    bool::from(left.ct_eq(right))
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn authenticate(headers: &HeaderMap, state: &AppState) -> Option<String> {
    if !state.authentication_required {
        return Some(ANONYMOUS_COLLECTOR_ID.to_string());
    }
    let token = bearer_token(headers)?;
    if let Some(expected) = state.legacy_token.as_deref()
        && constant_time_eq(token.as_bytes(), expected.as_bytes())
    {
        return Some(LEGACY_COLLECTOR_ID.to_string());
    }
    state.credentials.lock().await.authenticate(token)
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let mut parts = value.split_ascii_whitespace();
    let scheme = parts.next()?;
    let token = parts.next()?;
    if parts.next().is_some() || !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() {
        return None;
    }
    Some(token)
}

async fn allow_request(state: &AppState, collector_id: &str) -> bool {
    let mut windows = state.rate_windows.lock().await;
    windows.consume(collector_id, state.rate_limit)
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn configured_batch_size() -> Result<usize, std::io::Error> {
    let value = env::var("SINGULARITY_COLLECTOR_MAX_BATCH_SIZE")
        .unwrap_or_else(|_| MAX_BATCH_SIZE.to_string())
        .parse::<usize>()
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid batch limit")
        })?;
    if value == 0 || value > MAX_BATCH_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "batch limit must be between 1 and 1000",
        ));
    }
    Ok(value)
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
            collector_id: None,
        }
    }

    #[test]
    fn malformed_authorization_is_rejected_without_parsing_a_token() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Basic not-a-bearer-token".parse().unwrap());

        assert_eq!(bearer_token(&headers), None);
    }

    #[tokio::test]
    async fn authenticated_replays_are_deduplicated_and_attributed() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let spool_path = env::temp_dir().join(format!(
            "singularity-platform-test-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock is after epoch")
                .as_nanos()
        ));
        let spool = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&spool_path)
            .await
            .expect("test spool opens");
        let credential_path = spool_path.with_extension("credentials.json");
        let mut credential_store = credentials::CredentialStore::load(&credential_path)
            .await
            .expect("credential store loads");
        let token = credential_store
            .create("collector-a")
            .expect("credential creates");
        let state = AppState {
            legacy_token: None,
            credentials: Arc::new(Mutex::new(credential_store)),
            spool: Arc::new(Mutex::new(spool)),
            seen: Arc::new(Mutex::new(HashSet::new())),
            rate_windows: Arc::new(Mutex::new(rate_limit::Windows::default())),
            rate_limit: rate_limit::RateLimit {
                max_requests: 10,
                window: std::time::Duration::from_secs(60),
            },
            max_batch_size: MAX_BATCH_SIZE,
            authentication_required: true,
            read_api: read_api::ReadApiState::from_env().expect("read API state initializes"),
            live: Arc::new(Mutex::new(live::LiveStore::empty(
                spool_path.with_extension("live.jsonl"),
            ))),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {token}")
                .parse()
                .expect("authorization header"),
        );

        let mut submitted_record = sample_record();
        submitted_record.collector_id = Some("spoofed-collector".to_string());
        let first = ingest_salts(
            State(state.clone()),
            headers.clone(),
            Json(vec![submitted_record.clone()]),
        )
        .await
        .expect("first request succeeds");
        let second = ingest_salts(State(state), headers, Json(vec![submitted_record]))
            .await
            .expect("replayed request succeeds");

        assert_eq!(first.0.accepted, 1);
        assert_eq!(second.0.duplicates, 1);
        let contents = fs::read_to_string(&spool_path)
            .await
            .expect("test spool reads");
        assert!(contents.contains("\"collector_id\":\"collector-a\""));
        fs::remove_file(spool_path)
            .await
            .expect("test spool removes");
    }
}
