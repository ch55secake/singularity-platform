#![forbid(unsafe_code)]
#![deny(clippy::all)]

mod assets;
mod auth;
mod commands;
mod credentials;
mod discord;
mod discord_format;
mod discord_live;
mod ingest;
mod live;
mod metadata;
mod parser;
mod rate_limit;
mod read_api;
mod replay;
mod salt_spool;
mod server;
mod server_config;
mod server_state;

use commands::{Command, parse_command};
use salt_spool::SaltKey;
use std::{
    collections::HashSet,
    env,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{Json, http::StatusCode};
use serde::Serialize;
use subtle::ConstantTimeEq;
use tokio::{net::TcpListener, sync::Mutex};
use tracing::info;

const DEFAULT_BIND: &str = "0.0.0.0:8080";
const DEFAULT_SPOOL: &str = "data/salts.jsonl";
const DEFAULT_CREDENTIALS: &str = "data/collectors.json";
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

    let server_state::ServerState {
        app_state,
        spool_path,
        credential_count,
        recovered_records,
        rate_limit,
    } = server_state::load().await?;
    tokio::spawn(live::fetch_salts(app_state.clone()));
    let app = server::router(app_state);

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

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    bool::from(left.ct_eq(right))
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn allow_request(state: &AppState, collector_id: &str) -> bool {
    let mut windows = state.rate_windows.lock().await;
    windows.consume(collector_id, state.rate_limit)
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
    use axum::{
        extract::State,
        http::{HeaderMap, header::AUTHORIZATION},
    };
    use salt_spool::SaltRecord;
    use tokio::fs::{self, OpenOptions};

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
            max_batch_size: server_config::MAX_BATCH_SIZE,
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
        let first = ingest::ingest_salts(
            State(state.clone()),
            headers.clone(),
            Json(vec![submitted_record.clone()]),
        )
        .await
        .expect("first request succeeds");
        let second = ingest::ingest_salts(State(state), headers, Json(vec![submitted_record]))
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
