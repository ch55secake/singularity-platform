#![forbid(unsafe_code)]
#![deny(clippy::all)]

mod metadata;
mod parser;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    routing::{get, post},
};
use getrandom::fill as fill_random;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    net::TcpListener,
    sync::Mutex,
};
use tracing::{info, warn};

const DEFAULT_BIND: &str = "0.0.0.0:8080";
const DEFAULT_SPOOL: &str = "data/salts.jsonl";
const DEFAULT_CREDENTIALS: &str = "data/collectors.json";
const MAX_BATCH_SIZE: usize = 1_000;
const MAX_REQUEST_BODY_BYTES: usize = 512 * 1024;
const MAX_MATCH_ID: u64 = 10_000_000_000;
const DEFAULT_RATE_LIMIT: u32 = 60;
const DEFAULT_RATE_WINDOW_SECONDS: u64 = 60;
const LEGACY_COLLECTOR_ID: &str = "legacy-global-token";
const ANONYMOUS_COLLECTOR_ID: &str = "anonymous-development";

#[derive(Clone)]
struct AppState {
    legacy_token: Option<String>,
    credentials: Arc<Mutex<CredentialStore>>,
    spool: Arc<Mutex<tokio::fs::File>>,
    seen: Arc<Mutex<HashSet<SaltKey>>>,
    rate_windows: Arc<Mutex<HashMap<String, RateWindow>>>,
    rate_limit: RateLimit,
    max_batch_size: usize,
    authentication_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct SaltRecord {
    match_id: u64,
    cluster_id: Option<u32>,
    metadata_salt: Option<u32>,
    replay_salt: Option<u32>,
    username: Option<String>,
    #[serde(default)]
    collector_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CredentialRecord {
    token_hash: String,
    created_at: u64,
    #[serde(default)]
    rotated_at: Option<u64>,
    #[serde(default)]
    revoked_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CredentialFile {
    version: u32,
    #[serde(default)]
    credentials: BTreeMap<String, CredentialRecord>,
}

struct CredentialStore {
    path: PathBuf,
    records: BTreeMap<String, CredentialRecord>,
}

#[derive(Debug, Clone, Copy)]
struct RateLimit {
    max_requests: u32,
    window: Duration,
}

#[derive(Debug)]
struct RateWindow {
    started_at: Instant,
    requests: u32,
}

enum CredentialCommand {
    Create(String),
    Rotate(String),
    Revoke(String),
    List,
}

enum Command {
    Credentials(CredentialCommand),
    MetadataWorker,
    MetadataParser,
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
    let command = parse_command(env::args().skip(1))
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    if let Some(command) = command {
        return match command {
            Command::Credentials(command) => run_credential_command(command).await,
            Command::MetadataWorker => metadata::run().await,
            Command::MetadataParser => parser::run().await,
        };
    }

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

    let credentials_path = PathBuf::from(
        env::var("SINGULARITY_CREDENTIALS_PATH")
            .unwrap_or_else(|_| DEFAULT_CREDENTIALS.to_string()),
    );
    let credential_store = CredentialStore::load(&credentials_path).await?;
    let credential_count = credential_store.records.len();
    let legacy_token = non_empty_env("SINGULARITY_INGEST_TOKEN");
    let authentication_required = legacy_token.is_some() || credential_count > 0;
    if !authentication_required {
        warn!("no ingestion credentials configured; authentication is disabled for development");
    }
    let max_batch_size = configured_batch_size()?;
    let rate_limit = configured_rate_limit()?;

    let state = AppState {
        legacy_token,
        credentials: Arc::new(Mutex::new(credential_store)),
        spool: Arc::new(Mutex::new(spool)),
        seen: Arc::new(Mutex::new(seen)),
        rate_windows: Arc::new(Mutex::new(HashMap::new())),
        rate_limit,
        max_batch_size,
        authentication_required,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/ingest/salts", post(ingest_salts))
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
        if !valid_record(record) {
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

impl CredentialStore {
    async fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = match fs::read(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if contents.is_empty() {
            return Ok(Self {
                path: path.to_path_buf(),
                records: BTreeMap::new(),
            });
        }

        #[cfg(unix)]
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;

        let file: CredentialFile = serde_json::from_slice(&contents)?;
        if file.version != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported credential file version",
            )
            .into());
        }
        Ok(Self {
            path: path.to_path_buf(),
            records: file.credentials,
        })
    }

    async fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let file = CredentialFile {
            version: 1,
            credentials: self.records.clone(),
        };
        let contents = serde_json::to_vec_pretty(&file)?;
        let temp_path = self.path.with_file_name(format!(
            ".{}.tmp-{}-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("collectors.json"),
            std::process::id(),
            now_seconds()
        ));
        let mut temp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .await?;
        temp.write_all(&contents).await?;
        temp.flush().await?;
        temp.sync_data().await?;
        drop(temp);
        #[cfg(unix)]
        fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(0o600)).await?;
        fs::rename(temp_path, &self.path).await?;
        Ok(())
    }

    fn create(&mut self, collector_id: &str) -> Result<String, String> {
        if self.records.contains_key(collector_id) {
            return Err("collector already exists; use rotate instead".to_string());
        }
        let token = generate_token()?;
        self.records.insert(
            collector_id.to_string(),
            CredentialRecord {
                token_hash: hash_token(&token),
                created_at: now_seconds(),
                rotated_at: None,
                revoked_at: None,
            },
        );
        Ok(token)
    }

    fn rotate(&mut self, collector_id: &str) -> Result<String, String> {
        let record = self
            .records
            .get_mut(collector_id)
            .ok_or_else(|| "collector does not exist".to_string())?;
        let token = generate_token()?;
        record.token_hash = hash_token(&token);
        record.rotated_at = Some(now_seconds());
        record.revoked_at = None;
        Ok(token)
    }

    fn revoke(&mut self, collector_id: &str) -> Result<(), String> {
        let record = self
            .records
            .get_mut(collector_id)
            .ok_or_else(|| "collector does not exist".to_string())?;
        record.revoked_at = Some(now_seconds());
        Ok(())
    }

    fn authenticate(&self, token: &str) -> Option<String> {
        let token_hash = hash_token(token);
        self.records.iter().find_map(|(collector_id, record)| {
            if record.revoked_at.is_none()
                && constant_time_eq(record.token_hash.as_bytes(), token_hash.as_bytes())
            {
                Some(collector_id.clone())
            } else {
                None
            }
        })
    }
}

async fn run_credential_command(
    command: CredentialCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from(
        env::var("SINGULARITY_CREDENTIALS_PATH")
            .unwrap_or_else(|_| DEFAULT_CREDENTIALS.to_string()),
    );
    let mut store = CredentialStore::load(&path).await?;
    match command {
        CredentialCommand::Create(collector_id) => {
            let token = store
                .create(&collector_id)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::AlreadyExists, error))?;
            store.save().await?;
            println!("collector_id={collector_id}");
            println!("token={token}");
            println!("store this token securely; it will not be shown again");
        }
        CredentialCommand::Rotate(collector_id) => {
            let token = store
                .rotate(&collector_id)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::NotFound, error))?;
            store.save().await?;
            println!("collector_id={collector_id}");
            println!("token={token}");
            println!("the previous token is no longer valid");
        }
        CredentialCommand::Revoke(collector_id) => {
            store
                .revoke(&collector_id)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::NotFound, error))?;
            store.save().await?;
            println!("revoked collector_id={collector_id}");
        }
        CredentialCommand::List => {
            for (collector_id, record) in &store.records {
                let status = if record.revoked_at.is_some() {
                    "revoked"
                } else {
                    "active"
                };
                println!("{collector_id}\t{status}");
            }
        }
    }
    Ok(())
}

fn parse_command(mut args: impl Iterator<Item = String>) -> Result<Option<Command>, String> {
    let Some(command) = args.next() else {
        return Ok(None);
    };
    if command == "metadata-worker" {
        if args.next().is_some() {
            return Err("unexpected metadata worker argument".to_string());
        }
        return Ok(Some(Command::MetadataWorker));
    }
    if command == "metadata-parser" {
        if args.next().is_some() {
            return Err("unexpected metadata parser argument".to_string());
        }
        return Ok(Some(Command::MetadataParser));
    }
    if command != "credentials" && command != "credential" {
        return Err("unknown command".to_string());
    }
    let subcommand = args
        .next()
        .ok_or_else(|| "missing credential command".to_string())?;
    let parsed = match subcommand.as_str() {
        "create" => CredentialCommand::Create(parse_collector_id(&mut args)?),
        "rotate" => CredentialCommand::Rotate(parse_collector_id(&mut args)?),
        "revoke" => CredentialCommand::Revoke(parse_collector_id(&mut args)?),
        "list" => CredentialCommand::List,
        _ => return Err("unknown credential command".to_string()),
    };
    if args.next().is_some() {
        return Err("unexpected credential command argument".to_string());
    }
    Ok(Some(Command::Credentials(parsed)))
}

fn parse_collector_id(args: &mut impl Iterator<Item = String>) -> Result<String, String> {
    let collector_id = args
        .next()
        .ok_or_else(|| "missing collector id".to_string())?;
    if valid_collector_id(&collector_id) {
        Ok(collector_id)
    } else {
        Err("collector id must be 1-64 ASCII letters, numbers, '.', '_' or '-'".to_string())
    }
}

fn valid_collector_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || ".-_".contains(character))
}

fn generate_token() -> Result<String, String> {
    let mut bytes = [0_u8; 32];
    fill_random(&mut bytes).map_err(|_| "could not generate secure credential".to_string())?;
    Ok(format!("sg_{}", hex::encode(bytes)))
}

fn hash_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    bool::from(left.ct_eq(right))
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
    consume_rate_limit(&mut windows, collector_id, state.rate_limit)
}

fn consume_rate_limit(
    windows: &mut HashMap<String, RateWindow>,
    collector_id: &str,
    rate_limit: RateLimit,
) -> bool {
    let now = Instant::now();
    let window = windows
        .entry(collector_id.to_string())
        .or_insert(RateWindow {
            started_at: now,
            requests: 0,
        });
    if now.duration_since(window.started_at) >= rate_limit.window {
        window.started_at = now;
        window.requests = 0;
    }
    if window.requests >= rate_limit.max_requests {
        return false;
    }
    window.requests += 1;
    true
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

fn configured_rate_limit() -> Result<RateLimit, std::io::Error> {
    let max_requests = env::var("SINGULARITY_COLLECTOR_RATE_LIMIT")
        .unwrap_or_else(|_| DEFAULT_RATE_LIMIT.to_string())
        .parse::<u32>()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid rate limit"))?;
    let window_seconds = env::var("SINGULARITY_COLLECTOR_RATE_WINDOW_SECONDS")
        .unwrap_or_else(|_| DEFAULT_RATE_WINDOW_SECONDS.to_string())
        .parse::<u64>()
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid rate window")
        })?;
    if max_requests == 0 || window_seconds == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "rate limit and window must be greater than zero",
        ));
    }
    Ok(RateLimit {
        max_requests,
        window: Duration::from_secs(window_seconds),
    })
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

    #[test]
    fn credential_lifecycle_hashes_tokens_and_revokes_old_tokens() {
        let mut store = CredentialStore {
            path: PathBuf::from("collectors.json"),
            records: BTreeMap::new(),
        };

        let first_token = store.create("collector-a").expect("credential creates");
        let serialized = serde_json::to_string(&store.records).expect("credentials serialize");
        assert!(!serialized.contains(&first_token));
        assert_eq!(
            store.authenticate(&first_token),
            Some("collector-a".to_string())
        );

        let second_token = store.rotate("collector-a").expect("credential rotates");
        assert_ne!(first_token, second_token);
        assert_eq!(store.authenticate(&first_token), None);
        assert_eq!(
            store.authenticate(&second_token),
            Some("collector-a".to_string())
        );

        store.revoke("collector-a").expect("credential revokes");
        assert_eq!(store.authenticate(&second_token), None);
    }

    #[test]
    fn malformed_authorization_is_rejected_without_parsing_a_token() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Basic not-a-bearer-token".parse().unwrap());

        assert_eq!(bearer_token(&headers), None);
    }

    #[test]
    fn rate_limits_are_isolated_per_collector() {
        let mut windows = HashMap::new();
        let limit = RateLimit {
            max_requests: 2,
            window: Duration::from_secs(60),
        };

        assert!(consume_rate_limit(&mut windows, "collector-a", limit));
        assert!(consume_rate_limit(&mut windows, "collector-a", limit));
        assert!(!consume_rate_limit(&mut windows, "collector-a", limit));
        assert!(consume_rate_limit(&mut windows, "collector-b", limit));
    }

    #[tokio::test]
    async fn authenticated_replays_are_deduplicated_and_attributed() {
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
        let token = "collector-test-token";
        let mut records = BTreeMap::new();
        records.insert(
            "collector-a".to_string(),
            CredentialRecord {
                token_hash: hash_token(token),
                created_at: 1,
                rotated_at: None,
                revoked_at: None,
            },
        );
        let state = AppState {
            legacy_token: None,
            credentials: Arc::new(Mutex::new(CredentialStore {
                path: PathBuf::from("collectors.json"),
                records,
            })),
            spool: Arc::new(Mutex::new(spool)),
            seen: Arc::new(Mutex::new(HashSet::new())),
            rate_windows: Arc::new(Mutex::new(HashMap::new())),
            rate_limit: RateLimit {
                max_requests: 10,
                window: Duration::from_secs(60),
            },
            max_batch_size: MAX_BATCH_SIZE,
            authentication_required: true,
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
