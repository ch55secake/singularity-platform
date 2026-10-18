use std::{
    collections::HashSet,
    env,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs;

mod download;
mod parser;
mod state;

use download::download_replay;
#[cfg(test)]
use download::{decompress_file, decompress_replay, replay_url};
use parser::{MODEL_VERSION, PARSER_VERSION, parse_replay};
use state::{ReplayJob, ReplayState, ReplayStatus};

const DEFAULT_SPOOL_PATH: &str = "data/salts.jsonl";
const DEFAULT_STATE_PATH: &str = "data/replay-state.json";
const DEFAULT_OUTPUT_DIR: &str = "data/replays";
const DEFAULT_BASE_URL: &str = "http://replay{cluster}.valve.net/1422450";
const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
const DEFAULT_RETRY_BASE_SECONDS: u64 = 30;
const DEFAULT_RETRY_MAX_SECONDS: u64 = 3_600;
const DEFAULT_MAX_COMPRESSED_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_MAX_DECOMPRESSED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_MAX_DISK_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const DEFAULT_DEBUG_RETENTION_SECONDS: u64 = 0;
#[derive(Debug, Clone, Deserialize)]
struct ReplayRecord {
    match_id: u64,
    cluster_id: Option<u32>,
    replay_salt: Option<u32>,
}

struct ReplayConfig {
    spool_path: PathBuf,
    state_path: PathBuf,
    output_dir: PathBuf,
    base_url: String,
    timeout: Duration,
    retry_base_seconds: u64,
    retry_max_seconds: u64,
    max_compressed_bytes: u64,
    max_decompressed_bytes: u64,
    max_disk_bytes: u64,
    debug_retention_seconds: u64,
}

struct ClickHouseClient {
    http: Client,
    url: String,
    database: String,
    user: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ReplayMatchRow {
    match_id: u64,
    replay_job_id: String,
    parser_version: String,
    parsed_at: u64,
    position_count: u32,
    model_version: u64,
}

#[derive(Default)]
struct WorkerSummary {
    discovered: usize,
    downloaded: usize,
    parsed: usize,
    deferred: usize,
    failed: usize,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReplayConfig::from_env()?;
    if let Some(parent) = config
        .state_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).await?;
    }
    fs::create_dir_all(&config.output_dir).await?;

    let state_path = config.state_path.clone();
    let mut state = ReplayState::load(&state_path).await?;
    let client = Client::builder()
        .timeout(config.timeout)
        .user_agent("singularity-platform-replay-worker/0.1")
        .build()?;
    let clickhouse = ClickHouseClient::from_env(&config.timeout)?;
    let spool = match fs::read_to_string(&config.spool_path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };

    let mut summary = WorkerSummary::default();
    let mut job_ids = HashSet::new();
    for line in spool.lines() {
        let Ok(record) = serde_json::from_str::<ReplayRecord>(line) else {
            continue;
        };
        if !record.is_downloadable() {
            continue;
        }
        let job_id = job_id(&record);
        if !job_ids.insert(job_id.clone()) {
            continue;
        }
        summary.discovered += 1;
        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            &job_id,
            &record,
            &mut summary,
        )
        .await?;
    }

    state.save().await?;
    let disk_bytes = disk_usage(&config.output_dir).await?;
    println!(
        "replay worker: discovered={} downloaded={} parsed={} deferred={} failed={} disk_bytes={} disk_limit_bytes={}",
        summary.discovered,
        summary.downloaded,
        summary.parsed,
        summary.deferred,
        summary.failed,
        disk_bytes,
        config.max_disk_bytes,
    );
    Ok(())
}

impl ReplayRecord {
    fn is_downloadable(&self) -> bool {
        self.match_id > 0
            && self.cluster_id.is_some_and(|cluster| cluster > 0)
            && self.replay_salt.is_some_and(|salt| salt > 0)
    }
}

impl ReplayConfig {
    fn from_env() -> Result<Self, std::io::Error> {
        let timeout_seconds = positive_env(
            "SINGULARITY_REPLAY_TIMEOUT_SECONDS",
            DEFAULT_TIMEOUT_SECONDS,
        )?;
        let retry_base_seconds = positive_env(
            "SINGULARITY_REPLAY_RETRY_BASE_SECONDS",
            DEFAULT_RETRY_BASE_SECONDS,
        )?;
        let retry_max_seconds = positive_env(
            "SINGULARITY_REPLAY_RETRY_MAX_SECONDS",
            DEFAULT_RETRY_MAX_SECONDS,
        )?;
        Ok(Self {
            spool_path: PathBuf::from(
                env::var("SINGULARITY_REPLAY_SPOOL_PATH")
                    .or_else(|_| env::var("SINGULARITY_SPOOL_PATH"))
                    .unwrap_or_else(|_| DEFAULT_SPOOL_PATH.to_string()),
            ),
            state_path: PathBuf::from(
                env::var("SINGULARITY_REPLAY_STATE_PATH")
                    .unwrap_or_else(|_| DEFAULT_STATE_PATH.to_string()),
            ),
            output_dir: PathBuf::from(
                env::var("SINGULARITY_REPLAY_OUTPUT_DIR")
                    .unwrap_or_else(|_| DEFAULT_OUTPUT_DIR.to_string()),
            ),
            base_url: env::var("SINGULARITY_REPLAY_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            timeout: Duration::from_secs(timeout_seconds),
            retry_base_seconds,
            retry_max_seconds: retry_max_seconds.max(retry_base_seconds),
            max_compressed_bytes: positive_env(
                "SINGULARITY_REPLAY_MAX_COMPRESSED_BYTES",
                DEFAULT_MAX_COMPRESSED_BYTES,
            )?,
            max_decompressed_bytes: positive_env(
                "SINGULARITY_REPLAY_MAX_DECOMPRESSED_BYTES",
                DEFAULT_MAX_DECOMPRESSED_BYTES,
            )?,
            max_disk_bytes: positive_env(
                "SINGULARITY_REPLAY_MAX_DISK_BYTES",
                DEFAULT_MAX_DISK_BYTES,
            )?,
            debug_retention_seconds: non_negative_env(
                "SINGULARITY_REPLAY_DEBUG_RETENTION_SECONDS",
                DEFAULT_DEBUG_RETENTION_SECONDS,
            )?,
        })
    }
}

fn positive_env(name: &str, default: u64) -> Result<u64, std::io::Error> {
    let value = env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u64>()
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid replay setting")
        })?;
    if value == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "replay setting must be greater than zero",
        ));
    }
    Ok(value)
}

fn non_negative_env(name: &str, default: u64) -> Result<u64, std::io::Error> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u64>()
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid replay setting")
        })
}

async fn process_record(
    state: &mut ReplayState,
    client: &Client,
    clickhouse: &ClickHouseClient,
    config: &ReplayConfig,
    job_id: &str,
    record: &ReplayRecord,
    summary: &mut WorkerSummary,
) -> Result<(), Box<dyn std::error::Error>> {
    let raw_path = config.output_dir.join(format!("{job_id}.dem"));
    let now = now_seconds();
    let existing = state.jobs.get(job_id).cloned();
    if existing
        .as_ref()
        .is_some_and(|job| job.status == ReplayStatus::Parsed)
    {
        let job = existing.as_ref().expect("parsed job exists");
        if job.parser_version.as_deref() == Some(PARSER_VERSION) {
            if config.debug_retention_seconds == 0
                || job
                    .raw_retention_until
                    .is_some_and(|retention_until| retention_until <= now)
            {
                remove_raw_file(job.raw_path.as_deref()).await?;
            }
            return Ok(());
        }

        if let Some(path) = job.raw_path.as_deref() {
            if fs::try_exists(path).await? {
                state.jobs.insert(
                    job_id.to_string(),
                    ReplayJob {
                        status: ReplayStatus::Downloaded,
                        attempts: job.attempts,
                        next_retry_at: None,
                        last_error: Some("parser_version_mismatch".to_string()),
                        raw_path: Some(path.to_string()),
                        position_count: None,
                        parser_version: None,
                        raw_retention_until: None,
                    },
                );
            } else {
                record_failure(
                    state,
                    job_id,
                    ReplayStatus::Pending,
                    job.attempts,
                    "parser_version_mismatch",
                    None,
                    config,
                )
                .await?;
                summary.failed += 1;
                return Ok(());
            }
        }
    }
    if existing
        .as_ref()
        .and_then(|job| job.next_retry_at)
        .is_some_and(|retry_at| retry_at > now)
    {
        summary.deferred += 1;
        return Ok(());
    }

    let attempt = existing
        .as_ref()
        .map_or(1, |job| job.attempts.saturating_add(1));
    if !fs::try_exists(&raw_path).await? {
        if existing.is_none() {
            state.jobs.insert(
                job_id.to_string(),
                ReplayJob {
                    status: ReplayStatus::Pending,
                    attempts: 0,
                    next_retry_at: None,
                    last_error: None,
                    raw_path: None,
                    position_count: None,
                    parser_version: None,
                    raw_retention_until: None,
                },
            );
            state.save().await?;
        }
        if disk_usage(&config.output_dir)
            .await?
            .saturating_add(config.max_compressed_bytes)
            .saturating_add(config.max_decompressed_bytes)
            > config.max_disk_bytes
        {
            record_failure(
                state,
                job_id,
                ReplayStatus::Pending,
                attempt,
                "disk_limit",
                None,
                config,
            )
            .await?;
            summary.failed += 1;
            return Ok(());
        }
        match download_replay(client, config, record, &raw_path).await {
            Ok(()) => {
                summary.downloaded += 1;
                state.jobs.insert(
                    job_id.to_string(),
                    ReplayJob {
                        status: ReplayStatus::Downloaded,
                        attempts: attempt,
                        next_retry_at: None,
                        last_error: None,
                        raw_path: Some(raw_path.display().to_string()),
                        position_count: None,
                        parser_version: None,
                        raw_retention_until: None,
                    },
                );
                state.save().await?;
            }
            Err(error) => {
                record_failure(
                    state,
                    job_id,
                    ReplayStatus::Pending,
                    attempt,
                    error.code,
                    None,
                    config,
                )
                .await?;
                summary.failed += 1;
                return Ok(());
            }
        }
    }

    let model_version = MODEL_VERSION;
    let parsed = match parse_replay(record.match_id, &raw_path, model_version) {
        Ok(parsed) => parsed,
        Err(error) => {
            record_failure(
                state,
                job_id,
                ReplayStatus::Downloaded,
                attempt,
                error,
                Some(raw_path.display().to_string()),
                config,
            )
            .await?;
            summary.failed += 1;
            return Ok(());
        }
    };

    let match_row = ReplayMatchRow {
        match_id: record.match_id,
        replay_job_id: job_id.to_string(),
        parser_version: PARSER_VERSION.to_string(),
        parsed_at: now,
        position_count: parsed.positions.len() as u32,
        model_version,
    };
    clickhouse
        .insert("replay_matches", REPLAY_MATCH_COLUMNS, &[match_row])
        .await?;
    clickhouse
        .insert(
            "replay_positions",
            REPLAY_POSITION_COLUMNS,
            &parsed.positions,
        )
        .await?;

    state.jobs.insert(
        job_id.to_string(),
        ReplayJob {
            status: ReplayStatus::Parsed,
            attempts: attempt,
            next_retry_at: None,
            last_error: None,
            raw_path: Some(raw_path.display().to_string()),
            position_count: Some(parsed.positions.len() as u32),
            parser_version: Some(PARSER_VERSION.to_string()),
            raw_retention_until: (config.debug_retention_seconds > 0)
                .then(|| now.saturating_add(config.debug_retention_seconds)),
        },
    );
    state.save().await?;
    if config.debug_retention_seconds == 0 {
        remove_raw_file(Some(&raw_path.display().to_string())).await?;
    }
    summary.parsed += 1;
    Ok(())
}

async fn remove_raw_file(raw_path: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let Some(raw_path) = raw_path else {
        return Ok(());
    };
    match fs::remove_file(raw_path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

const REPLAY_MATCH_COLUMNS: &str =
    "match_id, replay_job_id, parser_version, parsed_at, position_count, model_version";
const REPLAY_POSITION_COLUMNS: &str =
    "match_id, entity_index, tick, time_s, x, y, z, parser_version, model_version";

async fn record_failure(
    state: &mut ReplayState,
    job_id: &str,
    status: ReplayStatus,
    attempts: u32,
    error: &str,
    raw_path: Option<String>,
    config: &ReplayConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    state.jobs.insert(
        job_id.to_string(),
        ReplayJob {
            status,
            attempts,
            next_retry_at: Some(now_seconds().saturating_add(retry_delay(config, attempts))),
            last_error: Some(error.to_string()),
            raw_path,
            position_count: None,
            parser_version: None,
            raw_retention_until: None,
        },
    );
    state.save().await?;
    Ok(())
}

async fn disk_usage(path: &Path) -> Result<u64, std::io::Error> {
    let mut total: u64 = 0;
    let mut entries = match fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    while let Some(entry) = entries.next_entry().await? {
        let metadata = entry.metadata().await?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

impl ClickHouseClient {
    fn from_env(timeout: &Duration) -> Result<Self, std::io::Error> {
        let url = env::var("SINGULARITY_CLICKHOUSE_URL").map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SINGULARITY_CLICKHOUSE_URL is required for replay-worker",
            )
        })?;
        let http = Client::builder()
            .timeout(*timeout)
            .build()
            .map_err(std::io::Error::other)?;
        Ok(Self {
            http,
            url,
            database: env::var("SINGULARITY_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| "singularity".to_string()),
            user: env::var("SINGULARITY_CLICKHOUSE_USER").ok(),
            password: env::var("SINGULARITY_CLICKHOUSE_PASSWORD").ok(),
        })
    }

    async fn insert<T: Serialize>(
        &self,
        table: &str,
        columns: &str,
        rows: &[T],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut body, row)?;
            body.push(b'\n');
        }
        let query = format!(
            "INSERT INTO {}.{} ({}) FORMAT JSONEachRow",
            self.database, table, columns
        );
        let mut request = self
            .http
            .post(&self.url)
            .query(&[("query", query)])
            .header("content-type", "application/json")
            .body(body);
        if let Some(user) = &self.user {
            request = request.basic_auth(user, self.password.as_deref());
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(std::io::Error::other("ClickHouse insert failed").into());
        }
        Ok(())
    }
}

fn job_id(record: &ReplayRecord) -> String {
    let mut digest = Sha256::new();
    digest.update(record.match_id.to_le_bytes());
    digest.update(record.cluster_id.unwrap_or_default().to_le_bytes());
    digest.update(record.replay_salt.unwrap_or_default().to_le_bytes());
    hex::encode(digest.finalize())
}

fn retry_delay(config: &ReplayConfig, attempt: u32) -> u64 {
    let exponent = attempt.saturating_sub(1).min(16);
    let multiplier = 1_u64 << exponent;
    config
        .retry_base_seconds
        .saturating_mul(multiplier)
        .min(config.retry_max_seconds)
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bzip2::{Compression, write::BzEncoder};
    use std::collections::BTreeMap;
    use std::fs as std_fs;
    use std::io::{Cursor, Write};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn sample_record() -> ReplayRecord {
        ReplayRecord {
            match_id: 113390164,
            cluster_id: Some(123),
            replay_salt: Some(456),
        }
    }

    fn sample_config() -> ReplayConfig {
        ReplayConfig {
            spool_path: PathBuf::new(),
            state_path: PathBuf::new(),
            output_dir: PathBuf::new(),
            base_url: "http://replay{cluster}.valve.net/1422450".to_string(),
            timeout: Duration::from_secs(1),
            retry_base_seconds: 30,
            retry_max_seconds: 3_600,
            max_compressed_bytes: 1024,
            max_decompressed_bytes: 2048,
            max_disk_bytes: 4096,
            debug_retention_seconds: 0,
        }
    }

    #[test]
    fn builds_the_cache_replay_url() {
        assert_eq!(
            replay_url(&sample_config(), &sample_record()),
            "http://replay123.valve.net/1422450/113390164_456.dem.bz2"
        );
    }

    #[test]
    fn decompresses_zstd_and_bzip2_payloads() {
        let input = b"demo bytes";
        let zstd = zstd::stream::encode_all(Cursor::new(input), 0).expect("zstd encodes");
        let mut encoder = BzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(input).expect("bzip2 writes");
        let bzip2 = encoder.finish().expect("bzip2 finishes");

        assert_eq!(decompress_replay(&zstd, 1024).expect("zstd decodes"), input);
        assert_eq!(
            decompress_replay(&bzip2, 1024).expect("bzip2 decodes"),
            input
        );
        assert_eq!(
            decompress_replay(b"not compressed", 1024),
            Err("unsupported_compression")
        );
        assert_eq!(decompress_replay(input, 4), Err("unsupported_compression"));
    }

    #[test]
    fn streams_decompression_to_a_bounded_raw_file() {
        let directory = std::env::temp_dir().join(format!(
            "singularity-replay-test-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        std_fs::create_dir(&directory).expect("test directory creates");
        let compressed_path = directory.join("demo.dem.bz2");
        let raw_path = directory.join("demo.dem");
        let input = b"demo bytes that are larger than the test limit";
        let mut encoder = BzEncoder::new(
            std_fs::File::create(&compressed_path).expect("compressed file creates"),
            Compression::best(),
        );
        encoder.write_all(input).expect("bzip2 writes");
        encoder.finish().expect("bzip2 finishes");

        assert_eq!(decompress_file(&compressed_path, &raw_path, 1024), Ok(()));
        assert_eq!(std_fs::read(&raw_path).expect("raw file reads"), input);

        std_fs::remove_file(&raw_path).expect("raw file removes");
        assert_eq!(
            decompress_file(&compressed_path, &raw_path, 4),
            Err("decompressed_too_large")
        );
        assert!(!raw_path.exists());
        std_fs::remove_dir_all(directory).expect("test directory removes");
    }

    #[test]
    fn parses_a_minimal_haste_demo_header() {
        let directory = std::env::temp_dir().join(format!(
            "singularity-replay-parser-test-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        std_fs::create_dir(&directory).expect("test directory creates");
        let raw_path = directory.join("minimal.dem");
        let mut demo = Vec::from(*b"PBDEMS2\0");
        demo.extend_from_slice(&[0; 8]);
        std_fs::write(&raw_path, demo).expect("minimal demo writes");

        let parsed = parse_replay(42, &raw_path, MODEL_VERSION).expect("Haste parses header");
        assert!(parsed.positions.is_empty());

        std_fs::remove_dir_all(directory).expect("test directory removes");
    }

    #[test]
    fn persists_the_explicit_replay_state_names() {
        assert_eq!(
            serde_json::to_string(&ReplayStatus::Pending).expect("state serializes"),
            "\"replay_pending\""
        );
        assert_eq!(
            serde_json::to_string(&ReplayStatus::Downloaded).expect("state serializes"),
            "\"replay_downloaded\""
        );
        assert_eq!(
            serde_json::to_string(&ReplayStatus::Parsed).expect("state serializes"),
            "\"replay_parsed\""
        );
    }

    #[tokio::test]
    async fn downloads_and_decompresses_a_replay_fixture() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fixture listener binds");
        let address = listener.local_addr().expect("fixture address reads");
        let mut demo = Vec::from(*b"PBDEMS2\0");
        demo.extend_from_slice(&[0; 8]);
        let compressed =
            zstd::stream::encode_all(Cursor::new(&demo), 0).expect("fixture compresses");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("fixture accepts");
            let mut request = [0_u8; 1024];
            let _ = stream
                .read(&mut request)
                .await
                .expect("fixture request reads");
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                compressed.len()
            );
            stream
                .write_all(headers.as_bytes())
                .await
                .expect("fixture headers write");
            stream
                .write_all(&compressed)
                .await
                .expect("fixture body writes");
        });

        let directory = std::env::temp_dir().join(format!(
            "singularity-replay-download-test-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        std_fs::create_dir(&directory).expect("test directory creates");
        let raw_path = directory.join("fixture.dem");
        let mut config = sample_config();
        config.base_url = format!("http://{address}/1422450");
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("fixture client builds");

        download_replay(&client, &config, &sample_record(), &raw_path)
            .await
            .expect("fixture downloads");
        assert_eq!(std_fs::read(&raw_path).expect("fixture reads"), demo);
        server.await.expect("fixture server completes");
        std_fs::remove_dir_all(directory).expect("test directory removes");
    }

    #[tokio::test]
    async fn persists_once_and_deletes_the_raw_demo_after_parse() {
        let replay_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("replay listener binds");
        let replay_address = replay_listener.local_addr().expect("replay address reads");
        let mut demo = Vec::from(*b"PBDEMS2\0");
        demo.extend_from_slice(&[0; 8]);
        let compressed =
            zstd::stream::encode_all(Cursor::new(&demo), 0).expect("fixture compresses");
        let replay_server = tokio::spawn(async move {
            let (mut stream, _) = replay_listener.accept().await.expect("replay accepts");
            let mut request = [0_u8; 1024];
            let _ = stream
                .read(&mut request)
                .await
                .expect("replay request reads");
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                compressed.len()
            );
            stream
                .write_all(headers.as_bytes())
                .await
                .expect("replay headers write");
            stream
                .write_all(&compressed)
                .await
                .expect("replay body writes");
        });

        let clickhouse_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ClickHouse listener binds");
        let clickhouse_address = clickhouse_listener
            .local_addr()
            .expect("ClickHouse address reads");
        let clickhouse_server = tokio::spawn(async move {
            let (mut stream, _) = clickhouse_listener
                .accept()
                .await
                .expect("ClickHouse accepts");
            let mut request = [0_u8; 4096];
            let count = stream
                .read(&mut request)
                .await
                .expect("ClickHouse request reads");
            assert!(String::from_utf8_lossy(&request[..count]).contains("replay_matches"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("ClickHouse response writes");
        });

        let directory = std::env::temp_dir().join(format!(
            "singularity-replay-process-test-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        std_fs::create_dir(&directory).expect("test directory creates");
        let mut config = sample_config();
        config.base_url = format!("http://{replay_address}/1422450");
        config.output_dir = directory.join("replays");
        config.state_path = directory.join("state.json");
        std_fs::create_dir(&config.output_dir).expect("replay directory creates");
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("fixture client builds");
        let clickhouse = ClickHouseClient {
            http: client.clone(),
            url: format!("http://{clickhouse_address}"),
            database: "singularity".to_string(),
            user: None,
            password: None,
        };
        let mut state = ReplayState {
            path: config.state_path.clone(),
            jobs: BTreeMap::new(),
        };
        let record = sample_record();
        let id = job_id(&record);
        let mut summary = WorkerSummary::default();

        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            &id,
            &record,
            &mut summary,
        )
        .await
        .expect("replay process succeeds");
        assert_eq!(summary.downloaded, 1);
        assert_eq!(summary.parsed, 1);
        assert_eq!(state.jobs[&id].status, ReplayStatus::Parsed);
        assert!(!config.output_dir.join(format!("{id}.dem")).exists());

        let mut retry_summary = WorkerSummary::default();
        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            &id,
            &record,
            &mut retry_summary,
        )
        .await
        .expect("replay retry succeeds");
        assert_eq!(retry_summary.parsed, 0);
        replay_server.await.expect("replay server completes");
        clickhouse_server
            .await
            .expect("ClickHouse server completes");
        std_fs::remove_dir_all(directory).expect("test directory removes");
    }

    #[test]
    fn replay_job_id_does_not_expose_the_salt() {
        let id = job_id(&sample_record());
        assert!(!id.contains("456"));
        assert_eq!(id.len(), 64);
    }

    #[test]
    fn rejects_replay_records_without_required_values() {
        let mut record = sample_record();
        assert!(record.is_downloadable());
        record.replay_salt = None;
        assert!(!record.is_downloadable());
        record.replay_salt = Some(456);
        record.cluster_id = None;
        assert!(!record.is_downloadable());
    }
}
