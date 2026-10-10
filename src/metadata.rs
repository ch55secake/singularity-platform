use std::{
    collections::{BTreeMap, HashSet},
    env,
    io::{Cursor, Read},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use bzip2::read::BzDecoder;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};
use zstd::stream::read::Decoder as ZstdDecoder;

const DEFAULT_SPOOL_PATH: &str = "data/salts.jsonl";
const DEFAULT_STATE_PATH: &str = "data/metadata-state.json";
const DEFAULT_OUTPUT_DIR: &str = "data/metadata";
const DEFAULT_BASE_URL: &str = "http://replay{cluster}.valve.net/1422450";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_RETRY_BASE_SECONDS: u64 = 30;
const DEFAULT_RETRY_MAX_SECONDS: u64 = 3_600;
const MAX_COMPRESSED_BYTES: u64 = 16 * 1024 * 1024;
const MAX_DECOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

#[derive(Debug, Clone, Deserialize)]
struct MetadataRecord {
    match_id: u64,
    cluster_id: Option<u32>,
    metadata_salt: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MetadataStatus {
    #[serde(rename = "metadata_pending")]
    MetadataPending,
    #[serde(rename = "metadata_processed")]
    MetadataProcessed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataJob {
    status: MetadataStatus,
    attempts: u32,
    next_retry_at: Option<u64>,
    last_error: Option<String>,
    output_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MetadataStateFile {
    version: u32,
    #[serde(default)]
    jobs: BTreeMap<String, MetadataJob>,
}

struct MetadataState {
    path: PathBuf,
    jobs: BTreeMap<String, MetadataJob>,
}

struct WorkerConfig {
    spool_path: PathBuf,
    state_path: PathBuf,
    output_dir: PathBuf,
    base_url: String,
    timeout: Duration,
    retry_base_seconds: u64,
    retry_max_seconds: u64,
}

#[derive(Debug)]
struct DownloadError {
    code: &'static str,
}

#[derive(Default)]
struct WorkerSummary {
    discovered: usize,
    processed: usize,
    deferred: usize,
    failed: usize,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = WorkerConfig::from_env()?;
    if let Some(parent) = config
        .state_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).await?;
    }
    fs::create_dir_all(&config.output_dir).await?;

    let mut state = MetadataState::load(&config.state_path).await?;
    let client = Client::builder()
        .timeout(config.timeout)
        .user_agent("singularity-platform-metadata-worker/0.1")
        .build()?;
    let spool = match fs::read_to_string(&config.spool_path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };

    let mut summary = WorkerSummary::default();
    let mut job_ids = HashSet::new();
    for line in spool.lines() {
        let Ok(record) = serde_json::from_str::<MetadataRecord>(line) else {
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
        process_record(&mut state, &client, &config, &job_id, &record, &mut summary).await?;
    }

    state.save().await?;
    println!(
        "metadata worker: discovered={} processed={} deferred={} failed={}",
        summary.discovered, summary.processed, summary.deferred, summary.failed
    );
    Ok(())
}

impl MetadataRecord {
    fn is_downloadable(&self) -> bool {
        self.match_id > 0
            && self.cluster_id.is_some_and(|cluster| cluster > 0)
            && self.metadata_salt.is_some_and(|salt| salt > 0)
    }
}

impl WorkerConfig {
    fn from_env() -> Result<Self, std::io::Error> {
        let spool_path = env::var("SINGULARITY_METADATA_SPOOL_PATH")
            .or_else(|_| env::var("SINGULARITY_SPOOL_PATH"))
            .unwrap_or_else(|_| DEFAULT_SPOOL_PATH.to_string());
        let timeout_seconds = positive_env(
            "SINGULARITY_METADATA_TIMEOUT_SECONDS",
            DEFAULT_TIMEOUT_SECONDS,
        )?;
        let retry_base_seconds = positive_env(
            "SINGULARITY_METADATA_RETRY_BASE_SECONDS",
            DEFAULT_RETRY_BASE_SECONDS,
        )?;
        let retry_max_seconds = positive_env(
            "SINGULARITY_METADATA_RETRY_MAX_SECONDS",
            DEFAULT_RETRY_MAX_SECONDS,
        )?;
        Ok(Self {
            spool_path: PathBuf::from(spool_path),
            state_path: PathBuf::from(
                env::var("SINGULARITY_METADATA_STATE_PATH")
                    .unwrap_or_else(|_| DEFAULT_STATE_PATH.to_string()),
            ),
            output_dir: PathBuf::from(
                env::var("SINGULARITY_METADATA_OUTPUT_DIR")
                    .unwrap_or_else(|_| DEFAULT_OUTPUT_DIR.to_string()),
            ),
            base_url: env::var("SINGULARITY_METADATA_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            timeout: Duration::from_secs(timeout_seconds),
            retry_base_seconds,
            retry_max_seconds: retry_max_seconds.max(retry_base_seconds),
        })
    }
}

fn positive_env(name: &str, default: u64) -> Result<u64, std::io::Error> {
    let value = env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u64>()
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid worker setting")
        })?;
    if value == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "worker setting must be greater than zero",
        ));
    }
    Ok(value)
}

impl MetadataState {
    async fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = match fs::read(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if contents.is_empty() {
            return Ok(Self {
                path: path.to_path_buf(),
                jobs: BTreeMap::new(),
            });
        }
        #[cfg(unix)]
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
        let file: MetadataStateFile = serde_json::from_slice(&contents)?;
        if file.version != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported metadata state version",
            )
            .into());
        }
        Ok(Self {
            path: path.to_path_buf(),
            jobs: file.jobs,
        })
    }

    async fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).await?;
        }
        let contents = serde_json::to_vec_pretty(&MetadataStateFile {
            version: 1,
            jobs: self.jobs.clone(),
        })?;
        let temp_path = self.path.with_file_name(format!(
            ".{}.tmp-{}-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("metadata-state.json"),
            std::process::id(),
            now_nanos()
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
}

async fn process_record(
    state: &mut MetadataState,
    client: &Client,
    config: &WorkerConfig,
    job_id: &str,
    record: &MetadataRecord,
    summary: &mut WorkerSummary,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_path = config.output_dir.join(format!("{job_id}.meta.pb"));
    let now = now_seconds();
    let existing = state.jobs.get(job_id).cloned();
    if let Some(job) = &existing {
        if job.status == MetadataStatus::MetadataProcessed && fs::try_exists(&output_path).await? {
            return Ok(());
        }
        if job.status == MetadataStatus::MetadataPending
            && job.next_retry_at.is_some_and(|retry_at| retry_at > now)
        {
            summary.deferred += 1;
            return Ok(());
        }
    }
    if fs::try_exists(&output_path).await? {
        state.jobs.insert(
            job_id.to_string(),
            MetadataJob {
                status: MetadataStatus::MetadataProcessed,
                attempts: existing.map_or(0, |job| job.attempts),
                next_retry_at: None,
                last_error: None,
                output_path: Some(output_path.display().to_string()),
            },
        );
        summary.processed += 1;
        return Ok(());
    }

    let attempt = existing.map_or(1, |job| job.attempts.saturating_add(1));
    match download_metadata(client, config, record).await {
        Ok(contents) => {
            write_output(&output_path, &contents).await?;
            state.jobs.insert(
                job_id.to_string(),
                MetadataJob {
                    status: MetadataStatus::MetadataProcessed,
                    attempts: attempt,
                    next_retry_at: None,
                    last_error: None,
                    output_path: Some(output_path.display().to_string()),
                },
            );
            summary.processed += 1;
        }
        Err(error) => {
            let retry_after = retry_delay(config, attempt);
            state.jobs.insert(
                job_id.to_string(),
                MetadataJob {
                    status: MetadataStatus::MetadataPending,
                    attempts: attempt,
                    next_retry_at: Some(now.saturating_add(retry_after)),
                    last_error: Some(error.code.to_string()),
                    output_path: Some(output_path.display().to_string()),
                },
            );
            summary.failed += 1;
        }
    }
    state.save().await?;
    Ok(())
}

async fn download_metadata(
    client: &Client,
    config: &WorkerConfig,
    record: &MetadataRecord,
) -> Result<Vec<u8>, DownloadError> {
    let url = metadata_url(config, record);
    let mut response = client.get(url).send().await.map_err(|_| DownloadError {
        code: "request_failed",
    })?;
    let status = response.status();
    if !status.is_success() {
        return Err(DownloadError {
            code: if status == StatusCode::NOT_FOUND {
                "http_404"
            } else if status.is_server_error() {
                "http_5xx"
            } else {
                "http_error"
            },
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_COMPRESSED_BYTES)
    {
        return Err(DownloadError {
            code: "compressed_too_large",
        });
    }
    let mut compressed = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| DownloadError {
        code: "response_failed",
    })? {
        if compressed.len() as u64 + chunk.len() as u64 > MAX_COMPRESSED_BYTES {
            return Err(DownloadError {
                code: "compressed_too_large",
            });
        }
        compressed.extend_from_slice(&chunk);
    }
    decompress_metadata(&compressed).map_err(|code| DownloadError { code })
}

fn metadata_url(config: &WorkerConfig, record: &MetadataRecord) -> String {
    let cluster = record.cluster_id.expect("downloadable record has cluster");
    let salt = record
        .metadata_salt
        .expect("downloadable record has metadata salt");
    format!(
        "{}/{match_id}_{salt}.meta.bz2",
        config
            .base_url
            .replace("{cluster}", &cluster.to_string())
            .trim_end_matches('/'),
        match_id = record.match_id,
    )
}

fn decompress_metadata(compressed: &[u8]) -> Result<Vec<u8>, &'static str> {
    if compressed.starts_with(&ZSTD_MAGIC) {
        let decoder =
            ZstdDecoder::new(Cursor::new(compressed)).map_err(|_| "decompression_failed")?;
        return read_limited(decoder);
    }
    if compressed.starts_with(b"BZh") {
        return read_limited(BzDecoder::new(Cursor::new(compressed)));
    }
    Err("unsupported_compression")
}

fn read_limited<R: Read>(reader: R) -> Result<Vec<u8>, &'static str> {
    let mut limited = reader.take(MAX_DECOMPRESSED_BYTES + 1);
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .map_err(|_| "decompression_failed")?;
    if output.len() as u64 > MAX_DECOMPRESSED_BYTES {
        return Err("decompressed_too_large");
    }
    Ok(output)
}

async fn write_output(path: &Path, contents: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let temp_path = path.with_file_name(format!(
        ".{}.tmp-{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        now_nanos()
    ));
    let mut temp = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)
        .await?;
    temp.write_all(contents).await?;
    temp.flush().await?;
    temp.sync_data().await?;
    drop(temp);
    #[cfg(unix)]
    fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(0o600)).await?;
    fs::rename(temp_path, path).await?;
    Ok(())
}

fn job_id(record: &MetadataRecord) -> String {
    let mut digest = Sha256::new();
    digest.update(record.match_id.to_le_bytes());
    digest.update(record.cluster_id.unwrap_or_default().to_le_bytes());
    digest.update(record.metadata_salt.unwrap_or_default().to_le_bytes());
    hex::encode(digest.finalize())
}

fn retry_delay(config: &WorkerConfig, attempt: u32) -> u64 {
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
    use std::io::Write;

    fn sample_record() -> MetadataRecord {
        MetadataRecord {
            match_id: 113390164,
            cluster_id: Some(123),
            metadata_salt: Some(456),
        }
    }

    #[test]
    fn builds_the_cache_metadata_url() {
        let config = WorkerConfig {
            spool_path: PathBuf::new(),
            state_path: PathBuf::new(),
            output_dir: PathBuf::new(),
            base_url: "http://replay{cluster}.valve.net/1422450".to_string(),
            timeout: Duration::from_secs(1),
            retry_base_seconds: 30,
            retry_max_seconds: 3_600,
        };

        assert_eq!(
            metadata_url(&config, &sample_record()),
            "http://replay123.valve.net/1422450/113390164_456.meta.bz2"
        );
    }

    #[test]
    fn decompresses_zstd_and_bzip2_payloads() {
        let input = b"metadata protobuf bytes";
        let zstd = zstd::stream::encode_all(Cursor::new(input), 0).expect("zstd encodes");
        let mut encoder = BzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(input).expect("bzip2 writes");
        let bzip2 = encoder.finish().expect("bzip2 finishes");

        assert_eq!(decompress_metadata(&zstd).expect("zstd decodes"), input);
        assert_eq!(decompress_metadata(&bzip2).expect("bzip2 decodes"), input);
        assert_eq!(
            decompress_metadata(b"not compressed"),
            Err("unsupported_compression")
        );
    }

    #[test]
    fn job_id_does_not_expose_the_salt() {
        let record = sample_record();
        let id = job_id(&record);
        assert_eq!(id.len(), 64);
        assert!(!id.contains(&record.metadata_salt.unwrap().to_string()));
    }

    #[test]
    fn retry_delay_is_exponential_and_capped() {
        let config = WorkerConfig {
            spool_path: PathBuf::new(),
            state_path: PathBuf::new(),
            output_dir: PathBuf::new(),
            base_url: String::new(),
            timeout: Duration::from_secs(1),
            retry_base_seconds: 30,
            retry_max_seconds: 100,
        };

        assert_eq!(retry_delay(&config, 1), 30);
        assert_eq!(retry_delay(&config, 2), 60);
        assert_eq!(retry_delay(&config, 3), 100);
    }
}
