use std::{env, path::PathBuf, time::Duration};

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

pub(super) struct ReplayConfig {
    pub(super) spool_path: PathBuf,
    pub(super) state_path: PathBuf,
    pub(super) output_dir: PathBuf,
    pub(super) base_url: String,
    pub(super) timeout: Duration,
    pub(super) retry_base_seconds: u64,
    pub(super) retry_max_seconds: u64,
    pub(super) max_compressed_bytes: u64,
    pub(super) max_decompressed_bytes: u64,
    pub(super) max_disk_bytes: u64,
    pub(super) debug_retention_seconds: u64,
}

impl ReplayConfig {
    pub(super) fn from_env() -> Result<Self, std::io::Error> {
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
