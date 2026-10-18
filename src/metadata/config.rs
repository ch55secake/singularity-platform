use std::{env, path::PathBuf, time::Duration};

const DEFAULT_SPOOL_PATH: &str = "data/salts.jsonl";
const DEFAULT_STATE_PATH: &str = "data/metadata-state.json";
const DEFAULT_OUTPUT_DIR: &str = "data/metadata";
const DEFAULT_BASE_URL: &str = "http://replay{cluster}.valve.net/1422450";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_RETRY_BASE_SECONDS: u64 = 30;
const DEFAULT_RETRY_MAX_SECONDS: u64 = 3_600;

pub(super) struct WorkerConfig {
    pub(super) spool_path: PathBuf,
    pub(super) state_path: PathBuf,
    pub(super) output_dir: PathBuf,
    pub(super) base_url: String,
    pub(super) timeout: Duration,
    pub(super) retry_base_seconds: u64,
    pub(super) retry_max_seconds: u64,
}

impl WorkerConfig {
    pub(super) fn from_env() -> Result<Self, std::io::Error> {
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
