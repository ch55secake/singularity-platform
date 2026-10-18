use std::{env, path::PathBuf, time::Duration};

const DEFAULT_BASE_URL: &str = "https://api.deadlock-api.com/v1/assets";
const DEFAULT_DATABASE: &str = "singularity";
const DEFAULT_STATE_PATH: &str = "data/assets-state.json";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

pub(super) struct AssetConfig {
    pub(super) base_url: String,
    pub(super) state_path: PathBuf,
    pub(super) timeout: Duration,
    pub(super) clickhouse_url: String,
    pub(super) clickhouse_database: String,
    pub(super) clickhouse_user: Option<String>,
    pub(super) clickhouse_password: Option<String>,
}

impl AssetConfig {
    pub(super) fn from_env() -> Result<Self, std::io::Error> {
        let timeout = env::var("SINGULARITY_ASSETS_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| DEFAULT_TIMEOUT_SECONDS.to_string())
            .parse::<u64>()
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid asset timeout")
            })?;
        if timeout == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "asset timeout must be greater than zero",
            ));
        }
        let clickhouse_url = env::var("SINGULARITY_CLICKHOUSE_URL").map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SINGULARITY_CLICKHOUSE_URL is required for assets-sync",
            )
        })?;
        Ok(Self {
            base_url: env::var("SINGULARITY_ASSETS_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            state_path: PathBuf::from(
                env::var("SINGULARITY_ASSETS_STATE_PATH")
                    .unwrap_or_else(|_| DEFAULT_STATE_PATH.to_string()),
            ),
            timeout: Duration::from_secs(timeout),
            clickhouse_url,
            clickhouse_database: env::var("SINGULARITY_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| DEFAULT_DATABASE.to_string()),
            clickhouse_user: env::var("SINGULARITY_CLICKHOUSE_USER").ok(),
            clickhouse_password: env::var("SINGULARITY_CLICKHOUSE_PASSWORD").ok(),
        })
    }
}
