use std::{env, path::PathBuf, sync::Arc};

use tokio::{
    fs::{self, OpenOptions},
    sync::Mutex,
};
use tracing::warn;

use crate::{
    AppState, DEFAULT_CREDENTIALS, DEFAULT_SPOOL, credentials, live, rate_limit, read_api,
    salt_spool, server_config,
};

pub(super) struct ServerState {
    pub(super) app_state: AppState,
    pub(super) spool_path: PathBuf,
    pub(super) credential_count: usize,
    pub(super) recovered_records: usize,
    pub(super) rate_limit: rate_limit::RateLimit,
}

pub(super) async fn load() -> Result<ServerState, Box<dyn std::error::Error>> {
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
    let legacy_token = server_config::non_empty_env("SINGULARITY_INGEST_TOKEN");
    let authentication_required = legacy_token.is_some() || credential_count > 0;
    if !authentication_required {
        warn!("no ingestion credentials configured; authentication is disabled for development");
    }
    let max_batch_size = server_config::configured_batch_size()?;
    let rate_limit = rate_limit::configured_rate_limit()?;
    let read_api = read_api::ReadApiState::from_env()?;
    let live_path = live::store_path(&spool_path);
    let live_store = live::LiveStore::load(&live_path).await?;

    let app_state = AppState {
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

    Ok(ServerState {
        app_state,
        spool_path,
        credential_count,
        recovered_records,
        rate_limit,
    })
}
