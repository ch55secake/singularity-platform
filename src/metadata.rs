use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;

mod config;
mod download;
mod state;
mod worker;

pub use worker::run;

#[derive(Debug, Clone, Deserialize)]
struct MetadataRecord {
    match_id: u64,
    cluster_id: Option<u32>,
    metadata_salt: Option<u32>,
}

impl MetadataRecord {
    fn is_downloadable(&self) -> bool {
        self.match_id > 0
            && self.cluster_id.is_some_and(|cluster| cluster > 0)
            && self.metadata_salt.is_some_and(|salt| salt > 0)
    }
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
