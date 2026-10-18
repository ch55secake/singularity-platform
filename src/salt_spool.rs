mod storage;

use serde::{Deserialize, Serialize};

pub(super) use storage::{load_seen, store_records};

pub(super) const MAX_MATCH_ID: u64 = 10_000_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct SaltRecord {
    pub(super) match_id: u64,
    pub(super) cluster_id: Option<u32>,
    pub(super) metadata_salt: Option<u32>,
    pub(super) replay_salt: Option<u32>,
    pub(super) username: Option<String>,
    #[serde(default)]
    pub(super) collector_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SaltKey {
    pub(super) match_id: u64,
    pub(super) cluster_id: Option<u32>,
    pub(super) metadata_salt: Option<u32>,
    pub(super) replay_salt: Option<u32>,
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

pub(super) fn valid_record(record: &SaltRecord) -> bool {
    record.match_id > 0
        && record.match_id <= MAX_MATCH_ID
        && (record.metadata_salt.is_some() || record.replay_salt.is_some())
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
}
