use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ReplayRecord {
    pub(super) match_id: u64,
    pub(super) cluster_id: Option<u32>,
    pub(super) replay_salt: Option<u32>,
}

impl ReplayRecord {
    pub(super) fn is_downloadable(&self) -> bool {
        self.match_id > 0
            && self.cluster_id.is_some_and(|cluster| cluster > 0)
            && self.replay_salt.is_some_and(|salt| salt > 0)
    }
}

pub(super) fn job_id(record: &ReplayRecord) -> String {
    let mut digest = Sha256::new();
    digest.update(record.match_id.to_le_bytes());
    digest.update(record.cluster_id.unwrap_or_default().to_le_bytes());
    digest.update(record.replay_salt.unwrap_or_default().to_le_bytes());
    hex::encode(digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> ReplayRecord {
        ReplayRecord {
            match_id: 113390164,
            cluster_id: Some(123),
            replay_salt: Some(456),
        }
    }

    #[test]
    fn job_id_does_not_expose_the_salt() {
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
