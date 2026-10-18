use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

const PARSE_STATE_VERSION: u32 = 3;

#[derive(Debug, Serialize, Deserialize)]
struct ParseStateFile {
    version: u32,
    #[serde(default)]
    processed: BTreeMap<String, u64>,
    #[serde(default)]
    public_ids: BTreeMap<u64, String>,
    #[serde(default)]
    reprocess_existing_matches: bool,
    #[serde(default)]
    reprocessed_matches: BTreeSet<u64>,
}

pub(super) struct ParseState {
    path: PathBuf,
    pub(super) processed: BTreeMap<String, u64>,
    pub(super) public_ids: BTreeMap<u64, String>,
    pub(super) reprocess_existing_matches: bool,
    pub(super) reprocessed_matches: BTreeSet<u64>,
}

pub(super) fn should_skip_duplicate_match(
    match_id: u64,
    processed_this_run: &HashSet<u64>,
    reprocessed_matches: &BTreeSet<u64>,
    exists_in_clickhouse: bool,
    reprocess_existing_matches: bool,
) -> bool {
    processed_this_run.contains(&match_id)
        || reprocessed_matches.contains(&match_id)
        || (exists_in_clickhouse && !reprocess_existing_matches)
}

impl ParseState {
    pub(super) async fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = match fs::read(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if contents.is_empty() {
            return Ok(Self {
                path: path.to_path_buf(),
                processed: BTreeMap::new(),
                public_ids: BTreeMap::new(),
                reprocess_existing_matches: false,
                reprocessed_matches: BTreeSet::new(),
            });
        }
        #[cfg(unix)]
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
        let file: ParseStateFile = serde_json::from_slice(&contents)?;
        let (processed, reprocess_existing_matches, reprocessed_matches) = match file.version {
            1 => (BTreeMap::new(), true, BTreeSet::new()),
            2 => (file.processed, false, BTreeSet::new()),
            PARSE_STATE_VERSION => (
                file.processed,
                file.reprocess_existing_matches,
                file.reprocessed_matches,
            ),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unsupported parser state version",
                )
                .into());
            }
        };
        Ok(Self {
            path: path.to_path_buf(),
            processed,
            public_ids: file.public_ids,
            reprocess_existing_matches,
            reprocessed_matches,
        })
    }

    pub(super) async fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).await?;
        }
        let contents = serde_json::to_vec_pretty(&ParseStateFile {
            version: PARSE_STATE_VERSION,
            processed: self.processed.clone(),
            public_ids: self.public_ids.clone(),
            reprocess_existing_matches: self.reprocess_existing_matches,
            reprocessed_matches: self.reprocessed_matches.clone(),
        })?;
        let temp_path = self.path.with_file_name(format!(
            ".{}.tmp-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("metadata-parse-state.json"),
            std::process::id()
        ));
        let mut temp = OpenOptions::new()
            .create(true)
            .truncate(true)
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

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[tokio::test]
    async fn upgrades_old_parse_state_to_backfill_new_metadata_events() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "singularity-parser-state-{}-{nonce}.json",
            std::process::id()
        ));
        let previous = ParseStateFile {
            version: 1,
            processed: BTreeMap::from([("processed-job".to_string(), 42)]),
            public_ids: BTreeMap::from([(42, "public-match-id".to_string())]),
            reprocess_existing_matches: false,
            reprocessed_matches: BTreeSet::new(),
        };
        fs::write(&path, serde_json::to_vec(&previous).unwrap())
            .await
            .unwrap();

        let mut state = ParseState::load(&path).await.unwrap();

        assert!(state.processed.is_empty());
        assert!(state.reprocess_existing_matches);
        assert!(state.reprocessed_matches.is_empty());
        assert_eq!(
            state.public_ids.get(&42).map(String::as_str),
            Some("public-match-id")
        );
        state.reprocessed_matches.insert(43);
        state.save().await.unwrap();
        let saved: ParseStateFile =
            serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
        assert_eq!(saved.version, PARSE_STATE_VERSION);
        assert!(saved.processed.is_empty());
        assert!(saved.reprocess_existing_matches);
        assert!(saved.reprocessed_matches.contains(&43));
        let resumed = ParseState::load(&path).await.unwrap();
        assert!(resumed.reprocess_existing_matches);
        assert!(resumed.reprocessed_matches.contains(&43));
        fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn migrates_current_parse_state_without_forcing_a_reparse() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "singularity-parser-state-v2-{}-{nonce}.json",
            std::process::id()
        ));
        let previous = ParseStateFile {
            version: 2,
            processed: BTreeMap::from([("processed-job".to_string(), 42)]),
            public_ids: BTreeMap::from([(42, "public-match-id".to_string())]),
            reprocess_existing_matches: false,
            reprocessed_matches: BTreeSet::new(),
        };
        fs::write(&path, serde_json::to_vec(&previous).unwrap())
            .await
            .unwrap();

        let state = ParseState::load(&path).await.unwrap();

        assert_eq!(state.processed.get("processed-job"), Some(&42));
        assert!(!state.reprocess_existing_matches);
        assert!(state.reprocessed_matches.is_empty());
        fs::remove_file(path).await.unwrap();
    }

    #[test]
    fn skips_processed_matches_but_allows_the_one_time_backfill() {
        let processed_this_run = HashSet::new();
        let no_backfill_matches = BTreeSet::new();

        assert!(should_skip_duplicate_match(
            42,
            &HashSet::from([42]),
            &no_backfill_matches,
            false,
            true,
        ));
        assert!(should_skip_duplicate_match(
            42,
            &processed_this_run,
            &no_backfill_matches,
            true,
            false,
        ));
        assert!(!should_skip_duplicate_match(
            42,
            &processed_this_run,
            &no_backfill_matches,
            true,
            true,
        ));
        assert!(!should_skip_duplicate_match(
            42,
            &processed_this_run,
            &no_backfill_matches,
            false,
            false,
        ));
        assert!(should_skip_duplicate_match(
            42,
            &processed_this_run,
            &BTreeSet::from([42]),
            false,
            true,
        ));
    }
}
