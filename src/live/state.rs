use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

use crate::now_seconds;

use super::{
    AFTER_END_SECONDS, ASSUMED_LENGTH_SECONDS, CANCELLED_WITHIN_SECONDS, GIVE_UP_SECONDS,
    LiveEvent, RETENTION_SECONDS,
};

/// One line of the live report log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct LogLine {
    pub(super) match_id: u64,
    pub(super) event: LiveEvent,
    #[serde(default)]
    pub(super) account_id: Option<u32>,
    pub(super) at: u64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub(crate) struct LiveMatch {
    pub(super) match_id: u64,
    pub(super) first_seen: u64,
    pub(super) started_at: Option<u64>,
    pub(super) ended_at: Option<u64>,
    pub(super) account_ids: BTreeSet<u32>,
    pub(super) salts_stored: bool,
    #[serde(skip)]
    pub(super) attempts: u32,
    #[serde(skip)]
    pub(super) next_attempt_at: u64,
}

pub(crate) struct LiveStore {
    path: PathBuf,
    pub(super) matches: BTreeMap<u64, LiveMatch>,
}

impl LiveMatch {
    fn apply(&mut self, line: &LogLine) {
        if self.first_seen == 0 || line.at < self.first_seen {
            self.first_seen = line.at;
        }
        match line.event {
            LiveEvent::Started => {
                self.started_at.get_or_insert(line.at);
            }
            LiveEvent::Ended => self.ended_at = Some(line.at),
            LiveEvent::SaltsStored => self.salts_stored = true,
        }
        if let Some(account_id) = line.account_id {
            self.account_ids.insert(account_id);
        }
    }

    /// When the platform should first ask for this match's salts.
    pub(super) fn salts_due_at(&self) -> u64 {
        let due = match self.ended_at {
            Some(ended) => ended + AFTER_END_SECONDS,
            None => self.first_seen + ASSUMED_LENGTH_SECONDS + AFTER_END_SECONDS,
        };
        due.max(self.next_attempt_at)
    }

    fn cancelled(&self) -> bool {
        match (self.started_at, self.ended_at) {
            (Some(started), Some(ended)) => {
                ended.saturating_sub(started) < CANCELLED_WITHIN_SECONDS
            }
            _ => false,
        }
    }

    pub(super) fn wants_salts(&self, now: u64) -> bool {
        !self.salts_stored
            && !self.cancelled()
            && now < self.first_seen + GIVE_UP_SECONDS
            && now >= self.salts_due_at()
    }
}

impl LiveStore {
    pub(crate) fn empty(path: PathBuf) -> Self {
        Self {
            path,
            matches: BTreeMap::new(),
        }
    }

    /// Loads the report log and rewrites it without reports past retention.
    pub(crate) async fn load(path: &Path) -> Result<Self, std::io::Error> {
        let contents = match fs::read_to_string(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error),
        };
        let cutoff = now_seconds().saturating_sub(RETENTION_SECONDS);
        let lines = contents
            .lines()
            .filter_map(|line| serde_json::from_str::<LogLine>(line).ok())
            .filter(|line| line.at >= cutoff)
            .collect::<Vec<_>>();
        let mut store = Self::empty(path.to_path_buf());
        let mut kept = String::new();
        for line in &lines {
            store.apply(line);
            kept.push_str(&serde_json::to_string(line).map_err(std::io::Error::other)?);
            kept.push('\n');
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(path, kept).await?;
        Ok(store)
    }

    pub(super) fn apply(&mut self, line: &LogLine) {
        self.matches
            .entry(line.match_id)
            .or_insert_with(|| LiveMatch {
                match_id: line.match_id,
                ..LiveMatch::default()
            })
            .apply(line);
    }

    pub(super) async fn record(&mut self, line: LogLine) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        let mut text = serde_json::to_string(&line).map_err(std::io::Error::other)?;
        text.push('\n');
        file.write_all(text.as_bytes()).await?;
        file.flush().await?;
        self.apply(&line);
        Ok(())
    }

    pub(super) fn recent(&self, since: u64) -> Vec<LiveMatch> {
        self.matches
            .values()
            .filter(|live| live.first_seen >= since || live.ended_at.is_some_and(|at| at >= since))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(match_id: u64, event: LiveEvent, account_id: Option<u32>, at: u64) -> LogLine {
        LogLine {
            match_id,
            event,
            account_id,
            at,
        }
    }

    #[test]
    fn folds_reports_into_matches() {
        let mut store = LiveStore::empty(PathBuf::new());
        store.apply(&line(5, LiveEvent::Started, Some(1), 100));
        store.apply(&line(5, LiveEvent::Started, Some(2), 110));
        store.apply(&line(5, LiveEvent::Ended, Some(1), 2_000));

        let live = &store.matches[&5];
        assert_eq!(live.started_at, Some(100));
        assert_eq!(live.ended_at, Some(2_000));
        assert_eq!(live.account_ids, BTreeSet::from([1, 2]));
        assert_eq!(store.recent(1_000).len(), 1);
        assert!(store.recent(3_000).is_empty());
    }

    #[test]
    fn salts_are_requested_after_the_match_ends() {
        let mut live = LiveMatch {
            match_id: 5,
            first_seen: 1_000,
            started_at: Some(1_000),
            ..LiveMatch::default()
        };
        assert!(!live.wants_salts(1_500));
        // No end report: assume the match ends after 45 minutes, then wait an hour.
        assert!(!live.wants_salts(1_000 + ASSUMED_LENGTH_SECONDS + 60 * 60 - 1));
        assert!(live.wants_salts(1_000 + ASSUMED_LENGTH_SECONDS + 60 * 60));

        live.ended_at = Some(2_000);
        assert!(!live.wants_salts(2_030));
        assert!(!live.wants_salts(2_000 + 60 * 60 - 1));
        assert!(live.wants_salts(2_000 + 60 * 60));

        live.next_attempt_at = 7_000;
        assert!(!live.wants_salts(6_000));
        assert!(live.wants_salts(7_000));

        assert!(!live.wants_salts(1_000 + GIVE_UP_SECONDS));
        live.salts_stored = true;
        assert!(!live.wants_salts(3_000));
    }

    #[test]
    fn cancelled_lobbies_are_never_fetched() {
        let live = LiveMatch {
            match_id: 5,
            first_seen: 1_000,
            started_at: Some(1_000),
            ended_at: Some(1_078),
            ..LiveMatch::default()
        };
        assert!(!live.wants_salts(1_000 + GIVE_UP_SECONDS - 1));
        let real = LiveMatch {
            ended_at: Some(1_000 + CANCELLED_WITHIN_SECONDS),
            ..live
        };
        assert!(real.wants_salts(1_000 + CANCELLED_WITHIN_SECONDS + AFTER_END_SECONDS));
    }

    #[tokio::test]
    async fn load_drops_old_reports_and_keeps_recent_ones() {
        let path =
            std::env::temp_dir().join(format!("singularity-live-{}.jsonl", std::process::id()));
        let now = now_seconds();
        let old = line(1, LiveEvent::Started, None, now - RETENTION_SECONDS - 10);
        let recent = line(2, LiveEvent::Started, Some(3), now - 60);
        let contents = format!(
            "{}\n{}\nnot json\n",
            serde_json::to_string(&old).unwrap(),
            serde_json::to_string(&recent).unwrap()
        );
        tokio::fs::write(&path, contents).await.unwrap();

        let mut store = LiveStore::load(&path).await.unwrap();
        assert_eq!(store.matches.keys().copied().collect::<Vec<_>>(), vec![2]);
        store
            .record(line(2, LiveEvent::Ended, Some(3), now))
            .await
            .unwrap();

        let reloaded = LiveStore::load(&path).await.unwrap();
        assert_eq!(reloaded.matches[&2].ended_at, Some(now));
        let _ = tokio::fs::remove_file(&path).await;
    }
}
