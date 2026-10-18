//! Match start and end reports from collectors.
//!
//! A collector sends a match ID when the game logs a new lobby (the match has
//! started) and again when the player leaves it. The Discord bot reads recent
//! reports to post both teams while the match is on. Once a match has ended,
//! the platform asks the community Deadlock API for its salts, so the match is
//! processed even when nobody opens it in the in-game match history.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};
use tracing::{info, warn};

use crate::{
    AppState, ErrorBody, allow_request, authenticate, internal_error, now_seconds, read_api,
    salt_spool::{MAX_MATCH_ID, SaltRecord, store_records},
};

const DEFAULT_DEADLOCK_API_URL: &str = "https://api.deadlock-api.com";
const SALTS_COLLECTOR_ID: &str = "community-api";
/// Reports older than this are dropped on restart.
const RETENTION_SECONDS: u64 = 24 * 60 * 60;
/// The default window for `GET /live`.
const DEFAULT_LIST_SECONDS: u64 = 6 * 60 * 60;
/// Give the local Steam cache an hour to expose the match salts before falling back.
const AFTER_END_SECONDS: u64 = 60 * 60;
/// A lobby destroyed this soon after it formed was cancelled (someone failed
/// to load in), so there is no match to fetch.
pub(crate) const CANCELLED_WITHIN_SECONDS: u64 = 5 * 60;
/// Without an end report, assume a match is over this long after it started.
const ASSUMED_LENGTH_SECONDS: u64 = 45 * 60;
/// Stop asking for a match's salts this long after it was first reported.
const GIVE_UP_SECONDS: u64 = 4 * 60 * 60;
/// Salts that the community API must fetch from Steam are limited to 10 per
/// 30 minutes per IP, so requests are spaced out across all matches.
const MIN_REQUEST_GAP_SECONDS: u64 = 180;
const TICK: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LiveEvent {
    Started,
    Ended,
    /// Written by the platform once salts are stored for the match.
    SaltsStored,
}

/// One line of the live report log.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogLine {
    match_id: u64,
    event: LiveEvent,
    #[serde(default)]
    account_id: Option<u32>,
    at: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReportBody {
    match_id: u64,
    event: LiveEvent,
    #[serde(default)]
    account_id: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub(crate) struct LiveMatch {
    match_id: u64,
    first_seen: u64,
    started_at: Option<u64>,
    ended_at: Option<u64>,
    account_ids: BTreeSet<u32>,
    salts_stored: bool,
    #[serde(skip)]
    attempts: u32,
    #[serde(skip)]
    next_attempt_at: u64,
}

pub(crate) struct LiveStore {
    path: PathBuf,
    matches: BTreeMap<u64, LiveMatch>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListQuery {
    since: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AcceptedBody {
    accepted: bool,
}

#[derive(Debug, Deserialize)]
struct SaltsResponse {
    match_id: u64,
    #[serde(default)]
    cluster_id: Option<u32>,
    #[serde(default)]
    metadata_salt: Option<u32>,
    #[serde(default)]
    replay_salt: Option<u32>,
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
    fn salts_due_at(&self) -> u64 {
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

    fn wants_salts(&self, now: u64) -> bool {
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

    fn apply(&mut self, line: &LogLine) {
        self.matches
            .entry(line.match_id)
            .or_insert_with(|| LiveMatch {
                match_id: line.match_id,
                ..LiveMatch::default()
            })
            .apply(line);
    }

    async fn record(&mut self, line: LogLine) -> Result<(), std::io::Error> {
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

    fn recent(&self, since: u64) -> Vec<LiveMatch> {
        self.matches
            .values()
            .filter(|live| live.first_seen >= since || live.ended_at.is_some_and(|at| at >= since))
            .cloned()
            .collect()
    }
}

/// The log lives next to the salt spool unless SINGULARITY_LIVE_PATH says otherwise.
pub(crate) fn store_path(spool_path: &Path) -> PathBuf {
    env::var("SINGULARITY_LIVE_PATH")
        .ok()
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            spool_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("live.jsonl")
        })
}

/// `POST /ingest/live` from a collector.
pub(crate) async fn report(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ReportBody>,
) -> Result<Json<AcceptedBody>, (StatusCode, Json<ErrorBody>)> {
    let Some(collector_id) = authenticate(&headers, &state).await else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorBody {
                error: "unauthorized",
            }),
        ));
    };
    if !allow_request(&state, &collector_id).await {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorBody {
                error: "collector request rate limit exceeded",
            }),
        ));
    }
    if body.match_id == 0 || body.match_id > MAX_MATCH_ID || body.event == LiveEvent::SaltsStored {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: "invalid live match report",
            }),
        ));
    }
    info!(
        match_id = body.match_id,
        event = ?body.event,
        collector = %collector_id,
        "live match report"
    );
    state
        .live
        .lock()
        .await
        .record(LogLine {
            match_id: body.match_id,
            event: body.event,
            account_id: body.account_id,
            at: now_seconds(),
        })
        .await
        .map_err(|_| internal_error())?;
    Ok(Json(AcceptedBody { accepted: true }))
}

/// `GET /live?since=<unix seconds>` for the Discord bot.
pub(crate) async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<LiveMatch>>, (StatusCode, Json<ErrorBody>)> {
    read_api::authorize(&headers, &state.read_api)?;
    let since = query
        .since
        .unwrap_or_else(|| now_seconds().saturating_sub(DEFAULT_LIST_SECONDS));
    Ok(Json(state.live.lock().await.recent(since)))
}

/// Fetches salts for ended matches until stopped. Off when
/// SINGULARITY_FETCH_SALTS is 0/false/off.
pub(crate) async fn fetch_salts(state: AppState) {
    let enabled = env::var("SINGULARITY_FETCH_SALTS")
        .map(|value| !matches!(value.trim(), "0" | "false" | "no" | "off"))
        .unwrap_or(true);
    if !enabled {
        return;
    }
    let base_url = env::var("SINGULARITY_DEADLOCK_API_URL")
        .unwrap_or_else(|_| DEFAULT_DEADLOCK_API_URL.to_string())
        .trim_end_matches('/')
        .to_string();
    let Ok(http) = Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent("singularity-platform/0.1")
        .build()
    else {
        warn!("salt fetcher could not build an HTTP client");
        return;
    };
    let mut not_before = 0;
    let mut ticker = tokio::time::interval(TICK);
    loop {
        ticker.tick().await;
        let now = now_seconds();
        if now < not_before {
            continue;
        }
        let Some(match_id) = next_match(&state, now).await else {
            continue;
        };
        not_before = now + MIN_REQUEST_GAP_SECONDS;
        info!(match_id, "asking the community API for salts");
        match request_salts(&http, &base_url, match_id).await {
            Ok(Some(record)) => {
                let stored = store_records(&state, vec![record]).await;
                match stored {
                    Ok(_) => {
                        info!(match_id, "stored salts from the community API");
                        mark_stored(&state, match_id).await;
                    }
                    Err(_) => warn!(match_id, "could not store fetched salts"),
                }
            }
            Ok(None) => {
                info!(match_id, "community API has no salts yet; will retry");
                retry_later(&state, match_id, now).await;
            }
            Err(RequestError::Busy(wait)) => {
                warn!(
                    match_id,
                    retry_after_seconds = wait,
                    "community API says Steam is busy; waiting before the next salt request"
                );
                not_before = now + wait.max(MIN_REQUEST_GAP_SECONDS);
                retry_later(&state, match_id, now).await;
            }
            Err(RequestError::Other(error)) => {
                warn!(match_id, %error, "salt request failed");
                retry_later(&state, match_id, now).await;
            }
        }
    }
}

/// The oldest due match, after marking matches the collectors already sent.
async fn next_match(state: &AppState, now: u64) -> Option<u64> {
    let have_salts = {
        let seen = state.seen.lock().await;
        seen.iter()
            .filter(|key| key.metadata_salt.is_some())
            .map(|key| key.match_id)
            .collect::<BTreeSet<_>>()
    };
    let mut live = state.live.lock().await;
    let already = live
        .matches
        .values()
        .filter(|live| !live.salts_stored && have_salts.contains(&live.match_id))
        .map(|live| live.match_id)
        .collect::<Vec<_>>();
    for match_id in already {
        if let Err(error) = live
            .record(LogLine {
                match_id,
                event: LiveEvent::SaltsStored,
                account_id: None,
                at: now,
            })
            .await
        {
            warn!(match_id, %error, "could not record stored salts");
        }
    }
    live.matches
        .values()
        .filter(|live| live.wants_salts(now))
        .min_by_key(|live| live.salts_due_at())
        .map(|live| live.match_id)
}

async fn mark_stored(state: &AppState, match_id: u64) {
    let result = state
        .live
        .lock()
        .await
        .record(LogLine {
            match_id,
            event: LiveEvent::SaltsStored,
            account_id: None,
            at: now_seconds(),
        })
        .await;
    if let Err(error) = result {
        warn!(match_id, %error, "could not record stored salts");
    }
}

/// Backs off from 3 minutes to 15 minutes between attempts for one match.
async fn retry_later(state: &AppState, match_id: u64, now: u64) {
    if let Some(live) = state.live.lock().await.matches.get_mut(&match_id) {
        live.attempts += 1;
        live.next_attempt_at = now + retry_delay(live.attempts);
    }
}

fn retry_delay(attempts: u32) -> u64 {
    (180 * u64::from(attempts)).min(900)
}

enum RequestError {
    /// The API asked us to wait this many seconds.
    Busy(u64),
    Other(String),
}

async fn request_salts(
    http: &Client,
    base_url: &str,
    match_id: u64,
) -> Result<Option<SaltRecord>, RequestError> {
    let response = http
        .get(format!("{base_url}/v1/matches/{match_id}/salts"))
        .send()
        .await
        .map_err(|error| RequestError::Other(error.to_string()))?;
    let status = response.status();
    if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
        let wait = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(MIN_REQUEST_GAP_SECONDS);
        return Err(RequestError::Busy(wait));
    }
    if status == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !status.is_success() {
        return Err(RequestError::Other(format!("HTTP {status}")));
    }
    let body = response
        .bytes()
        .await
        .map_err(|error| RequestError::Other(error.to_string()))?;
    let salts = serde_json::from_slice::<SaltsResponse>(&body)
        .map_err(|error| RequestError::Other(error.to_string()))?;
    Ok(salts_record(salts, match_id))
}

fn salts_record(salts: SaltsResponse, match_id: u64) -> Option<SaltRecord> {
    if salts.match_id != match_id || salts.metadata_salt.is_none() {
        return None;
    }
    Some(SaltRecord {
        match_id,
        cluster_id: salts.cluster_id,
        metadata_salt: salts.metadata_salt,
        replay_salt: salts.replay_salt,
        username: None,
        collector_id: Some(SALTS_COLLECTOR_ID.to_string()),
    })
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

    #[test]
    fn backs_off_between_attempts() {
        assert_eq!(retry_delay(1), 180);
        assert_eq!(retry_delay(3), 540);
        assert_eq!(retry_delay(10), 900);
    }

    #[test]
    fn only_complete_salts_become_records() {
        let salts = |match_id, metadata_salt| SaltsResponse {
            match_id,
            cluster_id: Some(390),
            metadata_salt,
            replay_salt: Some(9),
        };
        let record = salts_record(salts(5, Some(7)), 5).unwrap();
        assert_eq!(record.metadata_salt, Some(7));
        assert_eq!(record.collector_id.as_deref(), Some(SALTS_COLLECTOR_ID));
        assert!(salts_record(salts(5, None), 5).is_none());
        assert!(salts_record(salts(6, Some(7)), 5).is_none());
    }

    #[tokio::test]
    async fn load_drops_old_reports_and_keeps_recent_ones() {
        let path = env::temp_dir().join(format!("singularity-live-{}.jsonl", std::process::id()));
        let now = now_seconds();
        let old = line(1, LiveEvent::Started, None, now - RETENTION_SECONDS - 10);
        let recent = line(2, LiveEvent::Started, Some(3), now - 60);
        let contents = format!(
            "{}\n{}\nnot json\n",
            serde_json::to_string(&old).unwrap(),
            serde_json::to_string(&recent).unwrap()
        );
        fs::write(&path, contents).await.unwrap();

        let mut store = LiveStore::load(&path).await.unwrap();
        assert_eq!(store.matches.keys().copied().collect::<Vec<_>>(), vec![2]);
        store
            .record(line(2, LiveEvent::Ended, Some(3), now))
            .await
            .unwrap();

        let reloaded = LiveStore::load(&path).await.unwrap();
        assert_eq!(reloaded.matches[&2].ended_at, Some(now));
        let _ = fs::remove_file(&path).await;
    }
}
