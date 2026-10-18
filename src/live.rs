//! Match start and end reports from collectors.
//!
//! A collector sends a match ID when the game logs a new lobby (the match has
//! started) and again when the player leaves it. The Discord bot reads recent
//! reports to post both teams while the match is on. Once a match has ended,
//! the platform asks the community Deadlock API for its salts, so the match is
//! processed even when nobody opens it in the in-game match history.

mod community_api;
mod state;

use std::{
    collections::BTreeSet,
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
use tracing::{info, warn};

use crate::auth::authenticate;
use crate::{
    AppState, ErrorBody, allow_request, internal_error, now_seconds, read_api,
    salt_spool::{MAX_MATCH_ID, store_records},
};
use state::LogLine;
pub(crate) use state::{LiveMatch, LiveStore};

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
const TICK: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LiveEvent {
    Started,
    Ended,
    /// Written by the platform once salts are stored for the match.
    SaltsStored,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReportBody {
    match_id: u64,
    event: LiveEvent,
    #[serde(default)]
    account_id: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListQuery {
    since: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AcceptedBody {
    accepted: bool,
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
    let base_url = community_api::configured_base_url();
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
        not_before = now + community_api::MIN_REQUEST_GAP_SECONDS;
        info!(match_id, "asking the community API for salts");
        match community_api::request_salts(&http, &base_url, match_id).await {
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
            Err(community_api::RequestError::Busy(wait)) => {
                warn!(
                    match_id,
                    retry_after_seconds = wait,
                    "community API says Steam is busy; waiting before the next salt request"
                );
                not_before = now + wait.max(community_api::MIN_REQUEST_GAP_SECONDS);
                retry_later(&state, match_id, now).await;
            }
            Err(community_api::RequestError::Other(error)) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backs_off_between_attempts() {
        assert_eq!(retry_delay(1), 180);
        assert_eq!(retry_delay(3), 540);
        assert_eq!(retry_delay(10), 900);
    }
}
