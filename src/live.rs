//! Match start and end reports from collectors.
//!
//! A collector sends a match ID when the game logs a new lobby (the match has
//! started) and again when the player leaves it. The Discord bot reads recent
//! reports to post both teams while the match is on. Once a match has ended,
//! the platform asks the community Deadlock API for its salts, so the match is
//! processed even when nobody opens it in the in-game match history.

mod community_api;
mod state;
mod worker;

use std::{
    env,
    path::{Path, PathBuf},
};

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::auth::authenticate;
use crate::{
    AppState, ErrorBody, allow_request, internal_error, now_seconds, read_api,
    salt_spool::MAX_MATCH_ID,
};
use state::LogLine;
pub(crate) use state::{LiveMatch, LiveStore};
pub(crate) use worker::fetch_salts;

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
