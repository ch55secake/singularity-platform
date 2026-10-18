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

use super::{DEFAULT_LIST_SECONDS, LiveEvent, LiveMatch, LogLine};

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
