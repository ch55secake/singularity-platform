use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use serde::Serialize;

use crate::{AppState, ErrorBody};

use super::{
    ReadApiState, api_error, authorize, load_core, match_selector, number_u64, query_rows,
    read_backend_error, string_value,
};

const REQUEST_COOLDOWN_SECONDS: u64 = 60 * 60;
const REQUEST_COLUMNS: &str =
    "match_id, request_id, requested_at, status, error_code, model_version";

#[derive(Debug, Clone, Serialize)]
pub(super) struct ReparseRequestRow {
    pub(super) match_id: u64,
    pub(super) request_id: u64,
    pub(super) requested_at: u64,
    pub(super) status: String,
    pub(super) error_code: String,
    pub(super) model_version: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReparseStatus {
    status: String,
    requested_at: Option<u64>,
    error_code: Option<String>,
}

impl ReparseStatus {
    fn idle() -> Self {
        Self {
            status: "idle".to_string(),
            requested_at: None,
            error_code: None,
        }
    }
}

pub(crate) async fn get_resource_reparse_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
) -> Result<Json<ReparseStatus>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let selector = match_selector(&identifier)?;
    let Some(core) = load_core(&state.read_api, &selector).await? else {
        return Err(api_error(StatusCode::NOT_FOUND, "match not found"));
    };
    Ok(Json(latest_status(&state.read_api, core.match_id).await?))
}

pub(crate) async fn request_resource_reparse(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
) -> Result<(StatusCode, Json<ReparseStatus>), (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let selector = match_selector(&identifier)?;
    let Some(core) = load_core(&state.read_api, &selector).await? else {
        return Err(api_error(StatusCode::NOT_FOUND, "match not found"));
    };

    let previous = latest_status(&state.read_api, core.match_id).await?;
    let now = now_seconds();
    if previous
        .requested_at
        .is_some_and(|requested_at| now.saturating_sub(requested_at) < REQUEST_COOLDOWN_SECONDS)
    {
        let response_status = if previous.status == "pending" {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        };
        return Ok((response_status, Json(previous)));
    }

    let request_id = now_nanos();
    let row = ReparseRequestRow {
        match_id: core.match_id,
        request_id,
        requested_at: now,
        status: "pending".to_string(),
        error_code: String::new(),
        model_version: request_id,
    };
    let sql = format!(
        "INSERT INTO {}.replay_reparse_requests ({REQUEST_COLUMNS}) FORMAT JSONEachRow",
        state.read_api.database,
    );
    state
        .read_api
        .insert(&sql, std::slice::from_ref(&row))
        .await
        .map_err(read_backend_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(ReparseStatus {
            status: "pending".to_string(),
            requested_at: Some(now),
            error_code: None,
        }),
    ))
}

async fn latest_status(
    state: &ReadApiState,
    match_id: u64,
) -> Result<ReparseStatus, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT argMax(status, model_version) AS status, argMax(requested_at, model_version) AS requested_at, argMax(error_code, model_version) AS error_code FROM {db}.replay_reparse_requests WHERE match_id = {match_id} GROUP BY match_id",
        db = state.database,
    );
    let Some(row) = query_rows(state, &sql).await?.into_iter().next() else {
        return Ok(ReparseStatus::idle());
    };
    let requested_at = number_u64(&row, "requested_at");
    if requested_at.is_none() {
        return Ok(ReparseStatus::idle());
    }
    Ok(ReparseStatus {
        status: string_value(&row, "status").unwrap_or_else(|| "pending".to_string()),
        requested_at,
        error_code: string_value(&row, "error_code").filter(|code| !code.is_empty()),
    })
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}
