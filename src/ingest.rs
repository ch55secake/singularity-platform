use axum::{Json, extract::State, http::StatusCode};
use serde::Serialize;

use crate::{
    AppState, ErrorBody, allow_request, auth,
    salt_spool::{self, SaltRecord},
};

#[derive(Debug, Serialize)]
pub(super) struct IngestBody {
    pub(super) accepted: usize,
    pub(super) duplicates: usize,
}

pub(super) async fn ingest_salts(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(mut records): Json<Vec<SaltRecord>>,
) -> Result<Json<IngestBody>, (StatusCode, Json<ErrorBody>)> {
    let Some(collector_id) = auth::authenticate(&headers, &state).await else {
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
    if records.is_empty() || records.len() > state.max_batch_size {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: "batch exceeds the configured collector limit",
            }),
        ));
    }

    for record in &records {
        if !salt_spool::valid_record(record) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: "invalid match salt record",
                }),
            ));
        }
    }

    for record in &mut records {
        record.collector_id = Some(collector_id.clone());
    }

    let (accepted, duplicates) = salt_spool::store_records(&state, records).await?;
    Ok(Json(IngestBody {
        accepted,
        duplicates,
    }))
}
