use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};

use crate::{AppState, ingest, live, read_api, server_config::MAX_REQUEST_BODY_BYTES};

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "service": "singularity-platform",
        "status": "ok",
        "baseline": "v0.1.0",
    }))
}

pub(super) fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ingest/salts", post(ingest::ingest_salts))
        .route("/ingest/live", post(live::report))
        // Not under /api: the web app proxies /api to browsers, and this list
        // carries Steam account IDs for the Discord bot.
        .route("/live", get(live::list))
        .route("/matches", get(read_api::list_matches))
        .route("/lane-pairings", get(read_api::get_lane_pairings))
        .route("/sessions", get(read_api::list_sessions))
        .route("/events/matches", get(read_api::list_processed_events))
        .route("/matches/{identifier}", get(read_api::get_match))
        .route(
            "/matches/{identifier}/positions",
            get(read_api::get_positions),
        )
        .route("/matches/{identifier}/deaths", get(read_api::get_deaths))
        .route(
            "/matches/{identifier}/lane-stats",
            get(read_api::get_lane_stats),
        )
        .route("/api/matches", get(read_api::list_matches))
        .route("/api/lane-pairings", get(read_api::get_lane_pairings))
        .route("/api/sessions", get(read_api::list_sessions))
        .route("/api/events/matches", get(read_api::list_processed_events))
        .route("/api/matches/{identifier}", get(read_api::get_match))
        .route(
            "/api/matches/{identifier}/positions",
            get(read_api::get_positions),
        )
        .route(
            "/api/matches/{identifier}/deaths",
            get(read_api::get_deaths),
        )
        .route(
            "/api/matches/{identifier}/lane-stats",
            get(read_api::get_lane_stats),
        )
        .route("/players/{account_id}", get(read_api::get_player))
        .route("/api/players/{account_id}", get(read_api::get_player))
        .route("/players/by-name/{name}", get(read_api::get_player_by_name))
        .route(
            "/api/players/by-name/{name}",
            get(read_api::get_player_by_name),
        )
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}
