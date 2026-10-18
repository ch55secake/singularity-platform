use std::{collections::BTreeSet, env, time::Duration};

use reqwest::Client;
use tracing::{info, warn};

use crate::{AppState, now_seconds, salt_spool::store_records};

use super::{LiveEvent, LogLine, community_api};

const TICK: Duration = Duration::from_secs(30);

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
