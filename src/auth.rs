use axum::http::{HeaderMap, header::AUTHORIZATION};

use crate::{AppState, constant_time_eq};

const LEGACY_COLLECTOR_ID: &str = "legacy-global-token";
const ANONYMOUS_COLLECTOR_ID: &str = "anonymous-development";

pub(super) async fn authenticate(headers: &HeaderMap, state: &AppState) -> Option<String> {
    if !state.authentication_required {
        return Some(ANONYMOUS_COLLECTOR_ID.to_string());
    }
    let token = bearer_token(headers)?;
    if let Some(expected) = state.legacy_token.as_deref()
        && constant_time_eq(token.as_bytes(), expected.as_bytes())
    {
        return Some(LEGACY_COLLECTOR_ID.to_string());
    }
    state.credentials.lock().await.authenticate(token)
}

pub(super) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let mut parts = value.split_ascii_whitespace();
    let scheme = parts.next()?;
    let token = parts.next()?;
    if parts.next().is_some() || !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() {
        return None;
    }
    Some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_authorization_is_rejected_without_parsing_a_token() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Basic not-a-bearer-token".parse().unwrap());

        assert_eq!(bearer_token(&headers), None);
    }
}
