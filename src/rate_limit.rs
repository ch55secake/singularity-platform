use std::{
    collections::HashMap,
    env,
    time::{Duration, Instant},
};

const DEFAULT_RATE_LIMIT: u32 = 60;
const DEFAULT_RATE_WINDOW_SECONDS: u64 = 60;

#[derive(Debug, Clone, Copy)]
pub(super) struct RateLimit {
    pub(super) max_requests: u32,
    pub(super) window: Duration,
}

#[derive(Debug)]
struct RateWindow {
    started_at: Instant,
    requests: u32,
}

#[derive(Default)]
pub(super) struct Windows {
    by_collector: HashMap<String, RateWindow>,
}

impl Windows {
    pub(super) fn consume(&mut self, collector_id: &str, rate_limit: RateLimit) -> bool {
        let now = Instant::now();
        let window = self
            .by_collector
            .entry(collector_id.to_string())
            .or_insert(RateWindow {
                started_at: now,
                requests: 0,
            });
        if now.duration_since(window.started_at) >= rate_limit.window {
            window.started_at = now;
            window.requests = 0;
        }
        if window.requests >= rate_limit.max_requests {
            return false;
        }
        window.requests += 1;
        true
    }
}

pub(super) fn configured_rate_limit() -> Result<RateLimit, std::io::Error> {
    let max_requests = env::var("SINGULARITY_COLLECTOR_RATE_LIMIT")
        .unwrap_or_else(|_| DEFAULT_RATE_LIMIT.to_string())
        .parse::<u32>()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid rate limit"))?;
    let window_seconds = env::var("SINGULARITY_COLLECTOR_RATE_WINDOW_SECONDS")
        .unwrap_or_else(|_| DEFAULT_RATE_WINDOW_SECONDS.to_string())
        .parse::<u64>()
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid rate window")
        })?;
    if max_requests == 0 || window_seconds == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "rate limit and window must be greater than zero",
        ));
    }
    Ok(RateLimit {
        max_requests,
        window: Duration::from_secs(window_seconds),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limits_are_isolated_per_collector() {
        let mut windows = Windows::default();
        let limit = RateLimit {
            max_requests: 2,
            window: Duration::from_secs(60),
        };

        assert!(windows.consume("collector-a", limit));
        assert!(windows.consume("collector-a", limit));
        assert!(!windows.consume("collector-a", limit));
        assert!(windows.consume("collector-b", limit));
    }
}
