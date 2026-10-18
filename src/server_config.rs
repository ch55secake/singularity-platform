use std::env;

pub(super) const MAX_BATCH_SIZE: usize = 1_000;
pub(super) const MAX_REQUEST_BODY_BYTES: usize = 512 * 1024;

pub(super) fn non_empty_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

pub(super) fn configured_batch_size() -> Result<usize, std::io::Error> {
    let value = env::var("SINGULARITY_COLLECTOR_MAX_BATCH_SIZE")
        .unwrap_or_else(|_| MAX_BATCH_SIZE.to_string())
        .parse::<usize>()
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid batch limit")
        })?;
    if value == 0 || value > MAX_BATCH_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "batch limit must be between 1 and 1000",
        ));
    }
    Ok(value)
}
