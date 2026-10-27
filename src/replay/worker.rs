use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::fs;

use super::{
    clickhouse::{
        ClickHouseClient, REPLAY_MATCH_COLUMNS, REPLAY_REPARSE_REQUEST_COLUMNS,
        REPLAY_RESOURCE_COLUMNS, ReplayMatchRow,
    },
    config::ReplayConfig,
    download::download_replay,
    parser::{MODEL_VERSION, PARSER_VERSION, parse_replay},
    record::{ReplayRecord, job_id},
    state::{ReplayJob, ReplayState, ReplayStatus},
};

const MAX_PARSE_FAILURES: u32 = 3;

fn parse_failure_status(parse_failures: u32) -> ReplayStatus {
    if parse_failures >= MAX_PARSE_FAILURES {
        ReplayStatus::Failed
    } else {
        ReplayStatus::Pending
    }
}

pub(super) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReplayConfig::from_env()?;
    if let Some(parent) = config
        .state_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).await?;
    }
    fs::create_dir_all(&config.output_dir).await?;

    let state_path = config.state_path.clone();
    let mut state = ReplayState::load(&state_path).await?;
    let client = Client::builder()
        .timeout(config.timeout)
        .user_agent("singularity-platform-replay-worker/0.1")
        .build()?;
    let clickhouse = ClickHouseClient::from_env(&config.timeout)?;
    let spool = match fs::read_to_string(&config.spool_path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };

    let records: Vec<ReplayRecord> = spool
        .lines()
        .filter_map(|line| serde_json::from_str::<ReplayRecord>(line).ok())
        .filter(ReplayRecord::is_downloadable)
        .collect();
    let records_by_match: HashMap<u64, &ReplayRecord> = records
        .iter()
        .map(|record| (record.match_id, record))
        .collect();

    let mut summary = WorkerSummary::default();
    let mut processed_job_ids = HashSet::new();
    let requests = pending_resource_reparses(&clickhouse).await?;
    for request in requests {
        let Some(record) = records_by_match.get(&request.match_id).copied() else {
            update_reparse_request(&clickhouse, &request, "unavailable", "replay_not_available")
                .await?;
            continue;
        };
        let id = job_id(record);
        if processed_job_ids.insert(id.clone()) {
            process_record(
                &mut state,
                &client,
                &clickhouse,
                &config,
                ReplayProcess {
                    job_id: &id,
                    record,
                    force_reparse: true,
                },
                &mut summary,
            )
            .await?;
        }
        if state.jobs.get(&id).is_some_and(|job| {
            job.status == ReplayStatus::Parsed
                && job.parser_version.as_deref() == Some(PARSER_VERSION)
        }) {
            update_reparse_request(&clickhouse, &request, "completed", "").await?;
        } else if let Some(job) = state
            .jobs
            .get(&id)
            .filter(|job| job.status == ReplayStatus::Failed)
        {
            update_reparse_request(
                &clickhouse,
                &request,
                "failed",
                job.last_error.as_deref().unwrap_or("parser_failed"),
            )
            .await?;
        }
    }

    for record in &records {
        let id = job_id(record);
        if !processed_job_ids.insert(id.clone()) {
            continue;
        }
        summary.discovered += 1;
        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            ReplayProcess {
                job_id: &id,
                record,
                force_reparse: false,
            },
            &mut summary,
        )
        .await?;
    }

    state.save().await?;
    let disk_bytes = disk_usage(&config.output_dir).await?;
    println!(
        "replay worker: discovered={} downloaded={} parsed={} deferred={} failed={} disk_bytes={} disk_limit_bytes={}",
        summary.discovered,
        summary.downloaded,
        summary.parsed,
        summary.deferred,
        summary.failed,
        disk_bytes,
        config.max_disk_bytes,
    );
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
struct PendingResourceReparse {
    match_id: u64,
    request_id: u64,
    requested_at: u64,
}

#[derive(Debug, Serialize)]
struct ResourceReparseStatusRow {
    match_id: u64,
    request_id: u64,
    requested_at: u64,
    status: String,
    error_code: String,
    model_version: u64,
}

async fn pending_resource_reparses(
    clickhouse: &ClickHouseClient,
) -> Result<Vec<PendingResourceReparse>, Box<dyn std::error::Error>> {
    let sql = pending_resource_reparse_query(&clickhouse.database);
    clickhouse
        .query_rows(&sql)
        .await?
        .into_iter()
        .map(|row| Ok(serde_json::from_value(row)?))
        .collect()
}

fn pending_resource_reparse_query(database: &str) -> String {
    format!(
        "SELECT match_id, request_id, requested_at FROM (SELECT match_id, request_id, argMax(requested_at, model_version) AS requested_at, argMax(status, model_version) AS status FROM {database}.replay_reparse_requests GROUP BY match_id, request_id) WHERE status = 'pending' ORDER BY requested_at LIMIT 100"
    )
}

async fn update_reparse_request(
    clickhouse: &ClickHouseClient,
    request: &PendingResourceReparse,
    status: &str,
    error_code: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = ResourceReparseStatusRow {
        match_id: request.match_id,
        request_id: request.request_id,
        requested_at: request.requested_at,
        status: status.to_string(),
        error_code: error_code.to_string(),
        model_version: now_model_version().max(request.request_id.saturating_add(1)),
    };
    clickhouse
        .insert(
            "replay_reparse_requests",
            REPLAY_REPARSE_REQUEST_COLUMNS,
            std::slice::from_ref(&row),
        )
        .await
}

fn now_model_version() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

#[derive(Default)]
pub(super) struct WorkerSummary {
    pub(super) discovered: usize,
    pub(super) downloaded: usize,
    pub(super) parsed: usize,
    pub(super) deferred: usize,
    pub(super) failed: usize,
}

pub(super) struct ReplayProcess<'a> {
    pub(super) job_id: &'a str,
    pub(super) record: &'a ReplayRecord,
    pub(super) force_reparse: bool,
}

pub(super) async fn process_record(
    state: &mut ReplayState,
    client: &Client,
    clickhouse: &ClickHouseClient,
    config: &ReplayConfig,
    process: ReplayProcess<'_>,
    summary: &mut WorkerSummary,
) -> Result<(), Box<dyn std::error::Error>> {
    let ReplayProcess {
        job_id,
        record,
        force_reparse,
    } = process;
    let raw_path = config.output_dir.join(format!("{job_id}.dem"));
    let now = now_seconds();
    let existing = state.jobs.get(job_id).cloned();
    if let Some(job) = existing.as_ref()
        && job.status == ReplayStatus::Failed
        && job.parser_version.as_deref() == Some(PARSER_VERSION)
        && !force_reparse
    {
        remove_raw_file(job.raw_path.as_deref()).await?;
        return Ok(());
    }

    // Older state files kept parser-rejected demos indefinitely so the same
    // deterministic failure was retried forever. The salt remains in the
    // input spool, so discard that raw file and retry from a fresh download.
    if let Some(job) = existing.as_ref()
        && job.status == ReplayStatus::Downloaded
        && job.last_error.as_deref().is_some_and(is_parse_error)
    {
        remove_raw_file(job.raw_path.as_deref()).await?;
        state.jobs.insert(
            job_id.to_string(),
            ReplayJob {
                status: ReplayStatus::Pending,
                attempts: job.attempts,
                next_retry_at: job.next_retry_at,
                last_error: job.last_error.clone(),
                raw_path: None,
                position_count: None,
                parser_version: Some(PARSER_VERSION.to_string()),
                raw_retention_until: None,
                parse_failures: 0,
            },
        );
        state.save().await?;
    }

    let previous_parser_version = existing
        .as_ref()
        .and_then(|job| job.parser_version.as_deref());
    let parse_failures = existing.as_ref().map_or(0, |job| job.parse_failures);
    let parse_failures = if force_reparse
        || previous_parser_version.is_some_and(|version| version != PARSER_VERSION)
    {
        0
    } else {
        parse_failures
    };

    if existing
        .as_ref()
        .is_some_and(|job| job.status == ReplayStatus::Parsed)
    {
        let job = existing.as_ref().expect("parsed job exists");
        if job.parser_version.as_deref() == Some(PARSER_VERSION) && !force_reparse {
            if config.debug_retention_seconds == 0
                || job
                    .raw_retention_until
                    .is_some_and(|retention_until| retention_until <= now)
            {
                remove_raw_file(job.raw_path.as_deref()).await?;
            }
            return Ok(());
        }

        if let Some(path) = job.raw_path.as_deref()
            && fs::try_exists(path).await?
        {
            state.jobs.insert(
                job_id.to_string(),
                ReplayJob {
                    status: ReplayStatus::Downloaded,
                    attempts: job.attempts,
                    next_retry_at: None,
                    last_error: Some("parser_version_mismatch".to_string()),
                    raw_path: Some(path.to_string()),
                    position_count: None,
                    parser_version: None,
                    raw_retention_until: None,
                    parse_failures,
                },
            );
        }
    }
    if existing
        .as_ref()
        .and_then(|job| job.next_retry_at)
        .is_some_and(|retry_at| retry_at > now)
    {
        summary.deferred += 1;
        return Ok(());
    }

    let attempt = existing
        .as_ref()
        .map_or(1, |job| job.attempts.saturating_add(1));
    if !fs::try_exists(&raw_path).await? {
        if existing.is_none() {
            state.jobs.insert(
                job_id.to_string(),
                ReplayJob {
                    status: ReplayStatus::Pending,
                    attempts: 0,
                    next_retry_at: None,
                    last_error: None,
                    raw_path: None,
                    position_count: None,
                    parser_version: None,
                    raw_retention_until: None,
                    parse_failures,
                },
            );
            state.save().await?;
        }
        if disk_usage(&config.output_dir)
            .await?
            .saturating_add(config.max_compressed_bytes)
            .saturating_add(config.max_decompressed_bytes)
            > config.max_disk_bytes
        {
            record_failure(
                state,
                job_id,
                ReplayStatus::Pending,
                attempt,
                "disk_limit",
                None,
                config,
            )
            .await?;
            summary.failed += 1;
            return Ok(());
        }
        match download_replay(client, config, record, &raw_path).await {
            Ok(()) => {
                summary.downloaded += 1;
                state.jobs.insert(
                    job_id.to_string(),
                    ReplayJob {
                        status: ReplayStatus::Downloaded,
                        attempts: attempt,
                        next_retry_at: None,
                        last_error: None,
                        raw_path: Some(raw_path.display().to_string()),
                        position_count: None,
                        parser_version: None,
                        raw_retention_until: None,
                        parse_failures,
                    },
                );
                state.save().await?;
            }
            Err(error) => {
                record_failure(
                    state,
                    job_id,
                    ReplayStatus::Pending,
                    attempt,
                    error.code,
                    None,
                    config,
                )
                .await?;
                summary.failed += 1;
                return Ok(());
            }
        }
    }

    let model_version = MODEL_VERSION;
    let parsed = match parse_replay(record.match_id, &raw_path, model_version) {
        Ok(parsed) => parsed,
        Err(error) => {
            let parse_failures = parse_failures.saturating_add(1);
            let status = parse_failure_status(parse_failures);
            let failed_permanently = status == ReplayStatus::Failed;
            remove_raw_file(Some(&raw_path.display().to_string())).await?;
            state.jobs.insert(
                job_id.to_string(),
                ReplayJob {
                    status,
                    attempts: attempt,
                    next_retry_at: (!failed_permanently)
                        .then(|| now_seconds().saturating_add(retry_delay(config, attempt))),
                    last_error: Some(error.to_string()),
                    raw_path: None,
                    position_count: None,
                    parser_version: Some(PARSER_VERSION.to_string()),
                    raw_retention_until: None,
                    parse_failures,
                },
            );
            state.save().await?;
            summary.failed += 1;
            return Ok(());
        }
    };

    let match_row = ReplayMatchRow {
        match_id: record.match_id,
        replay_job_id: job_id.to_string(),
        parser_version: PARSER_VERSION.to_string(),
        parsed_at: now,
        position_count: parsed.positions.len() as u32,
        model_version,
    };
    clickhouse
        .insert("replay_matches", REPLAY_MATCH_COLUMNS, &[match_row])
        .await?;
    clickhouse
        .insert(
            "replay_resources",
            REPLAY_RESOURCE_COLUMNS,
            &parsed.resources,
        )
        .await?;

    state.jobs.insert(
        job_id.to_string(),
        ReplayJob {
            status: ReplayStatus::Parsed,
            attempts: attempt,
            next_retry_at: None,
            last_error: None,
            raw_path: Some(raw_path.display().to_string()),
            position_count: Some(parsed.positions.len() as u32),
            parser_version: Some(PARSER_VERSION.to_string()),
            raw_retention_until: (config.debug_retention_seconds > 0)
                .then(|| now.saturating_add(config.debug_retention_seconds)),
            parse_failures: 0,
        },
    );
    state.save().await?;
    if config.debug_retention_seconds == 0 {
        remove_raw_file(Some(&raw_path.display().to_string())).await?;
    }
    summary.parsed += 1;
    Ok(())
}

async fn remove_raw_file(raw_path: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let Some(raw_path) = raw_path else {
        return Ok(());
    };
    match fs::remove_file(raw_path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn record_failure(
    state: &mut ReplayState,
    job_id: &str,
    status: ReplayStatus,
    attempts: u32,
    error: &str,
    raw_path: Option<String>,
    config: &ReplayConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let parse_failures = state.jobs.get(job_id).map_or(0, |job| job.parse_failures);
    state.jobs.insert(
        job_id.to_string(),
        ReplayJob {
            status,
            attempts,
            next_retry_at: Some(now_seconds().saturating_add(retry_delay(config, attempts))),
            last_error: Some(error.to_string()),
            raw_path,
            position_count: None,
            parser_version: None,
            raw_retention_until: None,
            parse_failures,
        },
    );
    state.save().await?;
    Ok(())
}

fn is_parse_error(code: &str) -> bool {
    matches!(
        code,
        "parser_input_failed" | "invalid_demo" | "parser_failed" | "parser_state_failed"
    )
}

pub(super) async fn disk_usage(path: &Path) -> Result<u64, std::io::Error> {
    let mut total: u64 = 0;
    let mut entries = match fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    while let Some(entry) = entries.next_entry().await? {
        let metadata = entry.metadata().await?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn retry_delay(config: &ReplayConfig, attempt: u32) -> u64 {
    let exponent = attempt.saturating_sub(1).min(16);
    let multiplier = 1_u64 << exponent;
    config
        .retry_base_seconds
        .saturating_mul(multiplier)
        .min(config.retry_max_seconds)
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::{ReplayStatus, parse_failure_status, pending_resource_reparse_query};

    #[test]
    fn pending_reparse_query_selects_latest_status_per_request() {
        let query = pending_resource_reparse_query("my_db");

        assert!(query.contains("FROM my_db.replay_reparse_requests"));
        assert!(query.contains("argMax(status, model_version) AS status"));
        assert!(query.contains("WHERE status = 'pending'"));
        assert!(query.ends_with("ORDER BY requested_at LIMIT 100"));
    }

    #[test]
    fn parser_failures_become_terminal_after_three_attempts() {
        assert_eq!(parse_failure_status(1), ReplayStatus::Pending);
        assert_eq!(parse_failure_status(2), ReplayStatus::Pending);
        assert_eq!(parse_failure_status(3), ReplayStatus::Failed);
    }
}
