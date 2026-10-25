use std::{
    collections::HashSet,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use reqwest::Client;
use tokio::fs;

use super::{
    clickhouse::{
        ClickHouseClient, REPLAY_MATCH_COLUMNS, REPLAY_POSITION_COLUMNS, REPLAY_RESOURCE_COLUMNS,
        ReplayMatchRow,
    },
    config::ReplayConfig,
    download::download_replay,
    parser::{MODEL_VERSION, PARSER_VERSION, parse_replay},
    record::{ReplayRecord, job_id},
    state::{ReplayJob, ReplayState, ReplayStatus},
};

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

    let mut summary = WorkerSummary::default();
    let mut job_ids = HashSet::new();
    for line in spool.lines() {
        let Ok(record) = serde_json::from_str::<ReplayRecord>(line) else {
            continue;
        };
        if !record.is_downloadable() {
            continue;
        }
        let job_id = job_id(&record);
        if !job_ids.insert(job_id.clone()) {
            continue;
        }
        summary.discovered += 1;
        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            &job_id,
            &record,
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

#[derive(Default)]
pub(super) struct WorkerSummary {
    pub(super) discovered: usize,
    pub(super) downloaded: usize,
    pub(super) parsed: usize,
    pub(super) deferred: usize,
    pub(super) failed: usize,
}

pub(super) async fn process_record(
    state: &mut ReplayState,
    client: &Client,
    clickhouse: &ClickHouseClient,
    config: &ReplayConfig,
    job_id: &str,
    record: &ReplayRecord,
    summary: &mut WorkerSummary,
) -> Result<(), Box<dyn std::error::Error>> {
    let raw_path = config.output_dir.join(format!("{job_id}.dem"));
    let now = now_seconds();
    let existing = state.jobs.get(job_id).cloned();
    if existing
        .as_ref()
        .is_some_and(|job| job.status == ReplayStatus::Parsed)
    {
        let job = existing.as_ref().expect("parsed job exists");
        if job.parser_version.as_deref() == Some(PARSER_VERSION) {
            if config.debug_retention_seconds == 0
                || job
                    .raw_retention_until
                    .is_some_and(|retention_until| retention_until <= now)
            {
                remove_raw_file(job.raw_path.as_deref()).await?;
            }
            return Ok(());
        }

        if let Some(path) = job.raw_path.as_deref() {
            if fs::try_exists(path).await? {
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
                    },
                );
            } else {
                record_failure(
                    state,
                    job_id,
                    ReplayStatus::Pending,
                    job.attempts,
                    "parser_version_mismatch",
                    None,
                    config,
                )
                .await?;
                summary.failed += 1;
                return Ok(());
            }
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
            record_failure(
                state,
                job_id,
                ReplayStatus::Downloaded,
                attempt,
                error,
                Some(raw_path.display().to_string()),
                config,
            )
            .await?;
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
            "replay_positions",
            REPLAY_POSITION_COLUMNS,
            &parsed.positions,
        )
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
        },
    );
    state.save().await?;
    Ok(())
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
