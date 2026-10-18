use std::{
    collections::HashSet,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

use super::{
    MetadataRecord,
    config::WorkerConfig,
    download::download_metadata,
    now_nanos,
    state::{MetadataJob, MetadataState, MetadataStatus},
};

#[derive(Default)]
struct WorkerSummary {
    discovered: usize,
    processed: usize,
    deferred: usize,
    failed: usize,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = WorkerConfig::from_env()?;
    if let Some(parent) = config
        .state_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).await?;
    }
    fs::create_dir_all(&config.output_dir).await?;

    let mut state = MetadataState::load(&config.state_path).await?;
    let client = Client::builder()
        .timeout(config.timeout)
        .user_agent("singularity-platform-metadata-worker/0.1")
        .build()?;
    let spool = match fs::read_to_string(&config.spool_path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };

    let mut summary = WorkerSummary::default();
    let mut job_ids = HashSet::new();
    for line in spool.lines() {
        let Ok(record) = serde_json::from_str::<MetadataRecord>(line) else {
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
        process_record(&mut state, &client, &config, &job_id, &record, &mut summary).await?;
    }

    state.save().await?;
    println!(
        "metadata worker: discovered={} processed={} deferred={} failed={}",
        summary.discovered, summary.processed, summary.deferred, summary.failed
    );
    Ok(())
}

async fn process_record(
    state: &mut MetadataState,
    client: &Client,
    config: &WorkerConfig,
    job_id: &str,
    record: &MetadataRecord,
    summary: &mut WorkerSummary,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_path = config.output_dir.join(format!("{job_id}.meta.pb"));
    let now = now_seconds();
    let existing = state.jobs.get(job_id).cloned();
    if let Some(job) = &existing {
        if job.status == MetadataStatus::MetadataProcessed && fs::try_exists(&output_path).await? {
            return Ok(());
        }
        if job.status == MetadataStatus::MetadataPending
            && job.next_retry_at.is_some_and(|retry_at| retry_at > now)
        {
            summary.deferred += 1;
            return Ok(());
        }
    }
    if fs::try_exists(&output_path).await? {
        state.jobs.insert(
            job_id.to_string(),
            MetadataJob {
                status: MetadataStatus::MetadataProcessed,
                attempts: existing.map_or(0, |job| job.attempts),
                next_retry_at: None,
                last_error: None,
                output_path: Some(output_path.display().to_string()),
            },
        );
        summary.processed += 1;
        return Ok(());
    }

    let attempt = existing.map_or(1, |job| job.attempts.saturating_add(1));
    match download_metadata(client, config, record).await {
        Ok(contents) => {
            write_output(&output_path, &contents).await?;
            state.jobs.insert(
                job_id.to_string(),
                MetadataJob {
                    status: MetadataStatus::MetadataProcessed,
                    attempts: attempt,
                    next_retry_at: None,
                    last_error: None,
                    output_path: Some(output_path.display().to_string()),
                },
            );
            summary.processed += 1;
        }
        Err(error) => {
            let retry_after = retry_delay(config, attempt);
            state.jobs.insert(
                job_id.to_string(),
                MetadataJob {
                    status: MetadataStatus::MetadataPending,
                    attempts: attempt,
                    next_retry_at: Some(now.saturating_add(retry_after)),
                    last_error: Some(error.code.to_string()),
                    output_path: Some(output_path.display().to_string()),
                },
            );
            summary.failed += 1;
        }
    }
    state.save().await?;
    Ok(())
}

async fn write_output(path: &Path, contents: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let temp_path = path.with_file_name(format!(
        ".{}.tmp-{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        now_nanos()
    ));
    let mut temp = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)
        .await?;
    temp.write_all(contents).await?;
    temp.flush().await?;
    temp.sync_data().await?;
    drop(temp);
    #[cfg(unix)]
    fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(0o600)).await?;
    fs::rename(temp_path, path).await?;
    Ok(())
}

fn job_id(record: &MetadataRecord) -> String {
    let mut digest = Sha256::new();
    digest.update(record.match_id.to_le_bytes());
    digest.update(record.cluster_id.unwrap_or_default().to_le_bytes());
    digest.update(record.metadata_salt.unwrap_or_default().to_le_bytes());
    hex::encode(digest.finalize())
}

fn retry_delay(config: &WorkerConfig, attempt: u32) -> u64 {
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
    use std::{path::PathBuf, time::Duration};

    use super::*;

    fn sample_record() -> MetadataRecord {
        MetadataRecord {
            match_id: 113390164,
            cluster_id: Some(123),
            metadata_salt: Some(456),
        }
    }

    #[test]
    fn job_id_does_not_expose_the_salt() {
        let record = sample_record();
        let id = job_id(&record);
        assert_eq!(id.len(), 64);
        assert!(!id.contains(&record.metadata_salt.unwrap().to_string()));
    }

    #[test]
    fn retry_delay_is_exponential_and_capped() {
        let config = WorkerConfig {
            spool_path: PathBuf::new(),
            state_path: PathBuf::new(),
            output_dir: PathBuf::new(),
            base_url: String::new(),
            timeout: Duration::from_secs(1),
            retry_base_seconds: 30,
            retry_max_seconds: 100,
        };

        assert_eq!(retry_delay(&config, 1), 30);
        assert_eq!(retry_delay(&config, 2), 60);
        assert_eq!(retry_delay(&config, 3), 100);
    }
}
