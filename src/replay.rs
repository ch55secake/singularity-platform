use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::fs;

mod clickhouse;
mod config;
mod download;
mod parser;
mod state;
mod worker;

use clickhouse::{ClickHouseClient, REPLAY_MATCH_COLUMNS, REPLAY_POSITION_COLUMNS, ReplayMatchRow};
use config::ReplayConfig;
use download::download_replay;
#[cfg(test)]
use download::{decompress_file, decompress_replay, replay_url};
use parser::{MODEL_VERSION, PARSER_VERSION, parse_replay};
use state::{ReplayJob, ReplayState, ReplayStatus};
use worker::{WorkerSummary, disk_usage, process_record};

#[derive(Debug, Clone, Deserialize)]
struct ReplayRecord {
    match_id: u64,
    cluster_id: Option<u32>,
    replay_salt: Option<u32>,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
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

impl ReplayRecord {
    fn is_downloadable(&self) -> bool {
        self.match_id > 0
            && self.cluster_id.is_some_and(|cluster| cluster > 0)
            && self.replay_salt.is_some_and(|salt| salt > 0)
    }
}

fn job_id(record: &ReplayRecord) -> String {
    let mut digest = Sha256::new();
    digest.update(record.match_id.to_le_bytes());
    digest.update(record.cluster_id.unwrap_or_default().to_le_bytes());
    digest.update(record.replay_salt.unwrap_or_default().to_le_bytes());
    hex::encode(digest.finalize())
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bzip2::{Compression, write::BzEncoder};
    use std::collections::BTreeMap;
    use std::fs as std_fs;
    use std::io::{Cursor, Write};
    use std::{path::PathBuf, time::Duration};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn sample_record() -> ReplayRecord {
        ReplayRecord {
            match_id: 113390164,
            cluster_id: Some(123),
            replay_salt: Some(456),
        }
    }

    fn sample_config() -> ReplayConfig {
        ReplayConfig {
            spool_path: PathBuf::new(),
            state_path: PathBuf::new(),
            output_dir: PathBuf::new(),
            base_url: "http://replay{cluster}.valve.net/1422450".to_string(),
            timeout: Duration::from_secs(1),
            retry_base_seconds: 30,
            retry_max_seconds: 3_600,
            max_compressed_bytes: 1024,
            max_decompressed_bytes: 2048,
            max_disk_bytes: 4096,
            debug_retention_seconds: 0,
        }
    }

    #[test]
    fn builds_the_cache_replay_url() {
        assert_eq!(
            replay_url(&sample_config(), &sample_record()),
            "http://replay123.valve.net/1422450/113390164_456.dem.bz2"
        );
    }

    #[test]
    fn decompresses_zstd_and_bzip2_payloads() {
        let input = b"demo bytes";
        let zstd = zstd::stream::encode_all(Cursor::new(input), 0).expect("zstd encodes");
        let mut encoder = BzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(input).expect("bzip2 writes");
        let bzip2 = encoder.finish().expect("bzip2 finishes");

        assert_eq!(decompress_replay(&zstd, 1024).expect("zstd decodes"), input);
        assert_eq!(
            decompress_replay(&bzip2, 1024).expect("bzip2 decodes"),
            input
        );
        assert_eq!(
            decompress_replay(b"not compressed", 1024),
            Err("unsupported_compression")
        );
        assert_eq!(decompress_replay(input, 4), Err("unsupported_compression"));
    }

    #[test]
    fn streams_decompression_to_a_bounded_raw_file() {
        let directory = std::env::temp_dir().join(format!(
            "singularity-replay-test-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        std_fs::create_dir(&directory).expect("test directory creates");
        let compressed_path = directory.join("demo.dem.bz2");
        let raw_path = directory.join("demo.dem");
        let input = b"demo bytes that are larger than the test limit";
        let mut encoder = BzEncoder::new(
            std_fs::File::create(&compressed_path).expect("compressed file creates"),
            Compression::best(),
        );
        encoder.write_all(input).expect("bzip2 writes");
        encoder.finish().expect("bzip2 finishes");

        assert_eq!(decompress_file(&compressed_path, &raw_path, 1024), Ok(()));
        assert_eq!(std_fs::read(&raw_path).expect("raw file reads"), input);

        std_fs::remove_file(&raw_path).expect("raw file removes");
        assert_eq!(
            decompress_file(&compressed_path, &raw_path, 4),
            Err("decompressed_too_large")
        );
        assert!(!raw_path.exists());
        std_fs::remove_dir_all(directory).expect("test directory removes");
    }

    #[test]
    fn parses_a_minimal_haste_demo_header() {
        let directory = std::env::temp_dir().join(format!(
            "singularity-replay-parser-test-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        std_fs::create_dir(&directory).expect("test directory creates");
        let raw_path = directory.join("minimal.dem");
        let mut demo = Vec::from(*b"PBDEMS2\0");
        demo.extend_from_slice(&[0; 8]);
        std_fs::write(&raw_path, demo).expect("minimal demo writes");

        let parsed = parse_replay(42, &raw_path, MODEL_VERSION).expect("Haste parses header");
        assert!(parsed.positions.is_empty());

        std_fs::remove_dir_all(directory).expect("test directory removes");
    }

    #[test]
    fn persists_the_explicit_replay_state_names() {
        assert_eq!(
            serde_json::to_string(&ReplayStatus::Pending).expect("state serializes"),
            "\"replay_pending\""
        );
        assert_eq!(
            serde_json::to_string(&ReplayStatus::Downloaded).expect("state serializes"),
            "\"replay_downloaded\""
        );
        assert_eq!(
            serde_json::to_string(&ReplayStatus::Parsed).expect("state serializes"),
            "\"replay_parsed\""
        );
    }

    #[tokio::test]
    async fn downloads_and_decompresses_a_replay_fixture() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fixture listener binds");
        let address = listener.local_addr().expect("fixture address reads");
        let mut demo = Vec::from(*b"PBDEMS2\0");
        demo.extend_from_slice(&[0; 8]);
        let compressed =
            zstd::stream::encode_all(Cursor::new(&demo), 0).expect("fixture compresses");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("fixture accepts");
            let mut request = [0_u8; 1024];
            let _ = stream
                .read(&mut request)
                .await
                .expect("fixture request reads");
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                compressed.len()
            );
            stream
                .write_all(headers.as_bytes())
                .await
                .expect("fixture headers write");
            stream
                .write_all(&compressed)
                .await
                .expect("fixture body writes");
        });

        let directory = std::env::temp_dir().join(format!(
            "singularity-replay-download-test-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        std_fs::create_dir(&directory).expect("test directory creates");
        let raw_path = directory.join("fixture.dem");
        let mut config = sample_config();
        config.base_url = format!("http://{address}/1422450");
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("fixture client builds");

        download_replay(&client, &config, &sample_record(), &raw_path)
            .await
            .expect("fixture downloads");
        assert_eq!(std_fs::read(&raw_path).expect("fixture reads"), demo);
        server.await.expect("fixture server completes");
        std_fs::remove_dir_all(directory).expect("test directory removes");
    }

    #[tokio::test]
    async fn persists_once_and_deletes_the_raw_demo_after_parse() {
        let replay_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("replay listener binds");
        let replay_address = replay_listener.local_addr().expect("replay address reads");
        let mut demo = Vec::from(*b"PBDEMS2\0");
        demo.extend_from_slice(&[0; 8]);
        let compressed =
            zstd::stream::encode_all(Cursor::new(&demo), 0).expect("fixture compresses");
        let replay_server = tokio::spawn(async move {
            let (mut stream, _) = replay_listener.accept().await.expect("replay accepts");
            let mut request = [0_u8; 1024];
            let _ = stream
                .read(&mut request)
                .await
                .expect("replay request reads");
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                compressed.len()
            );
            stream
                .write_all(headers.as_bytes())
                .await
                .expect("replay headers write");
            stream
                .write_all(&compressed)
                .await
                .expect("replay body writes");
        });

        let clickhouse_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ClickHouse listener binds");
        let clickhouse_address = clickhouse_listener
            .local_addr()
            .expect("ClickHouse address reads");
        let clickhouse_server = tokio::spawn(async move {
            let (mut stream, _) = clickhouse_listener
                .accept()
                .await
                .expect("ClickHouse accepts");
            let mut request = [0_u8; 4096];
            let count = stream
                .read(&mut request)
                .await
                .expect("ClickHouse request reads");
            assert!(String::from_utf8_lossy(&request[..count]).contains("replay_matches"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("ClickHouse response writes");
        });

        let directory = std::env::temp_dir().join(format!(
            "singularity-replay-process-test-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        std_fs::create_dir(&directory).expect("test directory creates");
        let mut config = sample_config();
        config.base_url = format!("http://{replay_address}/1422450");
        config.output_dir = directory.join("replays");
        config.state_path = directory.join("state.json");
        std_fs::create_dir(&config.output_dir).expect("replay directory creates");
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("fixture client builds");
        let clickhouse = ClickHouseClient {
            http: client.clone(),
            url: format!("http://{clickhouse_address}"),
            database: "singularity".to_string(),
            user: None,
            password: None,
        };
        let mut state = ReplayState {
            path: config.state_path.clone(),
            jobs: BTreeMap::new(),
        };
        let record = sample_record();
        let id = job_id(&record);
        let mut summary = WorkerSummary::default();

        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            &id,
            &record,
            &mut summary,
        )
        .await
        .expect("replay process succeeds");
        assert_eq!(summary.downloaded, 1);
        assert_eq!(summary.parsed, 1);
        assert_eq!(state.jobs[&id].status, ReplayStatus::Parsed);
        assert!(!config.output_dir.join(format!("{id}.dem")).exists());

        let mut retry_summary = WorkerSummary::default();
        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            &id,
            &record,
            &mut retry_summary,
        )
        .await
        .expect("replay retry succeeds");
        assert_eq!(retry_summary.parsed, 0);
        replay_server.await.expect("replay server completes");
        clickhouse_server
            .await
            .expect("ClickHouse server completes");
        std_fs::remove_dir_all(directory).expect("test directory removes");
    }

    #[test]
    fn replay_job_id_does_not_expose_the_salt() {
        let id = job_id(&sample_record());
        assert!(!id.contains("456"));
        assert_eq!(id.len(), 64);
    }

    #[test]
    fn rejects_replay_records_without_required_values() {
        let mut record = sample_record();
        assert!(record.is_downloadable());
        record.replay_salt = None;
        assert!(!record.is_downloadable());
        record.replay_salt = Some(456);
        record.cluster_id = None;
        assert!(!record.is_downloadable());
    }
}
