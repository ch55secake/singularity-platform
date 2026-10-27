mod clickhouse;
mod config;
mod download;
mod parser;
mod record;
mod state;
mod worker;

#[cfg(test)]
use clickhouse::ClickHouseClient;
use config::ReplayConfig;
#[cfg(test)]
use download::{decompress_file, decompress_replay, download_replay, replay_url};
#[cfg(test)]
use parser::{MODEL_VERSION, parse_replay};
use record::ReplayRecord;
#[cfg(test)]
use record::job_id;
#[cfg(test)]
use reqwest::Client;
#[cfg(test)]
use state::{ReplayState, ReplayStatus};
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(test)]
use worker::{ReplayProcess, WorkerSummary, process_record};

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    worker::run().await
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
        assert_eq!(
            serde_json::to_string(&ReplayStatus::Failed).expect("state serializes"),
            "\"replay_failed\""
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
    async fn force_reparse_redownloads_a_deleted_demo_and_keeps_normal_retries_idempotent() {
        let replay_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("replay listener binds");
        let replay_address = replay_listener.local_addr().expect("replay address reads");
        let mut demo = Vec::from(*b"PBDEMS2\0");
        demo.extend_from_slice(&[0; 8]);
        let compressed =
            zstd::stream::encode_all(Cursor::new(&demo), 0).expect("fixture compresses");
        let replay_server = tokio::spawn(async move {
            for _ in 0..2 {
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
            }
        });

        let clickhouse_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ClickHouse listener binds");
        let clickhouse_address = clickhouse_listener
            .local_addr()
            .expect("ClickHouse address reads");
        let clickhouse_server = tokio::spawn(async move {
            for _ in 0..2 {
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
            }
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
            ReplayProcess {
                job_id: &id,
                record: &record,
                force_reparse: false,
            },
            &mut summary,
        )
        .await
        .expect("replay process succeeds");
        assert_eq!(summary.downloaded, 1);
        assert_eq!(summary.parsed, 1);
        assert_eq!(state.jobs[&id].status, ReplayStatus::Parsed);
        assert!(!config.output_dir.join(format!("{id}.dem")).exists());

        let mut reparse_summary = WorkerSummary::default();
        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            ReplayProcess {
                job_id: &id,
                record: &record,
                force_reparse: true,
            },
            &mut reparse_summary,
        )
        .await
        .expect("forced replay reparse succeeds");
        assert_eq!(reparse_summary.downloaded, 1);
        assert_eq!(reparse_summary.parsed, 1);

        let mut retry_summary = WorkerSummary::default();
        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            ReplayProcess {
                job_id: &id,
                record: &record,
                force_reparse: false,
            },
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

    #[tokio::test]
    async fn removes_legacy_parser_failed_raw_files_before_backoff() {
        let directory = std::env::temp_dir().join(format!(
            "singularity-replay-failed-raw-test-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        std_fs::create_dir(&directory).expect("test directory creates");
        let mut config = sample_config();
        config.output_dir = directory.join("replays");
        config.state_path = directory.join("state.json");
        std_fs::create_dir(&config.output_dir).expect("replay directory creates");
        let record = sample_record();
        let id = job_id(&record);
        let raw_path = config.output_dir.join(format!("{id}.dem"));
        std_fs::write(&raw_path, b"raw demo retained after parse error")
            .expect("raw replay writes");

        let mut state = ReplayState {
            path: config.state_path.clone(),
            jobs: BTreeMap::from([(
                id.clone(),
                state::ReplayJob {
                    status: ReplayStatus::Downloaded,
                    attempts: 2,
                    next_retry_at: Some(u64::MAX),
                    last_error: Some("invalid_demo".to_string()),
                    raw_path: Some(raw_path.display().to_string()),
                    position_count: None,
                    parser_version: None,
                    raw_retention_until: None,
                    parse_failures: 0,
                },
            )]),
        };
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("HTTP client builds");
        let clickhouse = ClickHouseClient {
            http: client.clone(),
            url: "http://127.0.0.1:1".to_string(),
            database: "singularity".to_string(),
            user: None,
            password: None,
        };
        let mut summary = WorkerSummary::default();

        process_record(
            &mut state,
            &client,
            &clickhouse,
            &config,
            ReplayProcess {
                job_id: &id,
                record: &record,
                force_reparse: false,
            },
            &mut summary,
        )
        .await
        .expect("legacy parse failure is migrated");

        assert!(!raw_path.exists());
        assert_eq!(state.jobs[&id].status, ReplayStatus::Pending);
        assert!(state.jobs[&id].raw_path.is_none());
        assert_eq!(summary.deferred, 1);
        std_fs::remove_dir_all(directory).expect("test directory removes");
    }
}
