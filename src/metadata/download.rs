use std::io::{Cursor, Read};

use bzip2::read::BzDecoder;
use reqwest::{Client, StatusCode};
use zstd::stream::read::Decoder as ZstdDecoder;

use super::{MetadataRecord, WorkerConfig};

const MAX_COMPRESSED_BYTES: u64 = 16 * 1024 * 1024;
const MAX_DECOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

#[derive(Debug)]
pub(super) struct DownloadError {
    pub(super) code: &'static str,
}

pub(super) async fn download_metadata(
    client: &Client,
    config: &WorkerConfig,
    record: &MetadataRecord,
) -> Result<Vec<u8>, DownloadError> {
    let url = metadata_url(config, record);
    let mut response = client.get(url).send().await.map_err(|_| DownloadError {
        code: "request_failed",
    })?;
    let status = response.status();
    if !status.is_success() {
        return Err(DownloadError {
            code: if status == StatusCode::NOT_FOUND {
                "http_404"
            } else if status.is_server_error() {
                "http_5xx"
            } else {
                "http_error"
            },
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_COMPRESSED_BYTES)
    {
        return Err(DownloadError {
            code: "compressed_too_large",
        });
    }
    let mut compressed = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| DownloadError {
        code: "response_failed",
    })? {
        if compressed.len() as u64 + chunk.len() as u64 > MAX_COMPRESSED_BYTES {
            return Err(DownloadError {
                code: "compressed_too_large",
            });
        }
        compressed.extend_from_slice(&chunk);
    }
    decompress_metadata(&compressed).map_err(|code| DownloadError { code })
}

fn metadata_url(config: &WorkerConfig, record: &MetadataRecord) -> String {
    let cluster = record.cluster_id.expect("downloadable record has cluster");
    let salt = record
        .metadata_salt
        .expect("downloadable record has metadata salt");
    format!(
        "{}/{match_id}_{salt}.meta.bz2",
        config
            .base_url
            .replace("{cluster}", &cluster.to_string())
            .trim_end_matches('/'),
        match_id = record.match_id,
    )
}

fn decompress_metadata(compressed: &[u8]) -> Result<Vec<u8>, &'static str> {
    if compressed.starts_with(&ZSTD_MAGIC) {
        let decoder =
            ZstdDecoder::new(Cursor::new(compressed)).map_err(|_| "decompression_failed")?;
        return read_limited(decoder);
    }
    if compressed.starts_with(b"BZh") {
        return read_limited(BzDecoder::new(Cursor::new(compressed)));
    }
    Err("unsupported_compression")
}

fn read_limited<R: Read>(reader: R) -> Result<Vec<u8>, &'static str> {
    let mut limited = reader.take(MAX_DECOMPRESSED_BYTES + 1);
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .map_err(|_| "decompression_failed")?;
    if output.len() as u64 > MAX_DECOMPRESSED_BYTES {
        return Err("decompressed_too_large");
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Cursor, Write},
        path::PathBuf,
        time::Duration,
    };

    use bzip2::{Compression, write::BzEncoder};

    use super::*;

    fn sample_record() -> MetadataRecord {
        MetadataRecord {
            match_id: 113390164,
            cluster_id: Some(123),
            metadata_salt: Some(456),
        }
    }

    #[test]
    fn builds_the_cache_metadata_url() {
        let config = WorkerConfig {
            spool_path: PathBuf::new(),
            state_path: PathBuf::new(),
            output_dir: PathBuf::new(),
            base_url: "http://replay{cluster}.valve.net/1422450".to_string(),
            timeout: Duration::from_secs(1),
            retry_base_seconds: 30,
            retry_max_seconds: 3_600,
        };

        assert_eq!(
            metadata_url(&config, &sample_record()),
            "http://replay123.valve.net/1422450/113390164_456.meta.bz2"
        );
    }

    #[test]
    fn decompresses_zstd_and_bzip2_payloads() {
        let input = b"metadata protobuf bytes";
        let zstd = zstd::stream::encode_all(Cursor::new(input), 0).expect("zstd encodes");
        let mut encoder = BzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(input).expect("bzip2 writes");
        let bzip2 = encoder.finish().expect("bzip2 finishes");

        assert_eq!(decompress_metadata(&zstd).expect("zstd decodes"), input);
        assert_eq!(decompress_metadata(&bzip2).expect("bzip2 decodes"), input);
        assert_eq!(
            decompress_metadata(b"not compressed"),
            Err("unsupported_compression")
        );
    }
}
