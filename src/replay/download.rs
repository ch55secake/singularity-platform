use std::{
    fs::File as StdFile,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use bzip2::read::BzDecoder;
use reqwest::{Client, StatusCode};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};
use zstd::stream::read::Decoder as ZstdDecoder;

use super::{ReplayConfig, ReplayRecord, now_nanos};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

#[derive(Debug)]
pub(super) struct ReplayError {
    pub(super) code: &'static str,
}

pub(super) async fn download_replay(
    client: &Client,
    config: &ReplayConfig,
    record: &ReplayRecord,
    raw_path: &Path,
) -> Result<(), ReplayError> {
    let compressed_path = raw_path.with_file_name(format!(
        ".{}.compressed-{}",
        raw_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("replay.dem"),
        now_nanos()
    ));
    let response = client
        .get(replay_url(config, record))
        .send()
        .await
        .map_err(|_| ReplayError {
            code: "request_failed",
        })?;
    let status = response.status();
    if !status.is_success() {
        return Err(ReplayError {
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
        .is_some_and(|length| length > config.max_compressed_bytes)
    {
        return Err(ReplayError {
            code: "compressed_too_large",
        });
    }
    let mut compressed_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&compressed_path)
        .await
        .map_err(|_| ReplayError {
            code: "temporary_file_failed",
        })?;
    let download_result = async {
        let mut compressed_bytes = 0_u64;
        let mut response = response;
        while let Some(chunk) = response.chunk().await.map_err(|_| ReplayError {
            code: "response_failed",
        })? {
            if compressed_bytes.saturating_add(chunk.len() as u64) > config.max_compressed_bytes {
                return Err(ReplayError {
                    code: "compressed_too_large",
                });
            }
            compressed_file
                .write_all(&chunk)
                .await
                .map_err(|_| ReplayError {
                    code: "temporary_file_failed",
                })?;
            compressed_bytes += chunk.len() as u64;
        }
        compressed_file.flush().await.map_err(|_| ReplayError {
            code: "temporary_file_failed",
        })?;
        compressed_file.sync_data().await.map_err(|_| ReplayError {
            code: "temporary_file_failed",
        })?;
        Ok(())
    }
    .await;
    if let Err(error) = download_result {
        let _ = fs::remove_file(&compressed_path).await;
        return Err(error);
    }
    drop(compressed_file);

    let compressed_path_for_worker = compressed_path.clone();
    let raw_path_for_worker = raw_path.to_path_buf();
    let max_decompressed_bytes = config.max_decompressed_bytes;
    let result = tokio::task::spawn_blocking(move || {
        decompress_file(
            &compressed_path_for_worker,
            &raw_path_for_worker,
            max_decompressed_bytes,
        )
    })
    .await
    .map_err(|_| ReplayError {
        code: "decompression_failed",
    })?;
    let _ = fs::remove_file(&compressed_path).await;
    result.map_err(|code| ReplayError { code })
}

pub(super) fn replay_url(config: &ReplayConfig, record: &ReplayRecord) -> String {
    let cluster = record.cluster_id.expect("downloadable record has cluster");
    let salt = record
        .replay_salt
        .expect("downloadable record has replay salt");
    format!(
        "{}/{match_id}_{salt}.dem.bz2",
        config
            .base_url
            .replace("{cluster}", &cluster.to_string())
            .trim_end_matches('/'),
        match_id = record.match_id,
    )
}

#[cfg(test)]
pub(super) fn decompress_replay(
    compressed: &[u8],
    max_decompressed_bytes: u64,
) -> Result<Vec<u8>, &'static str> {
    use std::io::Cursor;

    if compressed.starts_with(&ZSTD_MAGIC) {
        let decoder =
            ZstdDecoder::new(Cursor::new(compressed)).map_err(|_| "decompression_failed")?;
        return read_limited(decoder, max_decompressed_bytes);
    }
    if compressed.starts_with(b"BZh") {
        return read_limited(
            BzDecoder::new(Cursor::new(compressed)),
            max_decompressed_bytes,
        );
    }
    Err("unsupported_compression")
}

#[cfg(test)]
fn read_limited<R: Read>(reader: R, max_decompressed_bytes: u64) -> Result<Vec<u8>, &'static str> {
    let mut limited = reader.take(max_decompressed_bytes.saturating_add(1));
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .map_err(|_| "decompression_failed")?;
    if output.len() as u64 > max_decompressed_bytes {
        return Err("decompressed_too_large");
    }
    Ok(output)
}

pub(super) fn decompress_file(
    compressed_path: &Path,
    raw_path: &Path,
    max_decompressed_bytes: u64,
) -> Result<(), &'static str> {
    let mut compressed = StdFile::open(compressed_path).map_err(|_| "decompression_failed")?;
    let mut magic = [0_u8; 4];
    compressed
        .read_exact(&mut magic)
        .map_err(|_| "decompression_failed")?;
    compressed
        .seek(SeekFrom::Start(0))
        .map_err(|_| "decompression_failed")?;

    let raw_temp_path = raw_path.with_file_name(format!(
        ".{}.tmp-{}",
        raw_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("replay.dem"),
        now_nanos()
    ));
    let result = if magic == ZSTD_MAGIC {
        let decoder = ZstdDecoder::new(compressed).map_err(|_| "decompression_failed")?;
        write_decompressed(decoder, &raw_temp_path, max_decompressed_bytes)
    } else if magic.starts_with(b"BZh") {
        let decoder = BzDecoder::new(compressed);
        write_decompressed(decoder, &raw_temp_path, max_decompressed_bytes)
    } else {
        Err("unsupported_compression")
    };
    if result.is_err() {
        let _ = std::fs::remove_file(&raw_temp_path);
        return result;
    }
    #[cfg(unix)]
    std::fs::set_permissions(&raw_temp_path, std::fs::Permissions::from_mode(0o600))
        .map_err(|_| "temporary_file_failed")?;
    std::fs::rename(raw_temp_path, raw_path).map_err(|_| "temporary_file_failed")
}

fn write_decompressed<R: Read>(
    reader: R,
    raw_temp_path: &Path,
    max_decompressed_bytes: u64,
) -> Result<(), &'static str> {
    let file = StdFile::create_new(raw_temp_path).map_err(|_| "temporary_file_failed")?;
    let mut writer = LimitedWriter {
        file,
        written: 0,
        max: max_decompressed_bytes,
    };
    std::io::copy(
        &mut reader.take(max_decompressed_bytes.saturating_add(1)),
        &mut writer,
    )
    .map_err(|error| {
        if error.kind() == std::io::ErrorKind::FileTooLarge {
            "decompressed_too_large"
        } else {
            "decompression_failed"
        }
    })?;
    writer
        .file
        .sync_data()
        .map_err(|_| "temporary_file_failed")?;
    Ok(())
}

struct LimitedWriter {
    file: StdFile,
    written: u64,
    max: u64,
}

impl Write for LimitedWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() as u64 > self.max.saturating_sub(self.written) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "decompressed replay exceeds configured limit",
            ));
        }
        let written = self.file.write(buffer)?;
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
