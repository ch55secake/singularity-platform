use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

use super::now_nanos;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum MetadataStatus {
    #[serde(rename = "metadata_pending")]
    MetadataPending,
    #[serde(rename = "metadata_processed")]
    MetadataProcessed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MetadataJob {
    pub(super) status: MetadataStatus,
    pub(super) attempts: u32,
    pub(super) next_retry_at: Option<u64>,
    pub(super) last_error: Option<String>,
    pub(super) output_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MetadataStateFile {
    version: u32,
    #[serde(default)]
    jobs: BTreeMap<String, MetadataJob>,
}

pub(super) struct MetadataState {
    path: PathBuf,
    pub(super) jobs: BTreeMap<String, MetadataJob>,
}

impl MetadataState {
    pub(super) async fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = match fs::read(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if contents.is_empty() {
            return Ok(Self {
                path: path.to_path_buf(),
                jobs: BTreeMap::new(),
            });
        }
        #[cfg(unix)]
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
        let file: MetadataStateFile = serde_json::from_slice(&contents)?;
        if file.version != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported metadata state version",
            )
            .into());
        }
        Ok(Self {
            path: path.to_path_buf(),
            jobs: file.jobs,
        })
    }

    pub(super) async fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).await?;
        }
        let contents = serde_json::to_vec_pretty(&MetadataStateFile {
            version: 1,
            jobs: self.jobs.clone(),
        })?;
        let temp_path = self.path.with_file_name(format!(
            ".{}.tmp-{}-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("metadata-state.json"),
            std::process::id(),
            now_nanos()
        ));
        let mut temp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .await?;
        temp.write_all(&contents).await?;
        temp.flush().await?;
        temp.sync_data().await?;
        drop(temp);
        #[cfg(unix)]
        fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(0o600)).await?;
        fs::rename(temp_path, &self.path).await?;
        Ok(())
    }
}
