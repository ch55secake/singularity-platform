use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

#[derive(Debug, Serialize, Deserialize)]
struct AssetStateFile {
    version: u32,
    #[serde(default)]
    snapshots: BTreeMap<String, u64>,
}

pub(super) struct AssetState {
    path: PathBuf,
    pub(super) snapshots: BTreeMap<String, u64>,
}

impl AssetState {
    pub(super) async fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = match fs::read(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if contents.is_empty() {
            return Ok(Self {
                path: path.to_path_buf(),
                snapshots: BTreeMap::new(),
            });
        }
        #[cfg(unix)]
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
        let file: AssetStateFile = serde_json::from_slice(&contents)?;
        if file.version != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported asset state version",
            )
            .into());
        }
        Ok(Self {
            path: path.to_path_buf(),
            snapshots: file.snapshots,
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
        let contents = serde_json::to_vec_pretty(&AssetStateFile {
            version: 1,
            snapshots: self.snapshots.clone(),
        })?;
        let temp_path = self.path.with_file_name(format!(
            ".{}.tmp-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("assets-state.json"),
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

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
