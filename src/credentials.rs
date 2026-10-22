use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::{constant_time_eq, now_seconds};
use getrandom::fill as fill_random;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

mod worker;

pub(super) use worker::run_command;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CredentialRecord {
    token_hash: String,
    created_at: u64,
    #[serde(default)]
    rotated_at: Option<u64>,
    #[serde(default)]
    revoked_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CredentialFile {
    version: u32,
    #[serde(default)]
    credentials: BTreeMap<String, CredentialRecord>,
}

pub(super) struct CredentialStore {
    path: PathBuf,
    records: BTreeMap<String, CredentialRecord>,
}

impl CredentialStore {
    pub(super) async fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = match fs::read(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if contents.is_empty() {
            return Ok(Self {
                path: path.to_path_buf(),
                records: BTreeMap::new(),
            });
        }

        #[cfg(unix)]
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;

        let file: CredentialFile = serde_json::from_slice(&contents)?;
        if file.version != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported credential file version",
            )
            .into());
        }
        Ok(Self {
            path: path.to_path_buf(),
            records: file.credentials,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.records.len()
    }

    async fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let file = CredentialFile {
            version: 1,
            credentials: self.records.clone(),
        };
        let contents = serde_json::to_vec_pretty(&file)?;
        let temp_path = self.path.with_file_name(format!(
            ".{}.tmp-{}-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("collectors.json"),
            std::process::id(),
            now_seconds()
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

    pub(super) fn create(&mut self, collector_id: &str) -> Result<String, String> {
        if self.records.contains_key(collector_id) {
            return Err("collector already exists; use rotate instead".to_string());
        }
        let token = generate_token()?;
        self.records.insert(
            collector_id.to_string(),
            CredentialRecord {
                token_hash: hash_token(&token),
                created_at: now_seconds(),
                rotated_at: None,
                revoked_at: None,
            },
        );
        Ok(token)
    }

    fn rotate(&mut self, collector_id: &str) -> Result<String, String> {
        let record = self
            .records
            .get_mut(collector_id)
            .ok_or_else(|| "collector does not exist".to_string())?;
        let token = generate_token()?;
        record.token_hash = hash_token(&token);
        record.rotated_at = Some(now_seconds());
        record.revoked_at = None;
        Ok(token)
    }

    fn revoke(&mut self, collector_id: &str) -> Result<(), String> {
        let record = self
            .records
            .get_mut(collector_id)
            .ok_or_else(|| "collector does not exist".to_string())?;
        record.revoked_at = Some(now_seconds());
        Ok(())
    }

    pub(super) fn authenticate(&self, token: &str) -> Option<String> {
        let token_hash = hash_token(token);
        self.records.iter().find_map(|(collector_id, record)| {
            if record.revoked_at.is_none()
                && constant_time_eq(record.token_hash.as_bytes(), token_hash.as_bytes())
            {
                Some(collector_id.clone())
            } else {
                None
            }
        })
    }
}

fn generate_token() -> Result<String, String> {
    let mut bytes = [0_u8; 32];
    fill_random(&mut bytes).map_err(|_| "could not generate secure credential".to_string())?;
    Ok(format!("sg_{}", hex::encode(bytes)))
}

fn hash_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_lifecycle_hashes_tokens_and_revokes_old_tokens() {
        let mut store = CredentialStore {
            path: PathBuf::from("collectors.json"),
            records: BTreeMap::new(),
        };

        let first_token = store.create("collector-a").expect("credential creates");
        let serialized = serde_json::to_string(&store.records).expect("credentials serialize");
        assert!(!serialized.contains(&first_token));
        assert_eq!(
            store.authenticate(&first_token),
            Some("collector-a".to_string())
        );

        let second_token = store.rotate("collector-a").expect("credential rotates");
        assert_ne!(first_token, second_token);
        assert_eq!(store.authenticate(&first_token), None);
        assert_eq!(
            store.authenticate(&second_token),
            Some("collector-a".to_string())
        );

        store.revoke("collector-a").expect("credential revokes");
        assert_eq!(store.authenticate(&second_token), None);
    }
}
