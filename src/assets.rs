use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

mod normalize;

use normalize::{normalize_assets, normalize_assets_filtered, normalize_map, snapshot_version};

const DEFAULT_BASE_URL: &str = "https://api.deadlock-api.com/v1/assets";
const DEFAULT_DATABASE: &str = "singularity";
const DEFAULT_STATE_PATH: &str = "data/assets-state.json";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const MAX_ASSET_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

const HERO_COLUMNS: &str =
    "id, class_name, name, image_url, asset_type, ability_type, payload_json, snapshot_version";
const ITEM_COLUMNS: &str = HERO_COLUMNS;
const ABILITY_COLUMNS: &str = HERO_COLUMNS;
const MAP_COLUMNS: &str = "map_id, radius, world_min_x, world_min_y, world_max_x, world_max_y, minimap_url, payload_json, snapshot_version";

#[derive(Debug, Serialize, Deserialize)]
struct AssetStateFile {
    version: u32,
    #[serde(default)]
    snapshots: BTreeMap<String, u64>,
}

struct AssetState {
    path: PathBuf,
    snapshots: BTreeMap<String, u64>,
}

struct AssetConfig {
    base_url: String,
    state_path: PathBuf,
    timeout: Duration,
    clickhouse_url: String,
    clickhouse_database: String,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
}

struct ClickHouse {
    client: Client,
    url: String,
    database: String,
    user: Option<String>,
    password: Option<String>,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = AssetConfig::from_env()?;
    let mut state = AssetState::load(&config.state_path).await?;
    let client = Client::builder()
        .timeout(config.timeout)
        .user_agent("singularity-platform-assets/0.1")
        .build()?;
    let clickhouse = ClickHouse {
        client,
        url: config.clickhouse_url,
        database: config.clickhouse_database,
        user: config.clickhouse_user,
        password: config.clickhouse_password,
    };

    let (heroes_value, heroes_version) =
        fetch_json(&clickhouse.client, &config.base_url, "heroes").await?;
    let (items_value, items_version) =
        fetch_json(&clickhouse.client, &config.base_url, "items").await?;
    let (modifiers_value, modifiers_version) =
        fetch_json(&clickhouse.client, &config.base_url, "modifiers").await?;
    let (map_value, map_version) = fetch_json(&clickhouse.client, &config.base_url, "map").await?;

    let heroes = normalize_assets(&heroes_value, "hero", heroes_version)?;
    let items = normalize_assets_filtered(&items_value, "item", items_version, |value| {
        value.get("type").and_then(Value::as_str) != Some("ability")
    })?;
    let abilities = normalize_assets_filtered(&items_value, "ability", items_version, |value| {
        value.get("type").and_then(Value::as_str) == Some("ability")
    })?;
    let modifiers = normalize_assets(&modifiers_value, "ability_modifier", modifiers_version)?;
    let map = normalize_map(&map_value, map_version)?;

    let changed = state.snapshots.get("heroes") != Some(&heroes_version)
        || state.snapshots.get("items") != Some(&items_version)
        || state.snapshots.get("modifiers") != Some(&modifiers_version)
        || state.snapshots.get("map") != Some(&map_version);
    if changed {
        clickhouse.insert("heroes", HERO_COLUMNS, &heroes).await?;
        clickhouse.insert("items", ITEM_COLUMNS, &items).await?;
        clickhouse
            .insert("abilities", ABILITY_COLUMNS, &abilities)
            .await?;
        clickhouse
            .insert("abilities", ABILITY_COLUMNS, &modifiers)
            .await?;
        clickhouse
            .insert("map_assets", MAP_COLUMNS, std::slice::from_ref(&map))
            .await?;
        state.snapshots.insert("heroes".to_string(), heroes_version);
        state.snapshots.insert("items".to_string(), items_version);
        state
            .snapshots
            .insert("modifiers".to_string(), modifiers_version);
        state.snapshots.insert("map".to_string(), map_version);
        state.save().await?;
    }

    println!(
        "asset sync: heroes={} items={} abilities={} modifiers={} changed={}",
        heroes.len(),
        items.len(),
        abilities.len(),
        modifiers.len(),
        changed
    );
    Ok(())
}

impl AssetConfig {
    fn from_env() -> Result<Self, std::io::Error> {
        let timeout = env::var("SINGULARITY_ASSETS_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| DEFAULT_TIMEOUT_SECONDS.to_string())
            .parse::<u64>()
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid asset timeout")
            })?;
        if timeout == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "asset timeout must be greater than zero",
            ));
        }
        let clickhouse_url = env::var("SINGULARITY_CLICKHOUSE_URL").map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SINGULARITY_CLICKHOUSE_URL is required for assets-sync",
            )
        })?;
        Ok(Self {
            base_url: env::var("SINGULARITY_ASSETS_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            state_path: PathBuf::from(
                env::var("SINGULARITY_ASSETS_STATE_PATH")
                    .unwrap_or_else(|_| DEFAULT_STATE_PATH.to_string()),
            ),
            timeout: Duration::from_secs(timeout),
            clickhouse_url,
            clickhouse_database: env::var("SINGULARITY_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| DEFAULT_DATABASE.to_string()),
            clickhouse_user: env::var("SINGULARITY_CLICKHOUSE_USER").ok(),
            clickhouse_password: env::var("SINGULARITY_CLICKHOUSE_PASSWORD").ok(),
        })
    }
}

async fn fetch_json(
    client: &Client,
    base_url: &str,
    resource: &str,
) -> Result<(Value, u64), Box<dyn std::error::Error>> {
    let url = format!("{}/{}", base_url.trim_end_matches('/'), resource);
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(std::io::Error::other("asset endpoint request failed").into());
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ASSET_RESPONSE_BYTES)
    {
        return Err(std::io::Error::other("asset response is too large").into());
    }
    let bytes = response.bytes().await?;
    if bytes.len() as u64 > MAX_ASSET_RESPONSE_BYTES {
        return Err(std::io::Error::other("asset response is too large").into());
    }
    let version = snapshot_version(&bytes);
    Ok((serde_json::from_slice(&bytes)?, version))
}

impl ClickHouse {
    async fn insert<T: Serialize>(
        &self,
        table: &str,
        columns: &str,
        rows: &[T],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut body, row)?;
            body.push(b'\n');
        }
        let query = format!(
            "INSERT INTO {}.{} ({}) FORMAT JSONEachRow",
            self.database, table, columns
        );
        let mut request = self
            .client
            .post(&self.url)
            .query(&[("query", query)])
            .header("content-type", "application/json")
            .body(body);
        if let Some(user) = &self.user {
            request = request.basic_auth(user, self.password.as_deref());
        }
        if !request.send().await?.status().is_success() {
            return Err(std::io::Error::other("ClickHouse asset insert failed").into());
        }
        Ok(())
    }
}

impl AssetState {
    async fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
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

    async fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
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
