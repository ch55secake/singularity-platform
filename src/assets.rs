use reqwest::Client;
use serde_json::Value;

mod clickhouse;
mod config;
mod normalize;
mod state;

use clickhouse::ClickHouse;
use config::AssetConfig;
use normalize::{normalize_assets, normalize_assets_filtered, normalize_map, snapshot_version};
use state::AssetState;

const MAX_ASSET_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

const HERO_COLUMNS: &str =
    "id, class_name, name, image_url, asset_type, ability_type, payload_json, snapshot_version";
const ITEM_COLUMNS: &str = HERO_COLUMNS;
const ABILITY_COLUMNS: &str = HERO_COLUMNS;
const MAP_COLUMNS: &str = "map_id, radius, world_min_x, world_min_y, world_max_x, world_max_y, minimap_url, payload_json, snapshot_version";

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
