use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize)]
pub(super) struct AssetRow {
    id: u64,
    class_name: String,
    name: String,
    image_url: String,
    asset_type: String,
    ability_type: Option<String>,
    payload_json: String,
    snapshot_version: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct MapRow {
    map_id: String,
    radius: f64,
    world_min_x: f64,
    world_min_y: f64,
    world_max_x: f64,
    world_max_y: f64,
    minimap_url: String,
    payload_json: String,
    snapshot_version: u64,
}

pub(super) fn normalize_assets(
    value: &Value,
    asset_type: &str,
    snapshot_version: u64,
) -> Result<Vec<AssetRow>, std::io::Error> {
    normalize_assets_filtered(value, asset_type, snapshot_version, |_| true)
}

pub(super) fn normalize_assets_filtered(
    value: &Value,
    asset_type: &str,
    snapshot_version: u64,
    include: impl Fn(&Value) -> bool,
) -> Result<Vec<AssetRow>, std::io::Error> {
    let assets = value.as_array().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "asset response is not an array",
        )
    })?;
    Ok(assets
        .iter()
        .filter(|asset| include(asset))
        .filter_map(|asset| {
            let id = asset.get("id")?.as_u64()?;
            let class_name = asset.get("class_name")?.as_str()?.to_string();
            let name = asset
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(&class_name)
                .to_string();
            let ability_type = asset
                .get("ability_type")
                .and_then(Value::as_str)
                .map(str::to_string);
            Some(AssetRow {
                id,
                class_name,
                name,
                image_url: image_url(asset),
                asset_type: asset_type.to_string(),
                ability_type,
                payload_json: serde_json::to_string(asset).ok()?,
                snapshot_version,
            })
        })
        .collect())
}

pub(super) fn normalize_map(
    value: &Value,
    snapshot_version: u64,
) -> Result<MapRow, std::io::Error> {
    let radius = value.get("radius").and_then(Value::as_f64).unwrap_or(0.0);
    if radius <= 0.0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "map response has no radius",
        ));
    }
    Ok(MapRow {
        map_id: "midtown".to_string(),
        radius,
        world_min_x: -radius,
        world_min_y: -radius,
        world_max_x: radius,
        world_max_y: radius,
        minimap_url: value
            .get("images")
            .and_then(|images| images.get("minimap"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        payload_json: serde_json::to_string(value).map_err(std::io::Error::other)?,
        snapshot_version,
    })
}

fn image_url(asset: &Value) -> String {
    if let Some(image) = asset.get("image").and_then(Value::as_str) {
        return image.to_string();
    }
    if let Some(image) = asset.get("image_webp").and_then(Value::as_str) {
        return image.to_string();
    }
    asset
        .get("images")
        .and_then(Value::as_object)
        .and_then(|images| {
            images
                .get("icon_hero_card")
                .or_else(|| images.get("hero_card"))
                .or_else(|| images.values().find(|value| value.is_string()))
        })
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

pub(super) fn snapshot_version(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_le_bytes(digest[..8].try_into().expect("digest has eight bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_ability_assets_from_items() {
        let value = serde_json::json!([
            {"id": 1, "class_name": "item_one", "name": "Item", "type": "upgrade"},
            {"id": 2, "class_name": "ability_one", "name": "Ability", "type": "ability"}
        ]);

        let items = normalize_assets_filtered(&value, "item", 7, |asset| {
            asset.get("type").and_then(Value::as_str) != Some("ability")
        })
        .expect("items normalize");
        let abilities = normalize_assets_filtered(&value, "ability", 7, |asset| {
            asset.get("type").and_then(Value::as_str) == Some("ability")
        })
        .expect("abilities normalize");

        assert_eq!(items.len(), 1);
        assert_eq!(abilities.len(), 1);
        assert_eq!(abilities[0].name, "Ability");
    }

    #[test]
    fn normalizes_map_radius_to_world_bounds() {
        let value = serde_json::json!({
            "radius": 10752,
            "images": {"minimap": "https://example.test/minimap.png"}
        });

        let map = normalize_map(&value, 3).expect("map normalizes");

        assert_eq!(map.world_min_x, -10752.0);
        assert_eq!(map.world_max_y, 10752.0);
        assert_eq!(map.minimap_url, "https://example.test/minimap.png");
    }
}
