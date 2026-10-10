-- Public asset snapshots used to resolve numeric ids in read-side responses.

CREATE TABLE IF NOT EXISTS singularity.heroes
(
    id UInt64,
    class_name String,
    name String,
    image_url String,
    asset_type String,
    ability_type Nullable(String),
    payload_json String,
    snapshot_version UInt64
)
ENGINE = ReplacingMergeTree(snapshot_version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS singularity.items
(
    id UInt64,
    class_name String,
    name String,
    image_url String,
    asset_type String,
    ability_type Nullable(String),
    payload_json String,
    snapshot_version UInt64
)
ENGINE = ReplacingMergeTree(snapshot_version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS singularity.abilities
(
    id UInt64,
    class_name String,
    name String,
    image_url String,
    asset_type String,
    ability_type Nullable(String),
    payload_json String,
    snapshot_version UInt64
)
ENGINE = ReplacingMergeTree(snapshot_version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS singularity.map_assets
(
    map_id String,
    radius Float64,
    world_min_x Float64,
    world_min_y Float64,
    world_max_x Float64,
    world_max_y Float64,
    minimap_url String,
    payload_json String,
    snapshot_version UInt64
)
ENGINE = ReplacingMergeTree(snapshot_version)
ORDER BY map_id;

-- Match item rows resolve to the public asset snapshot when it is available.
-- `type = ability` identifies ability upgrades; `type = upgrade` identifies
-- shop upgrades. The packed upgrade_info fields are decoded by the parser.
CREATE VIEW IF NOT EXISTS singularity.match_player_items_with_assets AS
SELECT
    timeline.*,
    JSONExtractString(assets.payload_json, 'type') AS asset_kind,
    JSONExtractString(assets.payload_json, 'name') AS asset_name,
    assets.image_url AS asset_image_url,
    JSONExtractString(assets.payload_json, 'type') = 'ability'
        AS is_ability_upgrade,
    JSONExtractString(assets.payload_json, 'type') = 'upgrade'
        AS is_shop_upgrade
FROM singularity.match_player_items AS timeline
LEFT JOIN
(
    SELECT id, payload_json, image_url
    FROM singularity.items
    UNION ALL
    SELECT id, payload_json, image_url
    FROM singularity.abilities
) AS assets
    ON assets.id = if(timeline.item_id != 0, timeline.item_id, timeline.upgrade_id);
