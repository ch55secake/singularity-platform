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
