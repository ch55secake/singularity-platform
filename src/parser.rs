use std::{
    collections::HashSet,
    env,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use getrandom::fill as fill_random;
use tokio::fs;

const DEFAULT_OUTPUT_DIR: &str = "data/metadata";
const DEFAULT_PARSE_STATE: &str = "data/metadata-parse-state.json";
const DEFAULT_CLICKHOUSE_DATABASE: &str = "singularity";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

mod clickhouse;
mod match_events;
mod metadata;
mod player_rows;
mod rows;
mod state;

use clickhouse::ClickHouseClient;
use rows::*;
use state::{ParseState, should_skip_duplicate_match};

#[derive(Debug)]
struct ParsedMatch {
    match_row: MatchRow,
    player_rows: Vec<PlayerRow>,
    stat_rows: Vec<PlayerStatRow>,
    item_rows: Vec<PlayerItemRow>,
    damage_rows: Vec<DamageRow>,
    death_rows: Vec<DeathRow>,
    position_rows: Vec<PlayerPositionRow>,
    objective_rows: Vec<ObjectiveRow>,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = PathBuf::from(
        env::var("SINGULARITY_METADATA_OUTPUT_DIR")
            .unwrap_or_else(|_| DEFAULT_OUTPUT_DIR.to_string()),
    );
    let state_path = PathBuf::from(
        env::var("SINGULARITY_METADATA_PARSE_STATE_PATH")
            .unwrap_or_else(|_| DEFAULT_PARSE_STATE.to_string()),
    );
    let clickhouse = ClickHouseClient::from_env()?;
    let mut state = ParseState::load(&state_path).await?;
    let mut entries = fs::read_dir(&output_dir).await?;
    let mut parsed_count = 0;
    let mut duplicate_count = 0;
    let mut processed_match_ids = HashSet::new();

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("pb") {
            continue;
        }
        let Some(job_id) = path.file_stem().and_then(|name| name.to_str()) else {
            continue;
        };
        if state.processed.contains_key(job_id) {
            continue;
        }
        let bytes = fs::read(&path).await?;
        let contents = metadata::decode_metadata(&bytes)?;
        let match_id = metadata::metadata_match_id(&contents)?;
        let data_version = metadata::metadata_version(&bytes);
        let check_clickhouse = !processed_match_ids.contains(&match_id)
            && !state.reprocessed_matches.contains(&match_id)
            && !state.reprocess_existing_matches;
        let exists_in_clickhouse = if check_clickhouse {
            clickhouse.match_is_processed(match_id).await?
        } else {
            false
        };
        let already_processed = should_skip_duplicate_match(
            match_id,
            &processed_match_ids,
            &state.reprocessed_matches,
            exists_in_clickhouse,
            state.reprocess_existing_matches,
        );
        if already_processed {
            state.processed.insert(job_id.to_string(), data_version);
            state.save().await?;
            processed_match_ids.insert(match_id);
            duplicate_count += 1;
            continue;
        }
        let mut parsed = metadata::parse_decoded_metadata(contents, data_version)?;
        let public_id = match state.public_ids.entry(parsed.match_row.match_id) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let public_id = generate_public_id()?;
                entry.insert(public_id.clone());
                public_id
            }
        };
        parsed.match_row.public_id = public_id;
        state.save().await?;
        clickhouse
            .insert(
                "matches",
                MATCH_COLUMNS,
                std::slice::from_ref(&parsed.match_row),
            )
            .await?;
        clickhouse
            .insert("match_players", PLAYER_COLUMNS, &parsed.player_rows)
            .await?;
        clickhouse
            .insert("match_player_stats", STAT_COLUMNS, &parsed.stat_rows)
            .await?;
        clickhouse
            .insert("match_player_items", ITEM_COLUMNS, &parsed.item_rows)
            .await?;
        clickhouse
            .insert("match_damage", DAMAGE_COLUMNS, &parsed.damage_rows)
            .await?;
        clickhouse
            .insert("match_deaths", DEATH_COLUMNS, &parsed.death_rows)
            .await?;
        clickhouse
            .insert(
                "match_player_positions",
                POSITION_COLUMNS,
                &parsed.position_rows,
            )
            .await?;
        clickhouse
            .insert(
                "match_objectives",
                OBJECTIVE_COLUMNS,
                &parsed.objective_rows,
            )
            .await?;
        clickhouse
            .insert(
                "match_processed_events",
                EVENT_COLUMNS,
                &[MatchProcessedEventRow {
                    match_id: parsed.match_row.match_id,
                    public_id: parsed.match_row.public_id.clone(),
                    start_time: parsed.match_row.start_time.unwrap_or_default(),
                    processed_at: now_seconds(),
                    metadata_version: parsed.match_row.metadata_version,
                }],
            )
            .await?;
        state
            .processed
            .insert(job_id.to_string(), parsed.match_row.metadata_version);
        processed_match_ids.insert(match_id);
        if state.reprocess_existing_matches {
            state.reprocessed_matches.insert(match_id);
        }
        state.save().await?;
        parsed_count += 1;
    }

    state.reprocess_existing_matches = false;
    state.reprocessed_matches.clear();
    state.save().await?;
    println!("metadata parser: processed={parsed_count} duplicates={duplicate_count}");
    Ok(())
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_public_id() -> Result<String, std::io::Error> {
    let mut bytes = [0_u8; 16];
    fill_random(&mut bytes).map_err(std::io::Error::other)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(format!(
        "{}-{}-{}-{}-{}",
        hex::encode(&bytes[0..4]),
        hex::encode(&bytes[4..6]),
        hex::encode(&bytes[6..8]),
        hex::encode(&bytes[8..10]),
        hex::encode(&bytes[10..16]),
    ))
}
