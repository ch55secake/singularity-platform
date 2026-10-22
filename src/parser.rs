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
mod worker;

use rows::*;

pub use worker::run;

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
