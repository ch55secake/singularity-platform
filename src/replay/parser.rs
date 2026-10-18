use std::{
    collections::BTreeMap,
    fs::File as StdFile,
    io::BufReader,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Result as AnyResult;
use haste::{
    demofile::DemoFile,
    entities::{DeltaHeader, Entity, deadlock_coord_from_cell, fkey_from_path},
    fxhash,
    parser::{Context, Parser, Visitor},
};
use serde::Serialize;

pub(super) const PARSER_VERSION: &str = "haste-0.0.0-player-position-v1";
pub(super) const MODEL_VERSION: u64 = 1;
const POSITION_SAMPLE_TICKS: i32 = 60;

const DEADLOCK_PLAYERPAWN_ENTITY: u64 = fxhash::hash_bytes(b"CCitadelPlayerPawn");
const CELL_X: u64 = fkey_from_path(&[
    "CBodyComponent",
    "m_skeletonInstance",
    "m_vecOrigin",
    "m_cellX",
]);
const CELL_Y: u64 = fkey_from_path(&[
    "CBodyComponent",
    "m_skeletonInstance",
    "m_vecOrigin",
    "m_cellY",
]);
const CELL_Z: u64 = fkey_from_path(&[
    "CBodyComponent",
    "m_skeletonInstance",
    "m_vecOrigin",
    "m_cellZ",
]);
const VEC_X: u64 = fkey_from_path(&[
    "CBodyComponent",
    "m_skeletonInstance",
    "m_vecOrigin",
    "m_vecX",
]);
const VEC_Y: u64 = fkey_from_path(&[
    "CBodyComponent",
    "m_skeletonInstance",
    "m_vecOrigin",
    "m_vecY",
]);
const VEC_Z: u64 = fkey_from_path(&[
    "CBodyComponent",
    "m_skeletonInstance",
    "m_vecOrigin",
    "m_vecZ",
]);

#[derive(Debug, Clone, Serialize)]
pub(super) struct ReplayPositionRow {
    match_id: u64,
    entity_index: i32,
    tick: i32,
    time_s: f32,
    x: f32,
    y: f32,
    z: f32,
    parser_version: String,
    model_version: u64,
}

#[derive(Debug, Clone)]
pub(super) struct ReplayModel {
    pub(super) positions: Vec<ReplayPositionRow>,
}

#[derive(Default)]
struct ReplayVisitorState {
    next_sample_tick: Option<i32>,
    latest_positions: BTreeMap<i32, [f32; 3]>,
    positions: Vec<(i32, i32, f32, [f32; 3])>,
}

#[derive(Clone)]
struct ReplayVisitor {
    state: Arc<Mutex<ReplayVisitorState>>,
}

pub(super) fn parse_replay(
    match_id: u64,
    path: &Path,
    model_version: u64,
) -> Result<ReplayModel, &'static str> {
    let file = StdFile::open(path).map_err(|_| "parser_input_failed")?;
    let demo = DemoFile::start_reading(BufReader::new(file)).map_err(|_| "invalid_demo")?;
    let state = Arc::new(Mutex::new(ReplayVisitorState::default()));
    let visitor = ReplayVisitor {
        state: Arc::clone(&state),
    };
    let mut parser = Parser::from_stream_with_visitor(demo, visitor).map_err(|_| "invalid_demo")?;
    parser.run_to_end().map_err(|_| "parser_failed")?;
    drop(parser);
    let state = Arc::try_unwrap(state)
        .map_err(|_| "parser_state_failed")?
        .into_inner()
        .map_err(|_| "parser_state_failed")?;
    Ok(ReplayModel {
        positions: state
            .positions
            .into_iter()
            .map(
                |(entity_index, tick, time_s, [x, y, z])| ReplayPositionRow {
                    match_id,
                    entity_index,
                    tick,
                    time_s,
                    x,
                    y,
                    z,
                    parser_version: PARSER_VERSION.to_string(),
                    model_version,
                },
            )
            .collect(),
    })
}

fn entity_position(entity: &Entity) -> Option<[f32; 3]> {
    let cell_x: u16 = entity.get_value(&CELL_X)?;
    let cell_y: u16 = entity.get_value(&CELL_Y)?;
    let cell_z: u16 = entity.get_value(&CELL_Z)?;
    let vec_x: f32 = entity.get_value(&VEC_X)?;
    let vec_y: f32 = entity.get_value(&VEC_Y)?;
    let vec_z: f32 = entity.get_value(&VEC_Z)?;
    Some([
        deadlock_coord_from_cell(cell_x, vec_x),
        deadlock_coord_from_cell(cell_y, vec_y),
        deadlock_coord_from_cell(cell_z, vec_z),
    ])
}

impl Visitor for ReplayVisitor {
    fn on_entity(
        &mut self,
        ctx: &Context,
        _delta_header: DeltaHeader,
        entity: &Entity,
    ) -> AnyResult<()> {
        if !entity.serializer_name_heq(DEADLOCK_PLAYERPAWN_ENTITY) {
            return Ok(());
        }
        let Some(position) = entity_position(entity) else {
            return Ok(());
        };
        let tick = ctx.tick();
        if tick < 0 {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("replay state lock poisoned"))?;
        if state.next_sample_tick.is_none() {
            state.next_sample_tick = Some(tick);
        }
        state.latest_positions.insert(entity.index(), position);
        Ok(())
    }

    fn on_tick_end(&mut self, ctx: &Context) -> AnyResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("replay state lock poisoned"))?;
        let Some(mut sample_tick) = state.next_sample_tick else {
            return Ok(());
        };
        while sample_tick <= ctx.tick() {
            let latest_positions = state.latest_positions.clone();
            for (entity_index, position) in latest_positions {
                state.positions.push((
                    entity_index,
                    sample_tick,
                    sample_tick as f32 * ctx.tick_interval(),
                    position,
                ));
            }
            sample_tick = sample_tick.saturating_add(POSITION_SAMPLE_TICKS);
        }
        state.next_sample_tick = Some(sample_tick);
        Ok(())
    }
}
