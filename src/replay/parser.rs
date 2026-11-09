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

pub(super) const PARSER_VERSION: &str = "haste-0.0.0-player-position-resources-v3";
pub(super) const MODEL_VERSION: u64 = 3;
const POSITION_SAMPLE_TICKS: i32 = 60;

const DEADLOCK_PLAYERPAWN_ENTITY: u64 = fxhash::hash_bytes(b"CCitadelPlayerPawn");
const NEUTRAL_TROOPER_ENTITY: u64 = fxhash::hash_bytes(b"CNPC_TrooperNeutral");
const SINNERS_SACRIFICE_ENTITY: u64 = fxhash::hash_bytes(b"CNPC_Neutral_SinnersSacrifice");
const SINNERS_SACRIFICE_HIDEOUT_ENTITY: u64 =
    fxhash::hash_bytes(b"CNPC_Neutral_SinnersSacrifice_Hideout");
const HEALTH: u64 = fkey_from_path(&["m_iHealth"]);
const LIFE_STATE: u64 = fkey_from_path(&["m_lifeState"]);
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

#[derive(Debug, Clone, Serialize)]
pub(super) struct ReplayResourceRow {
    match_id: u64,
    entity_index: i32,
    event_index: u32,
    tick: i32,
    time_s: f32,
    resource_kind: String,
    active: u8,
    x: f32,
    y: f32,
    z: f32,
    parser_version: String,
    model_version: u64,
}

#[derive(Debug, Clone)]
pub(super) struct ReplayModel {
    pub(super) positions: Vec<ReplayPositionRow>,
    pub(super) resources: Vec<ReplayResourceRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ResourceKind {
    Neutral,
    Sinner,
}

impl ResourceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Neutral => "neutral",
            Self::Sinner => "sinner",
        }
    }
}

#[derive(Default)]
struct ReplayVisitorState {
    next_sample_tick: Option<i32>,
    latest_positions: BTreeMap<i32, [f32; 3]>,
    positions: Vec<(i32, i32, f32, [f32; 3])>,
    active_resources: BTreeMap<(ResourceKind, i32), [f32; 3]>,
    resources: Vec<(i32, i32, f32, ResourceKind, bool, [f32; 3])>,
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
        resources: state
            .resources
            .into_iter()
            .enumerate()
            .map(
                |(event_index, (entity_index, tick, time_s, kind, active, [x, y, z]))| {
                    ReplayResourceRow {
                        match_id,
                        entity_index,
                        event_index: event_index as u32,
                        tick,
                        time_s,
                        resource_kind: kind.as_str().to_string(),
                        active: u8::from(active),
                        x,
                        y,
                        z,
                        parser_version: PARSER_VERSION.to_string(),
                        model_version,
                    }
                },
            )
            .collect(),
    })
}

fn record_resource_transition(
    state: &mut ReplayVisitorState,
    entity_index: i32,
    tick: i32,
    time_s: f32,
    kind: ResourceKind,
    active: bool,
    position: Option<[f32; 3]>,
) {
    let key = (kind, entity_index);
    if active {
        let Some(position) = position else {
            return;
        };
        if state.active_resources.contains_key(&key) {
            return;
        }
        state.active_resources.insert(key, position);
        state
            .resources
            .push((entity_index, tick, time_s, kind, true, position));
    } else if let Some(position) = state.active_resources.remove(&key) {
        state
            .resources
            .push((entity_index, tick, time_s, kind, false, position));
    }
}

fn deactivate_entity_resources(
    state: &mut ReplayVisitorState,
    entity_index: i32,
    tick: i32,
    time_s: f32,
) {
    for kind in [ResourceKind::Neutral, ResourceKind::Sinner] {
        record_resource_transition(state, entity_index, tick, time_s, kind, false, None);
    }
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

fn neutral_is_alive(life_state: Option<u8>, health: Option<i64>) -> bool {
    // Position and class identify a neutral entity. Health fields are not sent
    // in every delta, so only explicit dead values should remove a camp marker.
    !life_state.is_some_and(|state| state != 0) && !health.is_some_and(|health| health <= 0)
}

impl Visitor for ReplayVisitor {
    fn on_entity(
        &mut self,
        ctx: &Context,
        delta_header: DeltaHeader,
        entity: &Entity,
    ) -> AnyResult<()> {
        let is_neutral = entity.serializer_name_heq(NEUTRAL_TROOPER_ENTITY);
        let is_sinner = entity.serializer_name_heq(SINNERS_SACRIFICE_ENTITY)
            || entity.serializer_name_heq(SINNERS_SACRIFICE_HIDEOUT_ENTITY);
        let is_player = entity.serializer_name_heq(DEADLOCK_PLAYERPAWN_ENTITY);

        if delta_header == DeltaHeader::DELETE {
            let tick = ctx.tick();
            if tick >= 0 {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("replay state lock poisoned"))?;
                deactivate_entity_resources(
                    &mut state,
                    entity.index(),
                    tick,
                    tick as f32 * ctx.tick_interval(),
                );
            }
            return Ok(());
        }
        if delta_header == DeltaHeader::LEAVE && (is_neutral || is_sinner) {
            return Ok(());
        }
        if !is_neutral && !is_sinner && !is_player {
            return Ok(());
        }

        let tick = ctx.tick();
        if tick < 0 {
            return Ok(());
        }
        let time_s = tick as f32 * ctx.tick_interval();
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("replay state lock poisoned"))?;

        if is_neutral {
            record_resource_transition(
                &mut state,
                entity.index(),
                tick,
                time_s,
                ResourceKind::Sinner,
                false,
                None,
            );
        } else if is_sinner {
            record_resource_transition(
                &mut state,
                entity.index(),
                tick,
                time_s,
                ResourceKind::Neutral,
                false,
                None,
            );
        }
        if is_neutral {
            if !neutral_is_alive(
                entity.get_value::<u8>(&LIFE_STATE),
                entity.get_value::<i64>(&HEALTH),
            ) {
                record_resource_transition(
                    &mut state,
                    entity.index(),
                    tick,
                    time_s,
                    ResourceKind::Neutral,
                    false,
                    None,
                );
                return Ok(());
            }
            let Some(position) = entity_position(entity) else {
                return Ok(());
            };
            record_resource_transition(
                &mut state,
                entity.index(),
                tick,
                time_s,
                ResourceKind::Neutral,
                true,
                Some(position),
            );
            return Ok(());
        }

        if is_sinner {
            let Some(position) = entity_position(entity) else {
                return Ok(());
            };
            record_resource_transition(
                &mut state,
                entity.index(),
                tick,
                time_s,
                ResourceKind::Sinner,
                true,
                Some(position),
            );
            return Ok(());
        }

        let Some(position) = entity_position(entity) else {
            return Ok(());
        };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_transitions_only_emit_presence_changes() {
        let mut state = ReplayVisitorState::default();
        let position = [12.0, -34.0, 0.0];

        record_resource_transition(
            &mut state,
            8,
            120,
            2.0,
            ResourceKind::Neutral,
            true,
            Some(position),
        );
        record_resource_transition(
            &mut state,
            8,
            121,
            2.02,
            ResourceKind::Neutral,
            true,
            Some([20.0, -30.0, 0.0]),
        );
        record_resource_transition(&mut state, 8, 240, 4.0, ResourceKind::Neutral, false, None);

        assert_eq!(state.resources.len(), 2);
        assert!(state.resources[0].4);
        assert!(!state.resources[1].4);
        assert_eq!(state.resources[1].5, position);
        assert!(state.active_resources.is_empty());
    }

    #[test]
    fn neutral_is_considered_alive_when_health_fields_are_missing() {
        assert!(neutral_is_alive(None, None));
        assert!(neutral_is_alive(Some(0), None));
        assert!(neutral_is_alive(None, Some(10)));
    }

    #[test]
    fn neutral_is_considered_inactive_only_for_explicit_dead_values() {
        assert!(!neutral_is_alive(Some(1), Some(10)));
        assert!(!neutral_is_alive(Some(0), Some(0)));
    }
}
