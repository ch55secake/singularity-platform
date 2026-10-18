use valveprotos::deadlock::c_msg_match_meta_data_contents::{
    MidBoss, Objective, Players, Position,
};
use valveprotos::deadlock::{CMsgMatchPlayerDamageMatrix, CMsgMatchPlayerPathsData};

use super::{DamageRow, DeathRow, ObjectiveRow, PlayerPositionRow};

pub(super) fn objective_rows(
    match_id: u64,
    objectives: &[Objective],
    mid_bosses: &[MidBoss],
    metadata_version: u64,
) -> Vec<ObjectiveRow> {
    let mut rows = Vec::new();
    for objective in objectives {
        let Some(game_time_s) = objective.destroyed_time_s.filter(|time| *time > 0) else {
            continue;
        };
        let Some((objective_id, team)) = objective_identity(objective) else {
            continue;
        };
        rows.push(ObjectiveRow {
            match_id,
            event_index: rows.len() as u32,
            objective_id,
            team,
            game_time_s,
            metadata_version,
        });
    }
    for boss in mid_bosses {
        let Some(game_time_s) = boss.destroyed_time_s.filter(|time| *time > 0) else {
            continue;
        };
        rows.push(ObjectiveRow {
            match_id,
            event_index: rows.len() as u32,
            objective_id: 16,
            team: boss.team_claimed.unwrap_or_default(),
            game_time_s,
            metadata_version,
        });
    }
    rows.sort_by_key(|row| (row.game_time_s, row.event_index));
    for (event_index, row) in rows.iter_mut().enumerate() {
        row.event_index = event_index as u32;
    }
    rows
}

pub(super) fn objective_identity(objective: &Objective) -> Option<(u32, i32)> {
    let legacy = objective.legacy_objective_id;
    let legacy_identity = match legacy {
        Some(0..=15) => legacy.map(|objective_id| (objective_id as u32, 0)),
        Some(16..=31) => legacy.map(|objective_id| ((objective_id - 16) as u32, 1)),
        _ => None,
    };
    let objective_id = objective
        .team_objective_id
        .and_then(|objective_id| u32::try_from(objective_id).ok())
        .or_else(|| legacy_identity.map(|(objective_id, _)| objective_id))?;
    let team = objective
        .team
        .or_else(|| legacy_identity.map(|(_, team)| team))
        .unwrap_or_default();
    Some((objective_id, team))
}

pub(super) fn damage_rows(
    match_id: u64,
    matrix: Option<&CMsgMatchPlayerDamageMatrix>,
    metadata_version: u64,
) -> Vec<DamageRow> {
    let Some(matrix) = matrix else {
        return Vec::new();
    };
    let source_details = matrix.source_details.as_ref();
    let mut rows = Vec::new();
    for dealer in &matrix.damage_dealers {
        let dealer_slot = dealer.dealer_player_slot.unwrap_or_default();
        for source in &dealer.damage_sources {
            let source_details_index = source.source_details_index.unwrap_or_default();
            let stat_type = source_details
                .and_then(|details| details.stat_type.get(source_details_index as usize))
                .copied()
                .unwrap_or(-1);
            let source_name = source_details
                .and_then(|details| details.source_name.get(source_details_index as usize))
                .cloned()
                .unwrap_or_else(|| format!("unknown:{source_details_index}"));
            for target in &source.damage_to_players {
                let target_slot = target.target_player_slot.unwrap_or_default();
                for (sample_index, raw_cumulative_damage) in
                    target.damage.iter().copied().enumerate()
                {
                    let sample_time_s = matrix
                        .sample_time_s
                        .get(sample_index)
                        .copied()
                        .unwrap_or_default();
                    rows.push(DamageRow {
                        match_id,
                        dealer_slot,
                        target_slot,
                        source_details_index,
                        stat_type,
                        source_name: source_name.clone(),
                        sample_time_s,
                        raw_cumulative_damage,
                        cumulative_damage: f64::from(raw_cumulative_damage) / 2.0,
                        metadata_version,
                    });
                }
            }
        }
    }
    rows
}

pub(super) fn death_rows(
    match_id: u64,
    players: &[Players],
    metadata_version: u64,
) -> Vec<DeathRow> {
    players
        .iter()
        .flat_map(|player| {
            let victim_slot = player.player_slot.unwrap_or_default();
            player
                .death_details
                .iter()
                .enumerate()
                .map(move |(death_index, death)| {
                    let (victim_x, victim_y, victim_z) = position_fields(death.death_pos.as_ref());
                    let (killer_x, killer_y, killer_z) = position_fields(death.killer_pos.as_ref());
                    DeathRow {
                        match_id,
                        victim_slot,
                        death_index: death_index as u32,
                        killer_slot: death.killer_player_slot.unwrap_or_default(),
                        game_time_s: death.game_time_s.unwrap_or_default(),
                        time_to_kill_s: death.time_to_kill_s,
                        victim_x,
                        victim_y,
                        victim_z,
                        killer_x,
                        killer_y,
                        killer_z,
                        death_duration_s: death.death_duration_s.unwrap_or_default(),
                        metadata_version,
                    }
                })
        })
        .collect()
}

fn position_fields(position: Option<&Position>) -> (Option<f32>, Option<f32>, Option<f32>) {
    position
        .map(|position| (position.x, position.y, position.z))
        .unwrap_or((None, None, None))
}

pub(super) fn position_rows(
    match_id: u64,
    paths: Option<&CMsgMatchPlayerPathsData>,
    metadata_version: u64,
) -> Vec<PlayerPositionRow> {
    let Some(paths) = paths else {
        return Vec::new();
    };
    let interval_s = paths.interval_s.unwrap_or(1.0);
    let x_resolution = paths.x_resolution.unwrap_or_default();
    let y_resolution = paths.y_resolution.unwrap_or_default();
    let mut rows = Vec::new();
    for path in &paths.paths {
        let sample_count = [
            path.x_pos.len(),
            path.y_pos.len(),
            path.health.len(),
            path.combat_type.len(),
            path.move_type.len(),
        ]
        .into_iter()
        .max()
        .unwrap_or_default();
        for sample_index in 0..sample_count {
            let x = world_coordinate(
                path.x_min,
                path.x_max,
                path.x_pos.get(sample_index).copied(),
                x_resolution,
            );
            let y = world_coordinate(
                path.y_min,
                path.y_max,
                path.y_pos.get(sample_index).copied(),
                y_resolution,
            );
            rows.push(PlayerPositionRow {
                match_id,
                player_slot: path.player_slot.unwrap_or_default(),
                sample_index: sample_index as u32,
                time_s: sample_index as f32 * interval_s,
                x,
                y,
                health: path.health.get(sample_index).copied(),
                combat_type: path.combat_type.get(sample_index).copied(),
                move_type: path.move_type.get(sample_index).copied(),
                metadata_version,
            });
        }
    }
    rows
}

fn world_coordinate(
    minimum: Option<f32>,
    maximum: Option<f32>,
    position: Option<u32>,
    resolution: u32,
) -> Option<f32> {
    let (minimum, maximum, position) = (minimum?, maximum?, position?);
    if resolution == 0 {
        return None;
    }
    Some(minimum + position as f32 / resolution as f32 * (maximum - minimum))
}
