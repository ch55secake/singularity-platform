use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct MatchRow {
    pub(super) match_id: u64,
    pub(super) public_id: String,
    pub(super) start_time: Option<u32>,
    pub(super) duration_s: Option<u32>,
    pub(super) winning_team: Option<i32>,
    pub(super) game_mode: Option<i32>,
    pub(super) match_mode: Option<i32>,
    pub(super) average_badge_team0: Option<u32>,
    pub(super) average_badge_team1: Option<u32>,
    pub(super) metadata_version: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct MatchProcessedEventRow {
    pub(super) match_id: u64,
    pub(super) public_id: String,
    pub(super) start_time: u32,
    pub(super) processed_at: u64,
    pub(super) metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct PlayerRow {
    pub(super) match_id: u64,
    pub(super) account_id: u32,
    pub(super) player_slot: u32,
    pub(super) team: Option<i32>,
    pub(super) hero_id: Option<u32>,
    pub(super) kills: Option<u32>,
    pub(super) deaths: Option<u32>,
    pub(super) assists: Option<u32>,
    pub(super) net_worth: Option<u32>,
    pub(super) last_hits: Option<u32>,
    pub(super) denies: Option<u32>,
    pub(super) level: Option<u32>,
    pub(super) assigned_lane: Option<u32>,
    pub(super) abandon_match_time_s: Option<u32>,
    pub(super) mvp_rank: Option<u32>,
    pub(super) metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct PlayerStatRow {
    pub(super) match_id: u64,
    pub(super) account_id: u32,
    pub(super) player_slot: u32,
    pub(super) snapshot_time_s: u32,
    pub(super) net_worth: Option<u32>,
    pub(super) gold_player: Option<u32>,
    pub(super) gold_player_orbs: Option<u32>,
    pub(super) gold_lane_creep_orbs: Option<u32>,
    pub(super) gold_neutral_creep_orbs: Option<u32>,
    pub(super) gold_boss: Option<u32>,
    pub(super) gold_boss_orb: Option<u32>,
    pub(super) gold_treasure: Option<u32>,
    pub(super) gold_denied: Option<u32>,
    pub(super) gold_death_loss: Option<u32>,
    pub(super) gold_lane_creep: Option<u32>,
    pub(super) gold_neutral_creep: Option<u32>,
    pub(super) kills: Option<u32>,
    pub(super) deaths: Option<u32>,
    pub(super) assists: Option<u32>,
    pub(super) creep_kills: Option<u32>,
    pub(super) neutral_kills: Option<u32>,
    pub(super) denies: Option<u32>,
    pub(super) player_damage: Option<u32>,
    pub(super) player_healing: Option<u32>,
    pub(super) self_healing: Option<u32>,
    pub(super) shots_hit: Option<u32>,
    pub(super) shots_missed: Option<u32>,
    pub(super) hero_bullets_hit: Option<u32>,
    pub(super) hero_bullets_hit_crit: Option<u32>,
    pub(super) bullet_kills: Option<u32>,
    pub(super) ability_kills: Option<u32>,
    pub(super) melee_kills: Option<u32>,
    pub(super) headshot_kills: Option<u32>,
    pub(super) player_damage_taken: Option<u32>,
    pub(super) damage_mitigated: Option<u32>,
    pub(super) damage_absorbed: Option<u32>,
    pub(super) self_damage: Option<u32>,
    pub(super) heal_prevented: Option<u32>,
    pub(super) heal_lost: Option<u32>,
    pub(super) teammate_healing: Option<u32>,
    pub(super) teammate_barriering: Option<u32>,
    pub(super) weapon_power: Option<u32>,
    pub(super) tech_power: Option<u32>,
    pub(super) max_health: Option<u32>,
    pub(super) level: Option<u32>,
    pub(super) metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct PlayerItemRow {
    pub(super) match_id: u64,
    pub(super) player_slot: u32,
    pub(super) game_time_s: u32,
    pub(super) item_id: u32,
    pub(super) upgrade_id: u32,
    pub(super) sold_time_s: u32,
    pub(super) flags: u32,
    pub(super) imbued_ability_id: u32,
    pub(super) upgrade_info: u32,
    pub(super) upgrade_unlocked: bool,
    pub(super) upgrade_tier: u32,
    pub(super) metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct DamageRow {
    pub(super) match_id: u64,
    pub(super) dealer_slot: u32,
    pub(super) target_slot: u32,
    pub(super) source_details_index: u32,
    pub(super) stat_type: i32,
    pub(super) source_name: String,
    pub(super) sample_time_s: u32,
    pub(super) raw_cumulative_damage: u32,
    pub(super) cumulative_damage: f64,
    pub(super) metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct DeathRow {
    pub(super) match_id: u64,
    pub(super) victim_slot: u32,
    pub(super) death_index: u32,
    pub(super) killer_slot: u32,
    pub(super) game_time_s: u32,
    pub(super) time_to_kill_s: Option<f32>,
    pub(super) victim_x: Option<f32>,
    pub(super) victim_y: Option<f32>,
    pub(super) victim_z: Option<f32>,
    pub(super) killer_x: Option<f32>,
    pub(super) killer_y: Option<f32>,
    pub(super) killer_z: Option<f32>,
    pub(super) death_duration_s: u32,
    pub(super) metadata_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct PlayerPositionRow {
    pub(super) match_id: u64,
    pub(super) player_slot: u32,
    pub(super) sample_index: u32,
    pub(super) time_s: f32,
    pub(super) x: Option<f32>,
    pub(super) y: Option<f32>,
    pub(super) health: Option<u32>,
    pub(super) combat_type: Option<i32>,
    pub(super) move_type: Option<i32>,
    pub(super) metadata_version: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ObjectiveRow {
    pub(super) match_id: u64,
    pub(super) event_index: u32,
    pub(super) objective_id: u32,
    pub(super) team: i32,
    pub(super) game_time_s: u32,
    pub(super) metadata_version: u64,
}

pub(super) const MATCH_COLUMNS: &str = "match_id, public_id, start_time, duration_s, winning_team, game_mode, match_mode, average_badge_team0, average_badge_team1, metadata_version";
pub(super) const PLAYER_COLUMNS: &str = "match_id, account_id, player_slot, team, hero_id, kills, deaths, assists, net_worth, last_hits, denies, level, assigned_lane, abandon_match_time_s, mvp_rank, metadata_version";
pub(super) const STAT_COLUMNS: &str = "match_id, account_id, player_slot, snapshot_time_s, net_worth, gold_player, gold_player_orbs, gold_lane_creep_orbs, gold_neutral_creep_orbs, gold_boss, gold_boss_orb, gold_treasure, gold_denied, gold_death_loss, gold_lane_creep, gold_neutral_creep, kills, deaths, assists, creep_kills, neutral_kills, denies, player_damage, player_healing, self_healing, shots_hit, shots_missed, hero_bullets_hit, hero_bullets_hit_crit, bullet_kills, ability_kills, melee_kills, headshot_kills, player_damage_taken, damage_mitigated, damage_absorbed, self_damage, heal_prevented, heal_lost, teammate_healing, teammate_barriering, weapon_power, tech_power, max_health, level, metadata_version";
pub(super) const ITEM_COLUMNS: &str = "match_id, player_slot, game_time_s, item_id, upgrade_id, sold_time_s, flags, imbued_ability_id, upgrade_info, upgrade_unlocked, upgrade_tier, metadata_version";
pub(super) const DAMAGE_COLUMNS: &str = "match_id, dealer_slot, target_slot, source_details_index, stat_type, source_name, sample_time_s, raw_cumulative_damage, cumulative_damage, metadata_version";
pub(super) const DEATH_COLUMNS: &str = "match_id, victim_slot, death_index, killer_slot, game_time_s, time_to_kill_s, victim_x, victim_y, victim_z, killer_x, killer_y, killer_z, death_duration_s, metadata_version";
pub(super) const POSITION_COLUMNS: &str = "match_id, player_slot, sample_index, time_s, x, y, health, combat_type, move_type, metadata_version";
pub(super) const OBJECTIVE_COLUMNS: &str =
    "match_id, event_index, objective_id, team, game_time_s, metadata_version";
pub(super) const EVENT_COLUMNS: &str =
    "match_id, public_id, start_time, processed_at, metadata_version";
