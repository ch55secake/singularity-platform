use valveprotos::deadlock::c_msg_match_meta_data_contents::{Items, PlayerStats, Players};

use super::{PlayerItemRow, PlayerRow, PlayerStatRow};

pub(super) fn player_row(match_id: u64, player: &Players, metadata_version: u64) -> PlayerRow {
    PlayerRow {
        match_id,
        account_id: player.account_id.unwrap_or_default(),
        player_slot: player.player_slot.unwrap_or_default(),
        team: player.team,
        hero_id: player.hero_id,
        kills: player.kills,
        deaths: player.deaths,
        assists: player.assists,
        net_worth: player.net_worth,
        last_hits: player.last_hits,
        denies: player.denies,
        level: player.level,
        assigned_lane: player.assigned_lane,
        abandon_match_time_s: player.abandon_match_time_s,
        mvp_rank: player.mvp_rank,
        metadata_version,
    }
}

pub(super) fn player_stat_row(
    match_id: u64,
    player: &Players,
    stats: &PlayerStats,
    metadata_version: u64,
) -> PlayerStatRow {
    PlayerStatRow {
        match_id,
        account_id: player.account_id.unwrap_or_default(),
        player_slot: player.player_slot.unwrap_or_default(),
        snapshot_time_s: stats.time_stamp_s.unwrap_or_default(),
        net_worth: stats.net_worth,
        gold_player: stats.gold_player,
        gold_player_orbs: stats.gold_player_orbs,
        gold_lane_creep_orbs: stats.gold_lane_creep_orbs,
        gold_neutral_creep_orbs: stats.gold_neutral_creep_orbs,
        gold_boss: stats.gold_boss,
        gold_boss_orb: stats.gold_boss_orb,
        gold_treasure: stats.gold_treasure,
        gold_denied: stats.gold_denied,
        gold_death_loss: stats.gold_death_loss,
        gold_lane_creep: stats.gold_lane_creep,
        gold_neutral_creep: stats.gold_neutral_creep,
        kills: stats.kills,
        deaths: stats.deaths,
        assists: stats.assists,
        creep_kills: stats.creep_kills,
        neutral_kills: stats.neutral_kills,
        denies: stats.denies,
        player_damage: stats.player_damage,
        player_healing: stats.player_healing,
        self_healing: stats.self_healing,
        shots_hit: stats.shots_hit,
        shots_missed: stats.shots_missed,
        hero_bullets_hit: stats.hero_bullets_hit,
        hero_bullets_hit_crit: stats.hero_bullets_hit_crit,
        bullet_kills: stats.bullet_kills,
        ability_kills: stats.ability_kills,
        melee_kills: stats.melee_kills,
        headshot_kills: stats.headshot_kills,
        player_damage_taken: stats.player_damage_taken,
        damage_mitigated: stats.damage_mitigated,
        damage_absorbed: stats.damage_absorbed,
        self_damage: stats.self_damage,
        heal_prevented: stats.heal_prevented,
        heal_lost: stats.heal_lost,
        teammate_healing: stats.teammate_healing,
        teammate_barriering: stats.teammate_barriering,
        weapon_power: stats.weapon_power,
        tech_power: stats.tech_power,
        max_health: stats.max_health,
        level: stats.level,
        metadata_version,
    }
}

pub(super) fn player_item_row(
    match_id: u64,
    player: &Players,
    item: &Items,
    metadata_version: u64,
) -> PlayerItemRow {
    let (upgrade_unlocked, upgrade_tier) = decode_upgrade_info(item.upgrade_info);
    PlayerItemRow {
        match_id,
        player_slot: player.player_slot.unwrap_or_default(),
        game_time_s: item.game_time_s.unwrap_or_default(),
        item_id: item.item_id.unwrap_or_default(),
        upgrade_id: item.upgrade_id.unwrap_or_default(),
        sold_time_s: item.sold_time_s.unwrap_or_default(),
        flags: item.flags.unwrap_or_default(),
        imbued_ability_id: item.imbued_ability_id.unwrap_or_default(),
        upgrade_info: item.upgrade_info.unwrap_or_default(),
        upgrade_unlocked,
        upgrade_tier,
        metadata_version,
    }
}

fn decode_upgrade_info(upgrade_info: Option<u32>) -> (bool, u32) {
    upgrade_info
        .map(|value| (value & (1 << 16) != 0, ((value >> 17) & 0xf).count_ones()))
        .unwrap_or_default()
}
