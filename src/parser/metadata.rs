use prost::Message;
use sha2::{Digest, Sha256};
use valveprotos::deadlock::{CMsgMatchMetaData, CMsgMatchMetaDataContents};

use super::{MatchRow, ParsedMatch, match_events, player_rows};

#[cfg(test)]
pub(super) fn parse_metadata(bytes: &[u8]) -> Result<ParsedMatch, std::io::Error> {
    parse_decoded_metadata(decode_metadata(bytes)?, metadata_version(bytes))
}

pub(super) fn decode_metadata(bytes: &[u8]) -> Result<CMsgMatchMetaDataContents, std::io::Error> {
    decode_contents(bytes).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid metadata protobuf")
    })
}

pub(super) fn metadata_match_id(
    contents: &CMsgMatchMetaDataContents,
) -> Result<u64, std::io::Error> {
    contents
        .match_info
        .as_ref()
        .and_then(|info| info.match_id)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "metadata has no match id")
        })
}

pub(super) fn parse_decoded_metadata(
    contents: CMsgMatchMetaDataContents,
    metadata_version: u64,
) -> Result<ParsedMatch, std::io::Error> {
    let info = contents.match_info.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "metadata has no match info",
        )
    })?;
    let match_id = info.match_id.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "metadata has no match id")
    })?;
    let match_row = MatchRow {
        match_id,
        public_id: String::new(),
        start_time: info.start_time,
        duration_s: info.duration_s,
        winning_team: info.winning_team,
        game_mode: info.game_mode,
        match_mode: info.match_mode,
        average_badge_team0: info.average_badge_team0,
        average_badge_team1: info.average_badge_team1,
        metadata_version,
    };
    let mut player_rows = Vec::with_capacity(info.players.len());
    let mut stat_rows = Vec::new();
    let mut item_rows = Vec::new();
    let damage_rows =
        match_events::damage_rows(match_id, info.damage_matrix.as_ref(), metadata_version);
    let death_rows = match_events::death_rows(match_id, &info.players, metadata_version);
    let position_rows =
        match_events::position_rows(match_id, info.match_paths.as_ref(), metadata_version);
    let objective_rows =
        match_events::objective_rows(match_id, &info.objectives, &info.mid_boss, metadata_version);
    for player in info.players {
        player_rows.push(player_rows::player_row(match_id, &player, metadata_version));
        item_rows.extend(
            player.items.iter().map(|item| {
                player_rows::player_item_row(match_id, &player, item, metadata_version)
            }),
        );
        stat_rows.extend(
            player.stats.iter().map(|stats| {
                player_rows::player_stat_row(match_id, &player, stats, metadata_version)
            }),
        );
    }
    Ok(ParsedMatch {
        match_row,
        player_rows,
        stat_rows,
        item_rows,
        damage_rows,
        death_rows,
        position_rows,
        objective_rows,
    })
}

/// Valve's `.meta` files hold a `CMsgMatchMetaData` wrapper (version, match ID)
/// whose `match_details` bytes are the `CMsgMatchMetaDataContents`. Bare
/// contents are still accepted.
fn decode_contents(bytes: &[u8]) -> Option<CMsgMatchMetaDataContents> {
    if let Ok(outer) = CMsgMatchMetaData::decode(bytes)
        && let Some(details) = outer.match_details
        && let Ok(contents) = CMsgMatchMetaDataContents::decode(details.as_slice())
        && contents.match_info.is_some()
    {
        return Some(contents);
    }
    CMsgMatchMetaDataContents::decode(bytes).ok()
}

pub(super) fn metadata_version(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_le_bytes(digest[..8].try_into().expect("digest has eight bytes"))
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use valveprotos::deadlock::c_msg_match_meta_data_contents::{
        Deaths, Items, MatchInfo, MidBoss, Objective, PlayerStats, Players, Position,
    };
    use valveprotos::deadlock::c_msg_match_player_damage_matrix::{
        DamageDealer, DamageSource, DamageToPlayer, SourceDetails,
    };
    use valveprotos::deadlock::c_msg_match_player_paths_data::Path as PlayerPath;

    use super::*;

    #[test]
    fn falls_back_to_legacy_team_objective_ids() {
        let objective = Objective {
            legacy_objective_id: Some(17),
            ..Default::default()
        };

        assert_eq!(
            super::super::match_events::objective_identity(&objective),
            Some((1, 1))
        );
    }

    #[test]
    fn decodes_match_players_and_timeline_rows() {
        let message = CMsgMatchMetaDataContents {
            match_info: Some(MatchInfo {
                match_id: Some(42),
                duration_s: Some(1_800),
                match_paths: Some(valveprotos::deadlock::CMsgMatchPlayerPathsData {
                    version: Some(1),
                    interval_s: Some(1.0),
                    x_resolution: Some(100),
                    y_resolution: Some(200),
                    paths: vec![PlayerPath {
                        player_slot: Some(3),
                        x_min: Some(-10.0),
                        y_min: Some(-20.0),
                        x_max: Some(10.0),
                        y_max: Some(20.0),
                        x_pos: vec![0, 50],
                        y_pos: vec![100, 200],
                        health: vec![100, 90],
                        combat_type: vec![1, 2],
                        move_type: vec![0, 3],
                    }],
                }),
                damage_matrix: Some(valveprotos::deadlock::CMsgMatchPlayerDamageMatrix {
                    damage_dealers: vec![DamageDealer {
                        dealer_player_slot: Some(3),
                        damage_sources: vec![DamageSource {
                            source_details_index: Some(0),
                            damage_to_players: vec![DamageToPlayer {
                                target_player_slot: Some(4),
                                damage: vec![20, 41],
                            }],
                        }],
                    }],
                    sample_time_s: vec![60, 120],
                    source_details: Some(SourceDetails {
                        stat_type: vec![0],
                        source_name: vec!["ability_test".to_string()],
                    }),
                }),
                objectives: vec![
                    Objective {
                        team_objective_id: Some(1),
                        team: Some(1),
                        destroyed_time_s: Some(240),
                        ..Default::default()
                    },
                    Objective {
                        team_objective_id: Some(5),
                        team: Some(0),
                        destroyed_time_s: None,
                        ..Default::default()
                    },
                ],
                mid_boss: vec![MidBoss {
                    team_claimed: Some(0),
                    destroyed_time_s: Some(900),
                    ..Default::default()
                }],
                winning_team: Some(2),
                players: vec![Players {
                    account_id: Some(7),
                    player_slot: Some(3),
                    hero_id: Some(10),
                    death_details: vec![Deaths {
                        game_time_s: Some(300),
                        time_to_kill_s: Some(2.5),
                        killer_player_slot: Some(4),
                        death_pos: Some(Position {
                            x: Some(10.0),
                            y: Some(20.0),
                            z: Some(30.0),
                        }),
                        killer_pos: Some(Position {
                            x: Some(40.0),
                            y: Some(50.0),
                            z: Some(60.0),
                        }),
                        death_duration_s: Some(4),
                    }],
                    items: vec![Items {
                        game_time_s: Some(95),
                        item_id: Some(1_841_661_392),
                        upgrade_id: Some(0),
                        upgrade_info: Some(196_608),
                        ..Default::default()
                    }],
                    kills: Some(8),
                    deaths: Some(2),
                    assists: Some(11),
                    stats: vec![PlayerStats {
                        time_stamp_s: Some(180),
                        net_worth: Some(4_200),
                        player_damage: Some(9_000),
                        shots_hit: Some(1_701),
                        shots_missed: Some(1_503),
                        hero_bullets_hit: Some(425),
                        hero_bullets_hit_crit: Some(49),
                        bullet_kills: Some(5),
                        ability_kills: Some(2),
                        melee_kills: Some(1),
                        headshot_kills: Some(3),
                        player_damage_taken: Some(7_500),
                        damage_mitigated: Some(1_200),
                        damage_absorbed: Some(350),
                        self_damage: Some(25),
                        heal_prevented: Some(100),
                        heal_lost: Some(50),
                        teammate_healing: Some(400),
                        teammate_barriering: Some(200),
                        weapon_power: Some(12),
                        tech_power: Some(8),
                        max_health: Some(850),
                        level: Some(6),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        };
        let bytes = message.encode_to_vec();

        let parsed = parse_metadata(&bytes).expect("metadata parses");

        assert_eq!(parsed.match_row.match_id, 42);
        assert_eq!(parsed.match_row.winning_team, Some(2));
        assert_eq!(parsed.player_rows[0].kills, Some(8));
        assert_eq!(parsed.item_rows.len(), 1);
        assert_eq!(parsed.item_rows[0].item_id, 1_841_661_392);
        assert!(parsed.item_rows[0].upgrade_unlocked);
        assert_eq!(parsed.item_rows[0].upgrade_tier, 1);
        assert_eq!(parsed.death_rows.len(), 1);
        assert_eq!(parsed.death_rows[0].victim_slot, 3);
        assert_eq!(parsed.death_rows[0].killer_slot, 4);
        assert_eq!(parsed.death_rows[0].victim_x, Some(10.0));
        assert_eq!(parsed.death_rows[0].killer_z, Some(60.0));
        assert_eq!(parsed.position_rows.len(), 2);
        assert_eq!(parsed.position_rows[0].x, Some(-10.0));
        assert_eq!(parsed.position_rows[0].y, Some(0.0));
        assert_eq!(parsed.position_rows[1].x, Some(0.0));
        assert_eq!(parsed.position_rows[1].y, Some(20.0));
        assert_eq!(parsed.position_rows[1].combat_type, Some(2));
        assert_eq!(parsed.position_rows[1].move_type, Some(3));
        assert_eq!(parsed.objective_rows.len(), 2);
        assert_eq!(parsed.objective_rows[0].objective_id, 1);
        assert_eq!(parsed.objective_rows[0].team, 1);
        assert_eq!(parsed.objective_rows[0].game_time_s, 240);
        assert_eq!(parsed.objective_rows[1].objective_id, 16);
        assert_eq!(parsed.objective_rows[1].team, 0);
        assert_eq!(parsed.objective_rows[1].game_time_s, 900);
        assert_eq!(parsed.damage_rows.len(), 2);
        assert_eq!(parsed.damage_rows[1].source_name, "ability_test");
        assert_eq!(parsed.damage_rows[1].sample_time_s, 120);
        assert_eq!(parsed.damage_rows[1].raw_cumulative_damage, 41);
        assert_eq!(parsed.damage_rows[1].cumulative_damage, 20.5);
        assert_eq!(parsed.stat_rows[0].snapshot_time_s, 180);
        assert_eq!(parsed.stat_rows[0].player_damage, Some(9_000));
        assert_eq!(parsed.stat_rows[0].shots_hit, Some(1_701));
        assert_eq!(parsed.stat_rows[0].hero_bullets_hit_crit, Some(49));
        assert_eq!(parsed.stat_rows[0].headshot_kills, Some(3));
        assert_eq!(parsed.stat_rows[0].player_damage_taken, Some(7_500));
        assert_eq!(parsed.match_row.metadata_version, metadata_version(&bytes));
    }

    #[test]
    fn decodes_contents_inside_valve_wrapper() {
        let contents = CMsgMatchMetaDataContents {
            match_info: Some(MatchInfo {
                match_id: Some(42),
                duration_s: Some(1_800),
                ..Default::default()
            }),
        };
        let wrapped = CMsgMatchMetaData {
            version: Some(1),
            match_details: Some(contents.encode_to_vec()),
            match_id: Some(42),
        }
        .encode_to_vec();

        let parsed = parse_metadata(&wrapped).expect("wrapped metadata parses");

        assert_eq!(parsed.match_row.match_id, 42);
        assert_eq!(parsed.match_row.duration_s, Some(1_800));
    }

    #[test]
    fn parses_sample_metadata_files() {
        // Decompressed .meta files can be dropped in tests/fixtures/ to check
        // real Valve data; the directory is optional.
        let Ok(entries) = std::fs::read_dir("tests/fixtures") else {
            return;
        };
        for entry in entries.flatten() {
            let bytes = std::fs::read(entry.path()).expect("fixture reads");
            let parsed = parse_metadata(&bytes).expect("sample metadata parses");
            assert!(parsed.match_row.match_id > 0);
        }
    }

    #[test]
    fn rejects_metadata_without_match_info() {
        let error = parse_metadata(&CMsgMatchMetaDataContents::default().encode_to_vec())
            .expect_err("missing match info is invalid");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
