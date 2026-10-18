use std::collections::{BTreeMap, HashMap};

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use serde::Serialize;

use crate::{AppState, ErrorBody};

use super::matches::CorePlayer;
use super::{
    IMPACT_SCORE_VERSION, ImpactMetrics, ImpactScore, Member, ReadApiState, api_error, authorize,
    calculate_impact_scores, member_for, mode_name, non_empty_string, number_f64, query_rows,
};
use super::{load_core, match_context, match_selector};

#[derive(Debug, Serialize)]
struct Aim {
    shots_hit: u32,
    shots_missed: u32,
    hero_hits: u32,
    hero_crits: u32,
}

#[derive(Debug, Serialize)]
struct KillTypes {
    bullet: u32,
    ability: u32,
    melee: u32,
    headshot: u32,
}

#[derive(Debug, Serialize, Clone, Default)]
struct Timeline {
    t: Vec<u32>,
    souls: Vec<u32>,
    hero_damage: Vec<u32>,
}

#[derive(Debug, Serialize, Clone)]
struct DamageSource {
    source: String,
    name: String,
    kind: String,
    icon: Option<String>,
    total: f64,
    by_target: BTreeMap<String, f64>,
}

#[derive(Debug, Serialize, Clone)]
struct BuildEntry {
    t: u32,
    id: u32,
    name: String,
    kind: String,
    tier: Option<u32>,
    icon: Option<String>,
    sold_t: Option<u32>,
}

#[derive(Debug, Serialize)]
struct Player {
    slot: u32,
    team: i32,
    member: Option<Member>,
    hero: super::Hero,
    kills: u32,
    deaths: u32,
    assists: u32,
    souls: u32,
    last_hits: u32,
    denies: u32,
    level: u32,
    hero_damage: u32,
    healing: u32,
    damage_taken: u32,
    damage_mitigated: u32,
    impact: ImpactScore,
    aim: Aim,
    kill_types: KillTypes,
    timeline: Timeline,
    damage_by_source: Vec<DamageSource>,
    build: Vec<BuildEntry>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MatchDetail {
    uuid: String,
    match_id: u64,
    start_time: u32,
    duration_s: u32,
    mode: String,
    winning_team: i32,
    impact_score_version: u32,
    average_badge: [Option<u32>; 2],
    players: Vec<Player>,
    deaths: Vec<match_context::Death>,
    objectives: Vec<match_context::ObjectiveEvent>,
    paths: match_context::Paths,
    map: match_context::MapData,
}

#[derive(Debug, Clone, Default)]
struct FinalStats {
    souls: u32,
    last_hits: u32,
    denies: u32,
    level: u32,
    hero_damage: u32,
    healing: u32,
    damage_taken: u32,
    damage_mitigated: u32,
    shots_hit: u32,
    shots_missed: u32,
    hero_hits: u32,
    hero_crits: u32,
    bullet_kills: u32,
    ability_kills: u32,
    melee_kills: u32,
    headshot_kills: u32,
}

pub(crate) async fn get_match(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
) -> Result<Json<MatchDetail>, (StatusCode, Json<ErrorBody>)> {
    authorize(&headers, &state.read_api)?;
    let selector = match_selector(&identifier)?;
    let Some(core) = load_core(&state.read_api, &selector).await? else {
        return Err(api_error(StatusCode::NOT_FOUND, "match not found"));
    };
    let final_stats = load_final_stats(&state.read_api, core.match_id).await?;
    let timelines = load_timelines(&state.read_api, core.match_id).await?;
    let damage = load_damage(&state.read_api, core.match_id).await?;
    let builds = load_builds(&state.read_api, core.match_id).await?;
    let deaths = match_context::load_deaths(&state.read_api, core.match_id).await?;
    let paths = match_context::load_paths(&state.read_api, core.match_id).await?;
    let map = match_context::load_map(&state.read_api).await?;
    let objectives = match_context::load_objectives(&state.read_api, core.match_id, &map).await?;
    let mut players: Vec<Player> = core
        .players
        .iter()
        .map(|player| {
            build_player(
                player,
                &state.read_api.members,
                &final_stats,
                &timelines,
                &damage,
                &builds,
            )
        })
        .collect();
    let impact_metrics = players
        .iter()
        .map(impact_metrics_from_player)
        .collect::<Vec<_>>();
    let impact_scores = calculate_impact_scores(core.duration_s, &impact_metrics);
    for player in &mut players {
        if let Some(impact) = impact_scores.get(&player.slot) {
            player.impact = *impact;
        }
    }
    Ok(Json(MatchDetail {
        uuid: core.uuid,
        match_id: core.match_id,
        start_time: core.start_time,
        duration_s: core.duration_s,
        mode: mode_name(core.players.len()),
        winning_team: core.winning_team,
        impact_score_version: IMPACT_SCORE_VERSION,
        average_badge: core.average_badge,
        players,
        deaths,
        objectives,
        paths,
        map,
    }))
}

async fn load_final_stats(
    state: &ReadApiState,
    match_id: u64,
) -> Result<HashMap<u32, FinalStats>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT players.player_slot, players.net_worth AS souls, players.last_hits, players.denies, players.level, argMax(ifNull(stats.player_damage, 0), stats.snapshot_time_s) AS hero_damage, argMax(ifNull(stats.player_healing, 0), stats.snapshot_time_s) AS healing, argMax(ifNull(stats.player_damage_taken, 0), stats.snapshot_time_s) AS damage_taken, argMax(ifNull(stats.damage_mitigated, 0), stats.snapshot_time_s) AS damage_mitigated, argMax(ifNull(stats.shots_hit, 0), stats.snapshot_time_s) AS shots_hit, argMax(ifNull(stats.shots_missed, 0), stats.snapshot_time_s) AS shots_missed, argMax(ifNull(stats.hero_bullets_hit, 0), stats.snapshot_time_s) AS hero_hits, argMax(ifNull(stats.hero_bullets_hit_crit, 0), stats.snapshot_time_s) AS hero_crits, argMax(ifNull(stats.bullet_kills, 0), stats.snapshot_time_s) AS bullet_kills, argMax(ifNull(stats.ability_kills, 0), stats.snapshot_time_s) AS ability_kills, argMax(ifNull(stats.melee_kills, 0), stats.snapshot_time_s) AS melee_kills, argMax(ifNull(stats.headshot_kills, 0), stats.snapshot_time_s) AS headshot_kills FROM {db}.match_players AS players LEFT JOIN {db}.match_player_stats AS stats ON stats.match_id = players.match_id AND stats.player_slot = players.player_slot WHERE players.match_id = {match_id} GROUP BY players.player_slot, players.net_worth, players.last_hits, players.denies, players.level",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            Some((
                super::number_u32(&row, "player_slot")?,
                FinalStats {
                    souls: super::number_u32(&row, "souls").unwrap_or_default(),
                    last_hits: super::number_u32(&row, "last_hits").unwrap_or_default(),
                    denies: super::number_u32(&row, "denies").unwrap_or_default(),
                    level: super::number_u32(&row, "level").unwrap_or_default(),
                    hero_damage: super::number_u32(&row, "hero_damage").unwrap_or_default(),
                    healing: super::number_u32(&row, "healing").unwrap_or_default(),
                    damage_taken: super::number_u32(&row, "damage_taken").unwrap_or_default(),
                    damage_mitigated: super::number_u32(&row, "damage_mitigated")
                        .unwrap_or_default(),
                    shots_hit: super::number_u32(&row, "shots_hit").unwrap_or_default(),
                    shots_missed: super::number_u32(&row, "shots_missed").unwrap_or_default(),
                    hero_hits: super::number_u32(&row, "hero_hits").unwrap_or_default(),
                    hero_crits: super::number_u32(&row, "hero_crits").unwrap_or_default(),
                    bullet_kills: super::number_u32(&row, "bullet_kills").unwrap_or_default(),
                    ability_kills: super::number_u32(&row, "ability_kills").unwrap_or_default(),
                    melee_kills: super::number_u32(&row, "melee_kills").unwrap_or_default(),
                    headshot_kills: super::number_u32(&row, "headshot_kills").unwrap_or_default(),
                },
            ))
        })
        .collect())
}

async fn load_timelines(
    state: &ReadApiState,
    match_id: u64,
) -> Result<HashMap<u32, Timeline>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT player_slot, snapshot_time_s, ifNull(net_worth, 0) AS souls, ifNull(player_damage, 0) AS hero_damage FROM {db}.match_player_stats WHERE match_id = {match_id} ORDER BY player_slot, snapshot_time_s",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    let mut timelines: HashMap<u32, Timeline> = HashMap::new();
    for row in rows {
        let Some(slot) = super::number_u32(&row, "player_slot") else {
            continue;
        };
        let timeline = timelines.entry(slot).or_default();
        timeline
            .t
            .push(super::number_u32(&row, "snapshot_time_s").unwrap_or_default());
        timeline
            .souls
            .push(super::number_u32(&row, "souls").unwrap_or_default());
        timeline
            .hero_damage
            .push(super::number_u32(&row, "hero_damage").unwrap_or_default());
    }
    Ok(timelines)
}

async fn load_damage(
    state: &ReadApiState,
    match_id: u64,
) -> Result<HashMap<u32, Vec<DamageSource>>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT d.dealer_slot, d.target_slot, d.source_name, d.total_damage, a.asset_name, a.asset_kind, a.image_url FROM {db}.match_damage_totals AS d INNER JOIN {db}.match_players AS dealer ON dealer.match_id = d.match_id AND dealer.player_slot = d.dealer_slot INNER JOIN {db}.match_players AS target ON target.match_id = d.match_id AND target.player_slot = d.target_slot LEFT JOIN (SELECT class_name, name AS asset_name, asset_type AS asset_kind, image_url FROM {db}.items UNION ALL SELECT class_name, name AS asset_name, asset_type AS asset_kind, image_url FROM {db}.abilities) AS a ON a.class_name = d.source_name WHERE d.match_id = {match_id} AND d.stat_type = 0 AND ifNull(dealer.team, -1) != ifNull(target.team, -2) AND d.source_name NOT IN ('Bullet', 'Melee', 'Ability', 'Misc')",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    let mut grouped: HashMap<(u32, String), DamageSource> = HashMap::new();
    for row in rows {
        let Some(dealer) = super::number_u32(&row, "dealer_slot") else {
            continue;
        };
        let source_name = super::string_value(&row, "source_name").unwrap_or_default();
        let (source, name, kind) = normalize_source(
            &source_name,
            non_empty_string(&row, "asset_name"),
            non_empty_string(&row, "asset_kind"),
        );
        let key = (dealer, source.clone());
        let damage = number_f64(&row, "total_damage");
        let entry = grouped.entry(key).or_insert_with(|| DamageSource {
            source,
            name,
            kind,
            icon: non_empty_string(&row, "image_url"),
            total: 0.0,
            by_target: BTreeMap::new(),
        });
        entry.total += damage;
        *entry
            .by_target
            .entry(
                super::number_u32(&row, "target_slot")
                    .unwrap_or_default()
                    .to_string(),
            )
            .or_default() += damage;
    }
    let mut result: HashMap<u32, Vec<DamageSource>> = HashMap::new();
    for ((slot, _), source) in grouped {
        result.entry(slot).or_default().push(source);
    }
    for sources in result.values_mut() {
        sources.sort_by(|left, right| left.source.cmp(&right.source));
    }
    Ok(result)
}

fn normalize_source(
    source: &str,
    asset_name: Option<String>,
    asset_kind: Option<String>,
) -> (String, String, String) {
    if source.starts_with("citadel_weapon_") || source == "weapon" {
        return (
            "weapon".to_string(),
            "Gun".to_string(),
            "weapon".to_string(),
        );
    }
    if source.starts_with("ability_melee_") || source == "melee" {
        return (
            "melee".to_string(),
            "Melee".to_string(),
            "melee".to_string(),
        );
    }
    let kind = if source.starts_with("ability_") {
        "ability".to_string()
    } else if source.starts_with("upgrade_") {
        "upgrade".to_string()
    } else {
        asset_kind.unwrap_or_else(|| "other".to_string())
    };
    let name = asset_name.unwrap_or_else(|| source.to_string());
    (source.to_string(), name, kind)
}

async fn load_builds(
    state: &ReadApiState,
    match_id: u64,
) -> Result<HashMap<u32, Vec<BuildEntry>>, (StatusCode, Json<ErrorBody>)> {
    let sql = format!(
        "SELECT player_slot, game_time_s, item_id, upgrade_id, sold_time_s, upgrade_tier, asset_name, asset_kind, asset_image_url FROM {db}.match_player_items_with_assets WHERE match_id = {match_id} AND (item_id != 0 OR upgrade_id != 0) ORDER BY player_slot, game_time_s, item_id, upgrade_id",
        db = state.database,
    );
    let rows = query_rows(state, &sql).await?;
    let mut builds: HashMap<u32, Vec<BuildEntry>> = HashMap::new();
    for row in rows {
        let Some(slot) = super::number_u32(&row, "player_slot") else {
            continue;
        };
        let item_id = super::number_u32(&row, "item_id").unwrap_or_default();
        let upgrade_id = super::number_u32(&row, "upgrade_id").unwrap_or_default();
        let id = if item_id != 0 { item_id } else { upgrade_id };
        let kind = if non_empty_string(&row, "asset_kind").as_deref() == Some("ability") {
            "ability"
        } else {
            "item"
        };
        builds.entry(slot).or_default().push(BuildEntry {
            t: super::number_u32(&row, "game_time_s").unwrap_or_default(),
            id,
            name: non_empty_string(&row, "asset_name").unwrap_or_else(|| id.to_string()),
            kind: kind.to_string(),
            tier: super::number_u32(&row, "upgrade_tier").filter(|tier| *tier != 0),
            icon: non_empty_string(&row, "asset_image_url"),
            sold_t: super::number_u32(&row, "sold_time_s").filter(|time| *time != 0),
        });
    }
    Ok(builds)
}

fn build_player(
    player: &CorePlayer,
    members: &HashMap<u32, String>,
    final_stats: &HashMap<u32, FinalStats>,
    timelines: &HashMap<u32, Timeline>,
    damage: &HashMap<u32, Vec<DamageSource>>,
    builds: &HashMap<u32, Vec<BuildEntry>>,
) -> Player {
    let stats = final_stats.get(&player.slot).cloned().unwrap_or_default();
    Player {
        slot: player.slot,
        team: player.team,
        member: member_for(members, player.account_id),
        hero: player.hero.clone(),
        kills: player.kills,
        deaths: player.deaths,
        assists: player.assists,
        souls: stats.souls,
        last_hits: stats.last_hits,
        denies: stats.denies,
        level: stats.level,
        hero_damage: stats.hero_damage,
        healing: stats.healing,
        damage_taken: stats.damage_taken,
        damage_mitigated: stats.damage_mitigated,
        impact: ImpactScore::default(),
        aim: Aim {
            shots_hit: stats.shots_hit,
            shots_missed: stats.shots_missed,
            hero_hits: stats.hero_hits,
            hero_crits: stats.hero_crits,
        },
        kill_types: KillTypes {
            bullet: stats.bullet_kills,
            ability: stats.ability_kills,
            melee: stats.melee_kills,
            headshot: stats.headshot_kills,
        },
        timeline: timelines.get(&player.slot).cloned().unwrap_or_default(),
        damage_by_source: damage.get(&player.slot).cloned().unwrap_or_default(),
        build: builds.get(&player.slot).cloned().unwrap_or_default(),
    }
}

fn impact_metrics_from_player(player: &Player) -> ImpactMetrics {
    ImpactMetrics {
        slot: player.slot,
        hero_damage: f64::from(player.hero_damage),
        kills: f64::from(player.kills),
        deaths: f64::from(player.deaths),
        assists: f64::from(player.assists),
        souls: f64::from(player.souls),
        last_hits: f64::from(player.last_hits),
        denies: f64::from(player.denies),
        healing: f64::from(player.healing),
        damage_mitigated: f64::from(player.damage_mitigated),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folds_weapon_and_melee_damage_sources() {
        assert_eq!(
            normalize_source("citadel_weapon_punkgoat_set", None, None),
            (
                "weapon".to_string(),
                "Gun".to_string(),
                "weapon".to_string()
            )
        );
        assert_eq!(
            normalize_source("ability_melee_punkgoat", None, None),
            (
                "melee".to_string(),
                "Melee".to_string(),
                "melee".to_string()
            )
        );
    }
}
