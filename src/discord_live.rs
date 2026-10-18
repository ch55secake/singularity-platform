//! Live match scouting for the Discord bot.
//!
//! When a linked player is in a live match, the bot posts both teams with each
//! player's rank, recent form and experience on their hero. Live matches come
//! from two places:
//!
//! - collectors report the match ID when a match starts, and the bot reads the
//!   players from the match's live broadcast through the community Deadlock
//!   API (`/v1/matches/demo/live/query`, 6 lookups per hour per IP); and
//! - the community API's active-match feed, which mirrors the in-game watch tab
//!   and only lists about the top 200 matches being played.

use std::{
    collections::{BTreeMap, HashMap},
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::Client as HttpClient;
use serde::{Deserialize, de::DeserializeOwned};
use serenity::all::{CreateEmbed, CreateEmbedFooter};

use crate::discord_format::{TEAM_COLOURS, safe_embed_text};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinSet,
};

const DEFAULT_DEADLOCK_API_URL: &str = "https://api.deadlock-api.com";
const DEFAULT_LIVE_POLL_SECONDS: u64 = 120;
const RECENT_MATCHES: usize = 20;
const LOOKUP_CONCURRENCY: usize = 6;
const NAMES_TTL: Duration = Duration::from_secs(24 * 60 * 60);
pub(crate) const TEAM_NAMES: [&str; 2] = ["Hidden King", "Archmother"];
/// Recent games per player searched for shared matches.
const STACK_WINDOW: usize = 30;
/// Same-team games two players must share to count as a stack.
const STACK_MIN_SHARED: usize = 2;
const STEAM64_BASE: u64 = 76561197960265728;
/// The broadcast can take a few minutes to start after the lobby forms.
const BROADCAST_TIMEOUT: Duration = Duration::from_secs(15 * 60);
/// Ticks to read after the first one, so every player controller is seen.
const SNAPSHOT_TICKS: u64 = 64;
const PLAYERS_QUERY: &str = "SELECT tick, \"m_steamID\" AS steam_id, \"m_iTeamNum\" AS team, \"m_PlayerDataGlobal__m_nHeroID\" AS hero_id FROM CCitadelPlayerController WHERE \"m_PlayerDataGlobal__m_nHeroID\" > 0";

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ActiveMatch {
    pub(crate) match_id: u64,
    #[serde(default)]
    start_time: Option<u64>,
    #[serde(default)]
    match_mode_parsed: Option<String>,
    #[serde(default)]
    region_mode_parsed: Option<String>,
    #[serde(default)]
    pub(crate) players: Vec<ActivePlayer>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ActivePlayer {
    #[serde(default)]
    pub(crate) account_id: Option<u32>,
    #[serde(default)]
    team: Option<i32>,
    #[serde(default)]
    hero_id: Option<u32>,
}

/// One player controller row from the live broadcast query.
#[derive(Debug, Deserialize)]
struct BroadcastRow {
    tick: u64,
    steam_id: u64,
    team: i32,
    hero_id: u32,
}

#[derive(Debug, Deserialize)]
struct BroadcastError {
    #[serde(default)]
    error: Option<String>,
}

/// Collects the players seen in the first ticks of a broadcast.
#[derive(Debug, Default)]
pub(crate) struct Snapshot {
    first_tick: Option<u64>,
    players: BTreeMap<u32, ActivePlayer>,
}

impl Snapshot {
    /// Adds one row; returns true once the snapshot is complete.
    fn add(&mut self, row: BroadcastRow) -> bool {
        let first_tick = *self.first_tick.get_or_insert(row.tick);
        if row.tick > first_tick + SNAPSHOT_TICKS {
            return true;
        }
        let Some(account_id) = row
            .steam_id
            .checked_sub(STEAM64_BASE)
            .and_then(|id| u32::try_from(id).ok())
            .filter(|id| *id > 0)
        else {
            return false;
        };
        // The broadcast numbers teams 2 (Hidden King) and 3 (Archmother).
        let team = match row.team {
            2 => 0,
            3 => 1,
            _ => return false,
        };
        self.players.insert(
            account_id,
            ActivePlayer {
                account_id: Some(account_id),
                team: Some(team),
                hero_id: Some(row.hero_id),
            },
        );
        false
    }

    pub(crate) fn into_match(self, match_id: u64, start_time: Option<u64>) -> ActiveMatch {
        let mut players = self.players.into_values().collect::<Vec<_>>();
        players.sort_by_key(|player| (player.team, player.hero_id));
        ActiveMatch {
            match_id,
            start_time,
            match_mode_parsed: None,
            region_mode_parsed: None,
            players,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct HistoryEntry {
    #[serde(default)]
    match_id: Option<u64>,
    #[serde(default)]
    hero_id: Option<u32>,
    #[serde(default)]
    player_kills: Option<u32>,
    #[serde(default)]
    player_deaths: Option<u32>,
    #[serde(default)]
    player_assists: Option<u32>,
    #[serde(default)]
    match_result: Option<i32>,
    #[serde(default)]
    player_team: Option<i32>,
}

/// A player's games and wins on one hero, over their whole history.
#[derive(Debug, Deserialize)]
struct HeroStat {
    account_id: u32,
    hero_id: u32,
    #[serde(default)]
    matches_played: u32,
    #[serde(default)]
    wins: u32,
}

/// The live post's embeds, plus (hero, name) for each friend in the match so
/// the after-match summary can name them.
pub(crate) struct LivePost {
    pub(crate) embeds: Vec<CreateEmbed>,
    pub(crate) friends: Vec<(String, String)>,
}

/// Everything shown on one player's row.
#[derive(Debug, Clone, Default)]
pub(crate) struct PlayerCard {
    pub(crate) name: String,
    pub(crate) hero: String,
    pub(crate) friend: bool,
    pub(crate) rank: String,
    /// (games, wins) on the hero being played.
    pub(crate) hero_games: Option<(u32, u32)>,
    pub(crate) summary: Option<Summary>,
    pub(crate) stack: Option<char>,
}

#[derive(Debug, Deserialize)]
struct Rank {
    #[serde(default)]
    badge: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct SteamProfile {
    account_id: u32,
    #[serde(default)]
    personaname: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NamedAsset {
    #[serde(default, alias = "tier")]
    id: Option<u32>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Summary {
    recent: usize,
    wins: usize,
    kda: f64,
    hero_matches: usize,
    hero_win_rate: Option<f64>,
}

type Names = (HashMap<u32, String>, HashMap<u32, String>);

/// Client for the community API calls behind a live match post.
pub(crate) struct LiveScout {
    http: HttpClient,
    base_url: Arc<str>,
    pub(crate) poll_interval: Duration,
    names: Mutex<Option<(Instant, Arc<Names>)>>,
}

impl LiveScout {
    /// Reads SINGULARITY_DISCORD_LIVE_MATCHES (default on),
    /// SINGULARITY_DISCORD_DEADLOCK_API_URL and
    /// SINGULARITY_DISCORD_LIVE_POLL_SECONDS. Returns None when turned off.
    pub(crate) fn from_env(http: HttpClient) -> Result<Option<Self>, std::io::Error> {
        let enabled = env::var("SINGULARITY_DISCORD_LIVE_MATCHES")
            .map(|value| !matches!(value.trim(), "0" | "false" | "no" | "off"))
            .unwrap_or(true);
        if !enabled {
            return Ok(None);
        }
        let poll_seconds = env::var("SINGULARITY_DISCORD_LIVE_POLL_SECONDS")
            .unwrap_or_else(|_| DEFAULT_LIVE_POLL_SECONDS.to_string())
            .parse::<u64>()
            .ok()
            .filter(|seconds| *seconds > 0)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "invalid live poll interval",
                )
            })?;
        let base_url = env::var("SINGULARITY_DISCORD_DEADLOCK_API_URL")
            .unwrap_or_else(|_| DEFAULT_DEADLOCK_API_URL.to_string());
        Ok(Some(Self {
            http,
            base_url: base_url.trim_end_matches('/').into(),
            poll_interval: Duration::from_secs(poll_seconds),
            names: Mutex::new(None),
        }))
    }

    pub(crate) async fn active_matches(&self, account_ids: &[u32]) -> LiveResult<Vec<ActiveMatch>> {
        get_json(
            &self.http,
            &self.base_url,
            &format!("v1/matches/active?account_ids={}", join_ids(account_ids)),
        )
        .await
    }

    /// Reads the players of a match from its live broadcast. Waits while the
    /// community API reports that the broadcast is not ready yet.
    pub(crate) async fn broadcast_players(
        &self,
        match_id: u64,
        start_time: Option<u64>,
    ) -> LiveResult<ActiveMatch> {
        let mut response = self
            .http
            .get(format!("{}/v1/matches/demo/live/query", self.base_url))
            .query(&[
                ("match_id", match_id.to_string()),
                ("query", PLAYERS_QUERY.to_string()),
            ])
            .timeout(BROADCAST_TIMEOUT)
            .send()
            .await?
            .error_for_status()?;
        let mut parser = SseParser::default();
        let mut snapshot = Snapshot::default();
        while let Some(chunk) = response.chunk().await? {
            for (event, data) in parser.push(&chunk) {
                match event.as_deref() {
                    Some("error") => {
                        let message = serde_json::from_str::<BroadcastError>(&data)
                            .ok()
                            .and_then(|error| error.error)
                            .unwrap_or(data);
                        return Err(message.into());
                    }
                    Some("end") => return finish(snapshot, match_id, start_time),
                    Some(_) => {}
                    None => {
                        if let Ok(row) = serde_json::from_str::<BroadcastRow>(&data)
                            && snapshot.add(row)
                        {
                            return finish(snapshot, match_id, start_time);
                        }
                    }
                }
            }
        }
        finish(snapshot, match_id, start_time)
    }

    async fn names(&self) -> Arc<Names> {
        let mut cached = self.names.lock().await;
        if let Some((loaded_at, names)) = cached.as_ref()
            && loaded_at.elapsed() < NAMES_TTL
        {
            return Arc::clone(names);
        }
        let heroes =
            get_json::<Vec<NamedAsset>>(&self.http, &self.base_url, "v1/assets/heroes").await;
        let ranks =
            get_json::<Vec<NamedAsset>>(&self.http, &self.base_url, "v1/assets/ranks").await;
        let (Ok(heroes), Ok(ranks)) = (heroes, ranks) else {
            return cached
                .as_ref()
                .map(|(_, names)| Arc::clone(names))
                .unwrap_or_default();
        };
        let names = Arc::new((index_names(heroes), index_names(ranks)));
        *cached = Some((Instant::now(), Arc::clone(&names)));
        names
    }

    /// Build the post for one live match. `linked` maps account IDs to Discord
    /// user IDs; `friends` are more of our accounts, such as the players whose
    /// collectors reported the match. Friends get a short line; everyone else
    /// gets their rank, games and win rate on their hero, recent form, and a
    /// stack tag when they keep queuing with someone on their team.
    pub(crate) async fn scout(
        &self,
        live: &ActiveMatch,
        linked: &HashMap<u32, u64>,
        friend_names: &HashMap<u32, String>,
        friends: &[u32],
    ) -> LivePost {
        let names = self.names().await;
        let is_friend =
            |account_id: u32| linked.contains_key(&account_id) || friends.contains(&account_id);
        let account_ids = live
            .players
            .iter()
            .filter_map(|player| player.account_id)
            .collect::<Vec<_>>();
        let strangers = account_ids
            .iter()
            .copied()
            .filter(|account_id| !is_friend(*account_id))
            .collect::<Vec<_>>();
        let profiles = get_json::<Vec<SteamProfile>>(
            &self.http,
            &self.base_url,
            &format!("v1/players/steam?account_ids={}", join_ids(&account_ids)),
        )
        .await
        .map(|profiles| {
            profiles
                .into_iter()
                .filter_map(|profile| Some((profile.account_id, profile.personaname?)))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
        let hero_stats = if strangers.is_empty() {
            HashMap::new()
        } else {
            get_json::<Vec<HeroStat>>(
                &self.http,
                &self.base_url,
                &format!("v1/players/hero-stats?account_ids={}", join_ids(&strangers)),
            )
            .await
            .map(index_hero_stats)
            .unwrap_or_default()
        };

        let semaphore = Arc::new(Semaphore::new(LOOKUP_CONCURRENCY));
        let mut lookups = JoinSet::new();
        for account_id in strangers {
            let (http, base_url, semaphore) = (
                self.http.clone(),
                Arc::clone(&self.base_url),
                Arc::clone(&semaphore),
            );
            lookups.spawn(async move {
                let _permit = semaphore.acquire_owned().await;
                let rank =
                    get_json::<Rank>(&http, &base_url, &format!("v1/players/{account_id}/rank"))
                        .await;
                let history = get_json::<Vec<HistoryEntry>>(
                    &http,
                    &base_url,
                    &format!("v1/players/{account_id}/match-history"),
                )
                .await;
                (
                    account_id,
                    rank.ok().and_then(|rank| rank.badge),
                    history.ok(),
                )
            });
        }
        let mut badges = HashMap::new();
        let mut histories = HashMap::new();
        while let Some(Ok((account_id, badge, history))) = lookups.join_next().await {
            badges.insert(account_id, badge);
            if let Some(history) = history {
                histories.insert(account_id, history);
            }
        }
        let teams = live
            .players
            .iter()
            .filter_map(|player| Some((player.account_id?, player.team?)))
            .filter(|(account_id, _)| histories.contains_key(account_id))
            .collect::<Vec<_>>();
        let stacks = find_stacks(&teams, &histories);

        let mut cards: [Vec<PlayerCard>; 2] = [Vec::new(), Vec::new()];
        let mut friend_teams = [false, false];
        for player in &live.players {
            let Some(team) = player
                .team
                .and_then(|team| usize::try_from(team).ok())
                .filter(|team| *team < 2)
            else {
                continue;
            };
            let account_id = player.account_id.unwrap_or_default();
            let friend = account_id != 0 && is_friend(account_id);
            friend_teams[team] |= friend;
            let hero_id = player.hero_id.unwrap_or_default();
            let card = PlayerCard {
                name: friend_names
                    .get(&account_id)
                    .or_else(|| profiles.get(&account_id))
                    .cloned()
                    .unwrap_or_else(|| "Unknown player".into()),
                hero: names
                    .0
                    .get(&hero_id)
                    .cloned()
                    .unwrap_or_else(|| format!("Hero {hero_id}")),
                friend,
                rank: badges
                    .get(&account_id)
                    .copied()
                    .flatten()
                    .map(|badge| rank_label(badge, &names.1))
                    .unwrap_or_else(|| "Unknown rank".into()),
                hero_games: hero_stats.get(&(account_id, hero_id)).copied(),
                summary: histories
                    .get(&account_id)
                    .map(|history| summarize_history(history, hero_id)),
                stack: stacks.get(&account_id).copied(),
            };
            cards[team].push(card);
        }
        let friends = cards
            .iter()
            .flatten()
            .filter(|card| card.friend)
            .map(|card| (card.hero.clone(), card.name.clone()))
            .collect();
        LivePost {
            embeds: live_embeds(&cards, friend_teams),
            friends,
        }
    }
}

type LiveResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn finish(snapshot: Snapshot, match_id: u64, start_time: Option<u64>) -> LiveResult<ActiveMatch> {
    if snapshot.players.is_empty() {
        return Err("the live broadcast ended before any players were seen".into());
    }
    Ok(snapshot.into_match(match_id, start_time))
}

/// Splits a Server-Sent Events stream into (event name, data) pairs.
#[derive(Debug, Default)]
struct SseParser {
    buffer: String,
    event: Option<String>,
}

impl SseParser {
    fn push(&mut self, chunk: &[u8]) -> Vec<(Option<String>, String)> {
        self.buffer.push_str(&String::from_utf8_lossy(chunk));
        let mut messages = Vec::new();
        while let Some(end) = self.buffer.find('\n') {
            let line = self.buffer[..end].trim_end_matches('\r').to_string();
            self.buffer.drain(..=end);
            if line.is_empty() {
                self.event = None;
            } else if let Some(event) = line.strip_prefix("event:") {
                self.event = Some(event.trim().to_string());
            } else if let Some(data) = line.strip_prefix("data:") {
                messages.push((self.event.clone(), data.trim_start().to_string()));
            }
        }
        messages
    }
}

async fn get_json<T: DeserializeOwned>(
    http: &HttpClient,
    base_url: &str,
    path: &str,
) -> LiveResult<T> {
    let response = http
        .get(format!("{base_url}/{path}"))
        .send()
        .await?
        .error_for_status()?;
    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

fn join_ids(ids: &[u32]) -> String {
    ids.iter().map(u32::to_string).collect::<Vec<_>>().join(",")
}

fn index_names(assets: Vec<NamedAsset>) -> HashMap<u32, String> {
    assets
        .into_iter()
        .filter_map(|asset| Some((asset.id?, asset.name?)))
        .collect()
}

/// A badge such as 81 is tier 8 (Oracle), subrank 1.
pub(crate) fn rank_label(badge: u32, rank_names: &HashMap<u32, String>) -> String {
    let (tier, subrank) = (badge / 10, badge % 10);
    if tier == 0 {
        return "Unranked".into();
    }
    let name = rank_names
        .get(&tier)
        .cloned()
        .unwrap_or_else(|| format!("Rank {tier}"));
    if subrank == 0 {
        name
    } else {
        format!("{name} {subrank}")
    }
}

fn won(entry: &HistoryEntry) -> bool {
    // player_match_outcome is only set on recent matches; the winning team is on all of them.
    entry.match_result.is_some() && entry.match_result == entry.player_team
}

/// Recent form plus experience on the hero being played. History is newest first.
pub(crate) fn summarize_history(history: &[HistoryEntry], hero_id: u32) -> Summary {
    let recent = &history[..history.len().min(RECENT_MATCHES)];
    let total = |pick: fn(&HistoryEntry) -> Option<u32>| {
        recent.iter().filter_map(pick).map(f64::from).sum::<f64>()
    };
    let kills = total(|entry| entry.player_kills);
    let deaths = total(|entry| entry.player_deaths);
    let assists = total(|entry| entry.player_assists);
    let on_hero = history
        .iter()
        .filter(|entry| entry.hero_id == Some(hero_id))
        .collect::<Vec<_>>();
    let hero_wins = on_hero.iter().filter(|entry| won(entry)).count();
    Summary {
        recent: recent.len(),
        wins: recent.iter().filter(|entry| won(entry)).count(),
        kda: (kills + assists) / deaths.max(1.0),
        hero_matches: on_hero.len(),
        hero_win_rate: (!on_hero.is_empty()).then(|| hero_wins as f64 / on_hero.len() as f64),
    }
}

/// Player detail shown in each mobile-friendly scouting card.
fn live_card_value(card: &PlayerCard) -> String {
    let hero = safe_embed_text(&card.hero, 80);
    if card.friend {
        return format!("**{hero}** · Friend");
    }

    let rank = if card.rank == "Unranked" || card.rank == "Unknown rank" {
        String::new()
    } else {
        safe_embed_text(&card.rank, 64)
    };
    let heading = if rank.is_empty() {
        format!("**{hero}**")
    } else {
        format!("**{hero}** · {rank}")
    };
    // Whole-history hero stats when the API has them, else the match history.
    let hero_games = card.hero_games.or_else(|| {
        let summary = card.summary.as_ref()?;
        let rate = summary.hero_win_rate?;
        let games = u32::try_from(summary.hero_matches).ok()?;
        Some((games, (rate * f64::from(games)).round() as u32))
    });
    let hero_line = match hero_games {
        Some((games, wins)) if games > 0 => {
            let rate = f64::from(wins) / f64::from(games);
            format!("{games} games · {:.0}% WR", rate * 100.0)
        }
        _ => "New pick · no hero history".to_string(),
    };
    let recent = match card.summary.as_ref().filter(|summary| summary.recent > 0) {
        Some(summary) => format!(
            "Recent {}–{} (last {})",
            summary.wins,
            summary.recent.saturating_sub(summary.wins),
            summary.recent
        ),
        None => "Recent form private".to_string(),
    };
    let stack = card
        .stack
        .map(|letter| format!(" · Stack {letter}"))
        .unwrap_or_default();
    format!("{heading}\n{hero_line}\n{recent}{stack}")
}

fn index_hero_stats(stats: Vec<HeroStat>) -> HashMap<(u32, u32), (u32, u32)> {
    stats
        .into_iter()
        .map(|stat| {
            (
                (stat.account_id, stat.hero_id),
                (stat.matches_played, stat.wins),
            )
        })
        .collect()
}

/// Groups teammates who were on the same team in at least
/// `STACK_MIN_SHARED` of their last `STACK_WINDOW` games, which in practice
/// means they queue together. `players` pairs account IDs with their team in
/// this match. Returns a letter per stacked player (A, B, ...).
pub(crate) fn find_stacks(
    players: &[(u32, i32)],
    histories: &HashMap<u32, Vec<HistoryEntry>>,
) -> HashMap<u32, char> {
    let recent = |account_id: u32| -> HashMap<u64, Option<i32>> {
        histories
            .get(&account_id)
            .map(|history| {
                history
                    .iter()
                    .take(STACK_WINDOW)
                    .filter_map(|entry| Some((entry.match_id?, entry.player_team)))
                    .collect()
            })
            .unwrap_or_default()
    };
    let games = players
        .iter()
        .map(|(account_id, _)| recent(*account_id))
        .collect::<Vec<_>>();
    // Union-find over the players' indexes.
    let mut parent = (0..players.len()).collect::<Vec<_>>();
    fn root(parent: &mut [usize], mut index: usize) -> usize {
        while parent[index] != index {
            parent[index] = parent[parent[index]];
            index = parent[index];
        }
        index
    }
    for left in 0..players.len() {
        for right in left + 1..players.len() {
            if players[left].1 != players[right].1 {
                continue;
            }
            let shared = games[left]
                .iter()
                .filter(|(match_id, team)| {
                    team.is_some() && games[right].get(match_id) == Some(team)
                })
                .count();
            if shared >= STACK_MIN_SHARED {
                let (a, b) = (root(&mut parent, left), root(&mut parent, right));
                parent[b] = a;
            }
        }
    }
    let mut sizes = HashMap::new();
    for index in 0..players.len() {
        *sizes.entry(root(&mut parent, index)).or_insert(0) += 1;
    }
    let mut letters = HashMap::new();
    let mut stacks = HashMap::new();
    for (index, (account_id, _)) in players.iter().enumerate() {
        let group = root(&mut parent, index);
        if sizes[&group] < 2 {
            continue;
        }
        let next = char::from(b'A' + u8::try_from(letters.len()).unwrap_or(25).min(25));
        let letter = *letters.entry(group).or_insert(next);
        stacks.insert(*account_id, letter);
    }
    stacks
}

/// Teams in display order with their headings, the friends' team first.
pub(crate) fn team_headings(friend_teams: [bool; 2]) -> Vec<(usize, String)> {
    let one_side = friend_teams[0] != friend_teams[1];
    let order = if one_side && friend_teams[1] {
        [1, 0]
    } else {
        [0, 1]
    };
    order
        .into_iter()
        .map(|team| {
            let name = if one_side {
                let side = if friend_teams[team] {
                    "Your team"
                } else {
                    "Enemy team"
                };
                format!("{side} · {}", TEAM_NAMES[team])
            } else {
                TEAM_NAMES[team].to_string()
            };
            (team, name)
        })
        .collect()
}

/// "Ranked · Europe", or "Match" when the mode is unknown.
pub(crate) fn live_title(live: &ActiveMatch) -> String {
    let mode = live.match_mode_parsed.as_deref().unwrap_or("Match");
    match live
        .region_mode_parsed
        .as_deref()
        .filter(|region| !region.is_empty())
    {
        Some(region) => {
            let region = region
                .split('_')
                .map(|word| {
                    let mut chars = word.chars();
                    chars
                        .next()
                        .map(|first| first.to_uppercase().chain(chars).collect::<String>())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(" ");
            format!("{mode} · {region}")
        }
        None => mode.to_string(),
    }
}

/// The message heading above the scouting cards.
pub(crate) fn live_heading(live: &ActiveMatch, who: &str) -> String {
    let mut details = vec![format!("Match `{}`", live.match_id)];
    if let Some(started) = live.start_time.map(|time| format!("Started <t:{time}:R>")) {
        details.push(started);
    }
    if !who.is_empty() {
        details.push(format!("With {who}"));
    }
    format!(
        "🔴 **LIVE MATCH** · **{}**\n{}",
        live_title(live),
        details.join(" · ")
    )
}

/// One embed per team with a compact card for each player.
fn live_embeds(cards: &[Vec<PlayerCard>; 2], friend_teams: [bool; 2]) -> Vec<CreateEmbed> {
    let has_stacks = cards.iter().flatten().any(|card| card.stack.is_some());
    let headings = team_headings(friend_teams)
        .into_iter()
        .filter(|(team, _)| !cards[*team].is_empty())
        .collect::<Vec<_>>();
    let last = headings.len().saturating_sub(1);
    headings
        .into_iter()
        .enumerate()
        .map(|(index, (team, heading))| {
            let badge = if team == 0 { "🟠" } else { "🔵" };
            let mut embed = CreateEmbed::new()
                .title(format!("{badge} {heading}"))
                .colour(TEAM_COLOURS[team])
                .description(format!("{} scouted", cards[team].len()));
            for card in &cards[team] {
                let name = safe_embed_text(&card.name, 96);
                let name = if name.is_empty() {
                    "Unknown player".to_string()
                } else {
                    name
                };
                embed = embed.field(name, live_card_value(card), true);
            }
            if index != last {
                return embed;
            }
            let stacks = if has_stacks {
                " · Stack letters mark frequent teammates"
            } else {
                ""
            };
            embed.footer(CreateEmbedFooter::new(format!(
                "Games/WR: on this hero · Recent: up to 20 matches{stacks} · Deadlock API"
            )))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(won_match: bool, hero_id: u32, kills: u32, deaths: u32, assists: u32) -> HistoryEntry {
        HistoryEntry {
            match_id: None,
            hero_id: Some(hero_id),
            player_kills: Some(kills),
            player_deaths: Some(deaths),
            player_assists: Some(assists),
            match_result: Some(if won_match { 0 } else { 1 }),
            player_team: Some(0),
        }
    }

    #[test]
    fn sse_parser_splits_events_across_chunks() {
        let mut parser = SseParser::default();
        assert!(
            parser
                .push(b"event: status\ndata: {\"message\":\"not ready\"}")
                .is_empty()
        );
        let messages = parser.push(b"\n\ndata: {\"tick\":1}\r\n\nevent: end\ndata: {}\n\n");
        assert_eq!(
            messages,
            vec![
                (Some("status".into()), "{\"message\":\"not ready\"}".into()),
                (None, "{\"tick\":1}".into()),
                (Some("end".into()), "{}".into()),
            ]
        );
    }

    #[test]
    fn snapshot_reads_players_from_the_first_ticks() {
        let row = |tick, steam_id, team, hero_id| BroadcastRow {
            tick,
            steam_id,
            team,
            hero_id,
        };
        let mut snapshot = Snapshot::default();
        assert!(!snapshot.add(row(14731, 76561198063672350, 3, 6)));
        assert!(!snapshot.add(row(14731, 76561198208794271, 2, 67)));
        assert!(!snapshot.add(row(14732, 76561198208794271, 2, 67)));
        assert!(!snapshot.add(row(14732, 0, 2, 1)));
        assert!(snapshot.add(row(14731 + SNAPSHOT_TICKS + 1, 76561198000000000, 2, 1)));

        let live = snapshot.into_match(117420947, Some(10));
        let players = live
            .players
            .iter()
            .map(|player| (player.account_id, player.team, player.hero_id))
            .collect::<Vec<_>>();
        assert_eq!(
            players,
            vec![
                (Some(248528543), Some(0), Some(67)),
                (Some(103406622), Some(1), Some(6)),
            ]
        );
        assert_eq!(live.start_time, Some(10));
    }

    #[test]
    fn rank_label_splits_badge_into_tier_and_subrank() {
        let names = HashMap::from([(8, "Oracle".to_string()), (11, "Eternus".to_string())]);

        assert_eq!(rank_label(81, &names), "Oracle 1");
        assert_eq!(rank_label(110, &names), "Eternus");
        assert_eq!(rank_label(0, &names), "Unranked");
        assert_eq!(rank_label(43, &names), "Rank 4 3");
    }

    #[test]
    fn summary_uses_recent_form_and_whole_hero_history() {
        let mut history = vec![entry(true, 10, 10, 2, 6); 15];
        history.extend(vec![entry(false, 20, 5, 8, 5); 10]);
        history.extend(vec![entry(true, 10, 5, 5, 5); 5]);

        let summary = summarize_history(&history, 10);

        assert_eq!(summary.recent, 20);
        assert_eq!(summary.wins, 15);
        assert!((summary.kda - (15.0 * 16.0 + 5.0 * 10.0) / (15.0 * 2.0 + 5.0 * 8.0)).abs() < 1e-9);
        assert_eq!(summary.hero_matches, 20);
        assert_eq!(summary.hero_win_rate, Some(1.0));
    }

    #[test]
    fn old_matches_without_an_outcome_still_count_wins() {
        let old_win = HistoryEntry {
            match_result: Some(1),
            player_team: Some(1),
            ..Default::default()
        };
        let no_result = HistoryEntry {
            player_team: Some(0),
            ..Default::default()
        };

        assert!(won(&old_win));
        assert!(!won(&no_result));
    }

    #[test]
    fn scouting_cards_show_hero_games_form_and_stack() {
        let summary = summarize_history(&[entry(true, 20, 5, 5, 5), entry(false, 20, 5, 5, 5)], 10);
        let card = PlayerCard {
            name: "J~".into(),
            hero: "Haze".into(),
            rank: "Oracle 1".into(),
            hero_games: Some((34, 19)),
            summary: Some(summary.clone()),
            stack: Some('A'),
            ..PlayerCard::default()
        };
        assert_eq!(
            live_card_value(&card),
            "**Haze** · Oracle 1\n34 games · 56% WR\nRecent 1–1 (last 2) · Stack A"
        );

        let new_hero = live_card_value(&PlayerCard {
            hero_games: None,
            stack: None,
            rank: "Unranked".into(),
            ..card.clone()
        });
        assert!(new_hero.contains("New pick · no hero history"));
        assert!(!new_hero.contains("Oracle 1"));

        let private = live_card_value(&PlayerCard {
            name: "a".into(),
            hero: "Haze".into(),
            rank: "Unranked".into(),
            ..PlayerCard::default()
        });
        assert!(private.contains("Recent form private"));
    }

    #[test]
    fn friends_get_a_short_scouting_card() {
        let card = PlayerCard {
            name: "hobnobs".into(),
            hero: "Vyper".into(),
            friend: true,
            rank: "Oracle 4".into(),
            hero_games: Some((50, 30)),
            ..PlayerCard::default()
        };
        assert_eq!(live_card_value(&card), "**Vyper** · Friend");
    }

    #[test]
    fn stacks_are_teammates_who_keep_sharing_games() {
        let game = |match_id: u64, team: i32| HistoryEntry {
            match_id: Some(match_id),
            player_team: Some(team),
            ..HistoryEntry::default()
        };
        let histories = HashMap::from([
            // 1 and 2 shared two games on the same team: a stack.
            (1, vec![game(10, 0), game(11, 1), game(12, 0)]),
            (2, vec![game(10, 0), game(11, 1), game(13, 1)]),
            // 3 met 1 once, and 4 was against 1 in game 12: no stack.
            (3, vec![game(10, 0), game(20, 0)]),
            (4, vec![game(12, 1), game(10, 1)]),
            // 5 and 6 are on the other team and share three games.
            (5, vec![game(30, 0), game(31, 0), game(32, 1)]),
            (6, vec![game(30, 0), game(31, 0), game(32, 1)]),
            // 7 shares games with 1 but is on the other team in this match.
            (7, vec![game(10, 0), game(11, 1)]),
        ]);
        let players = [(1, 0), (2, 0), (3, 0), (4, 0), (5, 1), (6, 1), (7, 1)];

        let stacks = find_stacks(&players, &histories);

        assert_eq!(stacks.get(&1), Some(&'A'));
        assert_eq!(stacks.get(&2), Some(&'A'));
        assert_eq!(stacks.get(&5), Some(&'B'));
        assert_eq!(stacks.get(&6), Some(&'B'));
        assert_eq!(stacks.len(), 4);
    }

    #[test]
    fn friends_team_comes_first() {
        let headings = team_headings([false, true]);

        assert_eq!(headings[0], (1, "Your team · Archmother".to_string()));
        assert_eq!(headings[1], (0, "Enemy team · Hidden King".to_string()));
        assert_eq!(team_headings([true, true])[0].1, "Hidden King");
    }

    #[test]
    fn title_names_mode_and_region() {
        let live = ActiveMatch {
            match_mode_parsed: Some("Ranked".into()),
            region_mode_parsed: Some("south_america".into()),
            ..Default::default()
        };

        assert_eq!(live_title(&live), "Ranked · South America");
    }

    #[test]
    fn live_heading_groups_match_details_and_linked_players() {
        let live = ActiveMatch {
            match_id: 42,
            start_time: Some(1_700_000_000),
            match_mode_parsed: Some("Ranked".into()),
            ..Default::default()
        };

        assert_eq!(
            live_heading(&live, "<@1>"),
            "🔴 **LIVE MATCH** · **Ranked**\nMatch `42` · Started <t:1700000000:R> · With <@1>"
        );
    }

    #[test]
    fn parses_the_active_match_feed() {
        let feed = r#"[{"match_id":117300454,"start_time":1792060426,"match_mode_parsed":"Unranked",
            "region_mode_parsed":"europe","duration_s":null,
            "players":[{"account_id":114558767,"team":0,"team_parsed":"Team0","abandoned":null,"hero_id":10}]}]"#;

        let matches: Vec<ActiveMatch> = serde_json::from_str(feed).expect("feed parses");

        assert_eq!(matches[0].match_id, 117300454);
        assert_eq!(matches[0].players[0].account_id, Some(114558767));
    }

    /// Builds a post from a match being played now. Needs the network:
    /// `cargo test -- --ignored live_scout_against_community_api --nocapture`
    #[tokio::test]
    #[ignore]
    async fn live_scout_against_community_api() {
        let scout = LiveScout::from_env(HttpClient::new()).unwrap().unwrap();
        let all: Vec<ActiveMatch> = get_json(&scout.http, &scout.base_url, "v1/matches/active")
            .await
            .unwrap();
        let friend = all[0].players[3].account_id.unwrap();
        let matches = scout.active_matches(&[friend]).await.unwrap();
        let post = scout
            .scout(
                &matches[0],
                &HashMap::from([(friend, 1)]),
                &HashMap::from([(friend, "friend".to_string())]),
                &[],
            )
            .await;
        let message = serde_json::json!({
            "content": live_heading(&matches[0], "<@1>"),
            "embeds": post.embeds,
        });
        println!("{message}");
        assert!(
            message["embeds"][0]["fields"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field["value"].as_str().unwrap().contains("Friend"))
        );
        assert_eq!(post.friends.len(), 1);
    }
}
