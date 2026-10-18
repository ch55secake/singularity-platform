use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    env,
    fmt::{self, Display},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serenity::{
    all::{
        Command, CommandInteraction, CommandOptionType, CreateAllowedMentions, CreateCommand,
        CreateCommandOption, CreateEmbed, CreateMessage, EditInteractionResponse, EditMessage,
        EventHandler, GatewayIntents, GuildId, Http, Interaction, MessageId, Ready, ResolvedOption,
        ResolvedValue, UserId,
    },
    async_trait,
    client::{Client as DiscordClient, Context},
};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
    time::interval,
};
use tracing::{info, warn};

use crate::discord_format::{TEAM_COLOURS, safe_embed_text};
use crate::discord_live::{LiveScout, TEAM_NAMES, live_heading};
use crate::live::CANCELLED_WITHIN_SECONDS;

const DEFAULT_API_URL: &str = "http://127.0.0.1:8080/api";
const DEFAULT_WEB_URL: &str = "http://127.0.0.1";
const DEFAULT_POLL_SECONDS: u64 = 60;
const DEFAULT_STATE_PATH: &str = "data/discord-state.json";
const DEFAULT_TIMEOUT_SECONDS: u64 = 15;
const MAX_STATS_MATCHES: u32 = 100;
const EVENT_PAGE_LIMIT: usize = 100;
const STEAM64_BASE: u64 = 76561197960265728;
// Live match IDs remembered so each live match is posted once.
const LIVE_POSTED_LIMIT: usize = 200;
// How often the bot checks for matches that collectors reported starting.
const REPORTED_POLL_INTERVAL: Duration = Duration::from_secs(20);
// Waits before retrying a live broadcast lookup that hit a server error.
const BROADCAST_RETRY_WAITS: [Duration; 2] = [Duration::from_secs(60), Duration::from_secs(120)];
// A reported match older than this is not posted as live any more.
const REPORTED_MAX_AGE_SECONDS: u64 = 40 * 60;
// Matches reported this recently get an after-match post.
const TRACKED_WINDOW_SECONDS: u64 = 24 * 60 * 60;

#[derive(Debug)]
struct Config {
    token: String,
    channel_id: serenity::all::ChannelId,
    guild_id: Option<GuildId>,
    api_url: String,
    api_token: Option<String>,
    web_url: String,
    poll_interval: Duration,
    state_path: PathBuf,
}

#[derive(Clone)]
struct ApiClient {
    http: HttpClient,
    base_url: String,
    token: Option<String>,
}

#[derive(Debug)]
struct ApiError(String);

#[derive(Debug, Clone, Deserialize)]
struct ApiMatchEvent {
    uuid: String,
    match_id: u64,
    start_time: u32,
}

/// A match that a collector reported starting or ending.
#[derive(Debug, Clone, Deserialize)]
struct ApiLiveMatch {
    match_id: u64,
    #[serde(default)]
    started_at: Option<u64>,
    #[serde(default)]
    ended_at: Option<u64>,
    #[serde(default)]
    account_ids: Vec<u32>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiMatchDetail {
    uuid: String,
    match_id: u64,
    duration_s: u32,
    mode: String,
    winning_team: i32,
    players: Vec<ApiDetailPlayer>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiDetailPlayer {
    team: i32,
    member: Option<ApiMember>,
    hero: ApiHero,
    kills: u32,
    deaths: u32,
    assists: u32,
    hero_damage: u32,
    aim: ApiAim,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiAim {
    shots_hit: u32,
    shots_missed: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiMember {
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiHero {
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiPlayerAverage {
    name: String,
    kdr: f64,
    kda: f64,
    accuracy: f64,
    crit_rate: f64,
    win_rate: f64,
    damage_per_minute: f64,
    heroes: Vec<ApiHeroAverage>,
    recent: Vec<ApiRecentMatch>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiHeroAverage {
    name: String,
    matches: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiRecentMatch {
    uuid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LinkedAccount {
    account_id: u32,
    discord_name: String,
    linked_at: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateFile {
    version: u32,
    initialized: bool,
    last_start_time: u32,
    last_match_id: u64,
    #[serde(default)]
    links: BTreeMap<String, LinkedAccount>,
    #[serde(default)]
    live_posted: Vec<u64>,
    /// (hero, name) of the friends in each live-posted match, for the summary.
    #[serde(default)]
    live_friends: BTreeMap<u64, Vec<(String, String)>>,
    /// The live post for each reported match, deleted if the match is cancelled.
    #[serde(default)]
    live_messages: BTreeMap<u64, u64>,
    /// Live posts still saying "match found", finished after a restart.
    #[serde(default)]
    live_unfilled: BTreeSet<u64>,
}

struct State {
    path: PathBuf,
    file: StateFile,
}

struct Service {
    config: Arc<Config>,
    api: ApiClient,
    live: Option<LiveScout>,
    state: Arc<Mutex<State>>,
    http: Arc<Mutex<Option<Arc<Http>>>>,
    // Live matches being looked up, so the two live sources don't both post one.
    live_in_flight: Mutex<HashSet<u64>>,
}

struct Handler {
    service: Arc<Service>,
}

enum Reply {
    Text(String),
    Embed(Box<CreateEmbed>),
    Post(Post),
}

/// Message text plus embeds.
#[derive(Debug, Clone)]
struct Post {
    content: String,
    embeds: Vec<CreateEmbed>,
}

#[derive(Debug)]
struct LeaderboardEntry {
    name: String,
    value: f64,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(Config::from_env()?);
    let state = Arc::new(Mutex::new(State::load(&config.state_path).await?));
    let api = ApiClient::from_config(&config)?;
    let live = LiveScout::from_env(api.http.clone())?;
    let service = Arc::new(Service {
        config: Arc::clone(&config),
        api,
        live,
        state,
        http: Arc::new(Mutex::new(None)),
        live_in_flight: Mutex::new(HashSet::new()),
    });

    let mut client = DiscordClient::builder(&config.token, GatewayIntents::empty())
        .event_handler(Handler {
            service: Arc::clone(&service),
        })
        .await?;
    let poll_service = Arc::clone(&service);
    let poll_task = tokio::spawn(async move { poll_notifications(poll_service).await });
    let live_service = Arc::clone(&service);
    let live_task = tokio::spawn(async move { poll_live_matches(live_service).await });
    let reported_service = Arc::clone(&service);
    let reported_task = tokio::spawn(async move { poll_reported_matches(reported_service).await });
    let result = client.start().await;
    poll_task.abort();
    live_task.abort();
    reported_task.abort();
    result?;
    Ok(())
}

impl Config {
    fn from_env() -> Result<Self, std::io::Error> {
        let token = required_env("SINGULARITY_DISCORD_TOKEN")?;
        let channel_id = parse_id(
            "SINGULARITY_DISCORD_CHANNEL_ID",
            &required_env("SINGULARITY_DISCORD_CHANNEL_ID")?,
        )?;
        let guild_id = env::var("SINGULARITY_DISCORD_GUILD_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| parse_guild_id("SINGULARITY_DISCORD_GUILD_ID", &value))
            .transpose()?;
        let poll_seconds = env::var("SINGULARITY_DISCORD_POLL_SECONDS")
            .unwrap_or_else(|_| DEFAULT_POLL_SECONDS.to_string())
            .parse::<u64>()
            .map_err(|_| invalid_setting("invalid Discord poll interval"))?;
        if poll_seconds == 0 {
            return Err(invalid_setting(
                "Discord poll interval must be greater than zero",
            ));
        }
        let timeout = env::var("SINGULARITY_DISCORD_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| DEFAULT_TIMEOUT_SECONDS.to_string())
            .parse::<u64>()
            .map_err(|_| invalid_setting("invalid Discord HTTP timeout"))?;
        if timeout == 0 {
            return Err(invalid_setting(
                "Discord HTTP timeout must be greater than zero",
            ));
        }
        Ok(Self {
            token,
            channel_id,
            guild_id,
            api_url: env::var("SINGULARITY_DISCORD_API_URL")
                .unwrap_or_else(|_| DEFAULT_API_URL.to_string()),
            api_token: env::var("SINGULARITY_DISCORD_READ_TOKEN")
                .or_else(|_| env::var("SINGULARITY_READ_TOKEN"))
                .ok(),
            web_url: env::var("SINGULARITY_DISCORD_WEB_URL")
                .unwrap_or_else(|_| DEFAULT_WEB_URL.to_string()),
            poll_interval: Duration::from_secs(poll_seconds),
            state_path: PathBuf::from(
                env::var("SINGULARITY_DISCORD_STATE_PATH")
                    .unwrap_or_else(|_| DEFAULT_STATE_PATH.to_string()),
            ),
        })
    }
}

impl ApiClient {
    fn from_config(config: &Config) -> Result<Self, std::io::Error> {
        let timeout = env::var("SINGULARITY_DISCORD_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| DEFAULT_TIMEOUT_SECONDS.to_string())
            .parse::<u64>()
            .map_err(|_| invalid_setting("invalid Discord HTTP timeout"))?;
        let http = HttpClient::builder()
            .timeout(Duration::from_secs(timeout))
            .user_agent("singularity-platform-discord-bot/0.1")
            .build()
            .map_err(std::io::Error::other)?;
        Ok(Self {
            http,
            base_url: config.api_url.trim_end_matches('/').to_string(),
            token: config.api_token.clone(),
        })
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ApiError> {
        self.get_url(format!(
            "{}/{}",
            self.base_url,
            path.trim_start_matches('/')
        ))
        .await
    }

    async fn get_url<T: DeserializeOwned>(&self, url: String) -> Result<T, ApiError> {
        let mut request = self.http.get(url);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|_| ApiError::unavailable())?;
        if !response.status().is_success() {
            return Err(ApiError::status(response.status().as_u16()));
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| ApiError::invalid_response())?;
        serde_json::from_slice(&body).map_err(|_| ApiError::invalid_response())
    }

    async fn list_events(
        &self,
        after_start_time: u32,
        after_match_id: u64,
    ) -> Result<Vec<ApiMatchEvent>, ApiError> {
        self.get(&format!(
            "events/matches?limit=100&after_start_time={after_start_time}&after_match_id={after_match_id}"
        ))
        .await
    }

    /// The live report list sits at the platform root, outside `/api`, so the
    /// web app's proxy never serves its account IDs to browsers.
    async fn list_live(&self, since: u64) -> Result<Vec<ApiLiveMatch>, ApiError> {
        let root = self.base_url.strip_suffix("/api").unwrap_or(&self.base_url);
        self.get_url(format!("{root}/live?since={since}")).await
    }

    async fn get_match(&self, identifier: &str) -> Result<ApiMatchDetail, ApiError> {
        self.get(&format!("matches/{}", identifier_segment(identifier)))
            .await
    }

    async fn get_player(
        &self,
        account_id: u32,
        last_matches: Option<u32>,
    ) -> Result<ApiPlayerAverage, ApiError> {
        let path = player_path(&format!("players/{account_id}"), last_matches);
        self.get(&path).await
    }

    async fn get_player_by_name(
        &self,
        name: &str,
        last_matches: Option<u32>,
    ) -> Result<ApiPlayerAverage, ApiError> {
        let path = player_path(
            &format!("players/by-name/{}", identifier_segment(name)),
            last_matches,
        );
        self.get(&path).await
    }
}

impl ApiError {
    fn unavailable() -> Self {
        Self("The match API is unavailable right now.".to_string())
    }

    fn invalid_response() -> Self {
        Self("The match API returned an invalid response.".to_string())
    }

    fn status(status: u16) -> Self {
        if status == 404 {
            Self("No matching player or match was found.".to_string())
        } else {
            Self("The match API could not complete that request.".to_string())
        }
    }
}

impl Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ApiError {}

impl State {
    async fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let bytes = match fs::read(path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if bytes.is_empty() {
            return Ok(Self {
                path: path.to_path_buf(),
                file: empty_state(),
            });
        }
        #[cfg(unix)]
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
        let file: StateFile = serde_json::from_slice(&bytes)?;
        if file.version != 1 {
            return Err(invalid_setting("unsupported Discord state version").into());
        }
        Ok(Self {
            path: path.to_path_buf(),
            file,
        })
    }

    async fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).await?;
        }
        let bytes = serde_json::to_vec_pretty(&self.file)?;
        let temp = self.path.with_file_name(format!(
            ".{}.tmp-{}-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("discord-state.json"),
            std::process::id(),
            now_seconds()
        ));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_data().await?;
        drop(file);
        #[cfg(unix)]
        fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o600)).await?;
        fs::rename(temp, &self.path).await?;
        Ok(())
    }
}

impl Service {
    async fn set_http(&self, http: Arc<Http>) {
        *self.http.lock().await = Some(http);
    }

    async fn handle_command(&self, command: &CommandInteraction) -> Result<Reply, ApiError> {
        let options = command.data.options();
        match command.data.name.as_str() {
            "link" => self.handle_link(command, &options).await,
            "last" => self.handle_last(command, &options).await,
            "match" => self.handle_match(&options).await,
            "stats" => self.handle_stats(command, &options).await,
            "leaderboard" => self.handle_leaderboard(&options).await,
            _ => Ok(Reply::Text("Unknown command.".to_string())),
        }
    }

    async fn handle_link(
        &self,
        command: &CommandInteraction,
        options: &[ResolvedOption<'_>],
    ) -> Result<Reply, ApiError> {
        let Some(input) = string_option(options, "account") else {
            return Ok(Reply::Text(
                "Provide a Steam profile URL or account ID.".to_string(),
            ));
        };
        let account_id = parse_account_id(&input).ok_or_else(|| {
            ApiError("That is not a supported Steam profile or account ID.".into())
        })?;
        // Linking doesn't need a tracked match: a new player has none until a
        // collector sends their first one. Only the reply depends on it.
        let tracked = self.api.get_player(account_id, Some(1)).await.is_ok();
        let user_id = command.user.id.get().to_string();
        let linked = LinkedAccount {
            account_id,
            discord_name: command.user.name.clone(),
            linked_at: now_seconds(),
        };
        let mut state = self.state.lock().await;
        state.file.links.insert(user_id, linked);
        state
            .save()
            .await
            .map_err(|_| ApiError("The bot could not save your account link.".into()))?;
        let note = if tracked {
            ""
        } else {
            " Singularity has no matches for you yet; they appear after your next match with the collector running."
        };
        Ok(Reply::Text(format!(
            "Your Steam account is linked. Account IDs are kept private by the bot.{note}"
        )))
    }

    async fn handle_last(
        &self,
        command: &CommandInteraction,
        options: &[ResolvedOption<'_>],
    ) -> Result<Reply, ApiError> {
        let player = if let Some(player) = string_option(options, "player") {
            self.player_lookup(&player, Some(1)).await?
        } else {
            let account_id = self.linked_account(command.user.id).await?;
            self.api.get_player(account_id, Some(1)).await?
        };
        let recent = player
            .recent
            .first()
            .ok_or_else(|| ApiError("That player has no processed matches yet.".into()))?;
        let detail = self.api.get_match(&recent.uuid).await?;
        Ok(Reply::Post(self.match_post(&detail, &[], &[])))
    }

    async fn handle_match(&self, options: &[ResolvedOption<'_>]) -> Result<Reply, ApiError> {
        let Some(identifier) = string_option(options, "identifier") else {
            return Ok(Reply::Text(
                "Provide a public match UUID or match ID.".to_string(),
            ));
        };
        let detail = self.api.get_match(&identifier).await?;
        Ok(Reply::Post(self.match_post(&detail, &[], &[])))
    }

    async fn handle_stats(
        &self,
        command: &CommandInteraction,
        options: &[ResolvedOption<'_>],
    ) -> Result<Reply, ApiError> {
        let last = integer_option(options, "last")
            .map(|value| value.clamp(1, i64::from(MAX_STATS_MATCHES)) as u32);
        let player = if let Some(player) = string_option(options, "player") {
            self.player_lookup(&player, last).await?
        } else {
            let account_id = self.linked_account(command.user.id).await?;
            self.api.get_player(account_id, last).await?
        };
        Ok(Reply::Embed(Box::new(self.stats_embed(&player, last))))
    }

    async fn handle_leaderboard(&self, options: &[ResolvedOption<'_>]) -> Result<Reply, ApiError> {
        let stat = string_option(options, "stat").unwrap_or_else(|| "kdr".to_string());
        let period = string_option(options, "period").unwrap_or_else(|| "all".to_string());
        let last = match period.as_str() {
            "all" => None,
            "last_10" => Some(10),
            "last_20" => Some(20),
            "last_50" => Some(50),
            _ => {
                return Ok(Reply::Text(
                    "Choose a supported leaderboard period.".to_string(),
                ));
            }
        };
        let links = {
            let state = self.state.lock().await;
            state.file.links.values().cloned().collect::<Vec<_>>()
        };
        if links.is_empty() {
            return Ok(Reply::Text(
                "Nobody has linked a Steam account yet.".to_string(),
            ));
        }
        let mut entries = Vec::new();
        for link in links {
            let Ok(player) = self.api.get_player(link.account_id, last).await else {
                continue;
            };
            entries.push(LeaderboardEntry {
                name: link.discord_name,
                value: stat_value(&stat, &player),
            });
        }
        entries.sort_by(|left, right| right.value.total_cmp(&left.value));
        if entries.is_empty() {
            return Err(ApiError(
                "No linked player has processed matches yet.".into(),
            ));
        }
        let lines = entries
            .iter()
            .take(10)
            .enumerate()
            .map(|(index, entry)| {
                format!(
                    "**{}. {}** — {}",
                    index + 1,
                    entry.name,
                    format_stat(&stat, entry.value)
                )
            })
            .collect::<Vec<_>>();
        Ok(Reply::Text(format!(
            "**Leaderboard: {} ({})**\n{}",
            stat_label(&stat),
            period_label(&period),
            lines.join("\n")
        )))
    }

    async fn linked_account(&self, user_id: UserId) -> Result<u32, ApiError> {
        self.state
            .lock()
            .await
            .file
            .links
            .get(&user_id.get().to_string())
            .map(|link| link.account_id)
            .ok_or_else(|| ApiError("Link your Steam account first with /link.".into()))
    }

    async fn player_lookup(
        &self,
        input: &str,
        last_matches: Option<u32>,
    ) -> Result<ApiPlayerAverage, ApiError> {
        if let Some(account_id) = parse_account_id(input) {
            self.api.get_player(account_id, last_matches).await
        } else {
            self.api.get_player_by_name(input, last_matches).await
        }
    }

    /// The after-match post. `played_by` lists the Discord users whose
    /// collectors reported the match; `friend_heroes` maps heroes to friends'
    /// names from the live post.
    fn match_post(
        &self,
        detail: &ApiMatchDetail,
        played_by: &[u64],
        friend_heroes: &[(String, String)],
    ) -> Post {
        match_post(detail, played_by, friend_heroes, &self.config.web_url)
    }

    fn stats_embed(&self, player: &ApiPlayerAverage, last: Option<u32>) -> CreateEmbed {
        let scope = last
            .map(|matches| format!("last {matches} matches"))
            .unwrap_or_else(|| "all processed matches".to_string());
        let heroes = player
            .heroes
            .iter()
            .take(5)
            .map(|hero| format!("{} ({})", hero.name, hero.matches))
            .collect::<Vec<_>>();
        CreateEmbed::new()
            .title(format!("{} — {}", display_name(&player.name), scope))
            .field("KDR", format!("{:.2}", player.kdr), true)
            .field("KDA", format!("{:.2}", player.kda), true)
            .field("Win rate", format!("{:.1}%", player.win_rate * 100.0), true)
            .field("Accuracy", format!("{:.1}%", player.accuracy * 100.0), true)
            .field(
                "Crit rate",
                format!("{:.1}%", player.crit_rate * 100.0),
                true,
            )
            .field(
                "Damage/min",
                format!("{:.0}", player.damage_per_minute),
                true,
            )
            .field(
                "Most played heroes",
                if heroes.is_empty() {
                    "No hero data".to_string()
                } else {
                    heroes.join(", ")
                },
                false,
            )
    }
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, context: Context, ready: Ready) {
        self.service.set_http(Arc::clone(&context.http)).await;
        let commands = command_definitions();
        let result = if let Some(guild_id) = self.service.config.guild_id {
            guild_id.set_commands(&context.http, commands).await
        } else {
            Command::set_global_commands(&context.http, commands).await
        };
        match result {
            Ok(commands) => {
                info!(count = commands.len(), user = %ready.user.name, "Discord bot ready")
            }
            Err(error) => warn!(%error, "Discord slash commands could not be registered"),
        }
    }

    async fn interaction_create(&self, context: Context, interaction: Interaction) {
        let Interaction::Command(command) = interaction else {
            return;
        };
        if let Err(error) = command.defer(&context.http).await {
            warn!(%error, "Discord interaction could not be deferred");
            return;
        }
        let reply = match self.service.handle_command(&command).await {
            Ok(reply) => reply,
            Err(error) => Reply::Text(error.to_string()),
        };
        let response = match reply {
            Reply::Text(content) => EditInteractionResponse::new().content(content),
            Reply::Embed(embed) => EditInteractionResponse::new().embed(*embed),
            Reply::Post(post) => EditInteractionResponse::new()
                .content(post.content)
                .embeds(post.embeds),
        };
        if let Err(error) = command.edit_response(&context.http, response).await {
            warn!(%error, "Discord interaction response failed");
        }
    }
}

async fn poll_notifications(service: Arc<Service>) {
    let mut ticker = interval(service.config.poll_interval);
    loop {
        ticker.tick().await;
        if let Err(error) = service.poll_once().await {
            warn!(%error, "Discord match notification poll failed");
        }
    }
}

async fn poll_reported_matches(service: Arc<Service>) {
    if service.live.is_none() {
        return;
    }
    let mut ticker = interval(REPORTED_POLL_INTERVAL);
    loop {
        ticker.tick().await;
        if let Err(error) = service.poll_reported_once().await {
            warn!(%error, "Reported live match poll failed");
        }
    }
}

async fn poll_live_matches(service: Arc<Service>) {
    let Some(live) = service.live.as_ref() else {
        return;
    };
    info!(
        interval_seconds = live.poll_interval.as_secs(),
        "Discord live match posts enabled"
    );
    let mut ticker = interval(live.poll_interval);
    loop {
        ticker.tick().await;
        if let Err(error) = service.poll_live_once(live).await {
            warn!(%error, "Discord live match poll failed");
        }
    }
}

impl Service {
    async fn poll_live_once(&self, live: &LiveScout) -> Result<(), Box<dyn std::error::Error>> {
        let Some(http) = self.http.lock().await.clone() else {
            return Ok(());
        };
        let linked = {
            let state = self.state.lock().await;
            state
                .file
                .links
                .iter()
                .filter_map(|(user_id, link)| Some((link.account_id, user_id.parse::<u64>().ok()?)))
                .collect::<HashMap<_, _>>()
        };
        if linked.is_empty() {
            return Ok(());
        }
        let account_ids = linked.keys().copied().collect::<Vec<_>>();
        let matches = live
            .active_matches(&account_ids)
            .await
            .map_err(|error| error.to_string())?;
        for active in matches {
            let has_linked = active
                .players
                .iter()
                .any(|player| player.account_id.is_some_and(|id| linked.contains_key(&id)));
            let posted = self
                .state
                .lock()
                .await
                .file
                .live_posted
                .contains(&active.match_id);
            if !has_linked || posted || !self.live_in_flight.lock().await.insert(active.match_id) {
                continue;
            }
            let names = Self::linked_names(&*self.state.lock().await);
            let post = live.scout(&active, &linked, &names, &[]).await;
            let message = CreateMessage::new()
                .content(live_heading(&active, ""))
                .embeds(post.embeds)
                .allowed_mentions(CreateAllowedMentions::new());
            let sent = self
                .service_send_message(&http, message)
                .await
                .map_err(|error| error.to_string());
            if sent.is_ok() {
                self.remember_live_post(active.match_id).await?;
            }
            self.live_in_flight.lock().await.remove(&active.match_id);
            sent?;
        }
        Ok(())
    }

    fn linked_accounts(state: &State) -> HashMap<u32, u64> {
        state
            .file
            .links
            .iter()
            .filter_map(|(user_id, link)| Some((link.account_id, user_id.parse::<u64>().ok()?)))
            .collect()
    }

    fn linked_names(state: &State) -> HashMap<u32, String> {
        state
            .file
            .links
            .values()
            .map(|link| (link.account_id, link.discord_name.clone()))
            .collect()
    }

    /// Keeps the friends' heroes for the after-match summary.
    async fn remember_live_friends(
        &self,
        match_id: u64,
        friends: Vec<(String, String)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.lock().await;
        state.file.live_friends.insert(match_id, friends);
        while state.file.live_friends.len() > LIVE_POSTED_LIMIT {
            state.file.live_friends.pop_first();
        }
        state.save().await
    }

    async fn remember_live_post(&self, match_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.lock().await;
        state.file.live_posted.push(match_id);
        let excess = state
            .file
            .live_posted
            .len()
            .saturating_sub(LIVE_POSTED_LIMIT);
        state.file.live_posted.drain(..excess);
        state.save().await
    }

    /// Deletes the live post of a lobby that was destroyed within minutes of
    /// forming: the match was cancelled because someone failed to load in.
    async fn delete_cancelled_posts(&self, reported: &[ApiLiveMatch]) {
        let Some(http) = self.http.lock().await.clone() else {
            return;
        };
        for report in reported.iter().filter(|report| cancelled(report)) {
            let Some(message_id) = self
                .state
                .lock()
                .await
                .file
                .live_messages
                .get(&report.match_id)
                .copied()
            else {
                continue;
            };
            let deleted = self
                .config
                .channel_id
                .delete_message(&http, MessageId::new(message_id))
                .await;
            match deleted {
                Ok(()) => info!(
                    match_id = report.match_id,
                    "Deleted the post for a cancelled match"
                ),
                Err(error) => {
                    warn!(match_id = report.match_id, %error, "Could not delete the post for a cancelled match")
                }
            }
            let mut state = self.state.lock().await;
            state.file.live_messages.remove(&report.match_id);
            if let Err(error) = state.save().await {
                warn!(%error, "Could not save the bot state");
            }
        }
    }

    /// Starts a lookup for each match a collector reported starting.
    async fn poll_reported_once(self: &Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        if self.live.is_none() || self.http.lock().await.is_none() {
            return Ok(());
        }
        let now = now_seconds();
        let reported = self
            .api
            .list_live(now.saturating_sub(REPORTED_MAX_AGE_SECONDS))
            .await?;
        self.delete_cancelled_posts(&reported).await;
        for report in reported {
            let Some(started_at) = report.started_at else {
                continue;
            };
            let fresh = report.ended_at.is_none()
                && now.saturating_sub(started_at) < REPORTED_MAX_AGE_SECONDS;
            let (posted, unfilled) = {
                let state = self.state.lock().await;
                let unfilled = state
                    .file
                    .live_unfilled
                    .contains(&report.match_id)
                    .then(|| state.file.live_messages.get(&report.match_id).copied())
                    .flatten();
                (state.file.live_posted.contains(&report.match_id), unfilled)
            };
            if !fresh || (posted && unfilled.is_none()) {
                continue;
            }
            if !self.live_in_flight.lock().await.insert(report.match_id) {
                continue;
            }
            let service = Arc::clone(self);
            tokio::spawn(async move {
                let match_id = report.match_id;
                if let Err(error) = service.post_reported(report, unfilled).await {
                    warn!(match_id, %error, "Reported live match could not be posted");
                }
                service.live_in_flight.lock().await.remove(&match_id);
            });
        }
        Ok(())
    }

    /// Reads the teams from the live broadcast, falling back to the watch-tab feed.
    /// Posts "match found" straight away, then fills the message in with both
    /// teams once the live broadcast opens (a few minutes into the match),
    /// falling back to the watch-tab feed.
    /// `existing` is a "match found" post left unfinished by a restart.
    async fn post_reported(
        &self,
        report: ApiLiveMatch,
        existing: Option<u64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(live) = self.live.as_ref() else {
            return Ok(());
        };
        let Some(http) = self.http.lock().await.clone() else {
            return Ok(());
        };
        let linked = Self::linked_accounts(&*self.state.lock().await);
        let message_id = if let Some(message_id) = existing {
            info!(
                match_id = report.match_id,
                "Resuming an unfinished live post"
            );
            MessageId::new(message_id)
        } else {
            let message = self
                .config
                .channel_id
                .send_message(
                    &http,
                    CreateMessage::new()
                        .content(match_found_text(&report, &linked))
                        .allowed_mentions(CreateAllowedMentions::new()),
                )
                .await?;
            // Remember it now so a restart doesn't post the match twice, and
            // so a restart can finish it.
            self.remember_live_post(report.match_id).await?;
            let mut state = self.state.lock().await;
            state
                .file
                .live_messages
                .insert(report.match_id, message.id.get());
            while state.file.live_messages.len() > LIVE_POSTED_LIMIT {
                state.file.live_messages.pop_first();
            }
            state.file.live_unfilled.insert(report.match_id);
            state.save().await?;
            info!(match_id = report.match_id, "Posted match found");
            message.id
        };

        let mut lookup = live
            .broadcast_players(report.match_id, report.started_at)
            .await;
        // Server errors and timeouts are usually brief; each try uses one of the
        // 6 lookups per hour, so try at most three times.
        for wait in BROADCAST_RETRY_WAITS {
            match &lookup {
                Err(error) if is_transient(error.as_ref()) => {
                    warn!(match_id = report.match_id, %error, retry_in_seconds = wait.as_secs(), "Live broadcast lookup failed; retrying");
                    tokio::time::sleep(wait).await;
                    lookup = live
                        .broadcast_players(report.match_id, report.started_at)
                        .await;
                }
                _ => break,
            }
        }
        let active = match lookup {
            Ok(active) => Some(active),
            Err(error) => {
                warn!(match_id = report.match_id, %error, "Live broadcast lookup failed; trying the watch-tab feed");
                live.active_matches(&report.account_ids)
                    .await
                    .ok()
                    .and_then(|feed| {
                        feed.into_iter()
                            .find(|active| active.match_id == report.match_id)
                    })
            }
        };
        let edit = match active {
            Some(active) => {
                let names = Self::linked_names(&*self.state.lock().await);
                let post = live
                    .scout(&active, &linked, &names, &report.account_ids)
                    .await;
                self.remember_live_friends(report.match_id, post.friends)
                    .await?;
                EditMessage::new()
                    .content(live_heading(&active, &players_text(&report, &linked)))
                    .embeds(post.embeds)
                    .allowed_mentions(CreateAllowedMentions::new())
            }
            None => EditMessage::new().content(format!(
                "{}\nCouldn't read the lobby for this match.",
                match_found_text(&report, &linked)
            )),
        };
        self.config
            .channel_id
            .edit_message(&http, message_id, edit)
            .await?;
        let mut state = self.state.lock().await;
        state.file.live_unfilled.remove(&report.match_id);
        state.save().await?;
        info!(match_id = report.match_id, "Posted reported live match");
        Ok(())
    }

    async fn poll_once(&self) -> Result<(), Box<dyn std::error::Error>> {
        let Some(http) = self.http.lock().await.clone() else {
            return Ok(());
        };
        let (initialized, cursor) = {
            let state = self.state.lock().await;
            (
                state.file.initialized,
                (state.file.last_start_time, state.file.last_match_id),
            )
        };
        if !initialized {
            let mut bootstrap_cursor = (0, 0);
            loop {
                let mut events = self
                    .api
                    .list_events(bootstrap_cursor.0, bootstrap_cursor.1)
                    .await?;
                events.sort_by_key(|item| (item.start_time, item.match_id));
                let Some(latest) = events.last() else {
                    break;
                };
                bootstrap_cursor = (latest.start_time, latest.match_id);
                if events.len() < EVENT_PAGE_LIMIT {
                    break;
                }
            }
            let mut state = self.state.lock().await;
            if !state.file.initialized {
                state.file.last_start_time = bootstrap_cursor.0;
                state.file.last_match_id = bootstrap_cursor.1;
                state.file.initialized = true;
                state.save().await?;
            }
            return Ok(());
        }

        let mut events = self.api.list_events(cursor.0, cursor.1).await?;
        events.sort_by_key(|item| (item.start_time, item.match_id));
        let mut state = self.state.lock().await;
        if !state.file.initialized {
            if let Some(latest) = events.last() {
                state.file.last_start_time = latest.start_time;
                state.file.last_match_id = latest.match_id;
            }
            state.file.initialized = true;
            state.save().await?;
            return Ok(());
        }
        // Matches our collectors reported live get a post even without
        // configured members, naming the linked players who were in them.
        let reported = if events.is_empty() {
            HashMap::new()
        } else {
            self.api
                .list_live(now_seconds().saturating_sub(TRACKED_WINDOW_SECONDS))
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|live| (live.match_id, live.account_ids))
                .collect::<HashMap<_, _>>()
        };
        let linked = Self::linked_accounts(&state);
        for event in events {
            let detail = self.api.get_match(&event.uuid).await?;
            let reporters = reported.get(&event.match_id);
            if reporters.is_some() || detail.players.iter().any(|player| player.member.is_some()) {
                let mut played_by = reporters
                    .into_iter()
                    .flatten()
                    .filter_map(|account_id| linked.get(account_id).copied())
                    .collect::<Vec<_>>();
                played_by.sort_unstable();
                played_by.dedup();
                let friends = state
                    .file
                    .live_friends
                    .get(&event.match_id)
                    .cloned()
                    .unwrap_or_default();
                let post = self.match_post(&detail, &played_by, &friends);
                let message = CreateMessage::new()
                    .content(post.content)
                    .embeds(post.embeds)
                    .allowed_mentions(CreateAllowedMentions::new());
                self.service_send_message(&http, message).await?;
            }
            state.file.last_start_time = event.start_time;
            state.file.last_match_id = event.match_id;
            state.save().await?;
        }
        Ok(())
    }

    async fn service_send_message(
        &self,
        http: &Http,
        message: CreateMessage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.config.channel_id.send_message(http, message).await?;
        Ok(())
    }
}

/// A lobby destroyed within five minutes of forming never became a match.
fn cancelled(report: &ApiLiveMatch) -> bool {
    match (report.started_at, report.ended_at) {
        (Some(started), Some(ended)) => ended.saturating_sub(started) < CANCELLED_WITHIN_SECONDS,
        _ => false,
    }
}

/// Server errors, timeouts and dropped connections are worth retrying; a
/// rejected query or an error event from the stream is not.
fn is_transient(error: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    error.downcast_ref::<reqwest::Error>().is_some_and(|error| {
        error.is_timeout()
            || error.is_connect()
            || error.is_body()
            || error
                .status()
                .is_some_and(|status| status.is_server_error())
    })
}

/// Linked players who reported the match, without adding presentation text.
fn players_text(report: &ApiLiveMatch, linked: &HashMap<u32, u64>) -> String {
    let mut players = report
        .account_ids
        .iter()
        .filter_map(|account_id| linked.get(account_id))
        .map(|user| format!("<@{user}>"))
        .collect::<Vec<_>>();
    players.sort();
    players.dedup();
    players.join(", ")
}

/// The first message for a reported match, before the teams are known.
fn match_found_text(report: &ApiLiveMatch, linked: &HashMap<u32, u64>) -> String {
    let reporters = players_text(report, linked);
    let reporters = if reporters.is_empty() {
        "Scouting the lobby".to_string()
    } else {
        format!("Reported by {reporters} · scouting the lobby")
    };
    format!(
        "🔎 **Match found**\nMatch `{}` · {reporters}",
        report.match_id
    )
}

/// The after-match post: result line, then one player card per team member.
fn match_post(
    detail: &ApiMatchDetail,
    played_by: &[u64],
    friend_heroes: &[(String, String)],
    web_url: &str,
) -> Post {
    let friend_of = |player: &ApiDetailPlayer| {
        friend_heroes
            .iter()
            .find(|(hero, _)| *hero == player.hero.name)
            .map(|(_, name)| name.clone())
            .or_else(|| player.member.as_ref().map(|member| member.name.clone()))
    };
    let our_teams = detail
        .players
        .iter()
        .filter(|player| friend_of(player).is_some())
        .map(|player| player.team)
        .collect::<Vec<_>>();
    let result = if our_teams.is_empty() {
        "🏁 **Match complete**"
    } else if our_teams.contains(&detail.winning_team) {
        "✅ **Victory**"
    } else {
        "❌ **Defeat**"
    };
    let mut details = Vec::new();
    if !played_by.is_empty() {
        let users = played_by
            .iter()
            .map(|user| format!("<@{user}>"))
            .collect::<Vec<_>>();
        details.push(format!("👥 {}", users.join(", ")));
    }
    details.push(format!(
        "[View full match](<{}/m/{}>)",
        web_url.trim_end_matches('/'),
        detail.uuid
    ));
    let content = format!(
        "{result} · **{}** · {}\n{}",
        detail.mode,
        format_duration(detail.duration_s),
        details.join(" · ")
    );

    let top_damage = detail
        .players
        .iter()
        .map(|player| player.hero_damage)
        .max()
        .unwrap_or_default();
    let our_team = our_teams.first().copied();
    let mut teams = vec![0, 1];
    if our_team == Some(1) {
        teams.reverse();
    }
    let embeds = teams
        .into_iter()
        .filter_map(|team| {
            let players = detail
                .players
                .iter()
                .filter(|player| player.team == team)
                .collect::<Vec<_>>();
            if players.is_empty() {
                return None;
            }
            let index = usize::try_from(team).unwrap_or_default().min(1);
            let mut title = TEAM_NAMES[index].to_string();
            let badge = if index == 0 { "🟠" } else { "🔵" };
            if team == detail.winning_team {
                title = format!("{badge} {title} · Victory 🏆");
            } else {
                title = format!("{badge} {title}");
            }
            let mut embed = CreateEmbed::new().title(title).colour(TEAM_COLOURS[index]);
            for player in players {
                let hero = safe_embed_text(&player.hero.name, 80);
                let player_name = friend_of(player)
                    .filter(|name| !name.trim().is_empty())
                    .map(|name| safe_embed_text(&name, 96));
                let field_name = player_name
                    .map(|name| format!("{hero} · {name}"))
                    .unwrap_or(hero);
                let shots = player.aim.shots_hit + player.aim.shots_missed;
                let aim = if shots > 0 {
                    format!("{:.0}%", accuracy(player) * 100.0)
                } else {
                    "—".to_string()
                };
                let damage = format_thousands(player.hero_damage);
                let damage = if player.hero_damage == top_damage && top_damage > 0 {
                    format!("💥 Top damage · **{damage}**")
                } else {
                    format!("Damage · **{damage}**")
                };
                let value = format!(
                    "K/D/A · **{} / {} / {}**\n{damage} · Accuracy · **{aim}**",
                    player.kills, player.deaths, player.assists
                );
                embed = embed.field(field_name, value, true);
            }
            Some(embed)
        })
        .collect::<Vec<_>>();
    let embeds = match embeds.split_last() {
        Some((last, rest)) => {
            let mut embeds = rest.to_vec();
            embeds.push(
                last.clone()
                    .footer(serenity::all::CreateEmbedFooter::new(format!(
                        "Match {} · K/D/A: kills / deaths / assists · Accuracy: shots hit / shots fired",
                        detail.match_id
                    ))),
            );
            embeds
        }
        None => embeds,
    };
    Post { content, embeds }
}

/// 47104 -> "47.1k".
fn format_thousands(value: u32) -> String {
    if value >= 1000 {
        format!("{:.1}k", f64::from(value) / 1000.0)
    } else {
        value.to_string()
    }
}

fn command_definitions() -> Vec<CreateCommand> {
    vec![
        CreateCommand::new("link")
            .description("Link your Steam profile or account ID")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "account",
                    "Steam profile URL or account ID",
                )
                .required(true),
            ),
        CreateCommand::new("last")
            .description("Show a player's latest match")
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "player", "Name or account ID")
                    .required(false),
            ),
        CreateCommand::new("match")
            .description("Show a match summary")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "identifier",
                    "Public UUID or match ID",
                )
                .required(true),
            ),
        CreateCommand::new("stats")
            .description("Show player statistics")
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "player", "Name or account ID")
                    .required(false),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "last",
                    "Only the last N processed matches",
                )
                .min_int_value(1)
                .max_int_value(u64::from(MAX_STATS_MATCHES))
                .required(false),
            ),
        CreateCommand::new("leaderboard")
            .description("Rank linked players")
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "stat", "Statistic to rank")
                    .add_string_choice("KDR", "kdr")
                    .add_string_choice("Accuracy", "accuracy")
                    .add_string_choice("Damage per minute", "damage_per_minute")
                    .required(true),
            )
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "period", "Match window")
                    .add_string_choice("All matches", "all")
                    .add_string_choice("Last 10", "last_10")
                    .add_string_choice("Last 20", "last_20")
                    .add_string_choice("Last 50", "last_50")
                    .required(false),
            ),
    ]
}

fn string_option(options: &[ResolvedOption<'_>], name: &str) -> Option<String> {
    options.iter().find_map(|option| {
        if option.name != name {
            return None;
        }
        match option.value {
            ResolvedValue::String(value) => Some(value.to_string()),
            _ => None,
        }
    })
}

fn integer_option(options: &[ResolvedOption<'_>], name: &str) -> Option<i64> {
    options.iter().find_map(|option| {
        if option.name != name {
            return None;
        }
        match option.value {
            ResolvedValue::Integer(value) => Some(value),
            _ => None,
        }
    })
}

fn stat_value(stat: &str, player: &ApiPlayerAverage) -> f64 {
    match stat {
        "accuracy" => player.accuracy,
        "damage_per_minute" => player.damage_per_minute,
        _ => player.kdr,
    }
}

fn stat_label(stat: &str) -> &str {
    match stat {
        "accuracy" => "Accuracy",
        "damage_per_minute" => "Damage per minute",
        _ => "KDR",
    }
}

fn period_label(period: &str) -> &str {
    match period {
        "last_10" => "last 10",
        "last_20" => "last 20",
        "last_50" => "last 50",
        _ => "all",
    }
}

fn format_stat(stat: &str, value: f64) -> String {
    match stat {
        "accuracy" => format!("{:.1}%", value * 100.0),
        "damage_per_minute" => format!("{value:.0}"),
        _ => format!("{value:.2}"),
    }
}

fn accuracy(player: &ApiDetailPlayer) -> f64 {
    let shots = player.aim.shots_hit + player.aim.shots_missed;
    if shots == 0 {
        0.0
    } else {
        f64::from(player.aim.shots_hit) / f64::from(shots)
    }
}

fn display_name(name: &str) -> &str {
    if name.is_empty() { "Player" } else { name }
}

fn format_duration(seconds: u32) -> String {
    format!("{}:{:02}", seconds / 60, seconds % 60)
}

fn parse_account_id(input: &str) -> Option<u32> {
    let input = input.trim();
    if let Ok(account_id) = input.parse::<u32>() {
        return (account_id > 0).then_some(account_id);
    }
    let profile_id = input
        .split('/')
        .rfind(|part| !part.is_empty())?
        .split(['?', '#'])
        .next()?;
    let steam64 = profile_id.parse::<u64>().ok()?;
    if steam64 > STEAM64_BASE {
        let account_id = steam64 - STEAM64_BASE;
        return u32::try_from(account_id).ok().filter(|id| *id > 0);
    }
    None
}

fn identifier_segment(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| {
            if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
                vec![byte as char]
            } else {
                format!("%{byte:02X}").chars().collect()
            }
        })
        .collect()
}

fn player_path(path: &str, last_matches: Option<u32>) -> String {
    match last_matches {
        Some(last_matches) => format!("{path}?last={last_matches}"),
        None => path.to_string(),
    }
}

fn empty_state() -> StateFile {
    StateFile {
        version: 1,
        initialized: false,
        last_start_time: 0,
        last_match_id: 0,
        links: BTreeMap::new(),
        live_posted: Vec::new(),
        live_friends: BTreeMap::new(),
        live_messages: BTreeMap::new(),
        live_unfilled: BTreeSet::new(),
    }
}

fn required_env(name: &str) -> Result<String, std::io::Error> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid_setting(&format!("{name} is required")))
}

fn parse_id(name: &str, value: &str) -> Result<serenity::all::ChannelId, std::io::Error> {
    value
        .parse::<u64>()
        .map(serenity::all::ChannelId::new)
        .map_err(|_| invalid_setting(&format!("{name} must be an unsigned Discord ID")))
}

fn parse_guild_id(name: &str, value: &str) -> Result<GuildId, std::io::Error> {
    value
        .parse::<u64>()
        .map(GuildId::new)
        .map_err(|_| invalid_setting(&format!("{name} must be an unsigned Discord ID")))
}

fn invalid_setting(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detail_player(team: i32, hero: &str, damage: u32) -> ApiDetailPlayer {
        ApiDetailPlayer {
            team,
            member: None,
            hero: ApiHero { name: hero.into() },
            kills: 8,
            deaths: 2,
            assists: 9,
            hero_damage: damage,
            aim: ApiAim {
                shots_hit: 30,
                shots_missed: 70,
            },
        }
    }

    #[test]
    fn summary_puts_our_team_first_and_marks_the_result() {
        let detail = ApiMatchDetail {
            uuid: "abc".into(),
            match_id: 7,
            duration_s: 1910,
            mode: "6v6".into(),
            winning_team: 0,
            players: vec![
                detail_player(0, "Haze", 30_000),
                detail_player(1, "Vyper", 47_104),
                detail_player(1, "Bebop", 12_000),
            ],
        };
        let friends = [("Vyper".to_string(), "hobnobs".to_string())];

        let post = match_post(&detail, &[5], &friends, "http://rake/");

        assert_eq!(
            post.content,
            "❌ **Defeat** · **6v6** · 31:50\n👥 <@5> · [View full match](<http://rake/m/abc>)"
        );
        let embeds = serde_json::to_value(&post.embeds).unwrap();
        assert_eq!(embeds[0]["title"], "🔵 Archmother");
        assert_eq!(embeds[1]["title"], "🟠 Hidden King · Victory 🏆");
        let ours = &embeds[0]["fields"][0];
        assert_eq!(ours["name"], "Vyper · hobnobs");
        assert!(
            ours["value"]
                .as_str()
                .unwrap()
                .contains("Top damage · **47.1k**")
        );
        assert!(ours["value"].as_str().unwrap().contains("8 / 2 / 9"));
        assert!(
            embeds[1]["fields"][0]["value"]
                .as_str()
                .unwrap()
                .contains("Accuracy · **30%**")
        );
        assert!(
            embeds[1]["footer"]["text"]
                .as_str()
                .unwrap()
                .starts_with("Match 7")
        );
    }

    /// Prints the summary post for a saved `/api/matches/{id}` response.
    #[test]
    #[ignore = "needs SINGULARITY_PREVIEW_MATCH pointing at a match JSON file"]
    fn summary_preview() {
        let path = env::var("SINGULARITY_PREVIEW_MATCH").unwrap();
        let detail: ApiMatchDetail = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let hero = detail.players[0].hero.name.clone();
        let post = match_post(
            &detail,
            &[1],
            &[(hero, "friend".into())],
            "http://192.168.1.110",
        );
        let message = serde_json::json!({"content": post.content, "embeds": post.embeds});
        println!("{message}");
    }

    #[test]
    fn short_lobbies_count_as_cancelled() {
        let report = |ended_at| ApiLiveMatch {
            match_id: 1,
            started_at: Some(1_000),
            ended_at,
            account_ids: Vec::new(),
        };
        assert!(cancelled(&report(Some(1_078))));
        assert!(!cancelled(&report(Some(1_000 + CANCELLED_WITHIN_SECONDS))));
        assert!(!cancelled(&report(None)));
    }

    #[test]
    fn only_request_failures_are_retried() {
        let rejected: Box<dyn std::error::Error + Send + Sync> = "Invalid query".into();
        assert!(!is_transient(rejected.as_ref()));
    }

    #[test]
    fn match_found_names_each_linked_player_once() {
        let report = ApiLiveMatch {
            match_id: 42,
            started_at: Some(1),
            ended_at: None,
            account_ids: vec![1, 2, 3],
        };
        let linked = HashMap::from([(1, 10), (3, 30)]);
        assert_eq!(
            match_found_text(&report, &linked),
            "🔎 **Match found**\nMatch `42` · Reported by <@10>, <@30> · scouting the lobby"
        );
    }

    #[test]
    fn parses_numeric_and_steam64_profile_ids() {
        assert_eq!(parse_account_id("12345"), Some(12345));
        assert_eq!(
            parse_account_id("https://steamcommunity.com/profiles/76561197960265728"),
            None
        );
        assert_eq!(
            parse_account_id("https://steamcommunity.com/profiles/76561197960265729"),
            Some(1)
        );
    }

    #[test]
    fn percent_encodes_api_path_segments() {
        assert_eq!(identifier_segment("The Friend/One"), "The%20Friend%2FOne");
    }

    #[test]
    fn computes_leaderboard_values() {
        let player = ApiPlayerAverage {
            name: String::new(),
            kdr: 2.0,
            kda: 3.0,
            accuracy: 0.5,
            crit_rate: 0.1,
            win_rate: 1.0,
            damage_per_minute: 1234.0,
            heroes: Vec::new(),
            recent: Vec::new(),
        };
        assert_eq!(stat_value("kdr", &player), 2.0);
        assert_eq!(stat_value("accuracy", &player), 0.5);
        assert_eq!(stat_value("damage_per_minute", &player), 1234.0);
    }
}
