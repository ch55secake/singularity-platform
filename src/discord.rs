use std::{
    collections::{BTreeMap, HashMap},
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
        Command, CommandInteraction, CommandOptionType, CreateCommand, CreateCommandOption,
        CreateEmbed, CreateMessage, EditInteractionResponse, EventHandler, GatewayIntents, GuildId,
        Http, Interaction, Ready, ResolvedOption, ResolvedValue, UserId,
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

use crate::discord_live::LiveScout;

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
}

struct Handler {
    service: Arc<Service>,
}

enum Reply {
    Text(String),
    Embed(Box<CreateEmbed>),
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
    let result = client.start().await;
    poll_task.abort();
    live_task.abort();
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
        let url = format!("{}/{}", self.base_url, path.trim_start_matches('/'));
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
        Ok(Reply::Embed(Box::new(self.match_embed(&detail))))
    }

    async fn handle_match(&self, options: &[ResolvedOption<'_>]) -> Result<Reply, ApiError> {
        let Some(identifier) = string_option(options, "identifier") else {
            return Ok(Reply::Text(
                "Provide a public match UUID or match ID.".to_string(),
            ));
        };
        let detail = self.api.get_match(&identifier).await?;
        Ok(Reply::Embed(Box::new(self.match_embed(&detail))))
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

    fn match_embed(&self, detail: &ApiMatchDetail) -> CreateEmbed {
        let group_players = detail
            .players
            .iter()
            .filter(|player| player.member.is_some())
            .collect::<Vec<_>>();
        let result = if group_players
            .iter()
            .any(|player| player.team == detail.winning_team)
        {
            "Group win"
        } else if group_players.is_empty() {
            "Match complete"
        } else {
            "Group loss"
        };
        let player_lines = detail
            .players
            .iter()
            .map(|player| {
                format!(
                    "{} — {} K/D/A {}/{}/{}",
                    player
                        .member
                        .as_ref()
                        .map(|member| member.name.as_str())
                        .unwrap_or(player.hero.name.as_str()),
                    player.hero.name,
                    player.kills,
                    player.deaths,
                    player.assists
                )
            })
            .collect::<Vec<_>>();
        let best_damage = detail
            .players
            .iter()
            .max_by_key(|player| player.hero_damage);
        let best_accuracy = detail
            .players
            .iter()
            .filter(|player| player.aim.shots_hit + player.aim.shots_missed > 0)
            .max_by(|left, right| accuracy(left).total_cmp(&accuracy(right)));
        let mut embed = CreateEmbed::new()
            .title(format!("{} — {}", result, detail.mode))
            .description(format!(
                "{}\n[Open match in Singularity]({}/m/{})",
                format_duration(detail.duration_s),
                self.config.web_url.trim_end_matches('/'),
                detail.uuid
            ))
            .field("Players", player_lines.join("\n"), false);
        if let Some(player) = best_damage {
            embed = embed.field(
                "Top hero damage",
                format!("{} — {}", player.hero.name, player.hero_damage),
                true,
            );
        }
        if let Some(player) = best_accuracy {
            embed = embed.field(
                "Best accuracy",
                format!("{} — {:.1}%", player.hero.name, accuracy(player) * 100.0),
                true,
            );
        }
        embed.footer(serenity::all::CreateEmbedFooter::new(format!(
            "Match {}",
            detail.match_id
        )))
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
            if !has_linked || posted {
                continue;
            }
            let embed = live.scout(&active, &linked).await;
            self.service_send_message(&http, CreateMessage::new().embed(embed))
                .await?;
            let mut state = self.state.lock().await;
            state.file.live_posted.push(active.match_id);
            let excess = state
                .file
                .live_posted
                .len()
                .saturating_sub(LIVE_POSTED_LIMIT);
            state.file.live_posted.drain(..excess);
            state.save().await?;
        }
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
        for event in events {
            let detail = self.api.get_match(&event.uuid).await?;
            if detail.players.iter().any(|player| player.member.is_some()) {
                let message = CreateMessage::new().embed(self.match_embed(&detail));
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
