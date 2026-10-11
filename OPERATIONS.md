# Operations

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `SINGULARITY_BIND` | `0.0.0.0:8080` | Listen address |
| `SINGULARITY_INGEST_TOKEN` | unset | Bearer token |
| `SINGULARITY_CREDENTIALS_PATH` | `data/collectors.json` | Hashed collector credentials |
| `SINGULARITY_SPOOL_PATH` | `data/salts.jsonl` | Durable input spool |
| `SINGULARITY_COLLECTOR_MAX_BATCH_SIZE` | `1000` | Maximum records per collector request |
| `SINGULARITY_COLLECTOR_RATE_LIMIT` | `60` | Requests per collector window |
| `SINGULARITY_COLLECTOR_RATE_WINDOW_SECONDS` | `60` | Rate-limit window |
| `SINGULARITY_METADATA_SPOOL_PATH` | `SINGULARITY_SPOOL_PATH` | Worker spool override |
| `SINGULARITY_METADATA_STATE_PATH` | `data/metadata-state.json` | Metadata job state |
| `SINGULARITY_METADATA_OUTPUT_DIR` | `data/metadata` | Decompressed metadata handoff files |
| `SINGULARITY_METADATA_PARSE_STATE_PATH` | `data/metadata-parse-state.json` | Parser idempotency state |
| `SINGULARITY_METADATA_BASE_URL` | `http://replay{cluster}.valve.net/1422450` | Metadata URL base |
| `SINGULARITY_METADATA_TIMEOUT_SECONDS` | `30` | Per-download timeout |
| `SINGULARITY_METADATA_RETRY_BASE_SECONDS` | `30` | Initial retry delay |
| `SINGULARITY_METADATA_RETRY_MAX_SECONDS` | `3600` | Maximum retry delay |
| `SINGULARITY_REPLAY_SPOOL_PATH` | `SINGULARITY_SPOOL_PATH` | Replay worker spool override |
| `SINGULARITY_REPLAY_STATE_PATH` | `data/replay-state.json` | Replay job state |
| `SINGULARITY_REPLAY_OUTPUT_DIR` | `data/replays` | Temporary decompressed demos |
| `SINGULARITY_REPLAY_BASE_URL` | `http://replay{cluster}.valve.net/1422450` | Replay URL base |
| `SINGULARITY_REPLAY_TIMEOUT_SECONDS` | `60` | Per-download and ClickHouse timeout |
| `SINGULARITY_REPLAY_RETRY_BASE_SECONDS` | `30` | Initial retry delay |
| `SINGULARITY_REPLAY_RETRY_MAX_SECONDS` | `3600` | Maximum retry delay |
| `SINGULARITY_REPLAY_MAX_COMPRESSED_BYTES` | `536870912` | Download size limit |
| `SINGULARITY_REPLAY_MAX_DECOMPRESSED_BYTES` | `2147483648` | Expanded demo size limit |
| `SINGULARITY_REPLAY_MAX_DISK_BYTES` | `34359738368` | Replay spool disk reservation |
| `SINGULARITY_REPLAY_DEBUG_RETENTION_SECONDS` | `0` | Raw demo retention after parse |
| `SINGULARITY_CLICKHOUSE_URL` | unset | ClickHouse HTTP endpoint for parser |
| `SINGULARITY_CLICKHOUSE_DATABASE` | `singularity` | ClickHouse database |
| `SINGULARITY_CLICKHOUSE_USER` | unset | ClickHouse user |
| `SINGULARITY_CLICKHOUSE_PASSWORD` | unset | ClickHouse password |
| `SINGULARITY_READ_TOKEN` | unset | Optional bearer token for read endpoints |
| `SINGULARITY_READ_TIMEOUT_SECONDS` | `10` | Read-side ClickHouse timeout |
| `SINGULARITY_MEMBER_NAMES_JSON` | unset | Optional JSON object mapping account IDs to friend names |
| `SINGULARITY_ASSETS_BASE_URL` | `https://api.deadlock-api.com/v1/assets` | Public asset API base |
| `SINGULARITY_ASSETS_STATE_PATH` | `data/assets-state.json` | Asset snapshot state |
| `SINGULARITY_ASSETS_TIMEOUT_SECONDS` | `30` | Asset request timeout |
| `RUST_LOG` | `singularity_platform=info` | Logging filter |

## Collector credentials

Provision credentials offline on the platform host:

```bash
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials create collector-name
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials rotate collector-name
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials revoke collector-name
SINGULARITY_CREDENTIALS_PATH=data/collectors.json \
  cargo run -- credentials list
```

The create and rotate commands print the new bearer token once. Store it in
the collector's private configuration and do not put it in source control.
Credential changes are written atomically, but the running platform reads the
file at startup; restart the service after creating, rotating, or revoking a
credential. The legacy `SINGULARITY_INGEST_TOKEN` remains available during
migration and is attributed as `legacy-global-token`.

## Metadata worker

Run one restart-safe metadata pass after ingestion, from a cron or deployment
worker:

```bash
singularity-platform metadata-worker
```

The worker reads `SINGULARITY_METADATA_SPOOL_PATH` or
`SINGULARITY_SPOOL_PATH`, downloads each eligible metadata file, accepts the
known zstd payload despite its `.bz2` suffix, and falls back to bzip2 when the
magic bytes indicate bzip2. Decompressed handoff files are written atomically
under `SINGULARITY_METADATA_OUTPUT_DIR`. The state file records
`metadata_pending`/`metadata_processed`, attempts, retry time, and a bounded
error code without storing the URL or salt. Failed downloads use exponential
backoff, including 404 and 5xx responses.

After a successful download, apply `schema/002_matches.sql` and run the
parser against the ClickHouse HTTP endpoint:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform metadata-parser
```

The parser writes match, player, item-timeline, and player-stat rows as JSONEachRow and
records processed job digests in `SINGULARITY_METADATA_PARSE_STATE_PATH`.
Timeline rows include the aim, kill-type, damage, mitigation, healing, power,
and health counters used by the derived views. `player_stat_averages` selects
the last snapshot for each player in each match before averaging across
matches. ReplacingMergeTree tables and the parser state make repeated parsing
idempotent.

## Replay worker

Apply `schema/004_replays.sql`, then run one restart-safe replay pass after
metadata ingestion:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform replay-worker
```

The worker selects records with a `replay_salt`, downloads the replay from the
Valve CDN, accepts zstd payloads despite the `.dem.bz2` suffix, and parses
player-pawn positions with the pinned Haste revision. State transitions are
`replay_pending`, `replay_downloaded`, and `replay_parsed`; failures retain a
bounded error code and exponential retry time without storing salts or URLs.
Rows use deterministic model-version keys and ReplacingMergeTree, so a
download or ClickHouse retry does not create a second logical match. Raw demos
are deleted after successful parsing by default. Set
`SINGULARITY_REPLAY_DEBUG_RETENTION_SECONDS` only when a bounded debug window is
needed; expired retained files are removed on the next worker pass.

Damage matrix samples are written to `match_damage`; query
`match_damage_player_reconciliation` to compare the normalized stat-type-0
totals against each player's final `player_damage` value. The raw matrix value
is retained because the protobuf encodes damage in doubled fixed-point units.

Death events are written to `match_deaths` with nullable position fields for
older or incomplete metadata. Use `match_deaths_with_players` for hero/team
labels and `match_death_count_reconciliation` to verify that recorded deaths
match the scoreboard.

Player paths are written to `match_player_positions` as world coordinates. A
sample match with 32 minutes, 12 players, and one-second samples produces
approximately 23,040 rows; ClickHouse's compact numeric schema keeps this at
well below a megabyte in the local smoke estimate before replication. To check
alignment, compare a death's `game_time_s` with the matching victim path
sample; the sample metadata's nearest point is within a few world units of
`death_pos`.

Apply `schema/003_assets.sql` after `schema/002_matches.sql` so
`match_player_items_with_assets` can resolve timeline IDs from the hero/item
snapshots. The view uses the asset payload's `type` field to distinguish
ability upgrades from shop upgrades.

## Read API

The platform serves the browser contract at both `/matches` and `/api/matches`
(and the corresponding player, positions, and deaths paths). The `/api/`
aliases are used by the web application's same-origin reverse proxy:

```text
GET /api/matches
GET /api/matches/{uuid}
GET /api/matches/{uuid}/positions
GET /api/matches/{uuid}/deaths
GET /api/players/{account_id}
GET /api/players/by-name/{name}
```

Read access is LAN-only by deployment policy and has no authentication by
default. Set `SINGULARITY_READ_TOKEN` to require a separate `Authorization:
Bearer ...` token without affecting collector credentials. The API returns
`503` when ClickHouse is unavailable; `/health` remains independent of it.

Friend names are resolved from the optional startup configuration, for example:

```bash
SINGULARITY_MEMBER_NAMES_JSON='{"12345678":"oscar"}' \
  singularity-platform
```

Steam account IDs are used only for this server-side lookup and are not
included in browser JSON responses. Hero, item, ability, and map names and
images come from the versioned asset tables.

## Asset lookups

Apply `schema/003_assets.sql`, then refresh the public hero, item, ability, and
map snapshots with:

```bash
SINGULARITY_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  singularity-platform assets-sync
```

The command classifies ability assets from the public items response, stores
the raw public payload for forward compatibility, and skips ClickHouse writes
when the endpoint snapshots have not changed. The map row includes the
minimap URL and `[-radius, radius]` world bounds for position conversion.

## Recovery

The JSONL spool is append-only and synchronized before an ingest request is
acknowledged. On startup, the platform rebuilds its duplicate index from valid
complete records. An incomplete final line is treated as a crash tail and can
be left in place; earlier records remain recoverable.

Stop the platform before copying the spool, preserve the original file, and
replay records through the ingestion endpoint after a restore. Investigate
malformed complete-line warnings before manually repairing a spool.

## Retention

The baseline keeps only salt records. Once replay downloading is added, raw
metadata and demos should be deleted after successful parsing unless explicitly
retained for debugging.
