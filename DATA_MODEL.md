# Data Model

The baseline spool uses the collector's JSON contract. The planned ClickHouse
tables are:

| Table | Purpose |
|---|---|
| `match_salts` | Candidate cluster and metadata/replay salts |
| `matches` | Parsed match-level metadata |
| `match_players` | Parsed scoreboard rows and core player totals |
| `match_player_items` | Item purchases, sales, imbued abilities, and ability-upgrade events |
| `match_player_items_with_assets` | Item timeline rows classified through public asset lookups |
| `match_damage` | Damage matrix samples by dealer, target, source, and time |
| `match_damage_totals` | Final cumulative damage per source and target |
| `match_damage_player_reconciliation` | Matrix damage compared with player timeline hero damage |
| `match_player_stats` | Per-player timeline snapshots, including aim, combat, damage, and healing fields |
| `match_players_with_derived` | Core KDR and KDA per player |
| `match_player_stats_with_derived` | Accuracy, hero crit rate, kill shares, and damage taken per minute |
| `player_stat_averages` | Per-account averages over each player's latest snapshot per match |
| `heroes` | Versioned hero names and images |
| `items` | Versioned item names and images |
| `abilities` | Versioned ability and modifier names |
| `map_assets` | Minimap URL and world bounds |
| `match_jobs` | Download and parse state machine |
| `match_metadata` | Match-level protobuf fields |
| `match_player` | Detailed player and match statistics |
| `player_match_history` | Per-account history rows |
| `demo_player` | Demo-derived hero build and observed identity data |

Salt values are credentials for Valve replay files and must not be exposed in
logs or public API responses. Accepted salt records also retain the opaque
collector identity assigned by the platform credential; bearer tokens are not
stored with the record. The metadata worker writes decompressed protobuf
handoff files keyed by a non-reversible job digest; the later parser owns
deletion after successful parsing.
