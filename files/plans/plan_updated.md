# NFL Data Lake — Expansion Plan

## Context

The current platform has only 2 data sources (combine.xls, team-stats.csv) producing 2 staged and 2 curated Parquet files. The architecture doc requires 10+ datasets covering rosters, injuries, snap counts, depth charts, college stats, draft history, play-by-play, contracts, and advanced metrics. The ingestion framework, DuckDB client, Neo4j layer, and FastAPI routers are all solid and should be extended, not replaced.

---

## Library Choice: Use nflreadpy (not nfl_data_py)

`nfl_data_py` is deprecated. The actively-maintained replacement is **nflreadpy**:
- Returns Polars DataFrames → convert with `.to_pandas()` or use Polars natively
- Adds `load_contracts()`, `load_pfr_advstats()`, `load_ff_playerids()` not in old lib
- Drop-in conceptual replacement with `load_*` prefix instead of `import_*`

New dependencies for `requirements.txt`:
```
nflreadpy>=0.1.0
cfbd>=1.0.0
rapidfuzz>=3.0.0
```

---

## Complete Dataset Inventory

### Primary: nflreadpy (all free)

| Dataset | nflreadpy Function | Year Range | Purpose |
|---|---|---|---|
| Seasonal rosters | `load_rosters(seasons)` | 1999+ | Canonical player_id (gsis_id) |
| Weekly rosters | `load_rosters(seasons)` | 2002+ | Week-by-week roster state |
| Schedules / results | `load_schedules(seasons)` | 1999+ | game_id, scores, venues |
| Weekly player stats | `load_player_stats(seasons)` | 1999+ | per-player per-game offense |
| Seasonal player stats | `load_player_stats(seasons, stat_type='season')` | 1999+ | season aggregates |
| Snap counts | `load_snap_counts(seasons)` | 2012+ | offense/defense/ST snaps |
| Depth charts | `load_depth_charts(seasons)` | 2001+ | positional depth per week |
| Injury reports | `load_injuries(seasons)` | 2009+ | weekly practice/game status |
| Draft picks | `load_draft_picks(seasons)` | 1980+ | round, pick, team, player |
| Draft value chart | `load_draft_values()` | Static | Johnson/Stuart Sharp trade chart |
| Combine measurables | `load_combine(seasons, positions)` | 2000+ | replaces existing XLS |
| Next Gen Stats | `load_ngs_data(stat_type, seasons)` | 2016+ | CPOE, target sep, RYOE |
| PFR advanced stats | `load_pfr_advstats(seasons, stat_type)` | 2018+ | pressures, pocket time, drops |
| FTN charting data | `load_ftn_data(seasons)` | 2022+ | routes run, targets per route |
| Cross-platform IDs | `load_ff_playerids()` | Static | gsis_id ↔ ESPN/Yahoo/Sleeper |
| Contracts / salaries | `load_contracts()` | Historical | cap hits, APY, guaranteed |
| Team descriptions | `load_teams()` | Static | full name, conf, division, colors |
| ESPN QBR | `load_espn_qbr(seasons, level)` | Varies | Total QBR (NFL + college) |
| Win totals | `load_win_totals(seasons)` | Varies | pre-season O/U lines |
| Play-by-play | `load_pbp(seasons)` | 1999+ | EPA, WPA, air yards, formation |

### Secondary: CFBD API

| Dataset | cfbd Function | Purpose |
|---|---|---|
| College player season stats | `PlayersApi.get_player_season_stats(year, category)` | College dominator rating |
| College team stats | `TeamsApi.get_team_season_stats(year)` | Dominator denominator |

> CFBD returns "tall" format (one row per stat type). The transform step must pivot wide by `(player, school, season, category)`.

### Free Stack Is Sufficient
All 7 analytical goals (Team Diagnosis, Player Projection, Long-Term Career Simulation, Player-Roster Fit, Positional Flexibility, Injury/Health Analysis, Draft Optimization) are fully achievable with nflreadpy + CFBD alone. No paid services (PFF, Sports Info Solutions, TruMedia) are required or used in this implementation.

---

## Dataset → Analytical Goal Mapping

| Goal | Required Datasets |
|---|---|
| Team Diagnosis | team_stats ✓, PBP (EPA/WPA), snap_counts, depth_charts, rosters, pfr_advstats (pressures), contracts |
| Player Projection | combine, college_stats (CFBD), ngs_data (CPOE, target sep), pfr_advstats, weekly_stats, draft_picks |
| Long-Term Career Simulation | rosters (career timeline), injuries, weekly_stats, snap_counts, contracts (cap hit), draft_picks |
| Player-Roster Fit | depth_charts, snap_counts, rosters, team_stats, PBP (formation/personnel), ftn_data (routes) |
| Positional Flexibility | depth_charts, snap_counts, rosters, PBP (personnel packages) |
| Injury / Health Analysis | injuries, snap_counts (workload), rosters, weekly_stats (missed games) |
| Draft Optimization | combine, college_stats (CFBD), draft_picks, draft_values, pfr_advstats, contracts (rookie scale), qbr |

---

## Architecture Changes

### Keep Existing Layer Names
Stay with `raw/staged/curated` (not bronze/silver/gold) to match existing code.

### New File Structure (additions only)

```
ingestion/
  loaders/                           ← NEW subdirectory
    __init__.py
    combine_loader.py                ← MOVED (ingestion/combine_loader.py)
    team_stats_loader.py             ← MOVED (ingestion/team_stats_loader.py)
    nflreadpy_loader.py              ← NEW (wraps all nflreadpy load_* calls)
    cfbd_loader.py                   ← NEW (college stats via CFBD API)
  transforms/                        ← NEW subdirectory
    __init__.py
    player_identity.py               ← build master_players table
    team_normalizer.py               ← canonical team_id + abbreviation aliases
    game_builder.py                  ← build master_games table
  features/                          ← NEW subdirectory
    __init__.py
    athletic_scores.py               ← speed/agility/burst/strength/size
    production_scores.py             ← college dominator, snap share, EPA/snap, CPOE
    durability_scores.py             ← injury frequency, durability composite
    draft_value.py                   ← career value relative to pick cost
  player_id_resolver.py             ← NEW (cross-source gsis_id matching)
  pipeline.py                        ← MODIFY (staged build phases)

lake/staged/
  players/combine.parquet           (existing)
  teams/team_statistics.parquet     (existing)
  players/rosters.parquet           ← NEW
  players/draft_history.parquet     ← NEW
  players/snap_counts.parquet       ← NEW
  players/injuries.parquet          ← NEW
  players/depth_charts.parquet      ← NEW
  players/college_stats.parquet     ← NEW
  players/weekly_stats.parquet      ← NEW
  players/ngs_data.parquet          ← NEW (2016+)
  players/pfr_advstats.parquet      ← NEW (2018+)
  players/ftn_data.parquet          ← NEW (2022+)
  players/contracts.parquet         ← NEW
  games/schedules.parquet           ← NEW
  games/play_by_play/season=YYYY/   ← NEW (partitioned, opt-in)

lake/curated/
  player_profiles.parquet           (existing — keep for backward compat)
  team_performance.parquet          (existing — keep for backward compat)
  master_players.parquet            ← NEW (canonical player_id anchor)
  master_teams.parquet              ← NEW (canonical team_id + metadata)
  master_games.parquet              ← NEW (canonical game_id table)
  player_athletic_profiles.parquet  ← NEW
  player_production_profiles.parquet ← NEW
  player_durability_profiles.parquet ← NEW
  team_performance_summary.parquet  ← NEW
  team_roster_composition.parquet   ← NEW
  positional_usage_profiles.parquet ← NEW
  draft_value_history.parquet       ← NEW
```

---

## Player Identity Resolution (Critical)

The `gsis_id` from nflreadpy rosters is the canonical `player_id` across all tables. All other sources must be linked via name + position fuzzy matching.

**New file**: `ingestion/player_id_resolver.py`

```python
class PlayerIdResolver:
    """
    Build from rosters_df containing gsis_id, player_name, position, college.

    resolve(player_name, position, college, draft_year) → gsis_id | None
      1. Exact normalized name match
      2. Fuzzy name (rapidfuzz token_sort_ratio >= 90) + position match
      3. Flag unresolved as None

    resolve_dataframe(df, name_col, position_col, college_col)
      → adds 'player_id' (gsis_id) and 'id_confidence' columns
    """
```

Also use `load_ff_playerids()` cross-reference table to link gsis_id to ESPN/Yahoo IDs for future integrations.

Target resolution rates:
- Rosters → 100% (source of truth)
- Draft history → >90%
- Combine → >85%
- College stats (drafted players only) → ~70–80%

---

## Gold-Layer Feature Engineering

### Athletic Profiles (`features/athletic_scores.py`)

```python
speed_score      = (weight_lbs * 200) / (forty_yard ** 4)      # Bill Barnwell formula
agility_score    = mean([-three_cone_z, -shuttle_z])            # by position group
burst_score      = mean([vertical_z, broad_jump_z])             # by position group
strength_score   = bench_reps_z                                 # by position group
size_score       = (height_in * weight_lbs) / position_avg_size
```

### Production Profiles (`features/production_scores.py`)

```python
# College
college_dominator = player_yards / team_total_yards_at_position  # CFBD

# NFL
snap_share          = player_snaps / team_total_snaps
epa_per_snap        = total_epa / total_snaps                    # requires PBP
cpoe                = completion_pct_over_expected               # from NGS (QBs)
target_separation   = avg_separation_at_catch_point              # from NGS (WRs)
yprr                = receiving_yards / routes_run               # from FTN (2022+)

# Composite
nfl_production_score = mean([epa_per_snap_z, snap_share_z, stat_production_z])
```

### Durability Profiles (`features/durability_scores.py`)

```python
injury_frequency = total_injury_events / seasons_active
durability_score = composite_z(inverse_injury_freq, games_played_rate)
```

### Draft Value (`features/draft_value.py`)

```python
# Uses load_draft_values() — built-in nflreadpy trade chart data
draft_value_score = career_snap_share / expected_snap_share_at_pick
# or AV-based: pfr_approximate_value / expected_av_curve_at_pick
```

---

## Pipeline Restructure (`ingestion/pipeline.py`)

Four explicit build stages with dependency order:

```
Stage 0 — Raw Ingestion (all sources, parallel-safe):
  NflreadpyLoader  → all staged tables under players/ and games/
  CfbdLoader       → lake/staged/players/college_stats.parquet
  CombineLoader    → lake/staged/players/combine.parquet (unchanged)
  TeamStatsLoader  → lake/staged/teams/team_statistics.parquet (unchanged)

Stage 1 — Master Tables (depends on Stage 0 rosters):
  build_master_teams()   → lake/curated/master_teams.parquet
  build_master_games()   → lake/curated/master_games.parquet
  build_master_players() → lake/curated/master_players.parquet
  PlayerIdResolver built from rosters

Stage 2 — Silver Resolution (depends on Stage 1):
  Attach player_id to all staged tables via PlayerIdResolver

Stage 3 — Gold Feature Engineering (depends on Stage 2):
  build_athletic_profiles()        → uses combine
  build_production_profiles()      → uses weekly_stats, college_stats, ngs_data
  build_durability_profiles()      → uses injuries, snap_counts, rosters
  build_team_performance_summary() → uses schedules, team_stats
  build_team_roster_composition()  → uses rosters, depth_charts, contracts
  build_positional_usage_profiles() → uses snap_counts, depth_charts, ftn_data
  build_draft_value_history()      → uses draft_picks, draft_values, career_stats

Stage 4 — Backward Compat (keep existing):
  _build_player_profiles()   — unchanged
  _build_team_performance()  — unchanged
```

**PBP ingestion is separate** (`run_pbp_ingestion(years)`) — opt-in, ~35 MB/season, used to add EPA metrics to production profiles once downloaded.

---

## `config.py` Additions

```python
# New raw dirs
RAW_NFLREADPY_DIR = LAKE_RAW_DIR / "nflreadpy"
RAW_CFBD_DIR      = LAKE_RAW_DIR / "cfbd"

# New staged paths
STAGED_ROSTERS        = LAKE_STAGED_DIR / "players" / "rosters.parquet"
STAGED_DRAFT_HISTORY  = LAKE_STAGED_DIR / "players" / "draft_history.parquet"
STAGED_SNAP_COUNTS    = LAKE_STAGED_DIR / "players" / "snap_counts.parquet"
STAGED_INJURIES       = LAKE_STAGED_DIR / "players" / "injuries.parquet"
STAGED_DEPTH_CHARTS   = LAKE_STAGED_DIR / "players" / "depth_charts.parquet"
STAGED_COLLEGE_STATS  = LAKE_STAGED_DIR / "players" / "college_stats.parquet"
STAGED_WEEKLY_STATS   = LAKE_STAGED_DIR / "players" / "weekly_stats.parquet"
STAGED_NGS_DATA       = LAKE_STAGED_DIR / "players" / "ngs_data.parquet"
STAGED_PFR_ADVSTATS   = LAKE_STAGED_DIR / "players" / "pfr_advstats.parquet"
STAGED_FTN_DATA       = LAKE_STAGED_DIR / "players" / "ftn_data.parquet"
STAGED_CONTRACTS      = LAKE_STAGED_DIR / "players" / "contracts.parquet"
STAGED_SCHEDULES      = LAKE_STAGED_DIR / "games"   / "schedules.parquet"
STAGED_PBP_DIR        = LAKE_STAGED_DIR / "games"   / "play_by_play"

# New curated paths
CURATED_MASTER_PLAYERS         = LAKE_CURATED_DIR / "master_players.parquet"
CURATED_MASTER_TEAMS           = LAKE_CURATED_DIR / "master_teams.parquet"
CURATED_MASTER_GAMES           = LAKE_CURATED_DIR / "master_games.parquet"
CURATED_ATHLETIC_PROFILES      = LAKE_CURATED_DIR / "player_athletic_profiles.parquet"
CURATED_PRODUCTION_PROFILES    = LAKE_CURATED_DIR / "player_production_profiles.parquet"
CURATED_DURABILITY_PROFILES    = LAKE_CURATED_DIR / "player_durability_profiles.parquet"
CURATED_TEAM_PERF_SUMMARY      = LAKE_CURATED_DIR / "team_performance_summary.parquet"
CURATED_TEAM_ROSTER_COMP       = LAKE_CURATED_DIR / "team_roster_composition.parquet"
CURATED_POSITIONAL_USAGE       = LAKE_CURATED_DIR / "positional_usage_profiles.parquet"
CURATED_DRAFT_VALUE_HISTORY    = LAKE_CURATED_DIR / "draft_value_history.parquet"

# External
CFBD_API_KEY        = os.getenv("CFBD_API_KEY", "")
NFLVERSE_START_YEAR = int(os.getenv("NFLVERSE_START_YEAR", "2010"))
NFLVERSE_END_YEAR   = int(os.getenv("NFLVERSE_END_YEAR",   "2024"))
```

---

## DuckDB — Dynamic Table Registration (`db/duckdb_client.py`)

Replace hardcoded dict in `_register_tables()` with a directory scanner:

```python
def _register_tables(conn):
    # Auto-register all flat curated parquets (table name = file stem)
    for f in sorted(config.LAKE_CURATED_DIR.glob("*.parquet")):
        conn.execute(f"CREATE VIEW {f.stem} AS SELECT * FROM read_parquet('{f}')")

    # Hive-partitioned dirs (play_by_play/season=YYYY/)
    for d in sorted(config.LAKE_CURATED_DIR.iterdir()):
        if d.is_dir() and any(d.rglob("*.parquet")):
            conn.execute(
                f"CREATE VIEW {d.name} AS SELECT * FROM "
                f"read_parquet('{d}/**/*.parquet', hive_partitioning=true)"
            )
    # Legacy aliases: 'players' → player_profiles, 'team_stats' → team_performance
    _register_legacy_aliases(conn)
```

---

## Neo4j Schema Additions (`graph/builder.py`)

Migrate Player unique constraint from `name` → `player_id` (gsis_id) — names are not globally unique.

New constraints:
```python
"CREATE CONSTRAINT IF NOT EXISTS FOR (g:Game) REQUIRE g.game_id IS UNIQUE"
```

New relationships:
- `(Player)-[:SNAPPED_IN {offense_snaps, defense_snaps}]->(Game)`
- `(Player)-[:INJURED_DURING {type, body_part, game_status}]->(Season)`
- `(Player)-[:SELECTED_IN_DRAFT {round, pick}]->(DraftClass)`
- `(Team)-[:HOME_IN | AWAY_IN]->(Game)`
- `(Player)-[:CONTRACTED_BY {cap_hit, guaranteed, apy, year}]->(Team)`

---

## Critical Files to Modify

| File | Change |
|---|---|
| `requirements.txt` | Replace nfl_data_py with nflreadpy; add cfbd, rapidfuzz |
| `config.py` | Add all new path constants + CFBD_API_KEY + year range |
| `ingestion/pipeline.py` | Restructure into 4 stages; keep backward-compat curated builders |
| `db/duckdb_client.py` | Replace hardcoded `_register_tables` with directory scanner |
| `graph/builder.py` | Migrate player key to player_id; add Game nodes + new relationships |

---

## Implementation Milestones

1. **Foundation**: `requirements.txt`, `config.py`, subdirectory `__init__.py`s, move existing loaders to `ingestion/loaders/`, fix pipeline imports
2. **nflreadpy raw ingestion**: `nflreadpy_loader.py` + Stage 0 (rosters, schedules, snap_counts, injuries, depth_charts, draft_picks, combine, weekly_stats, ngs, pfr_advstats, contracts)
3. **Master tables**: `team_normalizer.py`, `game_builder.py`, Stage 1 (master_teams, master_games, master_players)
4. **Player ID resolution**: `player_id_resolver.py` + `player_identity.py`, Stage 2 (attach player_id to all staged tables)
5. **College stats**: `cfbd_loader.py` (requires `CFBD_API_KEY` env var)
6. **Gold features**: All 4 `features/` modules, Stage 3 (athletic, production, durability, draft_value)
7. **DuckDB dynamic registration**: Update `_register_tables()` in `db/duckdb_client.py`
8. **Neo4j expansion**: New constraints, Game nodes, CONTRACTED_BY relationship in `graph/builder.py`
9. **API enrichment**: Player profile endpoints using gold tables; graph endpoints for career path and roster
10. **PBP (optional)**: `run_pbp_ingestion()` + EPA-based production metrics

---

## Data Volume Estimates

| Dataset | Rows (est.) | Staged Parquet |
|---|---|---|
| Rosters (2010–2024) | ~25,000 | ~3 MB |
| Schedules | ~7,000 | ~1 MB |
| Weekly stats | ~500,000 | ~40 MB |
| Snap counts (2012+) | ~400,000 | ~25 MB |
| Depth charts (2011+) | ~300,000 | ~20 MB |
| Injuries (2009+) | ~200,000 | ~15 MB |
| Draft picks (all-time) | ~20,000 | ~2 MB |
| Combine (2000+) | ~8,000 | ~1 MB |
| NGS data (2016+) | ~150,000 | ~12 MB |
| PFR advstats (2018+) | ~50,000 | ~8 MB |
| FTN data (2022+) | ~30,000 | ~5 MB |
| Contracts | ~10,000 | ~2 MB |
| College stats (CFBD) | ~100,000 | ~10 MB |
| PBP one season | ~45,000 | ~35 MB |
| PBP 2010–2024 | ~675,000 | ~500 MB (partitioned) |
| **Total (excl. PBP)** | **~1.8M** | **~144 MB** |

---

## Verification

```python
# After pipeline runs
import pandas as pd
mp = pd.read_parquet("lake/curated/master_players.parquet")
ap = pd.read_parquet("lake/curated/player_athletic_profiles.parquet")
pp = pd.read_parquet("lake/curated/player_production_profiles.parquet")

print(f"Master players: {len(mp):,}")
print(f"Unresolved IDs: {mp.player_id.isna().sum()}")
print(f"Speed scores computed: {ap.speed_score.notna().sum()}")
print(f"Production profiles: {len(pp):,}")
```

DuckDB join sanity:
```sql
-- All gold tables should join cleanly through player_id
SELECT p.player_name, p.position, a.speed_score, pr.snap_share, d.durability_score
FROM master_players p
JOIN player_athletic_profiles a USING (player_id)
JOIN player_production_profiles pr USING (player_id)
JOIN player_durability_profiles d USING (player_id)
LIMIT 10;

-- Speed score distribution by position (WRs/CBs should lead)
SELECT position, ROUND(AVG(speed_score), 1) as avg_speed
FROM player_athletic_profiles
GROUP BY position ORDER BY avg_speed DESC;
```
