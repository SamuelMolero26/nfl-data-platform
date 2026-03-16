# NFL Data Platform

An end-to-end NFL data lake platform with a REST API, SQL query engine (DuckDB), graph database (Neo4j), interactive data management UI, and an ML model training pipeline.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Directory Structure](#directory-structure)
- [Data Sources](#data-sources)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
  - [Local (manual)](#local-manual)
  - [Docker Compose](#docker-compose)
- [Configuration](#configuration)
- [Ingestion Pipeline](#ingestion-pipeline)
- [Query Layer (DuckDB)](#query-layer-duckdb)
- [Graph Layer (Neo4j)](#graph-layer-neo4j)
- [REST API](#rest-api)
  - [Health](#health)
  - [SQL Query](#sql-query-duckdb)
  - [Players](#players)
  - [Teams](#teams)
  - [Graph](#graph)
  - [Data Management](#data-management)
- [Management UI](#management-ui)
- [ML Pipeline](#ml-pipeline)
- [API Reference — Quick Lookup](#api-reference--quick-lookup)
- [Dependencies](#dependencies)

---

## Overview

The NFL Data Platform ingests raw NFL combine and team statistics data, transforms it through a **medallion architecture** (raw → staged → curated), and exposes the cleaned data through:

- **DuckDB** — in-process SQL queries over Parquet files
- **Neo4j** — graph traversal for player/team/college relationships
- **FastAPI** — REST API for querying, data management, and ML
- **Single-page UI** — interactive browser for dataset inspection and cleaning

> **Player identity note:** Player nodes are keyed on `player_id` (the NFL `gsis_id`), not player name. All gold-table and graph endpoints that target a specific player expect this identifier. Use `GET /players/search?name=<name>` or `GET /graph/player/search/<name>` to resolve a name to a `player_id` first.

---

## Architecture

```
[Sources]  →  [Ingestion]  →  [Raw]  →  [Staged]  →  [Curated]
                                                          ↓
                                                    [DuckDB SQL]
                                                    [Neo4j Graph]
                                                          ↓
                                                    [FastAPI REST]
                                                          ↓
                                                 [Management UI + ML]
```

### Lake Zones

| Zone | Path | Description |
|---|---|---|
| Raw | `lake/raw/` | Immutable source files, never modified |
| Staged | `lake/staged/` | Cleaned, typed Parquet (one file per source) |
| Curated | `lake/curated/` | Join-ready, query-optimized Parquet (primary query target) |

---

## Directory Structure

```
nfl-data-platform/
├── lake/
│   ├── raw/
│   │   ├── combine/nfl-combine.xls
│   │   └── team_stats/nfl-team-statistics.csv
│   ├── staged/
│   │   ├── players/combine.parquet
│   │   └── teams/team_statistics.parquet
│   └── curated/
│       ├── player_profiles.parquet
│       └── team_performance.parquet
├── ingestion/
│   ├── base.py                  # Abstract SourceLoader class
│   ├── combine_loader.py        # HTML-XLS parser + transformer
│   ├── team_stats_loader.py     # CSV loader + transformer
│   └── pipeline.py             # Orchestrates all loaders
├── db/
│   ├── duckdb_client.py         # DuckDB connection + query runner
│   └── neo4j_client.py         # Neo4j driver wrapper
├── graph/
│   ├── builder.py               # Parquet → Neo4j MERGE statements
│   └── queries.py              # Cypher query templates
├── api/
│   ├── main.py                  # FastAPI application entry point
│   └── routers/
│       ├── query.py             # POST /query — DuckDB SQL
│       ├── players.py          # GET /players — combine + gold tables
│       ├── teams.py            # GET /teams
│       ├── graph.py            # GET /graph/* — Neo4j traversal
│       └── manage.py           # Data cleaning & management endpoints
├── ui/
│   └── index.html              # Lightweight data management frontend
├── ml/
│   ├── dataset_builder.py      # Curated Parquet → ML-ready DataFrame
│   ├── trainer.py              # scikit-learn training pipeline
│   └── models/                 # Saved .pkl models + JSON metrics
├── config.py                    # Paths, DB credentials, settings
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── entrypoint.sh
```

---

## Data Sources

| File | Format | Rows | Description |
|---|---|---|---|
| `nfl-combine.xls` | HTML with XLS headers | 329 | 2025 NFL Combine player measurements and draft results |
| `nfl-team-statistics.csv` | CSV | 765 | Season stats for all 32 NFL teams (1999–2022), 56 columns |

> **Note:** Despite the `.xls` extension, `nfl-combine.xls` is an HTML file. The ingestion pipeline handles this automatically using `pd.read_html()`.

---

## Prerequisites

- Python 3.12+
- Docker (for Neo4j)

---

## Quick Start

### Local (manual)

**1. Install dependencies**

```bash
pip install -r requirements.txt
```

**2. Copy raw source files into the lake**

```bash
mkdir -p lake/raw/combine lake/raw/team_stats
cp nfl-combine.xls lake/raw/combine/
cp nfl-team-statistics.csv lake/raw/team_stats/
```

**3. Start Neo4j**

```bash
docker run -d --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:5
```

**4. Run the ingestion pipeline**

```bash
python ingestion/pipeline.py
```

This produces Parquet files in `lake/staged/` and `lake/curated/`.

**5. Populate the Neo4j graph**

```bash
python graph/builder.py
```

**6. Start the API server**

```bash
uvicorn api.main:app --reload
```

The API is available at `http://localhost:8000`.  
Interactive Swagger docs: `http://localhost:8000/docs`  
Management UI: `http://localhost:8000`

---

### Docker Compose

```bash
# Copy and configure the environment file
cp .env.example .env
# Edit .env as needed (defaults work for local development)

# Start all services (Neo4j + API)
docker compose up --build
```

On first start, the `entrypoint.sh` script:
1. Waits for Neo4j to become healthy
2. Runs the ingestion pipeline if curated Parquet files are not present
3. Populates the Neo4j graph (idempotent — safe to re-run)
4. Starts the FastAPI server

---

## Configuration

All settings are in `config.py` and can be overridden with environment variables:

| Variable | Default | Description |
|---|---|---|
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j connection URI |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `password` | Neo4j password |
| `API_HOST` | `0.0.0.0` | API bind address |
| `API_PORT` | `8000` | API port |

Create a `.env` file from the example for local overrides:

```bash
cp .env.example .env
```

---

## Ingestion Pipeline

Run the full pipeline:

```bash
python ingestion/pipeline.py
```

### Combine Loader

Parses `nfl-combine.xls` (HTML format) and applies these transforms:

- `Ht` string (e.g. `"6-2"`) → total inches (float)
- `Drafted (tm/rnd/yr)` string → `draft_team`, `draft_round`, `draft_pick`, `draft_year`
- Numeric combine metrics cast to float (NaN preserved for missing values)
- Columns renamed to snake_case

Output: `lake/staged/players/combine.parquet`

### Team Stats Loader

Parses `nfl-team-statistics.csv` and normalizes team abbreviations.  
Four air-yards columns (`offense_ave_air_yards`, `offense_ave_yac`, `defense_ave_air_yards`, `defense_ave_yac`) are null for early seasons (roughly pre-2006) — this is expected and not a data error.

Output: `lake/staged/teams/team_statistics.parquet`

### Curated Layer

After staging, the pipeline builds curated files used by DuckDB and the API:

- `lake/curated/player_profiles.parquet` — combine-based player profiles
- `lake/curated/team_performance.parquet` — season-level team stats
- `lake/curated/master_players.parquet` — unified player identity (gsis_id-keyed)
- `lake/curated/player_athletic_profiles.parquet` — combine-derived athletic scores
- `lake/curated/player_production_profiles.parquet` — per-season on-field production
- `lake/curated/player_durability_profiles.parquet` — career injury and availability data
- `lake/curated/draft_value_history.parquet` — draft pick value vs. actual career output

---

## Query Layer (DuckDB)

DuckDB reads `lake/curated/*.parquet` directly as virtual SQL tables — no import step, no server required.

**Core virtual tables:**

| Table | Source |
|---|---|
| `players` | `lake/curated/player_profiles.parquet` |
| `team_stats` | `lake/curated/team_performance.parquet` |
| `master_players` | `lake/curated/master_players.parquet` |
| `player_athletic_profiles` | `lake/curated/player_athletic_profiles.parquet` |
| `player_production_profiles` | `lake/curated/player_production_profiles.parquet` |
| `player_durability_profiles` | `lake/curated/player_durability_profiles.parquet` |
| `draft_value_history` | `lake/curated/draft_value_history.parquet` |

**The `/query` endpoint is read-only.** Any SQL containing `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, or `REPLACE` is rejected with HTTP 400.

Use `GET /query/tables` to list all registered tables at runtime.

Example queries:

```sql
-- Average 40-yard dash time by position
SELECT position, AVG(forty_yard) AS avg_forty
FROM players
WHERE forty_yard IS NOT NULL
GROUP BY position
ORDER BY avg_forty;

-- Top 10 WRs by production score in 2022
SELECT mp.player_name, pp.snap_share, pp.epa_per_game, pp.nfl_production_score
FROM player_production_profiles pp
JOIN master_players mp USING (player_id)
WHERE pp.position = 'WR' AND pp.season = 2022
ORDER BY pp.nfl_production_score DESC
LIMIT 10;
```

---

## Graph Layer (Neo4j)

### Node Types

| Label | Key Properties |
|---|---|
| `Player` | `player_id` (gsis_id), `player_name`, `position`, `height_in`, `weight_lbs`, combine metrics |
| `Team` | `full_name`, `abbreviation` |
| `College` | `name` |
| `DraftClass` | `year` |
| `Season` | `year` |
| `Game` | `game_id`, `season`, `week` |

### Relationship Types

| Relationship | From → To | Key Properties |
|---|---|---|
| `DRAFTED_BY` | Player → Team | `round`, `pick`, `year` |
| `SELECTED_IN_DRAFT` | Player → DraftClass | `round`, `pick`, `team`, `draft_value_score`, `draft_value_percentile`, `car_av` |
| `ATTENDED` | Player → College | — |
| `CONTRACTED_BY` | Player → Team | `year_signed`, `apy`, `cap_hit`, `guaranteed` |
| `PLAYED_IN` | Team → Season | `wins`, `losses`, `points_scored`, `score_differential`, `win_pct` |
| `SNAPPED_IN` | Player → Game | `offense_snaps` |
| `INJURED_DURING` | Player → Season | — |

### Building the Graph

```bash
python graph/builder.py
```

Uses idempotent `MERGE` Cypher statements — safe to re-run without creating duplicate nodes.

**Access Neo4j Browser:** `http://localhost:7474`  
Default credentials: `neo4j / password`

Example Cypher queries:

```cypher
-- All players drafted by the Dallas Cowboys
MATCH (p:Player)-[r:SELECTED_IN_DRAFT]->(d:DraftClass)
WHERE r.team = 'DAL'
RETURN p.player_name, p.position, r.round, r.pick, d.year
ORDER BY d.year DESC, r.pick;

-- Shortest path between two players (use real gsis_ids, e.g. from /players/search)
MATCH (a:Player {player_id: '00-0038789'}),
      (b:Player {player_id: '00-0033873'}),
      path = shortestPath((a)-[*..6]-(b))
RETURN path;
```

---

## REST API

Base URL: `http://localhost:8000`  
Swagger UI: `http://localhost:8000/docs`  
ReDoc: `http://localhost:8000/redoc`

All endpoints return JSON. Errors follow the format `{"detail": "Human-readable error message"}`.

---

### Health

#### `GET /health`

Returns API status. Useful as a liveness probe.

```bash
curl http://localhost:8000/health
```

```json
{"status": "ok"}
```

---

### SQL Query (DuckDB)

#### `POST /query`

Execute a read-only SQL query against all registered curated Parquet tables.

**Request body:**

| Field | Type | Description |
|---|---|---|
| `sql` | string | SQL SELECT statement |

**Response:**

| Field | Type | Description |
|---|---|---|
| `rows` | array | Query result rows as objects |
| `columns` | array | Column names in result order |
| `count` | int | Number of rows returned |

**Example — position breakdown:**

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT position, COUNT(*) AS count FROM players GROUP BY position ORDER BY count DESC"}'
```

```json
{
  "rows": [
    {"position": "WR", "count": 87},
    {"position": "CB", "count": 56},
    {"position": "QB", "count": 10}
  ],
  "columns": ["position", "count"],
  "count": 3
}
```

**Example — Python:**

```python
import requests

resp = requests.post(
    "http://localhost:8000/query",
    json={
        "sql": (
            "SELECT position, AVG(forty_yard) AS avg_forty "
            "FROM players WHERE forty_yard IS NOT NULL "
            "GROUP BY position ORDER BY avg_forty"
        )
    },
)
for row in resp.json()["rows"]:
    print(row["position"], round(row["avg_forty"], 2))
```

**Mutation rejection (HTTP 400):**

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "DROP TABLE players"}'
# {"detail": "Write operations are not allowed: DROP"}
```

---

#### `GET /query/tables`

List all available DuckDB virtual tables.

```bash
curl http://localhost:8000/query/tables
```

```json
{
  "tables": [
    "players",
    "team_stats",
    "master_players",
    "player_athletic_profiles",
    "player_production_profiles",
    "player_durability_profiles",
    "draft_value_history"
  ]
}
```

---

### Players

Player endpoints are split into two tiers:

- **Combine tier** (`/players`, `/players/search`, `/players/{name}`) — query the raw combine dataset; no `player_id` needed.
- **Gold-table tier** (`/players/id/{player_id}/...`, `/players/leaderboard/...`) — query enriched analytical tables keyed on `player_id` (gsis_id). Requires the ingestion pipeline to have been run.

**Workflow to use gold-table endpoints:**
1. Call `GET /players/search?name=<partial name>` to get the `player_id`.
2. Use that `player_id` with the `/players/id/{player_id}/...` endpoints.

---

#### `GET /players`

List combine players with optional filters.

| Query param | Type | Default | Description |
|---|---|---|---|
| `position` | string | — | Exact position code (e.g. `WR`, `QB`) |
| `school` | string | — | Partial college name (case-insensitive) |
| `draft_team` | string | — | Partial team name match |
| `drafted_only` | bool | `false` | Return only drafted players |
| `limit` | int | `100` | Max rows (≤ 500) |
| `offset` | int | `0` | Pagination offset |

```bash
# All drafted QBs
curl "http://localhost:8000/players?position=QB&drafted_only=true&limit=10"

# Players from Alabama
curl "http://localhost:8000/players?school=Alabama&limit=20"

# Page 2 of all WRs (100 per page)
curl "http://localhost:8000/players?position=WR&offset=100"
```

**Example response:**

```json
{
  "players": [
    {
      "player_name": "Cam Ward",
      "position": "QB",
      "school": "Miami",
      "height_in": 74.0,
      "weight_lbs": 221,
      "forty_yard": 4.72,
      "vertical_in": 32.5,
      "bench_reps": null,
      "broad_jump_in": 115,
      "three_cone": null,
      "shuttle": null,
      "draft_team": "Tennessee Titans",
      "draft_round": 1,
      "draft_pick": 1,
      "draft_year": 2025
    }
  ],
  "count": 1
}
```

---

#### `GET /players/search`

Search players by name. Uses the `master_players` gold table if available, otherwise falls back to the combine dataset.

| Query param | Type | Required | Description |
|---|---|---|---|
| `name` | string | ✓ (min 2 chars) | Partial name (case-insensitive contains) |
| `limit` | int | — | Max results (≤ 100, default 20) |

```bash
curl "http://localhost:8000/players/search?name=Ward"
```

**Example response (gold table available):**

```json
{
  "players": [
    {
      "player_id": "00-0038789",
      "player_name": "Cam Ward",
      "position": "QB",
      "team": "TEN",
      "first_season": 2025,
      "last_season": 2025,
      "college": "Miami"
    }
  ],
  "count": 1
}
```

> **Tip:** Copy `player_id` from this response to use with the gold-table and graph endpoints below.

---

#### `GET /players/{name}`

Retrieve all combine records whose `player_name` contains `{name}` (partial, case-insensitive).

```bash
curl "http://localhost:8000/players/Cam%20Ward"
```

---

#### `GET /players/id/{player_id}/profile`

Full enriched analytical profile — joins identity, athletic scores, season-by-season production, career durability, and draft value into one response.

```bash
curl "http://localhost:8000/players/id/00-0038789/profile"
```

**Example response (abbreviated):**

```json
{
  "player_id": "00-0038789",
  "player_name": "Cam Ward",
  "position": "QB",
  "team": "TEN",
  "first_season": 2025,
  "last_season": 2025,
  "athletic": {
    "speed_score": 82.4,
    "agility_score": 71.1,
    "burst_score": 68.3,
    "forty_yard": 4.72,
    "height_in": 74.0,
    "weight_lbs": 221
  },
  "production": [
    {
      "season": 2025,
      "snap_share": 0.94,
      "epa_per_game": 3.1,
      "passing_cpoe": 2.8,
      "nfl_production_score": 78.2,
      "games_played": 16
    }
  ],
  "durability": {
    "durability_score": 84.5,
    "injury_frequency": 0.06,
    "games_played_rate": 0.94
  },
  "draft_value": {
    "draft_year": 2025,
    "round": 1,
    "pick": 1,
    "car_av": null,
    "draft_value_score": null,
    "draft_value_percentile": null
  }
}
```

> Returns HTTP 503 if gold tables have not been built yet. Run `python ingestion/pipeline.py` first.

---

#### `GET /players/id/{player_id}/athletic`

Combine-derived athletic scores for a player.

```bash
curl "http://localhost:8000/players/id/00-0038789/athletic"
```

**Returned fields:** `speed_score`, `agility_score`, `burst_score`, `strength_score`, `size_score`, `height_in`, `weight_lbs`, `forty_yard`, `vertical_in`, `broad_jump_in`, `bench_reps`, `three_cone`, `shuttle`, `draft_year`, `position`.

---

#### `GET /players/id/{player_id}/production`

Season-by-season on-field production metrics.

```bash
curl "http://localhost:8000/players/id/00-0038789/production"
```

**Example response:**

```json
{
  "player_id": "00-0038789",
  "seasons": [
    {
      "season": 2025,
      "position": "QB",
      "snap_share": 0.94,
      "epa_per_game": 3.1,
      "passing_cpoe": 2.8,
      "target_share": null,
      "nfl_production_score": 78.2,
      "games_played": 16,
      "games_with_snaps": 16
    }
  ]
}
```

---

#### `GET /players/id/{player_id}/durability`

Career durability and injury metrics.

```bash
curl "http://localhost:8000/players/id/00-0038789/durability"
```

**Returned fields:** `durability_score`, `injury_frequency`, `games_played_rate` (relative to position peers).

---

#### `GET /players/id/{player_id}/draft-value`

Draft pick value relative to round peers.

```bash
curl "http://localhost:8000/players/id/00-0038789/draft-value"
```

**Returned fields:** `player_name`, `draft_year`, `team`, `round`, `pick`, `position`, `car_av`, `draft_value_score` (z-score within round), `draft_value_percentile` (0–100 within round), `allpro`, `probowls`, `games`, `seasons_started`.

---

#### `GET /players/leaderboard/athletic`

Rank players by a combine-derived athletic score.

| Query param | Type | Default | Description |
|---|---|---|---|
| `position` | string | — | Filter by position (e.g. `WR`, `QB`) |
| `metric` | string | `speed_score` | Score to rank by. One of: `speed_score`, `agility_score`, `burst_score`, `strength_score`, `size_score` |
| `limit` | int | `25` | Max rows (≤ 100) |

```bash
# Fastest WRs by speed score
curl "http://localhost:8000/players/leaderboard/athletic?position=WR&metric=speed_score&limit=10"

# Top 25 overall by burst score
curl "http://localhost:8000/players/leaderboard/athletic?metric=burst_score"
```

**Example response:**

```json
{
  "metric": "speed_score",
  "position": "WR",
  "players": [
    {
      "player_name": "Luther Burden III",
      "position": "WR",
      "speed_score": 112.4,
      "height_in": 71.0,
      "weight_lbs": 215,
      "forty_yard": 4.32,
      "draft_year": 2025
    }
  ]
}
```

---

#### `GET /players/leaderboard/production`

Rank players by `nfl_production_score`.

| Query param | Type | Default | Description |
|---|---|---|---|
| `position` | string | — | Filter by position |
| `season` | int | — | Filter by season year |
| `limit` | int | `25` | Max rows (≤ 100) |

```bash
# Top QBs in 2022
curl "http://localhost:8000/players/leaderboard/production?position=QB&season=2022&limit=10"
```

---

#### `GET /players/leaderboard/draft-value`

Rank draft picks by `draft_value_score` (career outperformance vs. round peers).

| Query param | Type | Default | Description |
|---|---|---|---|
| `round` | int | — | Filter by draft round (1–7) |
| `season` | int | — | Filter by draft year |
| `limit` | int | `25` | Max rows (≤ 100) |

```bash
# Best value picks from round 3 in 2020
curl "http://localhost:8000/players/leaderboard/draft-value?round=3&season=2020"
```

---

### Teams

#### `GET /teams`

List all unique team abbreviations present in the dataset.

```bash
curl "http://localhost:8000/teams"
```

```json
{"teams": ["ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", ...]}
```

---

#### `GET /teams/{abbr}/stats`

Return season-level stats for a team, optionally filtered by year range.

| Path param | Description |
|---|---|
| `abbr` | Team abbreviation (e.g. `NE`, `DAL`) — case-insensitive |

| Query param | Type | Description |
|---|---|---|
| `season_from` | int | Earliest season (inclusive) |
| `season_to` | int | Latest season (inclusive) |

```bash
# New England Patriots 2010–2020
curl "http://localhost:8000/teams/NE/stats?season_from=2010&season_to=2020"

# All seasons for the Chiefs
curl "http://localhost:8000/teams/KC/stats"
```

**Example response (one season):**

```json
{
  "team": "NE",
  "seasons": [
    {
      "season": 2019,
      "team": "NE",
      "wins": 12,
      "losses": 4,
      "ties": 0,
      "win_pct": 0.75,
      "points_scored": 420,
      "points_allowed": 225,
      "score_differential": 195,
      "point_diff_per_game": 12.2,
      "offense_total_yards_gained_pass": 3841,
      "offense_total_yards_gained_run": 1783,
      "defense_total_yards_gained_pass": 2847,
      "defense_total_yards_gained_run": 1614
    }
  ]
}
```

---

### Graph

Graph endpoints require Neo4j to be running. If Neo4j is unreachable the API returns HTTP 503 with instructions to start the container.

**Important:** Graph player endpoints are keyed on `player_id` (gsis_id), not player name. Use `GET /graph/player/search/{name}` to look up a player by name.

---

#### `GET /graph/player/search/{name}`

Search the graph for a player by name (case-sensitive contains match on `player_name` property). Returns up to 10 matches with college and draft info.

```bash
curl "http://localhost:8000/graph/player/search/Ward"
```

**Example response:**

```json
{
  "matches": [
    {
      "player": {"player_id": "00-0038789", "player_name": "Cam Ward", "position": "QB"},
      "college": "Miami",
      "draft_team": "TEN",
      "round": 1,
      "pick": 1,
      "draft_year": 2025,
      "draft_value_score": null,
      "draft_value_percentile": null
    }
  ]
}
```

---

#### `GET /graph/player/{player_id}/profile`

Full graph profile for a player: college, draft class, and draft value scores.

```bash
curl "http://localhost:8000/graph/player/00-0038789/profile"
```

---

#### `GET /graph/player/{player_id}/neighbors`

Return all graph nodes within `depth` hops of the player (traverses all relationship types).

| Query param | Type | Default | Description |
|---|---|---|---|
| `depth` | int | `1` | Hop depth (1–3) |

```bash
# Direct neighbors
curl "http://localhost:8000/graph/player/00-0038789/neighbors"

# 2-hop neighborhood
curl "http://localhost:8000/graph/player/00-0038789/neighbors?depth=2"
```

**Example response:**

```json
{
  "player_id": "00-0038789",
  "neighbors": [
    {"type": "Team",    "node": {"abbreviation": "TEN", "full_name": "Tennessee Titans"}},
    {"type": "College", "node": {"name": "Miami"}},
    {"type": "DraftClass", "node": {"year": 2025}}
  ]
}
```

---

#### `GET /graph/player/{player_id}/career`

Full career path for a player: teams (contracted or drafted), snap counts by season, injury seasons, and draft info.

```bash
curl "http://localhost:8000/graph/player/00-0038789/career"
```

**Example response:**

```json
{
  "player_id": "00-0038789",
  "teams": [
    {
      "team": "TEN",
      "team_name": "Tennessee Titans",
      "year_signed": null,
      "apy": null,
      "cap_hit": null,
      "drafted_year": 2025,
      "drafted_round": 1,
      "drafted_pick": 1
    }
  ],
  "snaps_by_season": [
    {"season": 2025, "games": 16, "total_offense_snaps": 1012}
  ],
  "injury_seasons": [],
  "draft": {
    "draft_year": 2025,
    "round": 1,
    "pick": 1,
    "car_av": null,
    "draft_value_score": null,
    "draft_value_percentile": null
  }
}
```

> Requires snap counts and injury data to be staged and Stage 2 to have been run.

---

#### `GET /graph/team/{abbr}/drafted`

All players drafted by a team (optionally filtered by draft year), with draft value scores.

| Path param | Description |
|---|---|
| `abbr` | Team abbreviation (e.g. `DAL`, `NE`) |

| Query param | Type | Description |
|---|---|---|
| `year` | int | Filter to a specific draft year |

```bash
# All Dallas Cowboys draft picks
curl "http://localhost:8000/graph/team/DAL/drafted"

# Cowboys 2023 draft class
curl "http://localhost:8000/graph/team/DAL/drafted?year=2023"
```

**Example response:**

```json
{
  "team": "DAL",
  "picks": [
    {
      "player": "Mazi Smith",
      "position": "DT",
      "round": 1,
      "pick": 26,
      "year": 2023,
      "draft_value_score": -0.4,
      "draft_value_percentile": 38
    }
  ]
}
```

---

#### `GET /graph/team/{abbr}/roster`

Players contracted to a team (from `CONTRACTED_BY` edges), optionally filtered by season.

| Query param | Type | Description |
|---|---|---|
| `season` | int | Return contracts signed on or before this season |

```bash
# Current roster
curl "http://localhost:8000/graph/team/KC/roster"

# Roster as of 2022
curl "http://localhost:8000/graph/team/KC/roster?season=2022"
```

> Returns HTTP 404 if no contract data exists. Requires contracts to be staged and the graph builder to have run.

---

#### `GET /graph/path`

Find the shortest path between any two entities in the graph (up to 6 hops).

| Query param | Type | Required | Description |
|---|---|---|---|
| `from_id` | string | ✓ | `player_id` (gsis_id), team `abbreviation` (e.g. `NE`), or the `full_name`/`name` of any College or DraftClass node |
| `to_id` | string | ✓ | Same format as `from_id` — the API tries each property in order: `player_id`, `player_name`, `full_name`, `abbreviation` |

```bash
# Path from a player to a team
curl "http://localhost:8000/graph/path?from_id=00-0038789&to_id=DAL"

# Path between two teams via shared players
curl "http://localhost:8000/graph/path?from_id=NE&to_id=SF"
```

**Example response:**

```json
{
  "path_nodes": [
    {"labels": ["Player"],    "id": "00-0038789"},
    {"labels": ["DraftClass"],"id": "2025"},
    {"labels": ["Team"],      "id": "DAL"}
  ],
  "hops": 2
}
```

---

#### `GET /graph/college/{name}/pipeline`

All players from a given college and their NFL draft outcomes.

```bash
curl "http://localhost:8000/graph/college/Alabama/pipeline"
```

**Example response:**

```json
{
  "college": "Alabama",
  "players": [
    {
      "player": "Will Anderson Jr.",
      "position": "EDGE",
      "drafted_by": "HOU",
      "round": 1,
      "pick": 3,
      "year": 2023,
      "draft_value_score": 1.8
    }
  ]
}
```

---

#### `GET /graph/full`

Return all nodes and relationships for graph visualization tools (e.g. D3, Cytoscape).

| Query param | Type | Default | Description |
|---|---|---|---|
| `limit` | int | `500` | Max nodes/edges to return (1–2000) |

```bash
curl "http://localhost:8000/graph/full?limit=200"
```

```json
{
  "nodes": [
    {"id": 1, "type": "Player", "props": {"player_id": "00-0038789", "player_name": "Cam Ward"}},
    {"id": 2, "type": "Team",   "props": {"abbreviation": "TEN", "full_name": "Tennessee Titans"}}
  ],
  "edges": [
    {"source": 1, "target": 2, "type": "DRAFTED_BY", "props": {"round": 1, "pick": 1, "year": 2025}}
  ]
}
```

---

### Data Management

Cleaning operations read from and write back to the **staged layer**. After modifying staged data, re-run the ingestion pipeline to propagate changes to the curated layer.

---

#### `GET /manage/datasets`

List all Parquet files in the staged and curated lake zones with row/column counts and file size.

```bash
curl "http://localhost:8000/manage/datasets"
```

**Example response:**

```json
{
  "staged": [
    {
      "name": "combine",
      "path": "lake/staged/players/combine.parquet",
      "rows": 329,
      "columns": 18,
      "size_kb": 22.4
    }
  ],
  "curated": [
    {
      "name": "player_profiles",
      "path": "lake/curated/player_profiles.parquet",
      "rows": 329,
      "columns": 15,
      "size_kb": 19.1
    }
  ]
}
```

---

#### `GET /manage/preview/{dataset}`

Preview the first N rows and schema info (column types, null counts) for any Parquet dataset.

| Path param | Description |
|---|---|
| `dataset` | Dataset stem name (e.g. `combine`, `team_statistics`) |

| Query param | Type | Default | Description |
|---|---|---|---|
| `rows` | int | `20` | Rows to preview (≤ 200) |

```bash
curl "http://localhost:8000/manage/preview/combine?rows=5"
```

**Example response (abbreviated):**

```json
{
  "dataset": "combine",
  "total_rows": 329,
  "columns": 18,
  "schema": [
    {"column": "player_name", "dtype": "object", "null_count": 0,  "null_pct": 0.0},
    {"column": "forty_yard",  "dtype": "float64","null_count": 31, "null_pct": 9.4}
  ],
  "preview": [
    {"player_name": "Cam Ward", "position": "QB", "forty_yard": 4.72, "draft_year": 2025}
  ]
}
```

---

#### `POST /manage/clean/drop-columns`

Remove one or more columns from a staged dataset.

**Request body:**

| Field | Type | Description |
|---|---|---|
| `dataset` | string | Dataset name (e.g. `combine`) |
| `columns` | array of strings | Column names to remove |

```bash
curl -X POST http://localhost:8000/manage/clean/drop-columns \
  -H "Content-Type: application/json" \
  -d '{"dataset": "combine", "columns": ["three_cone", "shuttle"]}'
```

```json
{
  "dropped": ["three_cone", "shuttle"],
  "remaining_columns": ["player_name", "position", "school", ...]
}
```

---

#### `POST /manage/clean/fill-nulls`

Fill null values in a column using a statistical strategy or a fixed value.

**Request body:**

| Field | Type | Description |
|---|---|---|
| `dataset` | string | Dataset name |
| `column` | string | Column to fill |
| `strategy` | string | One of: `mean`, `median`, `mode`, `value` |
| `value` | string \| float | Required when `strategy` is `value` |

```bash
# Fill missing 40-yard times with the column median
curl -X POST http://localhost:8000/manage/clean/fill-nulls \
  -H "Content-Type: application/json" \
  -d '{"dataset": "combine", "column": "forty_yard", "strategy": "median"}'

# Fill a string column with a fixed value
curl -X POST http://localhost:8000/manage/clean/fill-nulls \
  -H "Content-Type: application/json" \
  -d '{"dataset": "combine", "column": "draft_team", "strategy": "value", "value": "Undrafted"}'
```

```json
{"column": "forty_yard", "nulls_filled": 31}
```

---

#### `POST /manage/clean/rename`

Rename one or more columns in a staged dataset.

**Request body:**

| Field | Type | Description |
|---|---|---|
| `dataset` | string | Dataset name |
| `rename_map` | object | `{"old_name": "new_name", ...}` |

```bash
curl -X POST http://localhost:8000/manage/clean/rename \
  -H "Content-Type: application/json" \
  -d '{"dataset": "combine", "rename_map": {"player_name": "name", "weight_lbs": "weight"}}'
```

```json
{
  "renamed": {"player_name": "name", "weight_lbs": "weight"},
  "columns": ["name", "position", "school", "height_in", "weight", ...]
}
```

---

#### `POST /manage/clean/filter-rows`

Keep only rows that satisfy a SQL WHERE expression. Removes non-matching rows from the staged file.

**Request body:**

| Field | Type | Description |
|---|---|---|
| `dataset` | string | Dataset name |
| `sql_filter` | string | SQL WHERE clause fragment (no `WHERE` keyword) |

```bash
# Keep only players who ran the 40 in under 4.5 seconds
curl -X POST http://localhost:8000/manage/clean/filter-rows \
  -H "Content-Type: application/json" \
  -d '{"dataset": "combine", "sql_filter": "forty_yard < 4.5 OR forty_yard IS NULL"}'

# Keep only seasons from 2010 onward
curl -X POST http://localhost:8000/manage/clean/filter-rows \
  -H "Content-Type: application/json" \
  -d '{"dataset": "team_statistics", "sql_filter": "season >= 2010"}'
```

```json
{"rows_before": 329, "rows_after": 148, "removed": 181}
```

> After any cleaning operation, re-run `python ingestion/pipeline.py` to propagate changes to the curated layer and DuckDB virtual tables.

---

## Management UI

The data management UI is served at `http://localhost:8000` and provides:

- **Dataset browser** — list all staged and curated Parquet files with row/column counts
- **Schema viewer** — column names, data types, null counts, and null percentages
- **Preview** — paginated row preview for any dataset
- **Cleaning tools** — drop columns, fill nulls, rename columns, filter rows

Cleaning operations are applied to the **staged layer**. After cleaning, re-run the ingestion pipeline to propagate changes to the curated layer:

```bash
python ingestion/pipeline.py
```

---

## ML Pipeline

The ML pipeline is defined in `ml/`. It reads from the curated Parquet layer, trains scikit-learn models, and saves `.pkl` artifacts with accompanying JSON metrics in `ml/models/`.

```python
# Example: build a dataset and train a model programmatically
from ml.dataset_builder import build_dataset
from ml.trainer import train

df = build_dataset("SELECT * FROM player_athletic_profiles WHERE forty_yard IS NOT NULL")
results = train(df, target="forty_yard", model_type="regression")
print(results)  # {"rmse": 0.12, "r2": 0.81, "model_path": "ml/models/...pkl"}
```

---

## API Reference — Quick Lookup

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/query` | Execute read-only SQL via DuckDB |
| `GET` | `/query/tables` | List available virtual tables |
| `GET` | `/players` | List/filter combine players |
| `GET` | `/players/search` | Search players by name (gold table or combine fallback) |
| `GET` | `/players/{name}` | Get combine records by name (partial match) |
| `GET` | `/players/id/{player_id}/profile` | Full enriched player profile (gold tables) |
| `GET` | `/players/id/{player_id}/athletic` | Combine-derived athletic scores |
| `GET` | `/players/id/{player_id}/production` | Season-by-season production metrics |
| `GET` | `/players/id/{player_id}/durability` | Career durability profile |
| `GET` | `/players/id/{player_id}/draft-value` | Draft value relative to round peers |
| `GET` | `/players/leaderboard/athletic` | Athletic score leaderboard |
| `GET` | `/players/leaderboard/production` | Production score leaderboard |
| `GET` | `/players/leaderboard/draft-value` | Draft value leaderboard by round |
| `GET` | `/teams` | List all team abbreviations |
| `GET` | `/teams/{abbr}/stats` | Team season stats (optional year range) |
| `GET` | `/graph/player/search/{name}` | Search graph by player name |
| `GET` | `/graph/player/{player_id}/profile` | Player graph profile |
| `GET` | `/graph/player/{player_id}/neighbors` | Graph neighbors up to N hops |
| `GET` | `/graph/player/{player_id}/career` | Full career path through graph |
| `GET` | `/graph/team/{abbr}/drafted` | Players drafted by a team |
| `GET` | `/graph/team/{abbr}/roster` | Team roster (contracted players) |
| `GET` | `/graph/path` | Shortest path between two entities |
| `GET` | `/graph/college/{name}/pipeline` | College → NFL pipeline |
| `GET` | `/graph/full` | All nodes and relationships for visualization |
| `GET` | `/manage/datasets` | List all Parquet files across lake zones |
| `GET` | `/manage/preview/{dataset}` | Preview dataset rows and schema info |
| `POST` | `/manage/clean/drop-columns` | Remove columns from a staged dataset |
| `POST` | `/manage/clean/fill-nulls` | Fill null values using a strategy |
| `POST` | `/manage/clean/rename` | Rename columns in a staged dataset |
| `POST` | `/manage/clean/filter-rows` | Keep rows matching a SQL expression |

---

## Dependencies

| Package | Purpose |
|---|---|
| `fastapi` | REST API framework |
| `uvicorn` | ASGI server |
| `duckdb` | In-process SQL engine over Parquet |
| `pandas` | Data transformation and analysis |
| `pyarrow` | Parquet read/write |
| `neo4j` | Neo4j Python driver |
| `lxml` | HTML parsing (for XLS source file) |
| `openpyxl` | Excel support |
| `httpx` | Async HTTP client |
| `python-multipart` | Form/file upload support |
| `scikit-learn` | ML model training (regression/classification) |
| `joblib` | Model serialization (`.pkl` files) |
