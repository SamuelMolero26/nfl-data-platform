# NFL Data Platform — Implementation Plan

## Context
Build a centralized data lake platform that ingests NFL data from multiple sources, stores it in a structured medallion architecture, exposes SQL-style queries and a REST API, supports graph queries via Neo4j, allows users to clean/manage data interactively, and enables ML model generation on curated datasets.

Current source files:
- `nfl-combine.xls` — 329 players, 2025 combine class, HTML-based XLS requiring custom parsing
- `nfl-team-statistics.csv` — 765 rows, 56 columns, 1999–2022 season stats for all 32 NFL teams

---

## Architecture Overview

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

---

## Directory Structure

```
nfl-data-platform/
├── lake/
│   ├── raw/                          # Immutable source files
│   │   ├── combine/nfl-combine.xls
│   │   └── team_stats/nfl-team-statistics.csv
│   ├── staged/                       # Cleaned, typed Parquet
│   │   ├── players/combine.parquet
│   │   └── teams/team_statistics.parquet
│   └── curated/                      # Join-ready, query-optimized
│       ├── player_profiles.parquet
│       └── team_performance.parquet
├── ingestion/
│   ├── base.py                       # Abstract SourceLoader class
│   ├── combine_loader.py             # HTML-XLS parser + transformer
│   ├── team_stats_loader.py          # CSV loader + transformer
│   └── pipeline.py                  # Runs all loaders in order
├── db/
│   ├── duckdb_client.py              # DuckDB connection + query runner
│   └── neo4j_client.py              # Neo4j driver + graph builder
├── graph/
│   ├── builder.py                    # Reads Parquet → populates Neo4j
│   └── queries.py                   # Cypher query templates
├── api/
│   ├── main.py                       # FastAPI app entrypoint
│   └── routers/
│       ├── query.py                  # POST /query — DuckDB SQL
│       ├── players.py               # GET /players
│       ├── teams.py                 # GET /teams
│       ├── graph.py                 # GET /graph/* — Neo4j traversal
│       ├── manage.py                # Data cleaning/management endpoints
│       └── ml.py                    # Dataset export + model training trigger
├── ui/
│   └── index.html                   # Lightweight data management frontend
├── ml/
│   ├── dataset_builder.py           # Exports curated Parquet → ML-ready CSV/df
│   ├── trainer.py                   # scikit-learn model training pipeline
│   └── models/                      # Saved .pkl models + metadata JSON
├── config.py                         # Paths, DB credentials, settings
└── requirements.txt
```

---

## Phase 1 — Ingestion & Storage

### Combine Loader (`ingestion/combine_loader.py`)
- Parse `nfl-combine.xls` as HTML table (not binary XLS) using `pd.read_html()`
- Transformations:
  - Parse `Ht` string (e.g. "6-2") → total inches (float)
  - Split `Drafted (tm/rnd/yr)` → `draft_team`, `draft_round`, `draft_pick`, `draft_year`
  - Cast numeric columns (`40yd`, `Vertical`, `Bench`, `Broad Jump`, `3Cone`, `Shuttle`) to float
  - Fill nulls with `NaN`, rename columns to snake_case
- Output: `lake/staged/players/combine.parquet`

### Team Stats Loader (`ingestion/team_stats_loader.py`)
- Parse `nfl-team-statistics.csv` with `pd.read_csv()`
- Transformations:
  - Validate 56 columns all present
  - Handle nulls in `offense_ave_air_yards`, `offense_ave_yac`, `defense_ave_air_yards`, `defense_ave_yac` (early seasons) — keep as NaN
  - Normalize team abbreviations
- Output: `lake/staged/teams/team_statistics.parquet`

### Curated Layer
- `player_profiles.parquet` — combine stats enriched with draft info
- `team_performance.parquet` — team stats with derived columns (win_pct, point_diff_per_game)

---

## Phase 2 — Query Layer (DuckDB)

`db/duckdb_client.py`:
- In-process DuckDB connection (no server)
- Registers `lake/curated/*.parquet` as virtual tables on startup
- Exposes `execute(sql: str) -> pd.DataFrame`
- Read-only mode enforced (no INSERT/UPDATE/DELETE allowed)

---

## Phase 3 — Graph Layer (Neo4j)

### Node Types
| Label | Properties |
|---|---|
| `Player` | name, position, school, height_in, weight |
| `Team` | abbreviation, name |
| `College` | name |
| `DraftClass` | year |
| `Season` | year |

### Relationship Types
| Relationship | From → To | Properties |
|---|---|---|
| `DRAFTED_BY` | Player → Team | round, pick, year |
| `ATTENDED` | Player → College | |
| `COMPETED_IN` | Player → DraftClass | |
| `PLAYED_IN` | Team → Season | wins, losses, points_scored, score_differential |

### Graph Builder (`graph/builder.py`)
- Reads staged Parquet files
- Uses Neo4j Python driver (`neo4j` package) with batch `MERGE` Cypher statements
- Idempotent — safe to re-run without duplicating nodes

### Graph Queries (`graph/queries.py`)
- `get_player_neighbors(player_name, depth=1)` — teammates, college peers
- `get_team_draft_history(team_abbr, year)` — all players drafted by a team
- `shortest_path(player_a, player_b)` — connection via shared team/college
- `college_to_nfl_pipeline(college_name)` — all players from a school + their draft outcomes

Neo4j runs via Docker:
```bash
docker run -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:5
```

---

## Phase 4 — REST API (FastAPI)

### Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/query` | Execute read-only SQL via DuckDB |
| `GET` | `/players` | List/filter players (position, school, draft_team) |
| `GET` | `/players/{name}` | Full player profile |
| `GET` | `/teams` | List teams |
| `GET` | `/teams/{abbr}/stats` | Team stats by season range |
| `GET` | `/graph/player/{name}/neighbors` | Graph neighbors |
| `GET` | `/graph/team/{abbr}/drafted` | Players drafted by team |
| `GET` | `/graph/path?from=&to=` | Shortest path between entities |
| `GET` | `/manage/datasets` | List all staged/curated Parquet files |
| `POST` | `/manage/clean` | Apply cleaning rules to a dataset |
| `GET` | `/manage/preview/{dataset}` | Preview dataset (first N rows) |
| `POST` | `/ml/export` | Export curated dataset as CSV for ML |
| `POST` | `/ml/train` | Train a model (regression/classification) on selected columns |
| `GET` | `/ml/models` | List trained models + metrics |

---

## Phase 5 — Data Management UI

Lightweight single-page UI (`ui/index.html`, served by FastAPI at `/`):
- **Dataset browser**: list, preview, download Parquet files
- **Column editor**: rename, drop, cast, fill nulls on staged data
- **Filter builder**: visual WHERE clause builder → runs via `/query`
- **Schema viewer**: column names, types, null counts, value distributions
- **Cleaning rules**: save reusable transformation rules per dataset

---

## Key Dependencies

```
duckdb
fastapi
uvicorn
pandas
pyarrow
openpyxl
lxml
neo4j
scikit-learn
joblib
httpx
python-multipart
```

---

## Implementation Order

1. `config.py` — paths and credentials
2. `ingestion/base.py` + loaders + `pipeline.py`
3. `db/duckdb_client.py`
4. `db/neo4j_client.py` + `graph/builder.py` + `graph/queries.py`
5. `api/main.py` + all routers
6. `ui/index.html`
7. `ml/dataset_builder.py` + `ml/trainer.py`

---

## Verification

1. Run `python ingestion/pipeline.py` → confirm Parquet files appear in `lake/staged/` and `lake/curated/`
2. DuckDB: `SELECT * FROM 'lake/curated/player_profiles.parquet' LIMIT 5;`
3. Neo4j Browser (`http://localhost:7474`): `MATCH (p:Player)-[:DRAFTED_BY]->(t:Team) RETURN p, t LIMIT 10`
4. FastAPI: `uvicorn api.main:app --reload` → visit `/docs` for Swagger UI
5. POST `/query` with `SELECT position, AVG("40yd") FROM players GROUP BY position`
6. GET `/graph/player/{name}/neighbors` → verify correct connections
7. POST `/ml/train` with `score_differential` as target → verify metrics returned
