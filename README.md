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
- [Management UI](#management-ui)
- [ML Pipeline](#ml-pipeline)
- [API Reference](#api-reference)
- [Dependencies](#dependencies)

---

## Overview

The NFL Data Platform ingests raw NFL combine and team statistics data, transforms it through a **medallion architecture** (raw → staged → curated), and exposes the cleaned data through:

- **DuckDB** — in-process SQL queries over Parquet files
- **Neo4j** — graph traversal for player/team/college relationships
- **FastAPI** — REST API for querying, data management, and ML
- **Single-page UI** — interactive browser for dataset inspection and cleaning

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
│       ├── players.py          # GET /players
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

After staging, the pipeline builds two curated files used by DuckDB and the API:

- `lake/curated/player_profiles.parquet`
- `lake/curated/team_performance.parquet`

---

## Query Layer (DuckDB)

DuckDB reads `lake/curated/*.parquet` directly as virtual SQL tables — no import step, no server required.

**Available virtual tables:**

| Table | Source |
|---|---|
| `players` | `lake/curated/player_profiles.parquet` |
| `team_stats` | `lake/curated/team_performance.parquet` |

**The `/query` endpoint is read-only.** Any SQL containing `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, or `REPLACE` is rejected with HTTP 400.

Example query:

```sql
SELECT position, AVG(forty_yard) AS avg_forty
FROM players
WHERE forty_yard IS NOT NULL
GROUP BY position
ORDER BY avg_forty
```

---

## Graph Layer (Neo4j)

### Node Types

| Label | Key Properties |
|---|---|
| `Player` | `name`, `position`, `school`, `height_in`, `weight_lbs`, combine metrics |
| `Team` | `name`, `abbreviation` |
| `College` | `name` |
| `DraftClass` | `year` |
| `Season` | `year` |

### Relationship Types

| Relationship | From → To | Properties |
|---|---|---|
| `DRAFTED_BY` | Player → Team | `round`, `pick`, `year` |
| `ATTENDED` | Player → College | — |
| `COMPETED_IN` | Player → DraftClass | — |
| `PLAYED_IN` | Team → Season | `wins`, `losses`, `points_scored`, `score_differential`, `win_pct` |

### Building the Graph

```bash
python graph/builder.py
```

Uses idempotent `MERGE` Cypher statements — safe to re-run without creating duplicate nodes.

**Access Neo4j Browser:** `http://localhost:7474`  
Default credentials: `neo4j / password`

Example Cypher:

```cypher
MATCH (p:Player)-[:DRAFTED_BY]->(t:Team)
RETURN p.name, p.position, t.name
LIMIT 10
```

---

## REST API

Base URL: `http://localhost:8000`  
Swagger UI: `http://localhost:8000/docs`  
ReDoc: `http://localhost:8000/redoc`

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/query` | Execute read-only SQL via DuckDB |
| `GET` | `/query/tables` | List available virtual tables |
| `GET` | `/players` | List/filter players |
| `GET` | `/players/{name}` | Get player profile by name |
| `GET` | `/teams` | List all teams |
| `GET` | `/teams/{abbr}/stats` | Team season stats (optional year range) |
| `GET` | `/graph/player/{name}/neighbors` | Graph neighbors up to N hops |
| `GET` | `/graph/player/{name}/profile` | Full player graph profile |
| `GET` | `/graph/team/{name}/drafted` | Players drafted by a team |
| `GET` | `/graph/path` | Shortest path between two entities |
| `GET` | `/graph/full` | All nodes and relationships (for visualization) |
| `GET` | `/graph/college/{name}/pipeline` | All players from a college + draft outcomes |
| `GET` | `/manage/datasets` | List all Parquet files across lake zones |
| `GET` | `/manage/preview/{dataset}` | Preview dataset rows + schema info |
| `POST` | `/manage/clean/drop-columns` | Remove columns from a dataset |
| `POST` | `/manage/clean/fill-nulls` | Fill null values using a strategy |
| `POST` | `/manage/clean/rename` | Rename columns in a dataset |
| `POST` | `/manage/clean/filter-rows` | Filter rows using a SQL expression |

### Example Requests

**SQL Query**
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT position, COUNT(*) as count FROM players GROUP BY position ORDER BY count DESC"}'
```

**Filter Players**
```bash
curl "http://localhost:8000/players?position=QB&drafted_only=true&limit=10"
```

**Team Stats**
```bash
curl "http://localhost:8000/teams/NE/stats?season_from=2010&season_to=2020"
```

**Graph: Player Neighbors**
```bash
curl "http://localhost:8000/graph/player/Cam%20Ward/neighbors?depth=2"
```

**Graph: Shortest Path**
```bash
curl "http://localhost:8000/graph/path?from_name=Cam%20Ward&to_name=Dallas%20Cowboys"
```

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
