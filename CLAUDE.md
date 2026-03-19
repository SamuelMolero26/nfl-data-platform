# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

NFL-focused data lake platform with a REST API, SQL query engine (DuckDB), graph database (Neo4j), interactive data management UI, and ML model training pipeline. Full expansion plan at `files/plan_updated.md`.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Start Neo4j (required before running graph builder or API)
docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:5
# Or via Docker Compose (starts both Neo4j and API):
docker compose up -d

# Run ingestion pipeline (produces Parquet in lake/staged/ and lake/curated/)
python ingestion/pipeline.py
python ingestion/pipeline.py --legacy        # skip nflreadpy, combine/team-stats only
python ingestion/pipeline.py --pbp 2023 2024 # opt-in: large play-by-play (~35MB/season)

# Start the API server
uvicorn api.main:app --reload

# Populate Neo4j from curated Parquet
python graph/builder.py

# Format code
black .
```

No test suite currently exists (0% coverage). CI only runs a syntax compile check on key modules.

## Architecture

Medallion architecture — data flows one-way through three lake zones:

- `lake/raw/` — immutable source files, never modified
- `lake/staged/` — cleaned, typed Parquet per source (written by loaders)
- `lake/curated/` — join-ready Parquet (written by transforms + features; primary query target)

**Ingestion pipeline** (`ingestion/pipeline.py`) runs 5 sequential stages:
1. **Stage 0 — Raw ingestion**: Each `SourceLoader` subclass extracts → transforms → writes to `lake/staged/`
2. **Stage 1 — Master tables**: `ingestion/transforms/` builds canonical `master_players`, `master_teams`, `master_games` in `lake/curated/`
3. **Stage 2 — Player ID resolution**: `PlayerIdResolver` attaches canonical `player_id` (= `gsis_id` from nflreadpy) to all staged tables via three-pass matching: direct gsis_id → pfr_id lookup → rapidfuzz name match (threshold ≥ 90)
4. **Stage 3 — Gold features**: `ingestion/features/` computes athletic, production, durability, and draft-value profiles into `lake/curated/`
5. **Stage 4 — Backward compat**: Writes legacy `player_profiles.parquet` and `team_performance.parquet` aliases

**DuckDB** (`db/duckdb_client.py`) reads `lake/curated/*.parquet` as in-process virtual tables — no server. Auto-discovers files at startup, including hive-partitioned subdirs. The `/query` endpoint is read-only; mutations (INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE) are rejected via regex before execution.

**Neo4j** (`graph/builder.py`) is populated from curated Parquet using idempotent `MERGE` Cypher — safe to re-run. Node labels: `Player`, `Team`, `College`, `DraftClass`, `Season`, `Game`. Key relationships: `DRAFTED_BY`, `ATTENDED`, `COMPETED_IN`, `PLAYED_IN`, `SNAPPED_IN`, `INJURED_DURING`, `SELECTED_IN_DRAFT`, `CONTRACTED_BY`. Cypher query templates live in `graph/queries.py`.

**FastAPI** routers in `api/routers/` are split by domain:
- `query.py` — `POST /query` (DuckDB SQL), `GET /query/tables`
- `players.py` — legacy combine-based + gold profile endpoints (`/players/id/{player_id}/profile|athletic|production|durability|draft-value`) + leaderboards
- `teams.py` — team listing and season stats
- `graph.py` — Neo4j traversal (neighbors, career path, college pipeline, shortest path, full graph)
- `manage.py` — Parquet dataset inspection and in-memory cleaning operations (drop columns, fill nulls, rename, filter rows)

**`entrypoint.sh`** (Docker): waits for Neo4j on port 7687, runs ingestion if curated files are missing, then runs `graph/builder.py`, then starts uvicorn.

**ML pipeline** (`ml/`): stubs only — `dataset_builder.py` and `trainer.py` are not yet implemented. Models will be saved as `.pkl` + `.json` metrics in `ml/models/`.

## Loader Pattern

All loaders extend `SourceLoader` (`ingestion/base.py`):

```python
class SourceLoader(ABC):
    def extract(self) -> pd.DataFrame   # read raw source
    def transform(self, df) -> pd.DataFrame  # clean & type
    def load(self, df, output_path)     # write Parquet
    def run(self, output_path) -> pd.DataFrame  # full ETL
```

Current loaders in `ingestion/loaders/`: `combine_loader.py`, `team_stats_loader.py`, `nflreadpy_loader.py` (loads 12+ datasets via nflreadpy API, converts Polars → pandas).

## Data Source Quirks

**`nfl-combine.xls`** — Despite the `.xls` extension, this file is HTML with Excel metadata. Parse with `pd.read_html()`, not `pd.read_excel()`. Key transforms:
- `Ht` column is a string like `"6-2"` → convert to total inches (float)
- `Drafted (tm/rnd/yr)` is a single string like `"Dallas Cowboys / 7th / 247th pick / 2025"` → split into `draft_team`, `draft_round`, `draft_pick`, `draft_year`
- Many metric columns (`Bench`, `3Cone`, `Shuttle`) have high null rates — keep as `NaN`

**`nfl-team-statistics.csv`** — Clean CSV, 56 columns, 765 rows (1999–2022). Four columns (`offense_ave_air_yards`, `offense_ave_yac`, `defense_ave_air_yards`, `defense_ave_yac`) are null for early seasons — expected, not a data error.

## Feature Engineering Formulas

Gold athletic scores computed per position group (z-scores unless noted):
- `speed_score = (weight_lbs × 200) / (forty_yard⁴)` — Bill Barnwell formula
- `agility_score = mean(-three_cone_z, -shuttle_z)` — lower times → higher score
- `burst_score = mean(vertical_z, broad_jump_z)`
- `strength_score = bench_reps_z`
- `size_score = (height × weight) / position_group_avg_size`

Production: `snap_share`, `epa_per_game`, `passing_cpoe`, `target_share`, `nfl_production_score`

Draft value: `car_av`, `draft_value_score` (z-score within round), `draft_value_percentile`

## Configuration

All paths and credentials live in `config.py`. Environment variable overrides:
- `NEO4J_URI` / `NEO4J_USER` / `NEO4J_PASSWORD` (defaults: `bolt://localhost:7687`, `neo4j`, `password`)
- `API_HOST` / `API_PORT` (defaults: `0.0.0.0`, `8000`)
- `NFLVERSE_START_YEAR` / `NFLVERSE_END_YEAR` (defaults: `2010`, `2024`)

Create a `.env` file in the project root for local overrides.

## Deployment

CI (`.github/workflows/deploy.yml`) compiles key modules on PRs to `develop`/`main`. On push to `main`: builds a Docker image → pushes to GHCR → SSH deploys to VPS via Tailscale. Neo4j data persists via a named Docker volume.
