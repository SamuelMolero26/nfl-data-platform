# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

NFL-focused data lake platform with a REST API, SQL query engine (DuckDB), graph database (Neo4j), interactive data management UI, and ML model training pipeline. See `plan.md` for the full implementation roadmap.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Start Neo4j (required before running graph builder or API)
docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:5

# Run the ingestion pipeline (produces Parquet in lake/staged/ and lake/curated/)
python ingestion/pipeline.py

# Start the API server
uvicorn api.main:app --reload

# Populate Neo4j from staged Parquet
python graph/builder.py

```

## Architecture

The platform uses a **medallion architecture** with three lake zones:

- `lake/raw/` — immutable source files, never modified
- `lake/staged/` — cleaned, typed Parquet files (one per source)
- `lake/curated/` — join-ready, query-optimized Parquet (primary query target)

Data flows: `raw → ingestion loaders → staged → curated → DuckDB + Neo4j → FastAPI → UI/ML`

**DuckDB** reads `lake/curated/*.parquet` directly as virtual tables — no import step, no server. The `/query` endpoint is read-only; SQL with mutations must be rejected.

**Neo4j** is populated by `graph/builder.py` using idempotent `MERGE` Cypher statements — safe to re-run. Graph nodes: `Player`, `Team`, `College`, `DraftClass`, `Season`. Key relationships: `DRAFTED_BY`, `ATTENDED`, `COMPETED_IN`, `PLAYED_IN`.

**FastAPI** routers are split by domain: `query.py` (DuckDB SQL), `players.py`, `teams.py`, `graph.py` (Neo4j traversal), `manage.py` (data cleaning), `ml.py` (model training trigger).

**ML pipeline**: `ml/dataset_builder.py` converts a DuckDB SQL query into a training DataFrame; `ml/trainer.py` wraps scikit-learn and saves `.pkl` models + JSON metrics to `ml/models/`.

## Data Source Quirks

**`nfl-combine.xls`** — Despite the `.xls` extension, this file is HTML with Excel metadata. Parse it with `pd.read_html()`, not `pd.read_excel()`. Key transforms needed:
- `Ht` column is a string like `"6-2"` → convert to total inches (float)
- `Drafted (tm/rnd/yr)` is a single string like `"Dallas Cowboys / 7th / 247th pick / 2025"` → split into `draft_team`, `draft_round`, `draft_pick`, `draft_year`
- Many metric columns (`Bench`, `3Cone`, `Shuttle`) have high null rates — keep as `NaN`

**`nfl-team-statistics.csv`** — Clean CSV, 56 columns, 765 rows (1999–2022). Four columns (`offense_ave_air_yards`, `offense_ave_yac`, `defense_ave_air_yards`, `defense_ave_yac`) are null for early seasons — this is expected, not a data error.

## Configuration

All paths and credentials live in `config.py`. Neo4j default: `bolt://localhost:7687`, credentials `neo4j/password`. Override via environment variables for production.
