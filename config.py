import os
from pathlib import Path

BASE_DIR = Path(__file__).parent

# --- Lake zones ---
LAKE_RAW_DIR = BASE_DIR / "lake" / "raw"
LAKE_STAGED_DIR = BASE_DIR / "lake" / "staged"
LAKE_CURATED_DIR = BASE_DIR / "lake" / "curated"

# --- Raw source paths (existing) ---
RAW_COMBINE = LAKE_RAW_DIR / "combine" / "nfl-combine.xls"
RAW_TEAM_STATS = LAKE_RAW_DIR / "team_stats" / "nfl-team-statistics.csv"

# --- Raw dirs for API-sourced data ---
RAW_NFLREADPY_DIR = LAKE_RAW_DIR / "nflreadpy"

# --- Staged: existing ---
STAGED_COMBINE = LAKE_STAGED_DIR / "players" / "combine.parquet"
STAGED_TEAM_STATS = LAKE_STAGED_DIR / "teams" / "team_statistics.parquet"

# --- Staged: new nflreadpy datasets ---
STAGED_ROSTERS = LAKE_STAGED_DIR / "players" / "rosters.parquet"
STAGED_DRAFT_HISTORY = LAKE_STAGED_DIR / "players" / "draft_history.parquet"
STAGED_SNAP_COUNTS = LAKE_STAGED_DIR / "players" / "snap_counts.parquet"
STAGED_INJURIES = LAKE_STAGED_DIR / "players" / "injuries.parquet"
STAGED_DEPTH_CHARTS = LAKE_STAGED_DIR / "players" / "depth_charts.parquet"
STAGED_WEEKLY_STATS = LAKE_STAGED_DIR / "players" / "weekly_stats.parquet"
STAGED_NGS_DATA = LAKE_STAGED_DIR / "players" / "ngs_data.parquet"
STAGED_PFR_ADVSTATS = LAKE_STAGED_DIR / "players" / "pfr_advstats.parquet"
STAGED_FTN_DATA = LAKE_STAGED_DIR / "players" / "ftn_data.parquet"
STAGED_CONTRACTS = LAKE_STAGED_DIR / "players" / "contracts.parquet"
STAGED_SCHEDULES = LAKE_STAGED_DIR / "games" / "schedules.parquet"
STAGED_PBP_DIR = LAKE_STAGED_DIR / "games" / "play_by_play"

# --- Curated: existing (keep for backward compat) ---
CURATED_PLAYER_PROFILES = LAKE_CURATED_DIR / "player_profiles.parquet"
CURATED_TEAM_PERFORMANCE = LAKE_CURATED_DIR / "team_performance.parquet"

# --- Curated: new master / feature tables ---
CURATED_MASTER_PLAYERS = LAKE_CURATED_DIR / "master_players.parquet"
CURATED_MASTER_TEAMS = LAKE_CURATED_DIR / "master_teams.parquet"
CURATED_MASTER_GAMES = LAKE_CURATED_DIR / "master_games.parquet"
CURATED_ATHLETIC_PROFILES = LAKE_CURATED_DIR / "player_athletic_profiles.parquet"
CURATED_PRODUCTION_PROFILES = LAKE_CURATED_DIR / "player_production_profiles.parquet"
CURATED_DURABILITY_PROFILES = LAKE_CURATED_DIR / "player_durability_profiles.parquet"
CURATED_TEAM_PERF_SUMMARY = LAKE_CURATED_DIR / "team_performance_summary.parquet"
CURATED_TEAM_ROSTER_COMP = LAKE_CURATED_DIR / "team_roster_composition.parquet"
CURATED_POSITIONAL_USAGE = LAKE_CURATED_DIR / "positional_usage_profiles.parquet"
CURATED_DRAFT_VALUE_HISTORY = LAKE_CURATED_DIR / "draft_value_history.parquet"

# --- Neo4j ---
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# --- ML ---
ML_MODELS_DIR = BASE_DIR / "ml" / "models"
ML_MODELS_DIR.mkdir(parents=True, exist_ok=True)

# --- API ---
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# --- nflreadpy / nflverse ---
NFLVERSE_START_YEAR = int(os.getenv("NFLVERSE_START_YEAR", "2010"))
NFLVERSE_END_YEAR = int(os.getenv("NFLVERSE_END_YEAR", "2024"))
