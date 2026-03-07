import os
from pathlib import Path

BASE_DIR = Path(__file__).parent

# --- Lake zones ---
LAKE_RAW_DIR = BASE_DIR / "lake" / "raw"
LAKE_STAGED_DIR = BASE_DIR / "lake" / "staged"
LAKE_CURATED_DIR = BASE_DIR / "lake" / "curated"

# Raw source paths
RAW_COMBINE = LAKE_RAW_DIR / "combine" / "nfl-combine.xls"
RAW_TEAM_STATS = LAKE_RAW_DIR / "team_stats" / "nfl-team-statistics.csv"

# Staged output paths
STAGED_COMBINE = LAKE_STAGED_DIR / "players" / "combine.parquet"
STAGED_TEAM_STATS = LAKE_STAGED_DIR / "teams" / "team_statistics.parquet"

# Curated output paths
CURATED_PLAYER_PROFILES = LAKE_CURATED_DIR / "player_profiles.parquet"
CURATED_TEAM_PERFORMANCE = LAKE_CURATED_DIR / "team_performance.parquet"

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
