"""
DuckDB client — in-process SQL engine over the curated Parquet lake.

Virtual tables registered on connect:
  - players      → lake/curated/player_profiles.parquet
  - team_stats   → lake/curated/team_performance.parquet

All queries are read-only. Mutations raise ValueError before reaching DuckDB.
"""
import re
import json
import duckdb
import pandas as pd
from pathlib import Path

import config

_MUTATION_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE)\b",
    re.IGNORECASE,
)

_conn: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect(database=":memory:")
        _register_tables(_conn)
    return _conn


def _register_tables(conn: duckdb.DuckDBPyConnection) -> None:
    tables = {
        "players": config.CURATED_PLAYER_PROFILES,
        "team_stats": config.CURATED_TEAM_PERFORMANCE,
    }
    for name, path in tables.items():
        if path.exists():
            conn.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{path}')")
        else:
            print(f"[DuckDB] Warning: {path} not found — '{name}' view skipped.")


def execute(sql: str) -> pd.DataFrame:
    """Run a read-only SQL query and return the result as a DataFrame."""
    if _MUTATION_PATTERN.search(sql):
        raise ValueError("Only read-only SELECT queries are permitted.")
    conn = get_connection()
    return conn.execute(sql).df()


def list_tables() -> list[str]:
    conn = get_connection()
    return conn.execute("SHOW TABLES").df()["name"].tolist()


def df_to_records(df: pd.DataFrame) -> list[dict]:
    """
    Convert a DataFrame to JSON-safe records.
    Uses pandas to_json internally so NaN → null, int64 → int, etc.
    """
    return json.loads(df.to_json(orient="records"))


def reset() -> None:
    """Force reconnect — use after new Parquet files are written."""
    global _conn
    if _conn:
        _conn.close()
    _conn = None
