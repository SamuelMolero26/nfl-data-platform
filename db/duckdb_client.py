"""
DuckDB client — in-process SQL engine over the curated Parquet lake.

Views registered on connect (auto-discovered):
  Flat parquets  : every *.parquet in lake/curated/ → view named by file stem
                   e.g. master_players.parquet → SELECT * FROM master_players
  Partitioned dirs: any subdirectory containing *.parquet files (Hive layout)
                   e.g. play_by_play/season=2023/ → SELECT * FROM play_by_play

Legacy aliases (always registered if source exists):
  players    → player_profiles   (backward compat)
  team_stats → team_performance  (backward compat)

All queries are read-only. Mutations raise ValueError before reaching DuckDB.
"""

import logging
import re
import json
import duckdb
import pandas as pd
from pathlib import Path

import config

logger = logging.getLogger(__name__)

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


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier using standard double-quote escaping."""
    return '"' + name.replace('"', '""') + '"'


def _register_tables(conn: duckdb.DuckDBPyConnection) -> None:
    registered: list[str] = []
    skipped: list[str] = []

    curated = config.LAKE_CURATED_DIR
    if not curated.exists():
        logger.warning("Curated lake directory not found: %s", curated)
        return

    # Auto-register flat parquets (view name = file stem)
    for f in sorted(curated.glob("*.parquet")):
        try:
            conn.execute(
                 f"CREATE VIEW {_quote_identifier(f.stem)} AS "
                 f"SELECT * FROM read_parquet('{f}')"
             )
            registered.append(f.stem)
        except Exception as exc:
            logger.warning("Could not register view '%s': %s", f.stem, exc)
            skipped.append(f.stem)

    # Auto-register Hive-partitioned subdirectories
    for d in sorted(curated.iterdir()):
        if d.is_dir() and any(d.rglob("*.parquet")):
            try:
                conn.execute(
                    f"CREATE VIEW {_quote_identifier(d.name)} AS SELECT * FROM "
                    f"read_parquet('{d}/**/*.parquet', hive_partitioning=true)"
                )
                registered.append(d.name)
            except Exception as exc:
                logger.warning(
                    "Could not register partitioned view '%s': %s", d.name, exc
                )
                skipped.append(d.name)

    # Legacy aliases for backward compatibility
    _register_legacy_aliases(conn, registered)

    logger.info(
        "DuckDB: %s views registered%s",
        len(registered),
        f", {len(skipped)} skipped" if skipped else "",
    )


def _register_legacy_aliases(
    conn: duckdb.DuckDBPyConnection, registered: list[str]
) -> None:
    """Register short legacy names used by existing API code."""
    aliases = {
        "players": ("player_profiles", config.CURATED_PLAYER_PROFILES),
        "team_stats": ("team_performance", config.CURATED_TEAM_PERFORMANCE),
    }
    for alias, (stem, path) in aliases.items():
        if alias in registered:
            continue  # already registered by auto-discovery under this name
        if stem in registered:
            # The canonical view exists — alias it
            try:
                conn.execute(f"CREATE VIEW {alias} AS SELECT * FROM {stem}")
            except Exception as exc:
                logger.warning(
                    "Could not create alias '%s' → '%s': %s", alias, stem, exc
                )
        elif path.exists():
            # Fall back to registering from the file directly
            try:
                conn.execute(
                    f"CREATE VIEW {alias} AS SELECT * FROM read_parquet('{path}')"
                )
            except Exception as exc:
                logger.warning("Could not register legacy view '%s': %s", alias, exc)


def execute(sql: str, params: list | None = None) -> pd.DataFrame:
    """Run a read-only SQL query and return the result as a DataFrame."""
    if _MUTATION_PATTERN.search(sql):
        raise ValueError("Only read-only SELECT queries are permitted.")
    conn = get_connection()
    if params:
        return conn.execute(sql, params).df()
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
