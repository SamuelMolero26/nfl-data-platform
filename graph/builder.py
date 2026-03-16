"""
graph/builder.py — Populate Neo4j from curated + staged Parquet files.

Node labels:
  Player     — keyed on player_id (gsis_id); falls back to name for legacy combine rows
  Team       — keyed on abbreviation (canonical, post-normalization)
  College    — keyed on name
  DraftClass — keyed on year (integer)
  Season     — keyed on year (integer)
  Game       — keyed on game_id

Relationships built:
  (Player)-[:ATTENDED]->(College)
  (Player)-[:COMPETED_IN]->(DraftClass)
  (Player)-[:DRAFTED_BY {round, pick, year}]->(Team)
  (Player)-[:PLAYED_IN {wins, losses, ...}]->(Season)          [via team stats]
  (Team)-[:PLAYED_IN]->(Season)
  (Team)-[:HOME_IN {home_score, away_score, result}]->(Game)
  (Team)-[:AWAY_IN {home_score, away_score, result}]->(Game)
  (Player)-[:SNAPPED_IN {offense_snaps, defense_snaps, st_snaps}]->(Game)
  (Player)-[:INJURED_DURING {report_status, practice_status, body_part}]->(Season)
  (Player)-[:SELECTED_IN_DRAFT {round, pick, car_av, draft_value_score}]->(DraftClass)
  (Player)-[:CONTRACTED_BY {cap_hit, apy, guaranteed, year_signed}]->(Team)

Safe to re-run — all writes use idempotent MERGE.
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from neo4j import Session

import config
from db.neo4j_client import get_driver

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constraints
# ---------------------------------------------------------------------------

CONSTRAINTS = [
    # Player keyed on player_id (gsis_id) — names are not globally unique
    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Player) REQUIRE p.player_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Team)   REQUIRE t.abbreviation IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:College) REQUIRE c.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (d:DraftClass) REQUIRE d.year IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Season) REQUIRE s.year IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Game)   REQUIRE g.game_id IS UNIQUE",
]


def create_constraints(session: Session) -> None:
    for stmt in CONSTRAINTS:
        session.run(stmt)
    logger.info("Constraints created/verified.")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _to_records(df: pd.DataFrame) -> list[dict]:
    """Replace NaN/NaT with None so Neo4j receives null, not float('nan')."""
    return df.where(pd.notnull(df), None).to_dict("records")


# ---------------------------------------------------------------------------
# Master players → Player nodes
# ---------------------------------------------------------------------------


def _build_player_nodes(session: Session) -> None:
    """
    Build Player nodes from master_players (curated).
    Keyed on player_id (gsis_id). Falls back to combine if master_players
    doesn't exist yet (legacy mode).
    """
    path = config.CURATED_MASTER_PLAYERS
    if not path.exists():
        logger.warning("master_players not found — falling back to staged combine")
        _build_players_from_combine(session)
        return

    df = pd.read_parquet(path)
    records = _to_records(df)

    session.run(
        """
        UNWIND $rows AS row
        MERGE (p:Player {player_id: row.player_id})
          SET p.player_name  = row.player_name,
              p.position     = row.position,
              p.team         = row.team,
              p.first_season = toInteger(row.first_season),
              p.last_season  = toInteger(row.last_season),
              p.height_in    = row.height_in,
              p.weight_lbs   = row.weight_lbs,
              p.college      = row.college

        FOREACH (_ IN CASE WHEN row.college IS NOT NULL THEN [1] ELSE [] END |
            MERGE (c:College {name: row.college})
            MERGE (p)-[:ATTENDED]->(c)
        )
        """,
        rows=records,
    )
    logger.info("Merged %s Player nodes from master_players.", f"{len(records):,}")


def _build_players_from_combine(session: Session) -> None:
    """Legacy fallback: build Player nodes from staged combine (name-keyed)."""
    if not config.STAGED_COMBINE.exists():
        logger.warning("Staged combine not found — Player nodes skipped.")
        return

    df = pd.read_parquet(config.STAGED_COMBINE)
    records = _to_records(df)

    session.run(
        """
        UNWIND $rows AS row
        MERGE (p:Player {player_id: coalesce(row.player_id, row.player_name)})
          SET p.player_name = row.player_name,
              p.position    = row.position,
              p.height_in   = row.height_in,
              p.weight_lbs  = row.weight_lbs,
              p.forty_yard  = row.forty_yard,
              p.bench_reps  = row.bench_reps

        FOREACH (_ IN CASE WHEN row.school IS NOT NULL THEN [1] ELSE [] END |
            MERGE (c:College {name: row.school})
            MERGE (p)-[:ATTENDED]->(c)
        )
        FOREACH (_ IN CASE WHEN row.draft_year IS NOT NULL THEN [1] ELSE [] END |
            MERGE (d:DraftClass {year: toInteger(row.draft_year)})
            MERGE (p)-[:COMPETED_IN]->(d)
        )
        FOREACH (_ IN CASE WHEN row.draft_team IS NOT NULL THEN [1] ELSE [] END |
            MERGE (t:Team {abbreviation: row.draft_team})
            MERGE (p)-[r:DRAFTED_BY]->(t)
              SET r.round = row.draft_round,
                  r.pick  = toInteger(row.draft_pick),
                  r.year  = toInteger(row.draft_year)
        )
        """,
        rows=records,
    )
    logger.info("Merged %s Player nodes from combine (legacy).", f"{len(records):,}")


# ---------------------------------------------------------------------------
# Master teams → Team nodes
# ---------------------------------------------------------------------------


def _build_team_nodes(session: Session) -> None:
    path = config.CURATED_MASTER_TEAMS
    if not path.exists():
        logger.warning(
            "master_teams not found — Team nodes will be created implicitly."
        )
        return

    df = pd.read_parquet(path)
    records = _to_records(df)

    session.run(
        """
        UNWIND $rows AS row
        MERGE (t:Team {abbreviation: row.team_id})
          SET t.full_name    = row.team_name,
              t.conference   = row.conference,
              t.division     = row.division,
              t.is_active    = row.is_active,
              t.color        = row.team_color,
              t.color2       = row.team_color2
        """,
        rows=records,
    )
    logger.info("Merged %s Team nodes.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# Team stats → Team→Season PLAYED_IN relationships
# ---------------------------------------------------------------------------


def _build_team_seasons(session: Session) -> None:
    if not config.STAGED_TEAM_STATS.exists():
        logger.warning("team_stats not found — PLAYED_IN relationships skipped.")
        return

    df = pd.read_parquet(config.STAGED_TEAM_STATS)
    records = _to_records(df)

    session.run(
        """
        UNWIND $rows AS row
        MERGE (t:Team {abbreviation: row.team})
        MERGE (s:Season {year: toInteger(row.season)})
        MERGE (t)-[r:PLAYED_IN]->(s)
          SET r.wins                = row.wins,
              r.losses              = row.losses,
              r.ties                = row.ties,
              r.points_scored       = row.points_scored,
              r.points_allowed      = row.points_allowed,
              r.score_differential  = row.score_differential,
              r.win_pct             = row.win_pct,
              r.point_diff_per_game = row.point_diff_per_game
        """,
        rows=records,
    )
    logger.info("Merged %s Team→Season PLAYED_IN relationships.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# Master games → Game nodes + HOME_IN / AWAY_IN relationships
# ---------------------------------------------------------------------------


def _build_game_nodes(session: Session) -> None:
    path = config.CURATED_MASTER_GAMES
    if not path.exists():
        logger.warning("master_games not found — Game nodes skipped.")
        return

    df = pd.read_parquet(path)
    records = _to_records(df)

    session.run(
        """
        UNWIND $rows AS row
        MERGE (g:Game {game_id: row.game_id})
          SET g.season      = toInteger(row.season),
              g.week        = row.week,
              g.game_type   = row.game_type,
              g.home_team   = row.home_team,
              g.away_team   = row.away_team,
              g.home_score  = toInteger(row.home_score),
              g.away_score  = toInteger(row.away_score),
              g.result      = row.result,
              g.stadium     = row.stadium,
              g.location    = row.location

        FOREACH (_ IN CASE WHEN row.home_team IS NOT NULL THEN [1] ELSE [] END |
            MERGE (ht:Team {abbreviation: row.home_team})
            MERGE (ht)-[r:HOME_IN]->(g)
              SET r.score = toInteger(row.home_score),
                  r.result = row.result
        )
        FOREACH (_ IN CASE WHEN row.away_team IS NOT NULL THEN [1] ELSE [] END |
            MERGE (at:Team {abbreviation: row.away_team})
            MERGE (at)-[r:AWAY_IN]->(g)
              SET r.score = toInteger(row.away_score)
        )
        """,
        rows=records,
    )
    logger.info("Merged %s Game nodes.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# Snap counts → SNAPPED_IN relationships
# ---------------------------------------------------------------------------


def _build_snap_relationships(session: Session) -> None:
    path = config.STAGED_SNAP_COUNTS
    if not path.exists():
        logger.warning("snap_counts not found — SNAPPED_IN relationships skipped.")
        return

    df = pd.read_parquet(path)

    # Only rows with a resolved player_id and a game_id can link Player→Game
    id_col = "player_id" if "player_id" in df.columns else None
    if id_col is None:
        logger.warning(
            "snap_counts lacks player_id — run Stage 2 first. "
            "SNAPPED_IN relationships skipped."
        )
        return
    if "game_id" not in df.columns:
        logger.warning("snap_counts lacks game_id — SNAPPED_IN relationships skipped.")
        return

    df = df.dropna(subset=["player_id", "game_id"])
    records = _to_records(df)

    session.run(
        """
        UNWIND $rows AS row
        MATCH (p:Player {player_id: row.player_id})
        MATCH (g:Game   {game_id:   row.game_id})
        MERGE (p)-[r:SNAPPED_IN]->(g)
          SET r.offense_snaps = toInteger(row.offense_snaps),
              r.defense_snaps = toInteger(row.defense_snaps),
              r.st_snaps      = toInteger(row.st_snaps),
              r.offense_pct   = row.offense_pct
        """,
        rows=records,
    )
    logger.info("Merged %s SNAPPED_IN relationships.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# Injuries → INJURED_DURING relationships
# ---------------------------------------------------------------------------


def _build_injury_relationships(session: Session) -> None:
    path = config.STAGED_INJURIES
    if not path.exists():
        logger.warning("injuries not found — INJURED_DURING relationships skipped.")
        return

    df = pd.read_parquet(path)

    id_col = "player_id" if "player_id" in df.columns else "gsis_id"
    if id_col not in df.columns:
        logger.warning("injuries lacks player_id/gsis_id — INJURED_DURING skipped.")
        return
    if id_col != "player_id":
        df = df.rename(columns={id_col: "player_id"})

    if "season" not in df.columns:
        logger.warning("injuries lacks season column — INJURED_DURING skipped.")
        return

    df = df.dropna(subset=["player_id", "season"])
    records = _to_records(df)

    session.run(
        """
        UNWIND $rows AS row
        MATCH (p:Player {player_id: row.player_id})
        MERGE  (s:Season {year: toInteger(row.season)})
        MERGE  (p)-[r:INJURED_DURING]->(s)
          SET r.report_status   = row.report_status,
              r.practice_status = row.practice_status,
              r.primary_injury  = row.primary_injury,
              r.week            = row.week
        """,
        rows=records,
    )
    logger.info("Merged %s INJURED_DURING relationships.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# Draft value history → SELECTED_IN_DRAFT relationships
# ---------------------------------------------------------------------------


def _build_draft_relationships(session: Session) -> None:
    path = config.CURATED_DRAFT_VALUE_HISTORY
    if not path.exists():
        logger.warning("draft_value_history not found — SELECTED_IN_DRAFT skipped.")
        return

    df = pd.read_parquet(path)
    df = df.dropna(subset=["player_id", "season"])
    records = _to_records(df)

    session.run(
        """
        UNWIND $rows AS row
        MATCH (p:Player {player_id: row.player_id})
        MERGE (d:DraftClass {year: toInteger(row.season)})
        MERGE (p)-[r:SELECTED_IN_DRAFT]->(d)
          SET r.round                = toInteger(row.round),
              r.pick                 = toInteger(row.pick),
              r.team                 = row.team,
              r.car_av               = row.car_av,
              r.draft_value_score    = row.draft_value_score,
              r.draft_value_percentile = row.draft_value_percentile
        """,
        rows=records,
    )
    logger.info("Merged %s SELECTED_IN_DRAFT relationships.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# Contracts → CONTRACTED_BY relationships
# ---------------------------------------------------------------------------


def _build_contract_relationships(session: Session) -> None:
    path = config.STAGED_CONTRACTS
    if not path.exists():
        logger.warning("contracts not found — CONTRACTED_BY relationships skipped.")
        return

    df = pd.read_parquet(path)

    id_col = "player_id" if "player_id" in df.columns else "gsis_id"
    if id_col not in df.columns:
        logger.warning("contracts lacks player_id/gsis_id — CONTRACTED_BY skipped.")
        return
    if id_col != "player_id":
        df = df.rename(columns={id_col: "player_id"})

    team_col = next((c for c in ["team", "team_abbr"] if c in df.columns), None)
    if team_col is None:
        logger.warning("contracts lacks team column — CONTRACTED_BY skipped.")
        return
    if team_col != "team":
        df = df.rename(columns={team_col: "team"})

    df = df.dropna(subset=["player_id", "team"])
    records = _to_records(df)

    session.run(
        """
        UNWIND $rows AS row
        MATCH (p:Player {player_id: row.player_id})
        MERGE (t:Team   {abbreviation: row.team})
        MERGE (p)-[r:CONTRACTED_BY {year_signed: toInteger(row.year_signed)}]->(t)
          SET r.cap_hit    = row.cap_hit,
              r.apy        = row.apy,
              r.guaranteed = row.guaranteed,
              r.years      = toInteger(row.years)
        """,
        rows=records,
    )
    logger.info("Merged %s CONTRACTED_BY relationships.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def build_graph() -> None:
    logger.info("=== Building Neo4j Graph ===")

    driver = get_driver()
    with driver.session() as session:
        logger.info("Setting up constraints...")
        create_constraints(session)

        logger.info("Loading Player nodes...")
        _build_player_nodes(session)

        logger.info("Loading Team nodes...")
        _build_team_nodes(session)

        logger.info("Loading Game nodes + HOME_IN/AWAY_IN relationships...")
        _build_game_nodes(session)

        logger.info("Loading Team→Season PLAYED_IN relationships...")
        _build_team_seasons(session)

        logger.info("Loading SNAPPED_IN relationships...")
        _build_snap_relationships(session)

        logger.info("Loading INJURED_DURING relationships...")
        _build_injury_relationships(session)

        logger.info("Loading SELECTED_IN_DRAFT relationships...")
        _build_draft_relationships(session)

        logger.info("Loading CONTRACTED_BY relationships...")
        _build_contract_relationships(session)

    logger.info("Graph build complete.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s [%(name)s] %(message)s",
    )
    build_graph()
