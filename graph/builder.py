import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from neo4j import Session

import config
from db.neo4j_client import get_driver


# ---------------------------------------------------------------------------
# Constraint setup (run once)
# ---------------------------------------------------------------------------

CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Player) REQUIRE p.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Team) REQUIRE t.abbreviation IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:College) REQUIRE c.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (d:DraftClass) REQUIRE d.year IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Season) REQUIRE s.year IS UNIQUE",
]


def create_constraints(session: Session) -> None:
    for stmt in CONSTRAINTS:
        session.run(stmt)
    print("  Constraints created.")


# ---------------------------------------------------------------------------
# Player + College + DraftClass nodes + relationships
# ---------------------------------------------------------------------------

def _build_players(session: Session, df: pd.DataFrame) -> None:
    records = df.where(pd.notnull(df), None).to_dict("records")

    session.run("""
        UNWIND $rows AS row
        MERGE (p:Player {name: row.player_name})
          SET p.position   = row.position,
              p.school     = row.school,
              p.height_in  = row.height_in,
              p.weight_lbs = row.weight_lbs,
              p.forty_yard = row.forty_yard,
              p.vertical   = row.vertical_in,
              p.bench_reps = row.bench_reps,
              p.broad_jump = row.broad_jump_in,
              p.three_cone = row.three_cone,
              p.shuttle    = row.shuttle

        // College node + ATTENDED relationship
        FOREACH (_ IN CASE WHEN row.school IS NOT NULL THEN [1] ELSE [] END |
            MERGE (c:College {name: row.school})
            MERGE (p)-[:ATTENDED]->(c)
        )

        // DraftClass node + COMPETED_IN relationship
        FOREACH (_ IN CASE WHEN row.draft_year IS NOT NULL THEN [1] ELSE [] END |
            MERGE (d:DraftClass {year: toInteger(row.draft_year)})
            MERGE (p)-[:COMPETED_IN]->(d)
        )

        // Team node + DRAFTED_BY relationship
        FOREACH (_ IN CASE WHEN row.draft_team IS NOT NULL THEN [1] ELSE [] END |
            MERGE (t:Team {name: row.draft_team})
            MERGE (p)-[r:DRAFTED_BY]->(t)
              SET r.round = row.draft_round,
                  r.pick  = toInteger(row.draft_pick),
                  r.year  = toInteger(row.draft_year)
        )
    """, rows=records)

    print(f"  Merged {len(records)} Player nodes.")


# ---------------------------------------------------------------------------
# Team + Season nodes + PLAYED_IN relationships
# ---------------------------------------------------------------------------

def _build_team_seasons(session: Session, df: pd.DataFrame) -> None:
    records = df.where(pd.notnull(df), None).to_dict("records")

    session.run("""
        UNWIND $rows AS row
        MERGE (t:Team {abbreviation: row.team})
        MERGE (s:Season {year: row.season})
        MERGE (t)-[r:PLAYED_IN]->(s)
          SET r.wins               = row.wins,
              r.losses             = row.losses,
              r.ties               = row.ties,
              r.points_scored      = row.points_scored,
              r.points_allowed     = row.points_allowed,
              r.score_differential = row.score_differential,
              r.win_pct            = row.win_pct,
              r.point_diff_per_game = row.point_diff_per_game
    """, rows=records)

    print(f"  Merged {len(records)} Team→Season PLAYED_IN relationships.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build_graph() -> None:
    print("=== Building Neo4j Graph ===\n")

    combine = pd.read_parquet(config.STAGED_COMBINE)
    team_stats = pd.read_parquet(config.STAGED_TEAM_STATS)

    driver = get_driver()
    with driver.session() as session:
        print("Setting up constraints...")
        create_constraints(session)

        print("Loading players, colleges, draft classes...")
        _build_players(session, combine)

        print("Loading teams and season records...")
        _build_team_seasons(session, team_stats)

    print("\nGraph build complete.")


if __name__ == "__main__":
    build_graph()
