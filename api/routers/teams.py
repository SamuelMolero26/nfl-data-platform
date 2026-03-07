from fastapi import APIRouter, HTTPException, Query
import db.duckdb_client as duckdb_client

router = APIRouter(prefix="/teams", tags=["teams"])


@router.get("")
def list_teams():
    """List all unique team abbreviations in the dataset."""
    df = duckdb_client.execute("SELECT DISTINCT team FROM team_stats ORDER BY team")
    return {"teams": df["team"].tolist()}


@router.get("/{abbr}/stats")
def get_team_stats(
    abbr: str,
    season_from: int | None = Query(None),
    season_to: int | None = Query(None),
):
    """Return season stats for a team, optionally filtered by year range."""
    filters = [f"team = '{abbr.upper()}'"]
    if season_from:
        filters.append(f"season >= {season_from}")
    if season_to:
        filters.append(f"season <= {season_to}")

    where = "WHERE " + " AND ".join(filters)
    sql = f"""
        SELECT season, team, wins, losses, ties, win_pct,
               points_scored, points_allowed, score_differential, point_diff_per_game,
               offense_total_yards_gained_pass, offense_total_yards_gained_run,
               defense_total_yards_gained_pass, defense_total_yards_gained_run
        FROM team_stats
        {where}
        ORDER BY season DESC
    """
    df = duckdb_client.execute(sql)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No stats found for team '{abbr}'")
    return {"team": abbr.upper(), "seasons": duckdb_client.df_to_records(df)}
