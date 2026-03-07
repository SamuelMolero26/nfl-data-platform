from fastapi import APIRouter, HTTPException, Query
import db.duckdb_client as duckdb_client

router = APIRouter(prefix="/players", tags=["players"])


@router.get("")
def list_players(
    position: str | None = Query(None),
    school: str | None = Query(None),
    draft_team: str | None = Query(None),
    drafted_only: bool = Query(False),
    limit: int = Query(100, le=500),
    offset: int = Query(0),
):
    """List players with optional filters."""
    filters = []
    if position:
        filters.append(f"position = '{position}'")
    if school:
        filters.append(f"school ILIKE '%{school}%'")
    if draft_team:
        filters.append(f"draft_team ILIKE '%{draft_team}%'")
    if drafted_only:
        filters.append("draft_team IS NOT NULL")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT player_name, position, school, height_in, weight_lbs,
               forty_yard, vertical_in, bench_reps, broad_jump_in,
               three_cone, shuttle, draft_team, draft_round, draft_pick, draft_year
        FROM players
        {where}
        ORDER BY player_name
        LIMIT {limit} OFFSET {offset}
    """
    df = duckdb_client.execute(sql)
    return {"players": duckdb_client.df_to_records(df), "count": len(df)}


@router.get("/{name}")
def get_player(name: str):
    """Get a single player profile by name."""
    sql = f"SELECT * FROM players WHERE player_name ILIKE '%{name}%' LIMIT 10"
    df = duckdb_client.execute(sql)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No player found matching '{name}'")
    return {"players": duckdb_client.df_to_records(df)}
