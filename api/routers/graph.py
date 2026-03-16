from fastapi import APIRouter, HTTPException, Query
from graph import queries as gq

router = APIRouter(prefix="/graph", tags=["graph"])


def _neo4j_error(e: Exception):
    msg = str(e)
    if "Unable to retrieve routing" in msg or "ServiceUnavailable" in msg:
        raise HTTPException(
            status_code=503,
            detail="Neo4j is unavailable. Start it with: docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:5",
        )
    raise HTTPException(status_code=500, detail=msg)


# ---------------------------------------------------------------------------
# Player graph endpoints (player_id-keyed)
# ---------------------------------------------------------------------------


@router.get("/player/{player_id}/neighbors", summary="Graph neighbors of a player")
def player_neighbors(player_id: str, depth: int = Query(1, ge=1, le=3)):
    """Return graph neighbors of a player up to `depth` hops (keyed by gsis_id)."""
    try:
        return {
            "player_id": player_id,
            "neighbors": gq.get_player_neighbors(player_id, depth),
        }
    except Exception as e:
        _neo4j_error(e)


@router.get("/player/{player_id}/profile", summary="Graph profile for a player")
def player_graph_profile(player_id: str):
    """
    Return a player's graph profile including college, draft class,
    and draft value scores (from SELECTED_IN_DRAFT relationship).
    """
    try:
        result = gq.get_player_profile(player_id)
        if not result:
            raise HTTPException(
                status_code=404, detail=f"Player '{player_id}' not found in graph."
            )
        return result[0]
    except HTTPException:
        raise
    except Exception as e:
        _neo4j_error(e)


@router.get("/player/search/{name}", summary="Search graph player by name")
def player_graph_profile_by_name(name: str):
    """
    Search graph for a player by name (CONTAINS match on player_name property).
    Returns up to 10 matches with college and draft info.
    """
    try:
        result = gq.get_player_profile_by_name(name)
        if not result:
            raise HTTPException(
                status_code=404, detail=f"No player matching '{name}' found in graph."
            )
        return {"matches": result}
    except HTTPException:
        raise
    except Exception as e:
        _neo4j_error(e)


@router.get("/player/{player_id}/career", summary="Full career path through graph")
def player_career_path(player_id: str):
    """
    Return a player's full career path through the graph:
    teams (contracted/drafted), snaps per season, injury seasons, and draft info.
    Requires snap_counts and injuries staged data + Stage 2 to have run.
    """
    try:
        return gq.get_player_career_path(player_id)
    except Exception as e:
        _neo4j_error(e)


# ---------------------------------------------------------------------------
# Team graph endpoints
# ---------------------------------------------------------------------------


@router.get("/team/{abbr}/drafted", summary="Players drafted by a team")
def team_draft_history(abbr: str, year: int | None = Query(None)):
    """Return all players drafted by a team, with draft value scores."""
    try:
        return {
            "team": abbr.upper(),
            "picks": gq.get_team_draft_history(abbr.upper(), year),
        }
    except Exception as e:
        _neo4j_error(e)


@router.get("/team/{abbr}/roster", summary="Team roster from graph")
def team_roster(
    abbr: str,
    season: int | None = Query(
        None, description="Filter contracts signed on or before this season"
    ),
):
    """
    Return players contracted to a team from the graph (CONTRACTED_BY edges).
    Optionally filter to contracts signed on or before `season`.
    """
    try:
        players = gq.get_team_roster(abbr.upper(), season)
        if not players:
            raise HTTPException(
                status_code=404,
                detail=f"No roster data for team '{abbr}' in graph. "
                "Ensure contracts are staged and Stage 2 + graph builder have run.",
            )
        return {"team": abbr.upper(), "season": season, "roster": players}
    except HTTPException:
        raise
    except Exception as e:
        _neo4j_error(e)


# ---------------------------------------------------------------------------
# Path / exploration
# ---------------------------------------------------------------------------


@router.get("/path", summary="Shortest path between two entities")
def shortest_path(
    from_id: str = Query(..., description="player_id, team abbreviation, or name"),
    to_id: str = Query(..., description="player_id, team abbreviation, or name"),
):
    """Find the shortest path between any two entities in the graph."""
    try:
        result = gq.shortest_path(from_id, to_id)
        if not result:
            raise HTTPException(
                status_code=404, detail="No path found between the two entities."
            )
        return result[0]
    except HTTPException:
        raise
    except Exception as e:
        _neo4j_error(e)


@router.get("/college/{name}/pipeline", summary="College → NFL pipeline")
def college_pipeline(name: str):
    """Return all players from a college and their NFL draft outcomes + value scores."""
    try:
        return {"college": name, "players": gq.college_to_nfl_pipeline(name)}
    except Exception as e:
        _neo4j_error(e)


@router.get("/full", summary="Full graph for visualization")
def full_graph(limit: int = Query(500, ge=1, le=2000)):
    """Return all nodes and relationships for graph visualization."""
    try:
        return gq.get_full_graph(limit)
    except Exception as e:
        _neo4j_error(e)
