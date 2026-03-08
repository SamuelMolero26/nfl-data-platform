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


@router.get("/player/{name}/neighbors")
def player_neighbors(name: str, depth: int = Query(1, ge=1, le=3)):
    """Return graph neighbors of a player up to `depth` hops."""
    try:
        return {"player": name, "neighbors": gq.get_player_neighbors(name, depth)}
    except Exception as e:
        _neo4j_error(e)


@router.get("/player/{name}/profile")
def player_profile(name: str):
    """Return a player's full graph profile including college and draft info."""
    try:
        result = gq.get_player_profile(name)
        if not result:
            raise HTTPException(status_code=404, detail=f"Player '{name}' not found in graph.")
        return result[0]
    except HTTPException:
        raise
    except Exception as e:
        _neo4j_error(e)


@router.get("/team/{name}/drafted")
def team_draft_history(name: str, year: int | None = Query(None)):
    """Return all players drafted by a team."""
    try:
        return {"team": name, "picks": gq.get_team_draft_history(name, year)}
    except Exception as e:
        _neo4j_error(e)


@router.get("/path")
def shortest_path(
    from_name: str = Query(...),
    to_name: str = Query(...),
):
    """Find the shortest path between any two entities in the graph."""
    try:
        result = gq.shortest_path(from_name, to_name)
        if not result:
            raise HTTPException(status_code=404, detail="No path found between the two entities.")
        return result[0]
    except HTTPException:
        raise
    except Exception as e:
        _neo4j_error(e)


@router.get("/full")
def full_graph(limit: int = Query(500, ge=1, le=2000)):
    """Return all nodes and relationships for graph visualization."""
    try:
        return gq.get_full_graph(limit)
    except Exception as e:
        _neo4j_error(e)


@router.get("/college/{name}/pipeline")
def college_pipeline(name: str):
    """Return all players from a college and their NFL draft outcomes."""
    try:
        return {"college": name, "players": gq.college_to_nfl_pipeline(name)}
    except Exception as e:
        _neo4j_error(e)
