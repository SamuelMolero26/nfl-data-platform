import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.neo4j_client import run_query


# Helper <- for sanity check
def _sanitize(obj):
    """Recursively replace NaN/Inf floats with None for JSON safety."""
    if isinstance(obj, float) and (
        obj != obj or obj == float("inf") or obj == float("-inf")
    ):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


# ---------------------------------------------------------------------------
# Player lookups
# Note: Player nodes are now keyed on player_id (gsis_id).
# Name-based lookups search the player_name property.
# ---------------------------------------------------------------------------


def get_player_neighbors(player_id: str, depth: int = 1) -> list[dict]:
    """
    Return all nodes within `depth` hops of a player (keyed by player_id).
    Traverses all relationship types.
    """
    cypher = """
        MATCH path = (p:Player {player_id: $player_id})-[*1..$depth]-(neighbor)
        RETURN DISTINCT
            labels(neighbor)[0] AS type,
            neighbor            AS node
        LIMIT 100
    """
    return run_query(cypher, {"player_id": player_id, "depth": depth})


def get_player_profile(player_id: str) -> list[dict]:
    """Return full node properties for a player including college and draft info."""
    cypher = """
        MATCH (p:Player {player_id: $player_id})
        OPTIONAL MATCH (p)-[:ATTENDED]->(c:College)
        OPTIONAL MATCH (p)-[dr:SELECTED_IN_DRAFT]->(d:DraftClass)
        OPTIONAL MATCH (p)-[db:DRAFTED_BY]->(dt:Team)
        RETURN p                        AS player,
               c.name                  AS college,
               dt.abbreviation         AS draft_team,
               dr.round                AS round,
               dr.pick                 AS pick,
               d.year                  AS draft_year,
               dr.draft_value_score    AS draft_value_score,
               dr.draft_value_percentile AS draft_value_percentile
    """
    return run_query(cypher, {"player_id": player_id})


def get_player_profile_by_name(player_name: str) -> list[dict]:
    """Return player profile searching by player_name property (fuzzy CONTAINS)."""
    cypher = """
        MATCH (p:Player)
        WHERE p.player_name CONTAINS $name
        OPTIONAL MATCH (p)-[:ATTENDED]->(c:College)
        OPTIONAL MATCH (p)-[dr:SELECTED_IN_DRAFT]->(d:DraftClass)
        OPTIONAL MATCH (p)-[db:DRAFTED_BY]->(dt:Team)
        RETURN p                        AS player,
               c.name                  AS college,
               dt.abbreviation         AS draft_team,
               dr.round                AS round,
               dr.pick                 AS pick,
               d.year                  AS draft_year,
               dr.draft_value_score    AS draft_value_score,
               dr.draft_value_percentile AS draft_value_percentile
        LIMIT 10
    """
    return run_query(cypher, {"name": player_name})


def get_player_career_path(player_id: str) -> dict:
    """
    Return a player's full career path through the graph:
    - Teams they were drafted by or contracted to
    - Seasons they were active
    - Draft class
    - Games they snapped in (summary counts)
    - Injury seasons
    """
    teams_cypher = """
        MATCH (p:Player {player_id: $player_id})
        OPTIONAL MATCH (p)-[c:CONTRACTED_BY]->(t:Team)
        OPTIONAL MATCH (p)-[db:DRAFTED_BY]->(dt:Team)
        RETURN DISTINCT
            coalesce(t.abbreviation, dt.abbreviation) AS team,
            coalesce(t.full_name, dt.full_name)       AS team_name,
            c.year_signed                              AS year_signed,
            c.apy                                      AS apy,
            c.cap_hit                                  AS cap_hit,
            db.year                                    AS drafted_year,
            db.round                                   AS drafted_round,
            db.pick                                    AS drafted_pick
        ORDER BY year_signed
    """

    seasons_cypher = """
        MATCH (p:Player {player_id: $player_id})-[:INJURED_DURING]->(s:Season)
        RETURN DISTINCT s.year AS season, count(*) AS injury_events
        ORDER BY season
    """

    snaps_cypher = """
        MATCH (p:Player {player_id: $player_id})-[r:SNAPPED_IN]->(g:Game)
        RETURN g.season AS season,
               count(g) AS games,
               sum(r.offense_snaps) AS total_offense_snaps
        ORDER BY season
    """

    draft_cypher = """
        MATCH (p:Player {player_id: $player_id})-[r:SELECTED_IN_DRAFT]->(d:DraftClass)
        RETURN d.year AS draft_year, r.round AS round, r.pick AS pick,
               r.car_av AS car_av, r.draft_value_score AS draft_value_score,
               r.draft_value_percentile AS draft_value_percentile
    """

    teams = _sanitize(run_query(teams_cypher, {"player_id": player_id}))
    seasons = _sanitize(run_query(seasons_cypher, {"player_id": player_id}))
    snaps = _sanitize(run_query(snaps_cypher, {"player_id": player_id}))
    draft = _sanitize(run_query(draft_cypher, {"player_id": player_id}))

    return {
        "player_id": player_id,
        "teams": teams,
        "snaps_by_season": snaps,
        "injury_seasons": seasons,
        "draft": draft[0] if draft else None,
    }


# ---------------------------------------------------------------------------
# Team queries
# ---------------------------------------------------------------------------


def get_team_draft_history(team_abbr: str, year: int | None = None) -> list[dict]:
    """Return all players drafted by a team, optionally filtered by year."""
    if year:
        cypher = """
            MATCH (p:Player)-[r:SELECTED_IN_DRAFT]->(d:DraftClass)
            WHERE r.team = $team AND d.year = $year
            RETURN p.player_name AS player, p.position AS position,
                   r.round AS round, r.pick AS pick, d.year AS year,
                   r.draft_value_score AS draft_value_score,
                   r.draft_value_percentile AS draft_value_percentile
            ORDER BY r.pick
        """
        return run_query(cypher, {"team": team_abbr, "year": year})
    else:
        cypher = """
            MATCH (p:Player)-[r:SELECTED_IN_DRAFT]->(d:DraftClass)
            WHERE r.team = $team
            RETURN p.player_name AS player, p.position AS position,
                   r.round AS round, r.pick AS pick, d.year AS year,
                   r.draft_value_score AS draft_value_score,
                   r.draft_value_percentile AS draft_value_percentile
            ORDER BY d.year DESC, r.pick
        """
        return run_query(cypher, {"team": team_abbr})


def get_team_roster(team_abbr: str, season: int | None = None) -> list[dict]:
    """
    Return players contracted to a team.
    If season provided, filter by year_signed <= season.
    """
    if season:
        cypher = """
            MATCH (p:Player)-[c:CONTRACTED_BY]->(t:Team {abbreviation: $team})
            WHERE c.year_signed <= $season
            RETURN p.player_id   AS player_id,
                   p.player_name AS player_name,
                   p.position    AS position,
                   c.year_signed AS year_signed,
                   c.apy         AS apy,
                   c.cap_hit     AS cap_hit,
                   c.guaranteed  AS guaranteed
            ORDER BY c.year_signed DESC, p.position
        """
        return run_query(cypher, {"team": team_abbr, "season": season})
    else:
        cypher = """
            MATCH (p:Player)-[c:CONTRACTED_BY]->(t:Team {abbreviation: $team})
            RETURN p.player_id   AS player_id,
                   p.player_name AS player_name,
                   p.position    AS position,
                   c.year_signed AS year_signed,
                   c.apy         AS apy,
                   c.cap_hit     AS cap_hit,
                   c.guaranteed  AS guaranteed
            ORDER BY c.year_signed DESC, p.position
        """
        return run_query(cypher, {"team": team_abbr})


# ---------------------------------------------------------------------------
# Path / full graph (unchanged, updated to use player_id key)
# ---------------------------------------------------------------------------


def shortest_path(from_id: str, to_id: str) -> list[dict]:
    """
    Find the shortest path between two player_ids (or any named entity) in the graph.
    Tries player_id first, falls back to name-based search.
    """
    cypher = """
        MATCH (a)
        WHERE a.player_id = $from_id
            OR a.player_name = $from_id
            OR a.full_name = $from_id
            OR a.abbreviation = $from_id
        WITH a
        MATCH (b)
        WHERE b.player_id = $to_id
            OR b.player_name = $to_id
            OR b.full_name = $to_id
            OR b.abbreviation = $to_id
        MATCH path = shortestPath((a)-[*..6]-(b))
        RETURN [n IN nodes(path) | {
            labels: labels(n),
            id: coalesce(n.player_id, n.player_name, n.full_name, n.abbreviation, toString(n.year))
        }] AS path_nodes,
               length(path) AS hops
    """
    return run_query(cypher, {"from_id": from_id, "to_id": to_id})


def college_to_nfl_pipeline(college_name: str) -> list[dict]:
    """Return all players from a college and their draft outcomes."""
    cypher = """
        MATCH (p:Player)-[:ATTENDED]->(c:College {name: $college})
        OPTIONAL MATCH (p)-[r:SELECTED_IN_DRAFT]->(d:DraftClass)
        RETURN p.player_name        AS player,
               p.position          AS position,
               r.team              AS drafted_by,
               r.round             AS round,
               r.pick              AS pick,
               d.year              AS year,
               r.draft_value_score AS draft_value_score
        ORDER BY d.year DESC, r.pick
    """
    return run_query(cypher, {"college": college_name})


def get_full_graph(limit: int = 500) -> dict:
    """Return all nodes and relationships for graph visualization."""
    nodes_cypher = """
        MATCH (n)
        RETURN id(n) AS id, labels(n)[0] AS type, properties(n) AS props
        LIMIT $limit
    """
    rels_cypher = """
        MATCH (a)-[r]->(b)
        RETURN id(a) AS source, id(b) AS target, type(r) AS type, properties(r) AS props
        LIMIT $limit
    """
    nodes = _sanitize(run_query(nodes_cypher, {"limit": limit}))
    edges = _sanitize(run_query(rels_cypher, {"limit": limit}))
    return {"nodes": nodes, "edges": edges}
