import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.neo4j_client import run_query


def get_player_neighbors(player_name: str, depth: int = 1) -> list[dict]:
    """
    Return all nodes within `depth` hops of a player.
    Traverses: ATTENDED, COMPETED_IN, DRAFTED_BY relationships.
    """
    cypher = """
        MATCH path = (p:Player {name: $name})-[*1..$depth]-(neighbor)
        RETURN DISTINCT
            labels(neighbor)[0] AS type,
            neighbor            AS node
        LIMIT 100
    """
    return run_query(cypher, {"name": player_name, "depth": depth})


def get_team_draft_history(team_name: str, year: int | None = None) -> list[dict]:
    """Return all players drafted by a team, optionally filtered by year."""
    if year:
        cypher = """
            MATCH (p:Player)-[r:DRAFTED_BY]->(t:Team {name: $team})
            WHERE r.year = $year
            RETURN p.name AS player, p.position AS position,
                   r.round AS round, r.pick AS pick, r.year AS year
            ORDER BY r.pick
        """
        return run_query(cypher, {"team": team_name, "year": year})
    else:
        cypher = """
            MATCH (p:Player)-[r:DRAFTED_BY]->(t:Team {name: $team})
            RETURN p.name AS player, p.position AS position,
                   r.round AS round, r.pick AS pick, r.year AS year
            ORDER BY r.year DESC, r.pick
        """
        return run_query(cypher, {"team": team_name})


def shortest_path(from_name: str, to_name: str) -> list[dict]:
    """
    Find the shortest path between any two named entities (Player, Team, College).
    Returns nodes and relationships along the path.
    """
    cypher = """
        MATCH (a {name: $from_name}), (b {name: $to_name}),
              path = shortestPath((a)-[*..6]-(b))
        RETURN [n IN nodes(path) | {labels: labels(n), name: coalesce(n.name, n.abbreviation, toString(n.year))}] AS path_nodes,
               length(path) AS hops
    """
    return run_query(cypher, {"from_name": from_name, "to_name": to_name})


def college_to_nfl_pipeline(college_name: str) -> list[dict]:
    """Return all players from a college and their draft outcomes."""
    cypher = """
        MATCH (p:Player)-[:ATTENDED]->(c:College {name: $college})
        OPTIONAL MATCH (p)-[r:DRAFTED_BY]->(t:Team)
        RETURN p.name      AS player,
               p.position  AS position,
               t.name      AS drafted_by,
               r.round     AS round,
               r.pick      AS pick,
               r.year      AS year
        ORDER BY r.year DESC, r.pick
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
    nodes = run_query(nodes_cypher, {"limit": limit})
    edges = run_query(rels_cypher, {"limit": limit})
    return {"nodes": nodes, "edges": edges}


def get_player_profile(player_name: str) -> list[dict]:
    """Return full node properties for a player."""
    cypher = """
        MATCH (p:Player {name: $name})
        OPTIONAL MATCH (p)-[:ATTENDED]->(c:College)
        OPTIONAL MATCH (p)-[r:DRAFTED_BY]->(t:Team)
        RETURN p         AS player,
               c.name    AS college,
               t.name    AS draft_team,
               r.round   AS round,
               r.pick    AS pick,
               r.year    AS year
    """
    return run_query(cypher, {"name": player_name})
