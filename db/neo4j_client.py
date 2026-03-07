
from neo4j import GraphDatabase, Driver
import config

_driver: Driver | None = None


def get_driver() -> Driver:
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            config.NEO4J_URI,
            auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
        )
    return _driver

def run_query(cypher: str, params: dict = None) -> list[dict]:
    """Execute a Cypher query and return results as a list of dicts."""
    params = params or {}
    with get_driver().session() as session:
        result = session.run(cypher, **params)
        return [record.data() for record in result]


def close() -> None:
    global _driver
    if _driver:
        _driver.close()
        _driver = None
