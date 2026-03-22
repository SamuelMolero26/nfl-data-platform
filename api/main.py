from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from api.routers import query, players, teams, graph, manage
import db.duckdb_client as duckdb_client

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DuckDB connection
    duckdb_client.initialize_duckdb()
    yield
    # Cleanup if needed (e.g., close connections)
    duckdb_client.close_duckdb()


app = FastAPI(
    title="NFL Data Platform",
    description="Data lake API with SQL queries, graph traversal, and data management.",
    version="0.1.0",
    lifespan=lifespan
)

# Routers
app.include_router(query.router)
app.include_router(players.router)
app.include_router(teams.router)
app.include_router(graph.router)
app.include_router(manage.router)

# Serve the management UI from /
UI_DIR = Path(__file__).parent.parent / "ui"


@app.get("/", include_in_schema=False)
def serve_ui():
    return FileResponse(UI_DIR / "index.html")


@app.get("/health")
def health():
    return {"status": "ok"}
