from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import db.duckdb_client as duckdb_client

router = APIRouter(prefix="/query", tags=["query"])


class QueryRequest(BaseModel):
    sql: str


@router.post("")
def run_query(req: QueryRequest):
    """Execute a read-only SQL query against the data lake."""
    try:
        df = duckdb_client.execute(req.sql)
        return {
            "rows": duckdb_client.df_to_records(df),
            "columns": df.columns.tolist(),
            "count": len(df),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables")
def list_tables():
    """List all available virtual tables."""
    return {"tables": duckdb_client.list_tables()}
