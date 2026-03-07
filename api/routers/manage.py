"""
Data management endpoints — browse, preview, and clean Parquet datasets.

Cleaning operations are applied in-memory and written back to the staged layer.
The curated layer is NOT automatically updated — re-run the pipeline after cleaning.
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from pathlib import Path
from typing import Literal
import pandas as pd
import config
import db.duckdb_client as duckdb_client
from db.duckdb_client import df_to_records

router = APIRouter(prefix="/manage", tags=["manage"])

_LAKE_ZONES = {
    "staged": config.LAKE_STAGED_DIR,
    "curated": config.LAKE_CURATED_DIR,
}


def _find_parquet(dataset: str) -> Path:
    """Resolve a dataset name to its Parquet path across all lake zones."""
    for zone_path in _LAKE_ZONES.values():
        for f in zone_path.rglob("*.parquet"):
            if f.stem == dataset or f.name == dataset:
                return f
    raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")


@router.get("/datasets")
def list_datasets():
    """List all Parquet files across staged and curated lake zones."""
    result = {}
    for zone, path in _LAKE_ZONES.items():
        files = []
        for f in sorted(path.rglob("*.parquet")):
            df = pd.read_parquet(f)
            files.append({
                "name": f.stem,
                "path": str(f.relative_to(config.BASE_DIR)),
                "rows": len(df),
                "columns": len(df.columns),
                "size_kb": round(f.stat().st_size / 1024, 1),
            })
        result[zone] = files
    return result


@router.get("/preview/{dataset}")
def preview_dataset(dataset: str, rows: int = Query(20, le=200)):
    """Return the first N rows and schema info for a dataset."""
    path = _find_parquet(dataset)
    df = pd.read_parquet(path)

    schema = []
    for col in df.columns:
        schema.append({
            "column": col,
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isna().sum()),
            "null_pct": round(df[col].isna().mean() * 100, 1),
        })

    return {
        "dataset": dataset,
        "total_rows": len(df),
        "columns": len(df.columns),
        "schema": schema,
        "preview": df_to_records(df.head(rows)),
    }


# ---------------------------------------------------------------------------
# Cleaning operations
# ---------------------------------------------------------------------------

class DropColumnsRequest(BaseModel):
    dataset: str
    columns: list[str]


class FillNullsRequest(BaseModel):
    dataset: str
    column: str
    strategy: Literal["mean", "median", "mode", "value"]
    value: str | float | None = None


class RenameColumnRequest(BaseModel):
    dataset: str
    rename_map: dict[str, str]


class FilterRowsRequest(BaseModel):
    dataset: str
    sql_filter: str  # WHERE clause fragment, e.g. "forty_yard < 5.0"


@router.post("/clean/drop-columns")
def drop_columns(req: DropColumnsRequest):
    """Remove columns from a staged dataset."""
    path = _find_parquet(req.dataset)
    df = pd.read_parquet(path)
    missing = [c for c in req.columns if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Columns not found: {missing}")
    df = df.drop(columns=req.columns)
    df.to_parquet(path, index=False)
    duckdb_client.reset()
    return {"dropped": req.columns, "remaining_columns": df.columns.tolist()}


@router.post("/clean/fill-nulls")
def fill_nulls(req: FillNullsRequest):
    """Fill null values in a column using a strategy or fixed value."""
    path = _find_parquet(req.dataset)
    df = pd.read_parquet(path)
    if req.column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{req.column}' not found.")

    col = df[req.column]
    before = int(col.isna().sum())

    if req.strategy == "mean":
        df[req.column] = col.fillna(col.mean())
    elif req.strategy == "median":
        df[req.column] = col.fillna(col.median())
    elif req.strategy == "mode":
        df[req.column] = col.fillna(col.mode().iloc[0])
    elif req.strategy == "value":
        if req.value is None:
            raise HTTPException(status_code=400, detail="Provide 'value' when strategy is 'value'.")
        df[req.column] = col.fillna(req.value)

    df.to_parquet(path, index=False)
    duckdb_client.reset()
    return {"column": req.column, "nulls_filled": before - int(df[req.column].isna().sum())}


@router.post("/clean/rename")
def rename_columns(req: RenameColumnRequest):
    """Rename one or more columns in a staged dataset."""
    path = _find_parquet(req.dataset)
    df = pd.read_parquet(path)
    df = df.rename(columns=req.rename_map)
    df.to_parquet(path, index=False)
    duckdb_client.reset()
    return {"renamed": req.rename_map, "columns": df.columns.tolist()}


@router.post("/clean/filter-rows")
def filter_rows(req: FilterRowsRequest):
    """Remove rows that do NOT match the SQL filter (keep matching rows only)."""
    path = _find_parquet(req.dataset)
    df = pd.read_parquet(path)
    before = len(df)
    # Execute the filter via DuckDB for SQL compatibility
    result = duckdb_client.execute(
        f"SELECT * FROM read_parquet('{path}') WHERE {req.sql_filter}"
    )
    result.to_parquet(path, index=False)
    duckdb_client.reset()
    return {"rows_before": before, "rows_after": len(result), "removed": before - len(result)}
