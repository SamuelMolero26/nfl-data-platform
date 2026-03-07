#!/bin/bash
set -e

echo "==> Waiting for Neo4j to be ready..."
until nc -z "${NEO4J_HOST:-neo4j}" 7687; do
  sleep 2
done
echo "==> Neo4j is up."

# Give Neo4j a few extra seconds to finish initializing after the port opens
sleep 5

CURATED_DIR="/app/lake/curated"
PLAYER_PARQUET="$CURATED_DIR/player_profiles.parquet"
TEAM_PARQUET="$CURATED_DIR/team_performance.parquet"

if [ ! -f "$PLAYER_PARQUET" ] || [ ! -f "$TEAM_PARQUET" ]; then
  echo "==> Curated Parquet files not found. Running ingestion pipeline..."
  python ingestion/pipeline.py
  echo "==> Ingestion complete."
else
  echo "==> Curated Parquet files found, skipping ingestion."
fi

echo "==> Populating Neo4j graph (idempotent)..."
python graph/builder.py
echo "==> Graph ready."

echo "==> Starting API..."
exec uvicorn api.main:app --host 0.0.0.0 --port 8000
