#!/bin/bash
set -e

echo "==> Waiting for Neo4j to be ready..."
until nc -z "${NEO4J_HOST:-neo4j}" 7687; do
  sleep 2
done
echo "==> Neo4j is up."

# Give Neo4j a few extra seconds to finish initializing after the port opens
sleep 5

echo "==> Running ingestion pipeline..."
python ingestion/pipeline.py
echo "==> Ingestion complete."

echo "==> Populating Neo4j graph (idempotent)..."
python graph/builder.py
echo "==> Graph ready."

echo "==> Starting API..."
exec uvicorn api.main:app --host 0.0.0.0 --port 8000
