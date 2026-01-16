#!/usr/bin/env bash
set -e

echo "=== Superset init ==="

superset db upgrade

superset fab create-admin \
  --username admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@superset.com \
  --password admin || true

superset init

echo "=== Import dashboards ==="
superset import-dashboards \
  --path /app/superset_data.zip \
  --username admin || true

echo "=== Start server ==="
exec /usr/bin/run-server.sh