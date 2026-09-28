#!/usr/bin/env bash
#
# Creates the LMS Iceberg namespace and output tables through the REST catalog.
#
# This is intentionally separate from docker-compose startup: table creation is a demo choice,
# while starting an empty catalog should remain harmless. It is idempotent and treats the REST
# catalog's conflict response as success, so it can safely be run before every replay.
#
#   ./demos/iceberg/init-schema.sh
#   ICEBERG_CATALOG_URI=http://localhost:8181 ./demos/iceberg/init-schema.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CATALOG_URI="${ICEBERG_CATALOG_URI:-http://localhost:8181}"
CATALOG_URI="${CATALOG_URI%/}"
NAMESPACE_PATH="lms%1Fanalytics"
SCHEMA_DIR="$ROOT/demos/iceberg"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required to initialise the Iceberg REST catalog" >&2
  exit 1
fi

RESPONSE_FILE="$(mktemp)"
trap 'rm -f "$RESPONSE_FILE"' EXIT

echo "waiting for Iceberg REST catalog at $CATALOG_URI"
catalog_ready=false
for _ in {1..30}; do
  if [ "$(curl -sS -o /dev/null -w '%{http_code}' "$CATALOG_URI/v1/config" || true)" = "200" ]; then
    catalog_ready=true
    break
  fi
  sleep 1
done
if [ "$catalog_ready" != "true" ]; then
  echo "Iceberg REST catalog did not become ready within 30 seconds: $CATALOG_URI" >&2
  exit 1
fi

post_idempotent() {
  local url="$1"
  local payload="$2"
  local status
  status="$(curl -sS -o "$RESPONSE_FILE" -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' --data-binary "@$payload" "$url")"
  case "$status" in
    2??|409) ;;
    *)
      echo "REST catalog request failed ($status): $url" >&2
      cat "$RESPONSE_FILE" >&2
      exit 1
      ;;
  esac
}

echo "initialising Iceberg namespace lms.analytics at $CATALOG_URI"
post_idempotent "$CATALOG_URI/v1/namespaces" "$SCHEMA_DIR/namespace.json"

create_table() {
  local name="$1"
  local payload="$2"
  post_idempotent "$CATALOG_URI/v1/namespaces/$NAMESPACE_PATH/tables" "$payload"
  echo "table ready: lms.analytics.$name"
}

create_table browse_sessions \
  "$SCHEMA_DIR/browse_sessions.table.json"
create_table click_conversions \
  "$SCHEMA_DIR/click_conversions.table.json"

echo "Iceberg schema ready"
