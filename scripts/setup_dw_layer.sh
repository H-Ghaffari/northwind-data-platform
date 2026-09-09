#!/usr/bin/env bash
# ===========================================================================
# Create the star schema in ClickHouse.
#
# Run explicitly rather than through /docker-entrypoint-initdb.d: that hook
# fires only on a first-time volume and swallows errors, which makes a
# partially-created schema look like a successful start.
#
# Idempotent: every statement is CREATE ... IF NOT EXISTS.
#
# Usage:  ./scripts/setup_dw_layer.sh
# ===========================================================================
set -euo pipefail

cd "$(dirname "$0")/.."
source .env

CONTAINER="northwind_dw"

run_ch_file() {
    echo "  → $1"
    docker compose exec -T "${CONTAINER}" \
        clickhouse-client \
            --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" \
            --multiquery < "sql/$1"
}

run_ch() {
    docker compose exec -T "${CONTAINER}" \
        clickhouse-client \
            --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" \
            --query "$1"
}

echo "==> Checking that ${CONTAINER} is up"
if ! docker compose ps --status running --services | grep -qx "${CONTAINER}"; then
    echo "ERROR: ${CONTAINER} is not running. Start it with: docker compose up -d"
    exit 1
fi

echo "==> Waiting for ClickHouse to accept connections"
for i in $(seq 1 20); do
    if run_ch "SELECT 1" >/dev/null 2>&1; then
        echo "    ready"
        break
    fi
    [ "$i" -eq 20 ] && { echo "ERROR: timed out waiting for ClickHouse"; exit 1; }
    sleep 2
done

echo "==> Creating star schema"
run_ch_file "03_dw/00_create_dw_tables.sql"

echo "==> Creating data lake catalogue"
run_ch_file "03_dw/01_create_lake_tables.sql"

echo "==> Verifying"
run_ch "
SELECT name, engine, partition_key, sorting_key
FROM system.tables
WHERE database = 'NorthwindDW'
ORDER BY engine, name
FORMAT PrettyCompact
"

echo
echo "DW layer ready."
