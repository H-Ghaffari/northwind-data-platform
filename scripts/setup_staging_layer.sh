#!/usr/bin/env bash
# ===========================================================================
# Create the staging schema in PostgreSQL.
#
# Idempotent: every statement is CREATE ... IF NOT EXISTS, so re-running
# leaves an existing schema untouched.
#
# Usage:  ./scripts/setup_staging_layer.sh
# ===========================================================================
set -euo pipefail

cd "$(dirname "$0")/.."
source .env

CONTAINER="northwind_staging"

run_psql_file() {
    echo "  → $1"
    docker compose exec -T "${CONTAINER}" \
        psql -v ON_ERROR_STOP=1 \
             -U "${STAGING_USER}" -d "${STAGING_DB}" \
             -f "/opt/sql/$1"
}

run_psql() {
    docker compose exec -T "${CONTAINER}" \
        psql -v ON_ERROR_STOP=1 -U "${STAGING_USER}" -d "${STAGING_DB}" -c "$1"
}

echo "==> Checking that ${CONTAINER} is up"
if ! docker compose ps --status running --services | grep -qx "${CONTAINER}"; then
    echo "ERROR: ${CONTAINER} is not running. Start it with: docker compose up -d"
    exit 1
fi

echo "==> Waiting for PostgreSQL to accept connections"
for i in $(seq 1 20); do
    if run_psql "SELECT 1" >/dev/null 2>&1; then
        echo "    ready"
        break
    fi
    [ "$i" -eq 20 ] && { echo "ERROR: timed out waiting for PostgreSQL"; exit 1; }
    sleep 2
done

echo "==> Creating staging tables"
run_psql_file "02_staging/00_create_staging_tables.sql"

echo "==> Verifying"
run_psql "
SELECT table_name,
       (SELECT count(*) FROM information_schema.columns c
         WHERE c.table_name = t.table_name) AS columns
FROM information_schema.tables t
WHERE table_schema = 'public' AND table_name LIKE 'staging_%'
ORDER BY table_name;
"

echo
echo "Staging layer ready."
