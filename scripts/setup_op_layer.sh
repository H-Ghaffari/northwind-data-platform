#!/usr/bin/env bash
# ===========================================================================
# Build the OP layer from nothing.
#
#   1. Northwind          — the operational source database
#   2. instnwnd.sql       — Microsoft's schema and sample data
#   3. ETL_Settings       — the pipeline's own bookkeeping
#
# Idempotent: re-running rebuilds only what is missing.
#
# Usage:  ./scripts/setup_op_layer.sh
# ===========================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

# --- Configuration ---------------------------------------------------------
source .env
CONTAINER="northwind_op"
SQLCMD="/opt/mssql-tools18/bin/sqlcmd"
SA_PASSWORD="${MSSQL_SA_PASSWORD}"

run_sql_file() {
    local file="$1"
    local database="${2:-master}"
    echo "  → ${file}  (db: ${database})"
    docker compose exec -T "${CONTAINER}" "${SQLCMD}" \
        -S localhost -U sa -P "${SA_PASSWORD}" -C \
        -d "${database}" -b -i "/opt/sql/${file}"
}

run_sql() {
    docker compose exec -T "${CONTAINER}" "${SQLCMD}" \
        -S localhost -U sa -P "${SA_PASSWORD}" -C -b -Q "$1"
}

# --- Preflight -------------------------------------------------------------
echo "==> Checking that ${CONTAINER} is up"
if ! docker compose ps --status running --services | grep -qx "${CONTAINER}"; then
    echo "ERROR: ${CONTAINER} is not running. Start it with: docker compose up -d"
    exit 1
fi

echo "==> Waiting for SQL Server to accept connections"
for i in $(seq 1 30); do
    if run_sql "SELECT 1" >/dev/null 2>&1; then
        echo "    ready"
        break
    fi
    [ "$i" -eq 30 ] && { echo "ERROR: timed out waiting for SQL Server"; exit 1; }
    sleep 3
done

# --- Step 1: create the database ------------------------------------------
echo "==> Creating database Northwind"
run_sql_file "01_op/00_create_database.sql"

# --- Step 2: load schema and data -----------------------------------------
echo "==> Loading Northwind schema and sample data"
echo "    (this takes a minute — 830 orders, 2155 order details)"
run_sql_file "01_op/instnwnd.sql" "Northwind"

# --- Step 3: ETL bookkeeping ----------------------------------------------
echo "==> Creating ETL_Settings"
run_sql_file "04_etl_settings/00_create_etl_settings.sql"

# --- Verify ----------------------------------------------------------------
echo "==> Verifying"
run_sql "
SET NOCOUNT ON;
USE Northwind;
SELECT
    (SELECT COUNT(*) FROM Customers)      AS customers,
    (SELECT COUNT(*) FROM Employees)      AS employees,
    (SELECT COUNT(*) FROM Products)       AS products,
    (SELECT COUNT(*) FROM Orders)         AS orders,
    (SELECT COUNT(*) FROM [Order Details]) AS order_details;
"

echo
echo "OP layer ready."
echo "  Northwind      — 13 tables, sample data loaded"
echo "  ETL_Settings   — CDC_State seeded for Orders and OrderDetails"
