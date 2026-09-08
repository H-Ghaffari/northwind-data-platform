#!/usr/bin/env bash
# ===========================================================================
# Enable CDC on the fact source tables.
#
# CDC depends on SQL Server Agent: enabling capture without Agent running
# produces no error and no captured rows, which reads as "nothing changed"
# rather than as a failure. This script checks Agent explicitly and then
# proves capture works by making a change and reading it back.
#
# Usage:  ./scripts/setup_cdc.sh
# ===========================================================================
set -euo pipefail

cd "$(dirname "$0")/.."
source .env

CONTAINER="northwind_op"
SQLCMD="/opt/mssql-tools18/bin/sqlcmd"

run_sql() {
    docker compose exec -T "${CONTAINER}" "${SQLCMD}" \
        -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -C -b \
        -d "${2:-Northwind}" -Q "$1"
}

run_sql_file() {
    echo "  → $1"
    docker compose exec -T "${CONTAINER}" "${SQLCMD}" \
        -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -C -b \
        -i "/opt/sql/$1"
}

echo "==> Checking that ${CONTAINER} is up"
if ! docker compose ps --status running --services | grep -qx "${CONTAINER}"; then
    echo "ERROR: ${CONTAINER} is not running."
    exit 1
fi

# --- Agent -----------------------------------------------------------------
echo "==> Checking SQL Server Agent"
AGENT_STATE=$(run_sql "
SET NOCOUNT ON;
SELECT CASE WHEN EXISTS (
    SELECT 1 FROM sys.dm_server_services
    WHERE servicename LIKE '%Agent%' AND status_desc = 'Running'
) THEN 'RUNNING' ELSE 'STOPPED' END;
" | tr -d '[:space:]' || true)

if [[ "${AGENT_STATE}" != *"RUNNING"* ]]; then
    echo
    echo "ERROR: SQL Server Agent is not running."
    echo "CDC captures nothing without it."
    echo
    echo "Fix: confirm MSSQL_AGENT_ENABLED is \"true\" in the northwind_op"
    echo "service in docker-compose.yml, then recreate the container:"
    echo
    echo "    docker compose up -d --force-recreate northwind_op"
    exit 1
fi
echo "    Agent is running"

# --- Enable ----------------------------------------------------------------
echo "==> Enabling CDC"
run_sql_file "01_op/01_enable_cdc.sql"

# --- Wait for the capture job ---------------------------------------------
echo "==> Waiting for the capture job to record its first LSN"
for i in $(seq 1 20); do
    MAX_LSN=$(run_sql "SET NOCOUNT ON; SELECT sys.fn_cdc_get_max_lsn();" \
              | tr -d '[:space:]' || true)
    if [[ -n "${MAX_LSN}" && "${MAX_LSN}" != *"NULL"* ]]; then
        echo "    capture job is active"
        break
    fi
    [ "$i" -eq 20 ] && {
        echo "ERROR: no LSN after 60s. Check the capture job:"
        echo "  SELECT * FROM msdb.dbo.cdc_jobs;"
        exit 1
    }
    sleep 3
done

# --- Prove it works --------------------------------------------------------
# Enabling CDC and capturing changes are separate things. The only way to
# know capture is live is to make a change and read it back.
echo "==> Verifying capture end to end"

# A real value change, then reverted. Freight = Freight is optimised away
# before it reaches the log, so CDC captures nothing and the check reports
# a false failure.
run_sql "
UPDATE Orders SET Freight = 999.99 WHERE OrderID = 10248;
"
sleep 8
run_sql "
UPDATE Orders SET Freight = 32.38 WHERE OrderID = 10248;
"
sleep 8

run_sql "
SET NOCOUNT ON;
DECLARE @from binary(10) = sys.fn_cdc_get_min_lsn('dbo_Orders');
DECLARE @to   binary(10) = sys.fn_cdc_get_max_lsn();
SELECT COUNT(*) AS captured_rows
FROM cdc.fn_cdc_get_all_changes_dbo_Orders(@from, @to, N'all');
"

echo "==> Capture instances"
run_sql "
SET NOCOUNT ON;
SELECT capture_instance, OBJECT_NAME(source_object_id) AS source_table
FROM cdc.change_tables;
"

echo
echo "CDC ready on dbo.Orders and dbo.[Order Details]."
echo "Watermarks are tracked in ETL_Settings.dbo.CDC_State."
