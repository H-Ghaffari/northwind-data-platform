#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Prepare the source system for the streaming path.
#
#   1. enable CDC on all eleven source tables
#   2. create and seed ETL_Settings.Stream_State
#   3. prove capture works end to end
#   4. cross-check that every seeded source is actually captured
#
# Self-contained: does not require the batch setup to have been run. The two
# paths share a source system and a ClickHouse server, but neither needs the
# other to have been set up first.
#
# Separate from setup_all.sh on purpose. CDC on eleven tables means eleven
# capture jobs on SQL Server Agent and a change table each — someone running
# only the batch path should not pay for that.
#
# Idempotent — safe to rerun.
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
set -a; source .env; set +a

OP_CONTAINER=northwind_op
SQLCMD="/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${MSSQL_SA_PASSWORD} -C"

sql_query() {
  docker compose exec -T "${OP_CONTAINER}" ${SQLCMD} -h -1 -W "$@"
}

echo "=========================================================="
echo " Streaming path — source system setup"
echo "=========================================================="
echo

# --- 0. the container has to be up and answering --------------------------
if ! docker compose ps --status running --services | grep -qx "${OP_CONTAINER}"; then
  echo "ERROR: ${OP_CONTAINER} is not running." >&2
  echo "       Start it with: ./scripts/mode.sh stream" >&2
  exit 1
fi

echo "→ waiting for SQL Server"
for _ in $(seq 1 30); do
  if docker compose exec -T "${OP_CONTAINER}" ${SQLCMD} -Q "SELECT 1" -b -o /dev/null 2>/dev/null; then
    break
  fi
  sleep 2
done

# --- 1. the Northwind database must exist ---------------------------------
# Restored by setup_all.sh or from the professor's .bak. Checked explicitly
# because USE on a missing database fails with a message that does not say
# what to do about it.
NORTHWIND=$(sql_query -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = 'Northwind';" | tr -d '[:space:]')
if [[ "${NORTHWIND}" == "0" ]]; then
  echo "ERROR: the Northwind database does not exist." >&2
  echo "       Restore it first: ./scripts/setup_all.sh" >&2
  exit 1
fi

# --- 2. SQL Server Agent — CDC capture jobs cannot run without it ---------
echo "→ checking SQL Server Agent"
AGENT_RUNNING=$(sql_query -Q "
SET NOCOUNT ON;
SELECT COUNT(*) FROM sys.dm_server_services
WHERE servicename LIKE '%Agent%' AND status_desc = 'Running';" | tr -d '[:space:]')

if [[ "${AGENT_RUNNING}" == "0" ]]; then
  echo "ERROR: SQL Server Agent is not running. CDC would capture nothing." >&2
  echo "       Recreate the container:" >&2
  echo "       docker compose up -d --force-recreate ${OP_CONTAINER}" >&2
  exit 1
fi
echo "  running."

# --- 3. enable CDC on all eleven sources ----------------------------------
echo
echo "→ enabling CDC"
docker compose exec -T "${OP_CONTAINER}" ${SQLCMD} -b -i /opt/sql/01_op/03_enable_cdc_streaming.sql

# --- 4. create and seed the watermark table -------------------------------
echo
echo "→ creating Stream_State"
docker compose exec -T "${OP_CONTAINER}" ${SQLCMD} -b -i /opt/sql/04_etl_settings/03_stream_state.sql

# --- 5. prove capture actually works --------------------------------------
# Enabled-but-never-capturing produces no error and no rows, which looks
# exactly like a quiet period. The only way to tell them apart is to make a
# real change and read it back. A no-op update is optimised away before it
# reaches the log and would report a false failure, so the value is changed
# and then changed back.
echo
echo "→ proving capture end to end on dbo.Customers"

docker compose exec -T "${OP_CONTAINER}" ${SQLCMD} -d Northwind -b -Q "
SET NOCOUNT ON;
DECLARE @original NVARCHAR(30);
SELECT @original = ContactTitle FROM dbo.Customers WHERE CustomerID = 'ALFKI';
UPDATE dbo.Customers SET ContactTitle = 'CDC probe' WHERE CustomerID = 'ALFKI';
UPDATE dbo.Customers SET ContactTitle = @original   WHERE CustomerID = 'ALFKI';
" > /dev/null

echo "  waiting for the capture job"
CAPTURED=0
for _ in $(seq 1 20); do
  sleep 3
  ROWS=$(sql_query -d Northwind -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM cdc.dbo_Customers_CT;" | tr -d '[:space:]')
  if [[ "${ROWS}" =~ ^[0-9]+$ ]] && (( ROWS > 0 )); then
    CAPTURED=1
    echo "  captured ${ROWS} change rows"
    break
  fi
done

if (( CAPTURED == 0 )); then
  echo "ERROR: CDC is enabled but captured nothing after 60 seconds." >&2
  echo "       Inspect the capture jobs: EXEC sys.sp_cdc_help_jobs" >&2
  exit 1
fi

# --- 6. cross-check Stream_State against what is actually captured --------
# Every active row names a capture instance the producer will poll. A name
# with nothing behind it yields no error and no rows — indistinguishable
# from a source where nothing has changed. That is the exact failure this
# script exists to rule out, so it is checked rather than assumed.
echo
echo "→ cross-checking Stream_State against cdc.change_tables"

MISSING=$(sql_query -Q "
SET NOCOUNT ON;
SELECT s.capture_instance
FROM ETL_Settings.dbo.Stream_State s
WHERE s.is_active = 1
  AND NOT EXISTS (SELECT 1 FROM Northwind.cdc.change_tables ct
                  WHERE ct.capture_instance = s.capture_instance);" \
  | grep -v '^$' || true)

if [[ -n "${MISSING}" ]]; then
  echo "ERROR: Stream_State names capture instances that do not exist:" >&2
  echo "${MISSING}" | sed 's/^/       /' >&2
  exit 1
fi
echo "  every active source is backed by a live capture instance."

# --- 7. report ------------------------------------------------------------
echo
echo "=========================================================="
echo " Capture instances"
echo "=========================================================="
docker compose exec -T "${OP_CONTAINER}" ${SQLCMD} -d Northwind -W -Q "
SET NOCOUNT ON;
SELECT ct.capture_instance, s.name + '.' + t.name AS source_table
FROM cdc.change_tables ct
JOIN sys.tables  t ON t.object_id = ct.source_object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
ORDER BY ct.capture_instance;"

echo
echo "=========================================================="
echo " Stream_State"
echo "=========================================================="
docker compose exec -T "${OP_CONTAINER}" ${SQLCMD} -d ETL_Settings -W -Q "
SET NOCOUNT ON;
SELECT capture_instance, topic_name, propagate_deletes, is_active
FROM dbo.Stream_State ORDER BY capture_instance;"

echo
echo "Eleven capture instances are live and Stream_State is seeded."