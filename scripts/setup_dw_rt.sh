#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Build NorthwindRT, the streaming path's warehouse.
#
# Two ways to get the schema, and they end at the same place:
#
#   clone  CREATE TABLE ... AS NorthwindDW.X, when the batch warehouse
#          exists. Copies columns, types, engine and sort key, no rows.
#
#   ddl    replay sql/03_dw/*.sql with the database name substituted, when
#          it does not. Slower, and independent of the batch path.
#
# Neither maintains a second copy of the schema. The clone reads a live
# table, the ddl mode reads the same files the batch warehouse was built
# from, so a divergence between the two warehouses is not expressible.
#
# Idempotent — safe to rerun.
#
# Usage:
#   ./scripts/setup_dw_rt.sh              auto: clone if possible, else ddl
#   ./scripts/setup_dw_rt.sh --from-ddl   force ddl mode
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
set -a; source .env; set +a

DW_CONTAINER=northwind_dw
BATCH_DB="${DW_DB:-NorthwindDW}"
RT_DB="${DW_RT_DB:-NorthwindRT}"

FORCE_DDL=0
[[ "${1:-}" == "--from-ddl" ]] && FORCE_DDL=1

ch() {
  docker compose exec -T "${DW_CONTAINER}" clickhouse-client \
    --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" "$@"
}
ch_q() { ch --query "$1"; }

echo "=========================================================="
echo " Streaming warehouse — ${RT_DB}"
echo "=========================================================="
echo

# --- 0. preconditions ------------------------------------------------------
if ! docker compose ps --status running --services | grep -qx "${DW_CONTAINER}"; then
  echo "ERROR: ${DW_CONTAINER} is not running." >&2
  echo "       Start it with: ./scripts/mode.sh stream" >&2
  exit 1
fi

echo "→ waiting for ClickHouse"
for _ in $(seq 1 30); do
  ch_q "SELECT 1" > /dev/null 2>&1 && break
  sleep 2
done

# --- 1. pick a mode --------------------------------------------------------
BATCH_TABLES=$(ch_q "
  SELECT count() FROM system.tables
  WHERE database = '${BATCH_DB}' AND engine NOT LIKE '%View%'" | tr -d '[:space:]')

if [[ "${FORCE_DDL}" == "1" || "${BATCH_TABLES}" == "0" ]]; then
  MODE=ddl
  if [[ "${BATCH_TABLES}" == "0" ]]; then
    echo "→ ${BATCH_DB} not found — building from sql/03_dw/"
  else
    echo "→ --from-ddl given — building from sql/03_dw/"
  fi
else
  MODE=clone
  echo "→ cloning structure from ${BATCH_DB} (${BATCH_TABLES} tables)"
fi

ch_q "CREATE DATABASE IF NOT EXISTS ${RT_DB}"

# --- 2. build the schema ---------------------------------------------------
if [[ "${MODE}" == "clone" ]]; then

  # Lake tables are skipped: employee photographs come from a batch DAG and
  # no CDC source produces them, so a copy would be a table nothing writes.
  TABLES=$(ch_q "
    SELECT name FROM system.tables
    WHERE database = '${BATCH_DB}'
      AND engine NOT LIKE '%View%'
      AND name NOT LIKE 'Lake%'
    ORDER BY name FORMAT TabSeparated")

  echo
  echo "→ tables"
  for t in ${TABLES}; do
    ch_q "CREATE TABLE IF NOT EXISTS ${RT_DB}.\`${t}\` AS ${BATCH_DB}.\`${t}\`"
    echo "  ${t}"
  done

  # Views are cloned rather than rewritten for the same reason: each carries
  # the FINAL clause and the open-row predicate, and a divergence there would
  # surface only as two dashboards disagreeing.
  VIEWS=$(ch_q "
    SELECT name FROM system.tables
    WHERE database = '${BATCH_DB}'
      AND engine LIKE '%View%'
      AND name NOT LIKE '%Lake%'
      AND name NOT LIKE '%Employee%Profile%'
    ORDER BY name FORMAT TabSeparated")

  echo
  echo "→ views"
  for v in ${VIEWS}; do
    DDL=$(ch_q "
      SELECT create_table_query FROM system.tables
      WHERE database = '${BATCH_DB}' AND name = '${v}'
      FORMAT TabSeparatedRaw")
    RT_DDL=$(echo "${DDL}" \
      | sed "s/\b${BATCH_DB}\b/${RT_DB}/g" \
      | sed "s/^CREATE VIEW/CREATE OR REPLACE VIEW/")
    ch_q "${RT_DDL}"
    echo "  ${v}"
  done

else

  # The DDL files are fully qualified with the batch database name, so
  # retargeting is a substitution. IF NOT EXISTS is added where missing so a
  # rerun does not fail on an object already built.
  echo
  echo "→ replaying sql/03_dw/"
  shopt -s nullglob
  FILES=(sql/03_dw/*.sql)
  shopt -u nullglob

  if (( ${#FILES[@]} == 0 )); then
    echo "ERROR: sql/03_dw/ contains no .sql files." >&2
    exit 1
  fi

  for f in "${FILES[@]}"; do
    sed -e "s/\b${BATCH_DB}\b/${RT_DB}/g" \
        -e "s/^CREATE TABLE \([^I]\)/CREATE TABLE IF NOT EXISTS \1/" \
        -e "s/^CREATE VIEW/CREATE OR REPLACE VIEW/" \
        "${f}" | ch --multiquery
    echo "  $(basename "${f}")"
  done

  # Built by the batch DDL, but no CDC source feeds them.
  for t in $(ch_q "SELECT name FROM system.tables
                   WHERE database = '${RT_DB}' AND name LIKE 'Lake%'
                   FORMAT TabSeparated"); do
    ch_q "DROP TABLE IF EXISTS ${RT_DB}.\`${t}\`"
    echo "  dropped ${t} (no streaming source)"
  done
fi

# --- 3. reference and telemetry tables ------------------------------------
echo
echo "→ reference and telemetry tables"
ch --multiquery < sql/05_dw_rt/01_reference.sql
ch --multiquery < sql/05_dw_rt/02_stream_events.sql
echo "  RefCategories, RefRegion, StreamEvents"

# --- 4. the calendar -------------------------------------------------------
# DimDate is generated, not captured. Copying it once keeps the streaming
# database self-contained; a date dimension never changes, so there is
# nothing to keep in sync afterwards.
echo
echo "→ DimDate"
RT_DATES=$(ch_q "SELECT count() FROM ${RT_DB}.DimDate" | tr -d '[:space:]')

if [[ "${RT_DATES}" != "0" ]]; then
  echo "  already populated (${RT_DATES} rows)"
elif [[ "${MODE}" == "clone" ]]; then
  ch_q "INSERT INTO ${RT_DB}.DimDate SELECT * FROM ${BATCH_DB}.DimDate"
  echo "  copied $(ch_q "SELECT count() FROM ${RT_DB}.DimDate" | tr -d '[:space:]') rows"
else
  # No batch warehouse to copy from, and no CDC source generates a calendar.
  # Filled by the snapshot step, which runs the same generator the batch
  # path uses rather than a second implementation of it.
  echo "  left empty — snapshot_rt.sh generates it in --from-source mode"
fi

# --- 5. streaming-only views ----------------------------------------------
# The comparison views reference NorthwindDW. They are created either way —
# a view over a missing database is valid until queried — but only usable
# when the batch warehouse exists.
echo
echo "→ streaming-only views"
ch --multiquery < sql/05_dw_rt/03_views.sql
echo "  v_RefCategories_Current, v_RefRegion_Current, v_StreamHealth,"
echo "  v_PathComparison, v_CustomerDrift"

if [[ "${MODE}" == "ddl" ]]; then
  echo
  echo "  note: v_PathComparison and v_CustomerDrift read ${BATCH_DB}, which"
  echo "        does not exist here. They will error until the batch"
  echo "        warehouse is built. Everything else works."
fi

# --- 6. report -------------------------------------------------------------
echo
echo "=========================================================="
echo " ${RT_DB}"
echo "=========================================================="
ch_q "
SELECT name,
       if(engine LIKE '%View%', 'view', 'table') AS kind,
       engine,
       total_rows AS rows
FROM system.tables
WHERE database = '${RT_DB}'
ORDER BY kind, name
FORMAT PrettyCompact"

echo
echo "Done (mode: ${MODE}). Every table empty except DimDate."