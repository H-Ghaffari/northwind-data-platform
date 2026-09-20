#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Seed NorthwindRT once, before the consumer starts.
#
# CDC change tables carry only what happened after capture was enabled. A
# consumer started against empty tables would build a warehouse holding the
# handful of rows that have changed since — permanently incomplete, and
# incomplete in a way that looks like working software.
#
# So the streaming path starts from a copy of the current state and streams
# on top of it. This is snapshot-then-stream; Debezium performs the same step
# automatically on first connection.
#
# The copy is taken from NorthwindDW rather than from SQL Server because
# phase 1 already assigned the surrogate keys, resolved geography and built
# the history. Rebuilding that here would be phase 1 written a second time,
# in a worse language, with a second set of bugs.
#
# A --from-source mode that rebuilds from SQL Server belongs with the
# consumer, which has to contain those transforms anyway: seeding is the same
# work applied to whole tables instead of single events. Writing it before
# the consumer exists would mean writing that logic twice — the thing
# shared/scd_rules.py exists to prevent.
#
# Idempotent: refuses to run twice unless --force, because a second
# unconditional copy would duplicate every row.
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
set -a; source .env; set +a

DW_CONTAINER=northwind_dw
OP_CONTAINER=northwind_op
BATCH_DB="${DW_DB:-NorthwindDW}"
RT_DB="${DW_RT_DB:-NorthwindRT}"

FORCE=0
[[ "${1:-}" == "--force" ]] && FORCE=1

ch() {
  docker compose exec -T "${DW_CONTAINER}" clickhouse-client \
    --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" "$@"
}
ch_q() { ch --query "$1"; }
ch_scalar() { ch --query "$1" | tr -d '[:space:]'; }

# -h -1 suppresses the header and the dashes line; -W strips the padding
# SQL Server adds to fixed-width columns, which would otherwise arrive in
# ClickHouse as part of the value.
sql_tsv() {
  docker compose exec -T "${OP_CONTAINER}" /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -C -d "${OP_DB}" \
    -h -1 -W -s$'\t' -Q "SET NOCOUNT ON; $1" | sed '/^$/d'
}

echo "=========================================================="
echo " Snapshot ${BATCH_DB} -> ${RT_DB}"
echo "=========================================================="
echo

# --- 0. preconditions ------------------------------------------------------
for svc in "${DW_CONTAINER}" "${OP_CONTAINER}"; do
  if ! docker compose ps --status running --services | grep -qx "${svc}"; then
    echo "ERROR: ${svc} is not running. Try: ./scripts/mode.sh stream" >&2
    exit 1
  fi
done

SOURCE_ROWS=$(ch_scalar "SELECT count() FROM ${BATCH_DB}.DimCustomer")
if [[ "${SOURCE_ROWS}" == "0" ]]; then
  echo "ERROR: ${BATCH_DB} is empty." >&2
  echo "       The snapshot is copied from it. Load the batch warehouse first:" >&2
  echo "       ./scripts/mode.sh batch, then run the dimension DAGs." >&2
  exit 1
fi

EXISTING=$(ch_scalar "SELECT count() FROM ${RT_DB}.DimCustomer")
if [[ "${EXISTING}" != "0" && "${FORCE}" == "0" ]]; then
  echo "${RT_DB} already holds data (${EXISTING} customers)."
  echo "Copying again would duplicate every row."
  echo "Rerun with --force to truncate and reseed."
  exit 1
fi

# The consumer would otherwise apply changes to tables being rewritten
# underneath it, and the duplicates that produces are indistinguishable
# from a snapshot that ran twice.
if docker compose ps --status running --services 2>/dev/null | grep -qx cdc_consumer; then
  echo "→ stopping the consumer for the duration of the copy"
  docker compose stop cdc_consumer
  RESTART_CONSUMER=1
else
  RESTART_CONSUMER=0
fi

# --- 1. dimensions and facts ----------------------------------------------
# DimDate is excluded: setup_dw_rt.sh already copied the calendar, and it is
# generated rather than captured, so nothing keeps changing it.
TABLES=(
  DimGeography
  DimShippers
  DimTerritories
  DimSuppliers
  DimCustomer
  DimProducts
  DimEmployees
  FactEmployeeTerritories
  FactOrders
)

echo
echo "→ copying tables"
for t in "${TABLES[@]}"; do
  [[ "${FORCE}" == "1" ]] && ch_q "TRUNCATE TABLE IF EXISTS ${RT_DB}.\`${t}\`"

  # FINAL collapses parts the background merge has not yet resolved. Without
  # it a row mid-merge would be copied twice, and the streaming warehouse
  # would begin life with duplicates it did nothing to earn.
  #
  # Columns are named rather than SELECT *, because SELECT * omits ALIAS
  # columns on the read side but the insert still expects the stored set —
  # naming them keeps the two sides in step whatever the schema gains later.
  # Only the stored columns. ALIAS is evaluated at read time and MATERIALIZED
  # at write time — neither can appear in an INSERT column list, and both are
  # recomputed on the destination from the columns that are copied.
  COLS=$(ch_q "
    SELECT arrayStringConcat(groupArray(concat('\`', name, '\`')), ', ')
    FROM system.columns
    WHERE database = '${BATCH_DB}' AND table = '${t}'
      AND default_kind NOT IN ('ALIAS', 'MATERIALIZED')
    FORMAT TabSeparatedRaw")

  ch_q "INSERT INTO ${RT_DB}.\`${t}\` (${COLS})
        SELECT ${COLS} FROM ${BATCH_DB}.\`${t}\` FINAL"

  # Reported with FINAL, so the number is rows rather than rows plus parts a
  # merge has not yet collapsed. A type 2 dimension legitimately holds closed
  # versions alongside open ones, so this is not the current-row count either
  # — v_PathComparison below reports that.
  printf '  %-26s %s rows\n' "${t}" \
    "$(ch_scalar "SELECT count() FROM ${RT_DB}.\`${t}\` FINAL")"
done

# --- 2. the lake catalogue -------------------------------------------------
# Excluded from setup_dw_rt.sh because no CDC source produces employee
# photographs, so nothing would ever write to a cloned copy. Copied here
# anyway, for the same reason DimDate is: the streaming warehouse should
# answer every question the batch one does without reaching across to it.
echo
echo "→ copying the lake catalogue"

if [[ "$(ch_scalar "SELECT count() FROM system.tables
                    WHERE database = '${BATCH_DB}' AND name = 'LakeEmployeePhotos'")" == "1" ]]; then

  ch_q "CREATE TABLE IF NOT EXISTS ${RT_DB}.LakeEmployeePhotos AS ${BATCH_DB}.LakeEmployeePhotos"
  [[ "${FORCE}" == "1" ]] && ch_q "TRUNCATE TABLE ${RT_DB}.LakeEmployeePhotos"
  ch_q "INSERT INTO ${RT_DB}.LakeEmployeePhotos SELECT * FROM ${BATCH_DB}.LakeEmployeePhotos"
  echo "  LakeEmployeePhotos         $(ch_scalar "SELECT count() FROM ${RT_DB}.LakeEmployeePhotos") rows"

  # The view that joins the catalogue to the dimension, retargeted rather
  # than rewritten so its join stays identical to the batch path's.
  VIEW_DDL=$(ch_q "
    SELECT create_table_query FROM system.tables
    WHERE database = '${BATCH_DB}' AND name = 'v_EmployeeProfile'
    FORMAT TabSeparatedRaw" 2>/dev/null || true)

  if [[ -n "${VIEW_DDL}" ]]; then
    echo "${VIEW_DDL}" \
      | sed "s/\b${BATCH_DB}\b/${RT_DB}/g" \
      | sed "s/^CREATE VIEW/CREATE OR REPLACE VIEW/" \
      | ch --multiquery
    echo "  v_EmployeeProfile"
  fi
else
  echo "  not present in ${BATCH_DB} — skipped"
fi

# --- 3. reference tables ---------------------------------------------------
# These have no batch equivalent: the batch path resolves Categories and
# Region with a join in staging and never stores the id. The consumer needs
# the id to resolve an incoming change, so both come straight from the source.
#
# _version is seeded at 1, the lowest value the consumer will ever write, so
# the first real change to either table wins the merge without ambiguity.
echo
echo "→ seeding reference tables from the source"

ch_q "TRUNCATE TABLE IF EXISTS ${RT_DB}.RefCategories"
sql_tsv "SELECT CategoryID, RTRIM(CategoryName), 1 FROM dbo.Categories ORDER BY CategoryID" \
  | ch --query "INSERT INTO ${RT_DB}.RefCategories (category_id, category_name, _version) FORMAT TSV"
echo "  RefCategories              $(ch_scalar "SELECT count() FROM ${RT_DB}.RefCategories") rows"

ch_q "TRUNCATE TABLE IF EXISTS ${RT_DB}.RefRegion"
sql_tsv "SELECT RegionID, RTRIM(RegionDescription), 1 FROM dbo.Region ORDER BY RegionID" \
  | ch --query "INSERT INTO ${RT_DB}.RefRegion (region_id, region_description, _version) FORMAT TSV"
echo "  RefRegion                  $(ch_scalar "SELECT count() FROM ${RT_DB}.RefRegion") rows"

# --- 4. surrogate key high-water marks ------------------------------------
# The consumer reads these at startup and allocates upward. Printed here so
# the number it will start from is visible before it runs, rather than being
# something only the logs reveal.
echo
echo "=========================================================="
echo " Surrogate key high-water marks"
echo "=========================================================="
ch_q "
SELECT 'DimCustomer'    AS dimension, max(customer_key)  AS max_key FROM ${RT_DB}.DimCustomer
UNION ALL SELECT 'DimProducts',       max(product_key)   FROM ${RT_DB}.DimProducts
UNION ALL SELECT 'DimEmployees',      max(employee_key)  FROM ${RT_DB}.DimEmployees
UNION ALL SELECT 'DimSuppliers',      max(supplier_key)  FROM ${RT_DB}.DimSuppliers
UNION ALL SELECT 'DimShippers',       max(shipper_key)   FROM ${RT_DB}.DimShippers
UNION ALL SELECT 'DimTerritories',    max(territory_key) FROM ${RT_DB}.DimTerritories
UNION ALL SELECT 'DimGeography',      max(geography_key) FROM ${RT_DB}.DimGeography
ORDER BY 1 FORMAT PrettyCompact"

# --- 5. both paths, side by side ------------------------------------------
# Equal right now is the only moment this comparison is unambiguous: from
# here the two warehouses are fed by different pipelines on different
# schedules, and a later difference means one has seen changes the other has
# not — not that either is broken.
echo
echo "=========================================================="
echo " Both paths, side by side"
echo "=========================================================="
ch_q "SELECT * FROM ${RT_DB}.v_PathComparison FORMAT PrettyCompact"

if [[ "${RESTART_CONSUMER}" == "1" ]]; then
  echo
  echo "→ restarting the consumer"
  docker compose start cdc_consumer
fi

echo
echo "Done. The streaming warehouse now matches the batch one; the consumer"
echo "applies changes on top of it."