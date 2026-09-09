"""Staging → DW for the fact table.

Stage two of two, mirroring package 13 of the reference SSIS project. Reads
the operation-split staging tables, resolves surrogate keys, and applies
the result to FactOrders.

Nothing here touches the source system — that is the point of the split. If
a lookup fails or the warehouse is briefly unreachable, this stage reruns
from the rows already in staging. The reference implementation looks the
parent order up in Orders from this stage; landing it in staging first
removes that dependency.

Recovering the other side of the join:

    an order changed   → its lines come from staging_order_details
    a line changed     → its order comes from staging_orders

Both were landed by stage one, scoped to the orders the change window
touched.

Three operations, three behaviours — all expressed as inserts:

    insert  new fact rows
    update  reinserted with a higher _version; the merge replaces in place
    delete  reinserted with is_deleted = 1, which the merge then hides

The reference project issues DELETE statements here. ClickHouse mutations
rewrite whole parts, so a tombstone insert costs a fraction of what a
delete would for the same effect.

The watermark advances at the very end, once rows have landed. Doing it in
stage one would mean a failure here loses those changes: CDC does not
return a window twice.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.cdc_reader import write_watermark
from common.inferred_member import count_inferred_members, create_inferred_members
from common.spark_utils import (
    dw_client,
    log,
    read_from_staging,
    spark_session,
    truncate_dw_table,
    write_to_dw,
)

TARGET_TABLE = "FactOrders"

FACT_COLUMNS = [
    "order_id", "product_key", "geography_key", "customer_key", "employee_key",
    "shipper_key", "order_date_key", "required_date_key", "shipped_date_key",
    "freight", "unit_price", "quantity", "discount", "ship_name",
    "order_date", "required_date", "shipped_date", "is_deleted",
]

# The joined shape the warehouse needs. Orders is the parent and Order
# Details the child, so the grain is the child's: one row per (order,
# product). Parent attributes — freight, ship_name, the dates — repeat
# across every line, which is why summing freight over this table
# double-counts and must be aggregated over distinct order_id instead.
JOIN_TEMPLATE = """
    SELECT
        o.order_id,
        d.product_id,
        o.customer_id,
        o.employee_id,
        o.ship_via,
        COALESCE(o.ship_country,'')     AS ship_country,
        COALESCE(o.ship_region,'')      AS ship_region,
        COALESCE(o.ship_city,'')        AS ship_city,
        COALESCE(o.ship_postal_code,'') AS ship_postal_code,
        COALESCE(o.ship_address,'')     AS ship_address,
        COALESCE(o.ship_name,'')        AS ship_name,
        o.order_date,
        o.required_date,
        o.shipped_date,
        COALESCE(o.freight, 0)          AS freight,
        d.unit_price,
        d.quantity,
        d.discount
    FROM {orders} AS o
    INNER JOIN {details} AS d ON d.order_id = o.order_id
"""

SOURCE_COLUMNS = [
    "order_id", "product_id", "customer_id", "employee_id", "ship_via",
    "ship_country", "ship_region", "ship_city", "ship_postal_code",
    "ship_address", "ship_name", "order_date", "required_date",
    "shipped_date", "freight", "unit_price", "quantity", "discount",
]


# ---------------------------------------------------------------------------
# Dimension lookups
# ---------------------------------------------------------------------------

def _lookup_table(spark: SparkSession, table: str, key_column: str,
                  alternate_column: str, alias: str) -> DataFrame:
    """The alternate-key → surrogate-key mapping for one dimension.

    Only currently-open rows, so a fact joins to the version of the
    dimension in force now. Historical restatement — attaching a fact to the
    version in force when it happened — would key on the order date instead,
    and is out of scope here.
    """
    client = dw_client()
    try:
        has_history = table not in ("DimShippers", "DimGeography")
        where = (
            "WHERE end_date = toDateTime('2106-01-01 00:00:00')"
            if has_history else ""
        )
        rows = client.query(
            f"SELECT {key_column}, {alternate_column} FROM {table} FINAL {where}"
        ).result_rows
    finally:
        client.close()

    if not rows:
        log.warning("%s is empty — every lookup against it will miss", table)
        return spark.createDataFrame([], schema=f"{alias}_key int, {alias}_alt string")

    return spark.createDataFrame(rows, schema=[f"{alias}_key", f"{alias}_alt"])


def _resolve_with_inferred(
    spark: SparkSession,
    df: DataFrame,
    source_column: str,
    table: str,
    key_column: str,
    alternate_column: str,
    alias: str,
    target_column: str,
) -> DataFrame:
    """Attach a surrogate key, creating inferred members for any misses.

    The lookup runs twice when something is missing: once to find what is
    absent, then again once the stubs exist. Two passes rather than patching
    the first result keeps one code path for resolution, so the
    inferred-member case cannot drift from the normal one.
    """
    lookup = _lookup_table(spark, table, key_column, alternate_column, alias)

    joined = df.join(
        lookup,
        df[source_column].cast("string") == lookup[f"{alias}_alt"].cast("string"),
        how="left",
    )

    missing = (
        joined.filter(F.col(f"{alias}_key").isNull() & F.col(source_column).isNotNull())
        .select(source_column)
        .distinct()
        .collect()
    )

    if missing:
        create_inferred_members(table, [row[source_column] for row in missing])

        lookup = _lookup_table(spark, table, key_column, alternate_column, alias)
        joined = df.join(
            lookup,
            df[source_column].cast("string") == lookup[f"{alias}_alt"].cast("string"),
            how="left",
        )

    return (
        joined
        .withColumn(target_column, F.coalesce(F.col(f"{alias}_key"), F.lit(0)).cast("int"))
        .drop(f"{alias}_key", f"{alias}_alt")
    )


def _resolve_ship_geography(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Attach geography_key from the shipping address.

    Joined on all five address columns, where the reference implementation
    joins on four and ignores the street. DimGeography stores the street, so
    a four-column match can return any of several rows for the same city and
    postcode — SSIS silently takes the first.

    No inferred member here: geography has no source key to stub out, and an
    unknown address is far less consequential than an unknown customer.
    Unmatched rows take key 0 and are logged.
    """
    client = dw_client()
    try:
        rows = client.query(
            "SELECT geography_key, country, region, city, postal_code, address "
            "FROM DimGeography FINAL"
        ).result_rows
    finally:
        client.close()

    if not rows:
        log.warning("DimGeography is empty — every geography_key will be 0")
        return df.withColumn("geography_key", F.lit(0).cast("int"))

    geography = spark.createDataFrame(
        rows,
        schema=["geography_key", "g_country", "g_region", "g_city",
                "g_postal_code", "g_address"],
    )

    resolved = df.join(
        geography,
        on=(
            (df["ship_country"] == geography["g_country"])
            & (df["ship_region"] == geography["g_region"])
            & (df["ship_city"] == geography["g_city"])
            & (df["ship_postal_code"] == geography["g_postal_code"])
            & (df["ship_address"] == geography["g_address"])
        ),
        how="left",
    ).drop("g_country", "g_region", "g_city", "g_postal_code", "g_address")

    unresolved = resolved.filter(F.col("geography_key").isNull()).count()
    if unresolved:
        log.warning("%s fact rows have an unresolved shipping address", unresolved)

    return resolved.withColumn(
        "geography_key", F.coalesce(F.col("geography_key"), F.lit(0)).cast("int")
    )


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------

def _transform(spark: SparkSession, source: DataFrame, is_deleted: int = 0) -> DataFrame:
    """Resolve keys and derive date keys."""
    df = _resolve_with_inferred(
        spark, source, "product_id", "DimProducts",
        "product_key", "product_alternate_key", "prod", "product_key",
    )
    df = _resolve_with_inferred(
        spark, df, "customer_id", "DimCustomer",
        "customer_key", "customer_alternate_key", "cust", "customer_key",
    )
    df = _resolve_with_inferred(
        spark, df, "employee_id", "DimEmployees",
        "employee_key", "employee_alternate_key", "emp", "employee_key",
    )
    df = _resolve_with_inferred(
        spark, df, "ship_via", "DimShippers",
        "shipper_key", "shipper_alternate_key", "ship", "shipper_key",
    )
    df = _resolve_ship_geography(spark, df)

    # Smart keys: DimDate is keyed on yyyyMMdd, so a date needs no join —
    # the same trick the reference packages use with format(OrderDate,
    # 'yyyyMMdd'). A missing date gets 0, a deliberate miss rather than a
    # null.
    def date_key(column: str):
        return F.coalesce(
            F.date_format(F.col(column), "yyyyMMdd").cast("int"), F.lit(0)
        )

    return (
        df
        .withColumn("order_date_key", date_key("order_date"))
        .withColumn("required_date_key", date_key("required_date"))
        .withColumn("shipped_date_key", date_key("shipped_date"))
        .withColumn("is_deleted", F.lit(is_deleted).cast("int"))
        .withColumn("freight", F.col("freight").cast("decimal(19,4)"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(19,4)"))
        .withColumn("quantity", F.col("quantity").cast("short"))
        .withColumn("discount", F.col("discount").cast("float"))
        .select(*FACT_COLUMNS)
    )


def _row_count(spark: SparkSession, table: str) -> int:
    return read_from_staging(
        spark, f"SELECT count(*) AS n FROM {table}"
    ).collect()[0]["n"]


# ---------------------------------------------------------------------------
# Initial load
# ---------------------------------------------------------------------------

def initial_load(spark: SparkSession) -> int:
    """Load every fact row from the staging snapshot.

    Truncates first, so it is safe to rerun — but it discards anything the
    incremental path has applied since, which is why the DAGs keep the two
    apart rather than scheduling this one.
    """
    source = read_from_staging(
        spark,
        JOIN_TEMPLATE.format(
            orders="staging_orders", details="staging_order_details"
        ),
    ).toDF(*SOURCE_COLUMNS)

    count = source.count()
    log.info("Initial load: %s fact rows from the staging snapshot", f"{count:,}")
    if not count:
        raise RuntimeError(
            "The staging snapshot is empty. Run the OP → Staging fact job in "
            "initial mode first."
        )

    facts = _transform(spark, source)
    truncate_dw_table(TARGET_TABLE)
    written = write_to_dw(facts, TARGET_TABLE, FACT_COLUMNS)

    _report_inferred()
    return written


# ---------------------------------------------------------------------------
# Incremental: the three operations
# ---------------------------------------------------------------------------

def _apply_orders_side(spark: SparkSession, operation: str) -> int:
    """Apply changed orders, pulling their lines from the snapshot.

    An order-level change touches every line of that order, because the fact
    grain repeats the parent's attributes across all of them. CDC reports
    only the order, so the lines come from the snapshot.
    """
    orders_table = f"staging_orders_{operation}"
    if not _row_count(spark, orders_table):
        return 0

    source = read_from_staging(
        spark,
        JOIN_TEMPLATE.format(orders=orders_table, details="staging_order_details"),
    ).toDF(*SOURCE_COLUMNS)

    if source.count() == 0:
        log.info("%s: no lines found for these orders", orders_table)
        return 0

    facts = _transform(spark, source)
    return write_to_dw(facts, TARGET_TABLE, FACT_COLUMNS)


def _apply_details_side(spark: SparkSession, operation: str, is_deleted: int = 0) -> int:
    """Apply changed lines, pulling their order from the snapshot.

    A line-level change needs its parent's attributes — freight, ship name,
    the dates — which are not in the Order Details change table.
    """
    details_table = f"staging_order_details_{operation}"
    if not _row_count(spark, details_table):
        return 0

    source = read_from_staging(
        spark,
        JOIN_TEMPLATE.format(orders="staging_orders", details=details_table),
    ).toDF(*SOURCE_COLUMNS)

    if source.count() == 0:
        log.warning("%s: parent orders not found in the snapshot", details_table)
        return 0

    facts = _transform(spark, source, is_deleted=is_deleted)
    return write_to_dw(facts, TARGET_TABLE, FACT_COLUMNS)


def _tombstone_deleted_orders(spark: SparkSession) -> int:
    """Mark every line of a deleted order as deleted.

    Deleting an order takes its lines with it, and the order is gone from
    the source too — so there is nothing left to snapshot or join against.
    The existing fact rows are read back from the warehouse and reinserted
    with the tombstone set, which keeps the measures intact and changes only
    the flag.
    """
    if not _row_count(spark, "staging_orders_delete"):
        return 0

    order_ids = read_from_staging(
        spark, "SELECT DISTINCT order_id FROM staging_orders_delete"
    ).collect()
    id_list = ",".join(str(row["order_id"]) for row in order_ids)

    client = dw_client()
    try:
        columns = [c for c in FACT_COLUMNS if c != "is_deleted"]
        rows = client.query(
            f"SELECT {', '.join(columns)} FROM {TARGET_TABLE} FINAL "
            f"WHERE order_id IN ({id_list}) AND is_deleted = 0"
        ).result_rows

        if not rows:
            log.info("Nothing to tombstone for deleted orders")
            return 0

        tombstones = [row + (1,) for row in rows]
        client.insert(TARGET_TABLE, tombstones, column_names=columns + ["is_deleted"])
        log.info("Tombstoned %s fact row(s) from deleted orders", len(tombstones))
        return len(tombstones)
    finally:
        client.close()


def incremental_load(spark: SparkSession, window: dict | None = None) -> int:
    """Apply everything sitting in the fact staging tables.

    window carries the LSNs stage one read, and is what the watermark is set
    to on success. Passing it through rather than recomputing it here
    matters: changes may have arrived since stage one ran, and advancing
    past them would skip them entirely.
    """
    written = 0

    written += _apply_orders_side(spark, "insert")
    written += _apply_orders_side(spark, "update")

    written += _apply_details_side(spark, "insert")
    written += _apply_details_side(spark, "update")
    written += _apply_details_side(spark, "delete", is_deleted=1)

    written += _tombstone_deleted_orders(spark)

    if window:
        if window.get("orders_to_lsn"):
            write_watermark("dbo_Orders", bytes.fromhex(window["orders_to_lsn"]))
        if window.get("details_to_lsn"):
            write_watermark("dbo_OrderDetails", bytes.fromhex(window["details_to_lsn"]))

    _report_inferred()
    log.info("Applied %s fact row(s)", f"{written:,}")
    return written


def _report_inferred() -> None:
    """Log how many placeholder dimension rows are outstanding.

    A number climbing across runs means the dimension load is not keeping
    up, and someone will eventually see blank labels on a chart.
    """
    for table in ("DimCustomer", "DimProducts", "DimEmployees", "DimShippers"):
        stubs = count_inferred_members(table)
        if stubs:
            log.warning("%s holds %s inferred member(s) awaiting enrichment",
                        table, stubs)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run_initial() -> int:
    with spark_session("staging_to_dw_facts_initial") as spark:
        return initial_load(spark)


def run_incremental(window: dict | None = None) -> int:
    with spark_session("staging_to_dw_facts_incremental") as spark:
        return incremental_load(spark, window)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "incremental"
    if mode == "initial":
        run_initial()
    else:
        run_incremental()
    sys.exit(0)
