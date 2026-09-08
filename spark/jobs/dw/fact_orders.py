"""Load FactOrders.

Two entry points share one transformation:

    initial_load      every order, once, to seed the warehouse
    incremental_load  only what CDC reports as changed since the watermark

Grain is one row per (order, product) — the grain of Order Details, the
finer of the two sources. Attributes from the parent Orders row (freight,
ship_name, the date keys) therefore repeat across every line of an order.
Summing freight over this table double-counts; aggregate it over distinct
order_id instead. The DDL says the same thing next to the column.

Date keys are derived, not looked up: DimDate uses a yyyyMMdd smart key, so
toYYYYMMDD() on the source date produces the key directly.

Every other key goes through a lookup, and any that misses creates an
inferred member rather than dropping the fact.
"""

from __future__ import annotations

import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.cdc_reader import (
    cdc_is_healthy,
    get_cdc_window,
    read_changes,
    split_by_operation,
    write_watermark,
)
from common.inferred_member import count_inferred_members, create_inferred_members
from common.spark_utils import (
    dw_client,
    log,
    read_from_op,
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

# The joined source query, shared by both load paths. Orders is the parent
# and Order Details the child, so this is an INNER join: an order with no
# lines has nothing to record at this grain.
SOURCE_QUERY = """
    SELECT
        o.OrderID,
        od.ProductID,
        o.CustomerID,
        o.EmployeeID,
        o.ShipVia,
        ISNULL(o.ShipCountry,'')    AS ShipCountry,
        ISNULL(o.ShipRegion,'')     AS ShipRegion,
        ISNULL(o.ShipCity,'')       AS ShipCity,
        ISNULL(o.ShipPostalCode,'') AS ShipPostalCode,
        ISNULL(o.ShipAddress,'')    AS ShipAddress,
        ISNULL(o.ShipName,'')       AS ShipName,
        o.OrderDate,
        o.RequiredDate,
        o.ShippedDate,
        ISNULL(o.Freight, 0)        AS Freight,
        od.UnitPrice,
        od.Quantity,
        od.Discount
    FROM Orders AS o
    INNER JOIN [Order Details] AS od ON od.OrderID = o.OrderID
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
    dimension that is in force now. Historical restatement — attaching a
    fact to the version in force at the time it happened — would key on the
    order date instead, and is out of scope for this pipeline.
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
    absent, then again after the stubs exist. Two passes rather than
    patching the first result keeps one code path for resolution, so the
    inferred-member case cannot drift from the normal case.
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
        missing_keys = [row[source_column] for row in missing]
        create_inferred_members(table, missing_keys)

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

    No inferred member here: geography has no source key to stub out, and
    an unknown address is far less consequential than an unknown customer.
    Unmatched rows take key 0.
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
    """Turn source rows into fact rows: resolve keys, derive date keys."""
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

    # Smart keys: DimDate is keyed on yyyyMMdd, so no join is needed. Rows
    # with no date get 0, which is a deliberate miss rather than a null.
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


# ---------------------------------------------------------------------------
# Initial load
# ---------------------------------------------------------------------------

def initial_load(spark: SparkSession) -> int:
    """Load every order. Run once to seed the warehouse.

    Truncates first, so it is safe to rerun — but it discards anything the
    incremental path has applied since, which is why the DAG keeps the two
    apart rather than running this on a schedule.
    """
    log.info("Initial load: reading all orders from OP")
    source = read_from_op(spark, SOURCE_QUERY).toDF(*SOURCE_COLUMNS)

    facts = _transform(spark, source)

    truncate_dw_table(TARGET_TABLE)
    written = write_to_dw(facts, TARGET_TABLE, FACT_COLUMNS)

    _report_inferred()
    return written


# ---------------------------------------------------------------------------
# Incremental load
# ---------------------------------------------------------------------------

def incremental_load(spark: SparkSession) -> int:
    """Apply what changed since the last watermark.

    Order changes and line changes are read separately, because a change to
    either side affects the same fact rows. Both are resolved back to full
    fact rows by re-reading the joined source for the affected order ids —
    the change tables carry only one side of the join.

    The watermark advances only after the write succeeds. A run that fails
    partway reprocesses the same window next time, which the fact table's
    ReplacingMergeTree absorbs.
    """
    if not cdc_is_healthy():
        raise RuntimeError("CDC is not healthy — see the log above")

    order_window = get_cdc_window("dbo_Orders")
    detail_window = get_cdc_window("dbo_OrderDetails")

    if order_window.is_empty and detail_window.is_empty:
        log.info("No changes to apply")
        return 0

    affected_orders: set[int] = set()
    deleted_pairs: set[tuple] = set()

    # --- Orders ---------------------------------------------------------
    if not order_window.is_empty:
        rows = read_changes(order_window, ["OrderID"])
        split = split_by_operation(rows)
        for row in split["insert"] + split["update"]:
            affected_orders.add(row[0])
        for row in split["delete"]:
            # A deleted order takes all of its lines with it.
            deleted_pairs.add((row[0], None))

    # --- Order Details --------------------------------------------------
    if not detail_window.is_empty:
        rows = read_changes(detail_window, ["OrderID", "ProductID"])
        split = split_by_operation(rows)
        for row in split["insert"] + split["update"]:
            affected_orders.add(row[0])
        for row in split["delete"]:
            deleted_pairs.add((row[0], row[1]))

    written = 0

    # --- Re-read and rewrite the affected orders ------------------------
    if affected_orders:
        id_list = ",".join(str(o) for o in sorted(affected_orders))
        log.info("Reloading %s affected order(s)", len(affected_orders))

        source = read_from_op(
            spark, f"{SOURCE_QUERY} WHERE o.OrderID IN ({id_list})"
        ).toDF(*SOURCE_COLUMNS)

        if source.count():
            facts = _transform(spark, source)
            written += write_to_dw(facts, TARGET_TABLE, FACT_COLUMNS)

    # --- Tombstone the deletions ----------------------------------------
    if deleted_pairs:
        written += _apply_deletes(deleted_pairs)

    # --- Advance the watermarks, last ------------------------------------
    if not order_window.is_empty:
        write_watermark("dbo_Orders", order_window.to_lsn)
    if not detail_window.is_empty:
        write_watermark("dbo_OrderDetails", detail_window.to_lsn)

    _report_inferred()
    return written


def _apply_deletes(deleted_pairs: set[tuple]) -> int:
    """Mark deleted facts with a tombstone.

    A delete is written as an ordinary insert carrying is_deleted = 1, which
    ReplacingMergeTree then resolves. ALTER TABLE DELETE would rewrite whole
    partitions for what is usually a handful of rows.
    """
    client = dw_client()
    try:
        # Fetch the rows being deleted so the tombstone carries the same
        # values; a tombstone with zeroed measures would win the merge and
        # leave a corrupted row rather than a hidden one.
        conditions = []
        for order_id, product_key in deleted_pairs:
            if product_key is None:
                conditions.append(f"order_id = {order_id}")
            else:
                conditions.append(
                    f"(order_id = {order_id} AND product_key IN "
                    f"(SELECT product_key FROM DimProducts FINAL "
                    f" WHERE product_alternate_key = {product_key}))"
                )

        where = " OR ".join(conditions)
        columns = [c for c in FACT_COLUMNS if c != "is_deleted"]
        column_list = ", ".join(columns)

        rows = client.query(
            f"SELECT {column_list} FROM {TARGET_TABLE} FINAL "
            f"WHERE ({where}) AND is_deleted = 0"
        ).result_rows

        if not rows:
            log.info("Nothing to tombstone")
            return 0

        tombstones = [row + (1,) for row in rows]
        client.insert(TARGET_TABLE, tombstones, column_names=columns + ["is_deleted"])
        log.info("Tombstoned %s fact row(s)", len(tombstones))
        return len(tombstones)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _report_inferred() -> None:
    """Log how many placeholder dimension rows are outstanding.

    A number that keeps climbing across runs means the dimension load is not
    catching up, and someone will eventually see blank labels on a chart.
    """
    for table in ("DimCustomer", "DimProducts", "DimEmployees", "DimShippers"):
        count = count_inferred_members(table)
        if count:
            log.warning("%s holds %s inferred member(s) awaiting enrichment",
                        table, count)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

LOADERS = {
    "initial": initial_load,
    "incremental": incremental_load,
}


def run_one(name: str) -> int:
    if name not in LOADERS:
        raise ValueError(f"Unknown loader '{name}'. Available: {sorted(LOADERS)}")

    with spark_session(f"fact_orders_{name}") as spark:
        return LOADERS[name](spark)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "initial"
    run_one(mode)
