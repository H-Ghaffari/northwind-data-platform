"""OP → Staging for the fact tables.

Stage one of two, mirroring packages 11 and 12 of the reference SSIS
project. Two things land here:

  1. The CDC change window, split by operation, into six tables:
     staging_orders_insert / _update / _delete
     staging_order_details_insert / _update / _delete

  2. A snapshot of the orders and lines those changes touch, into
     staging_orders and staging_order_details.

The snapshot exists because the change tables only ever carry one side of
the join. A changed freight value puts a row into staging_orders_update but
nothing into the order-details tables — yet every line of that order needs
rewriting, because the fact grain repeats the parent's attributes across
all of them. The snapshot is where stage two finds the other side.

Crucially the snapshot covers only the affected orders, not the whole
table. Copying all of Orders every thirty minutes would defeat the point of
reading CDC at all: the pipeline would be doing a full extract on an
incremental schedule, and the cost would grow with the table rather than
with the change rate. An initial load asks for everything explicitly.

The reference implementation solves the same problem by looking the parent
up in Orders directly from the warehouse package. Landing it in staging
instead keeps stage two free of any dependency on the source system, so a
failure there is retried without touching SQL Server again.

The watermark is deliberately NOT advanced here. If it moved at the end of
this stage and the warehouse load then failed, those changes would be lost
for good — CDC does not return a window that has already been passed.
Stage two advances it once the rows have actually landed.
"""

from __future__ import annotations

import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.cdc_reader import (
    cdc_is_healthy,
    get_cdc_window,
    read_changes,
    split_by_operation,
)
from common.spark_utils import (
    log,
    read_from_op,
    spark_session,
    truncate_staging_table,
    write_to_staging,
)

# Columns pulled from each capture instance, in the order the staging tables
# expect them. Stated explicitly rather than selected with *: the change
# table also carries CDC's own metadata columns, and a positional mismatch
# would corrupt the load without erroring.
ORDER_COLUMNS = [
    "OrderID", "CustomerID", "EmployeeID", "OrderDate", "RequiredDate",
    "ShippedDate", "ShipVia", "Freight", "ShipName", "ShipAddress",
    "ShipCity", "ShipRegion", "ShipPostalCode", "ShipCountry",
]

DETAIL_COLUMNS = ["OrderID", "ProductID", "UnitPrice", "Quantity", "Discount"]

# Money and float values arrive from pymssql as Decimal and float. Spark
# cannot infer a single type for them across a mixed batch, so they are
# declared as strings here and cast back immediately before the write —
# see _cast_numerics below.
ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("required_date", TimestampType(), True),
    StructField("shipped_date", TimestampType(), True),
    StructField("ship_via", IntegerType(), True),
    StructField("freight", StringType(), True),
    StructField("ship_name", StringType(), True),
    StructField("ship_address", StringType(), True),
    StructField("ship_city", StringType(), True),
    StructField("ship_region", StringType(), True),
    StructField("ship_postal_code", StringType(), True),
    StructField("ship_country", StringType(), True),
])

DETAIL_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("unit_price", StringType(), True),
    StructField("quantity", ShortType(), True),
    StructField("discount", StringType(), True),
])

# What each string-carried column has to become before it reaches
# PostgreSQL. The JDBC driver will not coerce a varchar into a numeric
# column; it raises rather than guessing, which is the right behaviour and
# the reason this mapping exists.
ORDER_NUMERICS = {"freight": "decimal(19,4)"}
DETAIL_NUMERICS = {"unit_price": "decimal(19,4)", "discount": "float"}

CDC_TABLES = [
    "staging_orders_insert",
    "staging_orders_update",
    "staging_orders_delete",
    "staging_order_details_insert",
    "staging_order_details_update",
    "staging_order_details_delete",
]

ORDERS_SELECT = """
    SELECT
        OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate,
        ShipVia,
        ISNULL(Freight, 0)        AS Freight,
        ISNULL(ShipName,'')       AS ShipName,
        ISNULL(ShipAddress,'')    AS ShipAddress,
        ISNULL(ShipCity,'')       AS ShipCity,
        ISNULL(ShipRegion,'')     AS ShipRegion,
        ISNULL(ShipPostalCode,'') AS ShipPostalCode,
        ISNULL(ShipCountry,'')    AS ShipCountry
    FROM Orders
"""

DETAILS_SELECT = """
    SELECT OrderID, ProductID, UnitPrice, Quantity, Discount
    FROM [Order Details]
"""

ORDER_STAGING_COLUMNS = [
    "order_id", "customer_id", "employee_id", "order_date", "required_date",
    "shipped_date", "ship_via", "freight", "ship_name", "ship_address",
    "ship_city", "ship_region", "ship_postal_code", "ship_country",
]

DETAIL_STAGING_COLUMNS = [
    "order_id", "product_id", "unit_price", "quantity", "discount",
]


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------

def load_snapshot(spark: SparkSession, order_ids: set[int] | None = None) -> dict[str, int]:
    """Land orders and their lines into staging.

    order_ids None means everything — used by the initial load. A set means
    only those orders, which is what the incremental path passes so the
    extract stays proportional to the change rate rather than the table
    size.

    An empty set is not the same as None: it means "changes were reported
    but none of them name an order", and the correct response is to land
    nothing rather than everything.

    These rows come through the JDBC reader, which types the columns from
    the source metadata, so no casting is needed here — unlike the CDC path,
    which goes through pymssql.
    """
    counts: dict[str, int] = {}

    truncate_staging_table("staging_orders")
    truncate_staging_table("staging_order_details")

    if order_ids is not None and not order_ids:
        log.info("No affected orders — snapshot left empty")
        return {"staging_orders": 0, "staging_order_details": 0}

    if order_ids is None:
        orders_query = ORDERS_SELECT
        details_query = DETAILS_SELECT
        log.info("Snapshotting every order")
    else:
        id_list = ",".join(str(o) for o in sorted(order_ids))
        orders_query = f"{ORDERS_SELECT} WHERE OrderID IN ({id_list})"
        details_query = f"{DETAILS_SELECT} WHERE OrderID IN ({id_list})"
        log.info("Snapshotting %s affected order(s)", len(order_ids))

    orders = read_from_op(spark, orders_query).toDF(*ORDER_STAGING_COLUMNS)
    counts["staging_orders"] = write_to_staging(orders, "staging_orders")

    details = read_from_op(spark, details_query).toDF(*DETAIL_STAGING_COLUMNS)
    counts["staging_order_details"] = write_to_staging(details, "staging_order_details")

    return counts


# ---------------------------------------------------------------------------
# CDC window
# ---------------------------------------------------------------------------

def _to_dataframe(spark: SparkSession, rows: list[tuple], schema: StructType) -> DataFrame:
    """Build a DataFrame from CDC rows, normalising awkward types.

    pymssql hands back Decimals, floats and space-padded CHAR values. Each
    is flattened to something Spark can hold in a uniform column; the
    numerics are restored by _cast_numerics before the write.
    """
    if not rows:
        return spark.createDataFrame([], schema)

    normalised = []
    for row in rows:
        values = []
        for value in row:
            if isinstance(value, (bytes, bytearray)):
                values.append(value.decode("utf-8", errors="replace"))
            elif hasattr(value, "quantize"):          # Decimal
                values.append(str(value))
            elif isinstance(value, float):
                values.append(str(value))
            elif isinstance(value, str):
                values.append(value.rstrip())          # CHAR padding
            else:
                values.append(value)
        normalised.append(tuple(values))

    return spark.createDataFrame(normalised, schema)


def _cast_numerics(df: DataFrame, numeric_columns: dict[str, str]) -> DataFrame:
    """Cast the string-carried numerics back to their target types.

    Applied immediately before the write. PostgreSQL's JDBC driver refuses
    to coerce a varchar into a numeric column — it raises rather than
    guessing, which is correct but means the cast has to be explicit.
    """
    for column, target in numeric_columns.items():
        df = df.withColumn(column, F.col(column).cast(target))
    return df


def load_cdc_window(spark: SparkSession) -> dict:
    """Land the current CDC window into the six operation tables.

    Returns the window plus the set of order ids it touched, which the
    caller uses to scope the snapshot.
    """
    if not cdc_is_healthy():
        raise RuntimeError("CDC is not healthy — see the log above")

    # Truncate unconditionally, before reading. A table still holding rows
    # from a previous run is worse than an empty one: stage two cannot tell
    # the two apart and would reapply them.
    for table in CDC_TABLES:
        truncate_staging_table(table)

    order_window = get_cdc_window("dbo_Orders")
    detail_window = get_cdc_window("dbo_OrderDetails")

    counts: dict[str, int] = {t: 0 for t in CDC_TABLES}
    affected: set[int] = set()

    if not order_window.is_empty:
        split = split_by_operation(read_changes(order_window, ORDER_COLUMNS))
        for operation in ("insert", "update", "delete"):
            rows = split[operation]
            if not rows:
                continue
            table = f"staging_orders_{operation}"
            df = _cast_numerics(
                _to_dataframe(spark, rows, ORDER_SCHEMA), ORDER_NUMERICS
            )
            counts[table] = write_to_staging(df, table)
            # A deleted order is gone from the source, so snapshotting it
            # would return nothing. Stage two handles those from the
            # warehouse side instead.
            if operation != "delete":
                affected.update(row[0] for row in rows)

    if not detail_window.is_empty:
        split = split_by_operation(read_changes(detail_window, DETAIL_COLUMNS))
        for operation in ("insert", "update", "delete"):
            rows = split[operation]
            if not rows:
                continue
            table = f"staging_order_details_{operation}"
            df = _cast_numerics(
                _to_dataframe(spark, rows, DETAIL_SCHEMA), DETAIL_NUMERICS
            )
            counts[table] = write_to_staging(df, table)
            # A changed line still needs its parent order, including when
            # the line itself was deleted — the tombstone carries the
            # order's attributes.
            affected.update(row[0] for row in rows)

    return {
        "orders_to_lsn": order_window.to_lsn.hex() if not order_window.is_empty else None,
        "details_to_lsn": detail_window.to_lsn.hex() if not detail_window.is_empty else None,
        "counts": counts,
        "total_changes": sum(counts.values()),
        "affected_orders": sorted(affected),
        "read_at": datetime.utcnow().isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run_incremental() -> dict:
    """Land the change window, then snapshot only what it touched.

    Order matters: the CDC read comes first because its result determines
    which orders the snapshot needs. Snapshotting first would mean either
    copying everything or guessing.
    """
    with spark_session("op_to_staging_facts_incremental") as spark:
        window = load_cdc_window(spark)
        snapshot = load_snapshot(spark, set(window["affected_orders"]))

    log.info("Change window:")
    for table, count in window["counts"].items():
        log.info("  %-32s %s rows", table, f"{count:,}")

    log.info("Snapshot (affected orders only):")
    for table, count in snapshot.items():
        log.info("  %-32s %s rows", table, f"{count:,}")

    window["snapshot"] = snapshot
    return window


def run_initial() -> dict:
    """Snapshot every order, for the one-off seed.

    The CDC window is read and its LSNs returned so the caller may set the
    watermark, but the change tables are irrelevant here: an initial load
    replaces the fact table wholesale.
    """
    with spark_session("op_to_staging_facts_initial") as spark:
        window = load_cdc_window(spark)
        snapshot = load_snapshot(spark, order_ids=None)

    log.info("Snapshot (full):")
    for table, count in snapshot.items():
        log.info("  %-32s %s rows", table, f"{count:,}")

    window["snapshot"] = snapshot
    return window


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "incremental"
    result = run_initial() if mode == "initial" else run_incremental()
    log.info(
        "Window ends at orders=%s details=%s",
        result["orders_to_lsn"], result["details_to_lsn"],
    )
    sys.exit(0)
