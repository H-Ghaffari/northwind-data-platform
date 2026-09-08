"""SCD engine shared by every dimension load.

ClickHouse makes UPDATE expensive — a mutation rewrites whole parts — so
none of the three SCD outcomes is expressed as an update here. Everything
is an insert, and ReplacingMergeTree resolves the result:

    new row        insert with a fresh surrogate key
    type 1 change  insert over the same (alternate_key, start_date) with a
                   higher _version; the merge discards the older copy
    type 2 change  insert the existing row again with end_date set, then
                   insert a new row with a fresh key and an open end_date

The pattern is append-only merge, and it is the normal way to do slowly
changing dimensions on a columnar store.

Reads use FINAL so a load never sees a half-merged picture of its own
previous run.
"""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .spark_utils import dw_client, log

# The open-ended sentinel. See the DW DDL for why this is not NULL.
OPEN_END_DATE = datetime(2106, 1, 1, 0, 0, 0)


# ---------------------------------------------------------------------------
# Surrogate keys
# ---------------------------------------------------------------------------

def next_surrogate_key(table: str, key_column: str) -> int:
    """The next free surrogate key for a dimension.

    Read from the target rather than held in a sequence table: the dimension
    itself is the only thing that can say authoritatively which keys are
    taken, and a separate counter would drift the moment a load was rerun.
    """
    client = dw_client()
    try:
        result = client.query(f"SELECT max({key_column}) FROM {table}")
        current_max = result.result_rows[0][0] if result.result_rows else None
        return int(current_max or 0) + 1
    finally:
        client.close()


def assign_surrogate_keys(
    df: DataFrame,
    key_column: str,
    start_from: int,
    order_by: str,
) -> DataFrame:
    """Number the rows of a DataFrame from start_from upwards.

    Ordered by the natural key so a rerun on unchanged input produces the
    same assignment, which keeps reruns comparable when debugging.
    """
    from pyspark.sql.window import Window

    window = Window.orderBy(order_by)
    return df.withColumn(
        key_column,
        (F.row_number().over(window) + F.lit(start_from - 1)).cast("int"),
    )


# ---------------------------------------------------------------------------
# Reading the current state of a dimension
# ---------------------------------------------------------------------------

def load_current_dimension(
    spark: SparkSession,
    table: str,
    columns: Sequence[str],
) -> DataFrame:
    """The currently-open rows of a dimension, as a Spark DataFrame.

    FINAL is applied because a previous run may have inserted replacement
    rows that have not merged yet. Without it, a dimension can appear to
    hold two open rows for the same key and the comparison below would
    produce spurious type 2 changes.
    """
    client = dw_client()
    try:
        column_list = ", ".join(columns)
        query = (
            f"SELECT {column_list} FROM {table} FINAL "
            f"WHERE end_date = toDateTime('2106-01-01 00:00:00')"
            if "end_date" in columns
            else f"SELECT {column_list} FROM {table} FINAL"
        )
        result = client.query(query)
        rows = result.result_rows
    finally:
        client.close()

    if not rows:
        log.info("%s is empty — this is an initial load", table)
        return None

    log.info("Read %s current rows from %s", f"{len(rows):,}", table)
    return spark.createDataFrame(rows, schema=list(columns))


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------

def apply_scd(
    incoming: DataFrame,
    current: DataFrame | None,
    business_key: str,
    type1_columns: Sequence[str],
    type2_columns: Sequence[str],
    run_time: datetime,
) -> dict[str, DataFrame]:
    """Split incoming rows into new, type-1 changed, and type-2 changed.

    Returns three DataFrames keyed 'new', 'type1' and 'type2'. Unchanged
    rows appear in none of them: reinserting a row that has not changed
    would produce merge work and a misleading row count for no benefit.

    Comparison is on value, not on a hash. Northwind's dimensions are small
    enough that the extra columns cost nothing, and a value comparison says
    which attribute changed when something looks wrong.
    """
    if current is None:
        return {
            "new": incoming,
            "type1": incoming.limit(0),
            "type2": incoming.limit(0),
        }

    # Prefix the current-state columns so both sides can sit in one row.
    current_prefixed = current
    for column in current.columns:
        if column != business_key:
            current_prefixed = current_prefixed.withColumnRenamed(column, f"cur_{column}")

    joined = incoming.join(current_prefixed, on=business_key, how="left")

    # A row is new when nothing matched on the business key.
    marker = f"cur_{type1_columns[0] if type1_columns else type2_columns[0]}"
    is_new = F.col(marker).isNull()

    def changed(columns: Sequence[str]):
        """True when any of the given columns differs from the stored value.

        eqNullSafe rather than != so that a NULL on either side counts as a
        difference instead of evaluating to NULL and being treated as no
        change.
        """
        if not columns:
            return F.lit(False)
        condition = ~F.col(columns[0]).eqNullSafe(F.col(f"cur_{columns[0]}"))
        for column in columns[1:]:
            condition = condition | ~F.col(column).eqNullSafe(F.col(f"cur_{column}"))
        return condition

    type2_changed = changed(type2_columns)
    type1_changed = changed(type1_columns)

    incoming_columns = incoming.columns

    new_rows = joined.filter(is_new).select(*incoming_columns)

    # Type 2 takes precedence: a row whose historical attributes changed gets
    # a new version, and that new version carries the current type 1 values
    # anyway, so treating it as both would double-count it.
    type2_rows = joined.filter(~is_new & type2_changed).select(
        *incoming_columns,
        *[F.col(f"cur_{c}").alias(f"cur_{c}") for c in current.columns if c != business_key],
    )

    type1_rows = joined.filter(~is_new & ~type2_changed & type1_changed).select(
        *incoming_columns,
        *[F.col(f"cur_{c}").alias(f"cur_{c}") for c in current.columns if c != business_key],
    )

    counts = {
        "new": new_rows.count(),
        "type1": type1_rows.count(),
        "type2": type2_rows.count(),
    }
    log.info(
        "SCD: %s new, %s type-1 changes, %s type-2 changes",
        counts["new"], counts["type1"], counts["type2"],
    )

    return {"new": new_rows, "type1": type1_rows, "type2": type2_rows}


# ---------------------------------------------------------------------------
# Geography lookup, shared by three dimensions
# ---------------------------------------------------------------------------

def resolve_geography_keys(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Attach geography_key to a DataFrame carrying address columns.

    The address tuple is the natural key: DimGeography has no source key of
    its own. Rows whose address is not found get geography_key 0 rather than
    being dropped — losing a customer because its address is missing from a
    lookup table would be far worse than an unresolved key.

    The staging layer has already normalised NULLs to empty strings, so a
    plain equi-join is safe here.
    """
    client = dw_client()
    try:
        result = client.query(
            "SELECT geography_key, country, region, city, postal_code, address "
            "FROM DimGeography FINAL"
        )
        rows = result.result_rows
    finally:
        client.close()

    if not rows:
        log.warning("DimGeography is empty — all geography keys will be 0")
        return df.withColumn("geography_key", F.lit(0).cast("int"))

    geography = spark.createDataFrame(
        rows,
        schema=["geography_key", "g_country", "g_region", "g_city",
                "g_postal_code", "g_address"],
    )

    resolved = df.join(
        geography,
        on=(
            (df["country"] == geography["g_country"])
            & (df["region"] == geography["g_region"])
            & (df["city"] == geography["g_city"])
            & (df["postal_code"] == geography["g_postal_code"])
            & (df["address"] == geography["g_address"])
        ),
        how="left",
    ).drop("g_country", "g_region", "g_city", "g_postal_code", "g_address")

    unresolved = resolved.filter(F.col("geography_key").isNull()).count()
    if unresolved:
        log.warning("%s rows could not resolve a geography_key", unresolved)

    return resolved.withColumn(
        "geography_key", F.coalesce(F.col("geography_key"), F.lit(0)).cast("int")
    )
