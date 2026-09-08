"""Shared helpers: Spark session, JDBC access, and ClickHouse access.

Every job in this project goes through here rather than building its own
session or connection, so driver paths, memory limits and shuffle settings
are decided once.
"""

from __future__ import annotations

import glob
import logging
from contextlib import contextmanager
from typing import Iterator
from datetime import date, datetime

import clickhouse_connect
import psycopg2
from pyspark.sql import DataFrame, SparkSession

from .config import DW, OP, SPARK_JARS_DIR, STAGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
)
log = logging.getLogger("northwind")


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

def _jar_paths() -> str:
    jars = sorted(glob.glob(f"{SPARK_JARS_DIR}/*.jar"))
    if not jars:
        log.warning("No JDBC jars found in %s — JDBC reads will fail", SPARK_JARS_DIR)
    return ",".join(jars)


@contextmanager
def spark_session(app_name: str) -> Iterator[SparkSession]:
    """A local-mode Spark session, stopped on exit.

    Local mode is deliberate. The brief specifies PySpark inside a Python
    operator, not a separate cluster, and Northwind is small enough that a
    cluster would cost more in coordination than it saves in compute.

    shuffle.partitions defaults to 200, which on a dataset of this size
    produces hundreds of near-empty tasks. Four matches the core count and
    cuts job time noticeably.
    """
    session = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars", _jar_paths())
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    log.info("Spark session started: %s", app_name)
    try:
        yield session
    finally:
        session.stop()
        log.info("Spark session stopped: %s", app_name)


# ---------------------------------------------------------------------------
# OP layer — SQL Server, read only
# ---------------------------------------------------------------------------

def read_from_op(spark: SparkSession, query: str) -> DataFrame:
    """Run a query against SQL Server and return the result.

    The query is wrapped as a subquery rather than passed as a table name,
    which lets the source system do the joining and filtering. Pulling whole
    tables into Spark and joining there would move far more data across the
    wire for no benefit at this size.
    """
    df = (
        spark.read
        .format("jdbc")
        .option("url", OP.jdbc_url)
        .option("query", query)
        .options(**OP.jdbc_properties)
        .load()
    )
    log.info("Read %s rows from OP", f"{df.count():,}")
    return df


# ---------------------------------------------------------------------------
# Staging layer — PostgreSQL
# ---------------------------------------------------------------------------

def truncate_staging_table(table: str) -> None:
    """Empty a staging table.

    Staging keeps no history: every run starts from an empty table so a
    failed previous run cannot leave rows behind to be counted twice.
    """
    conn = psycopg2.connect(
        host=STAGING.host,
        port=STAGING.port,
        dbname=STAGING.database,
        user=STAGING.user,
        password=STAGING.password,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")
        log.info("Truncated %s", table)
    finally:
        conn.close()


def write_to_staging(df: DataFrame, table: str) -> int:
    """Append a DataFrame to a staging table.

    Mode is append, not overwrite: overwrite drops and recreates the table,
    losing the column types and indexes defined in the DDL. The table is
    emptied beforehand with truncate_staging_table instead.
    """
    row_count = df.count()
    (
        df.write
        .format("jdbc")
        .option("url", STAGING.jdbc_url)
        .option("dbtable", table)
        .options(**STAGING.jdbc_properties)
        .option("batchsize", "5000")
        .mode("append")
        .save()
    )
    log.info("Wrote %s rows to %s", f"{row_count:,}", table)
    return row_count


def read_from_staging(spark: SparkSession, query: str) -> DataFrame:
    """Run a query against the staging database and return the result."""
    return (
        spark.read
        .format("jdbc")
        .option("url", STAGING.jdbc_url)
        .option("query", query)
        .options(**STAGING.jdbc_properties)
        .load()
    )


# ---------------------------------------------------------------------------
# DW layer — ClickHouse
# ---------------------------------------------------------------------------

def dw_client():
    """A ClickHouse client bound to the DW database."""
    return clickhouse_connect.get_client(
        host=DW.host,
        port=DW.port,
        username=DW.user,
        password=DW.password,
        database=DW.database,
    )


def write_to_dw(
    df: DataFrame,
    table: str,
    columns: list[str],
    batch_size: int = 50_000,
) -> int:
    """Insert a Spark DataFrame into a ClickHouse table.

    The DataFrame is collected to the driver first. That is acceptable here
    and nowhere near a general solution: Northwind's largest table is a few
    thousand rows. For anything genuinely large this would stream through
    partitions instead.

    Column order is passed explicitly rather than inferred, because
    ClickHouse matches inserts positionally and a silent reordering would
    corrupt the load without raising anything.

    Nulls are replaced with the zero value of the target column's type.
    Almost every column in the star schema is non-Nullable by design — a
    Nullable column in ClickHouse costs an extra stored column — so a null
    arriving from a source that permits them would otherwise abort the whole
    insert. Doing the substitution here means each loader does not have to
    remember a COALESCE for every nullable source column.
    """
    if not columns:
        return 0

    # Ask the target what each column's type is, so the replacement matches.
    client = dw_client()
    try:
        schema = {
            row[0]: row[1]
            for row in client.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = currentDatabase() AND table = '{table}'"
            ).result_rows
        }

        def zero_value(column: str):
            column_type = schema.get(column, "String")
            if column_type.startswith("Nullable"):
                return None
            if "Int" in column_type or "Float" in column_type or "Decimal" in column_type:
                return 0
            if "Date32" in column_type:
                return date(1900, 1, 1)
            if "DateTime" in column_type:
                return datetime(1970, 1, 1)
            if "Date" in column_type:
                return date(1970, 1, 1)
            return ""

        defaults = {c: zero_value(c) for c in columns}

        rows = [
            tuple(
                defaults[c] if row[c] is None else row[c]
                for c in columns
            )
            for row in df.collect()
        ]

        if not rows:
            log.warning("Nothing to insert into %s", table)
            return 0

        for start in range(0, len(rows), batch_size):
            client.insert(table, rows[start:start + batch_size], column_names=columns)
        log.info("Inserted %s rows into %s", f"{len(rows):,}", table)
        return len(rows)
    finally:
        client.close()


def truncate_dw_table(table: str) -> None:
    client = dw_client()
    try:
        client.command(f"TRUNCATE TABLE IF EXISTS {table}")
        log.info("Truncated %s", table)
    finally:
        client.close()
