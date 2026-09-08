"""Shared helpers: Spark session construction and ClickHouse access.

Every job in this project goes through here rather than building its own
session, so driver paths, memory limits and shuffle settings are decided
once.
"""

from __future__ import annotations

import glob
import logging
from contextlib import contextmanager
from typing import Iterator

import clickhouse_connect
from pyspark.sql import DataFrame, SparkSession

from .config import DW, SPARK_JARS_DIR

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
# ClickHouse
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
    """
    rows = [tuple(row[c] for c in columns) for row in df.collect()]
    if not rows:
        log.warning("Nothing to insert into %s", table)
        return 0

    client = dw_client()
    try:
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
