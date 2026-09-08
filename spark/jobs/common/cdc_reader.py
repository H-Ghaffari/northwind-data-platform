"""Read changes out of SQL Server CDC and track how far the pipeline got.

The watermark lives in ETL_Settings.CDC_State, one row per capture
instance, and is only advanced after a load has succeeded. That ordering is
the whole safety property: a run that dies halfway leaves the watermark
where it was, so the next run reprocesses the same window rather than
skipping it. Reprocessing is harmless because the fact table deduplicates
on (order_id, product_key); skipping would lose data silently.

LSNs are stored as hex strings rather than binary. The reference SSIS
implementation stored a composite text token in the same column, and hex
keeps the value readable when someone inspects the table by hand.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pymssql

from .config import OP
from .spark_utils import log


@dataclass
class CdcWindow:
    """The LSN range a single run will process."""

    capture_instance: str
    from_lsn: bytes
    to_lsn: bytes

    @property
    def is_empty(self) -> bool:
        return self.from_lsn >= self.to_lsn


def _connect():
    """A direct connection to SQL Server.

    CDC access goes through stored functions rather than plain selects, so
    it is done with pymssql instead of Spark's JDBC reader.
    """
    return pymssql.connect(
        server=OP.host,
        port=str(OP.port),
        user=OP.user,
        password=OP.password,
        database=OP.database,
    )


def _connect_settings():
    """A connection to the bookkeeping database."""
    return pymssql.connect(
        server=OP.host,
        port=str(OP.port),
        user=OP.user,
        password=OP.password,
        database="ETL_Settings",
    )


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

def read_watermark(capture_instance: str) -> bytes | None:
    """The last LSN successfully processed for a capture instance.

    None means the instance has never been processed, which the caller
    treats as "start from the beginning of the change table".
    """
    conn = _connect_settings()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT state FROM dbo.CDC_State WHERE name = %s",
                (capture_instance,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or not row[0]:
        log.info("No watermark for %s — starting from the beginning", capture_instance)
        return None

    return bytes.fromhex(row[0])


def write_watermark(capture_instance: str, lsn: bytes) -> None:
    """Advance the watermark. Called only after a successful load."""
    conn = _connect_settings()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE dbo.CDC_State "
                "SET state = %s, last_run_time = %s "
                "WHERE name = %s",
                (lsn.hex(), datetime.utcnow(), capture_instance),
            )
        conn.commit()
        log.info("Watermark for %s advanced to %s", capture_instance, lsn.hex())
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Reading changes
# ---------------------------------------------------------------------------

def get_cdc_window(capture_instance: str) -> CdcWindow:
    """The LSN range available to process right now.

    to_lsn is the maximum LSN the capture job has written so far, not the
    current time. Reading past it would return rows the capture job has not
    finished writing.
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
            max_lsn = cur.fetchone()[0]

            if max_lsn is None:
                raise RuntimeError(
                    "fn_cdc_get_max_lsn() returned NULL. The CDC capture job "
                    "has never run — check that SQL Server Agent is started."
                )

            stored = read_watermark(capture_instance)
            if stored is None:
                cur.execute(
                    "SELECT sys.fn_cdc_get_min_lsn(%s)", (capture_instance,)
                )
                from_lsn = cur.fetchone()[0]
            else:
                # Start just past what was already processed.
                cur.execute("SELECT sys.fn_cdc_increment_lsn(%s)", (stored,))
                from_lsn = cur.fetchone()[0]
    finally:
        conn.close()

    window = CdcWindow(capture_instance, from_lsn, max_lsn)
    if window.is_empty:
        log.info("%s: no new changes", capture_instance)
    else:
        log.info(
            "%s: window %s .. %s",
            capture_instance, from_lsn.hex(), max_lsn.hex(),
        )
    return window


def read_changes(window: CdcWindow, columns: list[str]) -> list[tuple]:
    """Every change row in the window, as (operation, *columns) tuples.

    Operation 3 — the pre-image of an update — is filtered out. It records
    what a row looked like before the change, which the warehouse has no use
    for: the post-image (operation 4) carries the new state.

    'all' rather than 'all update old' as the row filter, since the
    pre-image is not wanted.
    """
    if window.is_empty:
        return []

    column_list = ", ".join(f"[{c}]" for c in columns)
    function = f"cdc.fn_cdc_get_all_changes_{window.capture_instance}"

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT __$operation, {column_list} "
                f"FROM {function}(%s, %s, N'all') "
                f"WHERE __$operation <> 3 "
                f"ORDER BY __$start_lsn, __$seqval",
                (window.from_lsn, window.to_lsn),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    log.info("%s: read %s change rows", window.capture_instance, f"{len(rows):,}")
    return rows


def split_by_operation(rows: list[tuple]) -> dict[str, list[tuple]]:
    """Group change rows into inserts, updates and deletes.

    The operation code is dropped from each tuple on the way out, so what
    comes back matches the shape of the staging tables.
    """
    result: dict[str, list[tuple]] = {"insert": [], "update": [], "delete": []}
    for row in rows:
        operation, payload = row[0], row[1:]
        if operation == 2:
            result["insert"].append(payload)
        elif operation == 4:
            result["update"].append(payload)
        elif operation == 1:
            result["delete"].append(payload)

    log.info(
        "Split: %s inserts, %s updates, %s deletes",
        len(result["insert"]), len(result["update"]), len(result["delete"]),
    )
    return result


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def cdc_is_healthy() -> bool:
    """Whether CDC is enabled and the capture job has actually run.

    Enabled-but-never-captured is the failure worth catching: it produces no
    error and no rows, which is indistinguishable from a quiet period unless
    checked explicitly.
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT is_cdc_enabled FROM sys.databases WHERE name = %s",
                (OP.database,),
            )
            row = cur.fetchone()
            if not row or not row[0]:
                log.error("CDC is not enabled on %s", OP.database)
                return False

            cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
            if cur.fetchone()[0] is None:
                log.error(
                    "CDC is enabled but no LSN has been captured. "
                    "SQL Server Agent is probably not running."
                )
                return False

            cur.execute("SELECT COUNT(*) FROM cdc.change_tables")
            instance_count = cur.fetchone()[0]
            log.info("CDC healthy: %s capture instances", instance_count)
            return instance_count > 0
    finally:
        conn.close()
