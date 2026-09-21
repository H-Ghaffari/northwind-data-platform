"""SQL Server access: the CDC change tables and the stream watermark."""
from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from typing import Iterator

import pymssql

from .config import OP

log = logging.getLogger(__name__)

# Capture instance names are interpolated into function names, because
# cdc.fn_cdc_get_all_changes_<instance> is part of the identifier and cannot
# be a bind parameter. They come from our own seeded table, but validating
# them keeps that assumption from quietly becoming an injection point if the
# table is ever written to from somewhere else.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")


def _require_safe(identifier: str) -> str:
    if not _SAFE_IDENTIFIER.match(identifier):
        raise ValueError(f"unsafe capture instance name: {identifier!r}")
    return identifier


# One long-lived connection per database, reopened only after a failure.
_connections: dict[str, pymssql.Connection] = {}


def _open(database: str) -> pymssql.Connection:
    return pymssql.connect(
        server=OP.host,
        port=str(OP.port),
        user=OP.user,
        password=OP.password,
        database=database,
        # Autocommit matters more with a long-lived connection than a short
        # one: an implicit transaction left open would hold its locks for
        # the life of the process instead of the life of one query.
        autocommit=True,
        timeout=30,
        login_timeout=15,
    )


@contextmanager
def connect(database: str) -> Iterator[pymssql.Connection]:
    """A reused connection to one database.

    The first version opened and closed a connection for every query. Each
    pass issues at least twelve, and every one paid for a TCP handshake,
    encryption negotiation and a login before running anything. Packetbeat
    recorded the result on an idle system: about 8,300 connections and
    200 KB/s to SQL Server — the cost of asking "anything new?" was mostly
    the cost of saying hello. With the connections reused it is two
    connections and about 14 KB/s. Nothing functional showed the problem;
    data arrived correctly and on time. Only watching the wire did.

    On any error the connection is discarded and the next call reconnects.
    That covers a restarted SQL Server as well as a connection left in an
    unknown state by a failed statement, and a reconnect is cheap next to
    reasoning about which failures leave a connection reusable.
    """
    conn = _connections.get(database)
    if conn is None:
        conn = _open(database)
        _connections[database] = conn
    try:
        yield conn
    except Exception:
        _connections.pop(database, None)
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
        raise


def close_all() -> None:
    """Close every pooled connection. Called once, at shutdown."""
    for database, conn in list(_connections.items()):
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
        _connections.pop(database, None)


# ---------------------------------------------------------------------------
# Stream_State — the producer's watermark
# ---------------------------------------------------------------------------

def load_active_sources() -> list[dict]:
    """Every source the producer should poll, newest configuration first.

    Read on every pass rather than cached at startup, so flipping is_active
    in the table takes effect within one poll interval instead of needing a
    restart.
    """
    with connect(OP.settings_db) as conn:
        with conn.cursor(as_dict=True) as cur:
            cur.execute(
                """
                SELECT capture_instance, last_lsn, propagate_deletes,
                       topic_name, rows_published
                FROM dbo.Stream_State
                WHERE is_active = 1
                ORDER BY capture_instance
                """
            )
            return cur.fetchall()


def advance_watermark(
    capture_instance: str,
    lsn: bytes,
    rows: int,
    lsn_time=None,
) -> None:
    """Move the watermark forward. Called only after Kafka has acknowledged.

    lsn_time is passed in rather than derived here. fn_cdc_map_lsn_to_time
    reads cdc.lsn_time_mapping, which lives in Northwind — and CDC functions
    resolve against the current database with no way to qualify them, so
    calling it on an ETL_Settings connection fails. The caller already holds
    the commit time from the change row it read, so the value travels with
    the watermark instead of being looked up a second time.
    """
    with connect(OP.settings_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dbo.Stream_State
                SET last_lsn       = %s,
                    last_lsn_time  = %s,
                    rows_published = rows_published + %s,
                    last_run_at    = SYSDATETIME(),
                    last_error     = NULL
                WHERE capture_instance = %s
                """,
                (lsn, lsn_time, rows, capture_instance),
            )


def record_error(capture_instance: str, message: str) -> None:
    """Persist a failure beside the watermark it affected.

    A traceback in the container log disappears with the container. A source
    that has been failing for an hour should be visible by querying the same
    table that says how far it has read.
    """
    with connect(OP.settings_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dbo.Stream_State
                SET last_error  = %s,
                    last_run_at = SYSDATETIME()
                WHERE capture_instance = %s
                """,
                (message[:2000], capture_instance),
            )


# ---------------------------------------------------------------------------
# CDC change tables
# ---------------------------------------------------------------------------

def read_changes(
    capture_instance: str,
    last_lsn: bytes | None,
    max_rows: int,
) -> tuple[list[dict], bytes | None, str | None]:
    """Read the next window of changes for one capture instance.

    Returns (rows, window_end_lsn, warning). rows is empty when there is
    nothing new, which is the normal state most of the time.
    """
    _require_safe(capture_instance)

    with connect(OP.database) as conn:
        with conn.cursor(as_dict=True) as cur:

            cur.execute(
                "SELECT sys.fn_cdc_get_min_lsn(%s) AS min_lsn,"
                "       sys.fn_cdc_get_max_lsn()   AS max_lsn",
                (capture_instance,),
            )
            bounds = cur.fetchone()
            min_lsn, max_lsn = bounds["min_lsn"], bounds["max_lsn"]

            if min_lsn is None:
                return [], None, f"{capture_instance} is not captured"
            if max_lsn is None:
                # No transaction has been committed since CDC was enabled.
                return [], None, None

            warning = None

            if last_lsn is None:
                from_lsn = min_lsn
            else:
                cur.execute(
                    "SELECT sys.fn_cdc_increment_lsn(%s) AS next_lsn",
                    (last_lsn,),
                )
                from_lsn = cur.fetchone()["next_lsn"]

                # SQL Server's cleanup job removes change rows older than its
                # retention window, three days by default. If the producer was
                # down longer than that, the rows between the watermark and
                # the current minimum are gone and no amount of retrying will
                # bring them back.
                #
                # Skipping ahead is the only option, but doing it silently
                # would leave an invisible hole in the warehouse. So the gap
                # is reported, recorded against the source, and then crossed.
                if from_lsn < min_lsn:
                    warning = (
                        f"watermark {last_lsn.hex()} predates the retained "
                        f"window (min {min_lsn.hex()}); changes in between "
                        f"were purged by the CDC cleanup job and cannot be "
                        f"recovered. Resuming from the minimum."
                    )
                    from_lsn = min_lsn

            if from_lsn > max_lsn:
                return [], None, warning

            # __$operation 3 is the before-image of an update and is dropped:
            # the consumer compares against what the warehouse already holds,
            # so the prior values carry no information it does not have.
            #
            # Ordered by (start_lsn, seqval) — commit order, then position
            # within the transaction. Applying a dimension's changes in any
            # other order can leave the older value winning.
            cur.execute(
                f"""
                SELECT TOP (%s)
                       *,
                       sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS __$commit_time
                FROM cdc.fn_cdc_get_all_changes_{capture_instance}(%s, %s, 'all')
                WHERE __$operation <> 3
                ORDER BY __$start_lsn, __$seqval
                """,
                (max_rows, from_lsn, max_lsn),
            )
            rows = cur.fetchall()

    if not rows:
        return [], None, warning

    # The window ends at the last row actually read, not at max_lsn. With TOP
    # in play they are not the same, and claiming the whole window would skip
    # everything the cap left behind.
    #
    # A transaction can span the cut. Its remainder is read next pass and
    # republished — at-least-once, which ReplacingMergeTree absorbs, whereas
    # truncating mid-transaction would drop rows outright.
    #
    # The commit time comes back with the rows, so the watermark update does
    # not have to look it up on a connection where the CDC functions are out
    # of reach.
    return rows, (rows[-1]["__$start_lsn"], rows[-1]["__$commit_time"]), warning