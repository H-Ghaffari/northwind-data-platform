"""ClickHouse access for the consumer: types, reads, writes, key allocation."""
from __future__ import annotations

import datetime as dt
import decimal
import os
import threading
from functools import lru_cache

import clickhouse_connect

from ..common.config import _env

OPEN_END_DATE = dt.datetime(2106, 1, 1, 0, 0, 0)
ZERO_DATE32 = dt.date(1900, 1, 1)


def client():
    return clickhouse_connect.get_client(
        host=os.environ.get("DW_HOST", "northwind_dw"),
        port=int(os.environ.get("DW_HTTP_PORT", "8123")),
        username=os.environ.get("DW_USER", "dw_admin"),
        password=_env("DW_PASSWORD"),
        database=os.environ.get("DW_RT_DB", "NorthwindRT"),
    )


# ---------------------------------------------------------------------------
# Column types
# ---------------------------------------------------------------------------

@lru_cache(maxsize=64)
def stored_columns(ch, table: str) -> tuple[tuple[str, str], ...]:
    """Name and type of every writable column, in table order.

    ALIAS is evaluated at read time and MATERIALIZED at write time; neither
    can appear in an INSERT column list. Asking the table rather than
    hardcoding the list means a column added to the DDL is picked up without
    a code change — and one removed fails loudly instead of silently.
    """
    rows = ch.query(
        "SELECT name, type FROM system.columns "
        "WHERE database = currentDatabase() AND table = {t:String} "
        "AND default_kind NOT IN ('ALIAS', 'MATERIALIZED') "
        "ORDER BY position",
        parameters={"t": table},
    ).result_rows
    return tuple((name, type_) for name, type_ in rows)


def coerce(value, ch_type: str):
    """Convert a JSON-decoded value to what the column actually stores.

    This is the whole reason a dimension does not rewrite itself on every
    event. JSON has no decimal and no date: a price arrives as "18.0000" and
    a hire date as "1992-05-01". Compared raw against Decimal('18.0000') and
    date(1992, 5, 1) they differ, every event looks like a change, and the
    dimension grows a version per message while looking like it is working.
    """
    nullable = ch_type.startswith("Nullable")
    inner = ch_type[9:-1] if nullable else ch_type

    if value is None or value == "":
        if nullable:
            return None
        if inner.startswith(("Int", "UInt")):
            return 0
        if inner.startswith(("Float", "Decimal")):
            return decimal.Decimal(0) if inner.startswith("Decimal") else 0.0
        if inner.startswith("Date32"):
            return ZERO_DATE32
        if inner.startswith("Date"):
            return dt.datetime(1970, 1, 1)
        return ""

    if inner.startswith(("Int", "UInt")):
        # SQL Server bit arrives as True/False; int() handles both.
        return int(value)
    if inner.startswith("Decimal"):
        return decimal.Decimal(str(value))
    if inner.startswith("Float"):
        return float(value)
    if inner.startswith("Date32"):
        return dt.date.fromisoformat(str(value)[:10])
    if inner.startswith("DateTime"):
        return dt.datetime.fromisoformat(str(value).replace("Z", ""))
    if inner.startswith("Date"):
        return dt.date.fromisoformat(str(value)[:10])
    return str(value)


# ---------------------------------------------------------------------------
# Surrogate keys and versions
# ---------------------------------------------------------------------------

class KeyAllocator:
    """Hands out surrogate keys, continuing from what the warehouse holds.

    The maximum is read once at startup and incremented in memory, rather
    than queried per event: a round trip per key would dominate the latency
    this path exists to minimise.

    Correct only while one consumer runs. Two would hand out the same key and
    one dimension row would overwrite another. Northwind's change rate makes
    a second instance pointless, and the alternative — a keeper-backed
    sequence, or letting ClickHouse generate keys — is a larger design than
    the workload justifies. Stated here because a future reader should find
    the limit written down rather than discover it.
    """

    def __init__(self):
        self._next: dict[str, int] = {}
        self._lock = threading.Lock()

    def prime(self, ch, table: str, key_column: str) -> int:
        # max() over every row, not the open ones: a closed version still
        # owns its key and reusing it would corrupt the history facts point at.
        result = ch.query(f"SELECT max({key_column}) FROM {table}").result_rows
        current = int(result[0][0] or 0) if result else 0
        self._next[table] = current + 1
        return current

    def take(self, table: str) -> int:
        with self._lock:
            key = self._next[table]
            self._next[table] = key + 1
            return key


class VersionAllocator:
    """Strictly increasing _version values on the same scale as the DDL default.

    The batch path omits _version and lets ClickHouse default it to
    toUnixTimestamp64Milli(now64()). At one load per night that can never
    collide. A consumer applying two changes to one entity inside the same
    millisecond can, and a tie leaves ReplacingMergeTree free to keep either
    row — sometimes the older one, which is a corrupted dimension that no
    error reports.
    """

    def __init__(self):
        self._last = 0
        self._lock = threading.Lock()

    def take(self) -> int:
        with self._lock:
            now = int(dt.datetime.now().timestamp() * 1000)
            self._last = max(now, self._last + 1)
            return self._last


KEYS = KeyAllocator()
VERSIONS = VersionAllocator()


# ---------------------------------------------------------------------------
# Reads and writes
# ---------------------------------------------------------------------------

def current_row(ch, table: str, alternate_key: str, value, columns: list[str]) -> dict | None:
    """The row for one business key — the open one, where openness exists.

    A dimension with no type 2 column carries no start_date or end_date, so
    the predicate is added only when the column list says there is one. The
    batch loader makes the same test for the same reason; a dimension
    without history has exactly one row per key and nothing to filter.

    FINAL for the reason the batch loader uses it: a change applied moments
    ago may not have merged, and without FINAL the same key can appear twice
    with the superseded copy winning the comparison.
    """
    predicate = ""
    if "end_date" in columns:
        predicate = " AND end_date = toDateTime('2106-01-01 00:00:00')"

    rows = ch.query(
        f"SELECT {', '.join(columns)} FROM {table} FINAL "
        f"WHERE {alternate_key} = {{k:String}}{predicate}",
        parameters={"k": str(value)},
    ).result_rows
    return dict(zip(columns, rows[0])) if rows else None


def insert_rows(ch, table: str, rows: list[dict]) -> int:
    """Insert, naming columns so a schema change cannot silently misalign."""
    if not rows:
        return 0
    columns = list(rows[0])
    ch.insert(table, [[r[c] for c in columns] for r in rows], column_names=columns)
    return len(rows)