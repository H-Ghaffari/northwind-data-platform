"""Foreign key resolution — what staging did with a join.

Every lookup here caches, because a consumer resolving one row at a time
would otherwise issue the same query thousands of times. The caches are
invalidated by the events that change them: a renamed category arrives on
its own topic, and the handler drops the entry before the next product is
resolved.
"""
from __future__ import annotations

from .warehouse import current_row

# address tuple -> geography_key
_geography: dict[tuple, int] = {}
# (table, alternate key) -> surrogate key
_surrogate: dict[tuple[str, str], int] = {}
# reference table -> {id: name}
_reference: dict[str, dict[int, str]] = {}


def clear_reference(table: str) -> None:
    _reference.pop(table, None)


def clear_surrogate(table: str, key) -> None:
    _surrogate.pop((table, str(key)), None)


def _norm(value) -> str:
    """Empty string for a missing address part.

    DimGeography stores '' where the source held NULL, because the batch
    path normalises at the staging boundary. The join is an equality, and
    NULL = NULL is not true in SQL, so a customer whose region is unset
    would match nothing and lose its geography key. Same rule, applied at
    the only boundary this path has.
    """
    return "" if value is None else str(value).strip()


def geography_key(ch, data: dict) -> int:
    """Resolve the address tuple against DimGeography.

    Five columns including the street, not the reference implementation's
    four. A four-column match returns any of several rows for the same city
    and postcode, and SSIS silently takes the first.

    An address that is not there gets key 0 rather than dropping the row:
    losing a customer because its address is missing from a lookup table
    would be far worse than an unresolved key, and key 0 is visible.
    """
    tuple_ = (
        _norm(data.get("Country")), _norm(data.get("Region")),
        _norm(data.get("City")), _norm(data.get("PostalCode")),
        _norm(data.get("Address")),
    )
    if tuple_ in _geography:
        return _geography[tuple_]

    rows = ch.query(
        "SELECT geography_key FROM DimGeography FINAL "
        "WHERE country = {c:String} AND region = {r:String} AND city = {t:String} "
        "AND postal_code = {p:String} AND address = {a:String}",
        parameters=dict(zip(("c", "r", "t", "p", "a"), tuple_)),
    ).result_rows

    key = int(rows[0][0]) if rows else 0
    _geography[tuple_] = key
    return key


def surrogate_key(ch, table: str, alternate_key: str, value) -> int:
    """The surrogate key of the currently-open row of another dimension.

    Returns 0 when there is no match. For supplier_key that means a product
    arrived before its supplier; for parent_employee_key it means the
    manager's own row is not written yet. Both are resolved by a later pass
    rather than treated as errors — the same two-pass shape the batch load
    uses for the employee hierarchy.
    """
    if value in (None, "", 0):
        return 0

    cache_key = (table, str(value))
    if cache_key in _surrogate:
        return _surrogate[cache_key]

    key_column = {
        "DimSuppliers": "supplier_key",
        "DimEmployees": "employee_key",
    }[table]

    row = current_row(ch, table, alternate_key, value, [key_column])
    key = int(row[key_column]) if row else 0

    # A miss is not cached. The row it was looking for is usually moments
    # away, and a cached zero would outlive the gap it describes.
    if key:
        _surrogate[cache_key] = key
    return key


def reference_value(ch, table: str, id_column: str, name_column: str, value) -> str:
    """Resolve a snowflake branch the warehouse flattens.

    RefCategories and RefRegion hold what staging resolved with a join. The
    whole table is loaded on first use: both are single digits of rows, and
    one query beats one per product.
    """
    if table not in _reference:
        rows = ch.query(
            f"SELECT {id_column}, {name_column} FROM {table} FINAL"
        ).result_rows
        _reference[table] = {int(i): str(n) for i, n in rows}

    if value in (None, ""):
        return ""
    return _reference[table].get(int(value), "")


def rows_referencing(ch, table: str, column: str, value: str) -> list[dict]:
    """Open dimension rows holding a reference value that has just changed.

    A renamed category invalidates every product carrying the old name. The
    batch path never faces this — it rebuilds the join every night — so this
    is the one place the streaming path does work the batch path does not.
    """
    columns = [
        name for (name,) in ch.query(
            "SELECT name FROM system.columns "
            "WHERE database = currentDatabase() AND table = {t:String} "
            "AND default_kind NOT IN ('ALIAS', 'MATERIALIZED') ORDER BY position",
            parameters={"t": table},
        ).result_rows
    ]
    rows = ch.query(
        f"SELECT {', '.join(columns)} FROM {table} FINAL "
        f"WHERE {column} = {{v:String}} "
        f"AND end_date = toDateTime('2106-01-01 00:00:00')",
        parameters={"v": value},
    ).result_rows
    return [dict(zip(columns, r)) for r in rows]

def surrogate_key_any(ch, table: str, alternate_column: str, value) -> int:
    """The surrogate key of the open row of any dimension, or 0.

    Wider than surrogate_key() above, which knows only the two dimensions the
    dimension loader looks up. The fact load can reach any of them, and the
    key column name follows from the table rather than needing a case.
    """
    if value in (None, "", 0):
        return 0

    cache_key = (table, str(value))
    if cache_key in _surrogate:
        return _surrogate[cache_key]

    from .inferred import KEY_COLUMNS
    key_column = KEY_COLUMNS[table][0]

    # A dimension with no type 2 attribute has no validity dates and exactly
    # one row per key, so there is nothing to filter on.
    has_history = ch.query(
        "SELECT count() FROM system.columns "
        "WHERE database = currentDatabase() AND table = {t:String} "
        "AND name = 'end_date'",
        parameters={"t": table},
    ).result_rows[0][0]

    predicate = " AND end_date = toDateTime('2106-01-01 00:00:00')" if has_history else ""

    rows = ch.query(
        f"SELECT {key_column} FROM {table} FINAL "
        f"WHERE {alternate_column} = {{k:String}}{predicate}",
        parameters={"k": str(value)},
    ).result_rows

    key = int(rows[0][0]) if rows else 0

    # A miss is not cached. The row it was looking for is usually moments
    # away, and a cached zero would outlive the gap it describes.
    if key:
        _surrogate[cache_key] = key
    return key