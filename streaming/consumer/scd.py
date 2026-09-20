"""The SCD decision, one row at a time.

Same rules as spark/jobs/common/scd_utils.py, applied to a single event
instead of a DataFrame. The column classification comes from
shared/scd_rules.py, so neither path can drift from the other on which
column carries history.
"""
from __future__ import annotations

import datetime as dt

from scd_rules import Dimension

from . import lookups
from .warehouse import (
    OPEN_END_DATE, VERSIONS, KEYS, coerce, current_row, stored_columns,
)


def build_row(ch, dim: Dimension, data: dict) -> dict:
    """Turn the source columns of an event into warehouse column values.

    This is staging's projection, per event: read the source column, resolve
    it if it needs resolving, coerce it to the stored type.
    """
    types = dict(stored_columns(ch, dim.table))
    row: dict = {}

    for column in dim.columns:
        if column.kind == "direct":
            value = data.get(column.source)

        elif column.kind == "computed":
            if column.formula == "full_name":
                first = (data.get("FirstName") or "").strip()
                last = (data.get("LastName") or "").strip()
                value = f"{first} {last}".strip()
            else:
                raise ValueError(f"no implementation for formula {column.formula}")

        elif column.kind == "derived":
            value = lookups.reference_value(
                ch, column.reference,
                *{
                    "RefCategories": ("category_id", "category_name"),
                    "RefRegion": ("region_id", "region_description"),
                }[column.reference],
                data.get(column.source),
            )

        elif column.kind == "lookup":
            if column.target == "geography_key":
                value = lookups.geography_key(ch, data)
            elif column.target == "supplier_key":
                value = lookups.surrogate_key(
                    ch, "DimSuppliers", "supplier_alternate_key",
                    data.get("SupplierID"))
            elif column.target == "parent_employee_key":
                value = lookups.surrogate_key(
                    ch, "DimEmployees", "employee_alternate_key",
                    data.get("ReportsTo"))
            else:
                raise ValueError(f"no lookup defined for {column.target}")
        else:
            raise ValueError(f"unknown column kind {column.kind}")

        row[column.target] = coerce(value, types[column.target])

    row[dim.alternate_key] = coerce(data.get(dim.source_key), types[dim.alternate_key])
    return row


def classify(dim: Dimension, incoming: dict, current: dict | None) -> str:
    """new | type2 | type1 | unchanged.

    Type 2 is checked first and wins outright. A row whose historical
    attributes changed gets a new version, and that version carries the
    current type 1 values anyway — treating it as both would write it twice.

    Comparison is by value rather than by hash, as in the batch engine:
    Northwind's dimensions are small enough that the extra columns cost
    nothing, and a value comparison can say which attribute changed.
    """
    if current is None:
        return "new"

    def differs(columns) -> bool:
        # Python's == already treats None == None as equal, which is what
        # eqNullSafe does on the batch side.
        return any(incoming[c.target] != current.get(c.target) for c in columns)

    if differs(dim.by_type(2)):
        return "type2"
    if differs(dim.by_type(1)):
        return "type1"
    return "unchanged"


def rows_to_write(
    dim: Dimension, outcome: str, incoming: dict, current: dict | None,
    applied_at: dt.datetime, stored: set[str], extra: dict | None = None,
) -> list[dict]:
    """The inserts one outcome produces. Every outcome is an insert.

    ClickHouse makes UPDATE expensive — a mutation rewrites whole parts — so
    a type 1 change is an insert over the same key with a higher version and
    a type 2 change is two inserts. ReplacingMergeTree resolves both during
    its background merge.

    `stored` is the table's actual writable columns. Bookkeeping columns are
    added only when the table has them: a dimension with no type 2 attribute
    has no validity dates, and none of the dimensions carry the is_deleted
    tombstone that FactOrders does — the brief does not propagate dimension
    deletes, so there is nothing for one to record.
    """
    extra = extra or {}
    has_history = dim.start_date in stored and dim.end_date in stored

    def finish(row: dict, key: int, start, end) -> dict:
        row = {k: v for k, v in row.items() if k in stored}
        row[dim.surrogate_key] = key
        if has_history:
            row[dim.start_date] = start
            row[dim.end_date] = end
        if dim.version_column in stored:
            row[dim.version_column] = VERSIONS.take()
        if dim.deleted_column in stored:
            row.setdefault(dim.deleted_column, 0)
        row.update({k: v for k, v in extra.items() if k in stored})
        return row

    if outcome == "new":
        return [finish(incoming, KEYS.take(dim.table), applied_at, OPEN_END_DATE)]

    if outcome == "type1":
        # Same key and the original start_date, so the merge replaces the
        # stored copy in place rather than adding a version beside it.
        return [finish(
            incoming, current[dim.surrogate_key],
            current[dim.start_date] if has_history else None, OPEN_END_DATE,
        )]

    if outcome == "type2":
        closed = finish(
            {c.target: current[c.target] for c in dim.columns}
            | {dim.alternate_key: current[dim.alternate_key]},
            current[dim.surrogate_key],
            current[dim.start_date] if has_history else None, applied_at,
        )
        opened = finish(incoming, KEYS.take(dim.table), applied_at, OPEN_END_DATE)
        return [closed, opened]

    return []