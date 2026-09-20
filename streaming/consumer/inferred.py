"""Inferred members: placeholder dimension rows created by the fact load.

A fact can arrive referring to a customer, product or employee the warehouse
has not seen — the dimension event is on another topic and may not have been
consumed yet. Dropping the fact would lose a real order; failing would stop
the pipeline over a routine race.

So a stub is created instead: a dimension row carrying only its alternate
key, every other attribute at its zero value. The order lands with a valid
foreign key, and the next dimension event matches the stub on alternate key
and takes the type 1 path, filling it in.

Same pattern as spark/jobs/common/inferred_member.py, and the one the
reference SSIS packages implement with their
`insert into dbo.DimCustomer(CustomerAlternateKey) values(?)` commands.

Stubs stay auditable afterwards:

    SELECT * FROM DimCustomer FINAL WHERE company_name = ''
"""
from __future__ import annotations

import datetime as dt
import logging

from .warehouse import (
    KEYS, OPEN_END_DATE, VERSIONS, ZERO_DATE32, insert_rows, stored_columns,
)

log = logging.getLogger("consumer.inferred")

# surrogate column, alternate column, per dimension
KEY_COLUMNS = {
    "DimCustomer":    ("customer_key",  "customer_alternate_key"),
    "DimProducts":    ("product_key",   "product_alternate_key"),
    "DimEmployees":   ("employee_key",  "employee_alternate_key"),
    "DimShippers":    ("shipper_key",   "shipper_alternate_key"),
    "DimSuppliers":   ("supplier_key",  "supplier_alternate_key"),
    "DimTerritories": ("territory_key", "territory_alternate_key"),
}


def _zero(ch_type: str):
    if ch_type.startswith("Nullable"):
        return None
    if ch_type.startswith(("Int", "UInt")):
        return 0
    if ch_type.startswith(("Float", "Decimal")):
        return 0
    if ch_type.startswith("Date32"):
        return ZERO_DATE32
    if ch_type.startswith("Date"):
        return dt.datetime(1970, 1, 1)
    return ""


def create(ch, table: str, alternate_value, applied_at: dt.datetime) -> int:
    """Create one stub and return its surrogate key.

    The column list is read from the table rather than hardcoded. A stub that
    silently stopped matching the schema would insert a row with a missing
    column at its default — the same shape as a real row, and never
    questioned.
    """
    surrogate_column, alternate_column = KEY_COLUMNS[table]
    columns = stored_columns(ch, table)
    key = KEYS.take(table)

    row = {}
    for name, ch_type in columns:
        if name == surrogate_column:
            row[name] = key
        elif name == alternate_column:
            row[name] = alternate_value
        elif name == "start_date":
            row[name] = applied_at
        elif name == "end_date":
            row[name] = OPEN_END_DATE
        elif name == "_version":
            row[name] = VERSIONS.take()
        else:
            row[name] = _zero(ch_type)

    insert_rows(ch, table, [row])
    log.warning("inferred member: %s %s -> key %s", table, alternate_value, key)
    return key