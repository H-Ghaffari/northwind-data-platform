"""Inferred members: placeholder dimension rows created by the fact load.

The dimensions reload nightly; the facts reload every half hour. In the gap
between the two, a fact can arrive referring to a customer, product or
employee the warehouse has never seen. Dropping that fact would lose a real
order. Failing the load would stop the pipeline over a routine race.

So the fact load creates a stub instead: a dimension row carrying only its
alternate key, with every other attribute left at its zero value. The order
lands with a valid foreign key, and the next dimension load fills in the
rest — the stub is matched on alternate key like any other existing row and
takes the type 1 path.

Stubs are identifiable after the fact by their empty attributes, which is
what makes them auditable:

    SELECT * FROM DimCustomer FINAL WHERE company_name = ''

This is the pattern the reference SSIS packages implement with their
`insert into dbo.DimCustomer(CustomerAlternateKey) values(?)` commands.
"""

from __future__ import annotations

from datetime import datetime

from .scd_utils import OPEN_END_DATE, next_surrogate_key
from .spark_utils import dw_client, log

# Which column carries the source key, per dimension.
BUSINESS_KEYS = {
    "DimCustomer": ("customer_key", "customer_alternate_key"),
    "DimProducts": ("product_key", "product_alternate_key"),
    "DimEmployees": ("employee_key", "employee_alternate_key"),
    "DimShippers": ("shipper_key", "shipper_alternate_key"),
}

# Dimensions with no history carry no start_date/end_date to populate.
NO_HISTORY = {"DimShippers"}


def create_inferred_members(
    table: str,
    missing_keys: list,
    run_time: datetime | None = None,
) -> dict:
    """Create a stub row for each unmatched key. Returns key → surrogate key.

    An empty input is the normal case and returns an empty mapping without
    touching the warehouse.
    """
    if not missing_keys:
        return {}

    if table not in BUSINESS_KEYS:
        raise ValueError(f"No inferred member rule defined for {table}")

    surrogate_column, alternate_column = BUSINESS_KEYS[table]
    run_time = run_time or datetime.utcnow().replace(microsecond=0)

    log.warning(
        "Creating %s inferred member(s) in %s: %s",
        len(missing_keys), table,
        ", ".join(str(k) for k in sorted(missing_keys)[:10])
        + (" ..." if len(missing_keys) > 10 else ""),
    )

    start_key = next_surrogate_key(table, surrogate_column)
    assignments = {
        key: start_key + offset for offset, key in enumerate(sorted(missing_keys))
    }

    client = dw_client()
    try:
        # Ask the target for its column list so the stub matches the table
        # exactly. Hard-coding it here would silently drift the moment a
        # column was added to the DDL.
        schema = client.query(
            f"SELECT name, type FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{table}' "
            f"AND default_kind != 'MATERIALIZED' "
            f"ORDER BY position"
        ).result_rows

        columns = [name for name, _ in schema]
        types = dict(schema)

        def stub_value(column: str, alternate_value):
            if column == surrogate_column:
                return assignments[alternate_value]
            if column == alternate_column:
                return alternate_value
            if column == "start_date":
                return run_time
            if column == "end_date":
                return OPEN_END_DATE
            if column == "_version":
                return int(run_time.timestamp() * 1000)

            column_type = types[column]
            if column_type.startswith("Nullable"):
                return None
            if any(t in column_type for t in ("Int", "Float", "Decimal")):
                return 0
            if "Date32" in column_type:
                return datetime(1900, 1, 1).date()
            if "Date" in column_type:
                return datetime(1970, 1, 1)
            return ""

        rows = [
            tuple(stub_value(c, key) for c in columns)
            for key in sorted(missing_keys)
        ]

        client.insert(table, rows, column_names=columns)
        log.info("Inserted %s inferred member(s) into %s", len(rows), table)
    finally:
        client.close()

    return assignments


def count_inferred_members(table: str) -> int:
    """How many stub rows a dimension currently holds.

    Stubs are recognised by an empty first descriptive attribute. A count
    that keeps growing means the dimension load is not catching up with the
    fact load, which is worth knowing before someone notices blank labels
    on a dashboard.
    """
    marker = {
        "DimCustomer": "company_name",
        "DimProducts": "product_name",
        "DimEmployees": "full_name",
        "DimShippers": "company_name",
    }.get(table)

    if marker is None:
        return 0

    client = dw_client()
    try:
        result = client.query(
            f"SELECT count() FROM {table} FINAL WHERE {marker} = ''"
        )
        return int(result.result_rows[0][0]) if result.result_rows else 0
    finally:
        client.close()
