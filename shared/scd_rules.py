"""Warehouse dimension contract: column mapping and SCD type per column.

This is the single statement of how a source row becomes a dimension row.
The streaming consumer reads it to apply a change it has no staging layer to
prepare; the batch path's staging projections encode the same assignments,
and this module is where they are stated rather than restated.

Why it exists: promoting a column from type 1 to type 2 is a one-line change
here. Held in two places it is a two-line change, and the run where somebody
makes only one of them produces two warehouses that disagree — without
failing, because both still return a number.

SCD types, as the reference SSIS packages define them:

    0   fixed       set once at insert, never revisited
    1   changing    overwrite in place; the previous value was wrong
    2   historical  close the current row and open a new one; the previous
                    value was right, and stopped being right

The split is not arbitrary. A misspelled contact name is a correction and
overwrites. A territory moving to another region is an event, and the old
row has to survive so facts loaded against it still add up.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Literal

ColumnKind = Literal["direct", "lookup", "derived", "computed"]


@dataclass(frozen=True)
class Column:
    """One warehouse column and where its value comes from.

    source is the SQL Server column name, target the ClickHouse one. For
    lookup and derived columns, source names the input to the resolution
    rather than a value that can be copied.
    """
    source: str
    target: str
    scd: Literal[0, 1, 2]
    kind: ColumnKind = "direct"

    # for kind="derived": which reference table resolves it
    reference: str | None = None

    # for kind="computed": a short name the consumer dispatches on. The
    # expression itself lives in code, not here — this states that the column
    # is computed and from which inputs, so a reader of the contract can see
    # that no source column carries it.
    formula: str | None = None


def direct(source: str, target: str, scd: Literal[0, 1, 2]) -> Column:
    return Column(source, target, scd, "direct")


def lookup(source: str, target: str, scd: Literal[0, 1, 2]) -> Column:
    return Column(source, target, scd, "lookup")


def derived(source: str, target: str, scd: Literal[0, 1, 2], reference: str) -> Column:
    return Column(source, target, scd, "derived", reference)

def computed(source: str, target: str, scd: Literal[0, 1, 2], formula: str) -> Column:
    """A column the warehouse calculates from other columns of the same row.

    source lists the inputs, pipe-separated, so the contract shows what the
    value depends on even though nothing copies it directly.
    """
    return Column(source, target, scd, "computed", formula=formula)


@dataclass(frozen=True)
class Dimension:
    table: str
    surrogate_key: str
    alternate_key: str          # warehouse column holding the business key
    source_key: str             # SQL Server column it comes from
    columns: tuple[Column, ...]

    start_date: str = "start_date"
    end_date: str = "end_date"
    version_column: str = "_version"
    deleted_column: str = "is_deleted"

    @property
    def has_history(self) -> bool:
        # Derived rather than declared, so the two can never contradict.
        return any(c.scd == 2 for c in self.columns)

    def by_type(self, scd: int) -> tuple[Column, ...]:
        return tuple(c for c in self.columns if c.scd == scd)

    @property
    def target_columns(self) -> tuple[str, ...]:
        return tuple(c.target for c in self.columns)


# ---------------------------------------------------------------------------
# The dimensions.
#
# Type assignments come from the reference SSIS packages — specifically from
# which columns appear in each package's two UPDATE statements. They are not
# re-derived here; this is a transcription, and the packages remain the
# authority.
# ---------------------------------------------------------------------------

DIM_CUSTOMER = Dimension(
    table="DimCustomer",
    surrogate_key="customer_key",
    alternate_key="customer_alternate_key",
    source_key="CustomerID",
    columns=(
        direct("CompanyName",  "company_name",  1),
        direct("ContactTitle", "contact_title", 1),
        direct("Phone",        "phone",         1),
        direct("Fax",          "fax",           1),

        # A different person now holds the account. Orders placed under the
        # previous contact stay attributed to them.
        direct("ContactName",  "contact_name",  2),

        # Resolved against DimGeography on the full address tuple, street
        # included — the five-column match the batch path documents as a
        # deliberate departure from the reference.
        lookup("Address|City|Region|PostalCode|Country", "geography_key", 2),
    ),
)

DIM_PRODUCTS = Dimension(
    table="DimProducts",
    surrogate_key="product_key",
    alternate_key="product_alternate_key",
    source_key="ProductID",
    columns=(
        direct("ProductName",     "product_name",      1),
        direct("QuantityPerUnit", "quantity_per_unit", 1),
        direct("UnitsInStock",    "units_in_stock",    1),
        direct("UnitsOnOrder",    "units_on_order",    1),
        direct("ReorderLevel",    "reorder_level",     1),

        # Price is why this dimension has history at all: an order from last
        # year was priced at last year's figure, and overwriting would
        # rewrite revenue that has already been reported.
        direct("UnitPrice",       "unit_price",        2),
        direct("Discontinued",    "discontinued",      2),

        # Categories is a snowflake branch the warehouse flattens. The batch
        # path resolves it with a join in staging; the consumer resolves it
        # against RefCategories, which exists for exactly this.
        derived("CategoryID",     "category_name",     2, reference="RefCategories"),
        lookup("SupplierID",      "supplier_key",      2),
    ),
)

DIM_EMPLOYEES = Dimension(
    table="DimEmployees",
    surrogate_key="employee_key",
    alternate_key="employee_alternate_key",
    source_key="EmployeeID",
    columns=(
        direct("FirstName",       "first_name",        1),
        direct("LastName",        "last_name",         1),
        direct("BirthDate",       "birth_date",        1),
        direct("HireDate",        "hire_date",         1),
        direct("HomePhone",       "home_phone",        1),
        direct("Extension",       "extension",         1),
        direct("TitleOfCourtesy", "title_of_courtesy", 1),
        direct("PhotoPath",       "photo_path",        1),

        # Concatenation, stored rather than computed at query time: every
        # dashboard that labels an employee wants it, and a stored column
        # keeps that spelling identical across all of them.
        computed("FirstName|LastName", "full_name", 1, formula="full_name"),

        direct("Title",           "title",             2),
        direct("Notes",           "notes",             2),
        lookup("Address|City|Region|PostalCode|Country", "geography_key", 2),

        # The manager's natural key, kept beside the surrogate key below.
        # parent_employee_key needs a lookup that can fail while the
        # manager's own row is still being written; this one never can, so
        # the relationship survives even when the resolution does not.
        direct("ReportsTo",       "reports_to",        2),

        # Self-referencing. ReportsTo is a source id; parent_employee_key is
        # a surrogate key that may not exist yet when this row is written, so
        # it is resolved in a second pass — the same two-pass shape the batch
        # load already uses.
        lookup("ReportsTo",       "parent_employee_key", 2),
    ),
)

DIM_SUPPLIERS = Dimension(
    table="DimSuppliers",
    surrogate_key="supplier_key",
    alternate_key="supplier_alternate_key",
    source_key="SupplierID",
    columns=(
        direct("CompanyName",  "company_name",  1),
        direct("Phone",        "phone",         1),
        direct("Fax",          "fax",           1),
        direct("HomePage",     "home_page",     1),

        direct("ContactName",  "contact_name",  2),
        direct("ContactTitle", "contact_title", 2),
        lookup("Address|City|Region|PostalCode|Country", "geography_key", 2),
    ),
)

DIM_TERRITORIES = Dimension(
    table="DimTerritories",
    surrogate_key="territory_key",
    alternate_key="territory_alternate_key",
    source_key="TerritoryID",
    columns=(
        direct("TerritoryDescription", "territory_description", 1),

        # A territory moving to another region is a real event, not a typo.
        derived("RegionID", "region_description", 2, reference="RefRegion"),
    ),
)

DIM_SHIPPERS = Dimension(
    table="DimShippers",
    surrogate_key="shipper_key",
    alternate_key="shipper_alternate_key",
    source_key="ShipperID",
    columns=(
        direct("CompanyName", "company_name", 1),
        direct("Phone",       "phone",        1),
    ),
)

DIMENSIONS: dict[str, Dimension] = {
    d.table: d
    for d in (DIM_CUSTOMER, DIM_PRODUCTS, DIM_EMPLOYEES,
              DIM_SUPPLIERS, DIM_TERRITORIES, DIM_SHIPPERS)
}


# ---------------------------------------------------------------------------
# Validation
#
# The mapping asserts things about a schema this module cannot see. Checked
# at consumer startup rather than trusted: a renamed column would otherwise
# surface as a dimension that quietly stops updating one field, which is the
# kind of fault nobody finds for weeks.
# ---------------------------------------------------------------------------

# Columns the warehouse keeps for its own bookkeeping. No source column maps
# to them, and their absence from the rules is correct rather than a gap.
BOOKKEEPING = {
    "_version", "_updated_at", "is_deleted", "is_inferred",
    "start_date", "end_date", "inserted_at", "updated_at",
    # ALIAS columns: computed by ClickHouse at read time, and not writable.
    # A rule mapping to one would produce an INSERT that fails.
    "age",
}


def validate(client, database: str) -> list[str]:
    """Return one message per mismatch. An empty list means the rules hold."""
    problems: list[str] = []

    rows = client.query(
        "SELECT table, name FROM system.columns WHERE database = {db:String}",
        parameters={"db": database},
    ).result_rows

    actual: dict[str, set[str]] = {}
    for table, column in rows:
        actual.setdefault(table, set()).add(column)

    for dim in DIMENSIONS.values():
        if dim.table not in actual:
            problems.append(f"{dim.table}: table does not exist in {database}")
            continue

        present = actual[dim.table]

        required = [dim.surrogate_key, dim.alternate_key, *dim.target_columns]
        if dim.has_history:
            required += [dim.start_date, dim.end_date]

        for column in required:
            if column not in present:
                problems.append(f"{dim.table}.{column}: mapped, but not in {database}")

        # The more dangerous direction: the consumer would never write this
        # column, and the field would sit at its zero value looking like
        # missing source data for as long as nobody queried it.
        unmapped = present - set(dim.target_columns) - BOOKKEEPING - {
            dim.surrogate_key, dim.alternate_key,
        }
        for column in sorted(unmapped):
            problems.append(f"{dim.table}.{column}: in {database}, but no rule maps to it")

    return problems


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Validate the SCD contract.")
    parser.add_argument("--host", default="northwind_dw")
    parser.add_argument("--port", type=int, default=8123)
    parser.add_argument("--user", default="dw_admin")
    parser.add_argument("--password", default="dw123456")
    parser.add_argument("--database", default="NorthwindRT")
    args = parser.parse_args()

    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=args.host, port=args.port,
        username=args.user, password=args.password,
    )

    problems = validate(client, args.database)

    if not problems:
        total = sum(len(d.columns) for d in DIMENSIONS.values())
        print(f"contract holds: {len(DIMENSIONS)} dimensions, {total} mapped columns")
        for dim in DIMENSIONS.values():
            print(f"  {dim.table:<16} type1={len(dim.by_type(1)):<3} "
                  f"type2={len(dim.by_type(2)):<3} "
                  f"history={'yes' if dim.has_history else 'no'}")
        return 0

    print(f"{len(problems)} problems:", file=sys.stderr)
    for problem in problems:
        print(f"  {problem}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(_cli())