"""FactOrders and FactEmployeeTerritories.

FactOrders has the grain of Order Details while most of its attributes come
from Orders, so a header change rewrites every line of that order and a line
change rewrites one. The two arrive as separate events on separate topics,
which Kafka does not order relative to each other — hence RtOrderHeaders,
where a header waits for its lines and lines find their header.

The header attributes are denormalised onto every line rather than joined at
query time. That is the Kimball recommendation for header/line schemas, and
it is the right trade at warehouse scale — but it is what makes a parent-only
change expensive here, and the rewrite below is the price of it.
"""
from __future__ import annotations

import datetime as dt
import decimal
import logging

from . import inferred, lookups
from .warehouse import VERSIONS, insert_rows, stored_columns

log = logging.getLogger("consumer.facts")

ZERO = decimal.Decimal(0)

HEADER_COLUMNS = [
    "order_id", "geography_key", "customer_key", "employee_key", "shipper_key",
    "order_date_key", "required_date_key", "shipped_date_key",
    "order_date", "required_date", "shipped_date", "freight", "ship_name",
]

EMPTY_HEADER = {
    "order_id": 0, "geography_key": 0, "customer_key": 0, "employee_key": 0,
    "shipper_key": 0, "order_date_key": 0, "required_date_key": 0,
    "shipped_date_key": 0, "order_date": None, "required_date": None,
    "shipped_date": None, "freight": ZERO, "ship_name": "",
}


def _date_key(value) -> int:
    """yyyyMMdd, so a fact derives its date keys without joining DimDate.

    The one place in the model where a surrogate key is computed rather than
    assigned — an accepted exception, since a date never changes.
    """
    return int(str(value)[:10].replace("-", "")) if value else 0


def _datetime(value):
    return dt.datetime.fromisoformat(str(value).replace("Z", "")) if value else None


def _key_or_stub(ch, table: str, alternate_column: str, value, applied_at) -> int:
    if value in (None, "", 0):
        return 0
    key = lookups.surrogate_key_any(ch, table, alternate_column, value)
    return key or inferred.create(ch, table, value, applied_at)


# ---------------------------------------------------------------------------
# Orders — the header
# ---------------------------------------------------------------------------

def handle_order(ch, envelope: dict, applied_at: dt.datetime) -> tuple[str, int]:
    """Store the header, then rewrite every line of that order.

    Rewriting rather than updating: a ClickHouse mutation rewrites whole
    parts, so reinserting the lines with a higher version costs a fraction of
    the same effect and ReplacingMergeTree resolves them on
    (order_id, product_key).

    A header change that touched no line is normal, not a failure — the
    order's lines may simply not have arrived yet, and they will read this
    header when they do.
    """
    data = envelope["data"]
    order_id = int(data["OrderID"])
    deleted = envelope["operation"] == "delete"

    header = {
        "order_id": order_id,
        "geography_key": lookups.geography_key(ch, {
            "Address": data.get("ShipAddress"), "City": data.get("ShipCity"),
            "Region": data.get("ShipRegion"), "PostalCode": data.get("ShipPostalCode"),
            "Country": data.get("ShipCountry"),
        }),
        "customer_key": _key_or_stub(
            ch, "DimCustomer", "customer_alternate_key", data.get("CustomerID"), applied_at),
        "employee_key": _key_or_stub(
            ch, "DimEmployees", "employee_alternate_key", data.get("EmployeeID"), applied_at),
        "shipper_key": _key_or_stub(
            ch, "DimShippers", "shipper_alternate_key", data.get("ShipVia"), applied_at),
        "order_date_key": _date_key(data.get("OrderDate")),
        "required_date_key": _date_key(data.get("RequiredDate")),
        "shipped_date_key": _date_key(data.get("ShippedDate")),
        "order_date": _datetime(data.get("OrderDate")),
        "required_date": _datetime(data.get("RequiredDate")),
        "shipped_date": _datetime(data.get("ShippedDate")),
        "freight": decimal.Decimal(str(data.get("Freight") or 0)),
        "ship_name": str(data.get("ShipName") or ""),
        "is_deleted": 1 if deleted else 0,
        "_version": VERSIONS.take(),
        "seen_at": applied_at,
    }
    insert_rows(ch, "RtOrderHeaders", [header])

    # argMax rather than a plain read. ReplacingMergeTree deduplicates only
    # on background merge, so an unmerged part can still hold a superseded
    # copy of a line — reinserting that copy with a fresh version would
    # resurrect measures the source had already corrected. argMax over
    # _version picks the latest state of each line regardless of merges.
    #
    # Deleted lines are excluded for the same reason: a rewrite that carried
    # them along would undo the tombstone.
    lines = ch.query(
        "SELECT product_key, "
        "       argMax(unit_price, _version), "
        "       argMax(quantity, _version), "
        "       argMax(discount, _version), "
        "       argMax(is_deleted, _version) AS deleted "
        "FROM FactOrders WHERE order_id = {o:Int32} "
        "GROUP BY product_key HAVING deleted = 0",
        parameters={"o": order_id},
    ).result_rows

    if not lines:
        return ("delete" if deleted else "header"), 1

    rows = [
        _fact_row(header, product_key, unit_price, quantity, discount, deleted)
        for product_key, unit_price, quantity, discount, _ in lines
    ]
    written = insert_rows(ch, "FactOrders", rows)
    log.info("order %s header applied to %s line(s)", order_id, written)
    return ("delete" if deleted else "header"), written + 1


# ---------------------------------------------------------------------------
# Order Details — the line
# ---------------------------------------------------------------------------

def handle_order_detail(ch, envelope: dict, applied_at: dt.datetime) -> tuple[str, int]:
    """One fact row, joined to whatever header is on hand.

    A line whose header has not arrived is written with zero-value header
    attributes rather than dropped or deferred. The header event rewrites
    every line of its order when it lands, so the row corrects itself — the
    inferred-member bargain, applied to a fact instead of a dimension.
    """
    data = envelope["data"]
    order_id = int(data["OrderID"])
    deleted = envelope["operation"] == "delete"

    rows = ch.query(
        f"SELECT {', '.join(HEADER_COLUMNS)} FROM RtOrderHeaders FINAL "
        f"WHERE order_id = {{o:Int32}} AND is_deleted = 0",
        parameters={"o": order_id},
    ).result_rows

    if rows:
        header = dict(zip(HEADER_COLUMNS, rows[0]))
    else:
        log.warning("order %s: no header yet, writing line with zero values", order_id)
        header = dict(EMPTY_HEADER, order_id=order_id)

    product_key = _key_or_stub(
        ch, "DimProducts", "product_alternate_key", data.get("ProductID"), applied_at)

    insert_rows(ch, "FactOrders", [_fact_row(
        header, product_key,
        decimal.Decimal(str(data.get("UnitPrice") or 0)),
        int(data.get("Quantity") or 0),
        float(data.get("Discount") or 0),
        deleted,
    )])
    return ("delete" if deleted else "line"), 1


def _fact_row(header, product_key, unit_price, quantity, discount, deleted) -> dict:
    """A FactOrders row. line_total is MATERIALIZED and must not be supplied.

    A delete is a tombstone rather than a DELETE statement: a mutation would
    rewrite whole partitions for what an insert expresses in one row, and the
    deletion stays auditable.
    """
    return {
        "order_id": header["order_id"],
        "product_key": product_key,
        "geography_key": header["geography_key"],
        "customer_key": header["customer_key"],
        "employee_key": header["employee_key"],
        "shipper_key": header["shipper_key"],
        "order_date_key": header["order_date_key"],
        "required_date_key": header["required_date_key"],
        "shipped_date_key": header["shipped_date_key"],
        "freight": header["freight"],
        "unit_price": unit_price,
        "quantity": quantity,
        "discount": discount,
        "ship_name": header["ship_name"],
        "order_date": header["order_date"],
        "required_date": header["required_date"],
        "shipped_date": header["shipped_date"],
        "is_deleted": 1 if deleted else 0,
        "_version": VERSIONS.take(),
    }


# ---------------------------------------------------------------------------
# FactEmployeeTerritories — factless
# ---------------------------------------------------------------------------

def handle_employee_territory(ch, envelope: dict, applied_at: dt.datetime) -> tuple[str, int]:
    """A factless fact: it records that a relationship exists, with no measure.

    Both keys must resolve to real rows. Unlike an order, there is nothing to
    preserve if they do not — a bridge row pointing at two stubs asserts a
    relationship between two things the warehouse cannot name.
    """
    if envelope["operation"] == "delete":
        return "skipped", 0

    data = envelope["data"]
    employee_key = lookups.surrogate_key_any(
        ch, "DimEmployees", "employee_alternate_key", data.get("EmployeeID"))
    territory_key = lookups.surrogate_key_any(
        ch, "DimTerritories", "territory_alternate_key",
        str(data.get("TerritoryID") or "").strip())

    if not (employee_key and territory_key):
        log.warning("bridge row skipped: employee=%s territory=%s not both resolved",
                    data.get("EmployeeID"), data.get("TerritoryID"))
        return "skipped", 0

    types = dict(stored_columns(ch, "FactEmployeeTerritories"))
    row = {"employee_key": employee_key, "territory_key": territory_key}
    if "_version" in types:
        row["_version"] = VERSIONS.take()

    insert_rows(ch, "FactEmployeeTerritories", [row])
    return "new", 1