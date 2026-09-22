"""Declarative registry of the CDC sources.

What each source means — which warehouse table it feeds, what identifies a
row in it — is stated once, here. The producer reads it to build a message
key; the consumer will read the same entries to decide what to apply where.

Routing and the delete policy deliberately live in ETL_Settings.Stream_State
instead, because those are operational switches: pausing a source or
repointing a topic should not need a code change. What belongs in code is
what cannot change without the code changing too.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    capture_instance: str
    source_table: str
    target_table: str

    # Columns that identify a row in the source system — the alternate key
    # in warehouse terms. Used as the Kafka message key, so every change to
    # one entity lands in one partition and is applied in commit order.
    business_key: tuple[str, ...]


SOURCES: dict[str, Source] = {
    s.capture_instance: s
    for s in (
        # --- fact sources -------------------------------------------------
        Source("dbo_Orders", "Orders", "FactOrders", ("OrderID",)),
        Source("dbo_OrderDetails", "Order Details", "FactOrders",
               ("OrderID", "ProductID")),
        Source("dbo_EmployeeTerritories", "EmployeeTerritories",
               "FactEmployeeTerritories", ("EmployeeID", "TerritoryID")),

        # --- dimension sources --------------------------------------------
        Source("dbo_Customers", "Customers", "DimCustomer", ("CustomerID",)),
        Source("dbo_Products", "Products", "DimProducts", ("ProductID",)),
        Source("dbo_Employees", "Employees", "DimEmployees", ("EmployeeID",)),
        Source("dbo_Suppliers", "Suppliers", "DimSuppliers", ("SupplierID",)),
        Source("dbo_Shippers", "Shippers", "DimShippers", ("ShipperID",)),
        Source("dbo_Territories", "Territories", "DimTerritories",
               ("TerritoryID",)),

        # --- reference sources --------------------------------------------
        # Not dimensions. The batch path folds these into DimProducts and
        # DimTerritories with a join in staging; the streaming path has no
        # staging, so the consumer keeps them as lookups instead.
        Source("dbo_Categories", "Categories", "RefCategories", ("CategoryID",)),
        Source("dbo_Region", "Region", "RefRegion", ("RegionID",)),
    )
}


def build_key(capture_instance: str, row: dict) -> str:
    """Message key for a change row: its business key, joined.

    Returns a string because Kafka partitions on the raw key bytes, and a
    stable text form keeps the same entity on the same partition across
    restarts and across producer versions.
    """
    source = SOURCES[capture_instance]
    return "|".join(str(row.get(col, "")) for col in source.business_key)