"""OP → Staging: full reload of every dimension source.

One function per staging table. Each follows the same shape: query the
source, empty the target, write. The queries are ports of the SSIS data
flows in the reference project, so the joins and the columns dropped along
the way match what the warehouse was designed against.

Full reload rather than incremental is deliberate and matches the brief:
the dimension sources are small — 91 customers, 77 products, 9 employees —
and reloading them wholesale is both simpler and impossible to get subtly
out of step with the source.

Each function can be called on its own, which is what lets the Airflow DAG
map them to independent tasks that fail and retry separately.
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

from common.spark_utils import (
    log,
    read_from_op,
    spark_session,
    truncate_staging_table,
    write_to_staging,
)


# ---------------------------------------------------------------------------
# Geography
# ---------------------------------------------------------------------------

def load_geography(spark: SparkSession) -> int:
    """Every distinct address across the source system.

    Four tables carry addresses and none of them owns the concept, so the
    dimension is their union. UNION rather than UNION ALL: the same city
    appears under many customers and only distinct locations are wanted.
    """
    query = """
        SELECT Country, Region, City, PostalCode, Address FROM Customers
        UNION
        SELECT Country, Region, City, PostalCode, Address FROM Employees
        UNION
        SELECT Country, Region, City, PostalCode, Address FROM Suppliers
        UNION
        SELECT ShipCountry, ShipRegion, ShipCity, ShipPostalCode, ShipAddress
        FROM Orders
    """
    df = read_from_op(spark, query).toDF(
        "country", "region", "city", "postal_code", "address"
    )
    truncate_staging_table("staging_geography")
    return write_to_staging(df, "staging_geography")


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

def load_products(spark: SparkSession) -> int:
    """Products joined to Categories.

    CategoryID is dropped and CategoryName carried in its place; Description
    and Picture are left behind. This is the flattening that turns a
    snowflake into a star — DimProducts has no category dimension beside it.

    LEFT JOIN, not INNER: a product with a missing category should still
    reach the warehouse rather than vanish from every report.
    """
    query = """
        SELECT
            p.ProductID,
            p.ProductName,
            p.SupplierID,
            c.CategoryName,
            p.QuantityPerUnit,
            p.UnitPrice,
            p.UnitsInStock,
            p.UnitsOnOrder,
            p.ReorderLevel,
            p.Discontinued
        FROM Products AS p
        LEFT JOIN Categories AS c ON c.CategoryID = p.CategoryID
    """
    df = read_from_op(spark, query).toDF(
        "product_id", "product_name", "supplier_id", "category_name",
        "quantity_per_unit", "unit_price", "units_in_stock",
        "units_on_order", "reorder_level", "discontinued",
    )
    truncate_staging_table("staging_products")
    return write_to_staging(df, "staging_products")


# ---------------------------------------------------------------------------
# Suppliers
# ---------------------------------------------------------------------------

def load_suppliers(spark: SparkSession) -> int:
    """Suppliers, carried across unchanged.

    The address columns travel with the row even though geography is its own
    dimension: the Staging → DW step needs them to resolve the geography_key.
    """
    query = """
        SELECT
            SupplierID, CompanyName, ContactName, ContactTitle,
            Address, City, Region, PostalCode, Country,
            Phone, Fax, CAST(HomePage AS NVARCHAR(MAX)) AS HomePage
        FROM Suppliers
    """
    df = read_from_op(spark, query).toDF(
        "supplier_id", "company_name", "contact_name", "contact_title",
        "address", "city", "region", "postal_code", "country",
        "phone", "fax", "home_page",
    )
    truncate_staging_table("staging_suppliers")
    return write_to_staging(df, "staging_suppliers")


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

def load_customers(spark: SparkSession) -> int:
    """Customers, carried across unchanged."""
    query = """
        SELECT
            CustomerID, CompanyName, ContactName, ContactTitle,
            Address, City, Region, PostalCode, Country, Phone, Fax
        FROM Customers
    """
    df = read_from_op(spark, query).toDF(
        "customer_id", "company_name", "contact_name", "contact_title",
        "address", "city", "region", "postal_code", "country", "phone", "fax",
    )
    truncate_staging_table("staging_customer")
    return write_to_staging(df, "staging_customer")


# ---------------------------------------------------------------------------
# Employees
# ---------------------------------------------------------------------------

def load_employees(spark: SparkSession) -> int:
    """Employees, with two derived columns.

    full_name and age do not exist in the source. They are computed here, in
    the integration layer, so every downstream consumer sees one definition
    instead of inventing its own.

    age is derived from birth_date at load time, which means it is only
    correct as of the last run. That is the accepted trade-off for a
    dimension attribute; a report needing exact current age should compute
    it from birth_date directly.

    Notes and Photo are read but not derived from — Photo is carried so the
    Data Lake step can key on employee_id later.
    """
    query = """
        SELECT
            EmployeeID,
            LastName,
            FirstName,
            FirstName + ' ' + LastName            AS FullName,
            Title,
            TitleOfCourtesy,
            BirthDate,
            DATEDIFF(YEAR, BirthDate, GETDATE())  AS Age,
            HireDate,
            Address, City, Region, PostalCode, Country,
            HomePhone,
            Extension,
            CAST(Notes AS NVARCHAR(MAX))          AS Notes,
            ReportsTo,
            PhotoPath
        FROM Employees
    """
    df = read_from_op(spark, query).toDF(
        "employee_id", "last_name", "first_name", "full_name",
        "title", "title_of_courtesy", "birth_date", "age", "hire_date",
        "address", "city", "region", "postal_code", "country",
        "home_phone", "extension", "notes", "reports_to", "photo_path",
    )
    truncate_staging_table("staging_employees")
    return write_to_staging(df, "staging_employees")


# ---------------------------------------------------------------------------
# Shippers
# ---------------------------------------------------------------------------

def load_shippers(spark: SparkSession) -> int:
    """Shippers, carried across unchanged."""
    query = "SELECT ShipperID, CompanyName, Phone FROM Shippers"

    df = read_from_op(spark, query).toDF("shipper_id", "company_name", "phone")
    truncate_staging_table("staging_shippers")
    return write_to_staging(df, "staging_shippers")


# ---------------------------------------------------------------------------
# Territories
# ---------------------------------------------------------------------------

def load_territories(spark: SparkSession) -> int:
    """Territories joined to Region — the second snowflake flattening.

    RTRIM is not cosmetic: both columns are CHAR in the source and arrive
    padded with trailing spaces. Left alone, the padding travels into the
    warehouse and every string comparison and GROUP BY downstream has to
    account for it.
    """
    query = """
        SELECT
            RTRIM(t.TerritoryID)          AS TerritoryID,
            RTRIM(t.TerritoryDescription) AS TerritoryDescription,
            RTRIM(r.RegionDescription)    AS RegionDescription
        FROM Territories AS t
        LEFT JOIN Region AS r ON r.RegionID = t.RegionID
    """
    df = read_from_op(spark, query).toDF(
        "territory_id", "territory_description", "region_description"
    )
    truncate_staging_table("staging_territories")
    return write_to_staging(df, "staging_territories")


# ---------------------------------------------------------------------------
# EmployeeTerritories
# ---------------------------------------------------------------------------

def load_employee_territories(spark: SparkSession) -> int:
    """The employee-to-territory bridge, source of the factless fact table."""
    query = """
        SELECT EmployeeID, RTRIM(TerritoryID) AS TerritoryID
        FROM EmployeeTerritories
    """
    df = read_from_op(spark, query).toDF("employee_id", "territory_id")
    truncate_staging_table("staging_employee_territories")
    return write_to_staging(df, "staging_employee_territories")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

LOADERS = {
    "geography": load_geography,
    "products": load_products,
    "suppliers": load_suppliers,
    "customers": load_customers,
    "employees": load_employees,
    "shippers": load_shippers,
    "territories": load_territories,
    "employee_territories": load_employee_territories,
}


def run_one(name: str) -> int:
    """Run a single loader. This is what the Airflow tasks call."""
    if name not in LOADERS:
        raise ValueError(f"Unknown loader '{name}'. Available: {sorted(LOADERS)}")

    with spark_session(f"op_to_staging_{name}") as spark:
        return LOADERS[name](spark)


def run_all() -> dict[str, int]:
    """Run every loader in one Spark session — useful outside Airflow."""
    results: dict[str, int] = {}
    with spark_session("op_to_staging_all") as spark:
        for name, loader in LOADERS.items():
            log.info("--- %s ---", name)
            results[name] = loader(spark)

    log.info("Summary:")
    for name, count in results.items():
        log.info("  %-22s %s rows", name, f"{count:,}")
    return results


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_one(sys.argv[1])
    else:
        run_all()
