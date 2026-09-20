"""Staging → DW: load the dimensions, applying SCD rules.

One function per dimension. Each reads its staging table, resolves any
foreign surrogate keys, compares against what is already in the warehouse,
and writes the result.

The type 1 / type 2 split for each dimension is taken from the SSIS
packages in the reference project — specifically from which columns appear
in each package's two UPDATE statements. Getting this split wrong is
invisible until someone asks a historical question and gets a present-day
answer.

Load order matters and is enforced by the DAG, not here: suppliers must
exist before products can look up supplier_key, and geography before any
of the three dimensions that carry an address.
"""

from __future__ import annotations

import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.scd_utils import (
    OPEN_END_DATE,
    apply_scd,
    assign_surrogate_keys,
    load_current_dimension,
    next_surrogate_key,
    resolve_geography_keys,
)
from common.spark_utils import (
    dw_client,
    log,
    read_from_staging,
    spark_session,
    write_to_dw,
)


def _now() -> datetime:
    """One timestamp per task run.

    Every row written by a single load shares it, so a dimension's history
    lines up on run boundaries instead of on individual insert times.
    """
    return datetime.utcnow().replace(microsecond=0)


# ---------------------------------------------------------------------------
# DimGeography — no SCD, no source key
# ---------------------------------------------------------------------------

def load_dim_geography(spark: SparkSession) -> int:
    """Load DimGeography.

    No history and no type 1/2 distinction: an address either exists or it
    does not. Only addresses absent from the warehouse are inserted, so
    existing geography_keys stay stable and the dimensions pointing at them
    keep pointing at the right row.
    """
    incoming = read_from_staging(
        spark,
        "SELECT country, region, city, postal_code, address FROM staging_geography",
    )

    current = load_current_dimension(
        spark, "DimGeography",
        ["geography_key", "country", "region", "city", "postal_code", "address"],
    )

    if current is None:
        new_rows = incoming
    else:
        new_rows = incoming.join(
            current.select("country", "region", "city", "postal_code", "address"),
            on=["country", "region", "city", "postal_code", "address"],
            how="left_anti",
        )

    if new_rows.count() == 0:
        log.info("DimGeography is up to date")
        return 0

    start_key = next_surrogate_key("DimGeography", "geography_key")
    keyed = assign_surrogate_keys(new_rows, "geography_key", start_key, "country")

    columns = ["geography_key", "country", "region", "city", "postal_code", "address"]
    return write_to_dw(keyed.select(*columns), "DimGeography", columns)


# ---------------------------------------------------------------------------
# DimShippers — type 1 only
# ---------------------------------------------------------------------------

def load_dim_shippers(spark: SparkSession) -> int:
    """Load DimShippers.

    The reference schema gives this dimension no start_date or end_date, so
    it is type 1 throughout: a changed phone number overwrites rather than
    creating a version.
    """
    run_time = _now()

    incoming = read_from_staging(
        spark,
        "SELECT shipper_id AS shipper_alternate_key, company_name, phone "
        "FROM staging_shippers",
    )

    current = load_current_dimension(
        spark, "DimShippers",
        ["shipper_key", "shipper_alternate_key", "company_name", "phone"],
    )

    changes = apply_scd(
        incoming, current,
        business_key="shipper_alternate_key",
        type1_columns=["company_name", "phone"],
        type2_columns=[],
        run_time=run_time,
    )

    columns = ["shipper_key", "shipper_alternate_key", "company_name", "phone"]
    written = 0

    new_rows = changes["new"]
    if new_rows.count():
        start_key = next_surrogate_key("DimShippers", "shipper_key")
        keyed = assign_surrogate_keys(
            new_rows, "shipper_key", start_key, "shipper_alternate_key"
        )
        written += write_to_dw(keyed.select(*columns), "DimShippers", columns)

    # Type 1: reinsert under the existing surrogate key. The higher _version
    # default makes the merge keep this copy.
    type1_rows = changes["type1"]
    if type1_rows.count():
        updated = type1_rows.withColumn("shipper_key", F.col("cur_shipper_key"))
        written += write_to_dw(updated.select(*columns), "DimShippers", columns)

    return written


# ---------------------------------------------------------------------------
# DimTerritories — type 2 on the description
# ---------------------------------------------------------------------------

def load_dim_territories(spark: SparkSession) -> int:
    run_time = _now()

    incoming = read_from_staging(
        spark,
        "SELECT territory_id AS territory_alternate_key, "
        "       territory_description, region_description "
        "FROM staging_territories",
    )

    current = load_current_dimension(
        spark, "DimTerritories",
        ["territory_key", "territory_alternate_key", "region_description",
         "territory_description", "start_date", "end_date"],
    )

    changes = apply_scd(
        incoming, current,
        business_key="territory_alternate_key",
        # Taken from package 05: the type 1 UPDATE names TerritoryDescription,
        # so a renamed territory is a correction. RegionDescription is absent
        # from it, which makes moving a territory to another region a
        # historical event worth versioning.
        type1_columns=["territory_description"],
        type2_columns=["region_description"],
        run_time=run_time,
    )

    return _write_type2_dimension(
        table="DimTerritories",
        key_column="territory_key",
        business_key="territory_alternate_key",
        changes=changes,
        payload_columns=["region_description", "territory_description"],
        run_time=run_time,
    )


# ---------------------------------------------------------------------------
# DimSuppliers — type 2, needs geography
# ---------------------------------------------------------------------------

def load_dim_suppliers(spark: SparkSession) -> int:
    run_time = _now()

    staged = read_from_staging(
        spark,
        "SELECT supplier_id AS supplier_alternate_key, company_name, contact_name, "
        "       contact_title, phone, fax, COALESCE(home_page,'') AS home_page, "
        "       country, region, city, postal_code, address "
        "FROM staging_suppliers",
    )

    incoming = resolve_geography_keys(spark, staged).drop(
        "country", "region", "city", "postal_code", "address"
    )

    current = load_current_dimension(
        spark, "DimSuppliers",
        ["supplier_key", "supplier_alternate_key", "geography_key", "company_name",
         "contact_name", "contact_title", "phone", "fax", "home_page",
         "start_date", "end_date"],
    )

    changes = apply_scd(
        incoming, current,
        business_key="supplier_alternate_key",
        type1_columns=["company_name", "phone", "fax", "home_page"],
        type2_columns=["contact_name", "contact_title", "geography_key"],
        run_time=run_time,
    )

    return _write_type2_dimension(
        table="DimSuppliers",
        key_column="supplier_key",
        business_key="supplier_alternate_key",
        changes=changes,
        payload_columns=["geography_key", "company_name", "contact_name",
                         "contact_title", "phone", "fax", "home_page"],
        run_time=run_time,
    )


# ---------------------------------------------------------------------------
# DimCustomer — type 2, needs geography
# ---------------------------------------------------------------------------

def load_dim_customer(spark: SparkSession) -> int:
    run_time = _now()

    staged = read_from_staging(
        spark,
        "SELECT customer_id AS customer_alternate_key, company_name, contact_name, "
        "       contact_title, COALESCE(phone,'') AS phone, COALESCE(fax,'') AS fax, "
        "       country, region, city, postal_code, address "
        "FROM staging_customer",
    )

    incoming = resolve_geography_keys(spark, staged).drop(
        "country", "region", "city", "postal_code", "address"
    )

    current = load_current_dimension(
        spark, "DimCustomer",
        ["customer_key", "customer_alternate_key", "geography_key", "company_name",
         "contact_name", "contact_title", "phone", "fax", "start_date", "end_date"],
    )

    changes = apply_scd(
        incoming, current,
        business_key="customer_alternate_key",
        type1_columns=["company_name", "contact_title", "phone", "fax"],
        type2_columns=["contact_name", "geography_key"],
        run_time=run_time,
    )

    return _write_type2_dimension(
        table="DimCustomer",
        key_column="customer_key",
        business_key="customer_alternate_key",
        changes=changes,
        payload_columns=["geography_key", "company_name", "contact_name",
                         "contact_title", "phone", "fax"],
        run_time=run_time,
    )


# ---------------------------------------------------------------------------
# DimProducts — type 2, needs suppliers
# ---------------------------------------------------------------------------

def load_dim_products(spark: SparkSession) -> int:
    run_time = _now()

    staged = read_from_staging(
        spark,
        "SELECT product_id AS product_alternate_key, supplier_id, product_name, "
        "       COALESCE(category_name,'')     AS category_name, "
        "       COALESCE(quantity_per_unit,'') AS quantity_per_unit, "
        "       COALESCE(unit_price, 0)        AS unit_price, "
        "       COALESCE(units_in_stock, 0)    AS units_in_stock, "
        "       COALESCE(units_on_order, 0)    AS units_on_order, "
        "       COALESCE(reorder_level, 0)     AS reorder_level, "
        "       CASE WHEN discontinued THEN 1 ELSE 0 END AS discontinued "
        "FROM staging_products",
    )

    incoming = _resolve_supplier_keys(spark, staged).drop("supplier_id")

    current = load_current_dimension(
        spark, "DimProducts",
        ["product_key", "product_alternate_key", "supplier_key", "product_name",
         "category_name", "quantity_per_unit", "unit_price", "units_in_stock",
         "units_on_order", "reorder_level", "discontinued", "start_date", "end_date"],
    )

    changes = apply_scd(
        incoming, current,
        business_key="product_alternate_key",
        type1_columns=["product_name", "quantity_per_unit", "units_in_stock",
                       "units_on_order", "reorder_level"],
        type2_columns=["category_name", "unit_price", "discontinued", "supplier_key"],
        run_time=run_time,
    )

    return _write_type2_dimension(
        table="DimProducts",
        key_column="product_key",
        business_key="product_alternate_key",
        changes=changes,
        payload_columns=["supplier_key", "product_name", "category_name",
                         "quantity_per_unit", "unit_price", "units_in_stock",
                         "units_on_order", "reorder_level", "discontinued"],
        run_time=run_time,
    )


def _resolve_supplier_keys(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Attach supplier_key by looking up the supplier's alternate key."""
    client = dw_client()
    try:
        result = client.query(
            "SELECT supplier_key, supplier_alternate_key FROM DimSuppliers FINAL "
            "WHERE end_date = toDateTime('2106-01-01 00:00:00')"
        )
        rows = result.result_rows
    finally:
        client.close()

    if not rows:
        log.warning("DimSuppliers is empty — all supplier keys will be 0")
        return df.withColumn("supplier_key", F.lit(0).cast("int"))

    suppliers = spark.createDataFrame(
        rows, schema=["supplier_key", "s_alternate_key"]
    )

    return (
        df.join(
            suppliers,
            df["supplier_id"] == suppliers["s_alternate_key"],
            how="left",
        )
        .drop("s_alternate_key")
        .withColumn("supplier_key", F.coalesce(F.col("supplier_key"), F.lit(0)).cast("int"))
    )


# ---------------------------------------------------------------------------
# DimEmployees — type 2, self-referencing, two passes
# ---------------------------------------------------------------------------

def load_dim_employees(spark: SparkSession) -> int:
    """Load DimEmployees, pass one.

    parent_employee_key is left at 0 here. It holds the *surrogate* key of
    the manager, which cannot be resolved while the manager's own row may
    not exist yet. The second pass fills it in.
    """
    run_time = _now()

    staged = read_from_staging(
        spark,
        "SELECT employee_id AS employee_alternate_key, "
        "       COALESCE(reports_to, 0) AS reports_to, "
        "       first_name, last_name, full_name, "
        "       COALESCE(title,'')             AS title, "
        "       COALESCE(title_of_courtesy,'') AS title_of_courtesy, "
        "       birth_date, hire_date, "
        "       COALESCE(home_phone,'') AS home_phone, "
        "       COALESCE(extension,'')  AS extension, "
        "       COALESCE(notes,'')      AS notes, "
        "       COALESCE(photo_path,'') AS photo_path, "
        "       country, region, city, postal_code, address "
        "FROM staging_employees",
    )

    incoming = resolve_geography_keys(spark, staged).drop(
        "country", "region", "city", "postal_code", "address"
    )

    incoming = (
        incoming
        .withColumn("birth_date", F.col("birth_date").cast("date"))
        .withColumn("hire_date", F.col("hire_date").cast("date"))
    )

    current = load_current_dimension(
        spark, "DimEmployees",
        ["employee_key", "employee_alternate_key", "reports_to", "geography_key",
         "first_name", "last_name", "full_name", "title", "title_of_courtesy",
         "birth_date", "hire_date", "home_phone", "extension", "notes",
         "photo_path", "start_date", "end_date"],
    )

    changes = apply_scd(
        incoming, current,
        business_key="employee_alternate_key",
        # BirthDate sits in the type 1 list because the reference package
        # puts it there: a wrong date of birth is a correction, not history.
        type1_columns=["first_name", "last_name", "full_name", "title_of_courtesy",
                       "birth_date", "hire_date", "home_phone", "extension",
                       "photo_path"],
        type2_columns=["title", "geography_key", "reports_to", "notes"],
        run_time=run_time,
    )

    return _write_type2_dimension(
        table="DimEmployees",
        key_column="employee_key",
        business_key="employee_alternate_key",
        changes=changes,
        payload_columns=["reports_to", "geography_key", "first_name", "last_name",
                         "full_name", "title", "title_of_courtesy", "birth_date",
                         "hire_date", "home_phone", "extension", "notes",
                         "photo_path"],
        run_time=run_time,
        extra_defaults={"parent_employee_key": 0},
    )


def resolve_employee_hierarchy(spark: SparkSession) -> int:
    """Load DimEmployees, pass two: fill in parent_employee_key.

    Runs after every employee row exists. Maps each row's reports_to — a
    source key — to the surrogate key of the manager's currently-open row,
    then reinserts the row so the merge picks up the resolved value.

    Employees with no manager keep parent_employee_key 0. Andrew Fuller,
    the root of the Northwind hierarchy, is the only such row.
    """
    client = dw_client()
    try:
        result = client.query(
            "SELECT employee_key, employee_alternate_key, parent_employee_key, "
            "       reports_to, geography_key, first_name, last_name, full_name, "
            "       title, title_of_courtesy, birth_date, hire_date, "
            "       home_phone, extension, notes, photo_path, start_date, end_date "
            "FROM DimEmployees FINAL "
            "WHERE end_date = toDateTime('2106-01-01 00:00:00')"
        )
        rows = result.result_rows
    finally:
        client.close()

    if not rows:
        log.warning("DimEmployees is empty — nothing to resolve")
        return 0

    columns = ["employee_key", "employee_alternate_key", "parent_employee_key",
               "reports_to", "geography_key", "first_name", "last_name", "full_name",
               "title", "title_of_courtesy", "birth_date", "hire_date",
               "home_phone", "extension", "notes", "photo_path",
               "start_date", "end_date"]

    employees = spark.createDataFrame(rows, schema=columns)

    employees = (
        employees
        .withColumn("birth_date", F.col("birth_date").cast("date"))
        .withColumn("hire_date", F.col("hire_date").cast("date"))
    )

    managers = employees.select(
        F.col("employee_alternate_key").alias("m_alternate_key"),
        F.col("employee_key").alias("m_employee_key"),
    )

    resolved = (
        employees.drop("parent_employee_key")
        .join(
            managers,
            employees["reports_to"] == managers["m_alternate_key"],
            how="left",
        )
        .withColumn(
            "parent_employee_key",
            F.coalesce(F.col("m_employee_key"), F.lit(0)).cast("int"),
        )
        .drop("m_alternate_key", "m_employee_key")
    )

    unresolved = resolved.filter(
        (F.col("reports_to") != 0) & (F.col("parent_employee_key") == 0)
    ).count()
    if unresolved:
        log.warning("%s employees have an unresolvable manager", unresolved)

    return write_to_dw(resolved.select(*columns), "DimEmployees", columns)


# ---------------------------------------------------------------------------
# FactEmployeeTerritories — factless, full reload
# ---------------------------------------------------------------------------

def load_fact_employee_territories(spark: SparkSession) -> int:
    """Load the employee-territory bridge.

    A full reload rather than a merge: the table holds only two surrogate
    keys, so a relationship that disappears from the source should simply
    stop appearing here. ReplacingMergeTree deduplicates any repeats.
    """
    from common.spark_utils import truncate_dw_table

    staged = read_from_staging(
        spark,
        "SELECT employee_id, territory_id FROM staging_employee_territories",
    )

    client = dw_client()
    try:
        employees = client.query(
            "SELECT employee_key, employee_alternate_key FROM DimEmployees FINAL "
            "WHERE end_date = toDateTime('2106-01-01 00:00:00')"
        ).result_rows
        territories = client.query(
            "SELECT territory_key, territory_alternate_key FROM DimTerritories FINAL "
            "WHERE end_date = toDateTime('2106-01-01 00:00:00')"
        ).result_rows
    finally:
        client.close()

    if not employees or not territories:
        log.warning("Employee or territory dimension is empty — nothing to bridge")
        return 0

    emp_df = spark.createDataFrame(employees, schema=["employee_key", "e_alt"])
    terr_df = spark.createDataFrame(territories, schema=["territory_key", "t_alt"])

    bridged = (
        staged
        .join(emp_df, staged["employee_id"] == emp_df["e_alt"], how="inner")
        .join(terr_df, staged["territory_id"] == terr_df["t_alt"], how="inner")
        .select("employee_key", "territory_key")
        .distinct()
    )

    dropped = staged.count() - bridged.count()
    if dropped:
        log.warning("%s bridge rows dropped — key not found in a dimension", dropped)

    truncate_dw_table("FactEmployeeTerritories")
    columns = ["employee_key", "territory_key"]
    return write_to_dw(bridged, "FactEmployeeTerritories", columns)


# ---------------------------------------------------------------------------
# Shared writer for type 2 dimensions
# ---------------------------------------------------------------------------

def _write_type2_dimension(
    table: str,
    key_column: str,
    business_key: str,
    changes: dict[str, DataFrame],
    payload_columns: list[str],
    run_time: datetime,
    extra_defaults: dict | None = None,
) -> int:
    """Write the three SCD outcomes for a type 2 dimension.

    All three are inserts:

      new     a fresh surrogate key, open end_date
      type 1  the same surrogate key and start_date, higher _version, so the
              merge replaces the stored copy in place
      type 2  the old row reinserted with end_date closed, plus a new row
              with a fresh key and an open end_date
    """
    all_columns = [key_column, business_key] + payload_columns + ["start_date", "end_date"]
    if extra_defaults:
        all_columns = (
            [key_column] + list(extra_defaults) + [business_key]
            + payload_columns + ["start_date", "end_date"]
        )

    written = 0
    next_key = next_surrogate_key(table, key_column)

    def with_defaults(df: DataFrame) -> DataFrame:
        for column, value in (extra_defaults or {}).items():
            df = df.withColumn(column, F.lit(value).cast("int"))
        return df

    # --- new rows -----------------------------------------------------
    new_rows = changes["new"]
    new_count = new_rows.count()
    if new_count:
        keyed = assign_surrogate_keys(new_rows, key_column, next_key, business_key)
        keyed = (
            with_defaults(keyed)
            .withColumn("start_date", F.lit(run_time).cast("timestamp"))
            .withColumn("end_date", F.lit(OPEN_END_DATE).cast("timestamp"))
        )
        written += write_to_dw(keyed.select(*all_columns), table, all_columns)
        next_key += new_count

    # --- type 1: overwrite in place -----------------------------------
    type1_rows = changes["type1"]
    if type1_rows.count():
        updated = (
            type1_rows
            .withColumn(key_column, F.col(f"cur_{key_column}"))
            .withColumn("start_date", F.col("cur_start_date"))
            .withColumn("end_date", F.lit(OPEN_END_DATE).cast("timestamp"))
        )
        updated = with_defaults(updated)
        written += write_to_dw(updated.select(*all_columns), table, all_columns)

    # --- type 2: close the old row, open a new one --------------------
    type2_rows = changes["type2"]
    type2_count = type2_rows.count()
    if type2_count:
        # Close: reinsert the stored version with end_date set to now.
        closed = (
            type2_rows
            .withColumn(key_column, F.col(f"cur_{key_column}"))
            .withColumn("start_date", F.col("cur_start_date"))
            .withColumn("end_date", F.lit(run_time).cast("timestamp"))
        )
        for column in payload_columns:
            closed = closed.withColumn(column, F.col(f"cur_{column}"))
        closed = with_defaults(closed)
        written += write_to_dw(closed.select(*all_columns), table, all_columns)

        # Open: a new version carrying the incoming values.
        opened = assign_surrogate_keys(type2_rows, key_column, next_key, business_key)
        opened = (
            with_defaults(opened)
            .withColumn("start_date", F.lit(run_time).cast("timestamp"))
            .withColumn("end_date", F.lit(OPEN_END_DATE).cast("timestamp"))
        )
        written += write_to_dw(opened.select(*all_columns), table, all_columns)

    return written


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

LOADERS = {
    "dim_geography": load_dim_geography,
    "dim_shippers": load_dim_shippers,
    "dim_territories": load_dim_territories,
    "dim_suppliers": load_dim_suppliers,
    "dim_customer": load_dim_customer,
    "dim_products": load_dim_products,
    "dim_employees": load_dim_employees,
    "employee_hierarchy": resolve_employee_hierarchy,
    "fact_employee_territories": load_fact_employee_territories,
}


def run_one(name: str) -> int:
    if name not in LOADERS:
        raise ValueError(f"Unknown loader '{name}'. Available: {sorted(LOADERS)}")

    with spark_session(f"staging_to_dw_{name}") as spark:
        return LOADERS[name](spark)


def run_all() -> dict[str, int]:
    """Run every loader in dependency order — useful outside Airflow."""
    results: dict[str, int] = {}
    with spark_session("staging_to_dw_all") as spark:
        for name, loader in LOADERS.items():
            log.info("--- %s ---", name)
            results[name] = loader(spark)

    log.info("Summary:")
    for name, count in results.items():
        log.info("  %-28s %s rows written", name, f"{count:,}")
    return results


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_one(sys.argv[1])
    else:
        run_all()
