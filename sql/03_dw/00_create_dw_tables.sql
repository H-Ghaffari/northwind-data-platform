/* ===========================================================================
   DW layer — ClickHouse

   The star schema. Structure follows the SSAS Data Source View from the
   reference project; storage choices are ClickHouse-specific.

   Three things behave differently here than in a row store:

     1. ORDER BY is not a primary key. It sets physical sort order and the
        sparse index. ClickHouse enforces no uniqueness at all.

     2. ReplacingMergeTree deduplicates rows sharing an ORDER BY tuple,
        keeping the highest _version — but only during background merges.
        Read with FINAL when correctness matters more than speed.

     3. Nullable columns cost an extra stored column. end_date therefore uses
        a sentinel far-future date instead of NULL, so "the current row" is
        `end_date = '2999-12-31'` rather than `end_date IS NULL`.

   Idempotent: safe to run more than once.
   =========================================================================== */

CREATE DATABASE IF NOT EXISTS NorthwindDW;

USE NorthwindDW;

-- ---------------------------------------------------------------------------
-- DimGeography — no source key, so the address tuple is the natural key
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DimGeography
(
    geography_key   UInt32,
    country         String,
    region          String,
    city            String,
    postal_code     String,
    address         String,
    _version        UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (geography_key);

-- ---------------------------------------------------------------------------
-- DimDate — generated, never sourced
--
-- date_key is a smart key in yyyyMMdd form, so facts can derive it directly
-- from a date without a lookup.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DimDate
(
    date_key                 UInt32,
    full_date                Date,
    calendar_year            UInt16,
    calendar_season          UInt8,
    season_name              String,
    month_number_of_year     UInt8,
    month_name               String,
    day_number_of_month      UInt8,
    day_of_week              UInt8,
    day_of_week_name         String
)
ENGINE = ReplacingMergeTree
ORDER BY (date_key);

-- ---------------------------------------------------------------------------
-- DimSuppliers — SCD Type 2
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DimSuppliers
(
    supplier_key           UInt32,
    supplier_alternate_key Int32,
    geography_key          UInt32,
    company_name           String,
    contact_name           String,
    contact_title          String,
    phone                  String,
    fax                    String,
    home_page              String,
    start_date             DateTime,
    end_date               DateTime DEFAULT toDateTime('2999-12-31 00:00:00'),
    _version               UInt64   DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (supplier_alternate_key, start_date);

-- ---------------------------------------------------------------------------
-- DimProducts — SCD Type 2
--
-- Categories does not exist as a dimension: category_name was folded in at
-- the staging step. That flattening is what turns the model from a snowflake
-- into a star.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DimProducts
(
    product_key            UInt32,
    product_alternate_key  Int32,
    supplier_key           UInt32,
    product_name           String,
    category_name          String,
    quantity_per_unit      String,
    unit_price             Decimal(19, 4),
    units_in_stock         Int16,
    units_on_order         Int16,
    reorder_level          Int16,
    discontinued           UInt8,
    start_date             DateTime,
    end_date               DateTime DEFAULT toDateTime('2999-12-31 00:00:00'),
    _version               UInt64   DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (product_alternate_key, start_date);

-- ---------------------------------------------------------------------------
-- DimCustomer — SCD Type 2
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DimCustomer
(
    customer_key            UInt32,
    customer_alternate_key  String,
    geography_key           UInt32,
    company_name            String,
    contact_name            String,
    contact_title           String,
    phone                   String,
    fax                     String,
    start_date              DateTime,
    end_date                DateTime DEFAULT toDateTime('2999-12-31 00:00:00'),
    _version                UInt64   DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (customer_alternate_key, start_date);

-- ---------------------------------------------------------------------------
-- DimEmployees — SCD Type 2, self-referencing
--
-- parent_employee_key holds the *surrogate* key of the manager, so it can
-- only be filled once every employee row exists. The load is therefore two
-- passes: insert everyone, then resolve the hierarchy.
--
-- full_name and age are derived, not sourced.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DimEmployees
(
    employee_key            UInt32,
    parent_employee_key     UInt32 DEFAULT 0,
    employee_alternate_key  Int32,
    reports_to              Int32  DEFAULT 0,
    geography_key           UInt32,
    first_name              String,
    last_name               String,
    full_name               String,
    title                   String,
    title_of_courtesy       String,
    birth_date              Nullable(DateTime),
    age                     UInt8,
    hire_date               Nullable(DateTime),
    home_phone              String,
    extension               String,
    notes                   String,
    photo_path              String,
    start_date              DateTime,
    end_date                DateTime DEFAULT toDateTime('2999-12-31 00:00:00'),
    _version                UInt64   DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (employee_alternate_key, start_date);

-- ---------------------------------------------------------------------------
-- DimShippers — SCD Type 1 only
--
-- The reference schema gives this dimension no start_date/end_date, so
-- changes overwrite rather than accumulate.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DimShippers
(
    shipper_key            UInt32,
    shipper_alternate_key  Int32,
    company_name           String,
    phone                  String,
    _version               UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (shipper_alternate_key);

-- ---------------------------------------------------------------------------
-- DimTerritories — SCD Type 2
--
-- Region is folded in as region_description; the second snowflake-to-star
-- flattening in this model.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DimTerritories
(
    territory_key            UInt32,
    territory_alternate_key  String,
    region_description       String,
    territory_description    String,
    start_date               DateTime,
    end_date                 DateTime DEFAULT toDateTime('2999-12-31 00:00:00'),
    _version                 UInt64   DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (territory_alternate_key, start_date);

-- ---------------------------------------------------------------------------
-- FactOrders
--
-- Grain: one row per (order, product) — the grain of Order Details.
--
-- Master/detail caution: freight, ship_name and the date keys come from the
-- parent Orders row and repeat across every line of an order. Summing
-- freight over this table double-counts; aggregate it over distinct
-- order_id instead.
--
-- Partitioned by order month so reporting queries over a date range skip
-- whole partitions.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FactOrders
(
    order_id            Int32,
    product_key         UInt32,
    geography_key       UInt32,
    customer_key        UInt32,
    employee_key        UInt32,
    shipper_key         UInt32,
    order_date_key      UInt32,
    required_date_key   UInt32,
    shipped_date_key    UInt32,
    freight             Decimal(19, 4),
    unit_price          Decimal(19, 4),
    quantity            Int16,
    discount            Float32,
    line_total          Decimal(19, 4)  MATERIALIZED
                            unit_price * quantity * (1 - toDecimal64(discount, 4)),
    ship_name           String,
    order_date          Nullable(DateTime),
    required_date       Nullable(DateTime),
    shipped_date        Nullable(DateTime),
    is_deleted          UInt8  DEFAULT 0,
    _version            UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
PARTITION BY toYYYYMM(ifNull(order_date, toDateTime('1900-01-01')))
ORDER BY (order_id, product_key);

-- ---------------------------------------------------------------------------
-- FactEmployeeTerritories — a factless fact table
--
-- No measures. It records that a relationship exists, which is enough to
-- answer "how many territories does each employee cover".
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FactEmployeeTerritories
(
    employee_key   UInt32,
    territory_key  UInt32,
    _version       UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (employee_key, territory_key);

-- ---------------------------------------------------------------------------
-- Convenience views: the currently-active row of each Type 2 dimension.
--
-- FINAL forces the deduplicating merge at read time. On dimensions this
-- size the cost is irrelevant, and it removes a whole class of "why did I
-- get two rows" confusion.
-- ---------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS v_DimProducts_Current AS
SELECT * FROM DimProducts FINAL
WHERE end_date = toDateTime('2999-12-31 00:00:00');

CREATE VIEW IF NOT EXISTS v_DimCustomer_Current AS
SELECT * FROM DimCustomer FINAL
WHERE end_date = toDateTime('2999-12-31 00:00:00');

CREATE VIEW IF NOT EXISTS v_DimEmployees_Current AS
SELECT * FROM DimEmployees FINAL
WHERE end_date = toDateTime('2999-12-31 00:00:00');

CREATE VIEW IF NOT EXISTS v_DimSuppliers_Current AS
SELECT * FROM DimSuppliers FINAL
WHERE end_date = toDateTime('2999-12-31 00:00:00');

CREATE VIEW IF NOT EXISTS v_DimTerritories_Current AS
SELECT * FROM DimTerritories FINAL
WHERE end_date = toDateTime('2999-12-31 00:00:00');

-- Live rows only: FINAL applies both the version replacement and the
-- is_deleted tombstones. Reporting should read this, never the raw table.
CREATE VIEW IF NOT EXISTS v_FactOrders_Current AS
SELECT * FROM FactOrders FINAL WHERE is_deleted = 0;
