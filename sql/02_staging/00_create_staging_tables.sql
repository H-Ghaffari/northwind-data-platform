/* ===========================================================================
   Staging layer — PostgreSQL

   Purpose: integration. Rows arrive here already joined and cleaned, so the
   Staging → DW step only has to deal with change detection, not shape.

   Two families of tables live here:

     1. Mirror tables   — truncated and fully reloaded on every run.
                          Feed the dimensions.
     2. CDC tables      — hold only what changed since the last watermark,
                          split by operation. Feed the facts.

   Naming: PostgreSQL folds unquoted identifiers to lower case, so
   `staging_products` and `Staging_Products` refer to the same table. Lower
   case is used throughout to avoid ever needing quotes.

   Idempotent: safe to run more than once.
   =========================================================================== */

-- ---------------------------------------------------------------------------
-- Mirror tables (dimension sources)
-- ---------------------------------------------------------------------------

/* Geography is the union of every address-bearing table in the source.
   No natural key exists, so the four-part address acts as one. */
CREATE TABLE IF NOT EXISTS staging_geography
(
    country      VARCHAR(15),
    region       VARCHAR(15),
    city         VARCHAR(15),
    postal_code  VARCHAR(10),
    address      VARCHAR(60),
    _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

/* Products arrives pre-joined with Categories: CategoryID is dropped and
   CategoryName carried across. Description and Picture are discarded.
   This is the Snowflake → Star flattening. */
CREATE TABLE IF NOT EXISTS staging_products
(
    product_id        INTEGER,
    product_name      VARCHAR(40),
    supplier_id       INTEGER,
    category_name     VARCHAR(15),
    quantity_per_unit VARCHAR(20),
    unit_price        NUMERIC(19,4),
    units_in_stock    SMALLINT,
    units_on_order    SMALLINT,
    reorder_level     SMALLINT,
    discontinued      BOOLEAN,
    _loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging_suppliers
(
    supplier_id   INTEGER,
    company_name  VARCHAR(40),
    contact_name  VARCHAR(30),
    contact_title VARCHAR(30),
    address       VARCHAR(60),
    city          VARCHAR(15),
    region        VARCHAR(15),
    postal_code   VARCHAR(10),
    country       VARCHAR(15),
    phone         VARCHAR(24),
    fax           VARCHAR(24),
    home_page     TEXT,
    _loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging_customer
(
    customer_id   CHAR(5),
    company_name  VARCHAR(40),
    contact_name  VARCHAR(30),
    contact_title VARCHAR(30),
    address       VARCHAR(60),
    city          VARCHAR(15),
    region        VARCHAR(15),
    postal_code   VARCHAR(10),
    country       VARCHAR(15),
    phone         VARCHAR(24),
    fax           VARCHAR(24),
    _loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

/* full_name and age do not exist in the source. They are derived during the
   OP → Staging step — the "New Column" cleaning the brief calls for. */
CREATE TABLE IF NOT EXISTS staging_employees
(
    employee_id       INTEGER,
    last_name         VARCHAR(20),
    first_name        VARCHAR(10),
    full_name         VARCHAR(31),
    title             VARCHAR(30),
    title_of_courtesy VARCHAR(25),
    birth_date        TIMESTAMP,
    age               INTEGER,
    hire_date         TIMESTAMP,
    address           VARCHAR(60),
    city              VARCHAR(15),
    region            VARCHAR(15),
    postal_code       VARCHAR(10),
    country           VARCHAR(15),
    home_phone        VARCHAR(24),
    extension         VARCHAR(4),
    photo             BYTEA,
    notes             TEXT,
    reports_to        INTEGER,
    photo_path        VARCHAR(255),
    _loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging_shippers
(
    shipper_id   INTEGER,
    company_name VARCHAR(40),
    phone        VARCHAR(24),
    _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

/* Territories arrives pre-joined with Region — the second flattening. */
CREATE TABLE IF NOT EXISTS staging_territories
(
    territory_id          VARCHAR(20),
    territory_description VARCHAR(50),
    region_description    VARCHAR(50),
    _loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging_employee_territories
(
    employee_id  INTEGER,
    territory_id VARCHAR(20),
    _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

/* Full snapshot of Orders, used for the initial fact load before CDC takes
   over. Kept separate from the CDC tables below on purpose. */
CREATE TABLE IF NOT EXISTS staging_orders
(
    order_id        INTEGER,
    customer_id     CHAR(5),
    employee_id     INTEGER,
    order_date      TIMESTAMP,
    required_date   TIMESTAMP,
    shipped_date    TIMESTAMP,
    ship_via        INTEGER,
    freight         NUMERIC(19,4),
    ship_name       VARCHAR(40),
    ship_address    VARCHAR(60),
    ship_city       VARCHAR(15),
    ship_region     VARCHAR(15),
    ship_postal_code VARCHAR(10),
    ship_country    VARCHAR(15),
    _loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging_order_details
(
    order_id   INTEGER,
    product_id INTEGER,
    unit_price NUMERIC(19,4),
    quantity   SMALLINT,
    discount   REAL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- CDC tables (fact sources)
--
-- One table per operation rather than a single table with an operation flag.
-- The DW step treats each differently, and keeping them apart means no
-- filtering — and no chance of applying the wrong branch to a row.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging_orders_insert (LIKE staging_orders INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS staging_orders_update (LIKE staging_orders INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS staging_orders_delete (LIKE staging_orders INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS staging_order_details_insert (LIKE staging_order_details INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS staging_order_details_update (LIKE staging_order_details INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS staging_order_details_delete (LIKE staging_order_details INCLUDING DEFAULTS);

-- ---------------------------------------------------------------------------
-- Indexes
--
-- Staging is written far more often than it is read, and every table is
-- truncated on each run, so indexes are kept to the few columns the DW step
-- actually joins on.
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS ix_staging_products_id    ON staging_products (product_id);
CREATE INDEX IF NOT EXISTS ix_staging_suppliers_id   ON staging_suppliers (supplier_id);
CREATE INDEX IF NOT EXISTS ix_staging_customer_id    ON staging_customer (customer_id);
CREATE INDEX IF NOT EXISTS ix_staging_employees_id   ON staging_employees (employee_id);
CREATE INDEX IF NOT EXISTS ix_staging_orders_id      ON staging_orders (order_id);
CREATE INDEX IF NOT EXISTS ix_staging_od_order_id    ON staging_order_details (order_id, product_id);

CREATE INDEX IF NOT EXISTS ix_staging_geography_key
    ON staging_geography (country, region, city, postal_code);
