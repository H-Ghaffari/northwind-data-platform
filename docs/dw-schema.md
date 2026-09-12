# NorthwindDW — Target Schema Reference

Reverse-engineered from the reference SSAS/SSIS project: the table and
column definitions come from `NorthwindDW_DSV.dsv` (the SSAS data source
view, which stores a complete copy of the warehouse schema as XML), and the
load logic from the SQL embedded in the fourteen `.dtsx` packages.

This document describes the **target** the pipeline was built against. Where
the implementation deviates, the deviation is listed in the project README
rather than silently applied here.

Database names in the reference project:

- `Northwind_BI_1404_05` — OP layer
- `Northwind_BI_1404_05_Staging` — Staging layer
- `Northwind_BI_1404_05_DW` — DW layer

---

## 1. Star schema

```
                        DimDate
                           │
      DimProducts ─────────┼───────── DimCustomer
                           │
      DimSuppliers ──── FactOrders ──── DimEmployees
                           │
      DimShippers ─────────┼───────── DimGeography
                           │
                   DimTerritories
                           │
                 FactEmployeeTerritories
```

Two fact tables:

| Table | Grain | Source |
|---|---|---|
| `FactOrders` | one row per (order × product) | `Orders` ⋈ `Order Details` |
| `FactEmployeeTerritories` | one row per (employee × territory) | `EmployeeTerritories` |

Eight dimensions: `DimDate`, `DimGeography`, `DimProducts`, `DimSuppliers`,
`DimCustomer`, `DimEmployees`, `DimShippers`, `DimTerritories`.

---

## 2. Dimensions

### DimGeography

The base dimension — no dependencies, loaded first.

| Column | Type | Null | Role |
|---|---|---|---|
| `GeographyKey` | `int` | ✗ | Surrogate key |
| `Country` | `nvarchar(15)` | ✓ | |
| `Region` | `nvarchar(15)` | ✓ | |
| `City` | `nvarchar(15)` | ✓ | |
| `PostalCode` | `nvarchar(10)` | ✓ | |
| `Address` | `nvarchar(60)` | ✓ | |

No alternate key. The address tuple is the natural key, and every lookup
against this dimension matches on it.

Source — the union of four queries:

```sql
SELECT Country, Region, City, PostalCode, Address FROM Customers
UNION
SELECT Country, Region, City, PostalCode, Address FROM Employees
UNION
SELECT Country, Region, City, PostalCode, Address FROM Suppliers
UNION
SELECT ShipCountry, ShipRegion, ShipCity, ShipPostalCode, ShipAddress FROM Orders
```

For an unmodified Northwind this yields 135 distinct rows.

---

### DimSuppliers

| Column | Type | Null | Role |
|---|---|---|---|
| `SupplierKey` | `int` | ✗ | Surrogate key |
| `SupplierAlternateKey` | `int` | ✓ | Source key (`SupplierID`) |
| `GeographyKey` | `int` | ✓ | FK → `DimGeography` |
| `CompanyName` | `nvarchar(40)` | ✓ | SCD Type 1 |
| `ContactName` | `nvarchar(30)` | ✓ | SCD Type 2 |
| `ContactTitle` | `nvarchar(30)` | ✓ | SCD Type 2 |
| `Phone` | `nvarchar(24)` | ✓ | SCD Type 1 |
| `Fax` | `nvarchar(24)` | ✓ | SCD Type 1 |
| `HomePage` | `ntext` | ✓ | Not compared |
| `Startdate` | `datetime` | ✓ | Metadata |
| `Enddate` | `datetime` | ✓ | Metadata |

`HomePage` appears in neither UPDATE statement of package 02, so it takes
part in no SCD comparison at all.

---

### DimProducts

| Column | Type | Null | Role |
|---|---|---|---|
| `ProductKey` | `int` | ✗ | Surrogate key |
| `ProductAlternateKey` | `int` | ✗ | Source key (`ProductID`) |
| `SupplierKey` | `int` | ✓ | FK → `DimSuppliers` |
| `ProductName` | `nvarchar(40)` | ✓ | SCD Type 1 |
| `CategoryName` | `nvarchar(15)` | ✓ | SCD Type 2 — from `Categories` |
| `QuantityPerUnit` | `nvarchar(20)` | ✓ | SCD Type 1 |
| `UnitPrice` | `money` | ✓ | SCD Type 2 |
| `UnitsInStock` | `smallint` | ✓ | SCD Type 1 |
| `UnitsOnOrder` | `smallint` | ✓ | SCD Type 1 |
| `ReorderLevel` | `smallint` | ✓ | SCD Type 1 |
| `Discontinued` | `bit` | ✓ | SCD Type 2 |
| `Startdate` | `datetime` | ✗ | Metadata |
| `Enddate` | `datetime` | ✓ | Metadata |

**Denormalisation:** `Categories` does not exist in the DW. It is joined to
`Products` in staging, `CategoryID` is dropped and `CategoryName` carried
in its place; `Description` and `Picture` are discarded entirely. This is
what turns the model from a snowflake into a star.

---

### DimCustomer

| Column | Type | Null | Role |
|---|---|---|---|
| `CustomerKey` | `int` | ✗ | Surrogate key |
| `CustomerAlternateKey` | `nchar(5)` | ✓ | Source key (`CustomerID`) |
| `GeographyKey` | `int` | ✓ | FK → `DimGeography` |
| `CompanyName` | `nvarchar(40)` | ✓ | SCD Type 1 |
| `ContactName` | `nvarchar(30)` | ✓ | SCD Type 2 |
| `ContactTitle` | `nvarchar(30)` | ✓ | SCD Type 1 |
| `Phone` | `nvarchar(24)` | ✓ | SCD Type 1 |
| `Fax` | `nvarchar(24)` | ✓ | SCD Type 1 |
| `Startdate` | `datetime` | ✗ | Metadata |
| `Enddate` | `datetime` | ✓ | Metadata |

---

### DimEmployees

The most complex dimension — three features set it apart.

| Column | Type | Null | Role |
|---|---|---|---|
| `EmployeeKey` | `int` | ✗ | Surrogate key |
| `ParentEmployeeKey` | `int` | ✓ | Self-reference — manager's surrogate key |
| `EmployeeAlternateKey` | `int` | ✓ | Source key (`EmployeeID`) |
| `ReportsTo` | `int` | ✓ | Manager's source key |
| `GeographyKey` | `int` | ✓ | FK → `DimGeography` |
| `FirstName` | `nvarchar(10)` | ✓ | SCD Type 1 |
| `LastName` | `nvarchar(20)` | ✓ | SCD Type 1 |
| `Title` | `nvarchar(30)` | ✓ | SCD Type 2 |
| `TitleOfCourtesy` | `nvarchar(25)` | ✓ | SCD Type 1 |
| `BirthDate` | `datetime` | ✓ | SCD Type 1 |
| `HireDate` | `datetime` | ✓ | SCD Type 1 |
| `HomePhone` | `nvarchar(24)` | ✓ | SCD Type 1 |
| `Extension` | `nvarchar(4)` | ✓ | SCD Type 1 |
| `Photo` | `image` | ✓ | SCD Type 2 — replaced by the data lake |
| `Notes` | `ntext` | ✓ | SCD Type 2 |
| `PhotoPath` | `nvarchar(255)` | ✓ | SCD Type 1 |
| `Startdate` | `datetime` | ✗ | Metadata |
| `Enddate` | `datetime` | ✓ | Metadata |
| `FullName` | `nvarchar(31)` | ✓ | Computed — `FirstName + ' ' + LastName` |
| `Age` | `int` | ✓ | Computed — from `BirthDate` |

**Self-reference.** The load runs in two passes. Every row is inserted
first, then a second pass resolves `ParentEmployeeKey` by looking up
`ReportsTo`:

```sql
UPDATE DimEmployees SET ParentEmployeeKey = ? WHERE EmployeeKey = ?
```

`ParentEmployeeKey` holds a *surrogate* key, which cannot be resolved while
the manager's own row may not exist yet.

**Computed columns.** `FullName` and `Age` do not exist in the source and
are derived in the transformation layer — the "New Column" step the brief
calls for.

**Photo.** The brief moves the photographs out of the database entirely,
onto a data lake keyed on employee code.

---

### DimShippers

The simplest dimension — no `Startdate`/`Enddate`, so SCD Type 1 throughout.

| Column | Type | Null | Role |
|---|---|---|---|
| `ShipperKey` | `int` | ✗ | Surrogate key |
| `ShipperAlternateKey` | `int` | ✓ | Source key (`ShipperID`) |
| `CompanyName` | `nvarchar(40)` | ✓ | SCD Type 1 |
| `Phone` | `nvarchar(24)` | ✓ | SCD Type 1 |

---

### DimTerritories

| Column | Type | Null | Role |
|---|---|---|---|
| `TerritoryKey` | `int` | ✗ | Surrogate key |
| `TerritoryAlternateKey` | `nvarchar(20)` | ✓ | Source key (`TerritoryID`) |
| `RegionDescription` | `nvarchar(50)` | ✓ | SCD Type 2 — from `Region` |
| `TerritoryDescription` | `nvarchar(50)` | ✓ | SCD Type 1 |
| `Startdate` | `datetime` | ✗ | Metadata |
| `Enddate` | `datetime` | ✓ | Metadata |

**Denormalisation:** `Region` does not exist in the DW. `RegionDescription`
is folded into `DimTerritories` by a lookup — the second snowflake-to-star
flattening in this model.

**Note on the SCD split.** Package 05's type 1 UPDATE names
`TerritoryDescription`, leaving `RegionDescription` to the historical path.
Renaming a territory is a correction; moving it to another region is an
event worth versioning.

Both columns are `CHAR` in the source and arrive space-padded. Without
`RTRIM` the padding travels into the warehouse and every string comparison
and `GROUP BY` downstream has to account for it.

---

### DimDate

A generated dimension — no source system.

| Column | Type | Null | Notes |
|---|---|---|---|
| `DateKey` | `int` | ✗ | Smart key, `yyyyMMdd` — e.g. `19960704` |
| `FullDateAlternateKey` | `date` | ✗ | The actual date |
| `CalendarYear` | `smallint` | ✗ | |
| `CalendarSeason` | `tinyint` | ✗ | 1–4 |
| `SeasonName` | `nvarchar(10)` | ✓ | |
| `MonthNumberOfYear` | `tinyint` | ✗ | |
| `MonthName` | `nvarchar(10)` | ✗ | |
| `DayNumberOfMonth` | `tinyint` | ✓ | |
| `DayOfWeek` | `smallint` | ✗ | |
| `DayOfWeekName` | `nvarchar(30)` | ✗ | |

**Smart key pattern.** `DateKey` is an integer in `yyyyMMdd` form, so facts
derive it directly rather than looking it up:

```sql
format(OrderDate, 'yyyyMMdd') AS OrderdateKey
```

This is the one place in the model where a surrogate key is derived rather
than assigned — an accepted exception, since a date never changes.

---

## 3. Fact tables

### FactOrders

| Column | Type | Null | Kind |
|---|---|---|---|
| `OrderID` | `int` | ✗ | Degenerate dimension |
| `ProductKey` | `int` | ✗ | FK |
| `GeographyKey` | `int` | ✓ | FK |
| `CustomerKey` | `int` | ✓ | FK |
| `EmployeeKey` | `int` | ✓ | FK |
| `ShipperKey` | `int` | ✓ | FK |
| `OrderdateKey` | `int` | ✓ | FK → `DimDate` |
| `RequiredDateKey` | `int` | ✓ | FK → `DimDate` |
| `ShippedDateKey` | `int` | ✓ | FK → `DimDate` |
| `Freight` | `money` | ✓ | Measure — from the parent |
| `UnitPrice` | `money` | ✓ | Measure — from the child |
| `Quantity` | `smallint` | ✓ | Measure — from the child |
| `Discount` | `real` | ✓ | Measure — from the child |
| `ShipName` | `nvarchar(40)` | ✓ | Degenerate — from the parent |
| `OrderDate` | `datetime` | ✓ | Helper |
| `ShippedDate` | `datetime` | ✓ | Helper |
| `RequiredDate` | `datetime` | ✓ | Helper |

Composite key: `(OrderID, ProductKey)` — the grain of the table.

**Master/detail caution.** `Orders` is the parent and `Order Details` the
child. `Freight`, `ShipName` and the dates come from the parent and repeat
across every line of an order. Summing `Freight` over this table
double-counts; aggregate it over distinct `OrderID` instead.

---

### FactEmployeeTerritories

| Column | Type | Null |
|---|---|---|
| `EmployeeKey` | `int` | ✗ |
| `TerritoryKey` | `int` | ✗ |

A factless fact table — no measures. It records that a relationship exists,
which is enough to answer "how many territories does each employee cover".

---

## 4. Staging tables

### Mirror tables — full load

Truncated and reloaded on every run:

- `Staging_Geography`
- `Staging_Suppliers`
- `Staging_Products`
- `Staging_Customer`
- `Staging_Employees`
- `Staging_Shippers`
- `Staging_Territories`
- `Staging_EmployeeTerritories`
- `Staging_Orders`
- `Staging_OrderDetails`

### CDC tables — incremental load

CDC output split by operation:

| Table | Contents |
|---|---|
| `Staging_Orders_Insert` | New orders |
| `Staging_Orders_Update` | Amended orders |
| `Staging_Orders_Delete` | Deleted orders |
| `Staging_OrderDetails_Insert` | New lines |
| `Staging_OrderDetails_Update` | Amended lines |
| `Staging_OrderDetails_Delete` | Deleted lines |

All six are truncated at the start of every run. The split is not
decoration: the warehouse step treats each operation differently, and
keeping them apart means no filtering and no chance of applying the wrong
branch to a row.

---

## 5. Mandatory load order

Derived from the lookup dependencies and not negotiable:

```
1.  DimGeography              (no dependencies)
2.  DimDate                   (no dependencies — generated)
3.  DimSuppliers              → needs DimGeography
4.  DimShippers               (no dependencies)
5.  DimTerritories            (no dependencies)
6.  DimCustomer               → needs DimGeography
7.  DimEmployees              → needs DimGeography, then self-update
8.  DimProducts               → needs DimSuppliers
9.  FactEmployeeTerritories   → needs DimEmployees + DimTerritories
10. FactOrders                → needs every dimension above
```

This is the same dependency graph `00_ETL_Orchestration.dtsx` expresses with
precedence constraints, and it translates directly to `>>` between Airflow
tasks.

Getting it wrong does not fail loudly: the lookup simply returns nothing and
rows land with key 0, which looks like a data problem rather than an
ordering one.

---

## 6. The inferred member pattern

Package 13 contains these statements:

```sql
insert into dbo.DimCustomer(CustomerAlternateKey) values(?)
insert into dbo.DimEmployees(EmployeeAlternateKey) values(?)
insert into dbo.DimProducts(ProductAlternateKey) values(?)
insert into dbo.DimShippers(ShipperAlternateKey) values(?)
insert into dbo.DimGeography(Country,Region,City,PostalCode,[Address]) values(?,?,?,?,?)
```

**The problem.** Dimensions reload nightly; facts reload every thirty
minutes. In the gap, an order can arrive for a customer the warehouse has
never seen. If the lookup fails, that order is lost.

**The solution.** A stub row is created in the dimension carrying only its
alternate key, with every other column left at its zero value. The order
lands with a valid foreign key, and the next dimension load matches the stub
on alternate key and fills in the rest — taking the type 1 path like any
other existing row.

Stubs stay identifiable afterwards by their empty attributes, which is what
makes them auditable.

---

## 7. The SCD pattern in the reference packages

Each dimension package has three output paths.

**Type 1 — overwrite in place:**

```sql
UPDATE DimProducts
SET ProductName = ?, QuantityPerUnit = ?, UnitsInStock = ?
WHERE ProductAlternateKey = ?
```

**Type 2 — close the old row:**

```sql
UPDATE DimProducts
SET Enddate = ?
WHERE ProductAlternateKey = ? AND Enddate IS NULL
```

Followed by an insert with `Startdate = now()` and `Enddate = NULL`.

**New — a genuinely new row:** a direct insert.

> The active row is always the one where `Enddate IS NULL`.

---

## 8. Reading the type assignments

The type 1 / type 2 split for each dimension is not documented anywhere in
the reference project. It was derived by comparing two statements in each
package:

1. The **full update** — used on the inferred-member path, listing every
   attribute the dimension carries.
2. The **type 1 update** — a subset.

Any column in the first but not the second is a type 2 attribute: it is not
overwritten, so a change to it must produce a new version.

That inference is the authority for the table in the project README. Where
the implementation differs, the difference is listed there under known
deviations.
