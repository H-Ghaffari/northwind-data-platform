/* ===========================================================================
   OP layer — verification.

   Run after the schema and data load to confirm the source system is in the
   state the pipeline expects. Row counts are the reference values for the
   standard Northwind sample.
   =========================================================================== */

USE Northwind;
GO

PRINT '--- Table inventory ---';
SELECT
    t.name                      AS table_name,
    SUM(p.rows)                 AS row_count
FROM sys.tables      AS t
JOIN sys.partitions  AS p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
GROUP BY t.name
ORDER BY t.name;
GO

PRINT '--- Expected row counts ---';
/*
    Categories             8
    CustomerCustomerDemo   0
    CustomerDemographics   0
    Customers             91
    Employees              9
    EmployeeTerritories   49
    Order Details       2155
    Orders               830
    Products              77
    Region                 4
    Shippers               3
    Suppliers             29
    Territories           53
*/
GO

PRINT '--- Referential sanity: orphaned order details ---';
SELECT COUNT(*) AS orphaned_details
FROM [Order Details] AS od
LEFT JOIN Orders     AS o ON o.OrderID = od.OrderID
WHERE o.OrderID IS NULL;
GO

PRINT '--- Employee hierarchy (self-reference) ---';
SELECT
    e.EmployeeID,
    e.FirstName + ' ' + e.LastName  AS employee,
    e.ReportsTo,
    m.FirstName + ' ' + m.LastName  AS manager
FROM Employees AS e
LEFT JOIN Employees AS m ON m.EmployeeID = e.ReportsTo
ORDER BY e.ReportsTo, e.EmployeeID;
GO

PRINT '--- Distinct geography rows the DW will need ---';
SELECT COUNT(*) AS distinct_geography FROM (
    SELECT Country, Region, City, PostalCode, Address FROM Customers
    UNION
    SELECT Country, Region, City, PostalCode, Address FROM Employees
    UNION
    SELECT Country, Region, City, PostalCode, Address FROM Suppliers
    UNION
    SELECT ShipCountry, ShipRegion, ShipCity, ShipPostalCode, ShipAddress FROM Orders
) AS g;
GO

USE ETL_Settings;
GO

PRINT '--- ETL bookkeeping ---';
SELECT name, state, last_run_time FROM dbo.CDC_State;
GO
