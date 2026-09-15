-- ---------------------------------------------------------------------------
-- Enable CDC on every source the streaming path reads.
--
-- All eleven, including Orders and Order Details. The batch setup enables
-- those two as well, but the streaming path must not depend on the batch
-- path having been run: someone bringing up only the Kafka stack never
-- executes setup_all.sh.
--
-- The failure this prevents is the silent one. Stream_State would still be
-- seeded with dbo_Orders, the producer would still start cleanly, and
-- fn_cdc_get_min_lsn would return NULL — which the producer treats as a
-- warning and skips. Dimensions would flow, facts never would, and every
-- surface anyone checks would look healthy.
--
-- Each table is guarded, so one already captured is skipped rather than
-- re-enabled. Safe to run before, after, or instead of the batch setup.
-- ---------------------------------------------------------------------------
USE Northwind;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases
               WHERE name = 'Northwind' AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT 'Database-level CDC enabled.';
END
ELSE
    PRINT 'Database-level CDC already enabled.';
GO

-- ---------------------------------------------------------------------------
-- capture_instance is spelled out rather than left to default. The default
-- for "Order Details" would be dbo_Order Details, with a space, and every
-- CDC function name is built from that string. The names here match what the
-- batch path already uses, so a table it enabled is recognised as enabled.
-- ---------------------------------------------------------------------------
DECLARE @sources TABLE (
    source_table     SYSNAME,
    capture_instance SYSNAME
);

INSERT INTO @sources (source_table, capture_instance) VALUES
    -- fact sources
    ('Orders',              'dbo_Orders'),
    ('Order Details',       'dbo_OrderDetails'),
    -- dimension sources
    ('Customers',           'dbo_Customers'),
    ('Products',            'dbo_Products'),
    ('Employees',           'dbo_Employees'),
    ('Suppliers',           'dbo_Suppliers'),
    ('Shippers',            'dbo_Shippers'),
    ('Territories',         'dbo_Territories'),
    ('EmployeeTerritories', 'dbo_EmployeeTerritories'),
    -- reference sources: snowflake branches the warehouse flattens.
    -- The batch path resolves them with a join in staging and never stores
    -- the id; the streaming consumer has no staging and needs both.
    ('Categories',          'dbo_Categories'),
    ('Region',              'dbo_Region');

DECLARE @table   SYSNAME,
        @capture SYSNAME;

DECLARE source_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT source_table, capture_instance FROM @sources;

OPEN source_cursor;
FETCH NEXT FROM source_cursor INTO @table, @capture;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (SELECT 1
                   FROM sys.tables t
                   JOIN sys.schemas s ON s.schema_id = t.schema_id
                   WHERE s.name = 'dbo' AND t.name = @table)
    BEGIN
        -- Louder than skipping. A missing source means the model cannot be
        -- built, and a warning in a log scrolls past.
        RAISERROR('Source table dbo.%s does not exist.', 16, 1, @table);
    END
    ELSE IF EXISTS (SELECT 1
                    FROM sys.tables t
                    JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE s.name = 'dbo' AND t.name = @table
                      AND t.is_tracked_by_cdc = 1)
    BEGIN
        PRINT 'CDC already enabled on dbo.' + @table;
    END
    ELSE
    BEGIN
        EXEC sys.sp_cdc_enable_table
            @source_schema        = N'dbo',
            @source_name          = @table,
            @capture_instance     = @capture,
            -- NULL means any member of db_owner may read the change tables.
            -- A gating role would be right in production; here the producer
            -- connects as sa and the extra role only adds setup.
            @role_name            = NULL,
            -- Net changes would collapse several updates to one row into a
            -- single result. The consumer applies SCD rules per change, and
            -- collapsing would erase intermediate type 2 versions.
            @supports_net_changes = 0;

        PRINT 'CDC enabled on dbo.' + @table + ' as ' + @capture;
    END

    FETCH NEXT FROM source_cursor INTO @table, @capture;
END

CLOSE source_cursor;
DEALLOCATE source_cursor;
GO

SELECT
    ct.capture_instance,
    s.name + '.' + t.name AS source_table
FROM cdc.change_tables ct
JOIN sys.tables   t ON t.object_id = ct.source_object_id
JOIN sys.schemas  s ON s.schema_id = t.schema_id
ORDER BY ct.capture_instance;
GO