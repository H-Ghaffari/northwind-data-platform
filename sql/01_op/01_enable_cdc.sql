/* ===========================================================================
   Enable Change Data Capture on the two fact sources.

   CDC reads the transaction log rather than the tables themselves, so it
   sees deletes — which a modified-date column cannot — and costs the source
   system almost nothing at write time.

   Two SQL Server Agent jobs appear once this runs:

     cdc.Northwind_capture   reads the log into the change tables
     cdc.Northwind_cleanup   drops change rows past their retention

   The capture job is what actually populates the change tables. If Agent is
   not running, CDC is enabled but nothing is ever captured — a failure mode
   that looks like "no changes happened" rather than an error.

   Idempotent: safe to run more than once.
   =========================================================================== */

USE Northwind;
GO

-- ---------------------------------------------------------------------------
-- Database level
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.databases
               WHERE name = 'Northwind' AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT 'CDC enabled on database Northwind.';
END
ELSE
    PRINT 'CDC already enabled on database Northwind.';
GO

-- ---------------------------------------------------------------------------
-- Orders
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables ct
               JOIN sys.tables t ON t.object_id = ct.source_object_id
               WHERE t.name = 'Orders')
BEGIN
    EXEC sys.sp_cdc_enable_table
         @source_schema      = N'dbo',
         @source_name        = N'Orders',
         @role_name          = NULL,          -- no gating role; sa reads it
         @capture_instance   = N'dbo_Orders',
         @supports_net_changes = 0;           -- requires a primary key; the
                                              -- pipeline resolves net effect
                                              -- itself from the raw rows
    PRINT 'CDC enabled on dbo.Orders.';
END
ELSE
    PRINT 'CDC already enabled on dbo.Orders.';
GO

-- ---------------------------------------------------------------------------
-- Order Details
--
-- The capture instance is named without the space. Everything downstream
-- refers to the capture instance rather than the table, so this is the name
-- that matters.
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables ct
               JOIN sys.tables t ON t.object_id = ct.source_object_id
               WHERE t.name = 'Order Details')
BEGIN
    EXEC sys.sp_cdc_enable_table
         @source_schema      = N'dbo',
         @source_name        = N'Order Details',
         @role_name          = NULL,
         @capture_instance   = N'dbo_OrderDetails',
         @supports_net_changes = 0;
    PRINT 'CDC enabled on dbo.[Order Details].';
END
ELSE
    PRINT 'CDC already enabled on dbo.[Order Details].';
GO

-- ---------------------------------------------------------------------------
-- Retention
--
-- Default is 3 days. The pipeline runs every 30 minutes, so three days is
-- ample; raising it only matters if the pipeline is expected to survive a
-- long outage. Left at the default and stated here so the assumption is
-- visible.
-- ---------------------------------------------------------------------------
EXEC sys.sp_cdc_change_job
     @job_type    = N'cleanup',
     @retention   = 4320;   -- minutes = 3 days
GO

-- ---------------------------------------------------------------------------
-- Verify
-- ---------------------------------------------------------------------------
PRINT '--- Capture instances ---';
SELECT
    ct.capture_instance,
    OBJECT_NAME(ct.source_object_id) AS source_table,
    ct.start_lsn,
    ct.create_date
FROM cdc.change_tables AS ct
ORDER BY ct.capture_instance;
GO

PRINT '--- Agent jobs (CDC does nothing without these running) ---';
SELECT job_type, maxtrans, maxscans, retention, threshold
FROM msdb.dbo.cdc_jobs
WHERE database_id = DB_ID('Northwind');
GO
