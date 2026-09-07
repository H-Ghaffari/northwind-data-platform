/* ===========================================================================
   OP layer — create the Northwind operational database.

   instnwnd.sql (Microsoft's original script) deliberately does not create a
   database; it expects the caller to have selected one. This script creates
   the container so instnwnd.sql can be run against it afterwards.

   Idempotent: safe to run more than once.
   =========================================================================== */

USE master;
GO

IF DB_ID('Northwind') IS NULL
BEGIN
    CREATE DATABASE Northwind;
    PRINT 'Database Northwind created.';
END
ELSE
BEGIN
    PRINT 'Database Northwind already exists — skipping creation.';
END
GO

/* CDC requires the database to be in a recovery model that keeps the
   transaction log. SIMPLE still works for CDC, but FULL is what a real
   operational system would use, and it makes the CDC behaviour in phase two
   representative of production. */
ALTER DATABASE Northwind SET RECOVERY FULL;
GO
