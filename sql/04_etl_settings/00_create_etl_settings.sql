/* ===========================================================================
   ETL_Settings — the pipeline's own bookkeeping database.

   Kept separate from Northwind on purpose: this is metadata about the ETL
   process, not business data. Mixing the two would mean a restore of the
   source system silently resets the pipeline's position.

   Idempotent: safe to run more than once.
   =========================================================================== */

USE master;
GO

IF DB_ID('ETL_Settings') IS NULL
BEGIN
    CREATE DATABASE ETL_Settings;
    PRINT 'Database ETL_Settings created.';
END
ELSE
BEGIN
    PRINT 'Database ETL_Settings already exists — skipping creation.';
END
GO

USE ETL_Settings;
GO

/* ---------------------------------------------------------------------------
   CDC_State — how far the pipeline has read from each captured table.

   One row per source table. `state` holds the last processed LSN in the same
   textual form SSIS used, so the value stays comparable with the reference
   implementation:

       TFEND/CS/0x0000016E00001CF80003/TS/2026-07-24T08:15:00

   The pipeline reads this row before extracting, and updates it only after
   the load has succeeded. That ordering is what makes a failed run safe to
   retry: nothing is marked as processed until it actually is.
   --------------------------------------------------------------------------- */
IF OBJECT_ID('dbo.CDC_State', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.CDC_State
    (
        name          NVARCHAR(128)  NOT NULL,
        state         NVARCHAR(1000) NULL,
        last_run_time DATETIME2(3)   NULL,
        CONSTRAINT PK_CDC_State PRIMARY KEY (name)
    );
    PRINT 'Table dbo.CDC_State created.';
END
GO

/* Seed the two fact sources named in the project brief. */
MERGE dbo.CDC_State AS target
USING (VALUES ('Orders'), ('OrderDetails')) AS source(name)
    ON target.name = source.name
WHEN NOT MATCHED THEN
    INSERT (name, state, last_run_time) VALUES (source.name, NULL, NULL);
GO

/* ---------------------------------------------------------------------------
   ETL_Log — one row per task execution.

   Not requested by the brief, but a pipeline you cannot inspect after the
   fact is a pipeline you cannot trust. Airflow keeps its own task logs;
   this table records what the data actually did.
   --------------------------------------------------------------------------- */
IF OBJECT_ID('dbo.ETL_Log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ETL_Log
    (
        log_id        BIGINT IDENTITY(1,1) NOT NULL,
        dag_id        NVARCHAR(200)  NULL,
        task_id       NVARCHAR(200)  NULL,
        target_object NVARCHAR(200)  NULL,
        rows_read     BIGINT         NULL,
        rows_written  BIGINT         NULL,
        status        NVARCHAR(20)   NULL,   -- SUCCESS | FAILED
        message       NVARCHAR(MAX)  NULL,
        started_at    DATETIME2(3)   NULL,
        finished_at   DATETIME2(3)   NULL,
        CONSTRAINT PK_ETL_Log PRIMARY KEY (log_id)
    );
    PRINT 'Table dbo.ETL_Log created.';
END
GO

SELECT name, state, last_run_time FROM dbo.CDC_State;
GO
