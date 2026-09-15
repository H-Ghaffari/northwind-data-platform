-- ---------------------------------------------------------------------------
-- Stream_State — the streaming path's replay position.
--
-- Deliberately separate from CDC_State. Both answer "how far have we read",
-- but for readers on different clocks: CDC_State is advanced by an Airflow
-- DAG every thirty minutes, Stream_State by the producer every few seconds.
-- One shared table would let either writer move a watermark the other had
-- not consumed, and the resulting skipped window fails silently.
--
-- The watermark here means "published to Kafka", not "present in the
-- warehouse". The consumer tracks its own progress through Kafka offsets, so
-- a dead consumer never forces the producer back to the source.
-- ---------------------------------------------------------------------------

-- The batch setup creates this database. Created here too, so the streaming
-- path can be brought up on its own — this is the only object it needs from
-- ETL_Settings, and requiring the whole batch setup for one table would make
-- the two paths dependent for no reason.
IF DB_ID('ETL_Settings') IS NULL
BEGIN
    CREATE DATABASE ETL_Settings;
    PRINT 'Database ETL_Settings created.';
END
GO

USE ETL_Settings;
GO

IF OBJECT_ID('dbo.Stream_State', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Stream_State (
        capture_instance  SYSNAME        NOT NULL,

        -- Everything up to and including this LSN has been published.
        -- NULL on a fresh row means "never read" — the producer then starts
        -- from the capture instance's own minimum LSN.
        last_lsn          BINARY(10)     NULL,

        -- Human-readable mirror of last_lsn. Not used for reads: an LSN is
        -- exact, a timestamp is approximate. It exists so that someone
        -- looking at this table can tell at a glance whether the stream is
        -- live or stalled.
        last_lsn_time     DATETIME2(3)   NULL,

        -- Dimension deletes are not propagated, per the brief. Storing the
        -- rule beside the watermark keeps the producer free of a hardcoded
        -- table list — the behaviour of a source is described by its row.
        propagate_deletes BIT            NOT NULL CONSTRAINT DF_Stream_State_del DEFAULT 0,

        -- Kafka topic this instance publishes to. Held here rather than in
        -- producer config so adding a source is one INSERT, not a code
        -- change plus a redeploy.
        topic_name        NVARCHAR(200)  NOT NULL,

        -- Lets a single source be paused without stopping the producer.
        is_active         BIT            NOT NULL CONSTRAINT DF_Stream_State_act DEFAULT 1,

        rows_published    BIGINT         NOT NULL CONSTRAINT DF_Stream_State_rows DEFAULT 0,
        last_run_at       DATETIME2(3)   NULL,
        last_error        NVARCHAR(2000) NULL,

        CONSTRAINT PK_Stream_State PRIMARY KEY (capture_instance)
    );

    PRINT 'Table dbo.Stream_State created.';
END
ELSE
    PRINT 'Table dbo.Stream_State already exists.';
GO

-- ---------------------------------------------------------------------------
-- Seed one row per capture instance.
--
-- last_lsn stays NULL so the first run starts from each instance's own
-- minimum LSN rather than a value guessed here. MERGE rather than INSERT, so
-- rerunning adds new sources without resetting the watermark of existing
-- ones.
-- ---------------------------------------------------------------------------
MERGE dbo.Stream_State AS target
USING (VALUES
    -- Fact sources: deletes are real events and reach the warehouse as
    -- tombstones, exactly as the batch path already handles them.
    ('dbo_Orders',              1, N'nw.cdc.orders'),
    ('dbo_OrderDetails',        1, N'nw.cdc.order_details'),

    -- Dimension and reference sources: deletes are captured by CDC but
    -- dropped by the producer. The brief is explicit that dimension deletes
    -- are not carried into the warehouse.
    ('dbo_Customers',           0, N'nw.cdc.customers'),
    ('dbo_Products',            0, N'nw.cdc.products'),
    ('dbo_Employees',           0, N'nw.cdc.employees'),
    ('dbo_Suppliers',           0, N'nw.cdc.suppliers'),
    ('dbo_Shippers',            0, N'nw.cdc.shippers'),
    ('dbo_Territories',         0, N'nw.cdc.territories'),
    ('dbo_EmployeeTerritories', 0, N'nw.cdc.employee_territories'),
    ('dbo_Categories',          0, N'nw.cdc.categories'),
    ('dbo_Region',              0, N'nw.cdc.region')
) AS source (capture_instance, propagate_deletes, topic_name)
ON target.capture_instance = source.capture_instance

WHEN NOT MATCHED BY TARGET THEN
    INSERT (capture_instance, propagate_deletes, topic_name)
    VALUES (source.capture_instance, source.propagate_deletes, source.topic_name)

-- Routing and delete policy are configuration, so the script owns them and
-- corrects drift. The watermark is runtime state and is never touched.
WHEN MATCHED THEN
    UPDATE SET
        target.propagate_deletes = source.propagate_deletes,
        target.topic_name        = source.topic_name;
GO

SELECT
    capture_instance, topic_name, propagate_deletes, is_active,
    last_lsn, last_lsn_time, rows_published
FROM dbo.Stream_State
ORDER BY capture_instance;
GO