-- ---------------------------------------------------------------------------
-- StreamEvents — one row per event the consumer applies.
--
-- Airflow's task history answers "did the batch run succeed". Nothing in the
-- streaming path answers the equivalent, because there are no runs: there is
-- a continuous flow that is either keeping up or falling behind. This table
-- is how that is measured.
--
-- lag_ms is the number the near-real-time claim rests on: the gap between
-- the source committing a transaction and the warehouse reflecting it. An
-- average that climbs means the consumer is losing ground, visible here long
-- before a dashboard looks stale.
--
-- Distinct from the MongoDB audit log added later. MongoDB keeps the whole
-- event document, for reading one record and asking what happened to it.
-- This keeps numbers, for drawing a line on a Grafana panel. Full payloads
-- in a columnar store would bloat every part and slow the aggregate queries
-- that are the entire point of the table.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS NorthwindRT.StreamEvents
(
    event_time        DateTime64(3),        -- when the consumer applied it
    source_time       DateTime64(3),        -- when SQL Server committed it
    lag_ms            Int64,                -- source_time -> event_time
    capture_instance  LowCardinality(String),
    topic             LowCardinality(String),
    target_table      LowCardinality(String),
    operation         LowCardinality(String), -- insert / update / delete
    scd_outcome       LowCardinality(String), -- new / type1 / type2 / unchanged / skipped
    business_key      String,
    rows_written      UInt32,
    kafka_partition   UInt16,
    kafka_offset      UInt64,
    error             String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, capture_instance)
-- Operational telemetry, not warehouse data. Thirty days is long enough to
-- investigate an incident and short enough that the table never becomes a
-- storage problem of its own.
TTL toDateTime(event_time) + INTERVAL 30 DAY;