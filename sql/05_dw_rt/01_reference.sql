-- ---------------------------------------------------------------------------
-- Reference tables for the streaming path.
--
-- The batch path resolves Categories into DimProducts and Region into
-- DimTerritories with a join in staging. The streaming path has no staging
-- layer: the consumer sees one row at a time and cannot join it against a
-- source it does not hold. These tables are that source.
--
-- They are not dimensions. No history, no surrogate key, never queried by a
-- dashboard. They exist so the consumer can answer "what is the name behind
-- this id" without going back to SQL Server on every message.
--
-- They also work in reverse: when a category is renamed, the old name read
-- here identifies the DimProducts rows that need a new version.
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS NorthwindRT;

CREATE TABLE IF NOT EXISTS NorthwindRT.RefCategories
(
    category_id    Int32,
    category_name  String,
    _version       UInt64,
    _updated_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY category_id;

CREATE TABLE IF NOT EXISTS NorthwindRT.RefRegion
(
    region_id           Int32,
    region_description  String,
    _version            UInt64,
    _updated_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY region_id;