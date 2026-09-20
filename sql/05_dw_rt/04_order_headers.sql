-- ---------------------------------------------------------------------------
-- RtOrderHeaders — the streaming path's order header cache.
--
-- FactOrders has the grain of Order Details, so every fact row needs both
-- sides of a master/detail pair. The batch path gets them together: staging
-- snapshots the affected orders and joins. The streaming path receives them
-- as separate events on separate topics, and Kafka orders within a partition
-- and not across them — so a line can arrive before its header.
--
-- This table is where a header waits for its lines. It is not a dimension
-- and not a fact: it is the consumer's working memory, which is the role the
-- brief assigns it when it says the consumer is the staging of this path.
-- The Rt prefix is deliberate, so nobody reads it as part of the star.
--
-- Kept in ClickHouse rather than in process memory so a consumer restart
-- does not lose headers whose lines have not arrived yet.
--
-- Keys are stored already resolved. A three-line order would otherwise do
-- the same four dimension lookups three times.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS NorthwindRT.RtOrderHeaders
(
    order_id           Int32,

    geography_key      UInt32,
    customer_key       UInt32,
    employee_key       UInt32,
    shipper_key        UInt32,

    order_date_key     UInt32,
    required_date_key  UInt32,
    shipped_date_key   UInt32,

    order_date         Nullable(DateTime),
    required_date      Nullable(DateTime),
    shipped_date       Nullable(DateTime),

    freight            Decimal(19, 4),
    ship_name          String,

    is_deleted         UInt8    DEFAULT 0,
    _version           UInt64   DEFAULT toUnixTimestamp64Milli(now64()),

    -- When the consumer wrote this header, not when the order was placed.
    -- Northwind's orders are from the 1990s, so an expiry keyed on
    -- order_date would delete every header the moment it was written.
    seen_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY order_id
-- A header exists so a line that arrived first can find it, and that window
-- is seconds. A week rather than minutes because a consumer down over a
-- weekend should still find the headers its backlog refers to. Nothing older
-- is ever read: a header change rewrites its lines from FactOrders, not from
-- here.
TTL seen_at + INTERVAL 7 DAY;