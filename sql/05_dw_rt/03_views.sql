-- ---------------------------------------------------------------------------
-- Views the streaming path owns.
--
-- The v_*_Current views are not defined here: setup_dw_rt.sh clones them so
-- their definitions cannot drift from the batch path's. Only views with no
-- batch equivalent belong in this file.
-- ---------------------------------------------------------------------------

-- Current state of each reference table, FINAL applied so an unmerged part
-- cannot show a stale name. The consumer reads these, never the raw tables.
CREATE OR REPLACE VIEW NorthwindRT.v_RefCategories_Current AS
SELECT category_id, category_name
FROM NorthwindRT.RefCategories FINAL;

CREATE OR REPLACE VIEW NorthwindRT.v_RefRegion_Current AS
SELECT region_id, region_description
FROM NorthwindRT.RefRegion FINAL;

-- Pipeline health, one row per capture instance over the last hour. Grafana
-- reads this rather than aggregating StreamEvents in a panel query, so the
-- definition of "healthy" lives in git and not in a dashboard JSON.
CREATE OR REPLACE VIEW NorthwindRT.v_StreamHealth AS
SELECT
    capture_instance,
    count()                AS events,
    sum(rows_written)      AS rows_written,
    round(avg(lag_ms))     AS avg_lag_ms,
    quantile(0.95)(lag_ms) AS p95_lag_ms,
    max(lag_ms)            AS max_lag_ms,
    max(event_time)        AS last_event,
    countIf(error != '')   AS errors
FROM NorthwindRT.StreamEvents
WHERE event_time >= now() - INTERVAL 1 HOUR
GROUP BY capture_instance
ORDER BY capture_instance;

-- ---------------------------------------------------------------------------
-- Comparing the two warehouses.
--
-- Matched on the alternate key, never on the surrogate key. A surrogate key
-- is invented at load time and its value depends on the order rows happened
-- to arrive in, so the same customer can be key 42 in one warehouse and 91
-- in the other with nothing wrong. The alternate key is the source system's
-- own identifier and means the same thing in both.
--
-- That also makes the comparison independent of how the streaming warehouse
-- was seeded — copied from the batch one, or rebuilt from the source with a
-- fresh numbering.
--
-- A difference is not automatically a fault. The two paths run under
-- separate profiles on a 16 GB machine, so whichever has been running has
-- seen changes the other has not. These views say where they differ, not
-- that something is broken; the meaningful check is that they agree right
-- after the snapshot, before either path has moved on.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW NorthwindRT.v_PathComparison AS
SELECT 'DimCustomer' AS dimension,
       (SELECT count() FROM NorthwindDW.v_DimCustomer_Current) AS batch_rows,
       (SELECT count() FROM NorthwindRT.v_DimCustomer_Current) AS stream_rows,
       (SELECT count() FROM
          (SELECT customer_alternate_key AS k FROM NorthwindDW.v_DimCustomer_Current
           INTERSECT
           SELECT customer_alternate_key FROM NorthwindRT.v_DimCustomer_Current)) AS in_both
UNION ALL
SELECT 'DimProducts',
       (SELECT count() FROM NorthwindDW.v_DimProducts_Current),
       (SELECT count() FROM NorthwindRT.v_DimProducts_Current),
       (SELECT count() FROM
          (SELECT product_alternate_key AS k FROM NorthwindDW.v_DimProducts_Current
           INTERSECT
           SELECT product_alternate_key FROM NorthwindRT.v_DimProducts_Current))
UNION ALL
SELECT 'DimEmployees',
       (SELECT count() FROM NorthwindDW.v_DimEmployees_Current),
       (SELECT count() FROM NorthwindRT.v_DimEmployees_Current),
       (SELECT count() FROM
          (SELECT employee_alternate_key AS k FROM NorthwindDW.v_DimEmployees_Current
           INTERSECT
           SELECT employee_alternate_key FROM NorthwindRT.v_DimEmployees_Current))
UNION ALL
SELECT 'DimSuppliers',
       (SELECT count() FROM NorthwindDW.v_DimSuppliers_Current),
       (SELECT count() FROM NorthwindRT.v_DimSuppliers_Current),
       (SELECT count() FROM
          (SELECT supplier_alternate_key AS k FROM NorthwindDW.v_DimSuppliers_Current
           INTERSECT
           SELECT supplier_alternate_key FROM NorthwindRT.v_DimSuppliers_Current))
UNION ALL
SELECT 'DimTerritories',
       (SELECT count() FROM NorthwindDW.v_DimTerritories_Current),
       (SELECT count() FROM NorthwindRT.v_DimTerritories_Current),
       (SELECT count() FROM
          (SELECT territory_alternate_key AS k FROM NorthwindDW.v_DimTerritories_Current
           INTERSECT
           SELECT territory_alternate_key FROM NorthwindRT.v_DimTerritories_Current))
UNION ALL
SELECT 'FactOrders',
       (SELECT count() FROM NorthwindDW.v_FactOrders_Current),
       (SELECT count() FROM NorthwindRT.v_FactOrders_Current),
       0;

-- Attribute-level differences for one dimension. Counts say the two
-- warehouses disagree; this says which entity and which value, which is what
-- anyone actually needs when investigating.
CREATE OR REPLACE VIEW NorthwindRT.v_CustomerDrift AS
SELECT
    coalesce(b.customer_alternate_key, r.customer_alternate_key) AS customer,
    b.company_name  AS batch_company,
    r.company_name  AS stream_company,
    b.contact_name  AS batch_contact,
    r.contact_name  AS stream_contact,
    multiIf(b.customer_alternate_key = '', 'stream only',
            r.customer_alternate_key = '', 'batch only',
            'differs') AS status
FROM NorthwindDW.v_DimCustomer_Current AS b
FULL OUTER JOIN NorthwindRT.v_DimCustomer_Current AS r
    ON b.customer_alternate_key = r.customer_alternate_key
WHERE b.customer_alternate_key = ''
   OR r.customer_alternate_key = ''
   OR b.company_name != r.company_name
   OR b.contact_name != r.contact_name;