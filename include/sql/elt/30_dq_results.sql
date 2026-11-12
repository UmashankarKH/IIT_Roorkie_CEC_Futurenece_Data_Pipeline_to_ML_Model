-- Quality scorecard: one dq_results row per check per run, computed from the chk_* flags in staging.
-- The catalog gives each check its dimension and severity; the orders batch (not the merged table) is scored
-- so an incremental run reports on the increment file only.
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP
);
DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}';

CREATE OR REPLACE TEMP TABLE dq_catalog(check_name VARCHAR, dimension VARCHAR, severity VARCHAR) ;
INSERT INTO dq_catalog VALUES
 ('stores_store_id_not_null','Completeness','Critical'), ('stores_zone_not_null','Completeness','Critical'),
 ('stores_store_id_unique','Uniqueness','Critical'), ('stores_zone_valid','Validity','Critical'),
 ('stores_pincode_6_digits','Validity','Warning'), ('stores_capacity_positive','Accuracy','Warning'),
 ('stores_open_date_not_future','Timeliness','Critical'),
 ('products_product_id_not_null','Completeness','Critical'), ('products_category_not_null','Completeness','Critical'),
 ('products_product_id_unique','Uniqueness','Critical'), ('products_mrp_positive','Accuracy','Critical'),
 ('products_cost_price_positive','Accuracy','Critical'), ('products_cost_not_above_mrp','Accuracy','Critical'),
 ('products_launch_date_not_future','Timeliness','Critical'), ('products_is_perishable_valid','Validity','Warning'),
 ('customers_customer_id_not_null','Completeness','Critical'), ('customers_city_not_null','Completeness','Warning'),
 ('customers_customer_id_unique','Uniqueness','Critical'), ('customers_email_format','Validity','Warning'),
 ('customers_phone_10_digits','Validity','Warning'), ('customers_pincode_6_digits','Validity','Warning'),
 ('customers_signup_date_parses','Validity','Critical'), ('customers_is_active_valid','Validity','Warning'),
 ('customers_loyalty_tier_casing','Consistency','Warning'), ('customers_signup_date_not_future','Timeliness','Critical'),
 ('orders_order_id_not_null','Completeness','Critical'), ('orders_customer_id_not_null','Completeness','Critical'),
 ('orders_order_ts_not_null','Completeness','Critical'), ('orders_delivered_has_delivery_minutes','Completeness','Critical'),
 ('orders_order_id_unique','Uniqueness','Critical'), ('orders_status_valid','Validity','Critical'),
 ('orders_payment_mode_valid','Validity','Critical'), ('orders_order_ts_parses','Validity','Critical'),
 ('orders_discount_pct_0_100','Validity','Critical'), ('orders_channel_valid','Validity','Warning'),
 ('orders_quantity_positive','Accuracy','Critical'), ('orders_unit_price_positive','Accuracy','Critical'),
 ('orders_amount_matches_formula','Accuracy','Critical'), ('orders_delivery_minutes_below_120','Accuracy','Critical'),
 ('orders_delivery_minutes_normal_8_35','Accuracy','Warning'), ('orders_unit_price_within_mrp','Accuracy','Warning'),
 ('orders_customer_in_clean_customers','Consistency','Critical'), ('orders_product_in_clean_products','Consistency','Critical'),
 ('orders_store_in_clean_stores','Consistency','Critical'), ('orders_order_ts_not_future','Timeliness','Critical'),
 ('payments_payment_id_not_null','Completeness','Critical'), ('payments_order_id_not_null','Completeness','Critical'),
 ('payments_gateway_ref_present','Completeness','Warning'), ('payments_payment_id_unique','Uniqueness','Critical'),
 ('payments_payment_mode_valid','Validity','Critical'), ('payments_status_valid','Validity','Critical'),
 ('payments_payment_ts_parses','Validity','Critical'), ('payments_amount_positive','Accuracy','Critical'),
 ('payments_order_exists','Consistency','Critical'), ('payments_payment_ts_not_future','Timeliness','Critical');

INSERT INTO dq_results
WITH counts AS (
    SELECT replace(check_col, 'chk_', '') AS check_name, sum(failed::INT) AS rows_failed, count(*) AS rows_checked
    FROM (UNPIVOT stg_stores     ON COLUMNS('^chk_') INTO NAME check_col VALUE failed)
    GROUP BY 1
    UNION ALL
    SELECT replace(check_col, 'chk_', ''), sum(failed::INT), count(*)
    FROM (UNPIVOT stg_products   ON COLUMNS('^chk_') INTO NAME check_col VALUE failed) GROUP BY 1
    UNION ALL
    SELECT replace(check_col, 'chk_', ''), sum(failed::INT), count(*)
    FROM (UNPIVOT stg_customers  ON COLUMNS('^chk_') INTO NAME check_col VALUE failed) GROUP BY 1
    UNION ALL
    SELECT replace(check_col, 'chk_', ''), sum(failed::INT), count(*)
    FROM (UNPIVOT stg_orders_batch ON COLUMNS('^chk_') INTO NAME check_col VALUE failed) GROUP BY 1
    UNION ALL
    SELECT replace(check_col, 'chk_', ''), sum(failed::INT), count(*)
    FROM (UNPIVOT stg_payments   ON COLUMNS('^chk_') INTO NAME check_col VALUE failed) GROUP BY 1
)
SELECT '{{run_id}}', '{{dag_id}}', c.check_name, k.dimension, k.severity, c.rows_checked, c.rows_failed,
       CASE WHEN c.rows_failed = 0 THEN 'PASS' WHEN k.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, now()
FROM counts c JOIN dq_catalog k USING (check_name);

-- Timeliness (dataset level): newest plausible (not future-dated) order must be within 1 day of "now".
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'orders_latest_order_not_stale', 'Timeliness', 'Warning', 1,
       (coalesce(max(order_ts) FILTER (WHERE order_ts <= TIMESTAMP '{{as_of}}'), TIMESTAMP '1970-01-01') < TIMESTAMP '{{as_of}}' - INTERVAL 1 DAY)::INT,
       CASE WHEN coalesce(max(order_ts) FILTER (WHERE order_ts <= TIMESTAMP '{{as_of}}'), TIMESTAMP '1970-01-01') < TIMESTAMP '{{as_of}}' - INTERVAL 1 DAY THEN 'WARN' ELSE 'PASS' END, now()
FROM stg_orders_batch;

-- Volume (dataset level, full loads only): more than 90% fewer orders than the peak of earlier runs.
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'orders_volume_vs_previous_run', 'Completeness', 'Warning', 1,
       (cur.n < 0.1 * prev.n)::INT, CASE WHEN cur.n < 0.1 * prev.n THEN 'WARN' ELSE 'PASS' END, now()
FROM (SELECT count(*) AS n FROM stg_orders_batch) cur,
     (SELECT max(rows_checked) AS n FROM dq_results
      WHERE dag_id = '{{dag_id}}' AND run_id <> '{{run_id}}' AND check_name = 'orders_order_id_not_null') prev
WHERE '{{mode}}' = 'full' AND prev.n IS NOT NULL;
