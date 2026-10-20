-- include/sql/dq/dq_results_elt.sql
-- ELT data quality scorecard (assignment Part D)
--
-- Writes one row per quality check executed by this ELT run into the shared
-- dq_results table - the same table the ETL pipeline writes to, so the two
-- pipelines can be compared check by check.
--
-- Three groups of checks are inserted:
--   1. row-level rules       - counted from elt_rule_failures
--   2. volume checks         - did a file shrink by more than 90%?
--   3. freshness check       - is the newest order older than one day?
--
-- Parameters substituted by the Airflow task: {run_id}, {dag_id}
INSERT INTO dq_results
-- 1. row-level rules: every catalogued rule, including the ones that passed
SELECT
    '{run_id}'                                             AS run_id,
    '{dag_id}'                                             AS dag_id,
    c.rule_id,
    c.dataset,
    c.check_name,
    c.dimension,
    c.severity,
    coalesce(rc.rows_checked, 0)                           AS rows_checked,
    coalesce(f.rows_failed, 0)                             AS rows_failed,
    CASE WHEN coalesce(f.rows_failed, 0) = 0 THEN 'PASS'
         WHEN c.severity = 'Critical'       THEN 'FAIL'
         ELSE 'WARN' END                                   AS status,
    ''                                                     AS detail,
    now()                                                  AS checked_at
FROM dq_rule_catalogue c
LEFT JOIN (
    SELECT dataset, rule_id, count(*) AS rows_failed
    FROM elt_rule_failures
    WHERE run_id = '{run_id}'
    GROUP BY dataset, rule_id
) f ON f.rule_id = c.rule_id AND f.dataset = c.dataset
LEFT JOIN (
    SELECT 'orders'    AS dataset, count(*) AS rows_checked FROM raw_orders    WHERE _run_id = '{run_id}'
    UNION ALL SELECT 'payments',   count(*) FROM raw_payments  WHERE _run_id = '{run_id}'
    UNION ALL SELECT 'customers',  count(*) FROM raw_customers WHERE _run_id = '{run_id}'
    UNION ALL SELECT 'products',   count(*) FROM raw_products  WHERE _run_id = '{run_id}'
    UNION ALL SELECT 'stores',     count(*) FROM raw_stores    WHERE _run_id = '{run_id}'
) rc ON rc.dataset = c.dataset
WHERE c.scope = 'row'

UNION ALL
-- 2. volume checks: a file that suddenly loses most of its rows is a red flag
SELECT '{run_id}', '{dag_id}', c.rule_id, c.dataset, c.check_name, c.dimension, c.severity,
       v.actual_rows,
       CASE WHEN v.expected_rows > 0
             AND (v.expected_rows - v.actual_rows) * 100.0 / v.expected_rows > 90 THEN 1
            ELSE 0 END,
       CASE WHEN v.expected_rows > 0
             AND (v.expected_rows - v.actual_rows) * 100.0 / v.expected_rows > 90 THEN 'WARN'
            ELSE 'PASS' END,
       'expected~' || v.expected_rows || ' rows, received ' || v.actual_rows || ' rows',
       now()
FROM dq_rule_catalogue c
JOIN (
    SELECT 'orders'   AS dataset, 2412 AS expected_rows, (SELECT count(*) FROM raw_orders    WHERE _run_id = '{run_id}') AS actual_rows
    UNION ALL SELECT 'payments',  2228, (SELECT count(*) FROM raw_payments  WHERE _run_id = '{run_id}')
    UNION ALL SELECT 'customers',  405, (SELECT count(*) FROM raw_customers WHERE _run_id = '{run_id}')
    UNION ALL SELECT 'products',   152, (SELECT count(*) FROM raw_products  WHERE _run_id = '{run_id}')
    UNION ALL SELECT 'stores',      15, (SELECT count(*) FROM raw_stores    WHERE _run_id = '{run_id}')
) v ON v.dataset = c.dataset
WHERE c.scope = 'dataset' AND c.dimension = 'Volume'

UNION ALL
-- 3. freshness: the 9 AM report must not be built on stale data
SELECT '{run_id}', '{dag_id}', c.rule_id, c.dataset, c.check_name, c.dimension, c.severity,
       (SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS'),
       CASE WHEN fresh.age_days > 1 THEN 1 ELSE 0 END,
       CASE WHEN fresh.age_days > 1 THEN 'WARN' ELSE 'PASS' END,
       'latest order_ts = ' || coalesce(strftime(fresh.latest_order_ts, '%Y-%m-%d %H:%M'), 'none')
           || ', ' || round(coalesce(fresh.age_days, -1), 1) || ' day(s) old',
       now()
FROM dq_rule_catalogue c
CROSS JOIN (
    SELECT max(order_ts) AS latest_order_ts,
           date_diff('second', max(order_ts), now()) / 86400.0 AS age_days
    FROM stg_orders WHERE dq_status = 'PASS'
) fresh
WHERE c.rule_id = 'RULE-035';
