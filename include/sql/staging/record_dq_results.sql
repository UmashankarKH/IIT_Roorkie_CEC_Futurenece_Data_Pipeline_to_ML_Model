-- Part D / Part F: roll the row-level reasons written by the staging models up into
-- one row per check per run, in the SAME dq_results table the ETL pipeline writes to.
-- One table, dag_id tells the two pipelines apart, so the quality scorecard covers
-- both and they can be compared check by check.
--
-- The driving table is dq_rule_catalog, not the failures, so a rule that found
-- nothing is still reported with status PASS. "No row for that check" and "that
-- check passed" are very different things when somebody is debugging a bad load.
--
-- Placeholders: {run_id} {dag_id}

CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
    severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR,
    checked_at VARCHAR, detail VARCHAR
);

INSERT INTO dq_results
WITH rows_per_dataset AS (
    SELECT 'orders'    AS dataset, count(*) AS rows_checked FROM stg_orders
    UNION ALL SELECT 'payments',   count(*) FROM stg_payments
    UNION ALL SELECT 'customers',  count(*) FROM stg_customers
    UNION ALL SELECT 'products',   count(*) FROM stg_products
    UNION ALL SELECT 'stores',     count(*) FROM stg_stores
),
exploded AS (
    SELECT 'orders' AS dataset, unnest(string_split(failure_reason, ' | ')) AS tagged
    FROM stg_orders WHERE failure_reason <> ''
    UNION ALL SELECT 'payments',  unnest(string_split(failure_reason, ' | '))
    FROM stg_payments WHERE failure_reason <> ''
    UNION ALL SELECT 'customers', unnest(string_split(failure_reason, ' | '))
    FROM stg_customers WHERE failure_reason <> ''
    UNION ALL SELECT 'products',  unnest(string_split(failure_reason, ' | '))
    FROM stg_products WHERE failure_reason <> ''
    UNION ALL SELECT 'stores',    unnest(string_split(failure_reason, ' | '))
    FROM stg_stores WHERE failure_reason <> ''
),
parsed AS (
    -- Every reason is written as "<Severity>: <reason text>".
    SELECT dataset,
           split_part(tagged, ': ', 1)                   AS severity,
           substr(tagged, position(': ' IN tagged) + 2)  AS reason
    FROM exploded
),
counted AS (
    SELECT dataset, severity, reason, count(*) AS rows_failed
    FROM parsed
    GROUP BY ALL
),
-- Rules that fired but are not in the catalogue. These should never appear; if one
-- does, a rule was added to a staging model and never registered.
uncatalogued AS (
    SELECT c.dataset, 'uncatalogued_rule' AS check_name, 'UNCATALOGUED' AS dimension,
           c.severity, c.reason, c.rows_failed
    FROM counted c
    LEFT JOIN dq_rule_catalog cat
           ON cat.dataset = c.dataset AND cat.reason = c.reason
    WHERE cat.reason IS NULL
),
all_results AS (
    SELECT cat.dataset, cat.check_name, cat.dimension, cat.severity, cat.reason,
           coalesce(c.rows_failed, 0) AS rows_failed
    FROM dq_rule_catalog cat
    LEFT JOIN counted c
           ON c.dataset = cat.dataset AND c.reason = cat.reason
    UNION ALL
    SELECT dataset, check_name, dimension, severity, reason, rows_failed
    FROM uncatalogued
)
SELECT
    '{run_id}'  AS run_id,
    '{dag_id}'  AS dag_id,
    r.check_name,
    r.dimension,
    r.severity,
    d.rows_checked,
    r.rows_failed,
    CASE WHEN r.rows_failed = 0 THEN 'PASS'
         WHEN r.severity = 'Critical' THEN 'FAIL'
         ELSE 'WARN' END                                   AS status,
    strftime(now(), '%Y-%m-%dT%H:%M:%S')                   AS checked_at,
    CASE WHEN r.rows_failed = 0 THEN '' ELSE r.reason END  AS detail
FROM all_results r
JOIN rows_per_dataset d ON d.dataset = r.dataset
ORDER BY r.check_name;

-- Batch-level checks. The rules above look at rows; these two look at the load as a
-- whole, which is what actually catches "the export was half empty this morning".
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time VARCHAR, end_time VARCHAR,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT,
    status VARCHAR, notes VARCHAR
);

INSERT INTO dq_results
WITH loaded AS (
    -- Passed rows only. Taking max(order_ts) over everything made freshness look
    -- great because the newest row in the table was one of the future-dated
    -- rejects from 2027.
    SELECT count(*) AS rows_passed, max(order_ts) AS latest_order
    FROM stg_orders WHERE dq_status = 'PASSED'
),
baseline AS (
    -- The previous successful run of this DAG. NULL on the first ever run, and the
    -- volume check then reports SKIP rather than a pass it has not earned.
    SELECT max_by(rows_loaded, end_time) AS rows_loaded
    FROM pipeline_runs
    WHERE dag_id = '{dag_id}' AND status = 'SUCCESS' AND rows_loaded > 0
)
SELECT '{run_id}', '{dag_id}', 'orders_volume_not_collapsed', 'Volume', 'Warning',
       l.rows_passed,
       CASE WHEN b.rows_loaded IS NULL THEN 0
            WHEN 1 - (l.rows_passed * 1.0 / b.rows_loaded) > 0.9 THEN 1 ELSE 0 END,
       CASE WHEN b.rows_loaded IS NULL THEN 'SKIP'
            WHEN 1 - (l.rows_passed * 1.0 / b.rows_loaded) > 0.9 THEN 'WARN'
            ELSE 'PASS' END,
       strftime(now(), '%Y-%m-%dT%H:%M:%S'),
       coalesce(l.rows_passed || ' rows vs ' || b.rows_loaded || ' last run',
                'no previous run to compare against')
FROM loaded l, baseline b
UNION ALL
SELECT '{run_id}', '{dag_id}', 'orders_freshness', 'Timeliness', 'Warning',
       l.rows_passed,
       CASE WHEN date_diff('day', CAST(l.latest_order AS DATE), DATE '2026-10-02') > 1
            THEN 1 ELSE 0 END,
       CASE WHEN date_diff('day', CAST(l.latest_order AS DATE), DATE '2026-10-02') > 1
            THEN 'WARN' ELSE 'PASS' END,
       strftime(now(), '%Y-%m-%dT%H:%M:%S'),
       'latest order is '
       || date_diff('day', CAST(l.latest_order AS DATE), DATE '2026-10-02')
       || ' day(s) before the end of the data window'
FROM loaded l;
