# Query results from `include/warehouse_dinesh.duckdb`

DuckDB has no web UI, so these are terminal output rather than screenshots. They come
from the warehouse after the two DAGs ran in Airflow, and anyone can reproduce them:

```bash
cd include && python -m nammamart.business_questions
```

The Airflow screenshots that *are* images are listed in SUBMISSION.md.

## `pipeline_runs` - the run audit (Part F)

The FAILED rows are experiment 4, where products.csv was renamed. Note they still have a row: the audit task runs with trigger_rule='all_done'.

```sql
SELECT run_id, dag_id, rows_extracted, rows_loaded, rows_quarantined, status
FROM pipeline_runs ORDER BY end_time DESC LIMIT 6
```

```
                                  run_id               dag_id  rows_extracted  rows_loaded  rows_quarantined  status
manual__2026-10-26T10:59:27.098962+00:00 nammamart_etl_dinesh            5212         2460               127 SUCCESS
manual__2026-10-26T10:53:18.111978+00:00 nammamart_etl_dinesh            5060            0               106  FAILED
manual__2026-10-26T10:46:59.806147+00:00 nammamart_elt_dinesh            5372         2460               137 SUCCESS
manual__2026-10-26T10:43:07.777401+00:00 nammamart_etl_dinesh            5212         2460               127 SUCCESS
    scheduled__2026-10-26T03:30:00+00:00 nammamart_etl_dinesh            5212         2460               127 SUCCESS
    scheduled__2026-10-26T03:30:00+00:00 nammamart_elt_dinesh            5372         2460               137 SUCCESS
```

## `pipeline_runs` - why the failed runs failed

```sql
SELECT run_id, status, notes FROM pipeline_runs
WHERE status = 'FAILED' ORDER BY end_time DESC LIMIT 2
```

```
                                  run_id status                                                                                                                                                   notes
manual__2026-10-26T10:53:18.111978+00:00 FAILED no result from: ['validate_products', 'transform', 'record_validation_dq', 'load_star', 'record_transform_dq', 'load_increment', 'reconcile', 'report']
```

## `dq_results` - the quality scorecard, by dimension

```sql
SELECT CASE WHEN dag_id LIKE '%etl%' THEN 'etl' ELSE 'elt' END AS pipeline,
       dimension, count(*) AS checks, sum(rows_failed) AS rows_failed,
       count_if(status = 'FAIL') AS fails, count_if(status = 'WARN') AS warns
FROM dq_results
WHERE run_id = (SELECT max(run_id) FROM dq_results WHERE dag_id LIKE '%elt%')
GROUP BY ALL ORDER BY dimension, pipeline
```

```
pipeline    dimension  checks  rows_failed  fails  warns
     elt     Accuracy       8         47.0    6.0    1.0
     etl     Accuracy       4          0.0    0.0    0.0
     elt Completeness      12        337.0    2.0    4.0
     etl Completeness       3          0.0    0.0    0.0
     elt  Consistency       6         38.0    2.0    3.0
     etl  Consistency       4          0.0    0.0    0.0
     elt   Timeliness       3         10.0    2.0    0.0
     etl   Timeliness       1          0.0    0.0    0.0
     elt   Uniqueness       5         33.0    4.0    0.0
     etl   Uniqueness       1          0.0    0.0    0.0
     elt     Validity       8         34.0    3.0    3.0
     etl     Validity       4          0.0    0.0    0.0
     elt       Volume       1          0.0    0.0    0.0
```

## `dq_results` - every check that failed on the latest ETL run

```sql
SELECT check_name, dimension, severity, rows_checked, rows_failed, status, detail
FROM dq_results
WHERE dag_id LIKE '%etl%' AND rows_failed > 0
  AND run_id = (SELECT max(run_id) FROM dq_results WHERE dag_id LIKE '%etl%')
ORDER BY rows_failed DESC
```

```
Empty DataFrame
Columns: [check_name, dimension, severity, rows_checked, rows_failed, status, detail]
Index: []
```

## `mart_payment_reconciliation` - the biggest variances

```sql
SELECT order_id, order_status, net_revenue, amount_paid, variance, mismatch_reason
FROM mart_payment_reconciliation ORDER BY abs(variance) DESC LIMIT 12
```

```
 order_id order_status  net_revenue  amount_paid  variance                            mismatch_reason
ORD001624         None          NaN       9576.0    9576.0       payment with no matching clean order
ORD000964         None          NaN       8370.0    8370.0       payment with no matching clean order
ORD002305         None          NaN       6757.5    6757.5       payment with no matching clean order
ORD002004         None          NaN       6720.0    6720.0       payment with no matching clean order
ORD000801         None          NaN       5467.5    5467.5       payment with no matching clean order
ORD000845         None          NaN       4712.0    4712.0       payment with no matching clean order
ORD002031         None          NaN       3210.0    3210.0       payment with no matching clean order
ORD001237         None          NaN       2408.0    2408.0       payment with no matching clean order
ORD000106    DELIVERED       2332.8          NaN   -2332.8 delivered order with no successful payment
ORD000583         None          NaN       2091.0    2091.0       payment with no matching clean order
ORD002525    DELIVERED       2052.0          NaN   -2052.0 delivered order with no successful payment
ORD002407    DELIVERED       1887.0          NaN   -1887.0 delivered order with no successful payment
```

## `mart_payment_reconciliation` - by reason

The 150 'delivered order with no successful payment' rows are the increment's new orders - their payments arrive in the next payments export.

```sql
SELECT mismatch_reason, count(*) AS orders, round(sum(abs(variance)), 2) AS total_variance
FROM mart_payment_reconciliation GROUP BY 1 ORDER BY orders DESC
```

```
                           mismatch_reason  orders  total_variance
delivered order with no successful payment     220       122095.75
      payment with no matching clean order      88        84203.45
      paid -25.00 against the order amount       5          125.00
     paid -100.00 against the order amount       4          400.00
      paid -50.00 against the order amount       3          150.00
      paid -10.00 against the order amount       1           10.00
```

## Quarantine - what was rejected and why

```sql
SELECT source_table, failure_reason, count(*) AS rows_rejected
FROM elt_quarantine
WHERE run_id = (SELECT max(run_id) FROM elt_quarantine)
GROUP BY 1, 2 ORDER BY rows_rejected DESC LIMIT 12
```

```
 source_table                                                               failure_reason  rows_rejected
   stg_orders                                 Critical: duplicate order_id, later row kept             22
   stg_orders                                             Critical: customer_id is missing             18
   stg_orders Critical: order_amount does not match quantity x unit_price x (1 - discount)             15
   stg_orders                                 Critical: quantity must be greater than zero             10
   stg_orders                              Critical: product_id does not exist in products              9
   stg_orders                            Critical: DELIVERED order has no delivery_minutes              8
   stg_orders                                          Critical: order_ts is in the future              7
 stg_payments                              Critical: amount_paid is negative or unreadable              6
   stg_orders                                         Critical: delivery_minutes above 120              6
   stg_orders                                            Critical: order_ts does not parse              6
stg_customers                              Critical: duplicate customer_id, later row kept              5
   stg_orders                                  Critical: store_id does not exist in stores              5
```

## ETL vs ELT - do the two pipelines agree?

```sql
SELECT 'etl' AS pipeline, count(*) AS fact_rows,
       round(sum(revenue_amount), 2) AS net_revenue,
       round(sum(margin_amount), 2) AS gross_margin
FROM etl_fact_orders
UNION ALL
SELECT 'elt', count(*), round(sum(revenue_amount), 2), round(sum(margin_amount), 2)
FROM mart_fact_orders
```

```
pipeline  fact_rows  net_revenue  gross_margin
     etl       2460    1211087.3     284875.31
     elt       2460    1211087.3     284875.31
```
