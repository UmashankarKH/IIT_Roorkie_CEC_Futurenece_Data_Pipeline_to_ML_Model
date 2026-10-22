# Six business questions - answered from both pipelines

Generated: 2026-10-22 18:55 by `scripts/run_business_questions.py` against `warehouse_waseem.duckdb`.

Each question is answered twice: once from the **ETL** star schema (`etl_fact_orders` and its dimensions) and once from the **ELT** mart (`mart_*` tables built with SQL only).

## Question 1: Daily net revenue by zone and by store (delivered orders only, after discount)

### ETL

```sql
-- Business question 1 (ETL): daily net revenue by zone and by store.
-- Delivered orders only, after discount. Source: etl_fact_orders (ETL star schema).
SELECT order_date,
       zone,
       store_id,
       store_name,
       count(*)                    AS delivered_orders,
       round(sum(net_revenue), 2)  AS net_revenue
FROM etl_fact_orders
WHERE is_revenue
GROUP BY order_date, zone, store_id, store_name
ORDER BY order_date DESC, net_revenue DESC
LIMIT 10;
```

| order_date | zone | store_id | store_name | delivered_orders | net_revenue |
| --- | --- | --- | --- | --- | --- |
| 2026-10-01 00:00:00 | North | S01 | NammaMart Hebbal | 13 | 8958.35 |
| 2026-10-01 00:00:00 | South | S05 | NammaMart JP Nagar | 13 | 8785.2 |
| 2026-10-01 00:00:00 | East | S11 | NammaMart HSR Layout | 18 | 8571.1 |
| 2026-10-01 00:00:00 | East | S08 | NammaMart Whitefield | 15 | 6569.9 |
| 2026-10-01 00:00:00 | West | S14 | NammaMart Vijayanagar | 15 | 6214.8 |
| 2026-10-01 00:00:00 | East | S10 | NammaMart Marathahalli | 13 | 6020.9 |
| 2026-10-01 00:00:00 | West | S12 | NammaMart Rajajinagar | 6 | 5554.25 |
| 2026-10-01 00:00:00 | South | S06 | NammaMart Banashankari | 6 | 4893.55 |
| 2026-10-01 00:00:00 | North | S02 | NammaMart Yelahanka | 11 | 4726.35 |
| 2026-10-01 00:00:00 | West | S13 | NammaMart Malleshwaram | 9 | 4498.65 |

### ELT

```sql
-- Business question 1 (ELT): daily net revenue by zone and by store.
-- Source: mart_daily_store_kpis, which is built in SQL from stg_orders (PASS rows only).
SELECT order_date,
       zone,
       store_id,
       store_name,
       delivered_orders,
       net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date DESC, net_revenue DESC
LIMIT 10;
```

| order_date | zone | store_id | store_name | delivered_orders | net_revenue |
| --- | --- | --- | --- | --- | --- |
| 2026-10-01 00:00:00 | North | S01 | NammaMart Hebbal | 13 | 8958.35 |
| 2026-10-01 00:00:00 | South | S05 | NammaMart JP Nagar | 13 | 8785.2 |
| 2026-10-01 00:00:00 | East | S11 | NammaMart HSR Layout | 18 | 8571.1 |
| 2026-10-01 00:00:00 | East | S08 | NammaMart Whitefield | 15 | 6569.9 |
| 2026-10-01 00:00:00 | West | S14 | NammaMart Vijayanagar | 15 | 6214.8 |
| 2026-10-01 00:00:00 | East | S10 | NammaMart Marathahalli | 13 | 6020.9 |
| 2026-10-01 00:00:00 | West | S12 | NammaMart Rajajinagar | 6 | 5554.25 |
| 2026-10-01 00:00:00 | South | S06 | NammaMart Banashankari | 6 | 4893.55 |
| 2026-10-01 00:00:00 | North | S02 | NammaMart Yelahanka | 11 | 4726.35 |
| 2026-10-01 00:00:00 | West | S13 | NammaMart Malleshwaram | 9 | 4498.65 |

## Question 2: Revenue and gross margin by product category (margin = revenue - quantity x cost_price)

### ETL

```sql
-- Business question 2 (ETL): revenue and gross margin by product category.
-- margin = revenue - quantity x cost_price, computed row by row during the transform.
SELECT category,
       delivered_orders,
       net_revenue,
       cost_of_goods,
       gross_margin,
       margin_pct
FROM etl_category_margin
ORDER BY net_revenue DESC;
```

| category | delivered_orders | net_revenue | cost_of_goods | gross_margin | margin_pct |
| --- | --- | --- | --- | --- | --- |
| Staples | 336 | 287601.45 | 225423.36 | 62178.09 | 21.62 |
| Personal Care | 337 | 212148.05 | 165497.72 | 46650.33 | 21.99 |
| Household | 363 | 212105.05 | 157638.9 | 54466.15 | 25.68 |
| Dairy & Bakery | 405 | 179568.95 | 131935.95 | 47633.0 | 26.53 |
| Snacks & Beverages | 341 | 174988.6 | 133558.93 | 41429.67 | 23.68 |
| Fruits & Vegetables | 392 | 99566.2 | 79054.82 | 20511.38 | 20.6 |
| Unknown | 54 | 45109.0 | 36968.36 | 8140.64 | 18.05 |

### ELT

```sql
-- Business question 2 (ELT): revenue and gross margin by product category.
SELECT category,
       delivered_orders,
       net_revenue,
       cost_of_goods,
       gross_margin,
       margin_pct
FROM mart_category_margin
ORDER BY net_revenue DESC;
```

| category | delivered_orders | net_revenue | cost_of_goods | gross_margin | margin_pct |
| --- | --- | --- | --- | --- | --- |
| Staples | 336 | 287601.45 | 225423.36 | 62178.09 | 21.62 |
| Personal Care | 337 | 212148.05 | 165497.72 | 46650.33 | 21.99 |
| Household | 363 | 212105.05 | 157638.9 | 54466.15 | 25.68 |
| Dairy & Bakery | 405 | 179568.95 | 131935.95 | 47633.0 | 26.53 |
| Snacks & Beverages | 341 | 174988.6 | 133558.93 | 41429.67 | 23.68 |
| Fruits & Vegetables | 392 | 99566.2 | 79054.82 | 20511.38 | 20.6 |
| Unknown | 54 | 45109.0 | 36968.36 | 8140.64 | 18.05 |

## Question 3: Average delivery time per store, and the % of delivered orders within 15 minutes

### ETL

```sql
-- Business question 3 (ETL): average delivery time per store and the share of
-- delivered orders inside the 15-minute promise.
SELECT store_id,
       store_name,
       zone,
       count(*)                                                     AS delivered_orders,
       round(avg(delivery_minutes), 2)                              AS avg_delivery_minutes,
       sum(CASE WHEN within_15_min THEN 1 ELSE 0 END)               AS orders_within_15_min,
       round(100.0 * sum(CASE WHEN within_15_min THEN 1 ELSE 0 END) / count(*), 2)
                                                                    AS pct_within_15_min
FROM etl_fact_orders
WHERE is_revenue AND delivery_minutes IS NOT NULL
GROUP BY store_id, store_name, zone
ORDER BY pct_within_15_min DESC;
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | orders_within_15_min | pct_within_15_min |
| --- | --- | --- | --- | --- | --- | --- |
| S08 | NammaMart Whitefield | East | 164 | 20.61 | 58.0 | 35.37 |
| S07 | NammaMart BTM Layout | Unknown | 150 | 21.13 | 49.0 | 32.67 |
| S01 | NammaMart Hebbal | North | 149 | 20.47 | 48.0 | 32.21 |
| S02 | NammaMart Yelahanka | North | 150 | 20.93 | 47.0 | 31.33 |
| S14 | NammaMart Vijayanagar | West | 166 | 21.51 | 52.0 | 31.33 |
| S05 | NammaMart JP Nagar | South | 142 | 21.19 | 42.0 | 29.58 |
| S03 | NammaMart RT Nagar | North | 143 | 20.65 | 42.0 | 29.37 |
| S11 | NammaMart HSR Layout | East | 161 | 20.82 | 46.0 | 28.57 |
| S04 | NammaMart Jayanagar | South | 138 | 21.31 | 38.0 | 27.54 |
| S13 | NammaMart Malleshwaram | West | 150 | 21.87 | 41.0 | 27.33 |

_15 rows in total, first 10 shown._

### ELT

```sql
-- Business question 3 (ELT): average delivery time per store and 15-minute SLA share.
SELECT store_id, store_name, zone, delivered_orders, avg_delivery_minutes,
       orders_within_15_min, pct_within_15_min
FROM mart_delivery_sla
ORDER BY pct_within_15_min DESC;
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | orders_within_15_min | pct_within_15_min |
| --- | --- | --- | --- | --- | --- | --- |
| S08 | NammaMart Whitefield | East | 164 | 20.61 | 58.0 | 35.37 |
| S07 | NammaMart BTM Layout | Unknown | 150 | 21.13 | 49.0 | 32.67 |
| S01 | NammaMart Hebbal | North | 149 | 20.47 | 48.0 | 32.21 |
| S02 | NammaMart Yelahanka | North | 150 | 20.93 | 47.0 | 31.33 |
| S14 | NammaMart Vijayanagar | West | 166 | 21.51 | 52.0 | 31.33 |
| S05 | NammaMart JP Nagar | South | 142 | 21.19 | 42.0 | 29.58 |
| S03 | NammaMart RT Nagar | North | 143 | 20.65 | 42.0 | 29.37 |
| S11 | NammaMart HSR Layout | East | 161 | 20.82 | 46.0 | 28.57 |
| S04 | NammaMart Jayanagar | South | 138 | 21.31 | 38.0 | 27.54 |
| S13 | NammaMart Malleshwaram | West | 150 | 21.87 | 41.0 | 27.33 |

_15 rows in total, first 10 shown._

## Question 4: Revenue by payment mode, and the total amount that doesn't reconcile with the gateway

### ETL

```sql
-- Business question 4 (ETL): revenue by payment mode, plus the money that does
-- not reconcile with the payment gateway.
WITH revenue_by_mode AS (
    SELECT payment_mode,
           count(*)                   AS delivered_orders,
           round(sum(net_revenue), 2) AS net_revenue
    FROM etl_fact_orders
    WHERE is_revenue
    GROUP BY payment_mode
),
unreconciled AS (
    SELECT round(sum(abs(coalesce(difference, 0))), 2) AS unreconciled_amount,
           count(*)                                    AS mismatch_rows
    FROM etl_payment_reconciliation
)
SELECT r.payment_mode, r.delivered_orders, r.net_revenue,
       u.mismatch_rows, u.unreconciled_amount
FROM revenue_by_mode r CROSS JOIN unreconciled u
ORDER BY r.net_revenue DESC;
```

| payment_mode | delivered_orders | net_revenue | mismatch_rows | unreconciled_amount |
| --- | --- | --- | --- | --- |
| UPI | 1205 | 638752.15 | 340 | 210983.25 |
| Card | 457 | 251882.3 | 340 | 210983.25 |
| Cash on Delivery | 318 | 181668.4 | 340 | 210983.25 |
| Wallet | 246 | 138118.45 | 340 | 210983.25 |
| Cashh | 2 | 666.0 | 340 | 210983.25 |

### ELT

```sql
-- Business question 4 (ELT): revenue by payment mode, plus the unreconciled total.
WITH revenue_by_mode AS (
    SELECT payment_mode,
           count(*)                   AS delivered_orders,
           round(sum(net_revenue), 2) AS net_revenue
    FROM stg_orders
    WHERE dq_status = 'PASS' AND is_revenue
    GROUP BY payment_mode
),
unreconciled AS (
    SELECT round(sum(abs(coalesce(difference, 0))), 2) AS unreconciled_amount,
           count(*)                                    AS mismatch_rows
    FROM mart_payment_reconciliation
)
SELECT r.payment_mode, r.delivered_orders, r.net_revenue,
       u.mismatch_rows, u.unreconciled_amount
FROM revenue_by_mode r CROSS JOIN unreconciled u
ORDER BY r.net_revenue DESC;
```

| payment_mode | delivered_orders | net_revenue | mismatch_rows | unreconciled_amount |
| --- | --- | --- | --- | --- |
| UPI | 1205 | 638752.15 | 2355 | 211768.25 |
| Card | 457 | 251882.3 | 2355 | 211768.25 |
| Cash on Delivery | 318 | 181668.4 | 2355 | 211768.25 |
| Wallet | 246 | 138118.45 | 2355 | 211768.25 |
| Cashh | 2 | 666.0 | 2355 | 211768.25 |

## Question 5: Top 10 customers by spend, with loyalty tier (no raw email or phone shown)

### ETL

```sql
-- Business question 5 (ETL): top 10 customers by spend, with loyalty tier.
-- Personal data is already masked in etl_dim_customer, so this query cannot
-- expose a real email address or phone number.
SELECT f.customer_id,
       c.customer_name_masked,
       c.email_masked,
       c.phone_masked,
       f.loyalty_tier,
       count(*)                   AS delivered_orders,
       round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f
LEFT JOIN etl_dim_customer c ON c.customer_id = f.customer_id
WHERE f.is_revenue
GROUP BY f.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, f.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
```

| customer_id | customer_name_masked | email_masked | phone_masked | loyalty_tier | delivered_orders | total_spend |
| --- | --- | --- | --- | --- | --- | --- |
| C00136 | Sanjay B. | sa***@example.com | 96******88 | BRONZE | 9 | 8738.1 |
| C00115 | Sanjay K. | sa***@example.com | 96******85 | BRONZE | 8 | 8037.7 |
| C00283 | Nikhil R. | ni***@example.com | 91******43 | BRONZE | 8 | 7934.6 |
| C00236 | Sneha N. | sn***@example.com | 90******01 | PLATINUM | 12 | 7831.8 |
| C00310 | Kiran P. | ki***@example | 91******35 | BRONZE | 8 | 7258.95 |
| C00265 | Vikram H. | vi***@example.com | 91******69 | GOLD | 8 | 7151.3 |
| C00071 | Mahesh P. | ma***@example.com | 91******42 | SILVER | 11 | 6933.3 |
| C00338 | Bhavana I. | bh***@example.com | 94******05 | SILVER | 9 | 6848.55 |
| C00357 | Arjun N. | ar***@example.com | 93*****12 | BRONZE | 11 | 6637.9 |
| C00209 | Kavya P. | ka***@example.com | 98******97 | PLATINUM | 8 | 6519.4 |

### ELT

```sql
-- Business question 5 (ELT): top 10 customers by spend, with loyalty tier.
SELECT customer_id, customer_name_masked, email_masked, phone_masked,
       loyalty_tier, delivered_orders, total_spend
FROM mart_top_customers
WHERE spend_rank <= 10
ORDER BY spend_rank;
```

| customer_id | customer_name_masked | email_masked | phone_masked | loyalty_tier | delivered_orders | total_spend |
| --- | --- | --- | --- | --- | --- | --- |
| C00136 | Sanjay B. | sa***@example.com | 96******88 | BRONZE | 9 | 8738.1 |
| C00115 | Sanjay K. | sa***@example.com | 96******85 | BRONZE | 8 | 8037.7 |
| C00283 | Nikhil R. | ni***@example.com | 91******43 | BRONZE | 8 | 7934.6 |
| C00236 | Sneha N. | sn***@example.com | 90******01 | PLATINUM | 12 | 7831.8 |
| C00310 | Kiran P. | ki***@example | 91******35 | BRONZE | 8 | 7258.95 |
| C00265 | Vikram H. | vi***@example.com | 91******69 | GOLD | 8 | 7151.3 |
| C00071 | Mahesh P. | ma***@example.com | 91******42 | SILVER | 11 | 6933.3 |
| C00338 | Bhavana I. | bh***@example.com | 94******05 | SILVER | 9 | 6848.55 |
| C00357 | Arjun N. | ar***@example.com | 93*****12 | BRONZE | 11 | 6637.9 |
| C00209 | Kavya P. | ka***@example.com | 98******97 | PLATINUM | 8 | 6519.4 |

## Question 6: Number of rows rejected today, by file and by reason

### ETL

```sql
-- Business question 6 (ETL): how many rows were rejected in the latest run,
-- by file and by reason. Source: the quarantine table written by the ETL DAG.
SELECT source_file,
       regexp_extract(failure_reason, 'RULE-[0-9]+') AS rule_id,
       count(*)                                      AS rows_rejected
FROM etl_quarantine
WHERE run_id = (SELECT run_id FROM pipeline_runs
                WHERE dag_id = 'nammamart_etl_waseem' ORDER BY start_time DESC LIMIT 1)
GROUP BY source_file, rule_id
ORDER BY rows_rejected DESC, source_file;
```

| source_file | rule_id | rows_rejected |
| --- | --- | --- |
| orders.csv | RULE-006 | 18 |
| orders.csv | RULE-011 | 15 |
| payments.csv | RULE-018 | 13 |
| orders.csv | RULE-002 | 12 |
| orders.csv | RULE-009 | 10 |
| payments.csv | RULE-038 | 10 |
| orders.csv | RULE-007 | 9 |
| orders.csv | RULE-014 | 8 |
| payments.csv | RULE-017 | 8 |
| orders.csv | RULE-005 | 7 |

_21 rows in total, first 10 shown._

### ELT

```sql
-- Business question 6 (ELT): how many rows were rejected in the latest run,
-- by file and by reason. Source: the per-row failure list built in SQL.
SELECT f.dataset,
       f.rule_id,
       c.check_name,
       c.severity,
       count(*) AS rows_rejected
FROM elt_rule_failures f
LEFT JOIN dq_rule_catalogue c ON c.rule_id = f.rule_id AND c.dataset = f.dataset
WHERE f.run_id = (SELECT run_id FROM pipeline_runs
                  WHERE dag_id = 'nammamart_elt_waseem' ORDER BY start_time DESC LIMIT 1)
  AND c.severity = 'Critical'
GROUP BY f.dataset, f.rule_id, c.check_name, c.severity
ORDER BY rows_rejected DESC, f.dataset;
```

| dataset | rule_id | check_name | severity | rows_rejected |
| --- | --- | --- | --- | --- |
| orders | RULE-006 | customer_id must be present | Critical | 18 |
| orders | RULE-011 | order_amount must match quantity x unit_price x (1 - discount_pct/100) | Critical | 18 |
| payments | RULE-018 | SUCCESS payment amount must equal the order amount | Critical | 14 |
| orders | RULE-002 | order_id must be unique (redundant copies quarantined) | Critical | 12 |
| orders | RULE-009 | quantity must be greater than zero | Critical | 10 |
| payments | RULE-038 | SUCCESS payments must have amount_paid greater than zero | Critical | 10 |
| orders | RULE-007 | product_id must exist in products.csv | Critical | 9 |
| orders | RULE-014 | DELIVERED orders must have delivery_minutes | Critical | 8 |
| payments | RULE-017 | order_id must exist in orders.csv | Critical | 8 |
| orders | RULE-005 | order_ts must not be future-dated | Critical | 7 |

_21 rows in total, first 10 shown._
