# SUBMISSION

## Part 0: Data Issues Log

| # | File | Row key | Column | Problem | Dimension | Proposed rule | Severity | Count |
|---|------|---------|--------|---------|-----------|---------------|----------|-------|
| 1 | orders | order_id | customer_id | customer_id is null | Completeness | customer_id not null | Critical | 18 |
| 2 | orders | order_id | delivery_minutes | DELIVERED order has no delivery_minutes | Completeness | DELIVERED requires delivery_minutes | Critical | 8 |
| 3 | orders | order_id | order_id | Duplicate order_id | Uniqueness | order_id unique, keep first | Critical | see CSV |
| 4 | orders | order_id | order_status | Status not in DELIVERED/CANCELLED/RETURNED | Validity | status in allowed list | Critical | 5 |
| 5 | orders | order_id | payment_mode | Casing/variants (e.g. upi) | Validity | map to UPI/Card/Cash on Delivery/Wallet | Warning | 10 |
| 6 | orders | order_id | discount_pct | Outside 0-100 | Validity | discount between 0 and 100 | Critical | 3 |
| 7 | orders | order_id | quantity | Quantity <= 0 | Accuracy | quantity > 0 | Critical | 10 |
| 8 | orders | order_id | order_amount | Does not equal qty x price x (1 - disc) | Accuracy | recompute and compare | Critical | 18 |
| 9 | orders | order_id | delivery_minutes | More than 120 minutes | Accuracy | delivery_minutes < 120 | Critical | 6 |
| 10 | orders | order_id | store_id | Store not in stores master | Consistency | store_id exists | Critical | 5 |
| 11 | orders | order_id | product_id | Product not in products master | Consistency | product_id exists | Critical | 9 |
| 12 | orders | order_id | order_ts | Future-dated or unparseable timestamp | Timeliness | order_ts not in future | Critical | see CSV |
| 13 | payments | payment_id | payment_id | Duplicate payment_id | Uniqueness | payment_id unique | Critical | see CSV |
| 14 | payments | payment_id | order_id | Payment has no matching order | Consistency | order_id exists in orders | Critical | 8 |
| 15 | payments | payment_id | amount_paid | SUCCESS amount differs from order_amount | Consistency | goes to reconciliation table | Warning | 14 |
| 16 | payments | order_id | (missing row) | Non-cancelled orders with no payment | Completeness | goes to reconciliation table | Warning | 28 |
| 17 | customers | customer_id | email | Invalid email format | Validity | email regex | Warning | 12 |
| 18 | customers | customer_id | phone | Phone not 10 digits | Validity | phone is 10 digits | Warning | 7 |
| 19 | customers | customer_id | pincode | Pincode not 6 digits | Validity | pincode is 6 digits | Warning | 3 |
| 20 | customers | customer_id | loyalty_tier | Mixed casing (BRONZE, bronze, SILVER) | Consistency | standardise to Title case | Warning | 8 |
| 21 | customers | customer_id | email | Same email on different customer ids | Uniqueness | deduplicate customers | Warning | 14 |
| 22 | customers | customer_id | signup_date | Future-dated signup | Timeliness | signup_date not in future | Critical | 3 |
| 23 | products | product_id | mrp | mrp <= 0 | Accuracy | mrp > 0 | Critical | 1 |
| 24 | products | product_id | cost_price | cost_price greater than mrp | Accuracy | cost_price <= mrp | Critical | 5 |
| 25 | products | product_id | category | Category is blank | Completeness | category not null | Critical | 3 |
| 26 | products | product_id | product_id | Duplicate product_id | Uniqueness | product_id unique | Critical | 4 |
| 27 | stores | store_id | zone | Blank zone | Completeness | zone in North/South/East/West | Critical | 1 |
| 28 | increment | order_id | (all) | Clean; 10 corrections + 150 new orders | n/a | merge on order_id | n/a | 0 |
| 29 | orders | order_id | order_ts | 2 orders dated October inside the September export | Timeliness | order_ts within the export window (1 to 30 Sep) | Warning | 2 |

## 3 questions for the client
1. Is a negative or zero quantity a return or a data-entry error, and should it ever reduce revenue?
2. When a SUCCESS payment amount differs from the order amount, which system is the source of truth, and who resolves it?
3. When an order is RETURNED later, should revenue be removed from the original delivery date or booked as a negative on the return date?
4. Two orders in the September export are dated in October (latest 21 Oct). Are they late-arriving orders, or a wrong export range?

## ETL vs ELT: do the totals match?

Checked on the full dataset (September files plus the 1 October increment) with `run_compare.py`. All 8 checks MATCH:
net revenue 1,210,981.90 | gross margin 279,862.12 | clean orders 2,459 | delivered orders 2,227 | orders within 15 min 635 | reconciliation rows 330 | amount not reconciling 212,443.75 | zones with different revenue 0.

The only difference is rows set aside: ETL quarantined 152, ELT rejected 136 in staging. The difference of 16 is exactly 12 duplicate orders + 4 duplicate payments. The ETL quarantines the extra copies; ELT keeps them in `raw_orders` / `raw_payments` only and does not repeat them in staging. Every other rejected row is identical in both pipelines.

## Design decisions

- Foreign-key checks (customer, product, store) compare against ALL ids in the master files, not only the ids that passed. Otherwise one bad store row (S07, blank zone) would have wrongly rejected every order from that store (516 orders were rejected before this fix).
- A store with capacity 0 (S12) is a Warning, not Critical: its zone is valid and its revenue must stay visible.
- A SUCCESS payment whose amount differs from the order is a Warning: it is not quarantined, it is listed in the reconciliation table instead.
- Refunds on RETURNED orders are expected and are not reported as reconciliation mismatches.
- Orders from a quarantined store/product still count in revenue under zone/category UNKNOWN; the cost price is unknown for those products, so their margin is blank (not 0).
- Duplicate ids: the first copy is kept; later copies are quarantined (ETL) or left in raw only (ELT).
- ELT Q4a (revenue by payment mode) reads `stg_orders`, because none of the five mart tables in the brief holds payment mode. A sixth mart table would fix this.

## Part C: Scheduling answers

**1. What is your cron expression?** `0 9 * * 1-6`: minute 0, hour 9, every day of the month, every month, day-of-week Monday (1) to Saturday (6). Sunday is skipped.

**2. What time does the run show in UTC?** IST is UTC+5:30, so 09:00 IST = **03:30 UTC**. The Airflow UI shows times in the browser's timezone (IST here), while the task logs and the metadata database use UTC. Example from this project: a run shown as 12:34 in the UI appears as 07:04 in the logs.

**3. What if the start_date had been left in UTC?** Airflow would read the cron in UTC, so the run would start at 09:00 UTC = **14:30 IST**, five and a half hours late and after the 9:30 stand-up. That repeats the original problem (late numbers). Declaring `pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")` makes the cron fire at 9:00 IST.

**4. What if the laptop or Docker is off at 9 AM?** The scheduler is not running, so no run is created and the numbers are not ready. When Docker starts again, `catchup=False` makes Airflow create only ONE run, for the most recent missed slot (we saw exactly this when we switched the DAGs on). It does not replay every missed day. The data would still be late for that morning, so production needs an always-on server (not a laptop) plus an alert when the 9:00 run does not start or fails.

**Why catchup=False?** Each run rebuilds the reports from the full source files and loads them idempotently, so replaying 25 missed days would reproduce the same output 25 times and write 25 meaningless audit rows. The business only needs today's correct numbers.

**Why retries=2, retry_delay=2 min, execution_timeout=10 min?** Most failures here are transient (a file still being written, DuckDB's single-writer lock). Two retries add at most 4 minutes, which still fits before 9:30. The 10-minute timeout stops a hung task from blocking the morning report; a normal run takes about 5 seconds.

**Note on the SLA:** the runs take about 5 seconds, so data is ready at about 9:01. That meets the 9:30 stand-up. Acceptance criterion 1 says "before 9:00 AM", so for a real client I would schedule the DAGs at 8:30 to leave a safety margin for retries.

## Business questions: answers from both pipelines

## Q1a. Net revenue by zone (delivered orders, after discount)

**Same result in ETL and ELT:** SAME

**ETL SQL**
```sql
SELECT zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue GROUP BY zone ORDER BY zone
```
```text
   zone  net_revenue
   East    313678.20
  North    244008.40
  South    256214.55
UNKNOWN     84955.75
   West    312125.00
```

**ELT SQL**
```sql
SELECT zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY zone ORDER BY zone
```
```text
   zone  net_revenue
   East    313678.20
  North    244008.40
  South    256214.55
UNKNOWN     84955.75
   West    312125.00
```

## Q1b. Net revenue by store

**Same result in ETL and ELT:** SAME

**ETL SQL**
```sql
SELECT store_id, store_name, zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue GROUP
BY 1, 2, 3 ORDER BY net_revenue DESC, store_id
```
```text
store_id                  store_name    zone  net_revenue
     S06      NammaMart Banashankari   South     94256.85
     S13      NammaMart Malleshwaram    West     89650.30
     S05          NammaMart JP Nagar   South     86163.65
     S01            NammaMart Hebbal   North     85510.70
     S10      NammaMart Marathahalli    East     84988.90
     S07                     UNKNOWN UNKNOWN     84955.75
     S11        NammaMart HSR Layout    East     83438.50
     S15 NammaMart Basaveshwaranagar    West     83052.90
     S03          NammaMart RT Nagar   North     81404.65
     S08        NammaMart Whitefield    East     78980.50
     S14       NammaMart Vijayanagar    West     77166.75
     S02         NammaMart Yelahanka   North     77093.05
     S04         NammaMart Jayanagar   South     75794.05
     S09       NammaMart Indiranagar    East     66270.30
     S12       NammaMart Rajajinagar    West     62255.05
```

**ELT SQL**
```sql
SELECT store_id, store_name, zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP
BY 1, 2, 3 ORDER BY net_revenue DESC, store_id
```
```text
store_id                  store_name    zone  net_revenue
     S06      NammaMart Banashankari   South     94256.85
     S13      NammaMart Malleshwaram    West     89650.30
     S05          NammaMart JP Nagar   South     86163.65
     S01            NammaMart Hebbal   North     85510.70
     S10      NammaMart Marathahalli    East     84988.90
     S07                     UNKNOWN UNKNOWN     84955.75
     S11        NammaMart HSR Layout    East     83438.50
     S15 NammaMart Basaveshwaranagar    West     83052.90
     S03          NammaMart RT Nagar   North     81404.65
     S08        NammaMart Whitefield    East     78980.50
     S14       NammaMart Vijayanagar    West     77166.75
     S02         NammaMart Yelahanka   North     77093.05
     S04         NammaMart Jayanagar   South     75794.05
     S09       NammaMart Indiranagar    East     66270.30
     S12       NammaMart Rajajinagar    West     62255.05
```

## Q1c. Daily net revenue by zone (30 Sep 2026 shown; the table holds every day)

**Same result in ETL and ELT:** SAME

**ETL SQL**
```sql
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue WHERE
order_date = DATE '2026-09-30' GROUP BY 1, 2 ORDER BY 2
```
```text
order_date    zone  net_revenue
2026-09-30    East      9750.55
2026-09-30   North      4469.55
2026-09-30   South      8638.95
2026-09-30 UNKNOWN      1702.10
2026-09-30    West      5910.10
```

**ELT SQL**
```sql
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis WHERE order_date
= DATE '2026-09-30' GROUP BY 1, 2 ORDER BY 2
```
```text
order_date    zone  net_revenue
2026-09-30    East      9750.55
2026-09-30   North      4469.55
2026-09-30   South      8638.95
2026-09-30 UNKNOWN      1702.10
2026-09-30    West      5910.10
```

## Q2. Revenue and gross margin by category (margin = revenue - quantity x cost_price)

**Same result in ETL and ELT:** SAME

**ETL SQL**
```sql
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct FROM etl_category_margin ORDER BY
net_revenue DESC
```
```text
           category  delivered_orders  net_revenue  gross_margin  margin_pct
            Staples               302    268583.30      67143.14        25.0
          Household               362    211727.05      54352.51        25.7
      Personal Care               322    210269.90      46989.14        22.3
     Dairy & Bakery               406    180555.80      47746.13        26.4
 Snacks & Beverages               341    174893.55      41425.82        23.7
Fruits & Vegetables               374     93604.20      22205.38        23.7
            UNKNOWN               120     71348.10          0.00         NaN
```

**ELT SQL**
```sql
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct FROM mart_category_margin ORDER BY
net_revenue DESC
```
```text
           category  delivered_orders  net_revenue  gross_margin  margin_pct
            Staples               302    268583.30      67143.14        25.0
          Household               362    211727.05      54352.51        25.7
      Personal Care               322    210269.90      46989.14        22.3
     Dairy & Bakery               406    180555.80      47746.13        26.4
 Snacks & Beverages               341    174893.55      41425.82        23.7
Fruits & Vegetables               374     93604.20      22205.38        23.7
            UNKNOWN               120     71348.10          0.00         NaN
```

## Q3. Average delivery time per store and % of delivered orders within 15 minutes

**Same result in ETL and ELT:** SAME

**ETL SQL**
```sql
SELECT f.store_id, coalesce(s.store_name, 'UNKNOWN') AS store_name, count(*) AS delivered_orders,
round(avg(f.delivery_minutes), 1) AS avg_delivery_minutes, round(100.0 * avg(CASE WHEN f.within_15_min THEN 1
ELSE 0 END), 1) AS pct_within_15_min FROM etl_fact_orders f LEFT JOIN etl_dim_store s ON s.store_id =
f.store_id WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2 ORDER BY 1
```
```text
store_id                  store_name  delivered_orders  avg_delivery_minutes  pct_within_15_min
     S01            NammaMart Hebbal               149                  20.5               32.2
     S02         NammaMart Yelahanka               150                  20.9               31.3
     S03          NammaMart RT Nagar               144                  20.6               29.2
     S04         NammaMart Jayanagar               138                  21.3               27.5
     S05          NammaMart JP Nagar               142                  21.2               29.6
     S06      NammaMart Banashankari               153                  22.5               21.6
     S07                     UNKNOWN               150                  21.1               32.7
     S08        NammaMart Whitefield               164                  20.6               35.4
     S09       NammaMart Indiranagar               136                  21.1               26.5
     S10      NammaMart Marathahalli               150                  22.1               26.0
     S11        NammaMart HSR Layout               160                  20.9               28.1
     S12       NammaMart Rajajinagar               118                  21.9               24.6
     S13      NammaMart Malleshwaram               150                  21.9               27.3
     S14       NammaMart Vijayanagar               166                  21.5               31.3
     S15 NammaMart Basaveshwaranagar               157                  22.0               22.9
```

**ELT SQL**
```sql
SELECT store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla
ORDER BY store_id
```
```text
store_id                  store_name  delivered_orders  avg_delivery_minutes  pct_within_15_min
     S01            NammaMart Hebbal               149                  20.5               32.2
     S02         NammaMart Yelahanka               150                  20.9               31.3
     S03          NammaMart RT Nagar               144                  20.6               29.2
     S04         NammaMart Jayanagar               138                  21.3               27.5
     S05          NammaMart JP Nagar               142                  21.2               29.6
     S06      NammaMart Banashankari               153                  22.5               21.6
     S07                     UNKNOWN               150                  21.1               32.7
     S08        NammaMart Whitefield               164                  20.6               35.4
     S09       NammaMart Indiranagar               136                  21.1               26.5
     S10      NammaMart Marathahalli               150                  22.1               26.0
     S11        NammaMart HSR Layout               160                  20.9               28.1
     S12       NammaMart Rajajinagar               118                  21.9               24.6
     S13      NammaMart Malleshwaram               150                  21.9               27.3
     S14       NammaMart Vijayanagar               166                  21.5               31.3
     S15 NammaMart Basaveshwaranagar               157                  22.0               22.9
```

## Q4a. Revenue by payment mode (delivered orders)

**Same result in ETL and ELT:** SAME

**ETL SQL**
```sql
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue FROM
etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY net_revenue DESC
```
```text
    payment_mode  delivered_orders  net_revenue
             UPI              1207    639931.95
            Card               457    251882.30
Cash on Delivery               317    181049.20
          Wallet               246    138118.45
```

**ELT SQL**
```sql
SELECT payment_mode, count(*) AS delivered_orders, round(sum(order_amount), 2) AS net_revenue FROM stg_orders
WHERE dq_status <> 'REJECTED' AND order_status = 'DELIVERED' GROUP BY 1 ORDER BY net_revenue DESC
```
```text
    payment_mode  delivered_orders  net_revenue
             UPI              1207    639931.95
            Card               457    251882.30
Cash on Delivery               317    181049.20
          Wallet               246    138118.45
```

## Q4b. Amount that does not reconcile with the payment gateway, by reason (TOTAL = answer)

**Same result in ETL and ELT:** SAME

**ETL SQL**
```sql
SELECT coalesce(reason, 'TOTAL') AS reason, count(*) AS mismatch_rows, round(sum(abs(difference)), 2) AS
amount FROM etl_payment_reconciliation GROUP BY ROLLUP(reason) ORDER BY reason IS NULL, amount DESC
```
```text
           reason  mismatch_rows    amount
  MISSING_PAYMENT            178  97128.55
ORDER_QUARANTINED             89  84252.38
   PAYMENT_FAILED             42  25722.25
   ORPHAN_PAYMENT              8   4580.57
  AMOUNT_MISMATCH             13    760.00
            TOTAL            330 212443.75
```

**ELT SQL**
```sql
SELECT coalesce(reason, 'TOTAL') AS reason, count(*) AS mismatch_rows, round(sum(abs(difference)), 2) AS
amount FROM mart_payment_reconciliation GROUP BY ROLLUP(reason) ORDER BY reason IS NULL, amount DESC
```
```text
           reason  mismatch_rows    amount
  MISSING_PAYMENT            178  97128.55
ORDER_QUARANTINED             89  84252.38
   PAYMENT_FAILED             42  25722.25
   ORPHAN_PAYMENT              8   4580.57
  AMOUNT_MISMATCH             13    760.00
            TOTAL            330 212443.75
```

## Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)

**Same result in ETL and ELT:** SAME

**ETL SQL**
```sql
SELECT f.customer_id, c.email_masked, c.phone_masked, c.loyalty_tier, count(*) AS delivered_orders,
round(sum(f.net_revenue), 2) AS total_spend FROM etl_fact_orders f JOIN etl_dim_customer c ON c.customer_id =
f.customer_id WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2, 3, 4 ORDER BY total_spend DESC, f.customer_id
LIMIT 10
```
```text
customer_id      email_masked phone_masked loyalty_tier  delivered_orders  total_spend
     C00136 sa***@example.com   96******88       Bronze                 9      8738.10
     C00115 sa***@example.com   96******85       Bronze                 8      8037.70
     C00283 ni***@example.com   91******43       Bronze                 8      7934.60
     C00236 sn***@example.com   90******01     Platinum                12      7831.80
     C00310     ki***@example   91******35       Bronze                 8      7258.95
     C00265 vi***@example.com   91******69         Gold                 8      7151.30
     C00071 ma***@example.com   91******42       Silver                11      6933.30
     C00338 bh***@example.com   94******05       Silver                 9      6848.55
     C00357 ar***@example.com    93*****12       Bronze                11      6637.90
     C00209 ka***@example.com   98******97     Platinum                 8      6519.40
```

**ELT SQL**
```sql
SELECT customer_id, email_masked, phone_masked, loyalty_tier, delivered_orders, total_spend FROM
mart_top_customers ORDER BY spend_rank
```
```text
customer_id      email_masked phone_masked loyalty_tier  delivered_orders  total_spend
     C00136 sa***@example.com   96******88       Bronze                 9      8738.10
     C00115 sa***@example.com   96******85       Bronze                 8      8037.70
     C00283 ni***@example.com   91******43       Bronze                 8      7934.60
     C00236 sn***@example.com   90******01     Platinum                12      7831.80
     C00310     ki***@example   91******35       Bronze                 8      7258.95
     C00265 vi***@example.com   91******69         Gold                 8      7151.30
     C00071 ma***@example.com   91******42       Silver                11      6933.30
     C00338 bh***@example.com   94******05       Silver                 9      6848.55
     C00357 ar***@example.com    93*****12       Bronze                11      6637.90
     C00209 ka***@example.com   98******97     Platinum                 8      6519.40
```

## Q6a. Rows rejected in the latest run, by file and by reason (a row failing 2 rules is counted under each)

**Same result in ETL and ELT:** n/a (different sources)

**ETL SQL**
```sql
SELECT split_part(check_name, '.', 1) AS file, split_part(check_name, '.', 2) AS reason, rows_failed FROM
dq_results WHERE dag_id = 'nammamart_etl_anikesh' AND run_id = (SELECT run_id FROM dq_results WHERE dag_id =
'nammamart_etl_anikesh' ORDER BY checked_at DESC LIMIT 1) AND severity = 'Critical' AND rows_failed > 0 ORDER
BY 1, 3 DESC
```
```text
     file                         reason  rows_failed
customers             customer_id_unique            5
customers         signup_date_not_future            3
   orders   order_amount_matches_formula           18
   orders           customer_id_not_null           18
   orders                order_id_unique           12
   orders              quantity_positive           10
   orders              product_id_exists            9
   orders delivered_has_delivery_minutes            8
   orders                order_ts_parses            6
   orders     delivery_minutes_plausible            6
   orders            order_ts_not_future            5
   orders                store_id_exists            5
   orders             discount_pct_0_100            3
   orders           payment_mode_allowed            3
   orders           order_status_allowed            3
 payments           amount_paid_positive           10
 payments              payment_has_order            8
 payments          payment_ts_not_future            5
 payments              payment_id_unique            4
 payments           payment_mode_allowed            3
 products              cost_price_le_mrp            5
 products              category_not_null            3
 products              product_id_unique            2
 products                   mrp_positive            1
   stores                   zone_allowed            1
```

**ELT SQL**
```sql
SELECT split_part(check_name, '.', 1) AS file, split_part(check_name, '.', 2) AS reason, rows_failed FROM
dq_results WHERE dag_id = 'nammamart_elt_anikesh' AND run_id = (SELECT run_id FROM dq_results WHERE dag_id =
'nammamart_elt_anikesh' ORDER BY checked_at DESC LIMIT 1) AND severity = 'Critical' AND rows_failed > 0 ORDER
BY 1, 3 DESC
```
```text
     file                         reason  rows_failed
customers             customer_id_unique            5
customers         signup_date_not_future            3
   orders           customer_id_not_null           18
   orders   order_amount_matches_formula           18
   orders                order_id_unique           12
   orders              quantity_positive           10
   orders              product_id_exists            9
   orders delivered_has_delivery_minutes            8
   orders                order_ts_parses            6
   orders     delivery_minutes_plausible            6
   orders            order_ts_not_future            5
   orders                store_id_exists            5
   orders             discount_pct_0_100            3
   orders           payment_mode_allowed            3
   orders           order_status_allowed            3
 payments           amount_paid_positive           10
 payments              payment_has_order            8
 payments          payment_ts_not_future            5
 payments              payment_id_unique            4
 payments           payment_mode_allowed            3
 products              cost_price_le_mrp            5
 products              category_not_null            3
 products              product_id_unique            2
 products                   mrp_positive            1
   stores                   zone_allowed            1
```

## Q6b. Total rows set aside in the latest run (from the run audit)

**Same result in ETL and ELT:** n/a (different sources)

**ETL SQL**
```sql
SELECT dag_id, status, rows_extracted, rows_loaded, rows_quarantined FROM pipeline_runs WHERE dag_id =
'nammamart_etl_anikesh' ORDER BY start_time DESC LIMIT 1
```
```text
               dag_id  status  rows_extracted  rows_loaded  rows_quarantined
nammamart_etl_anikesh SUCCESS            5372         3022               152
```

**ELT SQL**
```sql
SELECT dag_id, status, rows_extracted, rows_loaded, rows_quarantined FROM pipeline_runs WHERE dag_id =
'nammamart_elt_anikesh' ORDER BY start_time DESC LIMIT 1
```
```text
               dag_id  status  rows_extracted  rows_loaded  rows_quarantined
nammamart_elt_anikesh SUCCESS            5372         5372               136
```

## Hardening added after the observability experiments

Experiments 6 to 9 exposed weaknesses; each one was fixed and the experiment re-run (results below).

- **run_audit now fails the run** after writing its audit row, so a failed pipeline shows RED in Airflow
  (before, run_audit was the only last task and always succeeded, so failed pipelines looked green).
- **Schema contract** in every extract task: expected columns and order; no retries for a contract error.
- **Volume gate** (`orders.volume_vs_recent_runs`, Critical): stops a run whose orders file has fewer than
  50% of the rows of the largest of the last 7 runs. Raw still keeps the file as received.
- **Fact clean-up**: an order that now fails validation is removed from `etl_fact_orders`, so the fact only
  ever holds rows that pass every Critical check (acceptance criterion 2).
- **Freshness check** ignores future-dated rows when finding the newest order.
- **Pool `duckdb_writer` (1 slot)**: warehouse writers take turns (DuckDB allows one writer at a time).
- **One clock**: all audit timestamps are IST (the ELT scorecard used the container's UTC clock before).

## Part D: Data quality dimensions (both pipelines)

Checks live in `include/dq_checks.py` (Python, ETL) and `include/sql/stg_*.sql` (SQL, ELT). Every check is
written to `dq_results` on every run (run_id, dag_id, check_name, dimension, severity, rows_checked,
rows_failed, status, checked_at). Critical rows are quarantined (ETL) or marked REJECTED (ELT); Warning rows
still load and are logged. Latest runs: ETL 25 FAIL / 34 PASS / 9 WARN (68 checks, including the
increment file); ELT 25 FAIL / 14 PASS / 8 WARN (47 checks). Appendix D shows the table.

| Dimension | Checks (file) | Severity |
|---|---|---|
| Completeness | ids not null in every file; DELIVERED has delivery_minutes; category not null; volume gate | Critical |
| Uniqueness | order_id, payment_id, customer_id, product_id, store_id unique (first copy kept) | Critical |
| Validity | order_status and payment_mode in allowed lists; discount_pct 0-100; order_ts parses; zone allowed | Critical |
| Validity | email format, phone 10 digits, pincode 6 digits; payment_mode needs standardising (e.g. upi) | Warning |
| Accuracy | quantity and price > 0; amount = qty x price x (1 - discount); delivery < 120 min; mrp > 0; cost <= mrp | Critical |
| Accuracy | delivery_minutes outside 8-35; store capacity > 0 | Warning |
| Consistency | customer_id, product_id, store_id exist in their masters; every payment has an order | Critical |
| Consistency | loyalty_tier casing; SUCCESS payment amount = order amount (goes to reconciliation) | Warning |
| Timeliness | no future-dated order_ts, payment_ts, signup_date, launch_date | Critical |
| Timeliness | newest order not older than 1 day | Warning |

## Part E: Data governance

**1. Ownership.**

| Dataset | Business owner | Technical steward |
|---|---|---|
| orders, stores | Head of Operations (COO) | data engineer / FDE |
| payments | Finance (CFO) | data engineer |
| customers | CRM / customer team, with the Data Protection Officer for privacy | data engineer |
| products | Category / merchandising lead | data engineer |
| warehouse tables (raw, staging, marts, dq_results) | CTO (platform); content owned by the source's owner | data engineer |
| marts: revenue, margin, reconciliation | CFO (finance numbers); COO (operations KPIs) | data engineer |

A change to a quality rule such as the 120-minute delivery limit is a business definition: Operations
proposes it, the COO and Finance approve it (it changes what counts as a valid delivery and an SLA hit),
and the data engineer implements it through a reviewed git change (`MAX_DELIVERY_MIN` in
`include/nammamart_config.py`). The git history is the approval trail.

**2. Data dictionary.** Types: text, number, date, timestamp, boolean. PII: Y = personal data,
M = masked/derived from personal data, Q = quasi-identifier (can identify a person in combination), N = no.

`etl_fact_orders` (one row per clean order; source `orders.csv` + increment file)

| Column | Type | Meaning | Allowed values | PII |
|---|---|---|---|---|
| order_id | text | order key | unique | N |
| order_ts | timestamp | when the order was placed (IST) | not in the future | N |
| order_date | date | date part of order_ts | | N |
| customer_id | text | who ordered (CRM id) | exists in customers | Q |
| store_id | text | dark store | exists in stores | N |
| product_id | text | product | exists in products | N |
| quantity | number | units | > 0 | N |
| unit_price | number | price per unit (INR) | > 0 | N |
| discount_pct | number | discount percent | 0-100 | N |
| order_amount | number | quantity x unit_price x (1 - discount/100) | matches formula | N |
| payment_mode | text | how the customer pays | UPI, Card, Cash on Delivery, Wallet | N |
| order_status | text | order outcome | DELIVERED, CANCELLED, RETURNED | N |
| channel | text | ordering channel | e.g. App | N |
| delivery_minutes | number | minutes to deliver | < 120 (normal 8-35) | N |
| net_revenue | number | order_amount if DELIVERED, else 0 | | N |
| gross_margin | number | order_amount - quantity x cost_price if DELIVERED, else 0 | | N |
| within_15_min | boolean | DELIVERED and delivery_minutes <= 15 | | N |

`etl_dim_customer` (source `customers.csv`; full_name, raw email and raw phone are NOT loaded)

| Column | Type | Meaning | Allowed values | PII |
|---|---|---|---|---|
| customer_id | text | CRM key | unique | Q |
| email_masked | text | first 2 letters + *** + domain | pr***@example.com | M |
| phone_masked | text | first 2 and last 2 digits | 98******21 | M |
| city, area, pincode | text | location of the customer | pincode = 6 digits | Q |
| signup_date | date | when the customer joined | not in the future | N |
| loyalty_tier | text | loyalty level | Bronze, Silver, Gold, Platinum | N |
| is_active | text | active flag | | N |

`etl_dim_product` (source `products.csv`): product_id (unique key), product_name, category (not null),
sub_category, brand, mrp (> 0), cost_price (<= mrp), unit, is_perishable, launch_date. No personal data.

`etl_dim_store` (source `stores.csv`): store_id (unique key), store_name, zone (North, South, East, West),
area, pincode, open_date, manager_name (**personal data: employee name**), capacity_orders_per_day.

**3. Lineage.** Appendix A traces one number on `mart_daily_store_kpis` down to a source row.
In ELT it is one chain of joins because staging carries `_source_file`, `_run_id` and `_loaded_at`
for every row and raw keeps the row exactly as received. In ETL the fact table has no source columns, so
the trace ends at the order id and a person must search the CSV. **ELT is easier**; the ETL fix is to add
those three columns to the fact table.

**4. Personal data.** Personal data in the dataset: customers.full_name, email, phone; city, area and
pincode (location); customer_id (identifies a person when linked to orders and purchases);
stores.manager_name (employee). The ETL dimension and the ELT staging table drop full_name, raw email and
raw phone and keep only masked email (`pr***@example.com`) and masked phone (`98******21`). Masking, not
deleting, keeps the customer usable for counting, joining and de-duplicating, and keeps format problems
visible, while hiding the identifying part. Masked data is still personal data (customer_id plus area plus
order history can re-identify someone). The law that applies is India's **Digital Personal Data Protection
Act, 2023** (and the Rules made under it). NammaMart is the "data fiduciary" and is expected to: process data
only for a stated purpose with notice and consent (or a permitted legitimate use); collect only what is
needed; keep it accurate; protect it with reasonable security safeguards; report breaches to the Data
Protection Board and to the people affected; erase it when the purpose is served or consent is withdrawn; and
honour access, correction, erasure and grievance requests. Penalties for failing to protect data can reach
INR 250 crore. (This is an engineering summary, not legal advice; confirm details and dates with counsel.)

**5. Access control.**

| Layer | Who may read |
|---|---|
| raw (including raw_customers) | pipeline service account and named data engineers only; every read logged |
| raw_customers phone / email | nobody for analysis; break-glass only with DPO approval, logged (see note below) |
| staging (masked) | data engineers and analysts |
| marts | COO, finance, store managers, read-only through a BI tool (store managers see only their store) |
| quarantine files / tables | data engineers and the data owner who must fix the source; customer rows are written masked |

Note: marketing gets contact details from the CRM, not the warehouse. Recommended: tokenise or encrypt
the phone and email columns at load, and keep Finance's statutory retention for financial records.

**6. Retention.**

| Data | Keep | After that |
|---|---|---|
| raw tables | 13 months rolling (a year of replay plus one audit cycle) | delete older runs |
| quarantine files | 90 days after the issue is fixed or accepted | delete and log the deletion |
| masked customer data | while active, plus 12 months after the last order | delete or anonymise; erase on request |
| dq_results, pipeline_runs | 24 months (small, needed for trends and audits) | archive |

Why it matters here: ELT raw appends a full copy of every file on every run. `raw_orders` grew from 17,684
to 58,621 rows during our testing alone (each run adds 2,572 rows) without any new orders.

**7. Secrets.** This project needs no credentials (local DuckDB and CSV files). The bonus failure alert
would use `SLACK_WEBHOOK_URL`, stored as an **Airflow Variable** (Admin > Variables, or
`AIRFLOW_VAR_SLACK_WEBHOOK_URL` in the git-ignored `.env` file) and read with `Variable.get()`. Astro's
default `.gitignore` excludes `.env`, and `include/*.duckdb` is excluded too (the raw layer holds real
phone numbers). A secret in a DAG file or on GitHub stays in git history for ever, is copied by every
clone and fork, and is harvested by scanners within minutes; a leaked secret must be rotated.

**8. Audit ("show me a rejected order").** Appendix B does this for a real rejected order: the row exactly as
received (ELT raw, and the ETL quarantine file), the rule it broke (`failure_reason`), and proof it is not
in revenue (0 rows in `etl_fact_orders`, 0 non-rejected rows in staging, identical totals).

**9. Change history.** After the incremental load, 10 orders changed from DELIVERED to RETURNED. September
net revenue fell from 1,134,343.60 to 1,130,149.90 (-4,193.70) in **both** pipelines, because both overwrite
the status. Finance normally wants the September number it already reported and signed off to stay fixed,
and the return booked in October as a deduction; operations and analytics want the corrected number.
To keep both views: never overwrite a status without recording it. Keep an order-status history table
(order_id, status, valid_from, valid_to; a Type 2 history) or an events table fed from each increment, and
publish two marts: **as reported** (frozen at month end) and **as corrected**. ELT already holds the raw
history because raw is append-only (Appendix C shows both versions of one order, each stamped with its file
and run). The ETL fact keeps only the latest version, so ETL cannot reproduce the original view.

## Part F: Observability tests (every experiment run on both DAGs; files restored after each)

Tools: `exp.py` (applies and restores each data change; every restore was checked byte for byte),
`run_experiments.py` (runs both DAGs and records the result in `experiments_log.txt`), `exp_report.py`.
Two baselines are quoted: simultaneous start before the DuckDB pool existed, and after.

**Experiment 1: Happy path.**
- Expected: both DAGs green well before 9:30; the five extract tasks run in parallel; row counts match profiling.
- Observed: started together (as at 9:00), ETL 26.3 s and ELT 27.5 s; alone 5-6 s. **Before** the pool, the same
  simultaneous start took 127.8 s (ETL) and 380.4 s (ELT): both wrote the one DuckDB file, tasks failed on the
  lock and waited out the 2-minute retry delay. ETL rows in / passed / quarantined: orders 2,412 / 2,309 / 103;
  payments 2,228 / 2,198 / 30; customers 405 / 397 / 8; products 152 / 142 / 10; stores 15 / 14 / 1; increment
  160 / 160 / 0. ELT rejected in staging: 91 / 26 / 8 / 10 / 1 (the difference of 16 rows set aside, 152 vs 136, is
  12 duplicate orders + 4 duplicate payments that ELT leaves in raw only). Parallel: the five extract tasks.
- Where I saw it: Airflow graph and grid view, Task Instances durations, "Last 10 Dag Runs" bar chart (one
  2-minute bar, then tiny bars after the pool), `pipeline_runs`, `exp_report.py`.
- What I would change: keep the pool, schedule at 8:30 not 9:00, and move to a client-server warehouse so
  writers need not queue.

**Experiment 2: Re-run (idempotency).**
- Expected: fact table unchanged; ELT raw grows by one copy per run.
- Observed: `etl_fact_orders` 2,459 to 2,459 (+0); `stg_orders` 2,550 (+0); `raw_orders` +2,572 per run
  (2,412 + 160); `pipeline_runs` +2 and `dq_results` +113 rows (one run each). This is correct: raw is
  append-only history, reporting tables are rebuilt or upserted.
- Where I saw it: `experiments_log.txt` (CHANGE line), row counts in `exp_report.py`.
- What I would change: add raw retention (90 days of full snapshots) and load only new files at scale.

**Experiment 3: Incremental load.**
- Expected: 10 corrections update existing orders, 150 new orders are added, no duplicates.
- Observed: September-only load gave 2,309 fact rows and 2,400 staging rows with the 10 orders DELIVERED. With
  the increment both pipelines logged "10 updated, 150 inserted"; fact 2,459, staging 2,550; the 10 orders became
  RETURNED; 0 duplicate order_ids after repeated merges; the 160 rows were appended to raw as received.
- Where I saw it: task log lines `increment: ... 10 updated, 150 inserted` in both DAGs.
- What I would change: store inserted/updated counts in `pipeline_runs`, and keep status history (Part E.9).

**Experiment 4: Missing source file (products.csv renamed).**
- Expected: the extract task fails, retries twice 2 minutes apart, downstream tasks do not run, the run is
  visibly failed and the audit says FAILED.
- Observed: ETL: `extract_products` failed with FileNotFoundError after 3 attempts over 248.9 s (try 1 + 2
  retries, 2-minute delay); the other four extracts succeeded; downstream tasks were upstream_failed;
  `run_audit` wrote FAILED (extracted 5,060, loaded 0). ELT: `extract_products` failed the same way after
  251.5 s; no ELT audit row was recorded in this dags-test run and I did not inspect the UI to explain it (open item). Nothing sent an alert: the only signal was the red run in the Airflow UI.
- Where I saw it: Airflow grid (up_for_retry, failed, upstream_failed), Task Instances tab (Try Number),
  task log, `pipeline_runs`.
- What I would change: retrying a file that is simply absent wastes 4 minutes; use a file sensor that waits
  until 8:45 and then alerts; add an email or Slack failure callback with the webhook in an Airflow Variable.

**Experiment 5: New bad data (5 planted orders).**
- Expected: each lands in quarantine / REJECTED with the right reason; dq_results counts rise by one.
- Observed: all 5 caught with exactly one correct reason each: ORDTEST1 customer_id_not_null, ORDTEST2
  quantity_positive, ORDTEST3 store_id_exists, ORDTEST4 order_status_allowed, ORDTEST5 order_ts_not_future.
  None reached `etl_fact_orders` (0) or the marts (0). `dq_results.rows_failed` rose by exactly 1 per check in
  both pipelines (18 to 19, 10 to 11, 5 to 6, 3 to 4, 5 to 6). The runs stayed green, by design.
- Where I saw it: quarantine file `orders_failed_<run_id>.csv`, `stg_orders.failure_reason`, `dq_results`.
- What I would change: alert the data owner when quarantined rows exceed a threshold (for example 5% of a file).

**Experiment 6: Schema change (order_amount renamed to total).**
- Expected: a loud failure at the first task that needs the column.
- Observed (first attempt, no guard): ETL burned 248 s of retries in the validation stage and the DAG run was
  still GREEN, because `run_audit` was the only last task and succeeded (the audit row said FAILED). ELT
  loaded the renamed file silently: raw loads by position, the values landed in the old column, revenue did not
  move. After the fix (schema contract plus run_audit failing the run): ETL failed in 9.0 s and ELT in 14.6 s with
  the missing and unexpected column named; audit rows FAILED; `raw_orders` +0; revenue unchanged; runs RED.
- Where I saw it: `experiments_log.txt`, `pipeline_runs`, task log message "schema drift in orders.csv".
- What I would change: send the schema-drift message to the owner of the source export and version the contract.

**Experiment 7: Volume anomaly (50 orders instead of 2,412).**
- Expected: a warning; no change to the reports.
- Observed (before): no warning at all. ELT revenue fell from 1,210,981.9 to 102,755.8 because staging is
  rebuilt from the file, ETL kept its old rows, so the two pipelines silently disagreed. After adding the
  volume gate: both stopped; `dq_results` row `orders.volume_vs_recent_runs` = FAIL; audit FAILED; fact 2,459 and
  staging 2,550 untouched; revenue ETL = ELT = 1,210,981.9; the 50 received rows still in `raw_orders` (+50).
- Where I saw it: `dq_results`, `pipeline_runs`, `experiments_log.txt`.
- What I would change: apply the gate to every file, compare with the same weekday, and notify the owner.

**Experiment 8: Distribution drift (unit_price x100 for store S05, 144 orders).**
- Expected: some check or mart metric reveals it.
- Observed: the accuracy check `order_amount_matches_formula` caught the rows (rows_failed 18 to 162 in ETL, 18 to
  160 in ELT) and set them aside, so S05 revenue fell from 86,163.65 to 8,785.20 (-89.8%) and total revenue from
  1,210,981.9 to 1,133,603.45 (-6.4%) in both pipelines. The runs stayed GREEN. Before the fact clean-up, ETL
  kept the stale S05 rows while ELT dropped them. If price and amount had both been multiplied, nothing would
  have fired.
- Where I saw it: `dq_results`, `mart_daily_store_kpis` (store S05 net_revenue).
- What I would change: add a distribution check (a store's daily revenue against its trailing average, alert
  beyond 30%) and escalate when more than 5% of one store's rows fail one rule.

**Experiment 9: Freshness (latest order 30 days old).**
- Expected: the timeliness check warns.
- Observed: the first version said PASS before and after, because it took the newest timestamp including
  future-dated bad rows. Fixed to ignore future timestamps: now WARN in both pipelines (data ends 21 Oct before,
  9 Oct after). The check is yes/no, so it cannot tell 4 days from 18 days, and with September data it warns on
  every run. The experiment also revealed two leftover rows in the ETL fact, fixed by the clean-up above.
- Where I saw it: `dq_results` row `orders.latest_order_not_stale`, newest order_ts in the fact table.
- What I would change: record the age in days; WARN above 1 day, FAIL above 3 days on a weekday.

**Experiment 10: Recovery (Clear the failed task).**
- Expected: only the failed task and the tasks after it run again; tasks that succeeded are not repeated.
- Observed: not demonstrated in the UI for this submission (the failed runs were not cleared).
- Where I saw it: Airflow grid after Clear Task, with the file restored.
- What I would change: add a failure callback so the owner is told to clear the task as soon as the file arrives.

**Five pillars of data observability**

| Pillar | What covers it in this setup | How well |
|---|---|---|
| Freshness | `latest_order_not_stale` (Warning, yes/no); run times | partial: no age, warns every day |
| Volume | rows per file in `dq_results`; volume gate added after exp 7 | good for orders only |
| Schema | schema contract at every extract (added after exp 6) | good |
| Distribution | validity and accuracy rules only | **weakest**: exp 8 passed on a green run |
| Lineage | ELT raw and staging carry `_source_file`, `_run_id`, `_loaded_at` | **best** in ELT; the ETL fact has none |

## Part G: Recommendation to the CTO

**To:** Sameer (CTO). **From:** FDE team. **Re:** ETL or ELT for NammaMart.

**Recommendation: adopt ELT as the main pipeline, with the ETL-style safety gates kept in front of the
reports.** Land every file exactly as received (raw), clean and flag it in SQL (staging), and let finance read
only the marts. Keep the schema contract and the volume gate in front of staging. Retire the ETL DAG after
finance has signed off one month of ELT marts, and keep the quarantine report as a daily "what was
rejected" email.

**Why, based on what we measured**
- **Same answers.** On the full dataset all 8 headline checks matched (net revenue 1,210,981.90, 330
  reconciliation rows, 212,443.75 not reconciling) and matched again after every experiment, so correctness
  does not decide this.
- **Auditability: ELT wins.** ELT kept every received row, including the 50-row bad file and both versions
  of the 10 returned orders, each stamped with file and run. The ETL fact holds only the latest clean row.
  "Show me the rejected order exactly as received" is one query in ELT (Appendix B).
- **Quality guarantee: ETL wins on paper, tie in practice.** ETL cannot leak a bad row because it never loads
  it. In ELT bad rows sit in staging flagged REJECTED and every mart must filter them. In experiment 5, 0 of 5
  planted rows leaked in either pipeline. The risk is a future query that forgets the filter; give finance a
  view that already applies it.
- **Ease of change: ELT wins.** Rules are SQL files that can be re-run over raw history without re-reading
  the sources. In ETL a rule change only affects new loads; rows already in the fact keep the old verdict (we
  saw leftover rows after experiment 9 and had to add a clean-up step).
- **1000x growth** (2.4k to 2.4M orders a month): ETL holds each file in pandas memory on the Airflow worker,
  which breaks first. ELT pushes the work into the warehouse engine. The catch: our raw layer re-loads a full
  snapshot every run (2,572 rows per run; 58,621 raw rows after two days of testing), so before growth it
  needs incremental raw loads and a retention job.
- **Cost.** Both use only open-source tools. ELT stores extra copies (cheap at this size). The hidden cost
  we hit was DuckDB's single writer: simultaneous 9:00 runs took 128 s and 380 s until writes were queued.

**Top 3 risks of the recommended design**
1. **Raw holds real personal data and grows for ever.** `raw_customers` has unmasked phone numbers and
   emails; under the DPDP Act a leak is a reportable breach. Mitigation: raw readable only by the pipeline
   account, tokenise or encrypt PII at load, 13-month retention, an erasure procedure.
2. **One machine at 9:00 AM.** One DuckDB writer and one Airflow on one laptop: if it is off, nothing runs and
   `catchup=False` does not recover the day. Mitigation: an always-on server, a client-server warehouse, an
   alert if the 9:00 run has not started by 9:10, and a schedule of 8:30.
3. **Gates catch broken rows, not wrong-but-valid numbers.** In experiment 8 a store lost 90% of its revenue on
   a green run, and the freshness check warns every day so people will ignore it. Mitigation: distribution
   checks against history, freshness as age in days, and a named owner who reads the daily rejected-rows summary.

## Appendix A: lineage worked example

Number traced: `mart_daily_store_kpis` row **2026-09-19 / S02 (NammaMart Yelahanka, North)**: net_revenue **9732.0** from 13 delivered orders.

**ELT path** (every staging row carries `_source_file`, `_run_id`, `_loaded_at`):

```text
 order_id  order_amount _source_file                              _run_id
ORD001406         503.2   orders.csv scheduled__2026-10-28T03_30_00_00_00
ORD001421         610.2   orders.csv scheduled__2026-10-28T03_30_00_00_00
ORD001425         192.6   orders.csv scheduled__2026-10-28T03_30_00_00_00
ORD001435         666.0   orders.csv scheduled__2026-10-28T03_30_00_00_00
ORD001440        1975.0   orders.csv scheduled__2026-10-28T03_30_00_00_00
```

Sum of all those staging rows = **9732.0** (mart says 9732.0: match).

Down to the source row for order `ORD001406` (the raw table keeps it exactly as received, all text):

```text
 order_id            order_ts customer_id store_id product_id quantity unit_price discount_pct order_amount order_status delivery_minutes _source_file                              _run_id                 _loaded_at
ORD001406 2026-09-19 06:06:57      C00278      S02      P0123        1      629.0           20        503.2    DELIVERED               25   orders.csv scheduled__2026-10-28T03_30_00_00_00 2026-10-28 04:19:21.814980
```

**ETL path** (the fact table has no source columns, so the trace stops at the order id):

```text
 order_id  order_amount  net_revenue
ORD001406         503.2        503.2
ORD001421         610.2        610.2
ORD001425         192.6        192.6
ORD001435         666.0        666.0
ORD001440        1975.0       1975.0
```

ETL sum for the same store and day = **9732.0**. To find the file and row I must open `orders.csv` and search for the order id by hand. Finding: **ELT lineage is easier**; improvement: add `_source_file`, `_run_id`, `_loaded_at` to the ETL fact.

## Appendix B: audit of a rejected order

Order picked: `ORD000099` (rejected for `quantity_positive`).

**1. Exactly as received** (ELT raw, all text, newest run):

```text
 order_id            order_ts customer_id store_id product_id quantity unit_price discount_pct order_amount order_status _source_file                              _run_id
ORD000099 2026-09-02 12:22:47      C00203      S13      P0130        0      406.0            0          0.0    DELIVERED   orders.csv scheduled__2026-10-28T03_30_00_00_00
```

**2. The rule it broke:**

```text
 order_id dq_status    failure_reason
ORD000099  REJECTED quantity_positive
```

ETL quarantine file `orders_failed_scheduled__2026-10-28T03_30_00_00_00.csv` holds the same row with its reason:

```text
 order_id quantity order_amount order_status    failure_reason
ORD000099        0          0.0    DELIVERED quantity_positive
```

**3. Proof it is not in the CFO's revenue:**

- rows with this order id in `etl_fact_orders`: **0**
- rows with this order id in staging that are not REJECTED (the only rows marts read): **0**
- total net revenue: ETL **1210981.9** and ELT **1210981.9** (identical, neither includes the order).

## Appendix C: change history of a returned order

Order `ORD000017` was DELIVERED in `orders.csv` and became RETURNED in the 1 October increment. ELT raw is append-only, so **both versions are still there**, each stamped with its source file and run:

```text
order_status                    _source_file  copies               first_loaded                last_loaded
   DELIVERED                      orders.csv      29 2026-10-24 22:54:37.315857 2026-10-28 04:19:21.814980
    RETURNED orders_2026-10-01_increment.csv      24 2026-10-25 07:04:41.104645 2026-10-28 04:19:33.198354
```

Reporting tables show only the latest status:

```text
            tbl order_status
etl_fact_orders     RETURNED
     stg_orders     RETURNED
```

September net revenue **after** the increment: **1130149.9**. The 10 returned orders were worth **4193.7**, so the figure finance originally saw was **1134343.6** (difference = the returned orders).

## Appendix D: evidence tables from the warehouse

**dq_results, latest run of each DAG, by dimension and status**

```text
               dag_id    dimension status  checks  rows_failed
nammamart_elt_anikesh     Accuracy   FAIL       6         50.0
nammamart_elt_anikesh     Accuracy   PASS       2          0.0
nammamart_elt_anikesh     Accuracy   WARN       1          1.0
nammamart_elt_anikesh Completeness   FAIL       3         29.0
nammamart_elt_anikesh Completeness   PASS       8          0.0
nammamart_elt_anikesh  Consistency   FAIL       3         22.0
nammamart_elt_anikesh  Consistency   PASS       1          0.0
nammamart_elt_anikesh  Consistency   WARN       2         21.0
nammamart_elt_anikesh   Timeliness   FAIL       3         13.0
nammamart_elt_anikesh   Timeliness   PASS       1          0.0
nammamart_elt_anikesh   Timeliness   WARN       1          1.0
nammamart_elt_anikesh   Uniqueness   FAIL       4         23.0
nammamart_elt_anikesh   Uniqueness   PASS       1          0.0
nammamart_elt_anikesh     Validity   FAIL       6         19.0
nammamart_elt_anikesh     Validity   PASS       1          0.0
nammamart_elt_anikesh     Validity   WARN       4         27.0
nammamart_etl_anikesh     Accuracy   FAIL       6         50.0
nammamart_etl_anikesh     Accuracy   PASS       7          0.0
nammamart_etl_anikesh     Accuracy   WARN       1          1.0
nammamart_etl_anikesh Completeness   FAIL       3         29.0
nammamart_etl_anikesh Completeness   PASS      13          0.0
nammamart_etl_anikesh  Consistency   FAIL       3         22.0
nammamart_etl_anikesh  Consistency   PASS       4          0.0
nammamart_etl_anikesh  Consistency   WARN       2         21.0
nammamart_etl_anikesh   Timeliness   FAIL       3         13.0
nammamart_etl_anikesh   Timeliness   PASS       2          0.0
nammamart_etl_anikesh   Timeliness   WARN       2          2.0
nammamart_etl_anikesh   Uniqueness   FAIL       4         23.0
nammamart_etl_anikesh   Uniqueness   PASS       2          0.0
nammamart_etl_anikesh     Validity   FAIL       6         19.0
nammamart_etl_anikesh     Validity   PASS       6          0.0
nammamart_etl_anikesh     Validity   WARN       4         27.0
```

**pipeline_runs, latest runs (the run audit)**

```text
               dag_id  status  rows_extracted  rows_loaded  rows_quarantined                 start_time                   end_time
nammamart_etl_anikesh SUCCESS            5372         3022               152 2026-10-28 09:49:14.492018 2026-10-28 09:49:47.233300
nammamart_elt_anikesh SUCCESS            5372         5372               136 2026-10-28 09:49:14.490456 2026-10-28 09:49:46.302856
nammamart_elt_anikesh  FAILED            5060            0               136 2026-10-27 11:01:13.137985 2026-10-27 11:05:33.588913
nammamart_etl_anikesh  FAILED            5060            0                 0 2026-10-27 10:57:04.065588 2026-10-27 11:01:12.931727
nammamart_elt_anikesh SUCCESS            5372         5372               136 2026-10-27 10:56:56.266150 2026-10-27 10:57:01.226419
nammamart_etl_anikesh SUCCESS            5372         3022               152 2026-10-27 10:56:49.662053 2026-10-27 10:56:54.036061
nammamart_elt_anikesh SUCCESS            5212         5212               134 2026-10-27 10:56:42.455987 2026-10-27 10:56:47.247849
nammamart_etl_anikesh SUCCESS            5212         2864               150 2026-10-27 10:56:36.096911 2026-10-27 10:56:40.367370
```

**etl_payment_reconciliation, mismatches by reason**

```text
           reason  mismatch_rows   amount
  MISSING_PAYMENT            178 97128.55
ORDER_QUARANTINED             89 84252.38
   PAYMENT_FAILED             42 25722.25
   ORPHAN_PAYMENT              8  4580.57
  AMOUNT_MISMATCH             13   760.00
```

