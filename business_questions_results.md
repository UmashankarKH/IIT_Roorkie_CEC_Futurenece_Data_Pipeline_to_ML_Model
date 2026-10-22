# NammaMart business questions: query results

Source: `include/sql/business_questions.sql`
Database: `include/warehouse_sbonagiri.duckdb`

Both DAGs were run on the six files: a base run, then a second run with `load_increment=true`. ETL results come from the `etl_*` tables and ELT results from the `mart_*` tables. The two pipelines agree on every total; see the checks at the end.


## 1. Daily net revenue by zone and store

### ETL

```sql
SELECT order_date, zone, store_id, store_name, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, zone, store_id;
```

Rows returned: 460. Net revenue by zone (summed from the result):

```
   zone  net_revenue
   East     301697.7
  North     236222.2
  South     249010.5
UNKNOWN      81912.9
   West     301478.9
```

First 12 rows:

```
order_date    zone store_id             store_name  net_revenue
2026-09-01    East      S08   NammaMart Whitefield      2984.70
2026-09-01    East      S09  NammaMart Indiranagar      2620.60
2026-09-01    East      S10 NammaMart Marathahalli      2141.60
2026-09-01    East      S11   NammaMart HSR Layout      2121.15
2026-09-01   North      S01       NammaMart Hebbal      1410.80
2026-09-01   North      S02    NammaMart Yelahanka      2385.55
2026-09-01   North      S03     NammaMart RT Nagar      4583.80
2026-09-01   South      S04    NammaMart Jayanagar      1103.30
2026-09-01   South      S05     NammaMart JP Nagar       803.00
2026-09-01   South      S06 NammaMart Banashankari      2812.00
2026-09-01 UNKNOWN      S07   NammaMart BTM Layout      5844.75
2026-09-01    West      S12  NammaMart Rajajinagar       300.00
```

### ELT

```sql
SELECT order_date, zone, store_id, store_name, net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
ORDER BY order_date, zone, store_id;
```

Rows returned: 460. Net revenue by zone (summed from the result):

```
   zone  net_revenue
   East     301697.7
  North     236222.2
  South     249010.5
UNKNOWN      81912.9
   West     301478.9
```

First 12 rows:

```
order_date    zone store_id             store_name  net_revenue
2026-09-01    East      S08   NammaMart Whitefield      2984.70
2026-09-01    East      S09  NammaMart Indiranagar      2620.60
2026-09-01    East      S10 NammaMart Marathahalli      2141.60
2026-09-01    East      S11   NammaMart HSR Layout      2121.15
2026-09-01   North      S01       NammaMart Hebbal      1410.80
2026-09-01   North      S02    NammaMart Yelahanka      2385.55
2026-09-01   North      S03     NammaMart RT Nagar      4583.80
2026-09-01   South      S04    NammaMart Jayanagar      1103.30
2026-09-01   South      S05     NammaMart JP Nagar       803.00
2026-09-01   South      S06 NammaMart Banashankari      2812.00
2026-09-01 UNKNOWN      S07   NammaMart BTM Layout      5844.75
2026-09-01    West      S12  NammaMart Rajajinagar       300.00
```


## 2. Revenue and gross margin by product category

### ETL

```sql
SELECT category, revenue, gross_margin, margin_pct FROM etl_category_margin ORDER BY revenue DESC;
```

```
           category   revenue  gross_margin  margin_pct
            Staples 264456.90      66435.28       25.12
          Household 210229.95      54055.27       25.71
      Personal Care 208774.70      46709.57       22.37
     Dairy & Bakery 177404.85      47200.82       26.61
 Snacks & Beverages 174575.60      41339.24       23.68
Fruits & Vegetables  92968.45      22018.22       23.68
            UNKNOWN  41911.75       4943.39       11.79
```

### ELT

```sql
SELECT category, revenue, gross_margin, margin_pct FROM mart_category_margin ORDER BY revenue DESC;
```

```
           category   revenue  gross_margin  margin_pct
            Staples 264456.90      66435.28       25.12
          Household 210229.95      54055.27       25.71
      Personal Care 208774.70      46709.57       22.37
     Dairy & Bakery 177404.85      47200.82       26.61
 Snacks & Beverages 174575.60      41339.24       23.68
Fruits & Vegetables  92968.45      22018.22       23.68
            UNKNOWN  41911.75       4943.39       11.79
```


## 3. Average delivery time per store and % within 15 minutes

### ETL

```sql
SELECT s.store_id, s.store_name, COUNT(*) AS delivered_orders,
       ROUND(AVG(f.delivery_minutes), 2) AS avg_delivery_minutes,
       ROUND(100.0 * AVG(CAST(f.within_15_min AS INTEGER)), 2) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name ORDER BY s.store_id;
```

```
store_id                  store_name  delivered_orders  avg_delivery_minutes  pct_within_15_min
     S01            NammaMart Hebbal               145                 20.50              32.41
     S02         NammaMart Yelahanka               142                 21.18              30.28
     S03          NammaMart RT Nagar               139                 20.71              29.50
     S04         NammaMart Jayanagar               134                 21.46              26.87
     S05          NammaMart JP Nagar               135                 21.18              29.63
     S06      NammaMart Banashankari               149                 22.33              22.15
     S07        NammaMart BTM Layout               147                 21.12              33.33
     S08        NammaMart Whitefield               157                 20.52              35.67
     S09       NammaMart Indiranagar               128                 21.24              27.34
     S10      NammaMart Marathahalli               145                 21.94              26.21
     S11        NammaMart HSR Layout               149                 20.80              28.19
     S12       NammaMart Rajajinagar               113                 21.99              23.89
     S13      NammaMart Malleshwaram               143                 21.87              27.27
     S14       NammaMart Vijayanagar               163                 21.48              31.29
     S15 NammaMart Basaveshwaranagar               146                 22.21              21.92
```

### ELT

```sql
SELECT store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla ORDER BY store_id;
```

```
store_id                  store_name  delivered_orders  avg_delivery_minutes  pct_within_15_min
     S01            NammaMart Hebbal               145                 20.50              32.41
     S02         NammaMart Yelahanka               142                 21.18              30.28
     S03          NammaMart RT Nagar               139                 20.71              29.50
     S04         NammaMart Jayanagar               134                 21.46              26.87
     S05          NammaMart JP Nagar               135                 21.18              29.63
     S06      NammaMart Banashankari               149                 22.33              22.15
     S07        NammaMart BTM Layout               147                 21.12              33.33
     S08        NammaMart Whitefield               157                 20.52              35.67
     S09       NammaMart Indiranagar               128                 21.24              27.34
     S10      NammaMart Marathahalli               145                 21.94              26.21
     S11        NammaMart HSR Layout               149                 20.80              28.19
     S12       NammaMart Rajajinagar               113                 21.99              23.89
     S13      NammaMart Malleshwaram               143                 21.87              27.27
     S14       NammaMart Vijayanagar               163                 21.48              31.29
     S15 NammaMart Basaveshwaranagar               146                 22.21              21.92
```


## 4a. Revenue by payment mode

### ETL

```sql
SELECT payment_mode, COUNT(*) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY payment_mode ORDER BY net_revenue DESC;
```

```
    payment_mode  delivered_orders  net_revenue
             UPI              1158    620350.55
            Card               432    237148.25
Cash on Delivery               305    176240.70
          Wallet               240    136582.70
```

### ELT

```sql
SELECT payment_mode, delivered_orders, net_revenue FROM mart_payment_mode_revenue ORDER BY net_revenue DESC;
```

```
    payment_mode  delivered_orders  net_revenue
             UPI              1158    620350.55
            Card               432    237148.25
Cash on Delivery               305    176240.70
          Wallet               240    136582.70
```


## 4b. Amount that does not reconcile with the payment gateway

### ETL

```sql
SELECT COALESCE(mismatch_reason, 'TOTAL') AS mismatch_reason, COUNT(*) AS records, ROUND(SUM(ABS(difference)), 2) AS unreconciled_amount
FROM etl_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason IS NULL, mismatch_reason;
```

```
           mismatch_reason  records  unreconciled_amount
           AMOUNT_MISMATCH       11               560.00
          NO_PAYMENT_FOUND      168             92455.60
PAYMENT_FOR_REJECTED_ORDER      155            114259.83
 PAYMENT_FOR_UNKNOWN_ORDER        8              4580.57
   PAYMENT_STATUS_MISMATCH       52             29915.95
                     TOTAL      394            241771.95
```

### ELT

```sql
SELECT COALESCE(mismatch_reason, 'TOTAL') AS mismatch_reason, COUNT(*) AS records, ROUND(SUM(ABS(difference)), 2) AS unreconciled_amount
FROM mart_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason IS NULL, mismatch_reason;
```

```
           mismatch_reason  records  unreconciled_amount
           AMOUNT_MISMATCH       11               560.00
          NO_PAYMENT_FOUND      168             92455.60
PAYMENT_FOR_REJECTED_ORDER      155            114259.83
 PAYMENT_FOR_UNKNOWN_ORDER        8              4580.57
   PAYMENT_STATUS_MISMATCH       52             29915.95
                     TOTAL      394            241771.95
```


## 5. Top 10 customers by spend

### ETL

```sql
SELECT ROW_NUMBER() OVER (ORDER BY SUM(f.net_revenue) DESC, c.customer_id) AS spend_rank,
       c.customer_id, c.full_name, c.email_masked, c.loyalty_tier,
       COUNT(*) AS delivered_orders, ROUND(SUM(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.email_masked, c.loyalty_tier
QUALIFY spend_rank <= 10 ORDER BY spend_rank;
```

```
 spend_rank customer_id     full_name      email_masked loyalty_tier  delivered_orders  total_spend
          1      C00136   Sanjay Bhat sa***@example.com       Bronze                 9      8738.10
          2      C00283    Nikhil Rao ni***@example.com       Bronze                 8      7934.60
          3      C00236   Sneha Naidu sn***@example.com     Platinum                12      7831.80
          4      C00115  Sanjay Kumar sa***@example.com       Bronze                 7      7337.70
          5      C00310   Kiran Patil     ki***@example       Bronze                 8      7258.95
          6      C00265  Vikram Hegde vi***@example.com         Gold                 8      7151.30
          7      C00338  Bhavana Iyer bh***@example.com       Silver                 9      6848.55
          8      C00071 Mahesh Pillai ma***@example.com       Silver                10      6696.15
          9      C00357   Arjun Naidu ar***@example.com       Bronze                11      6637.90
         10      C00209  Kavya Pillai ka***@example.com     Platinum                 8      6519.40
```

### ELT

```sql
SELECT spend_rank, customer_id, full_name, email_masked, loyalty_tier, delivered_orders, total_spend
FROM mart_top_customers ORDER BY spend_rank;
```

```
 spend_rank customer_id     full_name      email_masked loyalty_tier  delivered_orders  total_spend
          1      C00136   Sanjay Bhat sa***@example.com       Bronze                 9      8738.10
          2      C00283    Nikhil Rao ni***@example.com       Bronze                 8      7934.60
          3      C00236   Sneha Naidu sn***@example.com     Platinum                12      7831.80
          4      C00115  Sanjay Kumar sa***@example.com       Bronze                 7      7337.70
          5      C00310   Kiran Patil     ki***@example       Bronze                 8      7258.95
          6      C00265  Vikram Hegde vi***@example.com         Gold                 8      7151.30
          7      C00338  Bhavana Iyer bh***@example.com       Silver                 9      6848.55
          8      C00071 Mahesh Pillai ma***@example.com       Silver                10      6696.15
          9      C00357   Arjun Naidu ar***@example.com       Bronze                11      6637.90
         10      C00209  Kavya Pillai ka***@example.com     Platinum                 8      6519.40
```


## 6. Rows rejected in the latest run, by file and reason

### ETL

```sql
SELECT q.source_file, u.reason, COUNT(*) AS rows_rejected
FROM etl_quarantine q, UNNEST(STRING_SPLIT(q.failure_reason, '; ')) AS u(reason)
WHERE q.run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_sbonagiri'
                  ORDER BY start_time DESC LIMIT 1)
GROUP BY q.source_file, u.reason ORDER BY q.source_file, rows_rejected DESC;
```

```
                    source_file                                                             reason  rows_rejected
                  customers.csv                                              duplicate customer_id              5
                  customers.csv                                          signup_date in the future              3
                     orders.csv                               product_id not in validated products             87
                     orders.csv                                                customer_id missing             18
                     orders.csv order_amount does not match quantity x unit_price x (1 - discount)             18
                     orders.csv                             customer_id not in validated customers             18
                     orders.csv                                                 duplicate order_id             12
                     orders.csv                                              quantity not positive             10
                     orders.csv                           DELIVERED order without delivery_minutes              8
                     orders.csv                                             order_ts in the future              7
                     orders.csv                                       delivery_minutes 120 or more              6
                     orders.csv                                     order_ts not a valid timestamp              5
                     orders.csv                                   store_id not in validated stores              5
                     orders.csv                                   order_status not in allowed list              3
                     orders.csv                                         discount_pct outside 0-100              3
                     orders.csv                                   payment_mode not in allowed list              2
                     orders.csv                                                   order_ts missing              1
orders_2026-10-01_increment.csv                               product_id not in validated products             10
orders_2026-10-01_increment.csv                             customer_id not in validated customers              1
                   payments.csv                                           amount_paid not positive             10
                   payments.csv                                           payment_ts in the future              5
                   payments.csv                                               duplicate payment_id              4
                   payments.csv                                   payment_mode not in allowed list              2
                   products.csv                                               cost_price above mrp              5
                   products.csv                                               duplicate product_id              2
                   products.csv                                                   mrp not positive              1
```

### ELT

```sql
SELECT r.source_file, u.reason, COUNT(*) AS rows_rejected
FROM mart_rejected_rows r, UNNEST(STRING_SPLIT(r.failure_reason, '; ')) AS u(reason)
GROUP BY r.source_file, u.reason ORDER BY r.source_file, rows_rejected DESC;
```

```
                    source_file                                                             reason  rows_rejected
                  customers.csv                                              duplicate customer_id              5
                  customers.csv                                          signup_date in the future              3
                     orders.csv                               product_id not in validated products             87
                     orders.csv                             customer_id not in validated customers             18
                     orders.csv order_amount does not match quantity x unit_price x (1 - discount)             18
                     orders.csv                                                customer_id missing             18
                     orders.csv                                                 duplicate order_id             12
                     orders.csv                                              quantity not positive             10
                     orders.csv                           DELIVERED order without delivery_minutes              8
                     orders.csv                                             order_ts in the future              7
                     orders.csv                                       delivery_minutes 120 or more              6
                     orders.csv                                     order_ts not a valid timestamp              5
                     orders.csv                                   store_id not in validated stores              5
                     orders.csv                                   order_status not in allowed list              3
                     orders.csv                                         discount_pct outside 0-100              3
                     orders.csv                                   payment_mode not in allowed list              2
                     orders.csv                                                   order_ts missing              1
orders_2026-10-01_increment.csv                               product_id not in validated products             10
orders_2026-10-01_increment.csv                             customer_id not in validated customers              1
                   payments.csv                                           amount_paid not positive             10
                   payments.csv                                           payment_ts in the future              5
                   payments.csv                                               duplicate payment_id              4
                   payments.csv                                   payment_mode not in allowed list              2
                   products.csv                                               cost_price above mrp              5
                   products.csv                                               duplicate product_id              2
                   products.csv                                                   mrp not positive              1
```

## ETL vs ELT totals

| Measure | ETL | ELT |
| --- | --- | --- |
| Net revenue, all delivered orders | 1170322.2 | 1170322.2 |
| Gross margin | 282701.79 | 282701.79 |
| Reconciliation records / amount | 394 / 241771.95 | 394 / 241771.95 |

## Data-quality observations

- Store S07 has no zone in `stores.csv`, so its revenue is reported under zone `UNKNOWN` instead of being dropped.
- Three products have no category; their revenue is reported under category `UNKNOWN`.
- Future-dated rows (orders dated 2026-10-21, 2026-11-02 and 2027-01-01, all later than the extract date of their file) are rejected, so they never reach these tables.
- Orders that reference a rejected product or customer are rejected too (see question 6).
- `PAYMENT_FOR_REJECTED_ORDER` rows are payments for orders that were quarantined. Finance should look at these first.
