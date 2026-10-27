# NammaMart Quick Commerce — ETL & ELT Pipeline Submission

**Author:** Nihir Kumar  
**Module:** FDAIE Module 2 · Data Engineering Foundations  
**Repository Branch:** `main`  
**Scheduled SLA:** 9:00 AM IST, Monday to Saturday (`0 9 * * 1-6`, `Asia/Kolkata`)  
**DuckDB Database:** `include/warehouse_nihir.duckdb`  
**Quarantine Directory:** `include/quarantine/nihir/`  

---

## Part 0: Discovery & Data Profiling

### 1. Dataset Profile Overview

The provided dataset in `include/data/nammamart/` contains 5,372 baseline rows across 5 files covering 1–30 September 2026, plus a next-day incremental feed of 160 rows for 1 October 2026.

| File Name | Total Rows | Columns | Key Identifiers | Nulls / Missing Values | Duplicate Primary Keys |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `orders.csv` | 2,412 | 13 | `order_id` (Fact) | `order_ts`: 2<br>`customer_id`: 18<br>`delivery_minutes`: 164 (8 on DELIVERED) | 12 duplicate keys (24 rows total) |
| `payments.csv` | 2,228 | 7 | `payment_id` (Fact) | `gateway_ref`: 301 (all Cash on Delivery) | 4 duplicate keys (8 rows total) |
| `customers.csv` | 405 | 10 | `customer_id` (Dim) | `email`: 4 (marked 'N/A')<br>`city`: 6 (empty string) | 5 duplicate keys (10 rows total) |
| `products.csv` | 152 | 10 | `product_id` (Dim) | `category`: 3 (empty string for GoldenGrain) | 2 duplicate keys (`P0030`, `P0140` old versions) |
| `stores.csv` | 15 | 8 | `store_id` (Dim) | `zone`: 1 (empty string for S07 BTM Layout) | 0 duplicate keys |
| `orders_2026-10-01_increment.csv` | 160 | 13 | `order_id` (Feed) | None | 0 duplicate keys (10 status updates, 150 new orders) |

### 2. Numeric & Date Ranges

- **`orders.csv`**:
  - `quantity`: min `-3.0`, max `5.0` (10 rows have `quantity <= 0`).
  - `unit_price`: min `₹10.0`, max `₹883.0`.
  - `discount_pct`: min `0.0%`, max `250.0%` (3 rows have discounts > 100%: 250%, 150%, 120%).
  - `order_amount`: min `-₹1,593.0`, max `₹9,576.0` (18 rows have arithmetic mismatches against formula).
  - `delivery_minutes`: min `8.0`, max `1,440.0` (6 rows have impossible delivery durations: 480, 900, 1440 min).
  - `order_ts`: min `2026-09-01 06:30:15`, max `2027-01-01 09:00:00` (7 future dates in Oct/Nov 2026 and Jan 2027; 5 unparseable timestamps like `31/09/2026 10:00` and `2026-13-05`).
- **`payments.csv`**:
  - `amount_paid`: min `-₹1,593.0`, max `₹9,576.0` (10 rows have `amount_paid <= 0`).
  - `payment_ts`: min `2026-09-01 06:30:24`, max `2027-01-01 09:01:46` (7 future dates).
- **`customers.csv`**:
  - `signup_date`: min `2024-06-01`, max `2027-01-05` (3 future signups: `C00400`, `C00398`, `C00038`).
  - `pincode`: min `000000`, max `560102` (3 rows contain invalid `5600XX`).
- **`products.csv`**:
  - `mrp`: min `-₹203.0`, max `₹883.0` (`P0109` shampoo has negative MRP).
  - `cost_price`: min `₹6.79`, max `₹631.79` (4 products have `cost_price > mrp`).
- **`stores.csv`**:
  - `capacity_orders_per_day`: min `0`, max `600` (`S12` Rajajinagar has capacity 0).

### 3. Referential Integrity Check

1. `orders.customer_id` → `customers.customer_id`: 18 orders have null `customer_id`. All other present IDs resolve to master records.
2. `orders.product_id` → `products.product_id`: 9 orders reference non-existent catalog IDs (`P9455`, `P9212`, `P9816`, `P9893`, `P9989`, `P9784`, `P9822`, `P9399`, `P9761`). 11 orders reference `P0109` (which has invalid negative MRP in master).
3. `orders.store_id` → `stores.store_id`: 5 orders reference invalid stores (`S99`, `S17` ×3, `S00`).
4. `payments.order_id` → `orders.order_id`: 8 payments reference non-existent orders (`ORD970544`, `ORD968496`, `ORD988106`, `ORD941071`, `ORD981600`, `ORD997176`, `ORD953136`, `ORD966342`).

---

### 4. Data Issues Log

Below is the verified Data Issues Log documenting defects discovered across all files:

| File | Row Key | Column | Problem Description | Quality Dimension | Proposed Rule | Severity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `orders.csv` | `ORD001462` (×12 keys) | `order_id` | Identical duplicate order rows appended at the end of the file | Uniqueness | Primary key must be unique; deduplicate keeping first valid occurrence | Critical |
| `orders.csv` | `ORD002255` (×18 rows) | `customer_id` | Null/empty customer ID | Completeness | Reject order if `customer_id` is missing | Critical |
| `orders.csv` | `ORD000362` | `order_ts` | Missing empty timestamp string | Completeness | Order timestamp must be non-null | Critical |
| `orders.csv` | `ORD000403`, `ORD002012` | `order_ts` | Unparseable timestamp (`N/A`, `31/09/2026 10:00`, `2026-13-05`) | Validity | Must strictly parse as ISO `YYYY-MM-DD HH:MM:SS` | Critical |
| `orders.csv` | `ORD002119`, `ORD001693` | `order_ts` | Future-dated orders (`2027-01-01`, `2026-10-21`) | Timeliness | Order timestamp must not exceed pipeline execution date | Critical |
| `orders.csv` | `ORD000469` (×10 rows) | `quantity` | Zero or negative quantities (`0`, `-1`, `-2`, `-3`) | Accuracy | Quantity must be integer > 0 | Critical |
| `orders.csv` | `ORD002103` (×3 rows) | `discount_pct` | Discount exceeds 100% (`250%`, `150%`, `120%`) | Validity | Discount percentage must be between 0 and 100 | Critical |
| `orders.csv` | `ORD002305` (×18 rows) | `order_amount` | Amount mismatch with formula `qty * price * (1 - disc/100)` | Accuracy | `abs(order_amount - round(expected, 2)) <= 0.02` | Critical |
| `orders.csv` | `ORD001554` (×8 rows) | `delivery_minutes`| Missing delivery duration on `DELIVERED` status | Completeness | `DELIVERED` orders must have recorded delivery minutes | Critical |
| `orders.csv` | `ORD001037` (×6 rows) | `delivery_minutes`| Impossible duration (480, 900, 1440 min) | Accuracy | Delivery duration must be between 1 and 120 minutes | Critical |
| `orders.csv` | `ORD000986` (×9 rows) | `product_id` | Unknown product IDs (`P9455`, `P9212`, etc.) | Consistency | Must exist in active product catalog | Critical |
| `orders.csv` | `ORD001799` (×5 rows) | `store_id` | Unknown store IDs (`S99`, `S17`, `S00`) | Consistency | Must exist in store master | Critical |
| `orders.csv` | `ORD000500` (×3 rows) | `order_status` | Invalid status `'Pending??'` | Validity | Must be in `['DELIVERED', 'CANCELLED', 'RETURNED']` | Critical |
| `orders.csv` | `ORD001812` (×2 rows) | `payment_mode` | Typo `'Cashh'` | Validity | Must be in `['UPI', 'Card', 'Cash on Delivery', 'Wallet']` | Critical |
| `orders.csv` | `ORD001066` (×2 rows) | `order_status` | Lowercase status `'delivered'` | Consistency | Standardise casing via `UPPER(TRIM())` | Warning |
| `orders.csv` | `ORD000159` (×8 rows) | `payment_mode` | Non-standard casing/spacing (`upi`, `UPI `, `CARD`, `cod`) | Consistency | Trim and standardise to canonical payment mode names | Warning |
| `orders.csv` | `ORD001249` (×72 rows) | `delivery_minutes`| Non-delivered order has recorded delivery minutes | Consistency | Clear delivery minutes or log operational note | Warning |
| `payments.csv`| `PAY0001651` (×4 keys) | `payment_id` | Duplicate payment records (8 rows total) | Uniqueness | `payment_id` must be unique; keep first occurrence | Critical |
| `payments.csv`| `PAY0001568` (×10 rows)| `amount_paid` | Zero or negative amounts (`-43.05`, `0.0`, `-1593.0`) | Accuracy | Payment amount must be strictly > 0 | Critical |
| `payments.csv`| `PAY002219` (×8 rows) | `order_id` | Orphan payment referencing non-existent order | Consistency | Every payment must map to a known `order_id` | Critical |
| `payments.csv`| `PAY0000396` (×7 rows) | `payment_ts` | Future-dated payment timestamp (`2027-01-01`, etc.) | Timeliness | Payment timestamp must not exceed pipeline execution date | Critical |
| `payments.csv`| `PAY0000354` (×12 rows)| `amount_paid` | `SUCCESS` payment amount differs from order amount | Consistency | Successful payment must equal `order_amount` | Critical |
| `payments.csv`| `PAY0002188` (×10 rows)| `payment_mode` | Inconsistent casing (`upi`, `cod`, `CARD`, `Cashh`) | Consistency | Standardise to canonical payment modes | Warning |
| `payments.csv`| `PAY0000133` (×301 rows)| `gateway_ref` | Missing gateway reference on Cash on Delivery | Completeness | Allow null only for Cash on Delivery; enforce for digital modes | Warning |
| `customers.csv`| `C00103` (×5 keys) | `customer_id` | Duplicate customer IDs with different address/phone | Uniqueness | Deduplicate customer ID keeping primary record | Critical |
| `customers.csv`| `C00337`, `C00307` (×12)| `email` | Malformed email or `'N/A'` (`arjun.rao307example.com`) | Validity | Must follow standard RFC email regex format | Warning |
| `customers.csv`| `C00151` (×7 rows) | `phone` | Phone number not 10 digits (`996292312`, `90917798`) | Validity | Must have exactly 10 numeric digits | Warning |
| `customers.csv`| `C00400` (×3 rows) | `signup_date` | Future signup dates (`2027-01-05`, `2026-11-12`) | Timeliness | Signup date cannot be in the future | Warning |
| `customers.csv`| `C00199` (×8 rows) | `loyalty_tier` | Inconsistent casing (`SILVER`, `BRONZE`, `bronze`) | Consistency | Standardise to title case (`Gold`, `Silver`, `Bronze`, `Platinum`) | Warning |
| `customers.csv`| `C00355` (×6 rows) | `city` | Empty city string | Completeness | Default to `'Bengaluru'` for local operations | Warning |
| `customers.csv`| `C00132` (×3 rows) | `pincode` | Masked/invalid pincode (`5600XX`) | Validity | Enforce 6-digit Indian PIN code format | Warning |
| `products.csv` | `P0030`, `P0140` | `product_id` | Old versions appended at end (`(old)` with lower MRP) | Uniqueness | Deduplicate keeping active catalog record | Critical |
| `products.csv` | `P0109` | `mrp` | Negative MRP (`-203.0`) on SmileBright Shampoo 9 | Accuracy | MRP must be positive (`mrp > 0`) | Critical |
| `products.csv` | `P0051`, `P0060`, `P0068`| `category` | Null category for GoldenGrain Dal, Rice, Oil | Completeness | Must not be empty; impute `'Staples'` from brand/name | Warning |
| `products.csv` | `P0019`, `P0073` (×4)| `cost_price` | Cost price exceeds MRP (`cost_price > mrp`) | Accuracy | Flag negative gross margin products for commercial review | Warning |
| `stores.csv` | `S07` | `zone` | Null zone for NammaMart BTM Layout | Completeness | Must not be null; resolve to `'South'` zone | Warning |
| `stores.csv` | `S12` | `capacity` | Zero daily order capacity (`capacity_orders_per_day = 0`) | Accuracy | Dark store capacity must be > 0 | Warning |
| `orders_increment` | `ORD000017` (×10 rows)| `order_status` | Status retroactively updated from DELIVERED to RETURNED | Timeliness | MERGE/upsert late status changes on `order_id` | Critical |

---

### 5. Client Clarification Questions (FDE Day One)

1. **Handling of Negative Quantities and Defective Orders:** In `orders.csv`, there are 10 rows with non-positive quantities (e.g., `ORD001296` has `quantity = -2` and `amount_paid = -394.0`). Does operations record customer refunds directly in the orders feed as negative lines, or are these point-of-sale data entry bugs that should be quarantined?
2. **Zone Mapping & Capacity for Dark Stores:** Store `S07` (BTM Layout) has an empty zone field, and `S12` (Rajajinagar) has a capacity of `0`. Can ops confirm that BTM Layout belongs to the **South Zone** and whether Rajajinagar's capacity is temporarily zero due to warehouse expansion?
3. **Discrepancy Policy for Late Returns Across Month Boundaries:** The October 1 increment changes 10 delivered September orders to `RETURNED`. Does Finance prefer to restate September net revenue retroactively (reducing closed September books by ₹5,123.40), or should September numbers remain closed with the return accounted for as an October adjustment?

---

## Part A & B: Six Business Questions (Answers from Both Pipelines)

As required by Acceptance Criterion #10 and the Submission Checklist, below are the SQL queries and tabulated outputs from **both pipelines** (the ETL Star Schema and the ELT Analytical Marts), followed by the reconciliation analysis explaining how late returns impact September revenue.

---

### Question 1: Daily Net Revenue by Zone and Store (Delivered Orders Only, After Discount)

#### ELT Query & Result (`mart_daily_store_kpis`)
```sql
SELECT
    order_date,
    zone,
    store_name,
    delivered_orders,
    net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date DESC, zone, store_name
LIMIT 10;
```

| order_date | zone | store_name | delivered_orders | net_revenue (₹) |
| :--- | :--- | :--- | :---: | :---: |
| 2026-10-01 | East | NammaMart HSR Layout | 18 | 8,571.10 |
| 2026-10-01 | East | NammaMart Indiranagar | 8 | 2,610.05 |
| 2026-10-01 | East | NammaMart Marathahalli | 13 | 6,020.90 |
| 2026-10-01 | East | NammaMart Whitefield | 15 | 6,569.90 |
| 2026-10-01 | North | NammaMart Hebbal | 13 | 8,958.35 |
| 2026-10-01 | North | NammaMart RT Nagar | 6 | 3,695.75 |
| 2026-10-01 | North | NammaMart Yelahanka | 11 | 4,726.35 |
| 2026-10-01 | South | NammaMart BTM Layout | 5 | 2,656.40 |
| 2026-10-01 | South | NammaMart Banashankari | 6 | 4,893.55 |
| 2026-10-01 | South | NammaMart JP Nagar | 13 | 8,785.20 |

#### ETL Query & Result (`etl_daily_zone_revenue`)
```sql
SELECT
    order_date,
    zone,
    store_name,
    delivered_orders,
    daily_net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date DESC, zone, store_name
LIMIT 10;
```

| order_date | zone | store_name | delivered_orders | daily_net_revenue (₹) |
| :--- | :--- | :--- | :---: | :---: |
| 2026-09-30 | East | NammaMart HSR Layout | 7 | 5,056.80 |
| 2026-09-30 | East | NammaMart Indiranagar | 1 | 52.25 |
| 2026-09-30 | East | NammaMart Marathahalli | 7 | 3,073.70 |
| 2026-09-30 | East | NammaMart Whitefield | 4 | 1,567.80 |
| 2026-09-30 | North | NammaMart Hebbal | 2 | 1,976.00 |
| 2026-09-30 | North | NammaMart RT Nagar | 4 | 2,878.00 |
| 2026-09-30 | North | NammaMart Yelahanka | 5 | 2,752.55 |
| 2026-09-30 | South | NammaMart Banashankari | 4 | 1,326.60 |
| 2026-09-30 | South | NammaMart JP Nagar | 3 | 1,607.75 |
| 2026-09-30 | South | NammaMart Jayanagar | 5 | 3,365.10 |

---

### Question 2: Revenue and Gross Margin by Product Category

#### ELT Query & Result (`mart_category_margin`)
```sql
SELECT
    category,
    delivered_orders,
    net_revenue,
    total_cogs,
    gross_margin,
    gross_margin_pct
FROM mart_category_margin
ORDER BY net_revenue DESC;
```

| category | delivered_orders | net_revenue (₹) | total_cogs (₹) | gross_margin (₹) | gross_margin_pct |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Staples** | 380 | 3,29,513.20 | 2,62,391.72 | 67,121.48 | 20.37% |
| **Household** | 384 | 2,25,358.85 | 1,67,181.48 | 58,177.37 | 25.82% |
| **Personal Care** | 338 | 2,12,726.60 | 1,65,985.22 | 46,741.38 | 21.97% |
| **Dairy & Bakery** | 422 | 1,81,101.45 | 1,33,268.82 | 47,832.63 | 26.41% |
| **Snacks & Beverages** | 340 | 1,74,700.60 | 1,33,329.82 | 41,370.78 | 23.68% |
| **Fruits & Vegetables** | 392 | 99,566.20 | 79,054.82 | 20,511.38 | 20.60% |

#### ETL Query & Result (`etl_category_margin`)
```sql
SELECT
    category,
    delivered_orders,
    total_revenue,
    total_cogs,
    gross_margin,
    margin_pct
FROM etl_category_margin
ORDER BY total_revenue DESC;
```

| category | delivered_orders | total_revenue (₹) | total_cogs (₹) | gross_margin (₹) | margin_pct |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Staples** | 348 | 3,06,212.55 | 2,42,920.38 | 63,292.17 | 20.67% |
| **Personal Care** | 320 | 2,02,398.10 | 1,58,134.32 | 44,263.78 | 21.87% |
| **Household** | 338 | 1,99,321.45 | 1,48,289.76 | 51,031.69 | 25.60% |
| **Dairy & Bakery** | 380 | 1,69,528.75 | 1,24,736.91 | 44,791.84 | 26.42% |
| **Snacks & Beverages** | 321 | 1,61,598.95 | 1,23,933.35 | 37,665.60 | 23.31% |
| **Fruits & Vegetables** | 370 | 93,284.30 | 73,683.68 | 19,600.62 | 21.01% |

---

### Question 3: Average Delivery Time per Store, and % within 15 Minutes

#### ELT Query & Result (`mart_delivery_sla`)
```sql
SELECT
    store_id,
    store_name,
    zone,
    delivered_orders,
    avg_delivery_minutes,
    orders_within_15_min,
    pct_within_15_min
FROM mart_delivery_sla
ORDER BY pct_within_15_min DESC;
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | orders_within_15_min | pct_within_15_min |
| :---: | :--- | :--- | :---: | :---: | :---: | :---: |
| **S08** | NammaMart Whitefield | East | 164 | 20.61 min | 58 | 35.37% |
| **S07** | NammaMart BTM Layout | South | 150 | 21.13 min | 49 | 32.67% |
| **S01** | NammaMart Hebbal | North | 147 | 20.40 min | 48 | 32.65% |
| **S14** | NammaMart Vijayanagar | West | 168 | 21.35 min | 54 | 32.14% |
| **S02** | NammaMart Yelahanka | North | 149 | 20.97 min | 47 | 31.54% |
| **S05** | NammaMart JP Nagar | South | 141 | 21.18 min | 42 | 29.79% |
| **S03** | NammaMart RT Nagar | North | 142 | 20.68 min | 42 | 29.58% |
| **S11** | NammaMart HSR Layout | East | 159 | 20.81 min | 45 | 28.30% |
| **S04** | NammaMart Jayanagar | South | 139 | 21.25 min | 39 | 28.06% |
| **S13** | NammaMart Malleshwaram | West | 150 | 21.87 min | 41 | 27.33% |
| **S09** | NammaMart Indiranagar | East | 134 | 21.19 min | 35 | 26.12% |
| **S10** | NammaMart Marathahalli | East | 150 | 22.09 min | 39 | 26.00% |
| **S12** | NammaMart Rajajinagar | West | 118 | 21.86 min | 29 | 24.58% |
| **S15** | NammaMart Basaveshwaranagar | West | 156 | 22.06 min | 36 | 23.08% |
| **S06** | NammaMart Banashankari | South | 153 | 22.52 min | 33 | 21.57% |

#### ETL Query & Result (`etl_fact_orders` + `etl_dim_store`)
```sql
SELECT
    s.store_id,
    s.store_name,
    s.zone,
    COUNT(f.order_id) AS delivered_orders,
    ROUND(AVG(f.delivery_minutes), 2) AS avg_delivery_minutes,
    SUM(CASE WHEN f.within_15_min THEN 1 ELSE 0 END) AS orders_within_15_min,
    ROUND(SUM(CASE WHEN f.within_15_min THEN 1 ELSE 0 END) * 100.0 / COUNT(f.order_id), 2) AS pct_within_15_min
FROM etl_fact_orders f
JOIN etl_dim_store s ON f.store_id = s.store_id
WHERE f.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name, s.zone
ORDER BY pct_within_15_min DESC;
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | orders_within_15_min | pct_within_15_min |
| :---: | :--- | :--- | :---: | :---: | :---: | :---: |
| **S08** | NammaMart Whitefield | East | 150 | 20.55 min | 54 | 36.00% |
| **S07** | NammaMart BTM Layout | South | 145 | 21.34 min | 47 | 32.41% |
| **S01** | NammaMart Hebbal | North | 134 | 20.64 min | 42 | 31.34% |
| **S03** | NammaMart RT Nagar | North | 137 | 20.66 min | 42 | 30.66% |
| **S14** | NammaMart Vijayanagar | West | 152 | 21.91 min | 46 | 30.26% |
| **S02** | NammaMart Yelahanka | North | 138 | 21.05 min | 41 | 29.71% |
| **S05** | NammaMart JP Nagar | South | 129 | 21.11 min | 38 | 29.46% |
| **S04** | NammaMart Jayanagar | South | 133 | 21.41 min | 38 | 28.57% |
| **S11** | NammaMart HSR Layout | East | 142 | 20.89 min | 40 | 28.17% |
| **S13** | NammaMart Malleshwaram | West | 141 | 21.84 min | 38 | 26.95% |
| **S09** | NammaMart Indiranagar | East | 126 | 21.24 min | 33 | 26.19% |
| **S10** | NammaMart Marathahalli | East | 138 | 22.09 min | 36 | 26.09% |
| **S12** | NammaMart Rajajinagar | West | 112 | 21.82 min | 28 | 25.00% |
| **S15** | NammaMart Basaveshwaranagar | West | 149 | 22.18 min | 34 | 22.82% |
| **S06** | NammaMart Banashankari | South | 147 | 22.61 min | 31 | 21.09% |

---

### Question 4: Revenue by Payment Mode, and Total Unreconciled Amount

#### ELT Query & Result (`stg_orders` & `mart_payment_reconciliation`)
```sql
-- Net Revenue by Payment Mode (Post-Increment)
SELECT
    payment_mode,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
    ROUND(SUM(CASE WHEN order_status = 'DELIVERED' THEN order_amount ELSE 0 END), 2) AS net_revenue
FROM stg_orders
WHERE dq_status = 'PASSED'
GROUP BY payment_mode
ORDER BY net_revenue DESC;
```

| payment_mode | total_orders | delivered_orders | net_revenue (₹) |
| :--- | :---: | :---: | :---: |
| **UPI** | 1,322 | 1,203 | 6,38,260.60 |
| **Card** | 509 | 453 | 2,50,491.75 |
| **Cash on Delivery** | 349 | 318 | 1,81,668.40 |
| **Wallet** | 271 | 246 | 1,38,118.45 |

```sql
-- Unreconciled Breakdown
SELECT
    reconciliation_status,
    COUNT(*) AS mismatch_count,
    ROUND(SUM(unreconciled_amount), 2) AS total_unreconciled_amount
FROM mart_payment_reconciliation
GROUP BY reconciliation_status
ORDER BY total_unreconciled_amount DESC;
```

| reconciliation_status | mismatch_count | total_unreconciled_amount (₹) |
| :--- | :---: | :---: |
| `MISSING_PAYMENT_FOR_ACTIVE_ORDER` | 181 | 97,865.15 |
| `PAYMENT_STATUS_REFUNDED` | 66 | 36,753.20 |
| `PAYMENT_STATUS_FAILED` | 42 | 25,722.25 |
| `AMOUNT_MISMATCH` | 12 | 660.00 |
| **Total Unreconciled Discrepancies** | **301** | **₹1,61,000.60** |

#### ETL Query & Result (`etl_fact_orders` & `etl_payment_reconciliation`)
```sql
-- Net Revenue by Payment Mode (September Baseline)
SELECT
    payment_mode,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
    ROUND(SUM(net_revenue), 2) AS net_revenue
FROM etl_fact_orders
GROUP BY payment_mode
ORDER BY net_revenue DESC;
```

| payment_mode | total_orders | delivered_orders | net_revenue (₹) |
| :--- | :---: | :---: | :---: |
| **UPI** | 1,277 | 1,164 | 6,19,159.70 |
| **Card** | 466 | 411 | 2,29,523.35 |
| **Cash on Delivery** | 312 | 284 | 1,60,022.25 |
| **Wallet** | 242 | 218 | 1,23,638.80 |

```sql
-- ETL Reconciliation Mismatches
SELECT
    mismatch_reason,
    COUNT(*) AS mismatch_count,
    ROUND(SUM(unreconciled_amount), 2) AS total_unreconciled_amount
FROM etl_payment_reconciliation
GROUP BY mismatch_reason
ORDER BY total_unreconciled_amount DESC;
```

| mismatch_reason | mismatch_count | total_unreconciled_amount (₹) |
| :--- | :---: | :---: |
| `PAYMENT_STATUS_REFUNDED` | 66 | 36,753.20 |
| `PAYMENT_STATUS_FAILED` | 42 | 25,722.25 |
| `MISSING_PAYMENT_FOR_ACTIVE_ORDER` | 29 | 17,533.30 |
| `AMOUNT_MISMATCH` | 11 | 560.00 |
| **Total ETL Unreconciled** | **148** | **₹80,568.75** |

---

### Question 5: Top 10 Customers by Spend, with Loyalty Tier (Masked PII)

#### ELT Query & Result (`mart_top_customers`)
```sql
SELECT
    customer_id,
    full_name,
    masked_email,
    masked_phone,
    city,
    loyalty_tier,
    delivered_orders,
    total_spend
FROM mart_top_customers;
```

| customer_id | full_name | masked_email | masked_phone | city | loyalty_tier | delivered_orders | total_spend (₹) |
| :---: | :--- | :--- | :--- | :--- | :--- | :---: | :---: |
| **C00136** | Sanjay Bhat | `sa***@example.com` | `96******88` | Bengaluru | Bronze | 9 | 8,738.10 |
| **C00115** | Sanjay Kumar | `sa***@example.com` | `96******85` | Bengaluru | Bronze | 8 | 8,037.70 |
| **C00283** | Nikhil Rao | `ni***@example.com` | `91******43` | Bengaluru | Bronze | 8 | 7,934.60 |
| **C00236** | Sneha Naidu | `sn***@example.com` | `90******01` | Bengaluru | Platinum | 12 | 7,831.80 |
| **C00310** | Kiran Patil | `ki***@example` | `91******35` | Bengaluru | Bronze | 8 | 7,258.95 |
| **C00265** | Vikram Hegde | `vi***@example.com` | `91******69` | Bengaluru | Gold | 8 | 7,151.30 |
| **C00071** | Mahesh Pillai | `ma***@example.com` | `91******42` | Bengaluru | Silver | 11 | 6,933.30 |
| **C00338** | Bhavana Iyer | `bh***@example.com` | `94******05` | Bengaluru | Silver | 9 | 6,848.55 |
| **C00357** | Arjun Naidu | `ar***@example.com` | `93******12` | Bengaluru | Bronze | 11 | 6,637.90 |
| **C00209** | Kavya Pillai | `ka***@example.com` | `98******97` | Bengaluru | Platinum | 8 | 6,519.40 |

#### ETL Query & Result (`etl_fact_orders` + `etl_dim_customer`)
```sql
SELECT
    c.customer_id,
    c.full_name,
    c.masked_email,
    c.masked_phone,
    c.city,
    c.loyalty_tier,
    COUNT(f.order_id) AS delivered_orders,
    ROUND(SUM(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f
JOIN etl_dim_customer c ON f.customer_id = c.customer_id
WHERE f.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.masked_email, c.masked_phone, c.city, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
```

| customer_id | full_name | masked_email | masked_phone | city | loyalty_tier | delivered_orders | total_spend (₹) |
| :---: | :--- | :--- | :--- | :--- | :--- | :---: | :---: |
| **C00136** | Sanjay Bhat | `sa***@example.com` | `96******88` | Bengaluru | Bronze | 9 | 8,738.10 |
| **C00283** | Nikhil Rao | `ni***@example.com` | `91******43` | Bengaluru | Bronze | 8 | 7,934.60 |
| **C00071** | Mahesh Pillai | `ma***@example.com` | `91******42` | Bengaluru | Silver | 12 | 7,349.30 |
| **C00310** | Kiran Patil | `ki***@example` | `91******35` | Bengaluru | Bronze | 8 | 7,258.95 |
| **C00265** | Vikram Hegde | `vi***@example.com` | `91******69` | Bengaluru | Gold | 8 | 7,151.30 |
| **C00236** | Sneha Naidu | `sn***@example.com` | `90******01` | Bengaluru | Platinum | 11 | 7,016.40 |
| **C00115** | Sanjay Kumar | `sa***@example.com` | `96******85` | Bengaluru | Bronze | 7 | 6,948.35 |
| **C00338** | Bhavana Iyer | `bh***@example.com` | `94******05` | Bengaluru | Silver | 9 | 6,848.55 |
| **C00357** | Arjun Naidu | `ar***@example.com` | `93******12` | Bengaluru | Bronze | 11 | 6,637.90 |
| **C00209** | Kavya Pillai | `ka***@example.com` | `98******97` | Bengaluru | Platinum | 8 | 6,519.40 |

---

### Question 6: Number of Rows Rejected Today, by File and Reason

#### ELT Query & Result (Staging Tables)
```sql
SELECT file, failure_reason, COUNT(*) AS rejected_rows
FROM (
    SELECT 'orders' AS file, failure_reason FROM stg_orders WHERE dq_status = 'FAILED'
    UNION ALL
    SELECT 'payments' AS file, failure_reason FROM stg_payments WHERE dq_status = 'FAILED'
    UNION ALL
    SELECT 'customers' AS file, failure_reason FROM stg_customers WHERE dq_status = 'FAILED'
    UNION ALL
    SELECT 'products' AS file, failure_reason FROM stg_products WHERE dq_status = 'FAILED'
)
GROUP BY file, failure_reason
ORDER BY file, rejected_rows DESC;
```

#### ETL Query & Result (`dq_results` / Quarantine Files)
```sql
SELECT
    SPLIT_PART(check_name, '_', 1) AS file,
    check_name,
    dimension,
    severity,
    rows_failed AS rejected_rows
FROM dq_results
WHERE severity = 'Critical' AND rows_failed > 0
ORDER BY file, rejected_rows DESC;
```

**Unified Rejection Breakdown (Identical Across Both Pipelines):**

| file | failure_reason / check_name | rejected_rows | Dimension | Severity |
| :--- | :--- | :---: | :--- | :--- |
| **orders** | `consistency:unknown_product_id` | 20 | Consistency | Critical |
| **orders** | `completeness:null_customer_id` | 18 | Completeness | Critical |
| **orders** | `accuracy:order_amount_mismatch` | 15 | Accuracy | Critical |
| **orders** | `uniqueness:duplicate_order_id` | 12 | Uniqueness | Critical |
| **orders** | `accuracy:quantity_le_zero` | 10 | Accuracy | Critical |
| **orders** | `completeness:missing_delivery_minutes` | 8 | Completeness | Critical |
| **orders** | `timeliness:future_order_ts` | 7 | Timeliness | Critical |
| **orders** | `accuracy:delivery_minutes_out_of_range`| 6 | Accuracy | Critical |
| **orders** | `completeness:null_or_invalid_order_ts` | 6 | Completeness/Validity | Critical |
| **orders** | `consistency:unknown_store_id` | 5 | Consistency | Critical |
| **orders** | `validity:discount_pct_out_of_range` | 3 | Validity | Critical |
| **orders** | `validity:invalid_order_status` | 3 | Validity | Critical |
| **orders** | `validity:invalid_payment_mode` | 2 | Validity | Critical |
| **payments** | `accuracy:amount_paid_le_zero` | 10 | Accuracy | Critical |
| **payments** | `consistency:orphan_order_id` | 8 | Consistency | Critical |
| **payments** | `timeliness:future_payment_ts` | 7 | Timeliness | Critical |
| **payments** | `uniqueness:duplicate_payment_id` | 4 | Uniqueness | Critical |
| **payments** | `validity:invalid_payment_mode` | 2 | Validity | Critical |
| **customers**| `uniqueness:duplicate_customer_id` | 5 | Uniqueness | Critical |
| **products** | `uniqueness:duplicate_product_id` | 2 | Uniqueness | Critical |
| **products** | `accuracy:mrp_le_zero` | 1 | Accuracy | Critical |
| **Total** | **All Rejections Combined** | **154** | | |


## Part C: Scheduling Answers (9:00 AM IST)

### 1. Cron Expression
`0 9 * * 1-6`

### 2. UTC Run Time in Airflow UI
In the Airflow UI, scheduled runs appear at **03:30 UTC**. Indian Standard Time (IST) is UTC+05:30; subtracting 5 hours and 30 minutes from 09:00 gives 03:30 UTC.

### 3. Impact of Leaving `start_date` in UTC
If `start_date` were set with the default UTC timezone instead of `pendulum.timezone("Asia/Kolkata")`:
- A cron of `0 9 * * 1-6` would fire at **09:00 UTC**, which corresponds to **2:30 PM IST**.
- This would miss the client's mandatory 9:00 AM IST SLA by 5.5 hours, meaning Kavya's 9:30 AM leadership standup would run with stale data.

### 4. Behavior When Laptop or Docker is Off at 9:00 AM
With `catchup=False`:
- Airflow skips all missed historical intervals while the system was offline.
- When Docker starts back up (e.g. at 10:00 AM), Airflow will **not** trigger a backfill storm of past runs; it will wait until the next scheduled window (9:00 AM IST the next day), or execute immediately only if a manual run or catchup trigger is issued. This prevents resource exhaustion and duplicate upstream extractions.

---

## Part D: Data Quality Dimensions Framework

Six dimensions of data quality are enforced in both pipelines:

```
               ┌────────────────────────────────────────────────────────┐
               │           NammaMart 6-Dimension DQ Engine              │
               └──────────────────────────┬─────────────────────────────┘
                                          │
       ┌───────────────┬──────────────────┼─────────────────┬───────────────┐
       ▼               ▼                  ▼                 ▼               ▼
 Completeness      Uniqueness         Validity          Accuracy       Consistency & Timeliness
 (Null checks)    (PK deduping)    (Allowed values)   (Math/SLA bounds) (FKs & Future dates)
```

1. **Completeness:** `customer_id` not null; `order_ts` not null; `delivery_minutes` present on `DELIVERED` orders; product `category` not null.
2. **Uniqueness:** `order_id`, `payment_id`, `customer_id`, and `product_id` must be unique. Secondary occurrences are quarantined.
3. **Validity:** `order_status` in `['DELIVERED', 'CANCELLED', 'RETURNED']`; `payment_mode` in allowed list; `discount_pct` in `[0, 100]`; `email` matches RFC regex; `phone` is 10 digits; `pincode` is 6 digits.
4. **Accuracy:** `quantity > 0`; `order_amount` formula matches within ₹0.02; `mrp > 0`; `cost_price <= mrp`; `delivery_minutes` between 1 and 120.
5. **Consistency:** Referential integrity of foreign keys (`customer_id`, `product_id`, `store_id`) against master dimensions; loyalty tier casing standardisation.
6. **Timeliness:** No future-dated timestamps on orders, payments, or signups. Freshness alerts on stale batches (> 1 day old).

Every pipeline run populates `dq_results` with: `run_id`, `dag_id`, `check_name`, `dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, `checked_at`.

---

## Part E: Data Governance

### 1. Ownership
The Order Management System (OMS) team owns orders; the Payment Operations team owns payments; the CRM team owns customers; the Merchandising team owns products; and the Dark Store Operations team owns stores. The Data Governance Committee (Operations Lead Kavya, CTO Sameer, and Finance Lead) must formally approve changes to quality thresholds, such as modifying the 120-minute delivery SLA limit.

### 2. Data Dictionary

| Table Name | Column Name | Data Type | Meaning | Source System | Allowed Values | PII? |
| :--- | :--- | :--- | :--- | :--- | :--- | :---: |
| `etl_fact_orders` | `order_id` | VARCHAR | Unique order identifier | OMS | `ORD` + 6 digits | No |
| `etl_fact_orders` | `order_ts` | VARCHAR | Order placement timestamp | OMS | ISO 8601 Timestamp | No |
| `etl_fact_orders` | `customer_id` | VARCHAR | Customer foreign key | OMS | `C` + 5 digits | No |
| `etl_fact_orders` | `net_revenue` | DOUBLE | Final delivered order value | Derived | ≥ 0.00 | No |
| `etl_fact_orders` | `order_status` | VARCHAR | Final order lifecycle status | OMS | DELIVERED, CANCELLED, RETURNED | No |
| `etl_dim_customer` | `masked_email`| VARCHAR | Masked email address | CRM | `xx***@domain.com` | Yes (Masked) |
| `etl_dim_customer` | `masked_phone`| VARCHAR | Masked phone number | CRM | `98******21` | Yes (Masked) |
| `etl_dim_customer` | `loyalty_tier`| VARCHAR | Customer loyalty classification| CRM | Gold, Silver, Bronze, Platinum | No |
| `etl_dim_product` | `cost_price` | DOUBLE | Unit acquisition cost | ERP | > 0.00 | No |
| `etl_dim_store` | `zone` | VARCHAR | Geographic operating quadrant | Master | North, South, East, West | No |

### 3. Lineage Trace
- **Target Metric:** ₹8,958.35 net revenue for store `S01` (NammaMart Hebbal) on `2026-10-01` in `mart_daily_store_kpis`.
- **Traceability in ELT:** Queried `stg_orders` filtered by `store_id = 'S01'`, `order_date = '2026-10-01'`, and `order_status = 'DELIVERED'`, which maps directly to 13 raw rows in `raw_orders_increment` originating from lines 2, 14, 28, 41, 55, 69, 82, 94, 107, 120, 133, 145, 158 of `orders_2026-10-01_increment.csv`.
- **Comparison:** ELT makes lineage auditing significantly simpler than ETL because every raw record is preserved in `raw_orders` with `_source_file`, `_row_id`, and `_loaded_at`, allowing an analyst to write one SQL query to trace any aggregated dashboard KPI back to its source CSV row. In ETL, dropped or transformed fields require inspecting external quarantine logs.

### 4. Personal Data & Indian DPDP Act Compliance
Columns containing personal data include `full_name`, `email`, and `phone` in `customers.csv`. We mask email (`pr***@example.com`) and phone (`98******39`) rather than delete them so downstream analytics can perform customer cohort grouping and spend segmentation without exposing cleartext PII. India's Digital Personal Data Protection (DPDP) Act 2023 mandates purpose limitation, data minimisation, and reasonable security safeguards to prevent unauthorised disclosure.

### 5. Access Control Matrix
- **Raw Layer (`raw_*`):** Restricted strictly to Data Engineers and Data Platform Administrators. Contains unmasked phone numbers and raw audit records.
- **Staging Layer (`stg_*`):** Accessible to Data Engineers and Analytics Engineers.
- **Mart / Reporting Layer (`mart_*`, `etl_*`):** Open to Business Analysts, Store Managers, Ops Leaders, and Finance. Contains only masked PII and clean dimensions.
- **Quarantine Files:** Accessible to Data Engineers and Ops Analysts conducting source data remediation.

### 6. Retention Policy
Raw data is archived to encrypted cold object storage (e.g. S3 Glacier) after 90 days and retained for 7 years to meet tax and regulatory compliance. Staging tables are retained for 30 days. Quarantine CSVs are retained for 60 days to allow source system correction. Masked reporting marts are retained indefinitely for year-over-year commercial trend reporting.

### 7. Secrets Management
Credentials (e.g., database connection strings, Slack webhook tokens) must be stored in Airflow Connections or Airflow Variables backed by an enterprise secrets backend (HashiCorp Vault or AWS Secrets Manager) and retrieved dynamically using `Variable.get()`. Hardcoding credentials in DAG files or committing them to GitHub exposes production systems to automated scrapers and breaches.

### 8. Audit Trail for Rejected Orders
When Finance queries why order `ORD000469` is missing from CFO revenue:
- **ETL:** Open `include/quarantine/nihir/orders_failed_<run_id>.csv` and locate `ORD000469`, which displays `failure_reason = 'accuracy:quantity_le_zero(0.0)'`. Then query `SELECT * FROM etl_fact_orders WHERE order_id = 'ORD000469'`, returning 0 rows, confirming it never entered revenue.
- **ELT:** Query `SELECT * FROM stg_orders WHERE order_id = 'ORD000469'`, which returns `dq_status = 'FAILED'` and `failure_reason = 'accuracy:quantity_le_zero'`. Querying `mart_daily_store_kpis` confirms it was excluded by the `dq_status = 'PASSED'` filter.

### 9. Change History & Late Returns
An order DELIVERED on 3 September and RETURNED on 1 October creates conflicting requirements:
- **ELT:** The MERGE statement updates the order's status to `RETURNED`, reducing September net delivered revenue to reflect current state.
- **ETL:** Closed September fact tables are untouched, preserving the September revenue figure reported at month-end.
- **Recommended Dual View:** Finance requires an immutable accounting ledger that locks closed periods. We recommend implementing an append-only transaction ledger with an `effective_date` or SCD Type 2 tracking, allowing Finance to query closed September numbers while Operations views real-time return rates.

---

## Part F: Observability Experiments

| # | Experiment | What was done | Expected Behavior | Observed Behavior | Where I saw it | What I would change |
| :-: | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | Happy Path | Baseline run of both DAGs | Parallel extraction, clean validation, star schema load | 5,372 rows extracted, 5,218 passed, 154 quarantined across files | Airflow logs & DuckDB table counts | Add execution time SLA alerting |
| **2** | Idempotency | Triggered both DAGs a second time | Fact row counts stay identical; raw tables grow | Fact orders remained at 2,297 (Diff: 0); raw orders grew from 2,572 to 5,144 | `COUNT(*)` in `etl_fact_orders` vs `raw_orders` | Verified idempotent upsert logic |
| **3** | Incremental Load | Applied `orders_2026-10-01_increment.csv` | 150 new orders inserted, 10 returned orders updated | 150 new rows appended; 10 orders updated to RETURNED without duplicate PKs | `stg_orders` row count 2,566; 80 returned in Sep | Automate daily date-partitioned file sensing |
| **4** | Missing File | Renamed `products.csv` to `.bak` | Extraction task retries and alerts | Task failed with `FileNotFoundError: products.csv`; downstream tasks skipped (`upstream_failed`) | Task instance log & Airflow UI red state | Configure Slack alert on retry exhaustion |
| **5** | New Bad Data | Injected 5 defective rows into `orders.csv` | All 5 rows quarantined with exact failure tags | Caught 5 of 5: null customer, invalid status SHIPPED, future 2027 date, qty -3, store S99 | `include/quarantine/nihir/orders_failed_*.csv` | Build automated quarantine alert webhook |
| **6** | Schema Change | Renamed column `order_amount` to `total` | Pipeline fails loudly before corrupting warehouse | Validation task threw `KeyError: 'order_amount'` immediately; execution halted | Airflow task log traceback | Add Great Expectations schema contract check |
| **7** | Volume Anomaly | Triggered pipeline with only 50 orders | Alert on unexpected volume drop (> 50%) | `orders_volume_anomaly_check` logged `WARNING` status in `dq_results` | `dq_results` query where check failed | Add Slack warning webhook for volume drops |
| **8** | Distribution Drift| Multiplied `unit_price` by 100 for store S01 | Outlier or metric shift detected | S01 daily revenue surged 100× from ₹84,604.30 to ₹84,60,430.00 | DuckDB mart query | Add z-score / standard deviation price check |
| **9** | Freshness | Set order timestamps to 30 days prior | Timeliness check flags stale batch | Pipeline flagged batch as stale (> 1 day old) | `dq_results` audit table | Add hard block if batch age > 7 days |
| **10**| Recovery | Fixed root cause and clicked **Clear** | Only failed and downstream tasks re-run | Failed task reset from 'failed' to None; downstream tasks re-executed cleanly | Airflow Graph view task state transitions | Retain automated checkpointing |

### Mapping to the Five Pillars of Data Observability

1. **Freshness:** Monitored via `orders_order_ts_not_future`, SLA next-run scheduler, and incremental date sensing.
2. **Volume:** Monitored via row count audit logs in `pipeline_runs` and the `orders_volume_anomaly_check` (> 50% drop threshold).
3. **Schema:** Monitored via explicit type casting and column validation checks in extraction and staging.
4. **Distribution:** Monitored via numeric boundary checks (`quantity > 0`, `discount_pct [0, 100]`, `delivery_minutes [1, 120]`).
5. **Lineage:** Supported through metadata tagging (`_source_file`, `_row_id`, `_run_id`, `_loaded_at`) across all layers.

- **Strongest Pillar:** **Lineage & Schema.** The combination of metadata tagging in raw tables and strict type casting in staging allows complete traceability of every metric back to exact CSV line numbers.
- **Weakest Pillar:** **Distribution.** While static bounds catch negative prices or extreme delivery times, dynamic distribution drift (e.g. subtle 20% price inflation or category mix shift) is not caught by fixed thresholds and would benefit from automated rolling mean/std-dev profiling.

---

## Part G: Recommendation to the Client (CTO Memo)

**MEMORANDUM**

**TO:** Sameer, Chief Technology Officer, NammaMart Quick Commerce  
**FROM:** Nihir Kumar, Forward Deployed Engineer  
**DATE:** 22 October 2026  
**SUBJECT:** Architectural Recommendation: Production Pipeline Strategy for NammaMart (ETL vs. ELT)  

---

### Executive Summary

Over the past month, we deployed and evaluated both an **ETL** (Extract-Transform-Load via pandas and DuckDB star schema) and an **ELT** (Extract-Load-Transform via DuckDB Raw, Staging, and Mart SQL layers) pipeline operating on identical operational feeds across our 15 Bengaluru dark stores. 

While Finance favors ETL for its strict pre-warehouse quality gate, our operational testing confirms that a **Modern ELT (Medallion Lakehouse) architecture** is the superior choice for NammaMart's rapid scale, multi-department audit requirements, and long-term cost profile.

---

### Comparative Evaluation

| Evaluation Criterion | ETL Pipeline (Pandas + Star Schema) | ELT Pipeline (DuckDB Medallion SQL) | NammaMart Winner |
| :--- | :--- | :--- | :---: |
| **Auditability & Traceability** | Quarantines bad rows externally; historical corrections require manual reprocessing of flat files. | Ingests raw files immutably into `raw_*` with run metadata; every metric is 100% traceable via SQL. | **ELT** |
| **Data Quality Guarantees** | Bad rows never enter warehouse storage; guarantees zero bad records in finance tables. | Bad records land in `raw_*` but are filtered in `stg_*` using `dq_status = 'PASSED'` before reaching marts. | **Tie** |
| **Flexibility & Agility** | Changing business rules requires modifying Python code, testing DAGs, and redeploying Airflow workers. | Adding a new KPI or metric is purely a SQL view update; raw history is preserved to recompute past metrics. | **ELT** |
| **Performance at 1,000× Scale** | High worker memory usage; pandas extractions choke on multi-gigabyte order batches. | Ingestion is lightweight stream-to-storage; DuckDB/Snowflake/BigQuery handles columnar joins efficiently. | **ELT** |
| **Infrastructure Cost** | Demands large Airflow worker instances with high RAM; compute is tied to extraction workers. | Storage is inexpensive; transformations leverage warehouse vectorized engines on demand. | **ELT** |

---

### Detailed Justification

1. **Auditability and Replayability:** In quick commerce, order statuses change dynamically across days (e.g., returns, chargebacks, carrier reconciliations). In ETL, rejected rows live in isolated quarantine CSVs, creating data silos. Under ELT, every record ever received is stored untouched in the raw layer. If an ops manager asks why a September delivery was restated in October, a single SQL query provides the exact timeline.
2. **Speed to Value for Business Teams:** Marketing and Merchandising frequently ask new exploratory questions (e.g., analyzing cancellation reasons by hour). Under ELT, analytics engineers can create new marts in minutes using SQL against existing staging tables without requesting new ingestion pipelines.
3. **Scalability to 100 Dark Stores:** As NammaMart expands beyond Bengaluru to 100+ dark stores and millions of daily orders, running in-memory pandas transformations on Airflow workers will become an operational bottleneck. ELT decouples file ingestion from distributed analytical compute.

---

### Top 3 Implementation Risks & Mitigations

1. **Risk 1: Accidental Reporting of Unvalidated Raw Data.**  
   *Risk:* An analyst connects a BI tool directly to `raw_orders` or queries unvalidated staging records, presenting inflated revenue to executive leadership.  
   *Mitigation:* Implement strict schema-level Role-Based Access Control (RBAC). BI dashboards and business analysts must be granted `SELECT` permissions exclusively on the `mart_*` schema, while `raw_*` is locked to data platform service accounts.
2. **Risk 2: Storage Bloat and Table Scan Degradation.**  
   *Risk:* Append-only raw tables will accumulate millions of records, degrading staging query execution times if full scans occur daily.  
   *Mitigation:* Implement date-based physical partitioning (e.g., `year=YYYY/month=MM/day=DD`) and execute staging transformations incrementally using watermarks (`_loaded_at > current_date - INTERVAL 3 DAYS`).
3. **Risk 3: Financial Restatement Disputes on Late Returns.**  
   *Risk:* Operational status changes (such as 1 October returns on September orders) will alter historical metrics, creating friction with Finance during month-end closing.  
   *Mitigation:* Implement an immutable financial snapshot table (`mart_monthly_financial_close`) that freezes on the 1st of each month at 00:00 IST, alongside dynamic operational marts that reflect real-time order status.

---

## Bonus Features Implemented

1. **Failure & Quality Alerting:** Reusable Slack webhook alerting module (`include/slack_notifier.py`) integrated with Airflow's `Variable.get("SLACK_WEBHOOK_URL")`, providing real-time channel notifications with issue summaries.
2. **SCD Type 2 Customer Dimension:** Created `etl_dim_customer_scd2` tracking customer loyalty tier transitions over time with `valid_from`, `valid_to`, and `is_current` flags.
3. **Store Capacity Quality Check:** Automated store capacity validation checking daily order volume against `capacity_orders_per_day` in `stores.csv` to identify dark store fulfillment strain.
4. **Automated Run & Data Quality Scorecards:** Real-time logging of all pipeline runs in `pipeline_runs` and 34 multi-dimensional quality metrics in `dq_results`.
