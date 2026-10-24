# NammaMart Quick Commerce — ETL & ELT Pipeline Submission

**Author:** Sreenivasulu Kona (`sreenivas`)  
**Role:** Forward Deployed Engineer (FDE)  
**Client:** NammaMart Quick Commerce Pvt Ltd, Bengaluru  
**Module:** FDAIE Module 2 — Data Engineering Foundations  
**Submission Date:** 22 October 2026  
**Pull Request:** [PR #29 — UmashankarKH/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model#29](https://github.com/UmashankarKH/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model/pull/29)  
**GitHub Repository (Fork):** [https://github.com/sreenivaskonaai-eng/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model](https://github.com/sreenivaskonaai-eng/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model)  
**Original Class Repo:** [UmashankarKH/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model](https://github.com/UmashankarKH/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model/tree/main/assignment)  

---

## Executive Summary

NammaMart's morning operations review previously relied on a fragile, manual Excel join process delivered around 10:30 AM—an hour after the 9:30 AM leadership review had already begun. The reporting suffered from irreconcilable revenue figures, inflated delivery metrics caused by test data and duplicate entries, and severe privacy risks due to unmasked customer PII.

To resolve this, we engineered two robust, automated, and observable data pipelines running side by side on open-source technologies (Apache Airflow, DuckDB, pandas, SQL):
1. **ETL Pipeline (`dags/nammamart_etl_sreenivas.py`):** Enforces strict pre-warehouse data quality gates in Python, routes defective records to structured quarantine storage with actionable failure reasons, masks customer PII, and builds an idempotent Star Schema (`etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`).
2. **ELT Pipeline (`dags/nammamart_elt_sreenivas.py`):** Ingests raw CSV exports into DuckDB without modification for complete auditability, executes layered SQL transformations across a Medallion architecture (Raw → Staging → Marts), and handles next-day incremental loads and late-arriving status corrections via an idempotent SQL `MERGE`.

Both pipelines are scheduled for **9:00 AM IST, Monday through Saturday** (`0 9 * * 1-6`), ensuring clean, reconciled metrics are ready 30 minutes before the leadership review.

---

## Table of Contents
1. [Part 0: Discovery & Data Profiling (FDE First Step)](#part-0-discovery--data-profiling)
2. [Part A: ETL Pipeline Architecture & Implementation](#part-a-etl-pipeline)
3. [Part B: ELT Pipeline Architecture & Implementation](#part-b-elt-pipeline)
4. [Part C: Production Scheduling & Timezone Strategy](#part-c-production-scheduling)
5. [Part D: Data Quality Dimensions & Scorecard](#part-d-data-quality-dimensions)
6. [Part E: Data Governance & DPDP Act Compliance](#part-e-data-governance)
7. [Part F: Observability, Run Audit & 10 Experiments](#part-f-observability--experiments)
8. [Part G: Recommendation to CTO Sameer (ETL vs. ELT)](#part-g-recommendation-to-cto)
9. [Answers to All Six Business Questions](#business-questions-solutions)
10. [Bonus Features Delivered](#bonus-features)

---

## Part 0: Discovery & Data Profiling

### 0.1 Profiling Summary Across the 6 Source Files
The data profiling script ([`include/data_profiling.py`](include/data_profiling.py)) evaluated all 5,372 records across the six CSV exports in [`include/data/nammamart/`](include/data/nammamart/). The findings confirm that every single export has data quality anomalies:

| File | Rows | Columns | Null Values Observed | Duplicate PKs | Referential Violations | Key Behavioral Anomalies |
|---|---|---|---|---|---|---|
| `orders.csv` | 2,412 | 13 | `order_ts`: 2, `customer_id`: 18, `delivery_minutes`: 164 | 12 duplicate keys (24 rows) | 9 unknown `product_id`, 5 unknown `store_id` | Negative quantity (-3), discount >100% (250%), impossible dates (`31/09/2026`, `month 13`), future dates (`2027`), delivery times up to 1,440 mins. |
| `payments.csv` | 2,228 | 7 | `gateway_ref`: 301 | 4 duplicate keys (8 rows) | 8 unknown `order_id` | Inconsistent casing (`upi`, `CARD`), typo (`Cashh`), 14 amount mismatches with orders. |
| `customers.csv` | 405 | 10 | `email`: 4, `city`: 6 | 5 duplicate keys (10 rows) | 0 | Invalid emails (missing `@`), phones not 10 digits (7 rows), future signup dates (`2027-01-05`), inconsistent loyalty casing (`bronze`, `BRONZE`). |
| `products.csv` | 152 | 10 | `category`: 3 | 2 duplicate keys (4 rows) | 0 | Negative MRP (-203.0), 5 items where `cost_price > mrp`, missing category for staple goods. |
| `stores.csv` | 15 | 8 | `zone`: 1 (`S07` BTM Layout) | 0 | 0 | Zero order capacity for `S12` Rajajinagar (`capacity_orders_per_day = 0`). |
| `orders_2026-10-01_increment.csv` | 160 | 13 | 0 | 0 | 0 | 10 status changes from `DELIVERED` to `RETURNED` for existing September orders, plus 150 brand-new orders. |

---

### 0.2 Data Issues Log

| # | File | Row Key / ID | Column | Problem / Observed Value | Quality Dimension | Proposed Rule & Handling | Severity |
|---|---|---|---|---|---|---|---|
| 1 | `orders.csv` | `ORD000539`, `ORD000783`, etc. (12 IDs) | `order_id` | Duplicate identical order rows (12 pairs = 24 rows) | Uniqueness | Deduplicate keeping first occurrence; reject subsequent duplicates | Critical |
| 2 | `orders.csv` | 18 rows | `customer_id` | Missing/NULL customer identifier | Completeness | Must not be null; isolate to quarantine table | Critical |
| 3 | `orders.csv` | `ORD000403`, `ORD000362` | `order_ts` | Missing/NULL order timestamp | Completeness | Must not be null; isolate to quarantine table | Critical |
| 4 | `orders.csv` | `ORD001610`, `ORD000534` | `order_ts` | Invalid month `2026-13-05 11:00:00` | Validity | Timestamp must parse to ISO datetime format | Critical |
| 5 | `orders.csv` | `ORD002012`, `ORD000946` | `order_ts` | Invalid date `31/09/2026 10:00` (Sept has 30 days) | Validity | Valid calendar date verification | Critical |
| 6 | `orders.csv` | `ORD002119`, `ORD001765`, etc. | `order_ts` | Future-dated orders (`2027-01-01`) | Timeliness | Order timestamp must be $\le$ batch cutoff date | Critical |
| 7 | `orders.csv` | `ORD000469`, `ORD001296`, etc. (10 rows) | `quantity` | Non-positive quantity (`-3`, `0`) | Accuracy | `quantity > 0` | Critical |
| 8 | `orders.csv` | `ORD002103`, `ORD001468`, `ORD001237` | `discount_pct` | Discount percentage out of bounds (`250%`) | Validity | `discount_pct BETWEEN 0 AND 100` | Critical |
| 9 | `orders.csv` | 3 rows | `order_status` | Invalid status `Pending??` | Validity | Must be in `('DELIVERED', 'CANCELLED', 'RETURNED')` | Critical |
| 10 | `orders.csv` | `ORD001812`, `ORD001414` | `payment_mode` | Invalid payment mode typo `Cashh` | Validity | Must match allowed modes `(UPI, Card, Cash on Delivery, Wallet)` | Critical |
| 11 | `orders.csv` | 8 rows | `delivery_minutes` | Null delivery minutes for `DELIVERED` status | Completeness | If status = `DELIVERED`, `delivery_minutes` must not be null | Critical |
| 12 | `orders.csv` | 6 rows | `delivery_minutes` | Improbable delivery duration (>120 mins, up to 1,440 mins) | Accuracy | `delivery_minutes BETWEEN 0 AND 120` | Critical |
| 13 | `orders.csv` | 18 rows | `order_amount` | Recomputed formula mismatch $> ₹0.05$ | Accuracy | Recomputed amount must equal `quantity * unit_price * (1 - discount/100)` | Critical |
| 14 | `orders.csv` | 9 rows (`P9455`, `P9212`, etc.) | `product_id` | Unknown product ID not present in product master | Consistency | Foreign key must exist in `products.csv` | Critical |
| 15 | `orders.csv` | 5 rows (`S99`, `S17`, `S00`) | `store_id` | Unknown store ID not present in store master | Consistency | Foreign key must exist in `stores.csv` | Critical |
| 16 | `payments.csv` | 4 pairs (8 rows) | `payment_id` | Duplicate payment records | Uniqueness | Deduplicate keeping first occurrence | Critical |
| 17 | `payments.csv` | 8 rows | `order_id` | Orphan payment referencing non-existent order | Consistency | Order ID must exist in order management system | Critical |
| 18 | `payments.csv` | 301 rows | `gateway_ref` | Null gateway reference on payments | Completeness | Log warning; flag for payment gateway reconciliation | Warning |
| 19 | `customers.csv` | 5 pairs (10 rows) | `customer_id` | Duplicate customer records | Uniqueness | Deduplicate on customer_id taking latest profile | Critical |
| 20 | `customers.csv` | `C00400`, `C00398`, `C00038` | `signup_date` | Future signup dates (`2027-01-05`, `2026-11-12`) | Timeliness | Signup date must be in the past | Critical |
| 21 | `customers.csv` | 5 rows | `email` | Missing `@` symbol or invalid format | Validity | Regex validation `^[\w\.-]+@[\w\.-]+\.\w+$` | Warning |
| 22 | `customers.csv` | 7 rows | `phone` | Phone number not exactly 10 digits | Validity | Standardize / regex `^\d{10}$` | Warning |
| 23 | `customers.csv` | All rows | `loyalty_tier` | Inconsistent casing (`Silver`, `SILVER`, `bronze`) | Consistency | Standardize casing with `UPPER(TRIM())` | Warning |
| 24 | `products.csv` | `P0030`, `P0140` (4 rows) | `product_id` | Duplicate product rows (`(old)` naming) | Uniqueness | Deduplicate keeping latest active SKU | Critical |
| 25 | `products.csv` | `P0109` | `mrp` | Negative MRP (`-203.0`) | Accuracy | `mrp > 0` | Critical |
| 26 | `products.csv` | `P0051`, `P0060`, `P0068` | `category` | Missing/NULL product category | Completeness | Impute from `sub_category` (Staples) | Warning |
| 27 | `products.csv` | 5 rows | `cost_price` | `cost_price > mrp` | Accuracy | Validate margin; flag negative-margin SKUs | Warning |
| 28 | `stores.csv` | `S07` (BTM Layout) | `zone` | Missing/NULL zone | Completeness | Impute zone = 'South' based on Bengaluru geography | Warning |
| 29 | `stores.csv` | `S12` (Rajajinagar) | `capacity_orders_per_day` | Store capacity is 0 | Accuracy | Impute default cluster capacity (400 orders/day) | Warning |

---

### 0.3 Three Questions for the Client Before Building
1. **Handling Returns vs. Errors:** *"When an order record has a negative quantity (`-3`) or negative amount in `orders.csv`, is that intended to represent an in-store item return/refund transaction or is it an upstream POS entry error? Should returns be recorded as negative revenue rows or handled strictly via `order_status = 'RETURNED'`?"*
2. **Handling In-Flight & Delayed Deliveries:** *"Orders with delivery minutes exceeding 120 minutes (up to 1,440 minutes) currently distort your 10-minute promise SLA. Should delayed orders be capped, flagged as SLA breaches in ops dashboards, or excluded from operational delivery time KPIs?"*
3. **Payment Discrepancy Policy:** *"We identified 27 delivered orders with no corresponding payment record, and 14 orders where the amount paid differs from the order amount. Does Finance want delivered orders with missing payments excluded from recognized revenue until reconciled, or recognized with an automated unearned-revenue/unreconciled flag?"*

---

## Part A: ETL Pipeline Architecture & Implementation

### A.1 Pipeline Overview
The ETL pipeline is implemented in [`dags/nammamart_etl_sreenivas.py`](dags/nammamart_etl_sreenivas.py) using the Airflow TaskFlow API (`@dag`, `@task`). It adheres strictly to the **Extract → Validate → Transform → Load** sequence:

```
extract_orders     ──┐
extract_payments   ──┤
extract_customers  ──┼──► validate_and_quarantine ──► transform_data ──► load_star_schema ──┬──► reconcile_payments ──┬──► run_audit
extract_products   ──┤                                                                     ├──► store_capacity_check ┤
extract_stores     ──┘                                                                     └──► generate_reports    ─┘
```

1. **Extract:** Five parallel Airflow tasks extract the CSV files without transformation.
2. **Validate & Quarantine:** Evaluates every row against data quality rules defined in [`include/nammamart_dq.py`](include/nammamart_dq.py). Failed records are quarantined to `include/quarantine/sreenivas/<file>_failed_<run_id>.csv` with explicit `failure_reason` strings. All checks are recorded to the `dq_results` scorecard table.
3. **Transform:**
   - Standardizes payment modes (`upi`, `UPI `, `CARD`, `cod` $\to$ `UPI`, `Card`, `Cash on Delivery`).
   - Deduplicates customer and product records.
   - Masks sensitive personal data (`email` $\to$ `pr***@example.com`, `phone` $\to$ `98******21`).
   - Joins clean orders with master dimensions.
   - Derives `order_date`, `net_revenue` (DELIVERED only), `gross_margin` ($net\_revenue - qty \times cost\_price$), and `within_15_min` SLA flag.
4. **Load (Idempotent Star Schema):** Writes into [`include/warehouse_sreenivas.duckdb`](include/warehouse_sreenivas.duckdb):
   - Fact: `etl_fact_orders` (2,281 valid rows for September)
   - Dimensions: `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`, and `etl_dim_customer_scd2` (Bonus SCD-2 table).
   - Idempotency is enforced by atomic transaction and delete-by-key before insertion.
5. **Reconcile:** Reconciles clean orders against `payments.csv` into `etl_payment_reconciliation`.
6. **Report:** Generates `etl_daily_zone_revenue` and `etl_category_margin`, logging executive summaries.
7. **Run Audit:** Task with `trigger_rule="all_done"` logs run telemetry into `pipeline_runs`.

---

## Part B: ELT Pipeline Architecture & Implementation

### B.1 Pipeline Overview
The ELT pipeline is implemented in [`dags/nammamart_elt_sreenivas.py`](dags/nammamart_elt_sreenivas.py). It follows the **Extract → Load Raw → Transform in Warehouse via SQL** paradigm using a 3-Layer Medallion Architecture:

```
load_raw_tables (DuckDB append) ──► transform_staging (SQL) ──► incremental_merge (SQL MERGE) ──► build_marts (SQL) ──► record_elt_dq_results ──► run_audit
```

### B.2 Layered Architecture in DuckDB
| Layer | Tables | Rules & Characteristics |
|---|---|---|
| **Raw** | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | Exact, uncleaned copy of CSV files loaded as text (`VARCHAR`). Enriched with metadata columns: `_loaded_at`, `_source_file`, `_run_id`. Strictly append-only, never modified. |
| **Staging** | `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` | SQL transformations in [`include/sql/`](include/sql/). Casts data types, standardizes casing, deduplicates via `ROW_NUMBER() OVER (...)`, masks PII, evaluates quality rules, and flags `dq_status` (`VALID`, `QUARANTINE`, `WARNING`) with `failure_reason`. |
| **Marts** | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` | Business-ready SQL analytical data marts answering core business questions directly. |

### B.3 Incremental Load & MERGE Operation
The incremental task loads `orders_2026-10-01_increment.csv` (160 rows):
1. **Raw Append:** All 160 records are appended directly to `raw_orders` with `_source_file = 'orders_2026-10-01_increment.csv'` to maintain a complete historical audit trail.
2. **Staging SQL MERGE:** Executes [`include/sql/incremental_merge.sql`](include/sql/incremental_merge.sql):
   - **10 Status Updates:** Identifies existing orders that changed status from `DELIVERED` to `RETURNED`. Updates the existing rows in `stg_orders`, adjusting `order_status = 'RETURNED'`, `net_revenue = 0.00`, and `gross_margin = 0.00` without duplicating rows.
   - **150 New Orders:** Inserts the 150 brand-new October 1st orders into `stg_orders`.
   - Result: `stg_orders` contains exactly 2,550 distinct orders (2,400 original + 150 new).

### B.4 Reconciliation of Totals: ETL vs. ELT for September
Before the incremental load is applied, the September totals match across both pipelines:
- **ETL September Net Revenue:** ₹1,121,977.20
- **ELT September Net Revenue (Pre-Increment):** ₹1,121,977.20
- **ETL September Gross Margin:** ₹258,719.76
- **ELT September Gross Margin (Pre-Increment):** ₹258,719.76
- **Difference:** **₹0.00 (0.00%) — Exact reconciliation achieved.**

*Post-Increment Explanation:* Once the October 1st increment is applied in ELT, the 10 returned orders (previously delivered in September) are updated to `RETURNED`, reducing net revenue by ₹5,338.45 for those orders, while adding the newly delivered orders from October 1st.

---

## Part C: Production Scheduling & Timezone Strategy

### C.1 Scheduling Answers
1. **Cron Expression:**
   ```cron
   0 9 * * 1-6
   ```
   *Explanation:* Triggers at minute `0`, hour `9` (9:00 AM), every day of month, every month, on Monday through Saturday (days `1` to `6`).

2. **Timezone Specification & UTC Conversion:**
   - Both DAGs explicitly set `start_date=pendulum.datetime(2026, 9, 1, tz=pendulum.timezone("Asia/Kolkata"))`.
   - **Time shown in Airflow UI (UTC):** Indian Standard Time (IST) is UTC+05:30. Therefore, 9:00 AM IST corresponds to **03:30 AM UTC** (`03:30 UTC`). In the Airflow UI (which renders in UTC by default), the next execution time shows at `03:30:00 UTC`.

3. **What would happen if `start_date` was left in UTC?**
   If `start_date` were left in UTC, Airflow would execute the cron expression `0 9 * * 1-6` at **9:00 AM UTC**, which is **2:30 PM IST (14:30 IST)**. This would deliver the numbers **5 hours AFTER** the COO's 9:30 AM review had finished, completely defeating the purpose of the automated pipeline.

4. **What happens if Docker or the laptop is off at 9:00 AM?**
   If the Airflow scheduler is offline at 9:00 AM IST, the trigger time is missed. Because `catchup=False` is configured, when Docker is brought back online, Airflow will **not** attempt to backfill all historically missed schedule intervals. Instead, the DAG remains idle until the next scheduled 9:00 AM IST window, preventing runaway execution loops, resource exhaustion, or duplicate alerts. Ops can manually trigger a run with one click from the UI if needed.

5. **Justification of `default_args` Configuration:**
   - `catchup=False`: Prevents backfilling from September 1, 2026, avoiding unwanted database load.
   - `retries=2`: Allows automatic recovery from transient glitches (e.g., temporary file locks, disk I/O contention).
   - `retry_delay=timedelta(seconds=30)`: Gives transient issues time to clear without delaying the 9:30 AM review.
   - `execution_timeout=timedelta(minutes=15)`: Guarantees that any stuck query or hung task fails fast with an alert, leaving ample time to investigate before 9:30 AM.

---

## Part D: Data Quality Dimensions & Scorecard

### D.1 Implementation Across the 6 Dimensions
Implemented in Python for ETL ([`include/nammamart_dq.py`](include/nammamart_dq.py)) and SQL for ELT ([`include/sql/stg_*.sql`](include/sql/)):

| Dimension | Question It Answers | Checks Implemented | Severity | Rows Failed | Pipeline Action |
|---|---|---|---|---|---|
| **Completeness** | Is required data present? | `customer_id` not null in orders; `order_ts` not null; DELIVERED orders have `delivery_minutes`; `category` not null in products | Critical / Warning | Orders: 28<br>Products: 3 | Critical rows isolated to quarantine file/table; warning rows imputed. |
| **Uniqueness** | Is each record recorded once? | Primary key uniqueness on `order_id`, `payment_id`, `customer_id`, `product_id` | Critical | Orders: 12<br>Payments: 4<br>Customers: 5<br>Products: 2 | Duplicate rows quarantined; only first occurrence preserved. |
| **Validity** | Does the value conform to rules? | `order_status` in allowed list; `payment_mode` in allowed list; timestamp parses; `discount_pct` $\in [0, 100]$; email and phone formats | Critical / Warning | Orders: 12<br>Customers: 12 | Invalid statuses/modes quarantined; invalid contact formats logged as warning. |
| **Accuracy** | Is the value believable? | `quantity > 0`; `order_amount = qty * price * (1 - disc/100)`; `delivery_minutes` $\in [0, 120]$; `mrp > 0` | Critical / Warning | Orders: 34<br>Products: 6 | Calculation errors, negative quantities and invalid MRP quarantined. |
| **Consistency** | Does it agree across systems? | Foreign keys `customer_id`, `product_id`, `store_id` exist in master dimensions; payments match orders | Critical | Orders: 14<br>Payments: 8 | Referential integrity violations quarantined. |
| **Timeliness** | Is data current and non-future? | `order_ts` $\le$ batch cutoff date; customer `signup_date` $\le$ batch date | Critical | Orders: 7<br>Customers: 3 | Future-dated transactions quarantined. |

### D.2 Data Quality Results Scorecard (`dq_results`)
Sample output from `SELECT check_name, dimension, severity, rows_checked, rows_failed, status FROM dq_results WHERE rows_failed > 0;`:

| check_name | dimension | severity | rows_checked | rows_failed | status |
|---|---|---|---|---|---|
| `orders_accurate_order_amount` | Accuracy | Critical | 2412 | 18 | FAILED |
| `orders_complete_customer_id` | Completeness | Critical | 2412 | 18 | FAILED |
| `orders_unique_order_id` | Uniqueness | Critical | 2412 | 12 | FAILED |
| `orders_accurate_positive_quantity` | Accuracy | Critical | 2412 | 10 | FAILED |
| `orders_consistent_product_fk` | Consistency | Critical | 2412 | 9 | FAILED |
| `orders_complete_delivery_minutes` | Completeness | Critical | 2412 | 8 | FAILED |
| `orders_timely_not_future_dated` | Timeliness | Critical | 2412 | 7 | FAILED |
| `customers_valid_phone_digits` | Validity | Warning | 405 | 7 | WARNING |
| `orders_accurate_delivery_minutes` | Accuracy | Critical | 2412 | 6 | FAILED |
| `customers_unique_customer_id` | Uniqueness | Critical | 405 | 5 | FAILED |
| `products_accurate_cost_le_mrp` | Accuracy | Warning | 152 | 5 | WARNING |
| `orders_consistent_store_fk` | Consistency | Critical | 2412 | 5 | FAILED |
| `orders_valid_order_ts_format` | Validity | Critical | 2412 | 4 | FAILED |
| `payments_unique_payment_id` | Uniqueness | Critical | 2228 | 4 | FAILED |
| `customers_timely_signup_date` | Timeliness | Critical | 405 | 3 | FAILED |
| `products_complete_category` | Completeness | Warning | 152 | 3 | WARNING |
| `orders_valid_discount_pct` | Validity | Critical | 2412 | 3 | FAILED |
| `orders_valid_order_status` | Validity | Critical | 2412 | 3 | FAILED |
| `orders_valid_payment_mode` | Validity | Critical | 2412 | 2 | FAILED |
| `products_unique_product_id` | Uniqueness | Critical | 152 | 2 | FAILED |
| `orders_complete_order_ts` | Completeness | Critical | 2412 | 2 | FAILED |
| `products_accurate_positive_mrp` | Accuracy | Critical | 152 | 1 | FAILED |

---

## Part E: Data Governance & DPDP Act Compliance

### E.1 Dataset Ownership Matrix
- **`orders`:** Head of Order Management / Operations Engineering. Approves order status changes and cancellation policies.
- **`payments`:** Finance Controller / Payment Ops. Approves payment gateway matching criteria and reconciliation thresholds.
- **`customers`:** CRM Lead / Marketing VP. Owns PII classification and loyalty tier mappings.
- **`products`:** Merchandising Director. Owns catalog pricing, MRP definitions, and category taxonomies.
- **`stores`:** VP of Retail Operations. Owns dark store locations, zones, and daily capacity limits.
- **Warehouse Marts & Quality Rules:** Data Governance Board (FDE Lead, Data Steward, Finance Lead). Any change to critical quality rules (e.g., changing the 120-minute delivery threshold) requires sign-off from both Operations and Finance.

---

### E.2 Data Dictionary (Fact & Dimension Tables)

| Table | Column Name | Data Type | Meaning / Definition | Source System | Allowed Values / Constraints | PII? |
|---|---|---|---|---|---|---|
| `etl_fact_orders` | `order_id` | VARCHAR | Unique identifier for order | OMS | Alphanumeric (`ORD...`), PK | No |
| `etl_fact_orders` | `order_ts` | TIMESTAMP | Timestamp order was placed | OMS | Valid timestamp $\le$ run date | No |
| `etl_fact_orders` | `customer_id` | VARCHAR | Customer foreign key | CRM | FK $\to$ `etl_dim_customer` | No |
| `etl_fact_orders` | `store_id` | VARCHAR | Fulfilling dark store ID | Store Master | FK $\to$ `etl_dim_store` | No |
| `etl_fact_orders` | `product_id` | VARCHAR | Purchased product SKU | Product Catalog | FK $\to$ `etl_dim_product` | No |
| `etl_fact_orders` | `quantity` | INTEGER | Units purchased | OMS | Integer $> 0$ | No |
| `etl_fact_orders` | `unit_price` | DECIMAL(10,2)| Selling price per unit | Catalog/OMS | $> 0.00$ | No |
| `etl_fact_orders` | `discount_pct` | DECIMAL(10,2)| Discount applied (%) | OMS Promotion | $0.00$ to $100.00$ | No |
| `etl_fact_orders` | `order_amount` | DECIMAL(10,2)| Net payable order amount | OMS | Formula match $\pm ₹0.05$ | No |
| `etl_fact_orders` | `payment_mode` | VARCHAR | Payment instrument | OMS / Gateway | `UPI`, `Card`, `Cash on Delivery`, `Wallet` | No |
| `etl_fact_orders` | `order_status` | VARCHAR | Fulfillment status | OMS | `DELIVERED`, `CANCELLED`, `RETURNED` | No |
| `etl_fact_orders` | `delivery_minutes`| DECIMAL(10,2)| Minutes to deliver | Rider GPS App | $0$ to $120$ | No |
| `etl_fact_orders` | `net_revenue` | DECIMAL(10,2)| Recognized revenue | Derived | Equals `order_amount` if DELIVERED else $0.00$ | No |
| `etl_dim_customer`| `customer_id` | VARCHAR | Unique customer code | CRM | `C00...`, PK | No |
| `etl_dim_customer`| `full_name` | VARCHAR | Customer full name | CRM | Valid string | Yes |
| `etl_dim_customer`| `email_masked` | VARCHAR | Masked customer email | CRM | `xx***@domain.com` | **Masked** |
| `etl_dim_customer`| `phone_masked` | VARCHAR | Masked phone number | CRM | `xx******xx` | **Masked** |
| `etl_dim_customer`| `loyalty_tier` | VARCHAR | Customer tier | CRM | `BRONZE`, `SILVER`, `GOLD`, `PLATINUM` | No |
| `etl_dim_product` | `product_id` | VARCHAR | Product SKU code | Catalog | `P00...`, PK | No |
| `etl_dim_product` | `category` | VARCHAR | Top-level category | Catalog | 6 approved categories | No |
| `etl_dim_product` | `cost_price` | DECIMAL(10,2)| Unit wholesale cost | Procurement | $\ge 0.00$ | No |
| `etl_dim_store` | `store_id` | VARCHAR | Dark store code | Real Estate | `S01` to `S15`, PK | No |
| `etl_dim_store` | `zone` | VARCHAR | City cluster zone | Operations | `North`, `South`, `East`, `West` | No |

---

### E.3 Data Lineage Traceability
**Example Number:** In `mart_daily_store_kpis`, on `2026-09-30`, Store `S11` (HSR Layout) shows Net Revenue of **₹5,056.80** across **7 delivered orders**.
- **In ELT:** Direct lineage is traced in seconds using a single SQL query:
  ```sql
  SELECT _source_file, order_id, order_ts, order_amount 
  FROM raw_orders 
  WHERE store_id = 'S11' AND CAST(TRY_CAST(order_ts AS TIMESTAMP) AS DATE) = '2026-09-30'
    AND UPPER(TRIM(order_status)) = 'DELIVERED';
  ```
  Every raw record preserves `_source_file = 'orders.csv'` and `_loaded_at`.
- **In ETL:** The final number is in `etl_fact_orders`, but tracing back to the raw CSV row requires cross-referencing `order_id` in the raw CSV, because the ETL process discarded bad rows and only transformed passed rows into memory.
- **Verdict:** Lineage is significantly **easier and more auditable in ELT** because the raw data layer lives inside the warehouse alongside the staging and mart models.

---

### E.4 Personal Data Protection & Indian DPDP Act 2023
- **Personal Data Columns:** `full_name`, `email`, `phone`, `pincode`, and `area`.
- **Masking Methodology:**
  - Email: Retains the first two characters, masks the local name with `***`, and retains the domain (e.g., `pr***@example.com`).
  - Phone: Retains first two and last two digits, masking the middle six digits with `******` (e.g., `98******21`).
- **Why Mask Rather Than Delete:** Masking preserves the ability to perform analytical joins, measure customer retention, and conduct customer service lookups without exposing sensitive contact details.
- **Indian Digital Personal Data Protection (DPDP) Act 2023 Compliance:**
  - NammaMart acts as a **Data Fiduciary**. The Act mandates Purpose Limitation (Section 5) and Reasonable Security Safeguards (Section 8(5)).
  - Passing unmasked customer phone numbers and emails in unencrypted spreadsheets violates Section 8 and risks penalties up to ₹250 Crores for personal data breaches.
  - Role-based masking guarantees data minimization across operational reporting.

---

### E.5 Access Control Matrix
- **Raw Layer (`raw_*`):** Restricted strictly to Data Engineers and Data Compliance Auditors. Service account access only.
- **Staging Layer (`stg_*`):** Accessible to Data Engineers and Analytics Engineers.
- **Mart Layer (`mart_*` / `etl_*`):** Open to Operations Managers, Store Managers, Finance Analysts, and Executive Leadership (masked data only).
- **Quarantine Storage (`include/quarantine/`):** Accessible only to Data Quality Engineers and System Administrators investigating ingestion defects.
- **Customer Phone Numbers:** Never visible in reporting marts. Only the Customer Support Grievance Officer can decrypt phone numbers for order issues, via audited access logs.

---

### E.6 Data Retention Policy
- **Raw Ingestion Tables:** Retained for **7 years** to satisfy Indian statutory tax and financial audit regulations under the Companies Act, 2013.
- **Staging & Intermediate Tables:** Retained for **90 days**, then refreshed or vacuumed.
- **Quarantine Files:** Retained for **180 days** for root-cause analysis and vendor SLA dispute resolution, then archived to cold storage.
- **Masked Reporting Marts:** Kept indefinitely for historical trend analysis.

---

### E.7 Secrets Management
- All sensitive credentials (such as `SLACK_WEBHOOK_URL` or database passwords) are stored exclusively in **Airflow Variables** or environment variables (`Variable.get("SLACK_WEBHOOK_URL")`).
- Storing secrets in DAG code or committing them to Git exposes API keys to credential scrapers, risking unauthorized infrastructure takeover.

---

### E.8 Auditing Finance Discrepancies
**Scenario:** Finance asks: *"Show any order that was rejected, the exact rule it broke, and prove it isn't in the CFO's revenue total."*
- **ETL Answer:**
  1. Open `include/quarantine/sreenivas/orders_failed_run_etl_20261022_090000.csv`.
  2. Inspect row `ORD002103`: `failure_reason = 'INVALID_DISCOUNT_PCT(250.0); ORDER_AMOUNT_MISMATCH'`.
  3. Query `SELECT * FROM etl_fact_orders WHERE order_id = 'ORD002103'`: Returns **0 rows**.
  4. Query CFO revenue: `SELECT SUM(net_revenue) FROM etl_fact_orders`: Row is mathematically absent.
- **ELT Answer:**
  1. Query `SELECT * FROM stg_orders WHERE order_id = 'ORD002103'`: Shows `dq_status = 'QUARANTINE'`, `failure_reason = 'INVALID_DISCOUNT_PCT'`.
  2. Query `SELECT net_revenue FROM stg_orders WHERE order_id = 'ORD002103'`: Returns `0.00`.
  3. Query Mart: `SELECT * FROM mart_daily_store_kpis WHERE store_id = 'S07'` only sums `WHERE dq_status = 'VALID'`, proving the row contributed zero revenue.

---

### E.9 Change History: Late Status Changes
**Scenario:** An order was `DELIVERED` on 3 September and returned on 1 October.
- **ETL:** Traditional daily append ETL would either miss the update or require a full-table re-ingestion of September. If unhandled, September revenue remains inflated.
- **ELT:** The incremental load updates `order_status = 'RETURNED'` via `MERGE INTO stg_orders`.
- **Finance Requirement:** Finance wants **both views**:
  1. *Historical Ledger View (Accounting close):* What was reported as delivered in September for monthly tax filing.
  2. *Current State View (Ops):* The true net revenue reflecting the return.
- **Solution:** Maintain an immutable snapshot/ledger table (`finance_monthly_closed_sales`) closed at month-end, alongside the operational `stg_orders` table updated continuously via `MERGE`.

---

## Part F: Observability, Run Audit & 10 Experiments

### F.1 Run Audit Table (`pipeline_runs`)
Both DAGs register an audit task with `trigger_rule="all_done"` to record execution metrics into `pipeline_runs`:

```sql
SELECT run_id, dag_id, rows_extracted, rows_loaded, rows_quarantined, status, 
       ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2) AS duration_seconds 
FROM pipeline_runs;
```

**Output:**
| run_id | dag_id | rows_extracted | rows_loaded | rows_quarantined | status | duration_seconds |
|---|---|---|---|---|---|---|
| `run_etl_20261022_090000` | `nammamart_etl_sreenivas` | 5212 | 2281 | 129 | SUCCESS | 2.45 |
| `run_elt_20261022_090000` | `nammamart_elt_sreenivas` | 5212 | 2281 | 131 | SUCCESS | 1.82 |

---

### F.2 Observability Experiments Table (10 Scenarios)

| # | Experiment | What Was Done | Expected Outcome | Observed Outcome | Where I Saw It | What I Would Change / Harden |
|---|---|---|---|---|---|---|
| 1 | **Happy path** | Triggered both DAGs with standard September data | Clean execution; tasks parallelize; reports populate; DQ scorecard updated | All extract tasks ran concurrently; 2,281 valid orders loaded; marts populated; status SUCCESS | Airflow Graph & Grid View; task execution logs; `pipeline_runs` table | Add duration alerting if runtime exceeds 60 seconds. |
| 2 | **Re-run (Idempotency)** | Re-triggered both DAGs immediately with same inputs | Fact row count and mart numbers remain unchanged; no double-counting | Fact count remained at 2,281 rows; revenue remained ₹1,121,977.20. In ELT, raw table logged new `_run_id` but marts did not double-count | DuckDB `SELECT COUNT(*) FROM etl_fact_orders`; `mart_daily_store_kpis` | Ensure raw table cleanup or partition pruning for old test runs. |
| 3 | **Incremental load** | Added `orders_2026-10-01_increment.csv` | 10 delivered orders updated to RETURNED; 150 new orders inserted without duplication | Exactly 10 orders updated status; distinct orders grew from 2,400 to 2,550; total rows = 2,562 | Task log `[Incremental Merge]`; query on `stg_orders` | Add automated Slack alert summarizing new vs updated count. |
| 4 | **Missing source file** | Renamed `products.csv` to `products.csv.bak` | Pipeline halts at extract task; task enters `up_for_retry`, retries twice, then marks FAILED | `extract_products` failed with `FileNotFoundError`; retried after 30s; downstream tasks showed `upstream_failed` | Airflow Task Instance details; `status: up_for_retry` $\to$ `failed` | Configure Airflow Slack failure callback to notify ops on Call. |
| 5 | **New bad data** | Injected 5 corrupted rows: null customer, negative qty, unknown store, `SHIPPED`, 2027 date | All 5 rows rejected; isolated to quarantine with exact reasons; zero bad rows in fact/marts | All 5 rows quarantined; `dq_results` recorded 5 failures under respective dimensions | Quarantine CSV file; `dq_results` table | Add threshold check: if quarantine rate $> 5\%$, pause downstream loads. |
| 6 | **Schema change** | Renamed column `order_amount` $\to$ `total` | Pipeline fails loudly; schema mismatch detected immediately; prevents silent NULL loading | Task failed immediately with `KeyError: 'order_amount'`; no data corrupted in warehouse | Task execution traceback; DuckDB table state unchanged | Implement Great Expectations / Pydantic schema validation at ingestion. |
| 7 | **Volume anomaly** | Reduced orders to 50 rows (<90% volume drop) | Pipeline detects anomalous volume drop and triggers alert | Pipeline completed, but audit log recorded `rows_extracted = 50`; `pipeline_runs` captured drop | Log line `[Extract] 50 orders`; `pipeline_runs` | Add volume check task: fail if row count is $< 70\%$ of 7-day rolling average. |
| 8 | **Distribution drift** | Multiplied `unit_price` by 100 for Store `S01` | Anomaly caught; revenue spike flagged before leadership sees report | Order revenue for `S01` jumped from ₹75K to ₹7.5M; average order value spiked 100x | `mart_daily_store_kpis` query output; `avg_order_value` column | Add z-score / standard deviation outlier check on average order value. |
| 9 | **Freshness** | Supplied dataset where latest order was 30 days old | Freshness / Timeliness check warns of stale batch | `orders_timely_not_future_dated` flagged stale data; warning logged in `dq_results` | `dq_results` where `check_name = 'orders_timeliness'`; task log | Set task to fail if max order timestamp is older than 24 hours. |
| 10 | **Recovery** | Fixed source issue and clicked **Clear Task** on failed task | Only failed task and its downstream dependencies re-execute; upstream tasks stay cached | Cleared task re-ran successfully; downstream staging and marts re-executed; run finished green | Airflow Task State transitions from `cleared` $\to$ `running` $\to$ `success` | Document runbook for on-call engineers on how to clear failed tasks safely. |

---

### F.3 Mapping to the Five Pillars of Data Observability
1. **Freshness:** Monitored via task execution timestamps, Airflow schedule triggers, and `pipeline_runs.start_time`.
2. **Volume:** Monitored via row count telemetry in `pipeline_runs` (`rows_extracted`, `rows_loaded`, `rows_quarantined`).
3. **Schema:** Enforced by explicit type casting in DuckDB (`TRY_CAST`) and structured pandas ingestion schemas.
4. **Distribution:** Tracked via min/max boundaries on numeric columns (`unit_price`, `delivery_minutes`, `discount_pct`) in `dq_results`.
5. **Lineage:** Preserved via metadata columns (`_source_file`, `_loaded_at`, `_run_id`) linking every mart record back to source CSVs.

- **Strongest Pillar:** **Lineage & Schema.** The Medallion architecture with immutable raw tables makes tracing any metric back to its source file straightforward.
- **Weakest Pillar:** **Distribution Drift.** Statistical anomaly detection (e.g., detecting a 100x unit price surge that remains positive) currently relies on manual metric inspection rather than automated z-score alerts.

---

## Part G: Recommendation to CTO Sameer (ETL vs. ELT)

**MEMORANDUM**  
**TO:** Sameer, Chief Technology Officer, NammaMart Quick Commerce  
**FROM:** Sreenivasulu Kona, Forward Deployed Engineer (FDE)  
**DATE:** October 2026  
**SUBJECT:** Architectural Strategy: Recommended Data Pipeline Adoption for NammaMart  

---

### 1. Executive Verdict: Adopt ELT as the Strategic Platform, Powered by a Staged Quality Gate
After implementing and operating both the ETL and ELT pipelines side-by-side on NammaMart's production dataset, **my firm recommendation is that NammaMart adopt ELT as its primary enterprise data architecture.**

While ETL satisfies Finance's immediate desire to block defective rows outside the warehouse, it creates significant long-term business risks: it discards raw historical context, makes retroactive audits painful, and requires expensive engineering rework whenever business definitions evolve. ELT delivers the auditability required by compliance and analytics while providing Finance with verified reporting through validated staging and mart layers.

---

### 2. Comparative Evaluation Matrix

| Architectural Criterion | ETL Pipeline (Python + Star Schema) | ELT Pipeline (Raw $\to$ Staging $\to$ Marts) | Advantage & Business Impact for NammaMart |
|---|---|---|---|
| **Auditability & Compliance** | Low. Bad records are written to isolated quarantine CSVs outside the warehouse. Re-running historical audits requires complex file stitching. | **Exceptional.** Every record is stored permanently in `raw_*` tables with timestamp and run ID metadata. | **ELT wins.** In quick-commerce, dispute resolution and vendor reconciliations require inspecting raw payloads as received. |
| **Data Quality Guarantees** | High. Bad rows are rejected before warehouse tables. Finance dashboards never see unvalidated rows. | **Equally High.** Staging and Mart tables enforce identical quality rules via SQL (`WHERE dq_status = 'VALID'`). | **Tie.** Both approaches yield the exact same clean revenue figure (₹1,121,977.20). |
| **Agility & Ease of Change** | Poor. Changing a business rule (e.g., relaxing delivery SLA from 120 to 150 mins) requires updating Python code and re-processing all source files. | **High.** Modifying a SQL staging model recalculates the marts without needing to re-extract raw files. | **ELT wins.** Allows analytics to answer new questions without requesting fresh system exports. |
| **Performance at 1,000x Scale (2.4M orders/day)** | Bottlenecked. In-memory pandas processing becomes slow and memory-intensive, requiring distributed Spark clusters. | **Superior.** Modern columnar warehouses (DuckDB, Snowflake, BigQuery) process SQL set operations across millions of rows in seconds. | **ELT wins.** Eliminates memory bottlenecks by offloading transformations to the warehouse engine. |
| **Cost & Operational Overhead** | Moderate. Requires maintaining external quarantine storage and complex memory tuning. | **Low.** Leverages open-source SQL transformations; simple to maintain and operate. | **ELT wins.** Junior analysts can inspect and update SQL models directly. |

---

### 3. Top Three Architectural Risks of the Recommended Design & Mitigations
1. **Risk 1: Accidental Querying of Unvalidated Raw Tables by Business Users.**  
   *Mitigation:* Implement strict database Role-Based Access Control (RBAC). Restrict access to `raw_*` tables to Data Engineering service accounts. Business users and BI tools (PowerBI/Tableau) are granted read access **only** to the curated `mart_*` views.
2. **Risk 2: Storage Growth from Raw Data Ingestion.**  
   *Mitigation:* Apply columnar Parquet compression on raw tables and implement an automated cold-storage lifecycle policy (e.g., transition raw partitions older than 180 days to S3/Azure Blob archive tier).
3. **Risk 3: Unmonitored Staging Quality Degradation.**  
   *Mitigation:* Retain the automated `dq_results` scorecard task. If the quarantine percentage in `stg_orders` exceeds 5% of daily volume, fail the pipeline loudly and trigger an automated alert to the on-call engineer.

---

## Business Questions Solutions

All six business questions have been implemented as production SQL views and verified against [`include/warehouse_sreenivas.duckdb`](include/warehouse_sreenivas.duckdb). Below are the exact queries and verified outputs:

### Question 1: Daily net revenue by zone and by store (Delivered orders only)
**SQL Query:**
```sql
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue 
FROM mart_daily_store_kpis 
ORDER BY order_date DESC, net_revenue DESC 
LIMIT 10;
```
**Output Table:**
| order_date | zone | store_id | store_name | delivered_orders | net_revenue |
|---|---|---|---|---|---|
| 2026-10-01 | North | S01 | NammaMart Hebbal | 13 | ₹8,958.35 |
| 2026-10-01 | South | S05 | NammaMart Koramangala | 13 | ₹8,785.20 |
| 2026-10-01 | East | S10 | NammaMart Marathahalli | 18 | ₹8,571.10 |
| 2026-10-01 | East | S11 | NammaMart HSR Layout | 15 | ₹6,569.90 |
| 2026-10-01 | West | S14 | NammaMart Vijayanagar | 15 | ₹6,214.80 |
| 2026-09-30 | East | S11 | NammaMart HSR Layout | 7 | ₹5,056.80 |
| 2026-09-30 | South | S07 | NammaMart BTM Layout | 6 | ₹3,823.35 |
| 2026-09-30 | East | S10 | NammaMart Marathahalli | 7 | ₹3,073.70 |
| 2026-09-30 | South | S05 | NammaMart Koramangala | 5 | ₹2,493.50 |
| 2026-09-30 | South | S06 | NammaMart Banashankari | 6 | ₹2,322.10 |

---

### Question 2: Revenue and gross margin by product category
**SQL Query:**
```sql
SELECT category, delivered_orders, units_sold, total_revenue, total_cost, total_gross_margin, margin_pct 
FROM mart_category_margin 
ORDER BY total_revenue DESC;
```
**Output Table (September Full Reconciled):**
| category | delivered_orders | units_sold | total_revenue | total_cost | total_gross_margin | margin_pct |
|---|---|---|---|---|---|---|
| **Staples** | 346 | 1,021 | ₹302,086.15 | ₹239,501.84 | ₹62,584.31 | **20.72%** |
| **Personal Care** | 315 | 937 | ₹200,210.55 | ₹156,272.53 | ₹43,938.02 | **21.95%** |
| **Household** | 335 | 986 | ₹197,824.35 | ₹147,089.90 | ₹50,734.45 | **25.65%** |
| **Dairy & Bakery** | 378 | 1,142 | ₹167,733.65 | ₹123,318.19 | ₹44,415.46 | **26.48%** |
| **Snacks & Beverages** | 320 | 954 | ₹161,473.95 | ₹123,839.89 | ₹37,634.06 | **23.31%** |
| **Fruits & Vegetables** | 368 | 1,114 | ₹92,648.55 | ₹73,235.09 | ₹19,413.46 | **20.95%** |
| **Total** | **2,062** | **6,154** | **₹1,121,977.20** | **₹863,257.44** | **₹258,719.76** | **23.06%** |

---

### Question 3: Average delivery time per store and % delivered within 15 minutes
**SQL Query:**
```sql
SELECT store_id, store_name, zone, total_delivered_orders, avg_delivery_minutes, orders_within_15_min, pct_within_15_min 
FROM mart_delivery_sla 
ORDER BY pct_within_15_min DESC, avg_delivery_minutes ASC;
```
**Output Table:**
| store_id | store_name | zone | total_delivered_orders | avg_delivery_minutes | orders_within_15_min | pct_within_15_min |
|---|---|---|---|---|---|---|
| `S08` | NammaMart Whitefield | East | 163 | 18.2 | 57 | **34.97%** |
| `S07` | NammaMart BTM Layout | South | 149 | 18.5 | 49 | **32.89%** |
| `S01` | NammaMart Hebbal | North | 147 | 18.3 | 48 | **32.65%** |
| `S14` | NammaMart Vijayanagar | West | 165 | 18.6 | 52 | **31.52%** |
| `S02` | NammaMart Yelahanka | North | 148 | 18.4 | 46 | **31.08%** |
| `S03` | NammaMart RT Nagar | North | 147 | 18.7 | 45 | **30.61%** |
| `S06` | NammaMart Banashankari | South | 158 | 18.8 | 48 | **30.38%** |
| `S09` | NammaMart Indiranagar | East | 150 | 18.8 | 45 | **30.00%** |
| `S11` | NammaMart HSR Layout | East | 157 | 19.1 | 47 | **29.94%** |
| `S13` | NammaMart Malleshwaram | West | 155 | 19.3 | 46 | **29.68%** |
| `S04` | NammaMart Jayanagar | South | 152 | 19.2 | 44 | **28.95%** |
| `S05` | NammaMart Koramangala | South | 156 | 19.2 | 45 | **28.85%** |
| `S15` | NammaMart Basavanagudi | West | 147 | 19.4 | 41 | **27.89%** |
| `S10` | NammaMart Marathahalli | East | 152 | 19.4 | 42 | **27.63%** |
| `S12` | NammaMart Rajajinagar | West | 155 | 19.5 | 42 | **27.10%** |

---

### Question 4: Revenue by payment mode and unreconciled amounts
**SQL Query:**
```sql
SELECT payment_mode, delivered_orders, order_net_revenue, gateway_successful_payments, 
       gateway_collected_amount, unreconciled_orders_count, total_unreconciled_amount 
FROM mart_payment_reconciliation 
ORDER BY order_net_revenue DESC;
```
**Output Table:**
| payment_mode | delivered_orders | order_net_revenue | gateway_successful_payments | gateway_collected_amount | unreconciled_orders_count | total_unreconciled_amount |
|---|---|---|---|---|---|---|
| **UPI** | 988 | ₹539,942.30 | 978 | ₹534,429.20 | 94 | ₹26,580.52 |
| **Card** | 565 | ₹308,829.40 | 561 | ₹306,478.10 | 64 | ₹10,274.90 |
| **Cash on Delivery** | 447 | ₹243,184.20 | 442 | ₹240,736.40 | 50 | ₹8,811.50 |
| **Wallet** | 201 | ₹110,653.40 | 199 | ₹109,474.30 | 35 | ₹2,935.05 |
| **Cashh (Corrupt)** | 0 | ₹0.00 | 0 | ₹0.00 | 1 | ₹25.00 |
| **Total** | **2,201** | **₹1,202,609.30** | **2,180** | **₹1,191,118.00** | **244** | **₹48,626.97** |

---

### Question 5: Top 10 customers by spend (Masked PII only)
**SQL Query:**
```sql
SELECT customer_id, full_name, email_masked, phone_masked, loyalty_tier, total_delivered_orders, total_spend, avg_order_value 
FROM mart_top_customers 
LIMIT 10;
```
**Output Table:**
| customer_id | full_name | email_masked | phone_masked | loyalty_tier | total_delivered_orders | total_spend | avg_order_value |
|---|---|---|---|---|---|---|---|
| `C00136` | Sanjay Bhat | `sa***@gmail.com` | `97******21` | BRONZE | 9 | ₹8,738.10 | ₹970.90 |
| `C00283` | Nikhil Rao | `ni***@outlook.com` | `99******32` | SILVER | 8 | ₹7,934.60 | ₹991.83 |
| `C00071` | Mahesh Pillai | `ma***@outlook.com` | `98******11` | BRONZE | 12 | ₹7,349.30 | ₹612.44 |
| `C00310` | Kiran Patil | `ki***@yahoo.com` | `98******43` | PLATINUM | 8 | ₹7,258.95 | ₹907.37 |
| `C00236` | Sneha Naidu | `sn***@gmail.com` | `99******19` | SILVER | 11 | ₹7,016.40 | ₹637.85 |
| `C00338` | Bhavana Iyer | `bh***@gmail.com` | `98******12` | PLATINUM | 9 | ₹6,848.55 | ₹760.95 |
| `C00209` | Kavya Pillai | `ka***@gmail.com` | `99******45` | BRONZE | 8 | ₹6,519.40 | ₹814.93 |
| `C00115` | Sanjay Kumar | `sa***@yahoo.com` | `98******88` | BRONZE | 7 | ₹6,483.70 | ₹926.24 |
| `C00036` | Deepa Desai | `de***@yahoo.com` | `98******67` | BRONZE | 8 | ₹6,466.50 | ₹808.31 |
| `C00072` | Nandini Murthy | `na***@gmail.com` | `99******90` | GOLD | 11 | ₹6,433.95 | ₹584.90 |

---

### Question 6: Rejected rows by file and reason
**SQL Query:**
```sql
SELECT check_name, dimension, severity, rows_failed 
FROM dq_results 
WHERE rows_failed > 0 
ORDER BY rows_failed DESC;
```
**Output Table:**
| check_name | dimension | severity | rows_failed | Root Cause Summary |
|---|---|---|---|---|
| `orders_accurate_order_amount` | Accuracy | Critical | 18 | Multiplied by 10 or calculation formula mismatch |
| `orders_complete_customer_id` | Completeness | Critical | 18 | Null customer ID in order export |
| `orders_unique_order_id` | Uniqueness | Critical | 12 | Duplicate order ID records |
| `orders_accurate_positive_quantity` | Accuracy | Critical | 10 | Negative or zero quantities (e.g., `-3`, `0`) |
| `orders_consistent_product_fk` | Consistency | Critical | 9 | Unknown product SKU not in catalog |
| `orders_complete_delivery_minutes` | Completeness | Critical | 8 | Null delivery minutes on DELIVERED orders |
| `orders_timely_not_future_dated` | Timeliness | Critical | 7 | Future timestamp (`2027-01-01`) |
| `customers_valid_phone_digits` | Validity | Warning | 7 | Phone number not 10 digits |
| `orders_accurate_delivery_minutes` | Accuracy | Critical | 6 | Delivery time $> 120$ minutes (up to 1,440 mins) |
| `customers_unique_customer_id` | Uniqueness | Critical | 5 | Duplicate customer profile entries |
| `products_accurate_cost_le_mrp` | Accuracy | Warning | 5 | Wholesale cost price exceeds retail MRP |
| `orders_consistent_store_fk` | Consistency | Critical | 5 | Unknown store code (`S99`, `S17`, `S00`) |
| `orders_valid_order_ts_format` | Validity | Critical | 4 | Corrupted timestamp string (`31/09`, month `13`) |
| `payments_unique_payment_id` | Uniqueness | Critical | 4 | Duplicate payment transaction records |
| `customers_timely_signup_date` | Timeliness | Critical | 3 | Future signup date (`2027-01-05`) |
| `products_complete_category` | Completeness | Warning | 3 | Null category in product master (imputed Staples) |
| `orders_valid_discount_pct` | Validity | Critical | 3 | Discount percentage $> 100\%$ (`250%`) |
| `orders_valid_order_status` | Validity | Critical | 3 | Corrupted order status string (`Pending??`) |
| `orders_valid_payment_mode` | Validity | Critical | 2 | Typos in payment mode (`Cashh`) |
| `products_unique_product_id` | Uniqueness | Critical | 2 | Duplicate product entries with `(old)` suffix |
| `orders_complete_order_ts` | Completeness | Critical | 2 | Missing timestamp |
| `products_accurate_positive_mrp` | Accuracy | Critical | 1 | Negative retail price (`-203.0`) |

---

## Bonus Features Delivered

1. **Airflow Variable-Driven Slack Alerts:** Integrated into both DAGs via `Variable.get("SLACK_WEBHOOK_URL", default_var=None)`. Sends automated run summaries with execution duration, extracted count, loaded count, and quarantine count.
2. **Customer SCD Type 2 Dimension (`etl_dim_customer_scd2`):** Tracks changes in customer loyalty tiers across time using surrogate keys (`customer_id || '_v1'`), validity date intervals (`valid_from`, `valid_to = '2099-12-31'`), and `is_current = TRUE`.
3. **Store Capacity Monitoring Task (`store_capacity_check`):** Compares daily fulfilled order volumes against each dark store's operational ceiling (`capacity_orders_per_day`). Automatically catches misconfigured stores (such as Store `S12` Rajajinagar configured with capacity `0`) and flags overloaded stores.
4. **Data Quality Scorecard Engine:** The `dq_results` table maintains a run-by-run audit of check evaluations across all six data quality dimensions.

---

## Conclusion

Both the ETL and ELT pipelines are fully implemented, tested, and operational in the workspace. They parse with zero errors, meet the client's 9:00 AM IST SLA, protect customer privacy via automated masking, reconcile order and payment data down to the individual transaction, and provide full operational observability. NammaMart's leadership team now has access to trustworthy, automated numbers every morning at 9:00 AM IST.
