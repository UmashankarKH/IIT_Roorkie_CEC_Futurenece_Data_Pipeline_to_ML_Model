# NammaMart Quick Commerce — ETL & ELT Pipeline Assignment Submission

**Candidate:** Akashbabu G J  
**Cohort:** Cohort-5-M2 Data & Pipeline Thinking  
**Date:** 22 October 2026  
**Repository:** IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model  

---

## Part 0: Discovery & Data Profiling (FDE First Step)

### 1. File Profiling Summary

| File | Total Rows | Primary Key | Key Observations & Integrity Findings |
| :--- | :--- | :--- | :--- |
| `orders.csv` | 2,412 | `order_id` | Contains non-standard status values, negative quantities, missing foreign keys, and invalid timestamp strings (e.g. month `13`). |
| `payments.csv` | 2,228 | `payment_id` | Missing payment records for some delivered orders; mismatches between `amount_paid` and `order_amount`. |
| `customers.csv` | 405 | `customer_id` | Contains unmasked PII (`email`, `phone`), duplicate customer IDs, and non-uniform case in `loyalty_tier`. |
| `products.csv` | 152 | `product_id` | Rows where `cost_price > mrp` (negative margins) and duplicate product IDs. |
| `stores.csv` | 15 | `store_id` | Clean master dimension with 15 dark stores across 4 zones (North, South, East, West). |
| `orders_2026-10-01_increment.csv` | 160 | `order_id` | 150 new orders and 10 status corrections (e.g. status changed from `DELIVERED` to `RETURNED`). |

### 2. Data Issues Log

| # | File | Row Key / ID | Column | Problem Observed | Quality Dimension | Proposed Rule & Handling | Severity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `orders.csv` | ORD_1021 | `customer_id` | Unmatched foreign key | Consistency | Foreign key must exist in `customers.csv` dimension | Critical (Quarantined) |
| 2 | `orders.csv` | ORD_1402 | `quantity` | Negative / 0 value | Accuracy | `quantity > 0` | Critical (Quarantined) |
| 3 | `orders.csv` | ORD_2190 | `order_status` | Trailing spaces / invalid value | Validity | Must be in `('DELIVERED', 'CANCELLED', 'RETURNED')` | Critical (Quarantined) |
| 4 | `orders.csv` | ORD_0894 | `order_ts` | Malformed date (`2026-13-05`) | Validity / Timeliness | Must parse as valid ISO timestamp | Critical (Quarantined) |
| 5 | `payments.csv` | PAY_0814 | `amount_paid` | Amount paid != Order amount | Consistency | For `SUCCESS`, payment amount must equal `order_amount` | Critical (Recon mismatch) |
| 6 | `customers.csv` | CUST_008 | `phone` | Non-10-digit phone number | Validity | Phone should be 10 digits | Warning (Logged & masked) |
| 7 | `customers.csv` | CUST_045 | `loyalty_tier` | Mixed casing (`gold`, `Gold`) | Validity | Standardise to `UPPER(TRIM(loyalty_tier))` | Warning (Standardised) |
| 8 | `products.csv` | PROD_044 | `cost_price` | `cost_price > mrp` | Accuracy | `cost_price <= mrp` | Critical (Quarantined) |

### 3. Three Clarification Questions for Client (CTO / Ops)
1. **Discrepancy Resolution:** When payment status is `SUCCESS` but the payment gateway amount differs by a fractional amount from `order_amount`, should the order be halted or fulfilled with a balance debit note?
2. **Negative Quantities:** Are negative quantity records intended to represent partial customer returns, or are they upstream transaction logging errors?
3. **Historical Revenue Adjustments:** When an order delivered in September is updated to `RETURNED` in October's incremental batch, does Finance want September's closed books restated, or an October accounting reversal transaction?

---

## Part A & B: Architecture & Pipeline Design

### 1. ETL Pipeline (`nammamart_etl_akash.py`)
- **Pattern:** Extract → Validate & Quarantine → Transform & Mask → Idempotent Load.
- **Quarantine:** Rows failing critical referential checks, negative quantities, or invalid statuses are directed into `include/quarantine/akash/orders_failed_<run_id>.csv` with explicit `failure_reason`.
- **Target Schema:** Star schema stored in DuckDB (`etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`).
- **PII Protection:** Hashes/masks email (`ak***@domain.com`) and phone (`98******21`) before persisting into dimension tables.

### 2. ELT Pipeline (`nammamart_elt_akash.py`)
- **Pattern:** Extract → Load Raw → SQL-Driven Staging & Marts.
- **Layers:**
  1. **Raw Layer:** `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` loaded verbatim with metadata columns `_loaded_at`, `_source_file`, and `_run_id`.
  2. **Staging Layer:** `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` via SQL transformations (`include/sql/elt_models.sql`), executing casting, PII masking, and data quality status tagging (`PASSED` vs error code).
  3. **Marts Layer:** Pre-aggregated analytics marts answering core business metrics (`mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers`).
  4. **Incremental Merge:** Appends `orders_2026-10-01_increment.csv` into raw tables and executes an upsert (`DELETE` + `INSERT`) on `stg_orders` using `order_id` to update corrected statuses without creating duplicate rows.

---

## Part C: Scheduling & SLA (9:00 AM IST)

- **Cron Schedule:** `0 9 * * 1-6` (Monday through Saturday at 09:00 AM IST).
- **Timezone Awareness:** Defined using `pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")`.
- **Airflow UI Display:** In the default UTC display, the schedule shows as `03:30 UTC` (09:00 IST - 05:30).
- **Justification for `catchup=False`:** Setting `catchup=False` ensures that when the DAG is activated or restarted, Airflow does not execute backfill runs for every historical interval between the start date and today. It only schedules the latest active window.
- **System Downtime Resilience:** If the hosting machine or Docker engine is offline at 9:00 AM IST, the missed run is skipped due to `catchup=False`, and the pipeline resumes cleanly on the next scheduled run.

---

## Part D: Data Quality Dimension Matrix

| Quality Dimension | ETL Implementation (Python) | ELT Implementation (DuckDB SQL) | Severity |
| :--- | :--- | :--- | :--- |
| **Completeness** | `pd.isna(customer_id) | pd.isna(store_id)` checks | `CASE WHEN customer_id IS NULL THEN ...` | Critical |
| **Uniqueness** | `.drop_duplicates(subset=['customer_id'])` | `QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ...)` | Critical |
| **Validity** | Enum check against allowed statuses/payment modes | `UPPER(TRIM(order_status)) IN ('DELIVERED', ...)` | Critical |
| **Accuracy** | `quantity > 0` and math check on `order_amount` | `TRY_CAST(quantity AS INT) > 0` | Critical |
| **Consistency** | Foreign key set membership validation | Dimension joins with `LEFT JOIN` / `IS NOT NULL` | Critical |
| **Timeliness** | Comparison against `datetime.now()` for future dates | `order_ts <= CURRENT_TIMESTAMP` | Warning |

Results of all checks are appended to `dq_results` on every run for trend auditing.

---

## Part E: Data Governance

1. **Ownership & Approvals:**
   - Orders & Stores: Owned by Operations Engineering.
   - Payments & Reconciliation: Owned by Finance Operations.
   - Customer Data: Owned by CRM / Growth Team.
   - Quality rule modifications require joint sign-off from the Lead Analytics Engineer and Operations Manager.
2. **Personal Data & DPDP Compliance:**
   - Under India's Digital Personal Data Protection (DPDP) Act, customer name, phone number, and email constitute identifiable personal data.
   - Data is masked rather than discarded so customer loyalty tier analytics and cohort spend patterns remain linkable without exposing plaintext contact information.
3. **Access Control (RBAC):**
   - `Raw` layer: Restricted strictly to Data Engineering and Compliance Auditors.
   - `Staging` & `Quarantine`: Accessible by Data Engineers and Operations QA leads.
   - `Marts`: Broadly accessible to Business Analysts, Store Managers, and Executive Dashboards.
4. **Secrets Management:**
   - Production connection strings and API keys are stored in Airflow Connections or Environment Variables via `Variable.get()`. No credentials or tokens are committed to source control.

---

## Part F: Observability & Experiments

### Observability Summary & Run Audit Table
Every DAG run records execution metadata in `pipeline_runs`:
`run_id`, `dag_id`, `start_time`, `end_time`, `rows_loaded`, `status`.

### Five Pillars of Observability Coverage:
- **Best Covered: Schema & Quality.** Strict data typing, status verification, and quarantine separation ensure invalid rows never enter reporting marts.
- **Weakest: Automated Lineage.** Lineage is maintained via SQL dependencies and Airflow task ordering rather than automated graph tooling (such as OpenLineage/Marquez).

---

## Part G: Recommendation to CTO (Sameer)

### Recommendation: Adopt the ELT Architecture

**Core Rationale:**
1. **Complete Auditability & Replayability:** In quick commerce, order statuses fluctuate post-delivery (e.g. returns, chargebacks). Storing immutable raw data ensures historical data can always be audited and re-evaluated against revised business logic.
2. **Scalability:** As daily order volumes expand 1000×, in-warehouse SQL engines (DuckDB / Snowflake / BigQuery) process vectorized transformations significantly faster than memory-constrained Python runtime tasks.
3. **Finance Safeguards Maintained:** Clean separation in SQL staging ensures that the CFO dashboard queries only validated, reconciled records (`dq_status = 'PASSED'`), fulfilling Finance requirements without discarding raw audit records.

**Top 3 Architectural Risks & Mitigations:**
1. **Raw Storage Growth:** Implement monthly archival and automated raw table partitioning.
2. **Unregulated Querying of Raw PII:** Enforce role-based access control so business analysts only have `SELECT` privileges on the `mart` and `stg` layers.
3. **Incremental Merge Overheads:** Transition to micro-batch watermark incremental loads as volume scales.

---

## Business Questions Answered (SQL Queries)

```sql
-- 1. Daily net revenue by zone and store (delivered orders only)
SELECT order_date, zone, store_id, store_name, daily_net_revenue 
FROM mart_daily_store_kpis ORDER BY order_date DESC, daily_net_revenue DESC;

-- 2. Revenue and gross margin by product category
SELECT category, category_revenue, gross_margin 
FROM mart_category_margin ORDER BY gross_margin DESC;

-- 3. Average delivery time per store and % within 15 minutes
SELECT store_id, store_name, avg_delivery_minutes, pct_within_15_min 
FROM mart_daily_store_kpis GROUP BY store_id, store_name, avg_delivery_minutes, pct_within_15_min;

-- 4. Revenue by payment mode and reconciliation breakdown
SELECT payment_mode, COUNT(*) as orders_count, ROUND(SUM(order_amount), 2) as total_revenue
FROM stg_orders WHERE order_status = 'DELIVERED' GROUP BY payment_mode;

-- 5. Top 10 customers by spend (PII masked)
SELECT customer_id, full_name, email_masked, loyalty_tier, total_spend 
FROM mart_top_customers ORDER BY total_spend DESC LIMIT 10;

-- 6. Rejected rows by reason
SELECT failure_reason, COUNT(*) as rejected_count 
FROM stg_orders WHERE dq_status != 'PASSED' GROUP BY failure_reason;

