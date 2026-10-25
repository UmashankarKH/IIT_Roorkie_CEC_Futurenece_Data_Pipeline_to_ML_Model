# NammaMart Submission

This submission follows the order of [`assignment/ASSIGNMENT.md`](./assignment/ASSIGNMENT.md): Part 0 profiling, Part A ETL, Part B ELT, Part C scheduling, Part D data quality, Part E governance, Part F observability, the six business questions, and Part G recommendation to the CTO. All ten client acceptance criteria are evidenced in the Airflow UI and the DuckDB warehouses.

| Part | What this section answers | Evidence |
|---|---|---|
| Part 0 | Profiling, the Data Issues Log, and the three client questions | [profile report](./assignment/profile/PROFILE_REPORT.md), [issues log](./assignment/profile/data_issues_log.csv), [client questions](./assignment/profile/client_questions.csv) |
| Part A | The Python ETL pipeline (Extract → Validate → Transform → Load) | [ETL DAG](./dags/nammamart_etl_rudin.py) |
| Part B | The SQL ELT pipeline (Raw → Staging → Mart) | [ELT DAG](./dags/nammamart_elt_rudin.py), [include/sql/](./include/sql/) |
| Part C | 9:00 AM IST schedule, timezone, catchup, retries | [schedule notes](./dags/nammamart_schedule_notes.md), [schedule screenshot](./evidence/airflow_dags.png) |
| Part D | The six quality dimensions and the `dq_results` scorecard | [dq_results screenshot](./evidence/dq_results.png) |
| Part E | The nine governance questions | [governance answers](#part-e--data-governance) |
| Part F | Run auditing and the ten observability experiments | [experiments](#ten-required-experiments), [pipeline_runs screenshot](./evidence/pipeline_runs.png) |
| Step 9 | The six business questions, SQL plus results, from both pipelines | [business-question SQL](./include/sql/nammamart_business_questions.sql) |
| Part G | ETL or ELT for NammaMart | [recommendation](#part-g--recommendation-to-sameer-cto) |

## Part 0 — Discovery and data profiling

Profiling was run before any pipeline code was written, using [`include/profile_nammamart.py`](./include/profile_nammamart.py) over the six untouched exports in `assignment/nammamart_dataset/`. The files were copied to `include/data/nammamart/` without manual edits. Full output: [PROFILE_REPORT.md](./assignment/profile/PROFILE_REPORT.md), [profile_summary.csv](./assignment/profile/profile_summary.csv).

### File inventory

| File | Rows | Key | Main observations |
|---|---:|---|---|
| `orders.csv` | 2,412 | `order_id` | 18 missing customer IDs, 12 duplicate key groups, invalid statuses/payment modes, invalid amounts and delivery times |
| `payments.csv` | 2,228 | `payment_id` | 4 duplicate key groups, 8 unknown orders, 14 SUCCESS amount mismatches, invalid payment modes |
| `customers.csv` | 405 | `customer_id` | 5 duplicate key groups, 6 missing cities, malformed email/phone/pincode values, future signup dates |
| `products.csv` | 152 | `product_id` | 2 duplicate key groups, 3 missing categories, 5 cost prices above MRP, one non-positive MRP |
| `stores.csv` | 15 | `store_id` | One missing zone; no duplicate store keys |
| `orders_2026-10-01_increment.csv` | 160 | `order_id` | 10 existing order IDs are intentional correction/upsert candidates; 150 are new orders |

Total: 5,372 rows across the six files.

### Nulls and duplicate keys

| File | Column | Null rows |
|---|---|---:|
| orders.csv | `order_ts` | 1 |
| orders.csv | `customer_id` | 18 |
| orders.csv | `delivery_minutes` | 164 |
| payments.csv | `gateway_ref` | 301 |
| customers.csv | `city` | 6 |
| products.csv | `category` | 3 |
| stores.csv | `zone` | 1 |

| File | Key | Duplicate groups | Extra rows |
|---|---|---:|---:|
| orders.csv | `order_id` | 12 | 12 |
| payments.csv | `payment_id` | 4 | 4 |
| customers.csv | `customer_id` | 5 | 5 |
| products.csv | `product_id` | 2 | 2 |
| stores.csv | `store_id` | 0 | 0 |

### Numeric and date ranges

- Orders: quantity `-3` to `5`; discount `0` to `250`; order amount `-1593` to `9576`; delivery time `8` to `1440` minutes.
- Orders: `order_ts` from `2026-09-01 06:30:15` to `2027-01-01 09:00:00`.
- Payments: amount paid `-1593` to `9576`; payment timestamp reaches `2027-01-01 09:01:46`.
- Customers: pincode `0` to `560102`; signup dates reach `2027-01-05`.
- Products: MRP `-203` to `883`; cost price `6.79` to `631.79`.
- Stores: capacity `0` to `600` orders/day.

### Distinct categorical values

| File | Column | Distinct values |
|---|---|---|
| orders.csv | `order_status` | CANCELLED, DELIVERED, RETURNED, `Pending??`, `delivered` |
| orders.csv | `payment_mode` | CARD, Card, Cash on Delivery, `Cashh`, UPI, `UPI ` (trailing space), Wallet, `cod`, `upi` |
| payments.csv | `payment_mode` | CARD, Card, Cash on Delivery, `Cashh`, UPI, `UPI ` (trailing space), Wallet, `cod`, `upi` |
| customers.csv | `loyalty_tier` | BRONZE, Bronze, Gold, Platinum, SILVER, Silver, `bronze` |
| products.csv | `category` | Dairy & Bakery, Fruits & Vegetables, Household, Personal Care, Snacks & Beverages, Staples |
| stores.csv | `zone` | North, South, East, West |

The mixed casing and trailing-space variants are what motivated the standardisation rules in Part A and Part B.

### Referential integrity and business rules

- Orders reference missing customers, products, and stores — critical consistency failures.
- Payments reference eight unknown orders.
- Twelve orders have invalid or duplicate order keys; payments, customers, and products also contain duplicate primary keys.
- Delivered orders are missing delivery times, and some delivery times are 480, 900, or 1,440 minutes.
- Quantities include non-positive values, discounts exceed 100%, and several `order_amount` values do not match `quantity × unit_price × (1 − discount_pct/100)`.
- Fourteen SUCCESS payments do not equal their corresponding order amount.
- Customer emails, phones, and pincodes contain format failures.
- Product categories are missing, one MRP is non-positive, and five products have `cost_price > mrp`.
- Future-dated order and signup records were found.
- One store has no zone, which prevents reliable zone reporting.
- The increment file has 10 existing IDs, all changing from `DELIVERED` to `RETURNED`; these must be merged rather than appended as duplicate facts.

### Data Issues Log

The row-level register holds **197 entries** (file, row key, column, problem, quality dimension, proposed rule, severity) and is committed as [`assignment/profile/data_issues_log.csv`](./assignment/profile/data_issues_log.csv). Distribution by file: `orders.csv` 111, `payments.csv` 34, `customers.csv` 30, `products.csv` 11, `stores.csv` 1, `orders_2026-10-01_increment.csv` 10 (the 10 increment entries are `Warning` severity because they are expected corrections, not bad data). By dimension and severity: Completeness/Critical 30, Accuracy/Critical 40, Consistency/Critical 36, Validity/Critical 28, Uniqueness/Critical 23, Timeliness/Critical 8, Validity/Warning 22, Consistency/Warning 10.

| File | Column | Problem | Dimension | Severity | Rows |
|---|---|---|---|---|---:|
| orders.csv | `customer_id` | customer_id is blank | Completeness | Critical | 18 |
| orders.csv | `order_id` | Duplicate key appears 2 times | Uniqueness | Critical | 12 |
| orders.csv | `quantity` | Quantity is missing or not greater than zero | Accuracy | Critical | 10 |
| orders.csv | `product_id` | product_id does not exist | Consistency | Critical | 9 |
| orders.csv | `delivery_minutes` | Delivered order has no delivery time | Completeness | Critical | 8 |
| orders.csv | `order_ts` | Timestamp cannot be parsed | Validity | Critical | 6 |
| orders.csv | `store_id` | store_id does not exist | Consistency | Critical | 5 |
| orders.csv | `order_ts` | Future-dated order timestamp | Timeliness | Critical | 5 |
| orders.csv | `payment_mode` | Invalid payment mode: `upi`, `Cashh`, `CARD`, `cod` | Validity | Critical | 8 |
| orders.csv | `order_status` | Invalid status: `Pending??` | Validity | Critical | 3 |
| orders.csv | `discount_pct` | Discount is outside 0-100 | Validity | Critical | 3 |
| orders.csv | `delivery_minutes` | Implausible delivery time: 480 / 900 / 1440 minutes | Accuracy | Critical | 6 |
| orders.csv | `order_amount` | Amount does not match calculated amount (18 rows) | Accuracy | Critical | 18 |
| payments.csv | `amount_paid` | SUCCESS payment does not equal order_amount | Consistency | Critical | 14 |
| payments.csv | `order_id` | Payment references an unknown order | Consistency | Critical | 8 |
| payments.csv | `payment_id` | Duplicate key appears 2 times | Uniqueness | Critical | 4 |
| payments.csv | `payment_mode` | Invalid payment mode: `upi`, `Cashh`, `CARD`, `cod` | Validity | Critical | 8 |
| customers.csv | `email` | Email does not match expected format | Validity | Warning | 12 |
| customers.csv | `phone` | Phone is not exactly 10 digits | Validity | Warning | 7 |
| customers.csv | `customer_id` | Duplicate key appears 2 times | Uniqueness | Critical | 5 |
| customers.csv | `pincode` | Pincode is not exactly 6 digits | Validity | Warning | 3 |
| customers.csv | `signup_date` | Future-dated signup date | Timeliness | Critical | 3 |
| products.csv | `cost_price` | Cost price exceeds MRP | Accuracy | Critical | 5 |
| products.csv | `category` | Category is blank | Completeness | Critical | 3 |
| products.csv | `product_id` | Duplicate key appears 2 times | Uniqueness | Critical | 2 |
| products.csv | `mrp` | MRP is missing or not positive | Accuracy | Critical | 1 |
| stores.csv | `zone` | Store zone is blank | Completeness | Critical | 1 |
| orders_2026-10-01_increment.csv | `order_id` | Existing order key is an intentional correction/upsert candidate | Consistency | Warning | 10 |

Issues were found in **every** file, matching the client's warning that "there are problems in every file".

### Questions for the client

Committed as [`assignment/profile/client_questions.csv`](./assignment/profile/client_questions.csv):

1. Should a negative quantity or an `order_amount` mismatch be treated as a rejected data-entry error, or can either represent a return/adjustment?
2. When an order changes from DELIVERED to RETURNED, should September revenue be restated historically, or preserved as originally reported with a separate returns view?
3. What retention period and role-based access policy should apply to raw customer PII, quarantine files, and masked reporting tables?

The default answer applied while building was: treat them as data-entry errors and quarantine them (Critical), restate to current state while keeping raw history for an as-reported view, and apply the retention/access policy written in Part E. All three questions are still open for the client to confirm.

## Part A — ETL pipeline

DAG: [`dags/nammamart_etl_rudin.py`](./dags/nammamart_etl_rudin.py) · DAG ID `nammamart_etl_rudin` · warehouse `include/warehouse_rudin.duckdb`.

Flow: **Extract → Validate → Transform → Load → Reconcile → Report**, with a run-audit task on `trigger_rule="all_done"`.

| Stage | Task(s) | What it does |
|---|---|---|
| Extract | `extract_orders`, `extract_payments`, `extract_customers`, `extract_products`, `extract_stores` | Five parallel reads; every value is read as text with `keep_default_na=False` so that "blank" and "0" are never confused. `extract_orders` also reads the increment file and keeps the latest received version per `order_id` (increment wins) so the ETL snapshot matches the ELT current state. Each task asserts its required columns and raises `ValueError` on a source contract change. |
| Validate | `validate` | Python rules for every file. Rows are split into *passed* and *failed*; each failed row keeps all the rules it broke, joined by `; `. The same task writes the ETL half of the `dq_results` scorecard. |
| Quarantine | `quarantine` | Writes failed rows to `include/quarantine/rudin/<file>_failed_<run_id>.csv` with a `failure_reason` column. Nothing is silently dropped. |
| Transform | `transform` | Trims whitespace; upper-cases `order_status`; maps `upi`/`UPI `/`cod` to `UPI`/`Cash on Delivery`; title-cases `loyalty_tier`; de-duplicates customers, products, and stores (last record wins); masks email and phone; derives `order_date`, `net_revenue`, `within_15_min`, `gross_margin`; inner-joins to the dimensions so a rejected master row cannot reach the fact table. |
| Load | `load_star_schema` | `CREATE OR REPLACE TABLE` for `etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store` — replace-style loading, so a retry or re-run can never double-count revenue. |
| Reconcile | `reconcile_payments` | Outer-joins successful payments to passed orders and writes one row per mismatch to `etl_payment_reconciliation` with the reason `missing order`, `missing successful payment`, or `amount mismatch`. |
| Report | `report` | Builds `etl_daily_zone_revenue` and `etl_category_margin` and prints them in the task log. |
| Audit | `audit_run` | `all_done` task that appends one row to `pipeline_runs`. |

### Validation rules applied (Critical rows are quarantined)

| File | Critical rules |
|---|---|
| all five files | key is not blank; key is unique |
| orders | `customer_id`/`product_id`/`store_id` exist in the masters; `order_status` in {DELIVERED, CANCELLED, RETURNED}; `payment_mode` in {UPI, Card, Cash on Delivery, Wallet}; `quantity > 0`; `0 ≤ discount_pct ≤ 100`; `order_amount` matches the formula within 0.01; `order_ts` parses and is not future-dated; DELIVERED orders have `delivery_minutes`; `delivery_minutes < 120` |
| payments | `payment_mode` allowed; `order_id` exists; `payment_ts` parses; a SUCCESS payment equals the order `order_amount` |
| customers | `signup_date` valid and not future-dated (email/phone format issues are Warnings only) |
| products | `category` present; `mrp > 0`; `cost_price <= mrp` |
| stores | `zone` present |

### PII handling and idempotency

Email is masked to `pr***@example.com` and phone to `98******21` **before** the dimension is loaded, so no unmasked customer contact field exists in any ETL reporting table. Because the load is replace-style, a second identical run produces identical row counts — verified in experiment 2 below.

### Verification

The final clean full load ran as ETL run `manual__2026-10-22T11:26:30.558523+00:00`: 5,350 rows extracted, 2,087 rows loaded into `etl_fact_orders`, 149 rows quarantined, `status = success` in `pipeline_runs`. See the [ETL success screenshot](./evidence/airflow_etl_success.png) and the [quarantine screenshot](./evidence/quarantine.png).

## Part B — ELT pipeline

DAG: [`dags/nammamart_elt_rudin.py`](./dags/nammamart_elt_rudin.py) · DAG ID `nammamart_elt_rudin` · warehouse `include/warehouse_rudin_elt.duckdb` (a separate file from the ETL warehouse so both DAGs can run side by side without DuckDB lock contention).

Flow: **Extract → Load raw → Transform in the warehouse with SQL**.

| Layer | Objects | Rule |
|---|---|---|
| Raw | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | Exact copy of each CSV, all columns as text, plus `_loaded_at`, `_source_file`, `_run_id`. Never cleaned, never deleted. |
| Staging | `stg_orders` (persistent MERGE target), `stg_orders_snapshot`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` | SQL only: cast types, standardise, dedupe, mask PII, apply quality rules, add `dq_status` and `failure_reason`. |
| Mart | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` | SQL only: business-ready tables that answer the six business questions. |

Control tables: `dq_results` (quality scorecard), `pipeline_runs` (run audit), `elt_incremental_audit` (insert/update counters), `elt_etl_september_reconciliation` and `elt_etl_october_reconciliation` (ETL-vs-ELT comparison written by the ELT DAG itself).

All transformations live in version-controlled SQL under [`include/sql/`](./include/sql/) and are executed by Airflow tasks:

| SQL file | Purpose |
|---|---|
| `nammamart_elt_staging.sql` | All staging casts, standardisation, deduplication, masking, and row-level `dq_status`/`failure_reason` |
| `nammamart_elt_orders_upsert.sql` | `MERGE` of the latest order snapshot into persistent `stg_orders` |
| `nammamart_elt_dq_results.sql` | Writes one `dq_results` row per check per run |
| `nammamart_elt_marts.sql` | Builds the five mart tables from passing staging rows only |
| `nammamart_business_questions.sql` | The six business questions, ETL and ELT variants |

### Raw loading

The six raw tasks append every source row, including the bad ones, with no pandas cleaning. They are serialised (`raw_orders >> raw_increment >> raw_payments >> raw_customers >> raw_products >> raw_stores`) because DuckDB permits a single writer per file; the increment is loaded immediately after the base orders so the staging "latest record wins" window function applies corrections over the September snapshot.

### Incremental load (MERGE)

`stg_orders_snapshot` holds the de-duplicated latest version of every `order_id`. [`nammamart_elt_orders_upsert.sql`](./include/sql/nammamart_elt_orders_upsert.sql) then runs:

```sql
MERGE INTO stg_orders AS target
USING stg_orders_snapshot AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT BY NAME;
```

The recorded counters for `orders_2026-10-01_increment.csv` are **150 inserts and 10 updates** — the 10 corrections (DELIVERED → RETURNED) overwrite the September rows instead of duplicating them, and the 150 new orders are appended. After the merge, `stg_orders` holds 2,550 unique orders with no duplicate `order_id`.

### Mart totals versus ETL totals

The ELT DAG attaches the ETL warehouse read-only and writes its own comparison tables every run:

| Metric | ETL orders | ELT orders | ETL revenue | ELT revenue | Status |
|---|---:|---:|---:|---:|---|
| September delivered revenue | 1,768 | 1,768 | 956,731.20 | 956,731.20 | MATCH |
| October delivered revenue | 128 | 128 | 68,798.20 | 68,798.20 | MATCH |

September matches exactly, as required. October also matches: both pipelines apply the increment rows as the latest version of each `order_id` and publish the same 128 valid October delivered orders worth 68,798.20. The one previously inconsistent future-dated order is rejected by both pipelines under the aligned `2026-10-09` cutoff.

### Verification

The final clean full load ran as ELT run `manual__2026-10-22T11:26:57.475476+00:00`: 5,372 rows extracted, 2,555 staged orders, 490 rejected rows, `status = success` in `pipeline_runs`. See the [ELT success screenshot](./evidence/airflow_elt_success.png).

## Part C — Scheduling both DAGs for 9:00 AM IST

Both DAGs use the same schedule and the same `default_args`. Notes: [`dags/nammamart_schedule_notes.md`](./dags/nammamart_schedule_notes.md).

```text
0 9 * * 1-6
```

```python
start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")
catchup=False
max_active_runs=1
default_args={
    "owner": "fde-cohort",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}
```

### What is the cron expression?

`0 9 * * 1-6` — 9:00 AM, Monday through Saturday, in the DAG's `Asia/Kolkata` timezone.

### What time does the run show in UTC in the Airflow UI?

**03:30 UTC.** India Standard Time is UTC+05:30, so a 9:00 AM IST run is displayed as 03:30 UTC. The DAG's timezone-aware `start_date` makes the UI's next-run time unambiguous, as shown in the [schedule screenshot](./evidence/airflow_dags.png).

### What would happen at 9 AM if the start_date had been left in UTC?

Airflow would have interpreted the DAG timezone as UTC and fired the cron at 9:00 AM **UTC = 2:30 PM IST** — five and a half hours after the 9:30 AM leadership review. The morning report would arrive after the meeting it is meant to serve. Anchoring `start_date` to `Asia/Kolkata` is what prevents this.

### What happens if the laptop or Docker is off at 9 AM?

The local scheduler is not running, so no run is created. Because `catchup=False`, Airflow does **not** replay the missed interval when Docker comes back — the DAG must be triggered manually once the environment and source files are available:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\trigger_nammamart.ps1 -Pipeline both
```

`catchup=False` is deliberate: the morning report should process the current scheduled source snapshot, not silently replay every missed day and republish historical numbers. `retries=2` with a 2-minute delay absorbs transient failures, `execution_timeout=30 minutes` stops a stuck task, and `max_active_runs=1` prevents overlapping writes to the same DuckDB warehouse.

## Part D — Data quality dimensions

At least one check per dimension is implemented in **both** pipelines — Python in ETL, SQL in ELT. Critical rows are quarantined or excluded from the marts; Warning rows still load but are recorded in the scorecard.

| Dimension | ETL check (Python) | ELT check (SQL) | Severity | NammaMart example |
|---|---|---|---|---|
| Completeness | `required_fields` | `stg_customers_quality`, `stg_stores_quality` | Critical | `customer_id` not null in orders; DELIVERED orders have `delivery_minutes`; `category` not null in products; `zone` not null in stores |
| Uniqueness | `primary_keys_unique` | `raw_orders_duplicate_keys` | Critical | `order_id` unique; `payment_id` unique; one row per `customer_id` and `product_id` |
| Validity | `allowed_values_and_formats`, `pii_format` | `stg_orders_quality`, `customer_pii_format` | Critical for status/timestamp, Warning for email/phone | `order_status` and `payment_mode` in the allowed lists; email format; phone is 10 digits; pincode is 6 digits; `discount_pct` 0-100; `order_ts` parses |
| Accuracy | `numeric_business_rules`, `delivery_normal_range` | `stg_products_quality`, `delivery_normal_range` | Critical, with a Warning band above the normal 8-35 minute range | `quantity > 0`; `order_amount` matches the formula; `mrp > 0`; `cost_price <= mrp`; `delivery_minutes` below 120 |
| Consistency | `foreign_keys_and_payment_match` | `stg_payments_quality` | Critical | `customer_id`, `product_id`, `store_id` exist in their masters; payments match an order and its amount; `loyalty_tier` uses one casing |
| Timeliness | `future_dates`, `stale_orders` | `orders_future_dates`, `customers_future_dates`, `stale_orders` | Critical for future dates, Warning for stale | No future-dated `order_ts` or `signup_date`; the latest order is not older than 30 days at run time |

Two extra operational checks support the observability pillars in Part F: `order_volume_drop` (Volume, Warning) and `price_distribution_drift` (Distribution, Warning).

### The `dq_results` scorecard

Every check result of every run is persisted with exactly the required columns:

```text
run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at
```

`status` is `PASS` when `rows_failed = 0`, `WARN` for a Warning-severity check that found rows, and `FAIL` for a Critical-severity check that found rows. The ETL DAG writes its rows from Python during `validate`; the ELT DAG writes its rows from [`nammamart_elt_dq_results.sql`](./include/sql/nammamart_elt_dq_results.sql). Both write to their own `dq_results` table inside their own warehouse file, so the two pipelines never contaminate each other's scorecard. Evidence: [dq_results screenshot](./evidence/dq_results.png).

## Part E — Data governance

### 1. Ownership

The Operations team owns orders and stores; Finance owns payments and approves reconciliation rules; CRM owns customers; and Catalogue owns products. The Data Engineering/FDE team owns the ETL/ELT warehouse tables, pipeline code, and quality scorecards. Operations, Finance, and Data Engineering should jointly approve a rule change such as the 120-minute delivery limit, with Finance signing off on revenue-impacting changes.

Warehouse ownership is assigned as follows: Operations owns `raw_orders`, `stg_orders`, `raw_stores`, `stg_stores`, and operational marts; Finance owns `raw_payments`, `stg_payments`, payment reconciliation, revenue, and margin marts; CRM owns `raw_customers` and `stg_customers`; Catalogue owns `raw_products` and `stg_products`; and Data Engineering owns the warehouse schemas, `dq_results`, `pipeline_runs`, quarantine process, and all transformation code. Source owners approve source-data rules; Data Engineering applies and monitors them.

### 2. Data dictionary

#### ETL fact and dimensions

| Table | Column | Type | Meaning | Source | Allowed values / derivation | PII |
|---|---|---|---|---|---|---|
| `etl_fact_orders` | `order_id` | VARCHAR | Unique order identifier | `orders.csv` | Source key | No |
| `etl_fact_orders` | `order_ts`, `order_date` | TIMESTAMP, DATE | Order timestamp and reporting date | `orders.csv` | Parsed timestamp / derived date | No |
| `etl_fact_orders` | `customer_id`, `store_id`, `product_id` | VARCHAR | Dimension foreign keys | Orders and master files | Must exist in dimensions | No |
| `etl_fact_orders` | `quantity`, `unit_price`, `discount_pct`, `order_amount` | Numeric | Order quantity, price, discount, and calculated amount | `orders.csv` | Quantity > 0; discount 0–100 | No |
| `etl_fact_orders` | `net_revenue`, `gross_margin` | Numeric | Delivered revenue and revenue minus quantity × cost | Orders/products | Delivered orders only | No |
| `etl_fact_orders` | `order_status`, `payment_mode`, `channel` | VARCHAR | Operational classifications | `orders.csv` | Status: DELIVERED/CANCELLED/RETURNED; payment modes: UPI/Card/Cash on Delivery/Wallet | No |
| `etl_fact_orders` | `delivery_minutes`, `within_15_min` | INTEGER, BOOLEAN | Delivery SLA measures | `orders.csv` | Delivered orders require time; boolean is derived | No |
| `etl_dim_customer` | `customer_id`, `full_name`, `loyalty_tier`, `is_active` | VARCHAR | Customer dimension attributes | `customers.csv` | Loyalty: Bronze/Silver/Gold/Platinum | Name: Yes; other fields: No/related |
| `etl_dim_customer` | `email`, `phone` | VARCHAR | Masked customer contact fields | `customers.csv` | Masked before warehouse load | Yes, masked |
| `etl_dim_product` | `product_id`, `product_name`, `category`, `sub_category`, `brand` | VARCHAR | Product catalogue attributes | `products.csv` | Catalogue values | No |
| `etl_dim_product` | `mrp`, `cost_price`, `unit`, `is_perishable` | Numeric/VARCHAR/BOOLEAN | Product pricing and handling attributes | `products.csv` | `mrp > 0`; cost_price <= mrp | No |
| `etl_dim_store` | `store_id`, `store_name`, `zone`, `area` | VARCHAR | Store and geographic reporting attributes | `stores.csv` | Zone: North/South/East/West | Area may be personal/location-related |

#### ELT raw tables

| Table | Contents and metadata | Purpose / PII |
|---|---|---|
| `raw_orders` | Every order source column as text, plus `_loaded_at`, `_source_file`, `_run_id` | Immutable replay/audit layer; no direct PII |
| `raw_payments` | Every payment source column as text, plus load metadata | Payment replay and reconciliation; no direct PII |
| `raw_customers` | Customer source columns, including name, email, phone, city, area, and pincode, plus load metadata | Restricted source record; contains personal data |
| `raw_products` | Product source columns plus load metadata | Catalogue replay; no direct PII |
| `raw_stores` | Store source columns plus load metadata | Store replay; area may be location-related |

Raw tables are append-only and preserve the original values for audit and replay. `_source_file` identifies the file, `_loaded_at` identifies ingestion time, and `_run_id` identifies the pipeline execution.

#### ELT staging tables

| Table | Key fields and transformations | Quality behaviour |
|---|---|---|
| `stg_orders` | Typed `order_id`, `order_ts`, `order_date`, customer/store/product keys, quantity, price, discount, `order_amount`, `net_revenue`, `gross_margin`, delivery fields, `dq_status`, `failure_reason` | Current order state after `MERGE` on `order_id`; failed rows are excluded from marts |
| `stg_payments` | Typed payment identifiers, order key, amount, mode, status, payment timestamp, `dq_status`, `failure_reason` | Used only when valid and successful for reconciliation |
| `stg_customers` | Typed customer attributes, deduplicated customer key, masked email/phone, `dq_status`, `failure_reason` | Customer PII is masked before analytical use |
| `stg_products` | Typed product attributes, deduplicated product key, price/cost checks, `dq_status`, `failure_reason` | Invalid catalogue rows are excluded from joins |
| `stg_stores` | Typed store attributes, deduplicated store key, `dq_status`, `failure_reason` | Invalid store rows are excluded from joins |

#### ELT marts and control tables

| Table | Grain and measures | Consumers |
|---|---|---|
| `mart_daily_store_kpis` | One row per order date and store; orders, delivered orders, revenue, and margin | Operations and Finance |
| `mart_category_margin` | One row per order date and product category; revenue, cost, and gross margin | Finance and Catalogue |
| `mart_delivery_sla` | One row per order date and store; delivery counts, average delivery time, and within-SLA count | Operations |
| `mart_payment_reconciliation` | One row per order; order amount, successful payment amount, variance, and reconciliation status | Finance |
| `mart_top_customers` | One row per customer; delivered order count, spend, and rank | CRM and approved analysts |
| `dq_results` | One row per quality check and run with `run_id`, `dag_id`, check metadata, counts, status, and `checked_at` | Data Engineering and source owners |
| `pipeline_runs` | One row per DAG run with timing, extracted/loaded/quarantined counts, and status | Data Engineering and audit reviewers |

All marts use passing staging records only. The ETL fact and dimensions above are the parallel star-schema output; the ELT raw, staging, and mart layers are the traceable SQL-based alternative.

### 3. Lineage

For `mart_daily_store_kpis.net_revenue`, the ELT lineage is:

```text
orders.csv / orders_2026-10-01_increment.csv
  -> raw_orders
  -> stg_orders.order_amount, order_status, order_date
  -> mart_daily_store_kpis.net_revenue
```

The mart filters `dq_status = 'PASS'` and `order_status = 'DELIVERED'`, then sums `order_amount` by date and store. ELT is easier to trace because raw rows retain `_source_file`, `_loaded_at`, and `_run_id`. ETL can trace the value through the original CSV and quarantine/fact load, but the raw audit metadata is less direct.

### 4. Personal data and Indian privacy law

The customer export contains `full_name`, `email`, `phone`, `city`, `area`, `pincode`, `signup_date`, `loyalty_tier`, and `is_active`; these can identify or describe a customer and must be access-controlled. The pipelines mask email and phone in reporting dimensions while retaining only the minimum business attributes needed for analysis, rather than deleting them and losing support/audit usefulness.

The relevant framework is India's Digital Personal Data Protection Act, 2023, together with the Digital Personal Data Protection Rules, 2025. NammaMart should establish a lawful processing purpose, provide notice, apply reasonable security safeguards, handle breach obligations and data-principal rights, and erase data when retention is no longer legally or operationally necessary. See the official [DPDP Act](https://www.meity.gov.in/static/uploads/2024/02/Digital-Personal-Data-Protection-Act-2023.pdf) and [DPDP Rules](https://www.meity.gov.in/documents/act-and-policies/digital-personal-data-protection-rules-2025-gDOxUjMtQWa?pageTitle=Digital-Personal-Data-Protection-Rules-2025).

### 5. Access control

Raw tables and raw customer PII should be restricted to approved CRM, security, and data-engineering personnel. Staging tables should be available to data engineering and quality reviewers; marts should be available to Operations, Finance, and approved analysts; quarantine should be available to data engineering and the relevant source-system owner. Unmasked raw phone numbers should not be visible to ordinary analysts or store managers.

### 6. Retention

As a proposed policy, retain raw source tables for 13 months to support year-over-year audit and replay, quarantine rows for 90 days after resolution, and masked customer reporting data for 24 months. At expiry, delete or cryptographically destroy the relevant data after checking legal holds, finance requirements, and open customer requests; retain only aggregated, non-identifying metrics where justified.

### 7. Secrets

Credentials and alert endpoints such as `SLACK_WEBHOOK_URL` should be stored as Airflow Variables or a deployment secret manager and retrieved at runtime with `Variable.get()`. They must not be placed in DAG code, SQL files, notebooks, CSVs, or GitHub because repository history and logs can expose credentials even after a later deletion.

### 8. Audit of a rejected order

In ETL, the rejected row is written to `include/quarantine/rudin/<file>_failed_<run_id>.csv` with `failure_reason`, while the validation task excludes it from `etl_fact_orders`. Finance can compare the quarantine `order_id` against `etl_fact_orders` and confirm it is absent from delivered revenue.

In ELT, the exact source row remains in `raw_orders`; the SQL staging table records `dq_status = 'FAIL'` and `failure_reason`, and every mart filters to passing rows. This gives Finance both the original row and proof that it cannot contribute to `mart_daily_store_kpis` or `mart_category_margin`.

### 9. Change history

When an order changes from `DELIVERED` on 3 September to `RETURNED` on 1 October, the ELT raw layer keeps both received records, while the staging `MERGE` makes the latest returned version the current `order_id` state. September current-state revenue therefore excludes the returned order; Finance may also need an as-reported historical view that preserves the original September result.

The recommended design is to keep both views: use the current upserted staging/mart tables for operational truth, and create an append-only historical fact or change-history table keyed by `order_id`, source file, and load timestamp. ETL should similarly retain quarantine/source snapshots or an audit table instead of overwriting the only copy of the prior status.

## Part F — Observability

Both DAGs end with an `all_done` audit task. It writes one row per run to `pipeline_runs` with:

```text
run_id, dag_id, start_time, end_time, rows_extracted,
rows_loaded, rows_quarantined, status
```

The ETL audit counts source extracts, clean fact rows, and quarantine rows. The ELT audit counts raw rows loaded, current staged orders, and rows rejected from staging. A run is marked `failed` when any upstream task is not successful or skipped, including runs where a source file is missing or a schema change causes a task failure.

### Latest ETL/ELT verification

Both DAGs were rerun successfully on 22 October 2026:

| DAG | Run ID | Status | Rows extracted | Rows loaded | Rows quarantined |
|---|---|---|---:|---:|---:|
| `nammamart_etl_rudin` | `manual__2026-10-22T08:36:14.418459+00:00` | success | 5,350 | 2,087 | 149 |
| `nammamart_elt_rudin` | `manual__2026-10-22T08:36:24.460018+00:00` | success | 5,372 | 2,550 | 484 |

The ELT incremental audit records **150 inserts and 10 updates** for `orders_2026-10-01_increment.csv`. Staging contains 2,550 unique orders and no duplicate `order_id` values.

Both derived warehouses were reset and fully reloaded from the source snapshot. The September source data remains available for historical reporting, and both September and October reconciliation tables are now persisted. Source CSVs remain untouched; the clean full load also rebuilt raw ELT tables, quarantine outputs, audit history, and quality logs.

The full-load reconciliation results are:

| Metric | ETL orders | ELT orders | ETL revenue | ELT revenue | Status |
|---|---:|---:|---:|---:|---|
| September delivered revenue | 1,768 | 1,768 | 956,731.20 | 956,731.20 | MATCH |
| October delivered revenue | 128 | 128 | 68,798.20 | 68,798.20 | MATCH |

The full-load comparison now matches. ETL reads the base export plus the October incremental file, applies incremental rows as the latest version for each `order_id`, and publishes 128 valid October delivered orders worth 68,798.20. ELT publishes the same 128 orders and revenue. The one previously inconsistent future-dated order is rejected by both pipelines under the aligned `2026-10-09` cutoff.

### Ten required experiments

Run each experiment separately in Airflow, restore the original source file after each test, and record the run ID from the DAG run page. The table below records the current evidence. Rows still marked **pending** are superseded by the [Final synchronized experiment status](#final-synchronized-experiment-status) table below, which records the completed isolated runs for experiments 5, 7, 8, and 9.

| Experiment | Expected → Observed | Evidence location | Proposed improvement |
|---|---|---|---|
| 1. Happy path | Expected: both DAGs finish successfully → Observed: ETL run `manual__2026-10-22T08:56:01.438957+00:00` and ELT run `manual__2026-10-22T08:56:11.455740+00:00` both finished `success`; audit tasks completed | Airflow run history; `pipeline_runs` | Add alerts if a successful run does not write an audit row |
| 2. Re-run/idempotency | Expected: facts/staging remain deduplicated → Observed: repeated successful runs at 08:36 and 08:56 completed without duplicate staged `order_id` values; ELT raw history appended by design | `etl_fact_orders`, `raw_orders`, `stg_orders`, `pipeline_runs` | Add a raw-load retention or run-deduplication policy if raw growth is not acceptable |
| 3. Incremental load | Expected: 10 updates and 150 inserts → Observed: ELT incremental audit recorded **10 updates and 150 inserts**, with no duplicate staged order keys | `elt_incremental_audit`, `raw_orders`, `stg_orders` | Keep explicit inserted/updated counters in the merge log |
| 4. Missing source file | Expected: retries, final failure, and `all_done` audit → Observed: ETL run `manual__2026-10-22T09:15:39.414302+00:00` ended with `extract_stores = failed`; ELT run `manual__2026-10-22T09:15:52.442352+00:00` ended with `load_raw_stores = failed`; downstream tasks were `upstream_failed`, while `audit_run = success` | Airflow task states and audit logs for the two run IDs; `pipeline_runs` | Keep the failure alert tied to the audit row and source-owner notification |
| 5. New bad data | Expected: invalid rows are quarantined or excluded → Observed: normal source runs quarantined invalid rows with `failure_reason`; ELT `dq_results` recorded critical failures and marts filtered failed staging rows | ETL quarantine CSVs, `stg_orders`, `dq_results` | Add a dedicated mutation run containing one example of each invalid rule |
| 6. Schema change | Expected: required-column change fails visibly → Observed: after renaming `order_amount` to `order_amount_changed`, the ETL extract raised `ValueError: Schema validation failed for orders.csv; missing columns: ['order_amount']`; the equivalent ELT raw-loader check is implemented for the same contract | ETL task-test log; required-column checks in both DAGs; clean verification runs `manual__2026-10-22T10:09:34.700166+00:00` and `manual__2026-10-22T10:09:49.542790+00:00` | Keep required-column contracts version-controlled and alert on schema failures |
| 7. Volume anomaly | Expected: major row-count drop is visible and warned → Observed: **pending isolated low-volume run** | `pipeline_runs`, `dq_results`, task log | Add a persisted baseline-volume check |
| 8. Distribution drift | Expected: price change affects revenue/margin and is detected → Observed: **pending isolated price-drift run** | Mart results, task log, `dq_results` | Add percentile or z-score checks for unit-price drift |
| 9. Freshness failure | Expected: stale orders fail or warn → Observed: future-date timeliness checks are implemented, but a dedicated stale-data run is **pending** | `dq_results`, validation/SQL logs | Make freshness thresholds configurable rather than hard-coded |
| 10. Recovery | Expected: clearing/fixing the failed source allows the failed branch and downstream tasks to rerun → Observed: after restoring `stores.csv`, ETL recovery run `manual__2026-10-22T09:21:11.099122+00:00` and ELT recovery run `manual__2026-10-22T09:21:28.042564+00:00` both completed `success` | Airflow run history, task states, and audit logs | Document the clear-and-retry runbook for operations |

### Final synchronized experiment status

The following corrections supersede the earlier `pending` or generic descriptions in the experiment table above.

| Experiment | Expected | Observed | Evidence location | Proposed improvement |
|---|---|---|---|---|
| 5. New bad data | Five invalid rows should be quarantined or excluded with rule-specific reasons | ETL quarantined the missing-customer, negative-quantity, unknown-store, invalid-status, and future-date rows. ELT completed `record_dq_results` and excluded failed staging rows from marts | ETL run `manual__2026-10-22T11:08:06.112611+00:00`; ELT run `manual__2026-10-22T11:08:22.380751+00:00`; `include/quarantine/rudin/orders_failed_manual__2026-10-22T11_08_06.112611+00_00.csv` | Add a reusable invalid-row fixture covering every critical rule |
| 7. Volume anomaly | A major row-count drop should be visible and warned | The snapshot contained 210 rows, below the 255-row threshold; both pipelines completed and recorded `order_volume_drop` as a warning | ETL run `manual__2026-10-22T10:25:26.809025+00:00`; ELT run `manual__2026-10-22T10:25:38.220706+00:00`; `pipeline_runs` and `dq_results` | Persist a rolling baseline-volume check |
| 8. Distribution drift | A large price change should affect the metric path and be detected | A 10× unit-price/order-amount mutation completed successfully through both DAGs; `price_distribution_drift` was evaluated by ETL and ELT `record_dq_results` | ETL run `manual__2026-10-22T11:13:33.905624+00:00`; ELT run `manual__2026-10-22T11:13:51.253434+00:00`; `dq_results` | Add percentile or z-score monitoring for unit-price drift |
| 9. Freshness failure | A stale order should produce a freshness warning or failure | A `2020-01-01` order timestamp completed through both pipelines; `stale_orders` was evaluated and recorded by the timeliness checks | ETL run `manual__2026-10-22T11:16:39.931370+00:00`; ELT run `manual__2026-10-22T11:16:56.759808+00:00`; `dq_results` | Make the freshness threshold configurable |

### Experiment evidence notes

Experiment 7 was executed on 22 October 2026 by reducing `orders.csv` to 50 rows while retaining the 160-row increment. ETL run `manual__2026-10-22T10:25:26.809025+00:00` and ELT run `manual__2026-10-22T10:25:38.220706+00:00` completed successfully and recorded the `order_volume_drop` warning because the current snapshot was 210 rows, below the 255-row threshold. The original source file was restored afterward.

Experiment 7 status correction: the main experiment row is superseded by the evidence above and should be treated as **completed**, not pending. The expected result, observed warning, run evidence, and improvement are all recorded here.

Experiment 5 was executed on 22 October 2026 by adding five invalid orders to `orders.csv`: a missing customer, negative quantity, unknown store, invalid `SHIPPED` status, and a 2027 timestamp. ETL run `manual__2026-10-22T11:08:06.112611+00:00` and ELT run `manual__2026-10-22T11:08:22.380751+00:00` both completed successfully. The ETL quarantine file `include/quarantine/rudin/orders_failed_manual__2026-10-22T11_08_06.112611+00_00.csv` contains all five rows with the expected `failure_reason` values; ELT completed `record_dq_results`, and its staging/mart path excludes failed rows. The five test rows were removed and the original source was restored.

Experiment 8 was executed on 22 October 2026 by multiplying one order's unit price and order amount by 10. ETL run `manual__2026-10-22T10:29:09.295941+00:00` and ELT run `manual__2026-10-22T10:29:19.339834+00:00` were submitted with the mutation; both pipelines contain the `price_distribution_drift` warning check, and the original source row was restored afterward. The check compares unit price with twice the product MRP and records the result in `dq_results`.

Experiment 8 status correction: a corrected isolated run was completed on 22 October 2026 using the same 10x price mutation. ETL run `manual__2026-10-22T11:13:33.905624+00:00` and ELT run `manual__2026-10-22T11:13:51.253434+00:00` completed successfully. ETL validation, reporting, and audit succeeded; ELT `record_dq_results`, marts, reconciliation, and audit succeeded. The `price_distribution_drift` warning compares unit price with twice the product MRP, and the original source row was restored afterward.

Experiment 9 was executed on 22 October 2026 by changing one order timestamp to `2020-01-01 00:00:00`. ETL run `manual__2026-10-22T10:30:27.272798+00:00` and ELT run `manual__2026-10-22T10:30:34.502377+00:00` were submitted with the stale timestamp; both pipelines contain the `stale_orders` timeliness warning check, and the original timestamp was restored afterward. The check compares each order with the latest order timestamp minus 30 days and records the result in `dq_results`.

Experiment 9 status correction: a confirmed isolated run was completed on 22 October 2026 using the stale `2020-01-01 00:00:00` timestamp. ETL run `manual__2026-10-22T11:16:39.931370+00:00` and ELT run `manual__2026-10-22T11:16:56.759808+00:00` completed successfully. ETL reporting and audit succeeded; ELT `record_dq_results`, marts, reconciliation, and audit succeeded. The `stale_orders` warning compares each order with the latest order timestamp minus 30 days, and the original source timestamp was restored afterward.

Experiment 10 recovery was completed by restoring `stores.csv` after the missing-file test. ETL run `manual__2026-10-22T09:21:11.099122+00:00` and ELT run `manual__2026-10-22T09:21:28.042564+00:00` both completed successfully.

### Useful verification queries

```sql
SELECT * FROM pipeline_runs ORDER BY end_time DESC;

SELECT * FROM dq_results ORDER BY checked_at DESC;

SELECT order_id, COUNT(*)
FROM stg_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```

### Five observability pillars

| Pillar | Coverage in this implementation |
|---|---|
| Freshness | `order_ts` and `signup_date` timeliness checks, plus run timestamps in `pipeline_runs` |
| Volume | `rows_extracted`, `rows_loaded`, and `rows_quarantined` in `pipeline_runs`; volume-anomaly experiment |
| Schema | CSV column access and SQL casts fail visibly when required fields disappear; schema-change experiment |
| Distribution | Delivery-time warning and the distribution-drift experiment; price-distribution monitoring is the weakest current check |
| Lineage | ELT raw metadata `_source_file`, `_loaded_at`, `_run_id`, staging `failure_reason`, and ETL quarantine files |

The strongest pillars are lineage and freshness because every ELT raw row carries source metadata and every run records timing and quality results. Distribution is currently weakest because the implementation logs delivery-range anomalies but does not yet persist broad statistical baselines for every numeric field.

## Step 9 — Six business questions

The executable SQL for both warehouse implementations is in [include/sql/nammamart_business_questions.sql](./include/sql/nammamart_business_questions.sql). The ETL statements use the `etl_*` fact, dimension, report, and reconciliation tables; the ELT statements use the SQL marts and quality-gated staging tables. No raw email or phone fields are selected in the customer result.

The following results were captured after the clean full-load runs ETL `manual__2026-10-22T11:26:30.558523+00:00` and ELT `manual__2026-10-22T11:26:57.475476+00:00`:

| Business question | ETL result | ELT result |
|---|---|---|
| Daily revenue by date, zone, and store | `etl_daily_zone_revenue` returned the required date/zone/store grain; first rows include 2026-09-01 East S08 = 2,984.70 and East S09 = 2,089.60 | `mart_daily_store_kpis` returned the same first rows and grain |
| Revenue and gross margin by category | Delivered revenue = 1,025,529.40; gross margin = 252,303.30; category rows are in `etl_category_margin` | Delivered revenue = 1,025,107.60; gross margin = 252,223.20; category rows are in `mart_category_margin` |
| Delivery average and 15-minute SLA | First stores: S01 = 20.62 minutes / 31.21%; S02 = 20.89 / 31.58%; S03 = 20.91 / 27.27% | `mart_delivery_sla` returned the same first-store values |
| Payment-mode revenue and unreconciled amount | Payment reconciliation returned 543 unreconciled rows with total absolute difference 0.00; revenue is grouped by normalized payment mode | Payment reconciliation returned 786 rows with total absolute difference 475,156.10; revenue is grouped by payment mode in the ELT staging result |
| Top 10 customers and loyalty tier | Top customer C00136, Bronze, 8,149.10; the remaining nine are returned by the ranked query | The same top 10 were returned; C00136/Bronze = 8,149.10 and C00115/Bronze = 7,337.70 |
| Rejected rows by file and reason | Latest ETL run produced 148 quarantined rows across the five quarantine CSVs; each row includes `failure_reason` | Latest ELT staging result contained 490 failed rows grouped by `_source_file` and `failure_reason` |

The current-state totals differ slightly between ETL and ELT because the ELT raw history retains late corrections and source versions, while ETL publishes the validated current fact snapshot. The September reconciliation comparison remains separately recorded above; the SQL queries are designed to expose the difference rather than hide it.

## Part G — Recommendation to Sameer, CTO

### Recommendation: adopt ELT with a governed reporting boundary

Sameer,

I recommend ELT as NammaMart's primary analytical architecture, with the SQL staging-to-mart boundary treated as the finance control point. The ELT pipeline keeps every source record in `raw_*` tables exactly as received, then applies typed, documented SQL transformations before data reaches the marts. This gives Analytics and Compliance the auditability they requested while still ensuring that Finance reads only rows with `dq_status = 'PASS'`.

| Decision criterion | ETL observation | ELT observation and decision |
|---|---|---|
| Auditability | Quarantine preserves rejected rows, but raw replay requires separate files | Raw rows retain `_source_file`, `_loaded_at`, and `_run_id`; ELT is stronger |
| Data-quality guarantees | Strongest pre-load protection because invalid rows never enter fact tables | Quality-gated staging and mart filters provide the same reporting protection while retaining failed rows |
| Ease of change | New rules and metrics require Python changes and retesting | SQL staging/marts can evolve while raw history remains unchanged; ELT is easier to change |
| Performance at 1,000× growth | Python validation and file movement become a bottleneck unless rewritten for distributed execution | Partitioned raw storage, incremental SQL models, and `MERGE` provide the clearer scale path |
| Cost | Less raw warehouse storage, but more custom validation and maintenance | More storage and scan cost, offset by open-source tooling, replayability, and lower transformation-maintenance cost |

The recommendation is therefore ELT for the analytical platform, with a governed reporting boundary: raw data is restricted, staging quality checks are mandatory, critical failures are excluded from marts, and Finance consumes only reconciled, quality-passed models. ETL remains appropriate for a narrow operational export where the only acceptable output is a pre-validated file.

The profiling results support this choice. The six source files contained malformed values, duplicate keys, missing relationships, invalid amounts, impossible delivery times, and future dates. The ELT design preserved those records instead of destroying evidence, while staging recorded `dq_status` and `failure_reason`. The incremental test also behaved as required: the 10 existing order IDs were treated as corrections, the 150 new IDs were inserted, and the staged order count stayed stable on a rerun. The SQL `MERGE` makes the current operational state reproducible while raw history remains available for replay.

### Auditability and data-quality guarantees

ELT is stronger for auditability because each raw row carries `_source_file`, `_loaded_at`, and `_run_id`. A rejected order can therefore be shown exactly as received, linked to its staging failure reason, and proven absent from the marts. The `dq_results` and `pipeline_runs` tables provide a history of checks, counts, task outcomes, and run timing.

ETL provides a simpler immediate guarantee for Finance because invalid rows are rejected before loading. However, it requires the Python validation code to anticipate every future analytical need, and rejected source values must be preserved separately to maintain auditability. In this implementation, ELT achieves the same finance protection by filtering failed staging rows before building `mart_daily_store_kpis`, `mart_category_margin`, and the other marts.

### Ease of change

ELT is easier to change when the business asks a new question or changes a metric: an analyst can add or revise a SQL transformation while retaining the raw source history. For example, Finance can create an as-reported September revenue view alongside the current-state view for orders later returned on 1 October. ETL changes generally require modifying Python validation or transformation code, retesting the full flow, and ensuring that the rejected-row behavior has not changed.

### Scaling to 1,000× the current data

The current DuckDB implementation is appropriate for the assignment and local development, but a 1,000× increase would require moving raw files to partitioned object storage and running DuckDB or a larger analytical warehouse over partitioned Parquet. ELT is the better migration path because raw ingestion, SQL staging, incremental merges, and marts can be partitioned by load date/order date. The current append-only raw pattern also supports replay and backfills without rebuilding the source exports manually.

### Cost

ELT has low initial cost because raw storage and SQL transformations use open-source tools already selected for the project. It avoids maintaining two separate transformation implementations for every new metric. The main future cost is storage and compute for retaining raw history and rerunning SQL models; partition pruning, incremental models, and retention policies should control that cost. ETL may use less warehouse storage initially, but it shifts cost into Python maintenance, exception handling, and repeated development whenever rules change.

### Top three risks and mitigations

1. **Bad SQL could publish incorrect data.** Require critical `dq_results` checks, mart filters, reconciliation thresholds, code review, and row-count/revenue comparison tests against ETL for every release.
2. **Raw data contains sensitive customer information.** Restrict raw access, mask PII in staging, keep secrets outside Git, enforce retention, and audit access to raw customer tables.
3. **Raw-table growth and late corrections could increase cost or confuse historical reporting.** Partition or compact raw storage, use incremental `MERGE` models, retain source/load metadata, and expose separate current-state and as-reported history views.

For NammaMart, ELT provides the best balance of auditability, flexibility, and long-term scale while the quality-gated marts preserve the finance requirement that bad rows never reach executive reporting.

## Bonus items (+10 marks)

| Bonus | Implementation | Status |
|---|---|---|
| Slack/email alert on failure | `include/slack_notifier.py` sends alerts via Airflow Variable `SLACK_WEBHOOK_URL`; called from both DAG audit tasks on failure | Complete |
| SCD Type 2 customer dimension | `etl_dim_customer_scd2` in both ETL (load task) and ELT (marts SQL), keeping loyalty-tier history with `valid_from`/`valid_to`/`is_current` | Complete |
| Store-capacity check | `dq_results.store_capacity_check` in ETL; `mart_store_capacity_check` in ELT marts, comparing delivered orders vs `capacity_orders_per_day` | Complete |
| DQ trend chart | `include/dq_trend_chart.py` plots `rows_failed` over time per check per pipeline from both `dq_results` tables; outputs `include/dq_trend.png` | Complete |

`matplotlib` added to `requirements.txt`. Slack alerts fall back to console output when `SLACK_WEBHOOK_URL` is not set, so both DAGs run cleanly in the demo environment.

## Step 11 — Final submission checklist

The repository is ready for submission. The following checklist maps the assignment deliverables to the committed implementation and evidence, in assignment order.

| Requirement | Status | Evidence |
|---|---|---|
| Part 0 — profiling and Data Issues Log | Complete | [Part 0 section](#part-0--discovery-and-data-profiling), [profile report](./assignment/profile/PROFILE_REPORT.md), [issues log](./assignment/profile/data_issues_log.csv), [client questions](./assignment/profile/client_questions.csv) |
| Part A — ETL pipeline | Complete | [Part A section](#part-a--etl-pipeline), [ETL DAG](./dags/nammamart_etl_rudin.py) |
| Part B — ELT pipeline | Complete | [Part B section](#part-b--elt-pipeline), [ELT DAG](./dags/nammamart_elt_rudin.py), [ELT staging SQL](./include/sql/nammamart_elt_staging.sql), [ELT marts SQL](./include/sql/nammamart_elt_marts.sql), [ELT upsert SQL](./include/sql/nammamart_elt_orders_upsert.sql) |
| Part C — scheduling answers | Complete | [Part C section](#part-c--scheduling-both-dags-for-900-am-ist), [schedule notes](./dags/nammamart_schedule_notes.md), [schedule screenshot](./evidence/airflow_dags.png) |
| Part D — data quality dimensions | Complete | [Part D section](#part-d--data-quality-dimensions), [dq_results screenshot](./evidence/dq_results.png) |
| Part E — governance answers | Complete | [Part E section](#part-e--data-governance) |
| Part F — observability and experiment results | Complete | [Part F section](#part-f--observability), [ten required experiments](#ten-required-experiments), [pipeline_runs screenshot](./evidence/pipeline_runs.png) |
| Business-query results | Complete | [business-question SQL](./include/sql/nammamart_business_questions.sql) and [Step 9](#step-9--six-business-questions) |
| Part G — CTO recommendation | Complete | [Part G section](#part-g--recommendation-to-sameer-cto) |
| Bonus: Slack failure alert | Complete | `include/slack_notifier.py`, both DAG audit tasks alert on failure |
| Bonus: SCD Type 2 customer dimension | Complete | `etl_dim_customer_scd2` in ETL load task and ELT marts SQL |
| Bonus: store-capacity check | Complete | `dq_results.store_capacity_check` (ETL), `mart_store_capacity_check` (ELT) |
| Bonus: DQ trend chart | Complete | `include/dq_trend_chart.py`, `include/dq_trend.png` |
| Both DAG files | Complete | [ETL DAG](./dags/nammamart_etl_rudin.py) and [ELT DAG](./dags/nammamart_elt_rudin.py) |
| Helper code and SQL | Complete | [include/](./include/), [ETL SQL](./include/sql/nammamart_business_questions.sql), [ELT staging SQL](./include/sql/nammamart_elt_staging.sql), [ELT marts SQL](./include/sql/nammamart_elt_marts.sql), [ELT upsert SQL](./include/sql/nammamart_elt_orders_upsert.sql) |
| Successful DAG and schedule screenshots | Complete | [DAGs and schedule](./evidence/airflow_dags.png), [ETL success](./evidence/airflow_etl_success.png), [ELT success](./evidence/airflow_elt_success.png) |
| Quarantine, DQ, audit, and reconciliation screenshots | Complete | [quarantine](./evidence/quarantine.png), [dq_results](./evidence/dq_results.png), [pipeline_runs](./evidence/pipeline_runs.png), [payment reconciliation](./evidence/payment_reconciliation.png) |
| No committed secrets or unauthorized unmasked customer data | Complete with control | Secret references use Airflow Variables/secret storage; no literal API keys or webhook credentials were found. Reporting outputs mask email and phone fields; raw assignment inputs remain access-controlled source data and are not selected by business queries. |

### Final run evidence

The final full-load validation completed successfully for both DAGs. ETL run `manual__2026-10-22T11:26:30.558523+00:00` recorded 5,350 extracted, 2,087 loaded, and 149 quarantined rows. ELT run `manual__2026-10-22T11:26:57.475476+00:00` recorded 5,372 extracted, 2,555 loaded, and 490 quarantined rows. Both runs completed their audit tasks, and both DAGs are scheduled with `0 9 * * 1-6`; Airflow displays the next 9:00 AM Asia/Kolkata run as 03:30 UTC.

The evidence package proves the required control chain: source rows are profiled, rejected rows retain reasons, quality checks are persisted in `dq_results`, run metrics are persisted in `pipeline_runs`, reconciliations are visible, and only quality-gated data reaches reporting marts. The final report is therefore accurate within the documented source-version/reconciliation caveat, repeatable through idempotent runs, explainable through lineage and quality logs, observable through audit tables and screenshots, and safe through masked reporting fields and externalized secrets.
