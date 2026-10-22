# NammaMart Pipeline Submission Notes

## Part C: Schedule

- **Cron:** `0 9 * * 1-6` (09:00, Monday through Saturday).
- **Timezone:** Both DAGs use `pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")`. The schedule fires at 09:00 IST, which is **03:30 UTC**. In UTC, that is the time shown for the scheduled run; the Airflow UI can display a configured user timezone.
- **Catchup:** `catchup=False` avoids launching a backlog of historical daily runs when these DAGs are first enabled or resumed. That suits this local assignment pipeline, where a missed interval should be handled deliberately rather than replayed automatically.
- **Retries and timeout:** Each DAG retries a failed task twice, with a two-minute delay, and gives each task a 25-minute execution timeout.

### Schedule questions

1. **Cron expression:** `0 9 * * 1-6`.
2. **UTC time:** 03:30 UTC.
3. **If `start_date` were naive/UTC:** Airflow would interpret the cron schedule in UTC, so it would run at 09:00 UTC (14:30 IST), 5 hours 30 minutes after the intended local time. The DAG timezone is derived from its timezone-aware `start_date`.
4. **If the laptop or Docker is off at 9 AM:** The local scheduler cannot create or run the scheduled DAG run while it is stopped. With `catchup=False`, Airflow does not automatically replay the missed scheduled interval after restart. Manually trigger/recover the run if that day's report is still needed. The next scheduled run remains 09:00 IST on the next Monday–Saturday schedule.

## Part E: Data governance

### 1. Ownership

Orders belong to Operations; payments to Finance; customers to CRM/Customer Operations and the privacy lead; products to Merchandising; and stores to Store Operations. Data Engineering is custodian of the ETL/ELT warehouse, while each domain owner remains accountable for its meaning and quality.

A change to a business rule such as the 120-minute delivery ceiling requires approval from the Operations process owner and the data-quality steward, with Finance consulted when revenue or reconciliation is affected. The approved definition and effective date should be versioned before deployment.

### 2. Data dictionary

The dictionary below describes the ETL star schema; source names are `orders.csv`, `customers.csv`, `products.csv`, and `stores.csv`. “PII yes” includes identifiers or attributes that are personal when linked to a customer; thus fact rows are personal data in context because they contain `customer_id`.

#### `etl_fact_orders`

| Column | Type | Meaning | Source / derivation | Allowed values / rule | PII? |
|---|---|---|---|---|---|
| `order_id` | VARCHAR | Order key | `orders.order_id` | Required, unique | Yes, linkable |
| `order_ts` | TIMESTAMP | Order time | `orders.order_ts` | Parseable, not future | Yes, linked activity |
| `order_date` | DATE | Reporting date | Date of `order_ts` | Derived | Yes, linked activity |
| `customer_id` | VARCHAR | Customer key | `orders.customer_id` | Must exist in customer dimension | Yes |
| `store_id` | VARCHAR | Store key | `orders.store_id` | Must exist in store dimension | Yes, linked activity |
| `product_id` | VARCHAR | Product key | `orders.product_id` | Must exist in product dimension | Yes, linked activity |
| `quantity` | INTEGER | Units ordered | `orders.quantity` | Greater than 0 | Yes, linked activity |
| `unit_price` | DOUBLE | Price per unit | `orders.unit_price` | Numeric, non-negative | Yes, linked activity |
| `discount_pct` | DOUBLE | Discount percent | `orders.discount_pct` | 0–100 | Yes, linked activity |
| `order_amount` | DOUBLE | Amount after discount | `orders.order_amount` | Equals quantity × unit price × (1 − discount/100), rounded to 2 decimals | Yes, linked activity |
| `payment_mode` | VARCHAR | Payment method | `orders.payment_mode` | UPI, Card, Cash on Delivery, Wallet | Yes, linked activity |
| `order_status` | VARCHAR | Current order state | `orders.order_status` | DELIVERED, CANCELLED, RETURNED | Yes, linked activity |
| `channel` | VARCHAR | Order channel | `orders.channel` | Trimmed/title-cased source value | Yes, linked activity |
| `delivery_minutes` | DOUBLE | Delivery duration | `orders.delivery_minutes` | 0–120 critical; usual range 8–35 | Yes, linked activity |
| `net_revenue` | DOUBLE | Delivered revenue after discount | `order_amount` for DELIVERED; otherwise 0 | Derived | Yes, linked activity |
| `gross_margin` | DOUBLE | Delivered revenue less cost | `net_revenue − quantity × product.cost_price`; otherwise 0 | Derived | Yes, linked activity |
| `within_15_min` | BOOLEAN | Delivered within 15 minutes | Status and `delivery_minutes` | True only for DELIVERED with duration ≤15 | Yes, linked activity |

#### `etl_dim_customer`

| Column | Type | Meaning | Source / derivation | Allowed values / rule | PII? |
|---|---|---|---|---|---|
| `customer_id` | VARCHAR | Customer key | `customers.customer_id` | Required, unique | Yes |
| `masked_email` | VARCHAR | Partially masked email | `customers.email` | Masked or blank; invalid source values masked | Yes, masked/linkable |
| `masked_phone` | VARCHAR | Partially masked phone | `customers.phone` | `NN******NN` for 10 digits; otherwise masked | Yes, masked/linkable |
| `city`, `area` | VARCHAR | Customer location | `customers.city`, `customers.area` | Trimmed source values | Yes, linked attributes |
| `pincode` | VARCHAR | Customer postal code | `customers.pincode` | Six digits (warning check) | Yes, linked attribute |
| `signup_date` | DATE | Registration date | `customers.signup_date` | Parseable, not future | Yes, linked attribute |
| `loyalty_tier` | VARCHAR | Customer tier | `customers.loyalty_tier` | Bronze, Silver, Gold, Platinum | Yes, linked attribute |
| `is_active` | BOOLEAN | Account activity flag | `customers.is_active` | True / False | Yes, linked attribute |

#### `etl_dim_product`

| Column | Type | Meaning | Source / derivation | Allowed values / rule | PII? |
|---|---|---|---|---|---|
| `product_id` | VARCHAR | Product key | `products.product_id` | Required, unique | No |
| `product_name` | VARCHAR | Display name | `products.product_name` | Trimmed source value | No |
| `category`, `sub_category` | VARCHAR | Product classification | `products.category`, `products.sub_category` | Source catalogue values; ETL title-cases | No |
| `brand` | VARCHAR | Brand | `products.brand` | Trimmed source value | No |
| `mrp` | DOUBLE | Maximum retail price | `products.mrp` | Greater than 0 | No |
| `cost_price` | DOUBLE | Unit cost | `products.cost_price` | Non-negative and ≤ MRP | No |
| `unit` | VARCHAR | Selling unit | `products.unit` | Trimmed source value | No |
| `is_perishable` | BOOLEAN | Perishability flag | `products.is_perishable` | True / False | No |
| `launch_date` | DATE | Product launch date | `products.launch_date` | Parseable date | No |

#### `etl_dim_store`

| Column | Type | Meaning | Source / derivation | Allowed values / rule | PII? |
|---|---|---|---|---|---|
| `store_id` | VARCHAR | Store key | `stores.store_id` | Required, unique | No |
| `store_name` | VARCHAR | Store display name | `stores.store_name` | Trimmed source value | No |
| `zone` | VARCHAR | Operating zone | `stores.zone` | North, South, East, West (ETL title case) | No |
| `area` | VARCHAR | Store locality | `stores.area` | Trimmed source value | No |
| `pincode` | VARCHAR | Store postal code | `stores.pincode` | Six digits (warning check) | No |
| `open_date` | DATE | Store opening date | `stores.open_date` | Parseable date | No |
| `capacity_orders_per_day` | BIGINT | Daily order capacity | `stores.capacity_orders_per_day` | Integer source value | No |

ELT keeps source columns as VARCHAR in `raw_*`, types and masks them in `stg_*`, and applies SQL normalization there. For example, ELT currently stores `zone` and `category` in uppercase, while ETL title-cases them; comparisons should normalize casing.

### 3. Lineage

Example KPI: `mart_daily_store_kpis` for **2026-09-01 / S01 (NammaMart Hebbal)** has `delivered_orders = 4` and `net_revenue = ₹2,247.80`. It comes from `orders.csv` physical lines **444, 728, 1369, and 1654** (`ORD000058`, `ORD000025`, `ORD000013`, `ORD000066`); their `order_amount` values sum to ₹2,247.80.

ETL can trace retained fact rows by `order_id` back to the supplied source CSV, but its fact table has no persisted source filename or row number and rejected rows are only in quarantine. ELT is easier: `raw_orders` preserves the original values with `_source_file` and `_run_id`, and the staging/mart filters and joins are SQL; it still does not persist a CSV row number, so adding `_source_row_number` would make row-position lineage direct.

### 4. Personal data

Direct customer identifiers are `customers.full_name`, `customers.email`, and `customers.phone`; in context, all customer attributes and `customer_id` are personal data, and all `orders`/`payments` fields are linkable personal transaction data through `customer_id`/`order_id`. `stores.manager_name` is personal data about an employee.

ETL omits full names from its customer dimension and masks email/phone; ELT masks these in staging but intentionally retains exact raw input in `raw_customers`. Masking preserves a stable join and limited analytical utility while raw data remains restricted; masking is pseudonymization, not anonymization. India’s DPDP Act, 2023 and DPDP Rules, 2025 apply to this digital personal data, with phased commencement; they require lawful-purpose processing, clear notice and consent or another permitted ground, reasonable safeguards, rights/grievance handling, breach processes, and erasure when the purpose ends unless retention is legally required ([Act](https://www.meity.gov.in/static/uploads/2024/02/Digital-Personal-Data-Protection-Act-2023.pdf), [Rules](https://www.meity.gov.in/static/uploads/2025/11/53450e6e5dc0bfa85ebd78686cadad39.pdf)).

### 5. Access control

Raw tables should be limited to ingestion operators and the privacy/data steward; raw `customers` with unmasked contacts should be visible only to specifically authorized CRM/privacy staff when necessary. Staging and quarantine should be limited to data engineering, quality stewards, and the relevant source owner; marts should be available to Finance, Operations, and Analytics according to job need.

ETL quarantine masks customer name/email/phone; ELT has no separate quarantine table, so failed staging rows remain restricted and raw records remain highly restricted. The assignment’s local DuckDB files do not enforce per-role grants, so these are target access policies that require a role-controlled production warehouse to enforce.

### 6. Retention

Proposed policy, subject to Finance/Privacy approval and legal holds: retain raw source records for 13 months for replay and reconciliation; retain quarantine files for 90 days after the issue is resolved, then delete them while retaining non-sensitive run/check metadata. Keep masked customer dimensions only while the account is active and for 12 months after closure, then erase them unless a documented statutory or dispute hold requires longer retention.

Deletion must cover warehouse files, quarantine copies, exports, and expired backups; retain only non-identifying aggregate metrics where practical. These are internal proposal periods, not statements of statutory retention duration.

### 7. Secrets

Neither DAG currently uses a credential or Slack webhook. The repository ignores `.env`; if a webhook is added, store it as an Airflow Variable or in a configured secrets backend and retrieve it with `Variable.get("SLACK_WEBHOOK_URL")`.

Never place a credential in a DAG or GitHub: repository history and shared code expose it to people and systems that should not have access, and deleting it from the latest commit does not reliably remove past copies.

### 8. Audit

Rejected example: `include/data/nammamart/orders.csv`, physical line 275, exactly `ORD001037,2026-09-14 13:29:58,C00385,S13,P0104,1,525.0,10,472.5,UPI,DELIVERED,App,480`. ETL’s quarantine records `delivery_minutes is outside the plausible 0..120 minute range`; `ORD001037` is excluded before `etl_fact_orders` is loaded.

ELT retains the exact row in `raw_orders` with `_source_file`/`_run_id`; `stg_orders` records `dq_status='FAIL'` and the same reason, and the mart SQL excludes failed rows with `dq_status <> 'FAIL'`. Finance can verify ETL exclusion with `SELECT COUNT(*) FROM etl_fact_orders WHERE order_id='ORD001037'` (expect 0), and inspect ELT’s `stg_orders` row and failure reason before checking the mart filter.

### 9. Change history

After the Oct 1 increment, ELT upserts the order’s current status to RETURNED, so September delivered revenue is restated downward by that order’s `order_amount`; the raw base export and increment remain available in `raw_orders`. ETL currently reads the September `orders.csv` snapshot only, so it still counts the order as delivered and its September revenue remains as originally reported.

Finance should use the corrected/restated figure for current net revenue, while preserving the originally reported figure for audit. Keep both with a dated revenue snapshot and an append-only order-status history (effective time, loaded time, prior/new status, run id), or equivalent SCD Type 2 history; extend ETL to consume the increment if the ETL view must also restate.

## Part F: Observability tests

Both DAGs now finish with an `audit_run` task using `trigger_rule="all_done"`. It writes one `pipeline_runs` row per `run_id` and `dag_id`, including start/end timestamps, extracted/loaded/quarantined row counts, and status. Re-running the audit for the same DAG run replaces that row instead of duplicating it. When upstream work failed, the audit writes `status='failed'` and then fails without retrying, so the Airflow run is visibly failed too. ELT raw-row counts include every CSV appended on that run; ETL loaded counts are the fact-table snapshot size. ELT quarantine counts are failed staging rows tagged with that run ID.

The experiments below were run on 22 October 2026 with `astro dev run dags test` against both DAGs. Test inputs were temporary replacements of files under `include/data/nammamart/`; each was restored from a byte-for-byte backup after its experiment. Airflow task logs and run history were inspected; recovery was exercised with Airflow's Clear action in the UI and its equivalent `airflow tasks clear` command.

### 1. Happy path

**Expected →** both DAGs finish successfully; the five ETL extracts can run concurrently; source, passed, and rejected counts are visible. **Observed →** ETL: 5,212 source rows, 2,007 fact rows, 817 rejected across all five files (orders 2,412/405, payments 2,228/393, customers 405/8, products 152/10, stores 15/1; values are rows in/quarantined). ELT appended 5,372 rows including the increment, with 813 failed staging rows. ETL completed in about 11 seconds and ELT in about 8 seconds; extract task timelines show the ETL fan-out. **Where →** Airflow task-instance timeline/logs and `pipeline_runs`. **Change →** retain these source-volume baselines as assignment-specific alert thresholds and calibrate them to a real daily feed before production.

### 2. Re-run / idempotency

**Expected →** ETL fact count stays fixed; ELT raw grows because it preserves each ingestion run, while staging and marts do not double-count orders. **Observed →** ETL fact count remained 2,007 after the second run. ELT appended another 5,372 raw rows (2,572 of those were orders from the base and increment files); staging still had one current row per `order_id`, including the 10 corrections. **Where →** `etl_fact_orders`, `raw_orders`, `stg_orders`, and the DAG logs. **Change →** keep append-only raw history and ensure all consumer marts use deduplicated staging keys.

### 3. Incremental load

**Expected →** ELT appends the 160-row increment, updates the 10 corrected order statuses, and adds 150 new orders; ETL should reflect those corrections only if it consumes the increment. **Observed →** ELT staged the ten corrected orders as `RETURNED` and the 150 new orders as `DELIVERED`, with no duplicate staged order IDs. ETL still reads `orders.csv` only and therefore does not apply the increment. **Where →** ELT `raw_orders` by `_source_file`/`_run_id`, `stg_orders`, and ETL `etl_fact_orders`. **Change →** add incremental extraction and an order-key upsert to ETL if Finance needs its report restated too.

### 4. Missing source file

**Expected →** the source-reading task retries, then fails; dependent tasks are marked `upstream_failed`, with the reason visible. **Observed →** ETL `extract_products` and ELT `load_raw_layer` each raised `FileNotFoundError` for `products.csv`, retried twice at the configured two-minute interval, then failed; the audit task still ran and recorded `status='failed'`. **Where →** task logs, Airflow run/task history, and `pipeline_runs` (ETL extracted 5,060 rows before failure; ELT 0 because its raw load is one transaction). **Change →** add an alert on failed or delayed runs; retries alone do not notify the owner.

### 5. Five new bad order rows

**Expected →** missing customer, negative quantity, unknown store, `SHIPPED`, and a future timestamp each receive a specific reason; ETL quarantines them and ELT marks them failed in staging and records the relevant checks. **Observed →** both DAGs completed, each with five additional source rows; all five appeared with `dq_status='FAIL'` and reasons in `stg_orders`, and ETL quarantine increased by five. The relevant required-field, quantity, store-key, status, and future-date checks were present in `dq_results`. **Where →** run-specific ETL quarantine CSVs, ELT `stg_orders`, `dq_results`, and `pipeline_runs`. **Change →** retain these isolated fixtures as regression cases; `dq_results` is check-level, so use staging/quarantine for the individual row reason.

### 6. Schema change

**Expected →** renaming `order_amount` to `total` should stop ingestion with a clear schema error, not silently load null amounts. **Observed →** ETL failed at `extract_orders` with `ValueError: Schema mismatch`. The first ELT trial showed a gap: DuckDB accepted the missing column and staging marked many rows failed instead of rejecting the changed header. I added an explicit CSV-header check to `load_raw_layer`; the rerun then failed loudly with the same schema mismatch before committing any raw rows. **Where →** ETL/ELT task logs and the failed `pipeline_runs` rows. **Change →** alert on schema mismatch and require an approved mapping before accepting a renamed field.

### 7. Volume anomaly

**Expected →** a 50-order input (over 90% below the 2,412-row assignment baseline) should warn without hiding the row counts. **Observed →** the new orders volume check recorded `WARN` with one failed file-level check in both `dq_results` tables. ETL loaded 47 valid order rows; ELT appended the 50-row orders file plus the other source files (3,010 raw rows on that run). **Where →** `dq_results`, `pipeline_runs`, and the DAG task logs. **Change →** replace the fixed 2,412 baseline with a rolling baseline or a client-approved daily range for production.

### 8. Distribution drift

**Expected →** multiplying S01 `unit_price` values by 100 should produce an outlier alert or visibly change store revenue. **Observed →** no dedicated distribution-drift check exists. Because `order_amount` was left unchanged, the existing amount-formula accuracy check rejected the affected rows: ETL's load fell from 2,007 to 1,859 rows, and S01 delivered revenue would fall by ₹72,996.75 (129 delivered orders in the valid fact snapshot). ELT likewise marked affected rows failed and excluded them from marts. **Where →** ETL formula check in `dq_results`, failed ELT rows in `stg_orders`, and `mart_daily_store_kpis` / ETL report logs. **Change →** add per-store price/revenue distribution thresholds that detect extreme but internally consistent values too.

### 9. Freshness

**Expected →** a feed whose latest `order_ts` is 30 days old should warn on timeliness. **Observed →** ETL's `order_ts is older than one day (stale)` check warned for all 2,412 modified order rows; ELT's `orders.not_stale_over_one_day` also warned (2,554 of 2,555 staged rows were stale in that run, including the unchanged increment feed). **Where →** `dq_results` and validation/staging task logs. **Change →** alert on the freshness warning and report the newest source timestamp in the mart/run audit for faster diagnosis.

### 10. Recovery after failure

**Expected →** after restoring the source, Clear should rerun the failed task and its downstream tasks, not unrelated upstream work. **Observed →** on the failed ELT schema-change run, I restored the original CSV and used Airflow UI **Clear** on `load_raw_layer` with downstream tasks selected. The five tasks (`load_raw_layer`, `build_staging_layer`, `build_mart_layer`, `compare_september_totals`, `audit_run`) reran and succeeded; the run's audit record was updated to `success`. For ETL, I cleared `validate_orders_task` with downstream selected after a successful baseline run; the failed validation and its downstream tasks reran successfully while the five source extracts and three master validations stayed at their original attempt. **Where →** Airflow run graph/task-instance history and `pipeline_runs`. **Change →** ETL intermediate pass/fail files now live in the shared, gitignored `include/.nammamart_etl_work/` directory and are retained per run, so cleared tasks can read upstream outputs across worker containers. Keep that directory access restricted because it contains source PII.

### Five observability pillars

- **Freshness:** covered by the one-day `order_ts` timeliness checks and run start/end times.
- **Volume:** covered by the new warning when the base orders file is more than 90% below the assignment baseline.
- **Schema:** ETL validates exact CSV headers; ELT now validates headers before raw insertion.
- **Distribution:** weakest coverage. The pipelines catch amount-formula mismatches, but do not yet track per-store price/revenue distributions or detect internally consistent outliers.
- **Lineage:** strongest coverage in ELT because raw rows retain `_source_file` and `_run_id`, and staging retains row-level failure reasons; add a source row number for exact physical-line lineage. ETL can trace accepted/rejected keys through fact/quarantine outputs but does not persist source row numbers.

## Part G: Recommendation to the client

**To: Sameer, CTO — Recommendation: adopt ELT, with a gated finance mart.**

Keep every source record in the restricted raw layer, transform and validate it in SQL, and publish finance tables only from passing, reconciled staging rows. The gate must prevent critical DQ failures and unresolved payment mismatches from entering the finance mart; the reconciliation table should still expose those exceptions for Finance to resolve.

The run results show why the raw history matters. ELT retained 5,372 rows per ingestion, including the 160-row increment, and staging marked 813 rows as failed. The increment corrected ten existing order statuses to `RETURNED` and added 150 orders. ETL produced a clean 2,007-row fact snapshot and quarantined 817 rows, but it reads only the September `orders.csv`; it therefore continued to count those ten orders as delivered. ELT can recalculate the current view while preserving both source versions for audit and replay. For finance assurance, ETL currently has the simpler guarantee because it loads only rows that pass Python validation. ELT should replace that assurance only after its mart promotion gate is explicit, tested, and blocks bad rows by construction.

ELT is also easier to adapt: raw values remain available while rules and business logic change in modular SQL. That proved useful when the schema experiment exposed silent acceptance of a renamed column; a header check now fails the load before raw insertion. The experiment also showed that catching formula errors does not catch internally consistent price drift, so distribution checks remain a needed improvement. On this assignment-sized dataset, ELT finished in about 8 seconds and ETL in about 11 seconds. That small difference is not a meaningful 1,000× forecast. SQL transformations avoid moving every row through Python, but the current local DuckDB file and CSV inputs are still single-machine components. Benchmark growth before committing to a production layout; partition raw files in open formats and move the warehouse to an appropriately sized open-source SQL platform if local DuckDB stops meeting the SLA.

Both designs use open-source tools and have no license fee. ELT adds storage, backup, retention, and access-control costs because raw history—including unmasked customer data—is kept. At scale, compute, storage, and operating the privacy controls will cost more than the software licenses; keeping only the useful audit history and automating retention will help manage that cost.

**Top three risks of the recommended design**

1. **A bad row reaches Finance if a mart query bypasses DQ or reconciliation status.** Require a central promotion gate, test it with rejected rows, and alert on every critical failure and unresolved mismatch.
2. **Raw-layer PII is exposed or retained too long.** Restrict raw access to approved operators, encrypt storage and backups, audit access, and enforce the retention and deletion policy.
3. **Single-node performance or storage misses the SLA as volume grows.** Track run duration and volume, load-test at projected scale, and set a migration threshold for partitioned storage and a larger open-source SQL engine.

## Bonus features

- **Failure alert:** both DAGs use a DAG-level failure callback that reads `SLACK_WEBHOOK_URL` from an Airflow Variable and posts the DAG/run ID to Slack. Configure it with `astro dev run variables set SLACK_WEBHOOK_URL '<incoming-webhook-url>'`. No webhook is configured in this local setup, so the callback logs that the alert was skipped; no credential is stored in the repository.
- **Customer loyalty SCD Type 2:** ELT writes `dim_customer_loyalty_history` with `customer_id`, `loyalty_tier`, `valid_from`, `valid_to`, `is_current`, and `source_run_id`. A changed tier closes the current version and inserts a new one; replaying an unchanged batch creates no extra version. A synthetic Bronze → Silver change and replay produced two versions with exactly one current row. The populated warehouse currently has one current version for each of 397 valid customer keys.
- **Store capacity:** both pipelines compare valid daily orders per store with `capacity_orders_per_day`, recording an over-capacity store-day as a Volume/Warning check without rejecting its orders. The latest test run flagged 30 of 418 ETL store-days and 31 of 432 ELT store-days; ELT includes the additional increment file.
- **DQ trend chart:** each DAG writes a dependency-free SVG from its `dq_results` history: [ETL chart](./include/reports/dq_trend_etl.svg) and [ELT chart](./include/reports/dq_trend_elt.svg). The latest runs generated both files successfully.
