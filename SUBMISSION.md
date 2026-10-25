# NammaMart Data Pipeline Assignment

**Implementation status:** both DAGs, profiling artifacts, SQL transformations, quarantine paths, reports, and run audits are prepared in this repository. I attempted to start Astro, but Docker could not fetch the Airflow runtime image from `astrocrpublic.azurecr.io` because the registry connection timed out. The Airflow DAGs and acceptance experiments have therefore not been run; UI screenshots, measured task durations, row-count comparisons, and query result snapshots remain pending. No runtime result is claimed below.

## Part 0 — Discovery and profiling

The six supplied CSVs were copied unchanged to [`include/data/nammamart/`](include/data/nammamart/). The source profile and reproducible row-level findings are [`assignment/DATA_PROFILE.md`](assignment/DATA_PROFILE.md), [`assignment/DATA_ISSUES_LOG.csv`](assignment/DATA_ISSUES_LOG.csv), and [`assignment/profile_nammamart.ps1`](assignment/profile_nammamart.ps1).

The profile records 5,372 source rows and 755 findings across the five September exports and the increment: 192 Critical, 563 Warning. Findings include missing keys and values, duplicate IDs, invalid timestamps and statuses, amount-formula errors, invalid quantities and discounts, broken foreign keys, payment mismatches, customer contact format problems, and product/store quality issues. The increment has 150 new delivered orders without successful payment rows and 10 existing orders changed from `DELIVERED` to `RETURNED`; those new payment gaps are treated as pending warnings until Finance chooses a stricter rule. The latest nonfuture source order timestamp is `2026-10-01 22:33:00`; the source timestamps have no timezone offset.

Questions to confirm with NammaMart:

1. Should a delivered order without a successful payment remain in operational revenue as `PENDING`, or be excluded from finance reporting until the payment arrives?
2. Is `Cashh` a valid alias for `Cash on Delivery`, or should those rows be rejected?
3. For duplicate `order_id` values in one file, should both rows be quarantined, or should the source owner designate a deterministic survivor?

## Parts A and B — ETL and ELT implementation

The DAGs are [`dags/nammamart_etl_Tusshar_Tikoo.py`](dags/nammamart_etl_Tusshar_Tikoo.py) and [`dags/nammamart_elt_Tusshar_Tikoo.py`](dags/nammamart_elt_Tusshar_Tikoo.py). Shared rules and masking helpers are in [`include/nammamart_common.py`](include/nammamart_common.py); ELT SQL is under [`include/sql/nammamart/`](include/sql/nammamart/).

| Area | ETL | ELT |
|---|---|---|
| Extract | Five parallel tasks; the order task reads the base and optional increment files. | Five parallel tasks; the order task reads the base and optional increment files. |
| Quality | Python checks; critical rows are excluded and quarantined, warning rows load with a warning status. | SQL staging checks; raw rows remain available, critical rows are marked `FAIL` and excluded from marts, warning rows remain eligible. |
| Quarantine | Run-specific CSVs and `etl_quarantine`, each with a reason and source provenance. Customer contacts are masked; customer location and signup fields and store manager names are omitted. | `elt_quarantine` records reasons and source row numbers. Duplicate customer records are serialized with masked contacts; staging customer rows contain no raw contact or location fields. |
| Warehouse | `etl_fact_orders` and `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`. Fact loads delete and insert matching order IDs in a transaction, making re-runs idempotent. | `raw_*` tables append text values plus load/file/run/row metadata. `stg_*` tables standardize and validate in SQL; `stg_orders` merges on `order_id`. `mart_*` tables answer reporting questions. |
| Increment | Latest increment version wins for an order ID; ten returned-status corrections update existing facts. | Raw appends the increment; staging MERGEs by order ID, retaining the latest increment version for current-state marts. |
| Reconciliation and reports | `etl_payment_reconciliation`, revenue, margin, delivery, payment-mode and top-customer reports. | `mart_payment_reconciliation`, daily store KPIs, category margin, delivery SLA, payment-mode revenue and top customers. |
| Audit | `dq_results`, `pipeline_runs`, plus task logs. | `dq_results`, `pipeline_runs`, plus task logs. |

Both implementations check source schemas and fail with a missing/unexpected-column message if a CSV layout changes. The ELT SQL runner preserves semicolons inside quoted SQL text. Local warehouse and generated runtime/quarantine artifacts are ignored by Git; raw source CSVs remain in the assignment-approved `include/data/nammamart/` location.

## Part C — Schedule

- Cron: `0 9 * * 1-6` in the explicit `Asia/Kolkata` DAG timezone, which schedules 9:00 AM Monday through Saturday.
- Airflow's default UI timezone is UTC, so the run appears at **03:30 UTC**.
- If the same cron were interpreted with a UTC `start_date`/DAG timezone, the run would be at 09:00 UTC, or **2:30 PM IST**.
- If the laptop or Docker is off at 9:00 AM, the local scheduler cannot run. With `catchup=False`, restarting does not backfill every missed interval; the next future scheduled interval is used.
- A 9:00 AM start does not itself guarantee completion before the 9:30 stand-up. The two-hour execution timeout is a maximum, not an SLA measurement; task duration and the 9:30 readiness target still need to be demonstrated in Airflow.

## Part D — Data quality dimensions

The same six dimensions are implemented in both paths. `Critical` failures are quarantined or marked `FAIL`; `Warning` rows are logged and may load.

| Dimension | ETL examples | ELT examples |
|---|---|---|
| Completeness | Required order/customer IDs, delivered delivery time, product category. | SQL required-key, delivered-duration and category checks. |
| Uniqueness | Duplicate order/payment IDs and dimension keys. | Raw-source duplicate counts and canonical dimension keys. |
| Validity | Allowed order/payment statuses and modes, timestamp parsing, contact formats, discount range. | SQL allowed lists, casts, timestamp and contact checks. |
| Accuracy | Positive quantity, amount formula, MRP/cost rule, delivery below 120 minutes. | SQL amount formula, quantity, price and delivery checks. |
| Consistency | Customer/product/store foreign keys and successful payment amount/mode checks. | SQL dimension joins, payment reconciliation and cancelled-order checks. |
| Timeliness | Future order/payment/signup timestamps and latest nonfuture order freshness. | SQL future-time checks and latest nonfuture order freshness. |

The scorecard also records an order-source volume warning when current rows fall below 10% of the prior run's count. The six dimensions plus volume checks are written to `dq_results` with run ID, check name, severity, counts, status and check time.

## Part E — Data governance

### 1. Ownership

The OMS owner owns orders; Finance/Gateway Operations owns payments; CRM owns customer records; Merchandising owns products; Retail Operations owns stores. Data Engineering owns warehouse implementation and quality operations. A proposed rule change (such as the 120-minute limit) needs approval from the owning business domain and Finance where it changes a financial or SLA report, with the rule version recorded in review history.

### 2. Short data dictionary

The ETL fact and dimensions contain these reporting fields. Source line numbers are one-based CSV line numbers, including the header; source fields retain their original values in the supplied CSV and ELT raw layer. Types below describe the loaded DuckDB reporting columns.

#### `etl_fact_orders`

| Column | Type | Meaning / source | Allowed values | PII? |
|---|---|---|---|---|
| `order_id` | VARCHAR | Order key from orders export | Source ID | No |
| `order_ts`, `order_date` | TIMESTAMPTZ, DATE | Source event timestamp and reporting date | Parseable source time | No |
| `customer_id` | VARCHAR | Link to customer dimension | Valid customer key | Indirectly linkable |
| `store_id`, `store_name`, `zone` | VARCHAR | Store key and labels from stores export | Zone: North/South/East/West | No |
| `product_id`, `product_name`, `category` | VARCHAR | Product key and labels from catalogue | Source category | No |
| `quantity`, `unit_price`, `discount_pct`, `order_amount`, `cost_price`, `net_revenue`, `gross_margin` | Numeric | Source measures and derived revenue/margin | Quantity > 0; discount 0–100 | No |
| `payment_mode` | VARCHAR | Standardized source mode | UPI, Card, Cash on Delivery, Wallet | No |
| `order_status` | VARCHAR | Standardized order status | DELIVERED, CANCELLED, RETURNED | No |
| `channel` | VARCHAR | Order channel | Source value | No |
| `delivery_minutes` | Numeric | Source delivery duration | Delivered rows require a value; accepted range below 120 | No |
| `customer_loyalty_tier` | VARCHAR | CRM tier joined by customer key | Bronze, Silver, Gold, Platinum | Customer-linked attribute |
| `within_15_min` | BOOLEAN | Derived delivery SLA flag | true/false | No |
| `payment_reconciliation_status` | VARCHAR | Payment match state | RECONCILED, PENDING, NOT_REQUIRED | No |
| `_source_file`, `_source_row_number` | VARCHAR, BIGINT | Exact source provenance | Source filename and CSV line | No |

#### Dimensions

| Table / column | Type | Meaning / source | Allowed values | PII? |
|---|---|---|---|---|
| `etl_dim_customer.customer_id` | VARCHAR | CRM customer join key | Source ID | Indirectly linkable |
| `etl_dim_customer.masked_email`, `masked_phone` | VARCHAR | Partially masked CRM contacts | Format-dependent mask | Masked personal data |
| `etl_dim_customer.loyalty_tier` | VARCHAR | CRM tier | Bronze, Silver, Gold, Platinum | Customer-linked attribute |
| `etl_dim_product.product_id`, `product_name` | VARCHAR | Product key and name | Source catalogue | No |
| `etl_dim_product.category`, `sub_category`, `brand`, `unit` | VARCHAR | Product classification | Source catalogue | No |
| `etl_dim_product.mrp`, `cost_price` | DOUBLE | Catalogue price and cost | MRP > 0; cost ≤ MRP | No |
| `etl_dim_product.is_perishable` | VARCHAR | Catalogue flag | Source flag | No |
| `etl_dim_store.store_id`, `store_name` | VARCHAR | Store key and display name | Source store master | No |
| `etl_dim_store.zone`, `area`, `pincode` | VARCHAR | Store reporting location | Zone: North/South/East/West | Business location |
| `etl_dim_store.capacity_orders_per_day` | Numeric | Store capacity | Positive | No |

Customer full name, unmasked email, phone, customer location, signup date, and `stores.manager_name` are not carried into ETL reporting dimensions. ELT raw tables preserve source fields for restricted audit use; staging customer rows and all marts expose masked contacts only or omit them.

### 3. Lineage

For `ORD001913`, `orders.csv` line 2 contains `order_ts=2026-09-24 22:56:41`, store `S10`, quantity 2, unit price 222.0, discount 5%, amount 421.80 and status `DELIVERED`. Store `S10` maps to the East zone; `PAY0000001` records a successful UPI payment of 421.80. ETL retains `_source_file` and `_source_row_number` in the fact. ELT retains the untouched row in `raw_orders` and its file/line metadata in staging. ELT is easier to audit because raw values and load/run history are queryable even after the current-state merge.

### 4. Personal data

Direct or linkable personal data includes CRM `full_name`, `email`, `phone`, `city`, `area`, `pincode`, `signup_date`, and customer-linked identifiers/attributes; `stores.manager_name` is also a person's name. Email and phone are masked; customer name, location and signup date are omitted from reporting; `customer_id` remains only as a restricted join key. Masking preserves a customer-level join and limited format signal while reducing exposure; it is not anonymization, so access remains restricted.

India's applicable framework is the Digital Personal Data Protection Act, 2023 and the Digital Personal Data Protection Rules, 2025. Commencement is phased: the notification schedules Rule 4 after one year (13 November 2026) and the principal provisions/rules after 18 months (13 May 2027), so those phases have not commenced as of the 19 October 2026 profile date. The pipeline is designed for purpose limitation, notice/consent governance, safeguards, retention control and breach handling rather than claiming every provision is already in force. See the [Act](https://www.meity.gov.in/static/uploads/2024/02/Digital-Personal-Data-Protection-Act-2023-1.pdf), [Rules](https://www.meity.gov.in/static/uploads/2025/11/53450e6e5dc0bfa85ebd78686cadad39.pdf), and [commencement notification](https://www.meity.gov.in/static/uploads/2025/11/c56ceae6c383460ca69577428d36828b.pdf).

### 5. Access control

Raw tables and source files should be limited to the data platform owner and explicitly authorized privacy/finance investigators; analysts and executives should use masked staging or marts. Quarantine access should be limited to data quality owners and the source-system owner. The local DuckDB file does not implement database roles or encryption in this code, so `.gitignore` alone is not an access-control solution; production deployment needs protected storage, encryption and role-based access.

### 6. Retention

Proposed policy, subject to NammaMart's counsel and statutory/accounting obligations: keep restricted raw extracts for 13 months; keep quarantine rows until issue closure plus 90 days; retain masked customer-level reporting while there is an approved business purpose, then delete or irreversibly aggregate after 24 months of inactivity. Apply legal holds and documented deletion jobs where required. These are proposed operational periods, not periods mandated by the Act.

### 7. Secrets

The current DAGs do not need external credentials and contain no keys or webhook URLs. If alerts or external services are added, store credentials in Airflow Connections/Variables and retrieve them at runtime with Airflow APIs such as `Variable.get()`; never commit values to the DAG or GitHub.

### 8. Audit of a rejected order

`orders.csv` line 127 contains this exact source record:

```json
{"order_id":"ORD000469","order_ts":"2026-09-07 11:52:46","customer_id":"C00076","store_id":"S02","product_id":"P0041","quantity":"0","unit_price":"444.0","discount_pct":"10","order_amount":"0.0","payment_mode":"UPI","order_status":"DELIVERED","channel":"App","delivery_minutes":"18"}
```

The critical rule is `quantity > 0`; ETL stores the row and reason in `etl_quarantine` and does not load it to `etl_fact_orders`. ELT preserves the exact source row in `raw_orders`, marks its current staging row `FAIL`, records the reason and line in `elt_quarantine`, and filters it out of all marts. After a run, verify with `SELECT * FROM etl_fact_orders WHERE order_id='ORD000469'` (zero rows) and `SELECT * FROM stg_orders WHERE order_id='ORD000469'` (one `FAIL` row), then join `elt_quarantine` to `raw_orders` using source file and line to display the untouched fields.

### 9. Change history

The ten increment corrections change current-state September orders from `DELIVERED` to `RETURNED`; their net revenue is therefore removed from the current September revenue in both pipelines. Finance should use the latest corrected status for current reporting. To preserve the prior view too, retain versioned order snapshots with `effective_from`, `effective_to`, `observed_at` and source run/file/line, then publish both current-state and as-known-at-the-time views. ELT raw already supplies the source history needed for replay; the current ELT staging and ETL fact tables are current-state tables and do not themselves provide full SCD-style history.

## Part F — Observability experiments

The run-audit task uses `TriggerRule.ALL_DONE` and writes one `pipeline_runs` row per DAG run. The following acceptance experiments must still be executed on **both** DAGs in Airflow. “Observed” is intentionally marked pending rather than fabricated. Results, UI locations, task durations and screenshots should be filled in after execution.

| # / experiment | Expected | Observed | Evidence location | Follow-up |
|---|---|---|---|---|
| 1. Happy path | Extract tasks run in parallel; critical failures are quarantined; run audit records counts/status. | Pending Airflow run. | Pending: Graph, task logs, `dq_results`, `pipeline_runs`, quarantine. | Record durations and per-file pass/fail counts. |
| 2. Re-run | ETL fact counts stay stable; ELT raw grows by a new source snapshot while current marts do not double-count. | Pending Airflow run. | Pending: row-count queries and run audit. | Confirm current-state reports remain idempotent. |
| 3. Incremental load | 150 new orders append and ten returned statuses update existing keys without duplicate current facts. | Pending Airflow run. | Pending: raw/staging/fact queries and reconciliation. | Record inserted/updated counts and revenue delta. |
| 4. Missing source | Retries occur, then affected task fails and downstream tasks are blocked; audit still records failure. | Pending Airflow run. | Pending: task state, retry logs, `pipeline_runs`. | Add operational alerting if no alert is configured. |
| 5. New bad rows | Null customer, negative quantity, unknown store, invalid status and future timestamp receive the correct critical reasons. | Pending Airflow run. | Pending: quarantine and `dq_results`. | Correct any missing rule before release. |
| 6. Schema rename | Renaming `order_amount` to `total` fails in ETL validation or ELT raw load with a schema mismatch message. | Pending Airflow run. | Pending: failed task log and run audit. | Review source contract and update schema deliberately. |
| 7. Volume anomaly | A 50-order source file produces a warning when volume is below 10% of the previous order-source count. | Pending Airflow run. | Pending: `dq_results` volume check and log. | Confirm baseline and threshold suit normal seasonality. |
| 8. Distribution drift | No explicit price-distribution rule currently blocks a 100× price shift; revenue and margin marts should move sharply. | Pending Airflow run. | Pending: changed mart aggregates and source comparison. | Add store-level unit-price distribution/median guardrail after agreeing a threshold. |
| 9. Freshness | A 30-day-old latest nonfuture order triggers a warning; future-dated rows do not mask staleness. | Pending Airflow run. | Pending: timeliness row in `dq_results`. | Confirm threshold against the real feed SLA. |
| 10. Recovery | Clearing a failed task reruns it and downstream tasks according to Airflow dependencies. | Pending Airflow run. | Pending: Grid/Graph states and task logs. | Record exactly which task instances were cleared. |

Observability pillars: **freshness** is covered by the latest nonfuture order check; **volume** by the 90% drop warning; **schema** by explicit source-column validation; **lineage** by source file and row metadata plus ELT raw run history; **distribution** is currently weakest because no statistical drift threshold is enabled. A price/margin mart comparison is visible, but it is not itself an alert.

## Part G — Recommendation to Sameer

**To: Sameer, CTO — Recommendation: adopt ELT as the primary warehouse pattern, with finance restricted to validated marts.**

The profile found 755 row-level issues, including duplicate keys, invalid amounts, foreign-key failures, malformed values and future timestamps. The increment also changes ten September orders to `RETURNED`. A raw append-only layer with source file, line, run ID and load time gives Analytics and Compliance a reproducible record for explaining those corrections and replaying new SQL rules. The current-state staging merge applies the returned status to reporting without adding a second current fact. ETL still has value as a controlled feed when a consumer requires a prevalidated dataset, and its fact load demonstrates a strong quarantine boundary.

ELT can meet Finance's “no bad row on the CFO dashboard” requirement only if Finance reads the curated marts, never raw or staging. The SQL marts filter `FAIL` rows, retain warnings visibly, and expose payment reconciliation. The 150 new delivered orders without successful payment rows remain explicitly `PENDING`; Finance should agree whether to include them in revenue or publish a reconciled-only close view. Before adoption, run both DAGs and verify September totals match; this submission does not claim that runtime comparison has happened.

At 1,000× volume, a local DuckDB file is a development target, not the final service. ELT rules are easy to change and replay from raw, but source retention, SQL review, incremental partitioning, concurrency and warehouse scale need production design. A managed warehouse can parallelize the same raw/staging/mart contracts; the current local setup has low infrastructure cost but limited availability and access controls.

Top three risks:

1. Raw ELT data contains personal data; local file permissions are not a substitute for encryption, audited roles, retention enforcement and incident response.
2. A bad SQL rule or undocumented threshold can change finance totals; require reviewed, versioned DQ rules and reconcile mart totals against an approved control total.
3. Local DuckDB and laptop/Docker scheduling do not meet production availability or 1,000× scale; migrate execution and storage before making the 9:00 AM SLA contractual.

## Submission evidence still required

- Airflow DAG parse/import check and successful green Graph screenshots for both DAGs.
- Schedule and next-run screenshot showing the IST schedule (Airflow UI may display UTC).
- Quarantine, `dq_results`, `pipeline_runs` and payment-reconciliation screenshots.
- Results from all ten experiments on both DAGs, including before/after row counts and observed task states.
- Result snapshots for the six ETL and six ELT business queries in [`include/sql/nammamart/business_questions.sql`](include/sql/nammamart/business_questions.sql), plus an executed ETL/ELT September totals comparison.
