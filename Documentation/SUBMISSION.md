# NammaMart Step 10 — observability experiments

The executable test notebook is [step10_observability_experiments_test.ipynb](./step10_observability_experiments_test.ipynb). It runs all ten required experiments against both pipeline implementations and produces 20 report rows: one ETL row and one ELT row per experiment.

## Required experiment matrix

| # | Experiment | Expected evidence | Change recorded by the notebook |
|---:|---|---|---|
| 1 | Normal successful run | Both implementations complete; row counts, quarantine counts, and audit records are visible | Keep run audit and row-count checks |
| 2 | Re-run for idempotency | ETL fact rows and ELT base raw/staging rows remain stable | Keep replacement/upsert semantics and add row-count assertions |
| 3 | Incremental load | 150 inserts, 10 updates, no duplicate staged order IDs | Monitor inserted/updated counts |
| 4 | Missing source file | Both fail loudly; Airflow shows retries and a failed source task | Add source-file preflight and alerting |
| 5 | New invalid data | Five injected defects receive matching quarantine or `dq_status` reasons | Assert every injected defect is recorded |
| 6 | Schema change | Missing `order_amount` fails loudly instead of silently loading incorrect data | Add a formal schema contract |
| 7 | Volume anomaly | A greater-than-90% drop is detected | Add a run-over-run volume check |
| 8 | Distribution drift | Price/amount consistency catches the drift; store metrics change | Add store-level distribution baselines |
| 9 | Stale data | A 30-day-old latest order raises a freshness warning | Add latest-order-age checks |
| 10 | Recovery after failure | After fixing the file, clearing failed/downstream Airflow tasks succeeds | Verify `pipeline_runs` retains the failed and recovered attempts |

## Restoration guarantee

Each experiment runs inside `TemporaryDirectory` after copying the six source files. Missing files and modified rows are applied only to that temporary copy. Cleanup runs when the experiment exits, including when a test raises an exception, so the original files are restored automatically.

## Airflow-specific evidence

The notebook tests the same business-logic modules imported by the DAGs. Experiments 4, 6, and 10 additionally require Airflow UI/log evidence for retry states, task failures, and Clear-based recovery. Add those screenshots after running [nammamart_etl_samy.py](./dags/nammamart_etl_samy.py) and [nammamart_elt_samy.py](./dags/nammamart_elt_samy.py).

## Observability pillars

- **Freshness:** weakest until the latest-order-age check is added.
- **Volume:** requires an explicit run-over-run threshold check.
- **Schema:** missing required columns fail loudly; a formal contract is recommended.
- **Distribution:** partially covered by amount consistency; store-level baselines are recommended.
- **Lineage:** strongest because ETL quarantine and ELT raw tables preserve traceability.

## Step 11 — business questions

The executable notebook is [step11_business_questions_test.ipynb](./step11_business_questions_test.ipynb). The SQL query files are [business_questions_etl.sql](./include/sql/business_questions_etl.sql) and [business_questions_elt.sql](./include/sql/business_questions_elt.sql).

The notebook answers these questions for both pipelines:

1. Daily net revenue by zone and store.
2. Revenue and gross margin by category.
3. Average delivery time and percentage within 15 minutes.
4. Revenue by payment mode and unreconciled amount.
5. Top 10 customers by spend and loyalty tier.
6. Rejected rows by file and failure reason.

It also compares like-for-like September delivered net revenue. If totals differ, it lists the exact order IDs included by only one pipeline so the difference can be explained from data, rather than reported as an unexplained mismatch. Results are exported to `assignment/step11_business_questions/` when the notebook is run.

## Step 12 — governance documentation

### Dataset ownership

| Dataset | Business owner | Technical custodian | Change approval |
|---|---|---|---|
| Orders and delivery facts | Operations and Finance | Data Engineering | Operations + Finance for business rules; Data Engineering for implementation |
| Payments and reconciliation | Finance | Data Engineering | Finance approves payment and revenue definitions |
| Customers | CRM / Customer Operations | Data Engineering | CRM and Compliance approve personal-data use |
| Products | Merchandising | Data Engineering | Merchandising approves catalogue and pricing rules |
| Stores and capacity | Operations | Data Engineering | Operations approves store and capacity rules |
| Raw, staging, marts and quarantine | Data Engineering, with Finance/Compliance access as needed | Data Engineering | Data Governance Council approves retention and access changes |

The 120-minute delivery limit and other quality rules require Operations and Finance approval because they affect service reporting and revenue. Data Engineering owns the implementation, tests, and deployment.

### Data dictionary

| Table / column | Type | Meaning and source | Allowed values / rule | PII |
|---|---|---|---|---|
| `etl_fact_orders.order_id` | string | Order key from `orders.csv` | Unique and non-empty | No |
| `etl_fact_orders.order_date` | date | Date derived from `orders.order_ts` | Valid timestamp | No |
| `etl_fact_orders.net_revenue` | decimal | Delivered `order_amount` from orders | Zero for non-delivered orders | No |
| `etl_fact_orders.gross_margin` | decimal | Net revenue less quantity × product cost | Derived finance metric | No |
| `etl_dim_customer.customer_id` | string | Customer key from `customers.csv` | Must exist in orders | No |
| `etl_dim_customer.full_name` | string | Customer name from customers | Restricted reporting field | Yes: indirect identifier |
| `etl_dim_customer.email_masked` | string | Masked customer email | Masked before reporting | Yes: derived from PII |
| `etl_dim_customer.phone_masked` | string | Masked customer phone | Masked before reporting | Yes: derived from PII |
| `etl_dim_customer.loyalty_tier` | string | Loyalty tier from customers | SILVER, GOLD, PLATINUM, or source-approved value | No |
| `etl_dim_product.category` | string | Product category from products | Non-empty catalogue category | No |
| `etl_dim_product.cost_price` | decimal | Product cost from products | Positive and not greater than MRP | No |
| `etl_dim_store.zone` | string | Store zone from stores | North, South, East, or West | No |
| `etl_payment_reconciliation.reason` | string | Reason an order and payment do not reconcile | Controlled reason text | No |
| `dq_results.status` | string | Result of a quality rule | PASS or FAIL | No |
| `dq_results.severity` | string | Impact of a quality failure | Critical or Warning | No |

Raw tables retain source columns as text plus `_loaded_at`, `_source_file`, and `_run_id`. ELT staging casts types, masks customer email and phone, and adds `dq_status` and `failure_reason`. The mart tables contain business-ready aggregates and masked customer attributes only.

### Data lineage

The lineage for September daily net revenue is:

`include/data/nammamart/orders.csv` → ETL extract/quality checks → `etl_fact_orders` → `etl_daily_zone_revenue`

or:

`include/data/nammamart/orders.csv` → `raw_orders` → `stg_orders` → `mart_daily_store_kpis`.

Products and stores are joined through their corresponding dimensions. The ETL quarantine files and `dq_results` explain rejected source rows. ELT is easier to replay because raw source values remain available with their source file and run ID.

### Personal-data handling

Customer `full_name`, `email`, `phone`, `city`, `area`, and address-related `pincode` are treated as personal or potentially identifying data. Raw customer data is retained only for approved audit users. ETL creates `email_masked` and `phone_masked`; ELT creates masked fields in staging and marts. Masking preserves analytical joins and customer segmentation without exposing direct contact details. Raw PII must not be copied into logs, screenshots, marts, or notebooks.

The applicable Indian framework is the Digital Personal Data Protection Act, 2023. NammaMart should document purpose, access, retention, security safeguards, and deletion/withdrawal handling, and should process only the personal data needed for the stated business purpose.

### Access control

- Raw tables and raw customer files: restricted to Data Engineering, authorised CRM, Finance, and Compliance users. Unmasked `raw_customers` is stored in a separate owner-only file (`*_pii.duckdb`), not in the shared ELT warehouse; see "Raw PII exposure fix" below.
- Quarantine: restricted to Data Engineering, Operations data stewards, and Compliance investigators.
- Staging: analysts may access masked fields; direct raw PII remains restricted.
- Marts and business reports: Finance, Operations, and approved analysts receive least-privilege read access.
- Airflow administration and warehouse write access: limited to Data Engineering service identities.

### Retention

Raw records should be retained for the approved audit and replay period, for example seven years if Finance and Compliance approve that period. Quarantine records should be retained until the incident is resolved plus the incident-retention period. Masked marts should follow the analytics retention policy. After expiry, records must be deleted or irreversibly anonymised by a logged, access-controlled retention job. Retention periods must be confirmed with Legal and Compliance before production use.

### Secrets management

No API keys, Slack webhook URLs, passwords, tokens, or credentials belong in this repository. The repository contains only code that reads values from Airflow Connections/Variables or environment variables, such as `AZURE_OPENAI_API_KEY` and `SLACK_WEBHOOK_URL`. Values are configured locally or in the deployment secret store, excluded by `.gitignore`, and never printed in logs. If a secret is exposed, revoke and rotate it immediately and inspect Git history.

### Auditability

ETL records `run_id`, `dag_id`, timing, extracted rows, loaded rows, quarantined rows, and status in `pipeline_runs`; it writes critical rejected records with `failure_reason` and stores rule-level results in `dq_results`. ELT preserves every raw source row with `_source_file`, `_loaded_at`, and `_run_id`, then records row-level `dq_status` and `failure_reason` in staging. Finance can select a rejected `order_id` from quarantine, show the original raw row, identify the failed rule, and verify that it is absent from `etl_fact_orders` and finance revenue totals.

### Change history and returned orders

The October increment contains both 150 new orders and 10 corrections. ETL upserts by `order_id`, so a September order corrected to `RETURNED` is reflected in the current fact view and no longer contributes delivered revenue. ELT appends the increment to raw storage and uses the latest row per `order_id` in staging, preserving both the original and corrected versions for audit while presenting the latest state in marts.

Finance needs both views: a current operational view reflecting the return and an immutable historical/as-of view showing what was reported before the correction. Keep raw ELT versions and add effective timestamps or an SCD/event-history table before production so either view can be reconstructed.

### Repository safety rules

Never commit API keys, Slack webhook URLs, passwords, other credentials, or unmasked customer data outside the permitted input-data folder. The governance validation notebook is [step12_governance_documentation_test.ipynb](./step12_governance_documentation_test.ipynb); it checks this document, scans text/config files for secret-shaped values, and reports customer-data files outside the approved input paths.

## Step 13 — recommendation

The one-page CTO recommendation is [RECOMMENDATION_TO_CTO.md](./RECOMMENDATION_TO_CTO.md), and its validation notebook is [step13_recommendation_test.ipynb](./step13_recommendation_test.ipynb).

The recommendation is to adopt **ELT as the primary warehouse architecture**, with a certified finance-facing contract that publishes only approved rows and metrics. It compares auditability, data-quality guarantees, rule-change effort, performance at 1,000× volume, cost, and operational risks using the observed NammaMart results. It ends with exactly three risks: incorrect `dq_status` filtering, warehouse cost/latency growth, and overexposure of raw PII.

## Step 14 — final submission validation

The validation notebook is [step14_final_submission_validation.ipynb](./step14_final_submission_validation.ipynb). It checks the final repository layout, parses both DAG files without importing Airflow, verifies both expected DAG IDs, and provides the exact Astro commands for final runtime validation:

```text
astro dev start
astro dev run dags list-import-errors
astro dev run dags test nammamart_etl_samy
astro dev run dags test nammamart_elt_samy
```

The required final artifacts are present: both NammaMart DAGs, `include/sql/`, `include/data/nammamart/`, `include/quarantine/samy/`, `include/warehouse_samy.duckdb`, and this `SUBMISSION.md`. The notebook also records whether Astro is installed before attempting those commands.

## ETL / ELT reconciliation fix

Part B requires the ELT mart totals to match the ETL totals. Before this fix they did not: ETL loaded **1,851** clean orders and ELT passed **2,318**, a gap of 467 orders. A row-by-row comparison of the order IDs showed the gap came from the two pipelines applying different quality rules, not from a data problem. Both pipelines now apply one shared rule set.

### Root causes

| Cause | Orders affected | Fix |
|---|---:|---|
| ETL dropped orders whose customer, product or store failed a Critical check (for example, a store with a missing zone or zero capacity). ELT only checked that the key existed in the raw master. | ~440 | `stg_orders` accepts an order only when its customer, product and store **passed** staging (`dq_status = 'PASS'`). |
| ETL also dropped orders for customers and products that only had a duplicate row, even though it already deduplicates those dimensions. | (part of the ~440) | ETL keeps the first dimension row and no longer blocks the key itself. The extra copies are still logged as Critical. |
| ELT's uniqueness check never fired, because duplicates were removed before they were counted. | 24 | `stg_orders` counts duplicates within each load, before the latest-row upsert. Repeated `order_id`s in one file now fail, as they do in ETL. |
| ETL quarantined `upi`, `UPI `, `CARD`, `cod`, `Cashh` and `delivered` instead of standardising them, as Part A asks. | 12 | Shared `standardise_values()` runs before validation and transformation, using the same mapping as the staging SQL. |
| The "delivery over 120 minutes" rule existed only in ETL. | 6 | Added to `stg_orders`. |
| An unparseable `order_ts` passed ETL and failed ELT with a blank `failure_reason`. | 3 | Both pipelines now reject it as `order_ts is not a valid timestamp` (Validity, Critical). A `discount_pct` 0–100 check was also added to ETL to match ELT. |
| The ELT reconciliation joined payments to *all* staged orders, so payments for failed orders matched silently (170 vs 628 mismatch rows). | — | `mart_payment_reconciliation` compares payments with passing orders only, the same way `etl_payment_reconciliation` uses the clean fact table. |

Two consistency fixes were made at the same time:

- **ETL incremental upsert:** this step previously collapsed duplicate orders already present in `orders.csv`. It now replaces only base rows whose `order_id` appears in the increment file, matching the cross-load-only MERGE in `stg_orders`.
- **Deterministic deduplication in ELT:** `stg_customers`, `stg_products` and `stg_stores` order ties by `rowid`, so ELT keeps the first source row, the same row as ETL. This matters because the duplicate rows differ: `P0030` and `P0140` each have an `(old)` row with a lower MRP, and the duplicate customers have different phone numbers and areas.

### Files changed

- [include/data_quality.py](../include/data_quality.py): `standardise_values()`, plus the `order_ts` and `discount_pct` checks
- [include/nammamart_etl.py](../include/nammamart_etl.py): standardised transform, duplicate-aware dimension filter, increment upsert
- [include/sql/stg_orders.sql](../include/sql/stg_orders.sql): per-load duplicate count, 120-minute limit, timestamp reason, checks against passing dimensions
- [include/sql/stg_customers.sql](../include/sql/stg_customers.sql), [stg_products.sql](../include/sql/stg_products.sql), [stg_stores.sql](../include/sql/stg_stores.sql): first-row deduplication
- [include/sql/mart_payment_reconciliation.sql](../include/sql/mart_payment_reconciliation.sql): order side restricted to passing orders

### Verified result

Both pipelines were run from the same source files into fresh DuckDB warehouses, and their outputs were compared:

| Metric | Initial load: ETL | Initial load: ELT | With increment: ETL | With increment: ELT |
|---|---:|---:|---:|---:|
| Clean orders | 1,904 | 1,904 | 2,030 | 2,030 |
| Delivered net revenue | 933,871.00 | 933,871.00 | 994,640.85 | 994,640.85 |
| Gross margin | 228,196.78 | 228,196.78 | 243,777.95 | 243,777.95 |
| Payment mismatch rows | 628 | 628 | 754 | 754 |
| Order IDs in only one pipeline | 0 | 0 | 0 | 0 |

### Business rule to confirm

Both pipelines now reject every order linked to a product or store that fails its own checks. One store with zero capacity or a missing zone accounts for about 311 orders on its own. This is the strict, finance-safe interpretation. If Operations decides those orders should load with only the store flagged, the rule must change in `data_quality.py`/`nammamart_etl.py` and in `stg_orders.sql` together, so the two pipelines stay reconciled.

## Raw PII exposure fix

Step 13 lists **raw PII overexposure** as the third risk of the ELT recommendation. A scan of every warehouse table and quarantine file for the exact email and phone values in `customers.csv` found that raw PII reached more places than the raw layer:

| Location | Before | After |
|---|---|---|
| `etl_fact_orders` (finance fact table) | Unmasked `email` and `phone` for every order, because the whole customer row was joined in | Only `email_masked` and `phone_masked`; customer attributes are read from `etl_dim_customer` |
| `include/quarantine/samy/customers_failed_<run_id>.csv` | Unmasked `email` and `phone` | Masked with the same helpers as the dimension (`ga***@example.com`, `91******05`); `customer_id` still traces each row to the source |
| ELT `raw_customers` | In the shared warehouse `warehouse_samy_elt.duckdb`, readable by anyone who can open that file | In a separate file `warehouse_samy_elt_pii.duckdb`, with owner-only permissions (`0600`) and listed in `.gitignore` |

### How the ELT raw layer stays an exact copy

The assignment requires `raw_customers` to be an exact, never-cleaned copy, so its values are not changed. Only its location changes:

1. `load_raw_tables()` writes `raw_customers` to the restricted `_pii` file, next to the warehouse, and drops any copy left in the shared warehouse by earlier runs.
2. `execute_sql_layers()` attaches that file **read-only** as `pii` only while the SQL runs. `stg_customers.sql` reads `pii.raw_customers` and stores masked email and phone.
3. All other layers (`stg_*`, `mart_*`, `pipeline_runs`, `dq_results`) live in the shared warehouse and contain only masked customer fields.

DuckDB has no per-table grants, so file-level permissions are the access control available locally. In production, the same split maps onto a restricted schema or database with role-based grants.

### Files changed

- [include/nammamart_elt.py](../include/nammamart_elt.py): restricted `_pii` warehouse, owner-only permissions, read-only attach during staging
- [include/sql/stg_customers.sql](../include/sql/stg_customers.sql): reads `pii.raw_customers`
- [include/nammamart_etl.py](../include/nammamart_etl.py): raw email and phone excluded from the fact table and masked in quarantine files
- [.gitignore](../.gitignore): `*_pii.duckdb`

### Verified result

Both pipelines were run, initial and incremental, into fresh warehouses, and every output was scanned for the 806 raw email and phone values in `customers.csv`. They were found only in `raw_customers` inside the `_pii` file, which has `0600` permissions. Every other table and quarantine file contained none. ETL and ELT still reconcile: 1,904 / 2,030 clean orders and 628 / 754 reconciliation rows in both pipelines.

### Still to do by hand

Artefacts produced before this fix still contain raw PII. These include the committed `include/warehouse_samy.duckdb` and `assignment/*.duckdb` snapshots (whose `etl_fact_orders` holds unmasked email and phone), plus 17 older `customers_failed_*.csv` quarantine files. Rerun both DAGs and the step notebooks to regenerate them, and delete the old customer quarantine files. The dataset is synthetic (`example.com` emails), but those values remain in Git history.
