# NammaMart ETL vs ELT — Submission Report

**Student:** Gowtham Manivel
**Project:** NammaMart data pipeline
**Report date:** 15 October 2026

> This report uses the run outputs and validation results shared during this conversation. Where an exact business-query result was not provided, it is marked as needing capture rather than fabricated.

## 1. Executive summary

Two Airflow pipelines were implemented over the NammaMart dataset:

- **ETL:** extract, validate and transform, quarantine rejected records, publish curated outputs, and record the run.
- **ELT:** load raw source data into DuckDB, transform through SQL staging tables, quarantine invalid rows, and publish analytical marts.

Both scheduled runs on 15 October 2026 succeeded, and subsequent manual verification runs of both DAGs also succeeded. The new manual runs report **5,372 physical source rows, 4,599 loaded, 747 quarantined, 16 exact duplicate rows, and 10 superseded order versions**. The counters now reconcile source rows explicitly while preserving the older scheduled-run records as historical audit evidence. The latest ELT DQ run contained 23 checks: 19 PASS and 4 Warning-level FAIL. All Critical checks passed.

**Recommendation:** adopt ELT as the primary analytics pattern, with strict Critical-quality gates before curated finance marts are exposed. The 15 October scheduled ELT observation took about 27 minutes and completed after the 09:30 IST target; later manual verification succeeded but was not timed, so the reporting SLA still needs performance work.

## 2. Input data

| File | Rows |
|---|---:|
| `customers.csv` | 405 |
| `products.csv` | 152 |
| `stores.csv` | 15 |
| `orders.csv` | 2,412 |
| `orders_2026-10-01_increment.csv` | 160 |
| `payments.csv` | 2,228 |
| **Total source-file rows** | **5,372** |

The ETL and ELT processing count is a run-level count and should not be confused with the total number of rows across the six files.

## 3. Part 0 — Data issues log

| File / area | Observed issue | Dimension | Severity / handling |
|---|---|---|---|
| Customers | Missing or blank source emails | Completeness | Warning; log in `dq_results` |
| Customers | Populated emails failing format check | Validity | Warning; mask PII in curated outputs |
| Customers | Future signup dates | Timeliness | Critical; quarantine invalid rows |
| Customers | Invalid pincode formats, including `5600XX` | Validity | Warning |
| Products | Invalid product records / pricing fields | Completeness / validity / accuracy | Critical rows quarantined |
| Stores | Missing, invalid, or non-positive `capacity_orders_per_day` | Validity | Critical; quarantine invalid store |
| Orders | Invalid rows and references to customers outside curated customer set | Referential integrity / validity | Critical; quarantine |
| Payments | References to orders outside curated order set | Referential integrity | Critical; quarantine |
| Payments | Successful payment amount differs from order amount | Accuracy / consistency | Critical; quarantine |
| Payments | Invalid amount/mode or future payment timestamp | Validity / timeliness | Critical; quarantine |
| Orders | Latest available order is more than seven days old at run time | Timeliness | Warning; supplied data is historical |

Observed warning check counts: customer email completeness 4/405 failures; email format 8/401; pincode format 3/392; latest-order freshness 1/1. **Part 0 status: partially complete, not fully evidenced.** The source row counts and several discovered issues are documented, but the submitted evidence does not contain the required per-column null counts, duplicate-key counts for every key, numeric/date min/max outputs, all category distinct-value lists, or a complete four-relationship referential-integrity report for all six files. Do not mark the full profiling deliverable complete until those outputs are captured. The duplicate investigation added later in this report covers exact duplicate rows in base orders, increment orders, and payments only.

### Quarantine counts

| Entity | ETL | ELT |
|---|---:|---:|
| Customers | 13 | 13 |
| Products | 9 | 9 |
| Stores | 1 | 1 |
| Orders | 373 | 373 |
| Payments | 351 | 351 |
| **Total** | **747** | **747** |

### ELT payment quarantine reasons

`payments_quarantine` has 351 rows; `payments_key_quarantine` has 0 rows.

| Reason | Rows |
|---|---:|
| `orphan_order_id` | 326 |
| `payment_amount_mismatch` | 10 |
| `orphan_order_id,future_payment_ts` | 7 |
| `orphan_order_id,invalid_amount_paid` | 5 |
| `orphan_order_id,invalid_payment_mode` | 2 |
| `invalid_amount_paid,payment_amount_mismatch` | 1 |
| **Total** | **351** |

Eleven rows have `payment_amount_mismatch` somewhere in the reason string: ten mismatch-only rows and one combined-reason row.

## 4. ETL and ELT implementation results

### ETL curated / quarantine counts

| Entity | Curated | Quarantined |
|---|---:|---:|
| Customers | 392 | 13 |
| Products | 143 | 9 |
| Stores | 14 | 1 |
| Orders | 2,177 | 373 |
| Payments | 1,873 | 351 |

### ELT raw counts

| Table | Rows |
|---|---:|
| `raw_customers` | 405 |
| `raw_products` | 152 |
| `raw_stores` | 15 |
| `raw_orders` | 2,572 |
| `raw_payments` | 2,228 |

The ELT raw layer is intended to retain all six physical source files exactly as ingested, including exact duplicate rows and superseded base versions; deduplication and upsert logic is downstream in staging, not a destructive raw-layer cleanup. The captured raw counts show: `raw_customers` 405, `raw_products` 152, `raw_stores` 15, `raw_orders` 2,572 (2,412 base + 160 increment rows), and `raw_payments` 2,228. The five raw tables represent all six files because the increment file is included in `raw_orders` with source-file metadata. Raw ingestion idempotency was tested and passed. The supporting terminal output previously showed separate raw table counts of `raw_orders` 2,412 and `raw_orders_increment` 160; use the actual current schema to select the matching count query. ELT staging/curated counts align with ETL.

### Mart counts

| Mart | Rows |
|---|---:|
| `mart_daily_store_kpis` | 432 |
| `mart_category_margin` | 7 |
| `mart_delivery_sla` | 14 |
| `mart_payment_reconciliation` | 2,550 |
| `mart_top_customers` | 10 |

## 5. Part C — Scheduling and pipeline audit

Both DAGs use cron `0 9 * * 1-6` and timezone `Asia/Kolkata`, targeting 9:00 AM IST Monday–Saturday. `catchup=False` avoids automatically creating historical runs. Retries and a bounded execution timeout help recover transient failures without running indefinitely.

### Scheduled and latest manual runs — 15 October 2026

| DAG | Run ID | Status | Extracted | Loaded | Quarantined | Start UTC | End UTC |
|---|---|---|---:|---:|---:|---|---|
| `nammamart_elt_pipeline` | `scheduled__2026-10-15T03:30:00+00:00` | SUCCESS | 5,346 | 4,599 | 747 | 03:42:04.257408 | 04:09:17.774766 |
| `nammamart_etl_pipeline` | `scheduled__2026-10-15T03:30:00+00:00` | SUCCESS | 5,346 | 4,599 | 747 | 03:42:04.256375 | 03:42:09.484675 |

The logical scheduled time, 03:30 UTC, equals 09:00 IST. The actual start was later than the scheduled logical time. ELT completed at about 09:39 IST, after the 09:30 leadership stand-up target. Therefore, the successful runs are verified, but the readiness SLA is not yet met. ETL took about five seconds in this observed run; ELT took about 27 minutes. This is a single observation, not a controlled benchmark.

The fresh manual verification runs executed after the audit change both succeeded: ETL run `manual__2026-10-15T08:38:23.580596+00:00` and ELT run `manual__2026-10-15T08:38:19.022844+00:00`. Both audit rows report 5,372 extracted, 4,599 loaded, 747 quarantined, 16 deduplicated, and 10 superseded. Their elapsed times were not captured in the query output and are not claimed here.

The scheduled run records retain their original historical values (`5,346 extracted, 4,599 loaded, 747 quarantined`). After the reconciliation code was updated, both fresh manual runs reported the physical source-row count and explicit adjustment counters.

| DAG | Manual run ID | Status | Extracted | Loaded | Quarantined | Deduplicated | Superseded |
|---|---|---|---:|---:|---:|---:|---:|
| `nammamart_etl_pipeline` | `manual__2026-10-15T08:38:23.580596+00:00` | SUCCESS | 5,372 | 4,599 | 747 | 16 | 10 |
| `nammamart_elt_pipeline` | `manual__2026-10-15T08:38:19.022844+00:00` | SUCCESS | 5,372 | 4,599 | 747 | 16 | 10 |

The verified source-level reconciliation is `5,372 = 4,599 + 747 + 16 + 10`. The older scheduled-run records were not rewritten; they remain useful historical evidence of the metric before this correction.

## Source-row reconciliation: 26-row accounting diagnosis

The six source CSVs contain **5,372 physical rows**. The source-level difference is accounted for by 16 exact duplicate copies and 10 superseded base-order versions. The newly verified manual-run audit records now report `rows_extracted=5,372`, `rows_deduplicated=16`, and `rows_superseded=10` alongside 4,599 loaded and 747 quarantined.

The follow-up source inspection identified the following:

| Finding | Count | Evidence / interpretation |
|---|---:|---|
| Exact duplicate rows in base `orders.csv` | 12 | `DataFrame.duplicated()` identified 12 duplicate copies; the duplicate order IDs were not among the IDs that also appear in the increment file. |
| Order IDs present in both base and increment files | 10 | Each inspected pair has the same order details but a status change from `DELIVERED` in the base file to `RETURNED` in the increment file. These are upserts; the base versions are superseded by the increment status. |
| Exact duplicate rows in `payments.csv` | 4 | `DataFrame.duplicated()` identified four duplicate copies. |
| **Total preprocessing rows to account for** | **26** | **12 exact order duplicates + 10 superseded base order versions + 4 exact payment duplicates.** |

### Reproducible duplicate/upsert evidence

The source diagnostic printed the following exact duplicate order IDs (12 duplicate copies; these IDs do not overlap the increment file):

```text
ORD000539, ORD000783, ORD001110, ORD001239, ORD001326, ORD001434,
ORD001441, ORD001462, ORD001549, ORD001692, ORD001870, ORD002129
```

The increment file has zero exact duplicate rows. Its ten overlapping order IDs are listed in `EVIDENCE.md`; the printed base/increment record pairs have matching order attributes and only the status changes from `DELIVERED` to `RETURNED`. The payment file has four exact duplicate row copies. The diagnostic was performed with pandas `DataFrame.duplicated()` and the set intersection of `order_id` values; the captured command output is preserved in the evidence appendix.

Entity-level arithmetic now has a supported explanation:

| Entity | Physical source rows | Curated | Quarantined | Duplicate/superseded rows | Reconciled |
|---|---:|---:|---:|---:|---:|
| Customers | 405 | 392 | 13 | 0 | 405 |
| Products | 152 | 143 | 9 | 0 | 152 |
| Stores | 15 | 14 | 1 | 0 | 15 |
| Orders, including increment | 2,572 | 2,177 | 373 | 22 (12 duplicate copies + 10 superseded versions) | 2,572 |
| Payments | 2,228 | 1,873 | 351 | 4 duplicate copies | 2,228 |
| **Total** | **5,372** | **4,599** | **747** | **26** | **5,372** |

The resulting source-level accounting is:

```text
5,372 physical source rows
= 4,599 curated/loaded rows
+ 747 quarantined rows
+ 16 exact duplicate copies removed during preprocessing
+ 10 superseded base-order versions replaced by incremental status updates
```

**Audit implementation status:** the schema and both DAGs have now been updated to persist `rows_deduplicated` and `rows_superseded`. Fresh manual-run records verify the new metrics for both ETL and ELT. Historical scheduled records remain unchanged at 5,346 extracted with zeroes in the newly added columns because they were written before the code change; do not interpret those historical values as the physical source-row total. ELT raw tables should continue retaining all 5,372 source rows; deduplication and upsert accounting belongs in staging/audit metrics, not destructive raw-layer deletion.



### Part C — Explicit schedule questions

- **Cron:** `0 9 * * 1-6` means 09:00 on Monday through Saturday in the DAG's `Asia/Kolkata` timezone.
- **UTC shown in Airflow:** 09:00 IST is 03:30 UTC. The 15 October logical run ID confirms `scheduled__2026-10-15T03:30:00+00:00`.
- **If the DAG timezone were UTC:** the same `0 9 * * 1-6` cron would mean 09:00 UTC, which is 14:30 IST, not the required 09:00 IST. A UTC-aware `start_date` can establish UTC as the DAG timetable timezone; use a timezone-aware `Asia/Kolkata` start date to avoid ambiguity.
- **If the laptop or Docker is off:** a local Airflow scheduler cannot execute the DAG while the environment is stopped. With `catchup=False`, it will not create all missed historical runs when restarted; the next eligible scheduled interval can run, and a missed report may require a manual trigger. A production SLA should run on an always-on scheduler, not a laptop.

The audit database is `include/output/nammamart_audit.duckdb`; `pipeline_runs` stores run ID, DAG ID, start/end time, extracted/loaded/quarantined counts, and status.

## 6. Part D — DQ results

Latest ELT run: **23 checks, 19 PASS, 4 FAIL; all four failures are Warning-level. All Critical checks passed.**

| Dimension | Check | Severity | Checked | Failed | Status |
|---|---|---|---:|---:|---|
| Accuracy | `delivery_time_accuracy` | Critical | 2,177 | 0 | PASS |
| Accuracy | `order_amount_formula_accuracy` | Critical | 2,177 | 0 | PASS |
| Accuracy | `product_pricing_accuracy` | Critical | 143 | 0 | PASS |
| Completeness | `curated_business_key_completeness` | Critical | 4,599 | 0 | PASS |
| Completeness | `customer_email_completeness` | Warning | 405 | 4 | FAIL |
| Completeness | `delivered_order_delivery_time_completeness` | Critical | 2,177 | 0 | PASS |
| Consistency | `customer_loyalty_tier_casing` | Warning | 392 | 0 | PASS |
| Consistency | `order_dimension_referential_integrity` | Critical | 2,177 | 0 | PASS |
| Consistency | `payment_order_referential_integrity` | Critical | 1,873 | 0 | PASS |
| Consistency | `successful_payment_amount_reconciliation` | Critical | 1,774 | 0 | PASS |
| Timeliness | `future_customer_signup_date` | Critical | 392 | 0 | PASS |
| Timeliness | `future_order_timestamp` | Critical | 2,177 | 0 | PASS |
| Timeliness | `future_payment_timestamp` | Critical | 1,873 | 0 | PASS |
| Timeliness | `latest_order_freshness_7_days` | Warning | 1 | 1 | FAIL |
| Uniqueness | `curated_business_key_uniqueness` | Critical | 4,599 | 0 | PASS |
| Validity | `customer_email_format` | Warning | 401 | 8 | FAIL |
| Validity | `customer_pincode_format` | Warning | 392 | 3 | FAIL |
| Validity | `no_invalid_rows_in_curated_staging` | Critical | 4,599 | 0 | PASS |
| Validity | `order_status_and_payment_mode_validity` | Critical | 2,177 | 0 | PASS |
| Validity | `order_timestamp_validity` | Critical | 2,177 | 0 | PASS |
| Validity | `payment_status_and_mode_validity` | Critical | 1,873 | 0 | PASS |
| Validity | `payment_timestamp_validity` | Critical | 1,873 | 0 | PASS |
| Validity | `store_capacity_validity` | Critical | 14 | 0 | PASS |

Warning failures do not block the run. The freshness warning is expected with the historical source data: the latest order is dated 1 October and the scheduled run occurred 15 October.

## 7. Part E — Governance

### Ownership and rule approval

| Dataset | Accountable owner | Pipeline/technical steward | Approval/change control |
|---|---|---|---|
| Orders and delivery events | Operations / store operations | Data engineering | Operations + Finance approve revenue/status semantics |
| Payments and gateway status | Finance / payments operations | Data engineering | Finance approves reconciliation and settlement rules |
| Customers | Customer operations / privacy owner | Data engineering | Privacy/legal owner approves access, masking, retention and purpose changes |
| Products and category/cost data | Merchandising / product owner | Data engineering | Merchandising + Finance approve category/cost/margin rules |
| Stores and capacity | Store operations | Data engineering | Operations approves capacity and delivery-SLA thresholds |
| Curated warehouse marts | Business owner of each KPI (Finance for revenue) | Data engineering | KPI owner signs off definition; changes are versioned and tested |

A change such as the 120-minute delivery limit should be proposed by Operations, assessed by Data Engineering, approved by the business owner and Finance when it affects revenue/reporting, recorded with a rule version/effective date, and covered by regression tests. These are recommended ownership assignments; they are not evidence of a formal organisation chart.

### Column-level data dictionary

The following dictionary describes the principal business columns. Exact types should be confirmed against `DESCRIBE` output in the running DuckDB database; types below are intended logical types. Source raw tables preserve CSV values as text plus ingestion metadata.

| Table / column | Logical type | Meaning / source | Allowed values or rule | Personal data? |
|---|---|---|---|---|
| `etl_fact_orders.order_id` / `orders_curated.order_id` | VARCHAR | Order identifier from `orders.csv` or increment | Non-empty, unique after upsert | No, pseudonymous transaction key |
| `order_ts` | TIMESTAMP | Order event time from order source | Parseable; not future-dated | No by itself |
| `customer_id` | VARCHAR | Customer foreign key | Must exist in curated customer dimension | Indirect identifier |
| `product_id` | VARCHAR | Product foreign key | Must exist in product dimension | No |
| `store_id` | VARCHAR | Store foreign key | Must exist in store dimension | No |
| `quantity` | INTEGER | Units ordered | Greater than zero | No |
| `unit_price`, `order_amount`, `net_revenue` | DECIMAL | Price, source order amount, revenue after discount | Positive/non-negative per rule; formula consistency; only qualifying delivered orders count as revenue | No |
| `discount_pct` | DECIMAL | Order discount percent | 0–100 | No |
| `order_status` | VARCHAR | Current status from order/increment source | `DELIVERED`, `CANCELLED`, `RETURNED` | No |
| `delivery_minutes` | INTEGER | Delivery duration in minutes | Non-negative; under 120 per current rule; delivered orders require a value | No |
| `etl_dim_customer.customer_id` | VARCHAR | Customer key | Unique in dimension | Indirect identifier |
| `full_name` | VARCHAR | Customer name from `customers.csv` | Non-empty where required | **Yes** |
| `email` | VARCHAR | Customer email, masked in curated output | Valid format warning; masked display | **Yes** |
| `phone` | VARCHAR | Customer phone, masked in curated output | 10-digit source format check; masked display | **Yes** |
| `city`, `area`, `pincode` | VARCHAR | Customer address/location attributes | Pincode format rule; values restricted by access policy | **Potentially identifying** |
| `loyalty_tier` | VARCHAR | Customer tier | Standardized tier values (for example Silver/Gold/Platinum; verify distinct source list) | Customer-linked attribute |
| `etl_dim_product.product_id` | VARCHAR | Product key from `products.csv` | Unique | No |
| `product_name`, `brand`, `category`, `sub_category` | VARCHAR | Product descriptive attributes | Category must be populated/standardized | No |
| `mrp`, `cost_price` | DECIMAL | Retail price and cost from product source | `mrp > 0`; cost must not exceed MRP under current rule | No |
| `etl_dim_store.store_id` | VARCHAR | Store key from `stores.csv` | Unique | No |
| `store_name`, `zone`, `area`, `pincode` | VARCHAR | Store/location attributes | Zone in North/South/East/West; valid pincode | No |
| `capacity_orders_per_day` | INTEGER | Store daily capacity | Positive integer | No |
| `mart_daily_store_kpis.order_date` | DATE | Calendar date derived from order timestamp | September filter for September report | No |
| `mart_daily_store_kpis.net_revenue` | DECIMAL | Sum of qualifying delivered order net revenue | Derived from order amount/discount; definition must remain versioned | No |
| `mart_category_margin.revenue`, `cogs`, `gross_margin` | DECIMAL | Category revenue, quantity × cost, revenue minus COGS | `gross_margin = revenue - cogs` | No |
| `mart_delivery_sla.average_delivery_minutes` | DECIMAL | Mean delivered-order duration by store | Valid delivery rows only | No |
| `mart_delivery_sla.delivered_within_15_min_pct` | DECIMAL | Percentage of delivered orders at or below 15 minutes | 0–100% | No |
| `mart_payment_reconciliation.reconciliation_status` | VARCHAR | Order/payment comparison status | `RECONCILED`, `NO_SUCCESSFUL_PAYMENT`, `ORDER_QUARANTINED` in the captured mart | No |
| `mart_top_customers.total_spend`, `loyalty_tier` | DECIMAL / VARCHAR | Top ten spenders and tier | Raw email/phone must not be exposed | Customer-linked, potentially personal |
| `dq_results.run_id`, `dag_id`, `check_name`, `dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, `checked_at` | VARCHAR / BIGINT / TIMESTAMP | Data-quality result per run | Controlled enumerations for dimension/severity/status | No |
| `pipeline_runs.run_id`, `dag_id`, `start_time`, `end_time`, `rows_extracted`, `rows_loaded`, `rows_quarantined`, `status` | VARCHAR / TIMESTAMP / BIGINT | Pipeline execution audit | One row per DAG run | No |

This is a useful logical dictionary, but final submission should attach actual `DESCRIBE` output and add any implementation-specific columns omitted here. Do not claim it is a database-generated schema dump.

### Lineage: what can and cannot be proven from current evidence

**ETL lineage:** source CSV → run-scoped extraction → Python validation/curation → `etl_fact_orders` → `etl_daily_zone_revenue`; rejected rows go to quarantine with a reason. An accepted-order fact-to-aggregate trace is now verified for `ORD002401` (2026-10-01, store S14, NammaMart Vijayanagar, West): fact net revenue 475.95; matching daily/store aggregate has 12 delivered orders and net revenue 5,453.90. The exact original CSV row/line and a separate ELT raw-to-mart contribution trace are still not evidenced.

**ELT lineage:** source CSV → `raw_*` (with `_source_file`, `_run_id`, `_loaded_at`) → SQL staging/upsert → mart. Raw/source metadata supports auditability. The accepted-order trace above verifies the ETL fact-to-aggregate link only; a separate ELT raw-to-mart trace with contributing order IDs and source-file lineage remains open.

### Personal data, masking and Indian law

Direct personal-data columns in the provided dataset include `customers.full_name`, `customers.email`, and `customers.phone`. Customer-linked location fields (`city`, `area`, `pincode`) and `customer_id` can also identify or single out a person when combined with other records, so they should be treated as personal or linkable data in this pipeline. The top-customer mart must not expose raw email or phone. Masking is preferable to deleting all contact attributes when a limited business use (for example, support lookup or deduplication) remains legitimate; raw access should be limited to authorised roles, while most reporting users need only aggregated or masked values.

The applicable central law is India's **Digital Personal Data Protection Act, 2023**, with the **Digital Personal Data Protection Rules, 2025** notified by MeitY on 14 November 2025. The Act/Rules framework is being brought into force on a phased timeline, so legal/compliance should confirm which provisions are in force for the relevant processing date. The framework centres on lawful purpose and notice, data minimisation, accuracy, storage limitation, reasonable security safeguards, accountability, and rights of individuals. This is an engineering summary, not legal advice. Official sources: [MeitY — DPDP Rules 2025](https://www.meity.gov.in/documents/act-and-policies/digital-personal-data-protection-rules-2025-gDOxUjMtQWa?pageTitle=Digital-Personal-Data-Protection-Rules-2025) and [PIB — notification overview](https://www.pib.gov.in/PressReleasePage.aspx?PRID=2190014&lang=2&reg=3).

### Access control and retention proposal

| Data zone | Who should have access | Proposed retention / disposal |
|---|---|---|
| Raw tables/source CSVs | Data engineering/platform admins; privacy-authorised personnel only when needed | Retain for a documented, approved replay/audit period (proposal: 90 days in this assignment); then delete or archive under approved policy |
| Staging | Data engineering and quality owners; restricted access to raw-derived PII | Retain for debugging/replay only (proposal: 30 days), then rebuild/drop according to policy |
| Quarantine | Data-quality owners and source-domain owner; Finance only for relevant case review; raw PII hidden/masked in routine views | Retain until issue resolution plus an approved audit window (proposal: 90 days); purge after policy/legal hold checks |
| Masked marts | Finance/Operations/Analytics by role and least privilege | Retain per business reporting schedule; aggregate historical data where possible |
| Run/DQ audit | Data engineering, auditors, authorised business owners | Keep non-PII operational metadata per audit policy (proposal: 1 year) |

These durations are **proposed assignment defaults, not legal mandates**. NammaMart must approve a retention schedule based on business need, tax/financial obligations, customer disputes, legal holds and applicable law before production use. Enforce role-based access, least privilege, encryption in transit/at rest where supported, access logging, and tested deletion.

### Secrets management

No credential should be hard-coded in a DAG, committed to GitHub, printed in task logs, or embedded in a screenshot. In a local Astro setup, use Airflow Connections/Variables for non-secret configuration and a secrets backend or environment-injected secret for credentials such as `SLACK_WEBHOOK_URL`; in production, prefer a managed secrets manager with rotation and least-privilege access. The current evidence pack does not prove that a Slack alert or external secret backend has been configured, so describe this as the required design unless you capture that configuration.

### Rejected-order audit example

`ORD000500` is the rejected-order example. The ETL query returned `finance_fact_rows = 0`, proving that it is absent from `etl_fact_orders` in the queried database state. The ELT quarantine query returned `ORD000500 | invalid_order_status`. The accepted-order ETL fact-to-aggregate trace for `ORD002401` is also now captured. The remaining lineage gap is the original source-row/line and ELT raw-to-mart trace, not the ETL fact-to-aggregate join. The following are historical capture commands; their outputs are already recorded in the later evidence addenda where available:

```sql
-- ELT: exact raw source version(s); avoid exporting PII if customer data is joined.
SELECT *
FROM raw_orders
WHERE order_id = 'ORD000500';

-- ELT: rejection reason.
SELECT order_id, quarantine_reason, source_file
FROM orders_quarantine
WHERE order_id = 'ORD000500';

-- ETL: prove the rejected order is absent from finance fact.
SELECT COUNT(*) AS finance_fact_rows
FROM etl_fact_orders
WHERE order_id = 'ORD000500';
```

Observed ETL result: `finance_fact_rows = 0`. The ELT quarantine query is now captured: `ORD000500` has reason `invalid_order_status` in `orders_quarantine`. If the current ELT table names differ, use `SHOW TABLES` / `DESCRIBE` and substitute the actual table name. Save both outputs in the evidence report. The accepted-order ETL fact-to-aggregate trace is now captured; exact source CSV row/line and ELT raw-to-mart trace remain open.

### Remaining `ORD000500` and lineage capture commands

Run inside the Astro container. First confirm the actual table names; use `DESCRIBE` if a query fails.

```bash
duckdb include/output/nammamart_elt/nammamart_elt.duckdb
```

```sql
SHOW TABLES;
SELECT order_id, quarantine_reason, source_file
FROM orders_quarantine
WHERE order_id = 'ORD000500';

DESCRIBE mart_daily_store_kpis;
DESCRIBE mart_category_margin;
DESCRIBE mart_delivery_sla;
DESCRIBE mart_payment_reconciliation;
DESCRIBE mart_top_customers;
```

Then query the ETL database and retain the already verified exclusion result:

```bash
duckdb include/warehouse_gowtham.duckdb
```

```sql
SELECT COUNT(*) AS finance_fact_rows
FROM etl_fact_orders
WHERE order_id = 'ORD000500';

DESCRIBE etl_fact_orders;
DESCRIBE etl_daily_zone_revenue;
```

A complete lineage trace needs the actual `order_id`/business key, the relevant fact row (if accepted), and the aggregate row it contributes to. For a quarantined order such as `ORD000500`, the expected ETL fact count is zero, so do not invent a contribution to a mart. Capture a separate example of a curated order to trace a real source amount through the fact and its reporting aggregate.

### Verified ETL warehouse schemas — 15 October 2026

The user-provided DuckDB `DESCRIBE` output confirms these columns.

`etl_fact_orders`:
- Identifiers/time: `order_id`, `order_ts`, `customer_id`, `store_id`, `product_id`, `order_date`, `run_id`
- Order values: `quantity`, `unit_price`, `discount_pct`, `order_amount`, `payment_mode`, `order_status`, `channel`, `delivery_minutes`
- Source audit: `source_file`, `source_order_status`, `source_payment_mode`
- Enrichment/metrics: `category`, `cost_price`, `store_name`, `zone`, `net_revenue`, `gross_margin`, `within_15_min`

`etl_daily_zone_revenue`:
- Grouping keys: `order_date`, `zone`, `store_id`, `store_name`
- Measures: `delivered_orders`, `net_revenue`

The rejected-order query returned `finance_fact_rows = 0` for `ORD000500`. The accepted-order trace now establishes that `ORD002401` contributes fact net revenue 475.95 to the 1 October 2026 aggregate for store S14; that aggregate contains 12 delivered orders and total net revenue 5,453.90. The original CSV row/line and ELT raw-to-mart trace remain unverified.

### Change history: DELIVERED on 3 September, RETURNED on 1 October

The increment file has 10 overlapping order IDs; the source inspection confirms each inspected base/increment pair has the same order details but `DELIVERED` in the base file and `RETURNED` in the increment file. For example, `ORD000017` changes from `DELIVERED` to `RETURNED`.

The current upserted view keeps the latest status, so a current-state September revenue mart should **exclude** a returned order from delivered-only revenue. Finance may also need “as originally reported” September revenue, which would still include the order if it was delivered at the September reporting cutoff. To preserve both views, store status history as events or an SCD Type 2/order-status history with `effective_from`, `effective_to`, and `is_current`; calculate “as-of September 30” from the historical version and “latest corrected” from the current version. The current implementation demonstrates current-state upsert, not a full historical snapshot. A specific September revenue delta for all 10 returned orders has not been computed in the supplied evidence and must not be invented.

## 8. Part F — Observability experiments and five pillars

The assignment requires the exact ten prescribed experiments to be run on **both DAGs**, restoring the original source file after each test. The table below is an operational-observability summary, not proof that the required experiment suite is complete. The supplied evidence does not include controlled runs for missing-file failure, injected five-row bad data, schema rename, 50-row volume anomaly, ×100 unit-price drift, 30-day stale data, or Clear-based recovery. These experiments remain **incomplete until run IDs, task states/logs, `dq_results` and before/after table outputs are attached**.

| # | Experiment | Expected | Observed | Evidence / action |
|---:|---|---|---|---|
| 1 | Scheduled run | Both DAGs succeed | Both latest scheduled runs succeeded; ELT missed 09:30 target | `pipeline_runs`; profile ELT tasks and schedule with lead time |
| 2 | Critical DQ gates | Invalid critical rows do not enter curated outputs | Critical checks passed; invalid rows quarantined | `dq_results` and quarantine; retain blocking gates |
| 3 | Warning DQ | Warnings are logged without failing the run | Four Warning checks failed, run succeeded | `dq_results`; assign warning owners |
| 4 | Raw retention | Keep source rows for audit | Raw counts: 405, 152, 15, 2,572, 2,228 | `raw_*`; maintain access controls |
| 5 | Idempotency | Rerun does not double-count | Raw idempotency test passed | Test output; retain deterministic keys |
| 6 | Incremental feed | Add new orders and apply corrections without duplicate order keys | Raw has 2,412 base + 160 increment rows; staging orders 2,177 after filtering | Raw/merged/staging tables; verify corrected statuses with dedicated query |
| 7 | Reconciliation | Successful payments match orders; rejected rows remain visible | 1,774 reconciled; 403 no successful payment; 373 order-quarantined; mismatch reasons in payment quarantine | Mart + `payments_quarantine`; preserve distinct statuses |
| 8 | PII | Reporting does not expose raw contact details | Masking implemented; top-customer mart has 10 rows | Inspect mart schema/output; capture masked result only |
| 9 | Run audit | Every run exposes status and counts | Both latest runs SUCCESS with matching counts | `pipeline_runs`; keep final audit task `all_done` |
| 10 | Recovery | Clear/retry reruns failed and downstream tasks only | Historical ETL failure exists before audit fix; later run succeeded; task-level recovery not verified by shared output | Capture task history/logs before claiming recovery test complete |

### Required experiment status (do not count as complete without evidence)

| Prescribed experiment | Status from evidence currently available | Evidence still needed for ETL and ELT |
|---|---|---|
| 1. Happy path | Partially evidenced | Per-file input/passed/quarantined counts, task durations and parallel-task evidence |
| 2. Re-run / idempotency | Partially evidenced; raw idempotency test passed | Before/after fact and raw counts for a second run of each DAG |
| 3. Incremental load | Partially evidenced; 10 overlapping status updates identified | Inserted vs updated counts and final status query for all 10 IDs |
| 4. Missing `products.csv` | Not evidenced | Controlled missing-file run, retry timeline, failed/upstream-failed states and alert/log |
| 5. Five injected bad order rows | Not evidenced | Test copy, five reason rows in quarantine, corresponding `dq_results` evidence |
| 6. Rename `order_amount` to `total` | Not evidenced | Controlled schema-change run, exact failing task/error, restore source copy |
| 7. Only 50 orders | Not evidenced | Controlled volume run and alert/check result; if absent, implement volume threshold |
| 8. Multiply one store's `unit_price` by 100 | Not evidenced | Before/after category/store mart metric and drift-check result |
| 9. Latest order 30 days old | Not evidenced | Controlled stale-data run and freshness DQ result |
| 10. Clear after failure | Not evidenced | Airflow task-state history showing exactly which failed/downstream tasks reran |

### Five pillars

| Pillar | Coverage | Assessment |
|---|---|---|
| Freshness | `latest_order_freshness_7_days` | Implemented; historical input and ELT duration reveal SLA risk |
| Volume | Run-level extracted/loaded/quarantined counts | Strongest coverage |
| Schema | SQL casts and validity checks | Partial; explicit schema-drift detection not evidenced |
| Distribution | Email/pincode/category/status/mode checks | Partial; trend baselines not evidenced |
| Lineage | Raw → staging → marts, run IDs and quarantine reasons | Good table-level lineage; column-level lineage not evidenced |

**Strongest pillar:** volume. **Weakest pillar:** distribution, because historical trend/drift monitoring is not evidenced.

## 9. Six business questions — SQL and results

> Verify column names with `DESCRIBE <mart_name>` before executing. Results not included in the shared outputs are marked for capture; do not claim them as verified until executed.

### Q1. Daily net revenue by zone and store, delivered only, after discount

**Order-grain validation query (shows the delivered-only rule explicitly):**

```sql
SELECT CAST(o.order_ts AS DATE) AS order_date,
       s.zone, o.store_id, s.store_name,
       ROUND(SUM(o.order_amount), 2) AS net_revenue
FROM orders_curated o
JOIN stores_curated s USING (store_id)
WHERE UPPER(TRIM(o.order_status)) = 'DELIVERED'
  AND CAST(o.order_ts AS DATE) >= DATE '2026-09-01'
  AND CAST(o.order_ts AS DATE) <  DATE '2026-10-01'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 4;
```

This query uses `order_amount`, which the source defines as quantity × unit price after discount. Confirm that definition against the assignment's source rules and `DESCRIBE orders_curated`.

**Final mart query (the mart's upstream SQL must already filter to delivered orders):**

```sql
SELECT order_date, zone, store_id, store_name,
       ROUND(SUM(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
WHERE order_date >= DATE '2026-09-01'
  AND order_date <  DATE '2026-10-01'
GROUP BY order_date, zone, store_id, store_name
ORDER BY order_date, zone, store_name;
```

The order-grain query visibly applies both required filters (`order_status = 'DELIVERED'` and September 2026). Confirm that the mart's upstream SQL uses the same delivered-only predicate; the aggregated mart itself has no row-level status to filter after aggregation. The query explicitly filters September 2026. **Verified comparison:** 415 September date/store keys; zero ETL-vs-ELT differences. The full 415-row result output is not present in this evidence pack and remains an attachment/deliverable gap.

### Q2. Revenue and gross margin by category

```sql
SELECT category, ROUND(revenue, 2) AS revenue,
       ROUND(cogs, 2) AS cost_of_goods,
       ROUND(gross_margin, 2) AS gross_margin
FROM mart_category_margin
ORDER BY category;
```

| Category | Revenue | COGS | Gross margin |
|---|---:|---:|---:|
| Dairy & Bakery | 152,206.90 | 111,763.19 | 40,443.71 |
| Fruits & Vegetables | 79,443.20 | 60,692.28 | 18,750.92 |
| Household | 171,667.25 | 128,238.79 | 43,428.46 |
| Personal Care | 187,583.45 | 145,837.50 | 41,745.95 |
| Snacks & Beverages | 154,777.80 | 118,877.80 | 35,900.00 |
| Staples | 230,402.90 | 171,740.05 | 58,662.85 |
| UNKNOWN | 39,180.65 | 34,705.10 | 4,475.55 |

All seven categories matched between ETL and ELT.

### Q3. Average delivery time and within-15-minute percentage per store

```sql
SELECT store_id, store_name,
       average_delivery_minutes, delivered_within_15_min_pct
FROM mart_delivery_sla
ORDER BY store_name;
```

**Verified comparison:** all 14 store-level SLA results matched. The full 14-row result output is not present in this evidence pack; capture it before treating the output attachment as complete.

### Q4. Revenue by payment mode and gateway reconciliation

Verified payment-mode comparison:

| Mode | Orders | Revenue |
|---|---:|---:|
| Card | 362 | 204,933.10 |
| Cash on Delivery | 250 | 139,236.70 |
| UPI | 1,027 | 557,137.65 |
| Wallet | 201 | 113,954.70 |

Reconciliation summary:

| Status | Orders | Sum of absolute stored `amount_difference` |
|---|---:|---:|
| `RECONCILED` | 1,774 | 0.00 |
| `NO_SUCCESSFUL_PAYMENT` | 403 | 229,599.95 |
| `ORDER_QUARANTINED` | 373 | 226,150.08 |

**Caution:** the last two totals are not confirmed gateway losses. The statuses mean there is no successful payment or the order was quarantined. The mart has no `AMOUNT_MISMATCH` rows because rejected payments are excluded from curated successful-payment totals. Separately, payment quarantine has 10 mismatch-only rows and one combined-reason row.

The four-mode table and the `RECONCILED`-only query answer different questions: the mode table is order-side revenue by order payment mode (including orders whose payment is not successfully reconciled), while the query below sums successful amounts only for reconciled orders. These values are not expected to match. Do not label the order-side revenue table as “gateway-collected revenue”.

```sql
SELECT payment_mode, COUNT(*) AS order_count,
       ROUND(SUM(successful_amount_paid), 2) AS successful_amount_paid
FROM mart_payment_reconciliation
WHERE reconciliation_status = 'RECONCILED'
GROUP BY payment_mode
ORDER BY payment_mode;
```

### Q5. Top ten customers by spend and loyalty tier, without raw contact data

```sql
SELECT *
FROM mart_top_customers
ORDER BY total_spend DESC
LIMIT 10;
```

The mart has 10 rows, but the actual customer IDs/names, spend amounts, and loyalty tiers were not included in the shared outputs. This remains an incomplete business-question deliverable. Execute the query and capture the ten spend values; use a submission-safe view with customer ID or masked name only, and verify that raw email/phone are absent.

### Q6. Rejected rows by file and reason

Aggregate counts verified: customers 13, products 9, stores 1, orders 373, payments 351; total 747. Payment reasons are listed in Part 0.

```sql
SELECT quarantine_reason, COUNT(*) AS rejected_rows
FROM payments_quarantine
GROUP BY quarantine_reason
ORDER BY rejected_rows DESC;
```

For the exact “today” answer, filter all entity quarantine tables/artifacts by the current run ID or run timestamp and group by source file/entity and reason. The currently supplied counts are per latest snapshot/entity, not verified current-run counts by every reason. A full Q6 result across all five entities remains missing. Do not label aggregate counts as “today” without that filter.

Example pattern (adjust table/column names after `SHOW TABLES` and `DESCRIBE`; the exact query has not yet been run):

```sql
SELECT 'customers.csv' AS source_file, quarantine_reason, COUNT(*) AS rejected_rows
FROM customers_quarantine
GROUP BY quarantine_reason
UNION ALL
SELECT 'products.csv', quarantine_reason, COUNT(*)
FROM products_quarantine
GROUP BY quarantine_reason
UNION ALL
SELECT 'stores.csv', quarantine_reason, COUNT(*)
FROM stores_quarantine
GROUP BY quarantine_reason
UNION ALL
SELECT source_file, quarantine_reason, COUNT(*)
FROM orders_quarantine
GROUP BY source_file, quarantine_reason
UNION ALL
SELECT source_file, quarantine_reason, COUNT(*)
FROM payments_quarantine
GROUP BY source_file, quarantine_reason
ORDER BY source_file, rejected_rows DESC;
```

## 10. Part G — Recommendation to Sameer, CTO

**To:** Sameer, CTO, NammaMart
**Subject:** Recommendation for the NammaMart reporting pipeline

I recommend **ELT as the primary analytics platform, with strict Critical-quality gates before curated finance marts are published**. ELT retains raw inputs for replay and audit while moving transformations into SQL, making rules easier to inspect and evolve. The scheduled ETL and ELT runs succeeded; after the reconciliation change, fresh manual runs of both pipelines also succeeded with 5,372 extracted, 4,599 loaded, 747 quarantined, 16 deduplicated, and 10 superseded. All Critical ELT DQ checks passed; four Warning checks were logged.

ETL remains useful where fast pre-warehouse validation is important. The 15 October scheduled observation was approximately 5.23 seconds for ETL and 27 minutes 13.52 seconds for ELT; ELT completed after the 09:30 leadership stand-up target. Later manual verification runs succeeded, but their elapsed times were not captured, so they cannot be used to claim a performance improvement. This is a single scheduled observation, not a controlled benchmark. Before production adoption, profile task-level timings, optimize SQL and incremental processing, and schedule with enough lead time.

Finance users must never consume raw or quarantined data. Reconciliation should distinguish successful matching payments, no successful payment, quarantined orders, and amount mismatches; missing payments should not be presented as proven gateway losses. At 1,000× growth, benchmark throughput, concurrency, and recovery, and consider a managed warehouse only if measured requirements justify the added cost.

**Top three risks**
1. **SLA risk:** ELT missed the observed 09:30 target. Profile tasks, optimize incremental processing, and schedule with sufficient lead time.
2. **Reconciliation ambiguity:** missing successful payments and quarantined orders can be confused with gateway losses. Keep status definitions explicit and monitor mismatch/quarantine trends.
3. **Scale and governance:** local DuckDB artifacts and raw PII may not meet future durability, concurrency, and access-control needs. Restrict raw access, mask reporting outputs, back up artifacts, and benchmark before scaling.

## 11. Tests and parity checks

- Python test suite: **32 passed** (`pytest -q -p no:cacheprovider`, latest reported run) (`pytest -q -p no:cacheprovider`).
- Latest scheduled ETL run: SUCCESS.
- Latest scheduled ELT run: SUCCESS.
- Latest manual ETL and ELT runs both recorded 5,372 extracted, 4,599 loaded, 747 quarantined, 16 deduplicated, and 10 superseded; both statuses were SUCCESS.
- Curated and quarantine counts align by entity.
- September daily store revenue: 415 keys, zero differences.
- Category margin: all seven categories match.
- Delivery SLA: all 14 store-level results match.
- Payment-mode revenue totals match.
- September reconciliation comparison: 2,047 orders, zero differing statuses.
- Raw ingestion idempotency test passed.
- A historical ETL run failed before an audit fix; a later scheduled run succeeded. Do not claim zero failures across all history.

## 12. Remaining evidence and deliverable gaps

The following items are **not complete based on the evidence currently available**. They should remain visible as open items rather than be represented as completed:

1. **Part 0:** full per-column profiling for all six files (null counts, duplicate keys, numeric/date min/max, distinct category values), and complete referential-integrity/business-rule check outputs.
2. **Part E:** ETL exclusion of `ORD000500` (`COUNT(*) = 0`), ELT quarantine reason (`invalid_order_status`), actual ETL table schemas, and accepted-order ETL fact-to-aggregate trace for `ORD002401` are verified. Still open: original source CSV row/line provenance and a separate ELT raw-to-mart trace.
3. **Part F:** function-level missing-file and schema-mismatch tests, the isolated 50-order case, and the 100× price experiment are now documented. Still open: stale-data test; Clear/recovery; alert delivery; and running the corresponding controlled tests against the ELT DAG with task states, logs, DQ output, and before/after counts. Do not describe the entire experiment matrix as complete.
4. **Business questions:** Q1 full 415-row output, Q3 full 14-store output, Q5 actual top-ten spend values, and Q6 current-run counts by every entity/file and reason.
5. **Change history:** exact September revenue impact for the ten returned orders, ideally comparing “as originally reported” with “latest corrected”; only the status updates are currently evidenced.
6. **Audit history distinction:** fresh manual runs verify explicit `rows_deduplicated` and `rows_superseded` counters. Historical scheduled records still show the earlier 5,346 metric and should remain unchanged; use fresh-run records for the corrected reconciliation.

## 13. Screenshot and final submission checklist

- [ ] ETL Graph view: retain screenshot of successful run with all tasks green.
- [ ] ELT Graph view: retain screenshot of successful run with all tasks green.
- [ ] Schedule evidence: `0 9 * * 1-6`, timezone `Asia/Kolkata`.
- [ ] Aggregate quarantine summary without customer-level PII.
- [ ] Capture `dq_results` output for the latest ELT run and label it with its run ID; the older supplied ELT snapshot had 23 checks, while the latest ETL DQ output had 30 checks.
- [ ] `pipeline_runs` output for both successful scheduled runs.
- [ ] `mart_payment_reconciliation` summary.
- [ ] `payments_quarantine` reason summary.
- [x] ELT `orders_quarantine` query result for `ORD000500` (`invalid_order_status`).
- [x] Accepted-order ETL fact-to-aggregate trace (`ORD002401`; fact revenue 475.95; aggregate 12 orders / 5,453.90).
- [ ] Original source CSV row/line provenance and separate ELT raw-to-mart trace.
- [ ] Q1 full result output (415 date/store keys).
- [ ] Q2 category margin output (7 rows).
- [ ] Q3 delivery SLA output (14 stores).
- [ ] Q4 payment-mode and reconciliation output.
- [ ] Q5 top-ten output with no raw email/phone.
- [ ] Q6 current-run rejected rows by file and reason.
- [ ] Recheck for secrets, webhook URLs, and unmasked customer data.
- [ ] Confirm DAG parse status and rerun tests before submission.

### Mart schema checks

```sql
DESCRIBE mart_daily_store_kpis;
DESCRIBE mart_category_margin;
DESCRIBE mart_delivery_sla;
DESCRIBE mart_payment_reconciliation;
DESCRIBE mart_top_customers;
```

**Repository workflow:** keep changes local until the assignment is complete and the final evidence/PII review has passed.


## 14. Reconciliation implementation verification (15 October 2026)

- Added `rows_deduplicated` and `rows_superseded` to `pipeline_runs`, including an `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migration for an existing audit database.
- Updated the reconciliation helper to count physical rows across all six source CSVs and identify exact duplicate copies and superseded order versions.
- Updated both ETL and ELT DAGs to pass these counters to `record_pipeline_run`.
- Validation: Python compilation succeeded and `pytest -q -p no:cacheprovider` returned **30 passed**.
- Fresh manual ETL run `manual__2026-10-15T08:38:23.580596+00:00`: SUCCESS; 5,372 extracted, 4,599 loaded, 747 quarantined, 16 deduplicated, 10 superseded.
- Fresh manual ELT run `manual__2026-10-15T08:38:19.022844+00:00`: SUCCESS with the same counts.
- The earlier scheduled-run audit rows are historical and remain unchanged at 5,346 extracted with zeroes in the newly added counters.
- This historical entry records the state at that time. The later controlled-test sections and the updated Part 12 checklist below record additional evidence collected on 15 October 2026; remaining gaps are still explicitly listed.

---

**Date:** 15 October 2026
**Purpose:** Consolidated assignment submission, including the original implementation/run history and the latest controlled-test evidence. Historical run records remain distinct; outstanding requirements are explicitly identified.

## 15. Controlled observability tests — 15 October 2026

### 1.1 Missing source file
A direct isolated invocation of the existing `_extract_group()` function used the deliberately nonexistent filename `missing_file_test_only.csv`.

- **Expected:** `FileNotFoundError`
- **Observed:** `FileNotFoundError: Required source file is missing: /usr/local/airflow/include/data/nammamart/missing_file_test_only.csv`
- **Result:** PASS
- **Scope:** Function-level test only. It did not trigger an Airflow task, retry, or alert.
- **Safety:** Original source files and warehouse were not modified.

### 1.2 Schema mismatch
The actual `validate_extractions` Python callable was invoked with a temporary run directory containing copies of the six source files. The `email` column was removed from the temporary `customers.csv`.

- **Expected:** Reject the altered schema before transformation.
- **Observed:** `Schema mismatch in customers.csv; missing=['email'], unexpected=[]`
- **Result:** PASS
- **Safety:** Original source files and warehouse were not modified.

### 1.3 Isolated 50-order transformation and validation
A temporary fixture selected 50 distinct base order IDs, retained the reference datasets, and used an empty temporary incremental order file. The existing `transform_orders()` and `validate_orders()` functions were called directly. No ETL output writer, warehouse loader, or Airflow DAG run was invoked.

| Stage | Rows |
|---|---:|
| Selected distinct source order IDs | 50 |
| Rejected by `transform_orders()` | 1 |
| Transformed orders passed to validation | 49 |
| Curated orders | 40 |
| Rejected during validation | 9 |
| **Total accounted for** | **50** |

The initial rejection was `ORD001812`, with payment mode `Cashh`, reason `invalid_payment_mode`. The nine validation-quarantined rows had ten reason occurrences because one row had both `orphan_customer_id` and `orphan_store_id`. Initial and validation quarantine IDs did not overlap. The total reconciles: 40 + 9 + 1 = 50.

- **Result:** PASS for isolated transformation/validation and count reconciliation.
- **Safety:** Original CSVs and warehouse were not modified.
- **Limitation:** This does not prove full Airflow DAG recovery, retry behaviour, or alert delivery.

### 1.4 100× unit-price anomaly experiment
An in-memory copy of a valid order was used; no source file or warehouse table was changed. The selected order was `ORD001913`, with original `unit_price=222.00` and `order_amount=421.80`.

| Experiment | Mutation | Observed result |
|---|---|---|
| A | Multiply `unit_price` by 100, leave `order_amount` unchanged | Critical Accuracy issue: `Order amount does not match the required formula` |
| B | Multiply `unit_price` by 100 and recompute `order_amount` consistently | No issue detected |

**Interpretation:** The current `order_accuracy_issues()` rule checks consistency between quantity, unit price, discount, and order amount. It detects the inconsistent amount in Experiment A, but does not independently identify an unusually high unit price if the amount is recalculated consistently (Experiment B). Therefore, an independent price-outlier rule is a documented gap; do not claim that current checks catch every 100× price anomaly.

## 16. ETL warehouse schema and integrity verification

The ETL warehouse inspection returned these reporting table counts:

| Table | Rows |
|---|---:|
| `etl_daily_zone_revenue` | 429 |
| `etl_category_margin` | 7 |
| `etl_payment_reconciliation` | 604 |

The `DESCRIBE etl_fact_orders` output showed the fact table includes the order identifiers, source status/payment mode, derived date/category/cost/store/zone fields, net revenue, gross margin, delivery KPI, and `run_id`.

A joined integrity query returned:

| Check | Count |
|---|---:|
| Fact rows | 2,177 |
| Distinct order IDs | 2,177 |
| Missing customer dimension matches | 0 |
| Missing product dimension matches | 0 |
| Missing store dimension matches | 0 |

The query `SELECT COUNT(*) FROM etl_fact_orders WHERE order_id = 'ORD000500'` returned `0`, proving absence from the ETL finance fact in the queried database state. The actual ELT quarantine query output is still required before claiming the full cross-pipeline audit path.

## 17. Part F verification status

| Experiment/control | Status |
|---|---|
| Missing source file detection | Verified at function level |
| Schema mismatch rejection | Verified by actual validator callable |
| Controlled 50-order validation | Verified; counts reconcile |
| 100× price anomaly | Partially detected; independent outlier detection gap identified |
| Full Airflow task failure/retry/recovery | Not yet verified |
| Alert delivery for injected failures | Not yet verified |
| Stale 30-day data experiment | Not yet verified |
| Clear/recovery experiment for both DAGs | Not yet verified |
| Full controlled experiment matrix for both ETL and ELT | Not complete |

## 18. Final evidence limitations

Use the terminal outputs as evidence for the specific tests above. Do not state that all Part F observability tests are complete. Keep earlier scheduled and manual run IDs, timings, and audit metrics unchanged; this addendum records new isolated tests rather than rewriting historical run results.

Previously documented outstanding deliverables, including full business-query outputs, current-run quarantine breakdowns, and the remaining controlled observability experiments. Preserve those open items until evidence is collected.


## 19. Final assignment-gap review — evidence collected 15 October 2026

This section is the final review against evidence actually supplied in the terminal conversation. It supersedes earlier statements that the accepted-order ETL fact-to-aggregate trace, ELT rejected-order proof, and final automated test/parse results were still pending. Historical sections are retained for audit chronology.

### Newly verified

- **Accepted-order ETL fact-to-aggregate lineage:** `ORD002401`, 2026-10-01, S14 / NammaMart Vijayanagar / West. `etl_fact_orders.net_revenue = 475.95`; the join on `order_date` and `store_id` matched `etl_daily_zone_revenue` with `delivered_orders = 12` and aggregate `net_revenue = 5453.90`.
- **Rejected order:** `ORD000500` is absent from `etl_fact_orders` (`finance_fact_rows = 0`) and appears in ELT `orders_quarantine` with `invalid_order_status`.
- **Application tests:** `pytest -q -p no:cacheprovider` → **32 passed in 2.15s**.
- **DAG integrity:** `astro dev parse` → **4 passed in 1.20s**, `No errors detected in your Dags`.
- **Repository check:** `git diff --check` still reports three trailing-whitespace lines: `EVIDENCE.md` lines 263–264 and `SUBMISSION.md` line 661. This is a documentation hygiene issue, not a pipeline test failure.
- **Repository inventory:** `include/nammamart/etl_warehouse.py` (302 lines) and `tests/test_nammamart_etl_warehouse.py` (97 lines) are untracked and appear to be required implementation/test files. Several `.bak` / `.before_*` files and generated DuckDB/CSV artifacts are also untracked; classify them before staging. `include/output/etl_extracts/`, `include/output/nammamart/`, and `include/output/nammamart_elt/` are ignored, but some top-level generated artifacts under `include/output/` are not ignored. No source-data changes were reported by `git status --short -- include/data`.
- **Test warning:** Astro's integrity pytest emitted a cache permission warning for `/.pytest_cache`; all four DAG integrity tests passed. Record the warning accurately rather than treating it as a DAG parse failure.

### Remaining gaps — do not claim complete

1. **Part 0 profiling:** full per-column null counts, duplicate business-key counts, numeric/date ranges, all distinct category values, complete relationship checks, and full business-rule profiling are not all in the evidence pack.
2. **Business questions:** full Q1 415-row result, Q3 14-store result, Q5 top-ten spend values, and Q6 current-run quarantine counts by entity/file/reason remain outstanding. Q2 has seven category rows documented; Q4 populations must remain clearly distinguished.
3. **Observability:** missing-file and schema-mismatch checks are function-level/callable tests, not full Airflow failure/retry/alert tests. The 50-order and 100× price experiments are isolated function-level tests. Stale 30-day data, Clear/recovery on both DAGs, alert delivery, and the full prescribed controlled matrix on both DAGs remain incomplete. An independent price-outlier check is not implemented/evidenced when price and amount are changed consistently.
4. **Lineage:** accepted-order ETL fact-to-aggregate link is now verified. Exact original CSV row/line provenance and a separate ELT raw-to-mart trace remain outstanding.
5. **Repository hygiene:** three trailing-whitespace issues remain in `SUBMISSION.md` / `EVIDENCE.md`; untracked backup and generated artifact files need classification. No deletion or staging was performed as part of this review.
6. **Governance:** retention durations in the report are proposals, not approved policy; actual Slack alert delivery and external secrets backend configuration have not been demonstrated.
7. **Performance:** the 27m13.52s ELT result is one historical scheduled observation and missed the 09:30 target. Later manual runs succeeded but elapsed time was not captured; do not claim a performance improvement.

### Final review conclusion

The pipeline implementation has positive execution evidence (32 application tests, four Astro DAG integrity tests, successful ETL/ELT runs, quarantine proof, and an accepted-order ETL aggregate trace). The assignment is **not fully evidenced end-to-end** until the outstanding profiling/business-query outputs and prescribed Airflow observability experiments are either completed or explicitly accepted as limitations. Do not push or claim all rubric items complete on the basis of this review alone.
