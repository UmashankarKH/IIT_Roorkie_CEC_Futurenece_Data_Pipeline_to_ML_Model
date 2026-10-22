# NammaMart Data Pipeline Submission

**Prepared for:** Sameer, CTO, NammaMart Quick Commerce
**Assignment:** FDAIE Module 2 — Data Engineering Foundations
**Implementation:** Apache Airflow, Python, pandas, DuckDB and SQL

## Executive summary

Two Airflow pipelines were built against the NammaMart source extracts. ETL
validates and transforms records before loading reporting tables; ELT retains
source rows in raw tables, applies SQL transformations in staging, and builds
business marts. Both DAGs use a 9:00 AM Asia/Kolkata schedule.

The successful 20 October 2026 runs report September delivered revenue of
**₹1,130,769.10 across 2,076 orders** in both pipelines. I recommend ELT as
NammaMart's long-term warehouse pattern because its append-only raw layer
supports replay and audit, while retaining ETL as an option for feeds that
must be screened before entering the warehouse.

The revenue total agrees, but category totals and some payment reconciliation
results differ between implementations. Those differences are recorded below
and must be reconciled before the affected figures are treated as finance
outputs. Controlled results for all ten observability experiments are not
available; this submission reports only evidence that was actually collected.

## 1. Data discovery and profiling

The six source files contain 5,372 rows in total. The profiling script
`profile_data.py` checked row counts, column counts, nulls and exact duplicate
rows. Additional rule and key findings are summarized below.

| Source | Rows × columns | Profile findings |
| --- | ---: | --- |
| `customers.csv` | 405 × 10 | 4 missing emails, 6 missing cities and 5 repeated customer keys. |
| `orders.csv` | 2,412 × 13 | 2 missing timestamps, 18 missing customer IDs, 164 missing delivery times and 12 duplicate rows. Additional rule checks found invalid quantities, statuses, payment modes, amounts, timestamps and foreign keys. |
| `payments.csv` | 2,228 × 7 | 301 missing gateway references and 4 duplicate rows; additional checks found invalid amounts/modes and unmatched or mismatched successful payments. |
| `products.csv` | 152 × 10 | 3 missing categories and repeated product keys; price relationships also require validation. |
| `stores.csv` | 15 × 8 | 1 missing zone. |
| `orders_2026-10-01_increment.csv` | 160 × 13 | 10 corrections to existing orders and 150 new orders; no nulls or duplicate rows in the profile. |

### Data issues log

| File | Issue and quality dimension | Severity | Pipeline handling |
| --- | --- | --- | --- |
| Customers | Missing email/city (completeness); repeated customer keys (uniqueness) | Warning / Critical for duplicate keys | Retain source values in restricted raw data; deduplicate for dimensions and mask contact fields in reporting. |
| Orders and increment | Missing required IDs/timestamps, duplicate order IDs, invalid status/mode/quantity, amount mismatch, implausible or future timestamps, unresolved customer/product/store IDs (completeness, uniqueness, validity, accuracy, consistency, timeliness) | Critical | ETL quarantines rejected rows with reasons; ELT retains raw rows and marks invalid staging rows. Increment corrections are merged by `order_id`. |
| Payments | Duplicate payment rows, missing gateway references, invalid amounts/modes, unmatched or mismatched successful payments (uniqueness, completeness, validity, consistency) | Critical for reconciliation failures; warning for missing reference | Retained for reconciliation; mismatches and missing successful payments are reported separately. |
| Products | Missing category, repeated product keys, invalid price relationships (completeness, uniqueness, accuracy) | Critical for invalid keys/prices; warning for missing category | Invalid or duplicate dimension records are excluded or flagged by transformation rules. |
| Stores | Missing zone (completeness) | Warning | Preserve the source value and flag it for steward review. |

Three questions for the business owners before finalizing production rules:

1. Should an order with a negative quantity represent a return, or should it
   always be rejected as a data-entry error?
2. When an order is later returned, should the correction restate the original
   day's revenue, or should Finance retain a separate historical revenue and
   refund view?
3. What freshness and volume thresholds should trigger a warning or prevent
   publication of the morning report?

## 2. Pipeline implementation

### ETL

[`dags/nammamart_etl_surender_singh.py`](./dags/nammamart_etl_surender_singh.py)
extracts customers, products, stores, orders and payments in parallel. It
validates orders, writes failures to
`include/quarantine/surender_singh/`, masks customer contact fields in
reporting dimensions, loads DuckDB tables, reconciles payments and builds
reporting outputs. A successful fresh run extracted 5,372 rows, loaded 3,023
and quarantined 92.

### ELT

[`dags/nammamart_elt_surender_singh.py`](./dags/nammamart_elt_surender_singh.py)
appends source records and run metadata to raw tables, applies SQL-based
staging and quality checks, merges orders by `order_id`, and builds five
business marts. A successful fresh run extracted 5,372 rows, staged 5,339 and
recorded 102 rejected rows. The current staging state contains 2,550 unique
order IDs. The live merge reported no inserts or updates because the increment
had already been loaded. The isolated merge regression test verified 150
initial inserts, 10 updates and an idempotent repeat.

The order merge and mart SQL are in
[`include/sql/elt_merge_orders.sql`](./include/sql/elt_merge_orders.sql) and
[`include/sql/elt_marts.sql`](./include/sql/elt_marts.sql). Customer, product,
store and payment staging transformations remain inline in the ELT DAG rather
than in separate SQL assets.

## 3. Schedule and service level

Both DAGs use cron `0 9 * * 1-6` and the `Asia/Kolkata` timezone. The Airflow
UI displays the equivalent run time as **03:30 UTC**. `catchup=False` prevents
automatic replay of every missed interval after downtime; if the laptop or
Docker is off at the scheduled time, the run does not execute then and the
scheduler waits for the next scheduled interval after service resumes.

The DAG defaults set two retries, a five-minute retry delay and a 30-minute
execution timeout. A UTC `start_date` with the same 09:00 cron would schedule
at 09:00 UTC, or 14:30 IST, which misses the morning target.

## 4. Data quality and run observability

Both pipelines write quality-check outcomes to `dq_results` with run/DAG IDs,
check name, dimension, severity, checked and failed row counts, status and
timestamp. Checks cover completeness, uniqueness, validity, accuracy,
consistency and timeliness. Critical order failures are quarantined in ETL or
marked failed in ELT staging; warnings are recorded for review.

| Run | Pipeline | Result | Extracted | Loaded / staged | Quarantined / rejected |
| --- | --- | --- | ---: | ---: | ---: |
| `wrap_up_etl_20261020_1630` | ETL | Success | 5,372 | 3,023 | 92 |
| `wrap_up_elt_rejections_20261020` | ELT | Success | 5,372 | 5,339 | 102 |

The ELT raw tables retain load time, source file and run ID. ETL quarantine
records retain failure reasons. The ETL fact table can be rebuilt from source,
but it does not retain the same source-file metadata as ELT. Both approaches
provide run and quality outcomes; ELT provides the stronger row-level replay
trail.

The available screenshots show DAG graph views:
[`ETL graph`](./include/screenshot/nammamart_etl_pipeline-graph.png),
[`ELT graph`](./include/screenshot/nammamart_elt_surender_singh-graph.png)
and [`Airflow graph`](./include/screenshot/nammamart_airflow_surender_singh-graph.png).
Schedule, `dq_results`, `pipeline_runs` and reconciliation UI screenshots are
not included in the repository.

### Observability experiment evidence

| Experiment | Evidence available |
| --- | --- |
| Happy path | Both pipelines have successful fresh runs; graph screenshots are available. |
| Re-run / idempotency | ELT rerun retained 2,550 unique staged order IDs. Controlled ETL before/after row-count comparison is not recorded. |
| Incremental load | Current state includes 150 new delivered orders and 10 returned corrections. A regression test verifies 150 inserts, 10 updates and an idempotent repeat; the live run had already loaded the increment. |
| Missing source file | No controlled experiment result recorded. |
| New invalid data | No controlled experiment result recorded. |
| Schema change | No controlled experiment result recorded. |
| Volume anomaly | No controlled experiment result recorded; a volume-anomaly alert is not implemented. |
| Distribution drift | No controlled experiment result recorded; distribution-drift alerting is not implemented. |
| Freshness | Future timestamps are checked. A controlled 30-day stale-data test and stale-data alert threshold are not recorded. |
| Recovery | No controlled failure-and-clear experiment result recorded. |

Of the five observability pillars, lineage is best supported by ELT's source
metadata and the order trace below. Volume, schema and distribution alerting
are the weakest areas; stale-data alerting also needs a threshold and a
controlled test.

## 5. Business-question results

The SQL variants for both pipelines are in
[`include/sql/nammamart_business_questions.sql`](./include/sql/nammamart_business_questions.sql).
The complete result snapshot is in
[`include/sql/business_question_results.md`](./include/sql/business_question_results.md).

| Question | Result summary |
| --- | --- |
| Daily net revenue by zone and store | September delivered revenue totals ₹1,130,769.10 in both pipelines. The top daily store total is ₹9,732.00 for Yelahanka on 19 September. |
| Revenue and gross margin by category | Results are available for both pipelines, but the category totals differ. This variance has not been isolated; do not treat category margins as final Finance outputs. |
| Delivery SLA by store | Both queries return 15 stores. The fastest average is Hebbal (about 20.5 minutes); the slowest is Banashankari (about 22.52 minutes). |
| Revenue by payment mode and reconciliation | Payment-mode totals and mismatch counts differ. ELT reports 12 amount mismatches and 220 missing successful payments; ETL reports 13 amount mismatches and 219 missing records. ELT also reports 81 unmatched successful payments, which ETL does not represent equivalently. |
| Top 10 customers by spend | Both queries return 10 customers. The top customer is C00136 at ₹8,738.10. The results omit email and phone; loyalty-tier casing should be normalized for customer-facing reporting. |
| Rejected rows by source and reason | ELT's most frequent reasons include missing customer ID (18), amount mismatch (15), invalid quantity (10), unknown product (9) and missing delivery time (8). ETL shows the same five counts in its persisted quarantine output. |

## 6. Data governance

### Ownership

Operations owns orders and stores; Finance owns payments and reconciliation
policy; Customer Operations owns customer records; Merchandising owns
products; Data Engineering operates warehouse models. The relevant business
owner approves quality-rule changes, with Data Engineering implementing and
testing the approved rule.

### Data dictionary

| Table | Columns / type | Meaning and source | Allowed values / PII |
| --- | --- | --- | --- |
| `etl_fact_orders` | `order_id` VARCHAR; `order_ts` TIMESTAMP; customer/store/product IDs VARCHAR; quantity INTEGER; revenue and margin DECIMAL; status VARCHAR; delivery minutes INTEGER | Order facts derived from `orders.csv` and the increment file; joined to customer, product and store dimensions. | Status: DELIVERED, CANCELLED, RETURNED. Customer ID is identifying data; no email or phone is stored in the fact. |
| `etl_dim_customer` | Customer ID, name, masked email/phone, city/area/pincode, signup date, loyalty tier | CRM records from `customers.csv`, deduplicated and transformed for reporting. | Loyalty tier follows source values; email/phone are masked. Name and location fields are personal or identifying data. |
| `etl_dim_product` | Product ID, name, category, sub-category, brand, MRP, cost, unit, perishability, launch date | Product catalogue from `products.csv`. | Category follows the catalogue; no customer PII. |
| `etl_dim_store` | Store ID, name, zone, area, pincode, open date, manager name, capacity | Store master from `stores.csv`. | Zone follows source values (North, South, East, West); manager name may identify an individual. |
| `raw_*` / `stg_*` / `mart_*` | Raw source columns and load metadata; typed/quality-marked staging fields; business metrics in marts | ELT raw, staging and mart layers. | Raw customer fields include unmasked PII; access is restricted. Reporting marts mask contact fields and exclude raw email/phone from customer spend results. |

### Lineage

In the verified example, order `ORD001440` contributes ₹1,975 to the 19
September Yelahanka delivered revenue total of ₹9,732. Its source is
`include/data/nammamart/orders.csv`, row 348 when counting the header. ELT
retains source-file and run metadata, so the source trace is more direct. ETL
can trace the order key back to the CSV, but its fact table does not retain
the same source metadata.

### Personal data, access and retention

Customer ID, full name, email, phone, city/area/pincode and other linked
location details can identify or relate to a customer; store manager name can
identify an employee. Email and phone are masked in reporting rather than
deleted so restricted audit/replay remains possible. Raw customer records
should be limited to approved custodians and engineers; staging and quarantine
to engineering, quality stewards and source owners; and marts to authorized
Finance, Operations and analytics roles under least privilege.

The Digital Personal Data Protection Act, 2023, and applicable rules govern
the handling of customer personal data. NammaMart should apply purpose
limitation, appropriate safeguards, access controls and approved retention
and deletion practices, with deployment-specific obligations confirmed by
legal counsel. Retention periods should be set by business and legal purpose;
90 days is a proposed quarantine review window, not an enforced repository
policy. The warehouse currently does not implement automated retention or
deletion.

### Secrets, audit and change history

No Slack webhook is configured. If notifications are enabled, credentials
belong in Airflow Connections or Variables, not DAG source, Git, logs or
screenshots.

For example, the ETL quarantine contains order `ORD002255` from the base
orders input with a missing `customer_id`; its recorded failure reason is
`Missing customer_id`. It remains available with its reason in quarantine and
is excluded from the accepted fact/revenue rows. ELT keeps its raw source
record and marks it failed in staging, so the rejected value can be inspected
without admitting it to the business mart.

ELT's append-only raw layer preserves the original and corrected order
versions, while its staging and marts represent the latest accepted order
state. ETL rebuilds current facts and does not retain a historical revenue
snapshot. In the increment, 10 returned corrections update prior orders.
Finance should confirm whether that restates September revenue; preserving
both views would require a separate refund/change ledger or effective-dated
history.

## 7. Recommendation to NammaMart

**To: Sameer, CTO — Recommendation: adopt ELT as the primary warehouse pattern.**

ELT is the better long-term fit for NammaMart's audit and change-history
requirements. Its raw append-only layer retains source rows with file and run
metadata, and the tested `order_id` merge applies the 150 new orders and 10
corrections without creating duplicate staged orders. SQL staging and marts
also make warehouse transformations inspectable and replayable as business
rules evolve.

ETL remains valuable for feeds whose invalid or sensitive records must be
blocked before warehouse loading. In the current build it quarantines failed
orders and masks customer contact fields in reporting, but it does not retain
ELT's equivalent raw history. Conversely, the current ELT design must enforce
strict access controls because raw customer data is intentionally retained.

The recommendation is based on the implementation and the verified run
evidence, not on a measured 1,000× scale test. Performance and cost at that
volume remain unknown. Before production adoption, NammaMart should:

1. Reconcile the category and payment-result differences between ETL and ELT.
2. Add and test stale-data, volume, schema-change and distribution-drift
   alerting, and complete the controlled failure/recovery experiments.
3. Define raw-data access and retention policies, and decide how returned
   orders should affect historical financial reporting.

## 8. Validation and evidence limits

The project test suite passed 12 tests. The merge SQL regression test verifies
initial inserts, correction updates and idempotency. This evidence does not
replace the unrun controlled observability experiments listed above. The
report also does not claim that category results, payment reconciliation
differences, retention enforcement or 1,000× performance/cost have been
validated.
