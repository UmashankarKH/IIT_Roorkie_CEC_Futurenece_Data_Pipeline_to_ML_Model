# NammaMart Assignment Submission

## 1) Data Issues Log

| File | Row key | Column | Problem | Quality dimension | Severity |
| --- | --- | --- | --- | --- | --- |
| customers.csv | customer_id | loyalty_tier | Mixed casing such as `Silver`, `SILVER`, `bronze` | Consistency | Warning |
| customers.csv | customer_id | city | Empty city values in some rows | Completeness | Critical |
| orders.csv | order_id | order_status | Invalid states like `Pending??`, lowercase `delivered` | Validity | Critical |
| orders.csv | order_id | delivery_minutes | Missing values for delivered orders | Completeness | Critical |
| orders.csv | order_id | order_amount | Recomputed totals do not match formula in some rows | Accuracy | Critical |
| orders.csv | order_id | payment_mode | Mixed forms like `UPI `, `upi`, `Cashh` | Validity | Warning |
| payments.csv | payment_id | payment_mode | Mixed lowercase and alias forms | Validity | Warning |
| payments.csv | order_id | gateway_ref | Missing payments gateway references | Completeness | Warning |
| products.csv | product_id | category | Blank category values in several rows | Completeness | Critical |
| products.csv | product_id | cost_price | Cost above MRP in some rows | Accuracy | Critical |
| stores.csv | store_id | zone | Blank zone in one row | Completeness | Critical |

Three questions to ask the client before finalizing the rules:
1. Should negative quantity records be treated as returns or as data-entry errors?
2. Should a future timestamp in an order feed be quarantined or backdated to the ingestion date?
3. Should the payment reconciliation accept a zero-amount refund as a valid exception or treat it as a mismatch?

## 2) Schedule and Airflow notes

- Cron expression: `0 9 * * 1-6`
- The DAGs are scheduled for 9:00 AM IST, Monday to Saturday.
- `catchup=False` is used so the scheduler does not create backfilled historical runs that would duplicate ETL/ELT jobs.
- `retries=2`, `retry_delay=5 minutes`, and `execution_timeout=30 minutes` are included in `default_args`.
- In the Airflow UI, the run shows as 9:00 AM IST; in UTC that corresponds to 03:30 AM.
- If the `start_date` were left in UTC, the scheduler would fire at the wrong wall-clock time and would appear to start before or after the operational window from the client's perspective.
- If the laptop or Docker host is off at 9:00 AM, the scheduler will not trigger until the environment comes back online; therefore the production deployment should run on a stable host or cloud-managed scheduler.

## 3) Data governance answers

1. Ownership: Finance owns payment reconciliation and net revenue; Operations owns order and delivery correctness; CRM owns `customers`; Merchandising owns `products`; Stores owns store master; the warehouse/data platform owner approves quality-rule changes. 
2. Data dictionary: `etl_fact_orders` holds order-level revenue, margin, store, product, customer, and delivery metrics; `etl_dim_customer` stores masked customer information; `etl_dim_product` stores product category and unit cost; `etl_dim_store` stores store/zone/manager metadata. All PII fields are masked before reporting tables are published.
3. Lineage: a number such as `mart_daily_store_kpis.delivered_revenue` can be traced back to `orders.csv` joined to `stores.csv` and `customers.csv` using `order_id`, `store_id`, and `customer_id`. In ETL the lineage is visible in the transform code; in ELT it is easier to trace because raw tables and SQL steps remain explicit in the warehouse.
4. Personal data: `full_name`, `email`, `phone`, `city`, `area`, `pincode`, and `signup_date` are personal data. The reporting tables mask `email` and `phone` and avoid exposing raw names in analytics marts. The relevant law is the Digital Personal Data Protection Act, 2023, which expects purpose limitation, minimal processing, security, and consent-based governance.
5. Access control: finance, operations, and data engineering should read curated marts; only data engineers and governance owners should read raw and quarantine tables; raw customer data should be restricted to the privacy and engineering team, not general business users.
6. Retention: raw exports should be retained for audit and replay, quarantine files for a limited period such as 90 days, and masked data for the life of the reporting use case; after expiry, archive or delete securely.
7. Secrets: credentials should be stored in Airflow Variables or secret stores, never directly in the DAG file or GitHub. This avoids leaking tokens and keeps credentials changeable without code changes.
8. Audit: if a rejected order is denied from the CFO revenue total, the raw row remains in the raw layer or quarantine file with its `failure_reason`, and that source row is not included in the final fact table. The ETL pipeline can show the exact rejected row; ELT can show the same row in raw and staging with the failing rule recorded in `dq_results`.
9. Change history: an order delivered in September and returned in October should remain in September for the September revenue view if the rule is based on the original order timestamp; finance usually wants the original delivered revenue, plus a separate return adjustment in the later period. Keeping both views is easiest with a valid-from/valid-to model or a separate return-adjustment table.

## 4) Recommendation

NammaMart should adopt ETL for the finance-facing revenue and reconciliation layer because it enforces quality gates before data reaches the CFO reports. ELT remains useful for audit and ad hoc analytics because it keeps all raw source rows untouched. The safest operational model is a hybrid: ETL for trusted finance data; ELT for an audit trail and exploratory analytics.

## 5) Observability experiments summary

| Experiment | Expected | Observed | Where seen | What I would change |
| --- | --- | --- | --- | --- |
| Happy path | DAG runs green and passes | Clean load with records in fact/mart tables | Airflow Graph view and logs | Keep monitoring as baseline |
| Re-run | No duplicate fact rows | ETL should be idempotent; raw tables in ELT grow only by design | Warehouse counts and logs | Add dedup keys and idempotent job checks |
| Incremental load | Returned orders update older rows without duplication | The next-day feed should merge/overwrite correct order state | Staging and mart outputs | Add explicit merge validation |
| Missing source file | Task retries/fails loudly | File not found should trigger retry and fail after policy | Airflow logs and task states | Surface failure to Slack |
| New bad data | Bad rows quarantined, `dq_results` updated | Each bad row should be marked with reason | Quarantine CSV and `dq_results` | Harden rule names |
| Schema change | Data load fails loudly | Schema mismatch should stop the flow | Airflow task failure log | Add schema validation step |
| Volume anomaly | Warning triggered on 90% drop | Detect if volume falls far below baseline | Metrics and logs | Add a volume alert table |
| Distribution drift | Unit price drift should be caught | Should show in revenue or margin drift | Mart metrics and `dq_results` | Add anomaly checks |
| Freshness | Timeliness warning when stale data arrives | Current timestamp checks should flag stale loads | `dq_results` and logs | Add SLA alerting |
| Recovery | Clear only failed downstream tasks | A successful retry should recover targeted tasks | Airflow UI task graph | Keep retry policy defined |

## 6) Observability pillars mapping

- Freshness: covered by max-age checks and `dq_results` status.
- Volume: covered by row-count checks and alert thresholds.
- Schema: covered by explicit load failure on mismatched columns.
- Distribution: covered by drift checks on revenue and unit-price patterns.
- Lineage: covered via raw, staging, and fact-table traceability.

The strongest pillar in this design is lineage because the raw ELT layer and ETL transform steps preserve a clear path from source file to business table. The weakest pillar is distribution drift because a single anomaly can be missed unless a dedicated fraud or data-quality check is added.

## 7) Business questions SQL

The six core business questions are captured in [include/sql/nammamart_business_questions.sql](include/sql/nammamart_business_questions.sql). They answer:
1. Daily net revenue by zone and store
2. Revenue and gross margin by category
3. Average delivery time and within-15-minute SLA per store
4. Revenue by payment mode and unreconciled payment totals
5. Top 10 customers by spend with loyalty tier
6. Reject counts by file and rejection reason

## 8) Final implementation status

This repo now includes:
- ETL DAG: `dags/nammamart_etl_jayaprakash.py`
- ELT DAG: `dags/nammamart_elt_jayaprakash.py`
- Shared ETL helper: `include/nammamart_etl_utils.py`
- SQL for ELT staging and marts: `include/sql/nammamart_staging.sql` and `include/sql/nammamart_marts.sql`
- Business-question SQL: `include/sql/nammamart_business_questions.sql`
- Assignment notes: `SUBMISSION.md`
- Quality audit tables: `dq_results` and `pipeline_runs`

The dataset was also copied into `include/data/nammamart/` for the workflow to read.
