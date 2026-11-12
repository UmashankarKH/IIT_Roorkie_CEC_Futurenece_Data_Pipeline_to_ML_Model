# NammaMart ETL/ELT Pipeline — Evidence Report

## Latest reconciliation verification — 15 October 2026

After updating the audit schema and both DAGs, the user triggered fresh manual runs and queried `pipeline_runs`. The actual terminal output was:

```text
dag_id                  run_id                                      rows_extracted rows_loaded rows_quarantined rows_deduplicated rows_superseded status
nammamart_etl_pipeline  manual__2026-10-15T08:38:23.580596+00:00   5372           4599        747              16                10              SUCCESS
nammamart_elt_pipeline  manual__2026-10-15T08:38:19.022844+00:00   5372           4599        747              16                10              SUCCESS
```

Both new manual runs report identical metrics and `SUCCESS`. The code validation immediately before these runs showed `30 passed` with `pytest -q -p no:cacheprovider`; `py_compile` completed without errors for the audit module, reconciliation helper, and both DAG files.

```text
5372 physical source rows = 4599 loaded + 747 quarantined + 16 deduplicated + 10 superseded
```

The older 15 October scheduled-run records still report 5,346 extracted and zero in the new counters. Those records predate the change and were intentionally left unchanged; use the two manual run IDs above as the verified corrected audit evidence.


**Prepared:** 15 October 2026
**Prepared by:** Gowtham Manivel
**Purpose:** Summarize the execution, quarantine, data-quality, and reconciliation evidence captured during the work period beginning the night of 14 October and continuing on 15 October 2026.

## Context and scope

The work was performed across 14–15 October 2026. The source exports available for these runs were a **static snapshot**, not a newly arriving daily feed. Therefore, the repeated runs demonstrate processing and audit behavior against the available snapshot; they do not demonstrate that fresh production data arrived each day. In particular, the latest-order freshness warning is expected for this historical data.

This report distinguishes the **14 October manual ELT run**, the **15 October scheduled ETL and ELT runs**, and the **15 October manual verification runs after the audit correction**. Counts and timings are reported as captured in the supplied terminal output. They are not a controlled performance benchmark.

## Run audit evidence

| Run | DAG | Status | Extracted | Loaded | Quarantined | Start (UTC) | End (UTC) | Elapsed |
|---|---|---:|---:|---:|---:|---|---|---:|
| 14 Oct manual | `nammamart_elt_pipeline` | SUCCESS | 5,346 | 4,599 | 747 | 11:49:33.484664 | 11:49:36.866841 | 3.38 sec |
| 15 Oct scheduled | `nammamart_etl_pipeline` | SUCCESS | 5,346 | 4,599 | 747 | 03:42:04.256375 | 03:42:09.484675 | 5.23 sec |
| 15 Oct scheduled | `nammamart_elt_pipeline` | SUCCESS | 5,346 | 4,599 | 747 | 03:42:04.257408 | 04:09:17.774766 | 27 min 13.52 sec |
| 15 Oct manual verification | `nammamart_etl_pipeline` | SUCCESS | 5,372 | 4,599 | 747 | Not captured | Not captured | Not benchmarked |
| 15 Oct manual verification | `nammamart_elt_pipeline` | SUCCESS | 5,372 | 4,599 | 747 | Not captured | Not captured | Not benchmarked |

The 15 October scheduled logical time was **03:30 UTC (09:00 IST)**. Both tasks began after that logical time. ETL completed at approximately **09:12 IST**; ELT completed at approximately **09:39 IST**, nine minutes after the 09:30 leadership stand-up target. The ELT timing differs substantially between the 14 October manual run and the 15 October scheduled run, so it should be treated as an observed run-to-run variation, not a stable benchmark or an explanation of the underlying cause.

The audit counts balance internally:

```text
Historical scheduled-run balance: 5,346 = 4,599 + 747
Latest manual-run balance:       5,372 = 4,599 + 747 + 16 + 10
```

For the latest manual runs, physical rows = loaded + quarantined + deduplicated + superseded.

## Quarantine evidence

The supplied file listings show quarantine outputs for both `nammamart` and `nammamart_elt`. The counts reported for each pipeline are the same:

| Entity | ETL quarantined | ELT quarantined |
|---|---:|---:|
| Customers | 13 | 13 |
| Products | 9 | 9 |
| Stores | 1 | 1 |
| Orders | 373 | 373 |
| Payments | 351 | 351 |
| **Total** | **747** | **747** |

The displayed tables include a `quarantine_reason` field. The detailed ELT payment quarantine table contains 351 rows, while `payments_key_quarantine` contains 0 rows.

| ELT payment quarantine reason | Rows |
|---|---:|
| `orphan_order_id` | 326 |
| `payment_amount_mismatch` | 10 |
| `orphan_order_id,future_payment_ts` | 7 |
| `orphan_order_id,invalid_amount_paid` | 5 |
| `orphan_order_id,invalid_payment_mode` | 2 |
| `invalid_amount_paid,payment_amount_mismatch` | 1 |
| **Total** | **351** |

There are **11 rows whose reason includes `payment_amount_mismatch`**: 10 mismatch-only rows and 1 combined-reason row. A broad search for `%amount%` is not an accurate mismatch count because it also matches `invalid_amount_paid`.

The evidence shows that the ETL and ELT quarantine **totals** match. It also shows some rule-name differences between the pipelines—for example, order delivery-time failures use different labels. Equal totals alone do not prove that the two pipelines apply exactly equivalent rules; a normalized reason-by-reason comparison is still needed.

## Data-quality evidence

The supplied DQ result contains **23 checks**: **19 PASS** and **4 Warning-level FAIL**. All Critical checks shown passed. The Warning failures are:

| Check | Checked | Failed | Interpretation in the supplied evidence |
|---|---:|---:|---|
| `customer_email_completeness` | 405 | 4 | Four customers have missing or blank email values. |
| `latest_order_freshness_7_days` | 1 | 1 | Latest order is stale relative to the seven-day rule; input is historical/static. |
| `customer_email_format` | 401 | 8 | Eight populated email values fail the format rule. |
| `customer_pincode_format` | 392 | 3 | Three populated pincodes fail the format rule. |

The output also records zero failures for the listed Critical checks, including order amount formula accuracy, product pricing accuracy, referential integrity, future-dated timestamps, payment reconciliation, and curated-key uniqueness. A separate duplicate-check query for the **14 October manual ELT run ID** returned no duplicate check names. That result is scoped to that specific run and DAG; it does not by itself prove that every run in both pipelines has no duplicate DQ records.

## Payment reconciliation evidence

The ELT mart query returned the following order-level statuses:

| Reconciliation status | Orders | Sum of absolute stored `amount_difference` |
|---|---:|---:|
| `RECONCILED` | 1,774 | 0.00 |
| `NO_SUCCESSFUL_PAYMENT` | 403 | 229,599.95 |
| `ORDER_QUARANTINED` | 373 | 226,150.08 |
| **Total** | **2,550** | — |

The mart returned no `AMOUNT_MISMATCH` rows. The captured pipeline behavior excludes rejected payment records from the successful-payment mart totals; mismatch cases are visible in `payments_quarantine` instead. The `NO_SUCCESSFUL_PAYMENT` and `ORDER_QUARANTINED` amounts are **not confirmed gateway losses** and should not be described as such. They are status-based stored differences requiring separate interpretation.

An initial query used a non-existent table name, `quarantine_payments`. The table inspection showed the actual names are `payments_quarantine` and `payments_key_quarantine`; the later corrected query against `payments_quarantine` produced the reason counts above.

## Source-row reconciliation: 26-row accounting diagnosis

The six source CSVs contain **5,372 physical rows**. The older scheduled-run records report **5,346 extracted, 4,599 loaded, and 747 quarantined** because they predate the audit correction. Two fresh manual runs on 15 October now report the physical source count and the explicit adjustment metrics: 5,372 extracted, 4,599 loaded, 747 quarantined, 16 deduplicated, and 10 superseded. The older historical records are preserved unchanged.

The follow-up source inspection identified the following. The counts below are directly from the supplied Python/pandas terminal output. The ten overlapping IDs were printed with base and increment records; in every displayed pair, the non-status fields match and `order_status` changes from `DELIVERED` to `RETURNED`.

| Finding | Count | Evidence / interpretation |
|---|---:|---|
| Exact duplicate rows in base `orders.csv` | 12 | `DataFrame.duplicated()` identified 12 duplicate copies; the duplicate order IDs were not among the IDs that also appear in the increment file. |
| Order IDs present in both base and increment files | 10 | Each inspected pair has the same order details but a status change from `DELIVERED` in the base file to `RETURNED` in the increment file. These are upserts; the base versions are superseded by the increment status. |
| Exact duplicate rows in `payments.csv` | 4 | `DataFrame.duplicated()` identified four duplicate copies. |
| **Total preprocessing rows to account for** | **26** | **12 exact order duplicates + 10 superseded base order versions + 4 exact payment duplicates.** |

### Source inspection evidence (actual counts and comparison output)

The diagnostic was run against the source CSVs using pandas `DataFrame.duplicated()` and the intersection of trimmed `order_id` values.

```text
EXACT DUPLICATE ROWS
Base orders: 12
Incremental orders: 0
Payments: 4

ORDER UPSERT OVERLAP
Unique IDs in both files: 10

SOURCE COUNTS
Base orders: 2412
Incremental orders: 160
Combined order rows: 2572
Payments: 2228

Exact duplicate copies whose order IDs also overlap with increment: 0
```

The ten order IDs printed by the comparison were:

| Order ID | Base status | Increment status | Fields changed in displayed pair |
|---|---|---|---|
| `ORD000017` | `DELIVERED` | `RETURNED` | `order_status` |
| `ORD000271` | `DELIVERED` | `RETURNED` | `order_status` |
| `ORD000332` | `DELIVERED` | `RETURNED` | `order_status` |
| `ORD000337` | `DELIVERED` | `RETURNED` | `order_status` |
| `ORD000584` | `DELIVERED` | `RETURNED` | `order_status` |
| `ORD000708` | `DELIVERED` | `RETURNED` | `order_status` |
| `ORD000822` | `DELIVERED` | `RETURNED` | `order_status` |
| `ORD001025` | `DELIVERED` | `RETURNED` | `order_status` |
| `ORD001067` | `DELIVERED` | `RETURNED` | `order_status` |
| `ORD001990` | `DELIVERED` | `RETURNED` | `order_status` |

Example of the actual printed comparison for `ORD000017`:

```text
BASE:      order_id=ORD000017, order_status=DELIVERED, order_amount=16.0
INCREMENT: order_id=ORD000017, order_status=RETURNED,  order_amount=16.0
```

The full terminal output also printed every field for all ten pairs. The 12 duplicate base-order IDs were `ORD000539`, `ORD000783`, `ORD001110`, `ORD001239`, `ORD001326`, `ORD001434`, `ORD001441`, `ORD001462`, `ORD001549`, `ORD001692`, `ORD001870`, and `ORD002129`. None of those IDs overlapped with the increment file. This supports classifying the 10 overlapping versions as status upserts rather than exact duplicate rows.

### ELT raw-retention evidence

The captured raw table count output showed:

```text
raw_customers:        405
raw_products:         152
raw_stores:            15
raw_orders:          2412
raw_orders_increment: 160
raw_payments:        2228
```

Those are the six source-file counts represented in the raw layer.

The equivalent DuckDB verification pattern for a consolidated raw layout is:

```sql
SELECT _source_file, COUNT(*) AS raw_rows
FROM raw_orders
GROUP BY _source_file
ORDER BY _source_file;

SELECT 'customers.csv' AS source_file, COUNT(*) AS raw_rows FROM raw_customers
UNION ALL
SELECT 'products.csv', COUNT(*) FROM raw_products
UNION ALL
SELECT 'stores.csv', COUNT(*) FROM raw_stores
UNION ALL
SELECT 'payments.csv', COUNT(*) FROM raw_payments;
```

If the database keeps `raw_orders_increment` as a separate table instead, count it separately and do not query it as part of `raw_orders`. These are reproducible verification queries; the current evidence shows the counts above but does not include a newly captured same-run grouped-by-`_source_file` result. In the later consolidated raw layout, `raw_orders` contains 2,572 rows (2,412 base + 160 increment) with source-file metadata. Therefore, ELT raw retention is evidenced for all six source files, including duplicate rows. The duplicate and upsert handling occurs downstream in staging. The evidence pack does not include a fresh query output of raw counts grouped by `_source_file` for the same latest run; capture it if strict same-run proof is required.

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

**Audit implementation status:** the schema migration and both DAG code paths now persist `rows_deduplicated` and `rows_superseded`. The latest manual-run query verified the same corrected metrics for both pipelines. Historical scheduled-run records remain unchanged and should not be rewritten. ELT raw retains all six source-file row sets in the captured table counts, including the 2,412 base orders, 160 increment rows, and 2,228 payments. Exact duplicate rows and superseded base versions must remain in raw; deduplication and upsert accounting belongs in staging/audit metrics, not in destructive raw-layer deletion.

## Evidence limitations and next steps

- The 14 October run is a manual ELT run; the 15 October entries are the scheduled ETL/ELT runs. Keep their dates and run types distinct in screenshots and written claims.
- The static input snapshot explains why a freshness warning can persist across runs. It does not demonstrate successful ingestion of newly arriving next-day data or incremental corrections.
- The counts, query outputs, and table schemas here come from pasted terminal logs. Attach the requested Airflow UI screenshots and saved query outputs separately if required for submission.
- The explicit `rows_deduplicated` and `rows_superseded` audit metrics are implemented and verified in fresh ETL and ELT manual runs.
- The older scheduled records still show `rows_extracted=5,346`; this is historical and not the physical source-row total. Fresh manual runs are the evidence for the corrected 5,372 physical-row count.
- Full Part 0 per-column profiling, exact row-to-mart lineage, a rejected-order proof query, top-ten spend output, full Q1/Q3 outputs, all current-run Q6 reasons, and prescribed controlled observability experiments remain missing from the evidence pack.
- Align quarantine reason names and compare ETL/ELT counts by normalized rule, not just by entity total.
- For the assignment’s observability section, separately document the required failure, bad-data, schema-change, volume, distribution-drift, freshness, and recovery experiments for both DAGs. The evidence in this report does not establish that all those tests were performed.

## Conclusion

The supplied logs support successful audited runs, matching ETL/ELT entity quarantine totals, a 23-check DQ result with Critical checks passing, and a payment-reconciliation mart that distinguishes reconciled orders from orders without successful payments and quarantined orders. The 15 October scheduled ELT run missed the 09:30 stand-up target. Follow-up source inspection explains the 26-row difference (12 duplicate base-order copies, 10 superseded base-order versions, and 4 duplicate payment copies). Fresh manual-run audit records now explicitly verify 16 deduplicated and 10 superseded rows in both pipelines. The static-data context should be stated explicitly: these runs validate processing of the available snapshot, not daily freshness of a live source feed.


## Remaining assignment deliverables not yet evidenced

This evidence report intentionally does not imply full assignment completion. The following items still need output captured from the live repo/runtime:

- **Part 0:** per-column null counts, duplicate business-key counts, numeric/date min/max, all requested category distinct values, four relationship checks, full business-rule profiling, and three client questions. Three candidate questions are now drafted in the submission report; they still require client confirmation.
- **Part C:** answers are documented in the submission report: 09:00 IST equals 03:30 UTC; a UTC cron/timezone would run at 14:30 IST; a local laptop/Docker outage prevents the local scheduler from running and `catchup=False` does not replay all missed intervals.
- **Part E:** actual `DESCRIBE` output, exact row-to-mart lineage, raw rejected-record/fact-exclusion query output, implemented role/access controls, approved retention policy, and confirmation of actual secret storage are not evidenced. Proposed retention durations are design recommendations, not approved policy.
- **Part F:** the ten prescribed experiments are not all demonstrated on both DAGs. Missing-file, injected-bad-data, schema-change, 50-row volume, distribution drift, 30-day freshness, and Clear/recovery experiments must be run and evidenced before marking Part F complete.
- **Business questions:** full Q1 (415 rows), Q3 (14 stores), Q5 (top-ten spend values), and Q6 (current-run reason counts across all entities) are not in the evidence pack. Q1 SQL now includes a September date filter; delivered-only semantics must be confirmed in the mart SQL. Q4 order-side revenue by mode and successful reconciled payment amounts are different populations and must be labelled separately.
- **Return accounting:** the ten `DELIVERED` → `RETURNED` updates are identified, but the exact September revenue impact has not been calculated. A current-state view should exclude returned orders from delivered-only revenue; a finance “as originally reported” view requires retained status history/snapshotting.


## Code and runtime verification details

- `python -m py_compile include/nammamart/audit.py include/nammamart/reconciliation.py dags/nammamart_etl_dag.py dags/nammamart_elt_dag.py` completed successfully.
- `pytest -q -p no:cacheprovider`: **30 passed in 2.00s**.
- `pipeline_runs` was queried read-only after fresh manual runs. Both new run IDs, statuses, and all five row counters are recorded in the Latest reconciliation verification section above.
- No historical run rows were rewritten.

---

**Captured:** 15 October 2026
**Environment:** Astro container at `/usr/local/airflow`
**Purpose:** Consolidated evidence report containing the original run evidence and the latest controlled tests. Historical records are preserved and new evidence is separately dated.

## Evidence E7 — Missing source file

Command: direct isolated invocation of `_extract_group("missing_file_test", ["missing_file_test_only.csv"])`.

Observed output:
```text
PASS: Missing source file detected
Expected exception: FileNotFoundError
Error message: Required source file is missing: /usr/local/airflow/include/data/nammamart/missing_file_test_only.csv
```

Scope: the function raised the expected exception. This did not exercise Airflow retries or alert delivery.

## Evidence E8 — Schema mismatch

The actual callable for DAG task `validate_extractions` was obtained from `nammamart_etl_pipeline` and invoked using a temporary extraction directory. The temporary `customers.csv` lacked `email`.

Observed output:
```text
VALIDATOR ERROR: Schema mismatch in customers.csv; missing=['email'], unexpected=[]
PASS: Actual validator rejected the missing email column.
Original source CSVs and warehouse were not modified.
```

## Evidence E9 — 50-order transformation and validation

The first fixture attempt retained 159 incremental rows and was invalid as a 50-order experiment. It is not used as final passing evidence.

The corrected fixture emptied only the temporary incremental CSV. The diagnostic identified `ORD001812` as an initial quarantine record with `invalid_payment_mode` (`Cashh`). The final diagnostic showed no overlap between initial and validation quarantine IDs.

Final counts:
```text
Selected distinct IDs: 50
Transformed IDs: 49
Initial quarantine rows: 1
Curated: 40
Validation quarantine rows: 9
Overlap: []
```

Reconciliation:
```text
40 curated + 9 validation-quarantined + 1 initially quarantined = 50 selected rows
```

Validation quarantine reasons included five `orphan_store_id` rows, one invalid payment mode, one combined `orphan_customer_id; orphan_store_id` row, one `orphan_product_id`, one `orphan_customer_id`, and one `missing_customer_id`. There are nine validation-quarantined rows and ten reason occurrences because one row has two reasons.

The transformation/validation functions were called directly. No shared output writer, DuckDB warehouse loader, or Airflow DAG execution was used.

## Evidence E10 — 100× unit-price experiment

The test used in-memory copies and selected order `ORD001913`.

Baseline:
```text
Original unit_price: 222.0
Original order_amount: 421.8
```

Experiment A — unit price multiplied by 100; amount left unchanged:
```text
row_key     column       problem
ORD001913   order_amount Order amount does not match the required formula
```
The rule emitted a Critical Accuracy issue.

Experiment B — unit price multiplied by 100; amount recalculated consistently:
```text
No issues detected
```

Interpretation: the current rule detects formula inconsistency, not an independently unusual price. A price-outlier threshold/rule would be needed to flag Experiment B. No source CSV or warehouse writes occurred.

## Evidence E11 — ETL warehouse table inventory and schema

`include/warehouse_gowtham.duckdb` contains:
```text
etl_category_margin
etl_daily_zone_revenue
etl_dim_customer
etl_dim_product
etl_dim_store
etl_fact_orders
etl_payment_reconciliation
```

Observed counts:
```text
etl_daily_zone_revenue       429
etl_category_margin            7
etl_payment_reconciliation   604
```

The `DESCRIBE etl_fact_orders` output included `order_id`, `order_ts`, customer/store/product IDs, quantity, unit price, discount, amount, payment mode, status, channel, delivery minutes, source metadata, order date, category, cost, zone, net revenue, gross margin, `within_15_min`, and `run_id`.

Referential integrity query output:
```text
fact_rows  distinct_orders  missing_customers  missing_products  missing_stores
2177       2177             0                  0                 0
```

Rejected-order finance-fact proof:
```sql
SELECT COUNT(*) AS finance_fact_rows
FROM etl_fact_orders
WHERE order_id = 'ORD000500';
```
Observed result: `finance_fact_rows = 0`.

This proves absence from the ETL fact table for the queried database state. The captured ELT `orders_quarantine` result is `ORD000500 | invalid_order_status`. A source-row-to-mart trace also remains open; use a separate accepted order for the fact-to-aggregate trace.

## Evidence E12 — Scope and open gaps

Verified in this addendum:
- Missing-file exception at function level.
- Schema rejection by the actual validator callable.
- Isolated 50-order transformation/validation and count reconciliation.
- Detection of an amount-formula mismatch after a 100× unit-price mutation.
- Existing rule gap when unit price and amount are changed consistently.
- ETL fact uniqueness and dimension-reference counts for the queried warehouse state.

Still open:
- Airflow task failure, retry, and successful recovery for missing-file/schema failures.
- Alert/notification delivery evidence.
- Controlled stale-data experiment.
- Full recovery/clear experiments for both ETL and ELT.
- Remaining business-query outputs and current-run rejection breakdowns listed in the main submission.
- A complete controlled observability matrix across both pipelines.

Do not label the full Part F matrix complete until these open experiments have actual evidence.


## Remaining query evidence to capture

The ETL fact exclusion for `ORD000500` is evidenced (`finance_fact_rows = 0`). The following output is still required to close the ELT side of the rejected-order audit path and demonstrate a concrete lineage example.

ELT database:
```sql
SHOW TABLES;
SELECT order_id, quarantine_reason, source_file
FROM orders_quarantine
WHERE order_id = 'ORD000500';
```

ETL database:
```sql
SELECT COUNT(*) AS finance_fact_rows
FROM etl_fact_orders
WHERE order_id = 'ORD000500';

DESCRIBE etl_fact_orders;
DESCRIBE etl_daily_zone_revenue;
```

The accepted-order ETL fact-to-aggregate trace has since been captured for `ORD002401` (Evidence E15). Exact source CSV row/line provenance, separate ELT raw-to-mart lineage, and ELT mart schema evidence remain open if required by the rubric.

## Performance interpretation

The 15 October scheduled run durations are a single observation: ETL approximately 5.23 seconds and ELT approximately 27 minutes 13.52 seconds. Later manual verification runs succeeded, but their elapsed times were not captured. The manual runs therefore establish successful execution and corrected reconciliation counters, not a measured performance improvement.


## Evidence E13 — ELT quarantine proof for `ORD000500`

The user-provided DuckDB query output was:

```text
SELECT order_id, quarantine_reason
FROM orders_quarantine
WHERE order_id = 'ORD000500';

order_id   quarantine_reason
ORD000500  invalid_order_status
```

This confirms that the ELT quarantine table contains `ORD000500` and records `invalid_order_status` as its rejection reason. Together with the separately captured ETL query result `finance_fact_rows = 0` for `etl_fact_orders`, the evidence supports the cross-pipeline rejection outcome for this order.

An accepted-order ETL fact-to-aggregate trace was subsequently captured for `ORD002401` (see Evidence E15). Exact original CSV row/line provenance and a separate ELT raw-to-mart trace remain open. Do not infer a mart contribution for quarantined `ORD000500`.


## Evidence E14 — ETL fact exclusion and schema inspection

The user-provided DuckDB query output:

```sql
SELECT COUNT(*) AS finance_fact_rows
FROM etl_fact_orders
WHERE order_id = 'ORD000500';
```

Observed:
```text
finance_fact_rows
0
```

The `DESCRIBE etl_fact_orders` output confirms these fields (with types shown in the user's output): `order_id VARCHAR`, `order_ts TIMESTAMP_NS`, `customer_id VARCHAR`, `store_id VARCHAR`, `product_id VARCHAR`, `quantity BIGINT`, `unit_price DOUBLE`, `discount_pct BIGINT`, `order_amount DOUBLE`, `payment_mode VARCHAR`, `order_status VARCHAR`, `channel VARCHAR`, `delivery_minutes BIGINT`, `source_file VARCHAR`, `source_order_status VARCHAR`, `source_payment_mode VARCHAR`, `order_date DATE`, `category VARCHAR`, `cost_price DOUBLE`, `store_name VARCHAR`, `zone VARCHAR`, `net_revenue DOUBLE`, `gross_margin DOUBLE`, `within_15_min BOOLEAN`, and `run_id VARCHAR`.

The `DESCRIBE etl_daily_zone_revenue` output confirms:
- `order_date DATE`
- `zone VARCHAR`
- `store_id VARCHAR`
- `store_name VARCHAR`
- `delivered_orders BIGINT`
- `net_revenue DOUBLE`

The accepted-order trace was subsequently captured for `ORD002401`: fact net revenue 475.95, matched daily/store aggregate with 12 delivered orders and net revenue 5,453.90 (Evidence E15).


## Evidence E15 — Final lineage, test, parse, and repository review (15 October 2026)

### Accepted-order ETL fact-to-aggregate trace

The user-provided DuckDB query joined `etl_fact_orders` to `etl_daily_zone_revenue` on `order_date` and `store_id` for `ORD002401` and returned:

| Field | Observed value |
|---|---|
| `order_id` | `ORD002401` |
| `order_date` | `2026-10-01` |
| `store_id` | `S14` |
| `store_name` | `NammaMart Vijayanagar` |
| `zone` | `West` |
| `fact_net_revenue` | `475.95` |
| `delivered_orders` | `12` |
| `aggregate_net_revenue` | `5453.90` |

This proves a matching ETL fact row joins to the corresponding daily/store aggregate. The aggregate contains 12 delivered orders; its total is not expected to equal the one order's revenue. The original CSV row/line and a separate ELT raw-to-mart trace were not provided.

### Final automated checks

Application tests, executed in `/usr/local/airflow`:

```text
pytest -q -p no:cacheprovider
32 passed in 2.15s
```

Astro DAG integrity, from `astro dev parse`:

```text
collected 4 items
.... [100%]
4 passed, 1 warning in 1.20s
✔ No errors detected in your Dags
```

The warning was `PytestCacheWarning` because pytest could not create a cache path under `/.pytest_cache` due to permissions. It did not fail the four DAG integrity tests.

### Git review

`git diff --check` reported trailing whitespace in three added lines:
- `EVIDENCE.md:263`
- `EVIDENCE.md:264`
- `SUBMISSION.md:661`

The status output showed modified tracked files `.gitignore`, `EVIDENCE.md`, `SUBMISSION.md`, `dags/nammamart_etl_dag.py`, `include/nammamart/etl.py`, `include/nammamart/reconciliation.py`, and `tests/test_nammamart_etl.py`. Untracked items included the new `include/nammamart/etl_warehouse.py` (302 lines), `tests/test_nammamart_etl_warehouse.py` (97 lines), backup versions of DAG/ELT/SQL files, and generated artifacts `include/output/backups/*.duckdb`, `include/output/nammamart_audit.duckdb`, and `include/output/quarantine_summary.csv`.

The `git check-ignore` output confirmed `include/output/nammamart/` and `include/output/etl_extracts/` are ignored. Top-level generated artifacts under `include/output/` are not all ignored. The command `git status --short -- include/data` produced no output, so no source data changes were reported. No files were staged, deleted, or pushed as part of this review.

### Final review status

- Verified: ETL/ELT rejected-order evidence for `ORD000500`; accepted-order ETL fact-to-aggregate trace for `ORD002401`; 32 application tests; four Astro DAG integrity tests; current Git whitespace findings and untracked-file inventory.
- Still open: full Part 0 profiling, remaining full business-query outputs, exact original source-row/line and ELT raw-to-mart lineage, stale-data/Clear-recovery/alert delivery experiments, complete controlled observability matrix on both DAGs, and final cleanup/classification of untracked artifacts.
- Do not claim the full assignment or all Part F experiments are complete until those remaining items have evidence or are explicitly accepted as limitations.
