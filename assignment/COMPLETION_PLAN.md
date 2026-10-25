# NammaMart Assignment Completion Plan

This document tracks what has been completed and what remains before submission. Work through the remaining steps in order.

## Current status

Profiling, both DAG implementations, DuckDB tables, quality-scorecard scaffolding, governance answers, and the recommendation are present. The main remaining work is verification, evidence collection, business-query results, and tightening a few ELT quality rules.

## Step 1 — Confirm source data and repository state

- [x] Copy the six source files into `include/data/nammamart/`.
- [x] Keep the original files unchanged.
- [ ] Decide which generated DuckDB files and quarantine outputs should be committed.
- [ ] Remove or ignore temporary run outputs that are not required as submission evidence.
- [ ] Confirm that no secrets or unintended unmasked customer data are present outside the permitted source-data directory.

Relevant locations: `assignment/nammamart_dataset/`, `include/data/nammamart/`, and `include/quarantine/rudin/`.

## Step 2 — Complete and verify Part 0 profiling

- [x] Profile all six files for row counts, nulls, duplicates, ranges, and categorical values.
- [x] Check referential integrity and business rules.
- [x] Create the row-level Data Issues Log.
- [x] Identify issues in every source file.
- [x] Write three client questions.
- [ ] Confirm the final issues log contains file, row key, column, problem, quality dimension, proposed rule, and severity.
- [ ] Use the profiling results consistently in the final submission.

Evidence: `assignment/profile/PROFILE_REPORT.md`, `assignment/profile/data_issues_log.csv`, and `assignment/profile/client_questions.csv`.

## Step 3 — Verify the ETL pipeline

Review `dags/nammamart_etl_rudin.py`.

- [x] Five extraction tasks, Python validation, quarantine, masking, joins, derived metrics, star-schema loading, reconciliation, reporting, and audit are implemented.
- [x] ETL loading is replace-style and idempotent for reporting tables.
- [ ] Run the DAG successfully from Airflow and save the run ID.
- [ ] Trigger it a second time and confirm fact-table counts do not change.
- [ ] Confirm no failed order appears in `etl_fact_orders` or ETL revenue totals.
- [ ] Confirm the payment reconciliation table contains every mismatch.

## Step 4 — Complete and verify the ELT pipeline

Review `dags/nammamart_elt_rudin.py` and `include/sql/`.

- [x] Raw tables retain source values with `_loaded_at`, `_source_file`, and `_run_id`.
- [x] Staging SQL casts, standardizes, deduplicates, masks PII, and records `dq_status`.
- [x] Incremental orders use a `MERGE`/upsert on `order_id`.
- [x] SQL mart tables, `dq_results`, and `pipeline_runs` are implemented.
- [ ] Add or verify ELT checks for required fields, numeric parsing, positive quantity, discount range, order-amount formula, unknown payment orders, payment mismatches, future dates, and delivered-order delivery time.
- [ ] Replace hard-coded freshness dates with a run-date or configurable reference date.
- [ ] Trigger the ELT DAG successfully.
- [ ] Trigger it again and verify raw history behavior, staging deduplication, and stable mart totals.
- [ ] Run the increment-file test and record exactly 150 inserts and 10 updates.
- [ ] Confirm there are no duplicate `order_id` values in `stg_orders`.

## Step 5 — Confirm scheduling and DAG parsing

- [x] Both DAGs use `0 9 * * 1-6`.
- [x] Both DAGs use the `Asia/Kolkata` timezone.
- [x] Both DAGs use `catchup=False`, retries, retry delay, and execution timeout.
- [ ] Run `astro dev run dags list-import-errors`.
- [ ] Confirm both DAGs are visible in Airflow.
- [ ] Capture the schedule and next-run time in the Airflow UI.
- [ ] Confirm the UI displays 03:30 UTC, equivalent to 09:00 IST.
- [ ] Record the offline-Docker behavior: with `catchup=False`, a missed local run must be triggered manually.

Reference: `dags/nammamart_schedule_notes.md`.

## Step 6 — Compare ETL and ELT outputs

Create and save a September comparison.

- [ ] Compare delivered order counts.
- [ ] Compare net revenue.
- [ ] Compare revenue by zone and store.
- [ ] Compare category revenue and gross margin.
- [ ] Compare delivery SLA metrics.
- [ ] Compare payment reconciliation totals.
- [ ] Investigate and document every difference.
- [ ] Add the final comparison result to `SUBMISSION.md`.

The final report must explicitly state whether ETL and ELT totals match and why.

## Step 7 — Produce the six business-question outputs

For both pipelines, save the SQL query and result for:

- [ ] Daily net revenue by zone and store.
- [ ] Revenue and gross margin by product category.
- [ ] Average delivery time and percentage within 15 minutes by store.
- [ ] Revenue by payment mode and unreconciled amount.
- [ ] Top 10 customers by spend and loyalty tier, without raw email or phone.
- [ ] Rejected rows by file and failure reason.

Add the queries and representative results to `SUBMISSION.md` or a linked results document.

## Step 8 — Run the ten observability experiments

For each experiment, record Expected → Observed → Evidence location → Change proposed. Restore the original source file after each test.

- [ ] 1. Happy path.
- [ ] 2. Re-run/idempotency.
- [ ] 3. Incremental load.
- [ ] 4. Missing source file.
- [ ] 5. Five new bad rows.
- [ ] 6. Schema change.
- [ ] 7. Volume anomaly.
- [ ] 8. Distribution drift.
- [ ] 9. Freshness failure.
- [ ] 10. Recovery after clearing a failed task.

Replace the template-style observations in `SUBMISSION.md` with actual task states, counts, log lines, and table results.

## Step 9 — Capture required screenshots

- [ ] ETL DAG Graph view with a green run.
- [ ] ELT DAG Graph view with a green run.
- [ ] Schedule showing 09:00 IST / 03:30 UTC.
- [ ] A quarantine file containing `failure_reason`.
- [ ] `dq_results` query output.
- [ ] `pipeline_runs` query output.
- [ ] Payment reconciliation output.

Store screenshots in a clearly named submission folder and reference them from `SUBMISSION.md`.

## Step 10 — Finish the written submission

Already present:

- [x] Governance answers for Part E.
- [x] ETL-versus-ELT recommendation for Part G.

Still required:

- [ ] Actual Part C schedule answers and evidence.
- [ ] ETL and ELT validation results.
- [ ] ETL-versus-ELT totals comparison.
- [ ] Six business-question SQL queries and results.
- [ ] Actual observability experiment results.
- [ ] Screenshot links.
- [ ] Final risks and mitigations tied to observed behavior.

## Step 11 — Final validation

```powershell
astro dev run dags list-import-errors
astro dev run dags list
git status --short
```

Before submission, confirm:

- [ ] Both DAGs parse with no import errors and have successful Airflow runs.
- [ ] ETL fact tables contain only validated rows.
- [ ] ELT raw tables retain all source rows.
- [ ] Quarantine rows have reasons and are not silently dropped.
- [ ] Re-running is idempotent for reporting tables.
- [ ] Incremental corrections update existing orders without duplicates.
- [ ] PII is masked in every reporting table.
- [ ] `dq_results` and `pipeline_runs` contain run evidence.
- [ ] All six business questions are answered from both pipelines.
- [ ] All ten experiments are documented with observed results.
- [ ] Required screenshots are included.
- [ ] No secrets are committed.

## Definition of done

The assignment is ready to submit only when the code works in Airflow and the repository contains evidence for every acceptance criterion, not merely planned or expected outcomes.
