# Part F - Observability experiment results

Generated: 2026-10-20 13:39 - produced by `scripts/run_observability_experiments.py`.

Every experiment ran against **both** pipelines on a throw-away copy of the dataset and a throw-away DuckDB file, so the original files were never modified.

## Experiment 1: Happy path

- **Expected:** Both DAGs succeed; every file is counted; rejected rows are quarantined with a reason.
- **Observed:** ETL 0.195s loaded 2310 fact rows and quarantined 152; ELT 0.236s loaded 2310 PASS rows. Revenue is identical in both marts.
- **Where I saw it:** pipeline_runs table, dq_results table, task logs, quarantine CSV files
- **What I would change:** Publish the per-file counts as an Airflow metric so a drop is visible without opening the table.

## Experiment 2: Re-run (idempotency)

- **Expected:** Fact and mart row counts stay the same; the ELT raw layer grows, which is correct.
- **Observed:** ETL fact stayed at 2310.0 rows (0 inserted, 2310 updated); ELT PASS rows stayed at 2310.0 and the MERGE reported 0 inserts; raw_orders grew from 2412.0 to 4824.0 rows by design (append-only audit log).
- **Where I saw it:** warehouse row counts; MERGE log line; pipeline_runs has one row per run
- **What I would change:** Add an assertion task that fails the DAG if the fact count changes on a pure re-run.

## Experiment 3: Incremental load

- **Expected:** 150 new orders inserted, 10 existing orders updated to RETURNED, no duplicates.
- **Observed:** ELT MERGE reported 150 inserted and 10 updated; ETL upsert inserted 150 and updated 2310; fact grew from 2310 to 2460 rows with 2460 distinct order_ids; all 10 corrected orders now show ['RETURNED'] and their revenue became 0.
- **Where I saw it:** MERGE log line, etl_fact_orders upsert log, stg_orders dq_status = SUPERSEDED rows
- **What I would change:** Keep the superseded version as an SCD Type 2 row in the ETL fact as well, so finance can still see the September figure that was reported at the time.

## Experiment 4: Missing source file

- **Expected:** The task fails immediately with a clear error; downstream tasks do not run; the run audit records the failure.
- **Observed:** ETL failed in read_source_csv() with FileNotFoundError; ELT failed in load_raw() with FileNotFoundError. Both wrote a FAILED row to pipeline_runs. In Airflow the task would retry twice at 2-minute intervals (default_args) and then go to 'failed', leaving downstream tasks 'upstream_failed'.
- **Where I saw it:** task log, pipeline_runs.status = FAILED, Airflow Grid view colours
- **What I would change:** Add a lightweight 'all five files present' sensor task before the extracts so the failure is reported once, not five times.

## Experiment 5: New bad data

- **Expected:** All five planted rows are quarantined with the right rule and none reaches the fact table.
- **Observed:** All 5 rows were caught by both pipelines with the expected rule (RULE-006 null customer, RULE-009 negative quantity, RULE-008 unknown store, RULE-012 status SHIPPED, RULE-005 2027 timestamp) and none appears in etl_fact_orders; dq_results recorded every one.
- **Where I saw it:** quarantine CSV, etl_quarantine table, stg_orders.failure_reason, dq_results
- **What I would change:** Add a 'planted row' smoke test to CI so the rules keep working after a refactor.

## Experiment 6: Schema change (order_amount renamed to total)

- **Expected:** Both pipelines stop with a clear message naming the missing column; nothing is loaded.
- **Observed:** ETL failed in assert_schema() with SchemaError: "orders: missing expected column(s) ['order_amount']. Columns received: ['channel', 'customer_id', 'delivery_minutes', 'discount_pct', 'order_id', 'order_status', 'order_ts', 'payment_mode', 'product_id', 'quantity', 'store_id', 'total', 'unit_price']". ELT failed in run_sql_file() with BinderException: "Binder Error: Referenced column "order_amount" not found in FROM clause!". Neither loaded a single row, so the failure is loud, not silent.
- **Where I saw it:** task log and the FAILED row in pipeline_runs
- **What I would change:** Publish the expected column list as a contract document and notify the source team automatically when it changes.

## Experiment 7: Volume anomaly (only 50 orders)

- **Expected:** A volume check warns that the file lost more than 90% of its rows.
- **Observed:** RULE-034 fired in both pipelines with status WARN: expected~2412 rows, received 50 rows. The run still succeeded, which is intended - a volume drop is a warning for a human, not a reason to withhold the 9 AM report.
- **Where I saw it:** dq_results rows where rule_id = 'RULE-034'; the WARNING line in the task log
- **What I would change:** Promote the volume check to Critical once the client confirms a minimum daily volume, and send the warning to Slack rather than only to the table.

## Experiment 8: Distribution drift (unit_price x 100 for store S01)

- **Expected:** A price explosion in one store should be noticed before it reaches the mart.
- **Observed:** Variant A (unit_price only, as instructed): RULE-011 'order_amount must match the formula' failed for 176 rows in both pipelines, so every affected order was quarantined and store S01 kept only 0 rows in the fact table. Variant B (unit_price and order_amount both multiplied, so the arithmetic still agrees): no Critical rule fired, and S01 revenue moved from 76,552.35 to 7,655,235.00 (100x) with the data loading cleanly.
- **Where I saw it:** dq_results (RULE-011), mart_daily_store_kpis revenue per store
- **What I would change:** Add a distribution check that compares today's average order value per store with the trailing 7-day average and warns on a move larger than, say, 3 standard deviations. Rule-based checks catch broken arithmetic, not believable-but-wrong values.

## Experiment 9: Freshness (newest order 30 days old)

- **Expected:** The timeliness check warns that the data is stale.
- **Observed:** RULE-035 returned status WARN in both pipelines. ETL detail: "latest order_ts = 2026-10-11 00:07, 9.6 day(s) old". ELT detail: "latest order_ts = 2026-10-11 00:07, 9.6 day(s) old".
- **Where I saw it:** dq_results rows where rule_id = 'RULE-035'
- **What I would change:** Fail the DAG (not just warn) when the newest order is more than 2 days old, because a stale 9 AM report is worse than no report.

## Experiment 10: Recovery after fixing the cause

- **Expected:** After the cause is fixed, re-running produces the correct numbers with no duplicates.
- **Observed:** With stores.csv missing both pipelines failed (FileNotFoundError) and wrote FAILED rows to pipeline_runs. After restoring the file, both runs succeeded and loaded 2310 ETL fact rows / 2310 ELT PASS rows - the same numbers as a clean run, because the loads are idempotent. In the Airflow UI, pressing Clear on the failed task re-runs only that task and everything downstream of it; the already successful extract tasks are not repeated.
- **Where I saw it:** pipeline_runs (FAILED then SUCCESS), warehouse row counts; Airflow Grid view for Clear
- **What I would change:** Keep the quarantine and audit tables keyed by run_id so a partially failed day can be replayed without deleting yesterday's evidence.
