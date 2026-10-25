# NammaMart Astro/Airflow Execution Runbook

This runbook explains how to start Astro/Airflow, validate the DAGs, run both pipelines from the command line, trigger them from the Airflow UI, and capture the runtime evidence required by the assignment.

## 1. Open the project

Open PowerShell at the repository root:

```powershell
Set-Location "C:\Users\M720s_5\Rudin\FDE\Github\IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model"
```

Confirm the important files exist:

```powershell
Test-Path .\dags\nammamart_etl_rudin.py
Test-Path .\dags\nammamart_elt_rudin.py
Test-Path .\scripts\trigger_nammamart.ps1
Test-Path .\include\data\nammamart\orders.csv
```

## 2. Start Astro/Airflow

Start the local Airflow deployment:

```powershell
astro dev start
```

Wait until the containers report healthy. Open the Airflow UI at:

```text
http://localhost:8080
```

Default local credentials are normally:

```text
Username: admin
Password: admin
```

If the containers were already running, use:

```powershell
astro dev ps
```

## 3. Check DAG import and schedule from the command line

Check that Airflow can import both DAG files:

```powershell
astro dev run dags list-import-errors
```

List the DAGs and confirm that both NammaMart DAGs were discovered:

```powershell
astro dev run dags list | Select-String "nammamart"
```

With the current Airflow 3 CLI, the output uses these columns:

```text
dag_id | fileloc | owners | is_paused | bundle_name | bundle_version
```

The expected NammaMart rows are similar to:

```text
nammamart_elt_rudin | /usr/local/airflow/dags/nammamart_elt_rudin.py | fde-cohort | True | dags-folder | None
nammamart_etl_rudin | /usr/local/airflow/dags/nammamart_etl_rudin.py | fde-cohort | True | dags-folder | None
```

Here, `None` is the local `bundle_version`; it is not the DAG schedule and is normal for DAGs loaded from `dags-folder`. `True` means that the DAG is currently paused. The `dags list` command does not display the schedule in this Airflow version. Check each schedule explicitly with:

```powershell
astro dev run dags details nammamart_etl_rudin
astro dev run dags details nammamart_elt_rudin
```

The details output should show `0 9 * * 1-6` with the `Asia/Kolkata` timezone. This means Monday through Saturday at 9:00 AM IST; Airflow displays the corresponding UTC time as 03:30 UTC.

## 4. Enable the DAGs in the Airflow UI

1. Open `http://localhost:8080`.
2. Find `nammamart_etl_rudin` and `nammamart_elt_rudin` in the DAG list.
3. Turn on the toggle for each DAG if it is paused.
4. Open each DAG and select **Schedule** or **Details**.
5. Capture a screenshot showing the DAG ID, `0 9 * * 1-6`, and the next run time.

Save the screenshot as:

```text
assignment/evidence/01_schedule.png
```

## 5. Run both DAGs from code/Astro

The repository includes a PowerShell helper:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\trigger_nammamart.ps1 -Pipeline both
```

To run only one pipeline:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\trigger_nammamart.ps1 -Pipeline etl
powershell -ExecutionPolicy Bypass -File .\scripts\trigger_nammamart.ps1 -Pipeline elt
```

The equivalent direct Astro commands are:

```powershell
astro dev run dags trigger nammamart_etl_rudin
astro dev run dags trigger nammamart_elt_rudin
```

The ETL and ELT DAGs use separate DuckDB files, so both can run without competing for the same database lock. Each DAG also has `max_active_runs=1`, preventing overlapping runs of that same DAG.

## 6. Run a single DAG for development testing

For a task-level development test using a fixed logical date:

```powershell
astro dev run dags test nammamart_etl_rudin 2026-09-30
astro dev run dags test nammamart_elt_rudin 2026-09-30
```

Use the normal `dags trigger` command when you need a persistent DAG run visible in the Airflow UI and recorded in `pipeline_runs`.

## 7. Monitor the runs in Airflow

For each DAG:

1. Open the DAG in Airflow.
2. Open **Grid** or **Graph** view.
3. Confirm the run finishes green.
4. Open the extract tasks and confirm the five ETL extracts or raw ELT loads run.
5. Open validation, quarantine, staging, load, reconciliation, mart, and audit task logs.
6. Capture a screenshot of the completed Graph view.

Save screenshots as:

```text
assignment/evidence/02_etl_graph_success.png
assignment/evidence/03_elt_graph_success.png
```

## 8. Capture quarantine evidence

The ETL pipeline writes failed rows to:

```text
include/quarantine/rudin/<file>_failed_<run_id>.csv
```

In the Airflow UI, open the ETL quarantine task log and record the output path. From PowerShell, list the files after a successful run:

```powershell
Get-ChildItem .\include\quarantine\rudin -File
```

Open one quarantine CSV and confirm it contains `failure_reason`. Capture the file or a table view as:

```text
assignment/evidence/04_quarantine_failure_reason.png
```

## 9. Inspect DuckDB runtime artifacts

The pipelines create separate warehouses:

```text
include/warehouse_rudin.duckdb
include/warehouse_rudin_elt.duckdb
```

Confirm they exist:

```powershell
Get-ChildItem .\include -Filter "warehouse_rudin*.duckdb"
```

The ETL tables include:

```text
etl_fact_orders
etl_dim_customer
etl_dim_product
etl_dim_store
etl_payment_reconciliation
etl_daily_zone_revenue
etl_category_margin
dq_results
pipeline_runs
```

The ELT tables include:

```text
raw_orders, raw_payments, raw_customers, raw_products, raw_stores
stg_orders, stg_payments, stg_customers, stg_products, stg_stores
mart_daily_store_kpis
mart_category_margin
mart_delivery_sla
mart_payment_reconciliation
mart_top_customers
dq_results
pipeline_runs
```

Use a DuckDB-capable SQL client in VS Code, or run a short Python query against each warehouse. The following verification queries are required:

```sql
SELECT *
FROM dq_results
ORDER BY checked_at DESC;

SELECT *
FROM pipeline_runs
ORDER BY end_time DESC;

SELECT *
FROM etl_payment_reconciliation
ORDER BY ABS(difference) DESC;

SELECT *
FROM mart_payment_reconciliation
ORDER BY ABS(difference) DESC;
```

Capture screenshots as:

```text
assignment/evidence/05_dq_results.png
assignment/evidence/06_pipeline_runs.png
assignment/evidence/07_payment_reconciliation.png
```

## 10. Check the incremental load

The ELT raw layer appends `orders_2026-10-01_increment.csv`. Staging merges by `order_id`.

Run the ELT DAG after the increment file is present, then verify:

```sql
SELECT COUNT(*) AS raw_increment_rows
FROM raw_orders
WHERE _source_file = 'orders_2026-10-01_increment.csv';

SELECT order_id, order_status
FROM stg_orders
WHERE order_id IN (
    'ORD000017', 'ORD000271', 'ORD000332', 'ORD000337', 'ORD000584',
    'ORD000708', 'ORD000822', 'ORD001025', 'ORD001067', 'ORD001990'
);

SELECT order_id, COUNT(*)
FROM stg_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```

Expected result: 160 raw increment rows, 10 corrected orders with their latest status, 150 new orders, and no duplicate `order_id` in `stg_orders`.

## 11. Troubleshoot failed or queued runs

### Check the run and task states

The DAG can be registered correctly and still fail during task execution. Check the latest run and then inspect the task states:

```powershell
astro dev run dags list-runs nammamart_etl_rudin -o json
astro dev run dags list-runs nammamart_elt_rudin -o json

astro dev run tasks states-for-dag-run nammamart_etl_rudin <run_id> -o json
astro dev run tasks states-for-dag-run nammamart_elt_rudin <run_id> -o json
```

`queued` means the run is waiting for the scheduler; `running` means task execution has started; `success` means the complete DAG, including `audit_run`, finished successfully. Open the first task shown as `failed` in the Airflow UI and read its log. Tasks marked `upstream_failed` are usually consequences of an earlier failure.

### ETL validation error involving `.str`

If the ETL `validate` task reports:

```text
Can only use .str accessor with string values!
```

The source CSV or JSON/XCom conversion inferred a value such as `phone` as numeric. The validation code converts the email and phone columns to pandas' nullable string dtype before applying regular expressions. Keep that conversion when adding new string-based quality checks; do not call `.str` directly on an inferred column.

### ELT DuckDB lock error

If an ELT raw-load task reports:

```text
Could not set lock on file ... warehouse_rudin_elt.duckdb
```

More than one task is trying to write the same DuckDB file. The raw-load tasks in [nammamart_elt_rudin.py](../dags/nammamart_elt_rudin.py) are intentionally chained one after another. If a new raw task is added, place it in that chain instead of making it an independent parallel branch. The ETL and ELT DAGs also use separate warehouse files.

### Airflow 3 audit-task error

If `audit_run` reports either `DagRun object has no attribute get_task_instances` or `Direct database access via the ORM is not allowed in Airflow 3.0`, do not query Airflow's metadata database from a task. The audit implementation uses an XCom success marker from its direct upstream reporting task, which is compatible with Airflow 3.

### Stale runs blocking a new run

Each DAG has `max_active_runs=1`. If an old run remains `running` after a crash or restart, a new run can remain queued. First inspect the old run in the UI. For a local development environment, restart Astro and clear only the stale running task instances:

```powershell
astro dev restart
astro dev run tasks clear nammamart_etl_rudin --only-running --yes
astro dev run tasks clear nammamart_elt_rudin --only-running --yes
```

Use these commands only for stale local runs. Clearing task instances changes their state and may cause them to be retried. Afterward, trigger one fresh run per DAG and verify that the final state is `success`.

### Import errors versus runtime errors

Use the following distinction when diagnosing a problem:

```powershell
astro dev run dags list-import-errors
```

`No data found` means no DAG import errors were found. It does not prove that a task will succeed at runtime. Runtime failures must be diagnosed from the failed task's log and the task-state command above. Warnings about Graphviz, deprecated Airflow imports, or the Astro managed secrets backend are not DAG import failures.

## 12. Stop Astro/Airflow

When finished, stop the local deployment:

```powershell
astro dev stop
```

Do not delete the DuckDB warehouses, quarantine outputs, or screenshots until the submission has been reviewed.

## Evidence checklist

- [ ] Import-error check is empty
- [ ] Both DAGs show a successful green run
- [ ] Schedule screenshot shows 9:00 AM IST
- [ ] Quarantine file contains `failure_reason`
- [ ] `dq_results` query result captured
- [ ] `pipeline_runs` query result captured
- [ ] Payment reconciliation query result captured
- [ ] Incremental run proves 10 updates and 150 inserts
- [ ] Screenshots are stored under `assignment/evidence/`
