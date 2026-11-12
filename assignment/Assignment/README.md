# NammaMart Data Pipeline: ETL and ELT with Apache Airflow

A data engineering assignment that implements two data-processing
approaches for NammaMart: a traditional **ETL pipeline** and a
warehouse-oriented **ELT pipeline**. Both workflows are orchestrated
with Apache Airflow and use DuckDB as the local analytical warehouse.

> **Project status:** Both DAGs have run successfully in the current
> development environment. The latest warehouse inspection reported the
> ETL fact table passing its checks. The ELT staging and mart row-count
> checks also passed, while the inspection script flagged duplicate IDs
> in the raw customer and product tables. See [Validation results and
> known considerations](#validation-results-and-known-considerations).

------------------------------------------------------------------------

## Table of contents

-   [Project overview](#project-overview)
-   [Objectives](#objectives)
-   [Architecture](#architecture)
-   [ETL versus ELT](#etl-versus-elt)
-   [Project structure](#project-structure)
-   [Technology stack](#technology-stack)
-   [Configuration](#configuration)
-   [Prerequisites](#prerequisites)
-   [Setup and run](#setup-and-run)
-   [Pipeline workflows](#pipeline-workflows)
-   [Warehouse tables](#warehouse-tables)
-   [Data quality and auditability](#data-quality-and-auditability)
-   [Validation results and known
    considerations](#validation-results-and-known-considerations)
-   [Troubleshooting](#troubleshooting)
-   [Useful commands](#useful-commands)
-   [Future improvements](#future-improvements)

## Project overview

The project demonstrates how raw retail data can be processed into
analytics-ready datasets using two pipeline patterns:

1.  **ETL (Extract, Transform, Load):** extract source data, validate
    and transform it, handle invalid records, and load the resulting
    fact dataset into a DuckDB warehouse.
2.  **ELT (Extract, Load, Transform):** ingest source data into raw
    warehouse tables first, transform and validate it inside DuckDB,
    process an incremental orders file, and build analytical marts.

Apache Airflow manages task dependencies, execution state, retries, and
scheduling. DuckDB provides a lightweight analytical database that can
be inspected locally without requiring a separate database server.

## Objectives

-   Build and orchestrate repeatable data pipelines with Apache Airflow.
-   Demonstrate both ETL and ELT processing patterns.
-   Use DuckDB for local analytical storage and SQL-based
    transformations.
-   Apply data-quality checks and separate staging data from raw source
    data.
-   Process incremental order records in the ELT workflow.
-   Produce business-oriented marts for store performance, category
    margin, delivery service levels, payment reconciliation, customers,
    and payment modes.
-   Record quality and pipeline-run information in the ELT workflow.
-   Validate expected table counts and key uniqueness using a warehouse
    inspection script.

## Architecture

### High-level flow

``` text
                       NammaMart source files
                                 |
                 +---------------+---------------+
                 |                               |
                 v                               v
            ETL pipeline                    ELT pipeline
                 |                               |
      Extract source datasets          Load source datasets
                 |                               |
      Validate and transform           Raw DuckDB tables
                 |                               |
      Handle invalid records           SQL staging and DQ
                 |                               |
      Load fact warehouse              Incremental merge
                 |                               |
          etl_fact_orders               Analytics marts
                 |                               |
                 v                               v
          warehouse.duckdb             warehouse_elt.duckdb
```

The ETL and ELT workflows use separate DuckDB files so that their
outputs can be inspected independently.

## ETL versus ELT

  ---------------------------------------------------------------------------------------------------------
  Aspect                  ETL                                    ELT
  ----------------------- -------------------------------------- ------------------------------------------
  Processing order        Extract → Transform → Load             Extract → Load → Transform

  Transformation location Before loading the final warehouse     Inside DuckDB after raw ingestion
                          table                                  

  Main warehouse file     `include/warehouse/warehouse.duckdb`   `include/warehouse/warehouse_elt.duckdb`

  Main validated output   `etl_fact_orders`                      Staging tables and analytics marts

  Data-quality handling   Validation and invalid-record handling Raw, staging, quality-result, and mart
                          in the ETL workflow                    layers

  Incremental orders      Not confirmed as part of the current   Increment file is loaded and merged into
                          ETL output                             orders

  Audit tables            Not present in the latest inspected    `dq_results` and `pipeline_runs` are
                          ETL database                           present
  ---------------------------------------------------------------------------------------------------------

## Project structure

The following is the logical layout used by the project. Some
source-data filenames may vary depending on the files included in your
local assignment directory.

``` text
Assignment/
├── dags/
│   ├── ETL.py
│   └── ELT.py
├── config/
│   └── global_config.yaml
├── include/
│   ├── data/                         # Source CSV files, if configured here
│   ├── quarantine/                   # Invalid/rejected data, if written by the pipeline
│   ├── scripts/
│   │   ├── etl/                      # ETL SQL scripts, where externalized
│   │   └── elt/                      # ELT SQL scripts
│   └── warehouse/
│       ├── warehouse.duckdb          # ETL database (created at runtime)
│       └── warehouse_elt.duckdb      # ELT database (created at runtime)
├── tests/
│   └── scripts/
│       └── inspect_warehouse.py      # Warehouse and row-count validation
├── requirements.txt
├── Dockerfile                        # If supplied by the assignment
└── README.md
```

**Important:** The paths above reflect the project configuration and
discussed layout. Confirm the actual locations of your CSV files and SQL
scripts against `config/global_config.yaml` and the path constants in
the DAG files. Do not create duplicate folders merely to match this
illustration.

## Technology stack

-   **Python** --- pipeline implementation and data handling.
-   **Apache Airflow** --- DAG scheduling, task orchestration,
    dependencies, retries, and execution monitoring.
-   **Astro CLI / Astro Runtime** --- local Airflow development
    environment.
-   **Docker** --- containerized Airflow runtime.
-   **DuckDB** --- local analytical warehouse and SQL transformations.
-   **SQL** --- staging transformations, incremental processing,
    data-quality checks, and marts.
-   **YAML** --- shared pipeline configuration.
-   **CSV** --- source datasets and incremental input.

The exact package versions should be taken from the repository's
`requirements.txt` and Astro Runtime configuration. When working with
the Astro Runtime used during development, keep PyYAML compatible with
the runtime; the earlier environment required `PyYAML>=6.0.3`.

## Configuration

Shared settings are stored in `config/global_config.yaml`. The
configuration discussed during development was:

``` yaml
DATA_DIR: include/data
WAREHOUSE_DB: include/warehouse/warehouse.duckdb
WAREHOUSE_DB_ELT: include/warehouse/warehouse_elt.duckdb
QUARANTINE_DIR: include/quarantine
RETRIES: 2
RETRY_DELAY: 5
SCHEDULE: "0 9 * * 1-6"
EXECUTION_TIMEOUT: 30
TIMEZONE: Asia/Kolkata
START_DATE: "2026-01-01"
catchup: false
```

### Configuration reference

  -----------------------------------------------------------------------
  Setting                             Purpose
  ----------------------------------- -----------------------------------
  `DATA_DIR`                          Directory containing input data
                                      files. Ensure this matches the
                                      actual source-data location.

  `WAREHOUSE_DB`                      DuckDB file used by the ETL
                                      workflow.

  `WAREHOUSE_DB_ELT`                  DuckDB file used by the ELT
                                      workflow.

  `QUARANTINE_DIR`                    Destination for invalid or
                                      quarantined records, if enabled by
                                      the ETL implementation.

  `RETRIES`                           Number of task retries configured
                                      by the DAG.

  `RETRY_DELAY`                       Retry delay value; confirm whether
                                      the DAG interprets this value as
                                      seconds or another unit.

  `SCHEDULE`                          Cron schedule. `0 9 * * 1-6` means
                                      09:00 from Monday through Saturday.

  `EXECUTION_TIMEOUT`                 Task execution timeout value;
                                      confirm the unit used by the DAG
                                      code.

  `TIMEZONE`                          Time zone used for
                                      schedule/start-date interpretation.

  `START_DATE`                        Initial logical start date for the
                                      DAG.

  `catchup`                           Whether Airflow should schedule
                                      historical runs. `false` disables
                                      catch-up runs.
  -----------------------------------------------------------------------

### Configuration notes

-   Resolve relative paths consistently from the project root or from a
    known DAG/include directory. Relative paths interpreted from
    different working directories can point to different locations.
-   YAML may parse a date as a date object rather than a string. If the
    DAG expects a string, convert it explicitly before parsing.
-   If changing a database path, update the relevant configuration and
    verify the path used by the DAG. The inspection script may use its
    own configured or hard-coded path.
-   Avoid running multiple writers against the same DuckDB file
    concurrently. DuckDB file locking can cause tasks to fail when
    separate worker processes try to write at the same time.

## Prerequisites

Install or prepare:

1.  Docker Desktop, running on your machine.
2.  Astro CLI.
3.  Python for running local validation scripts.
4.  The project repository or assignment folder, including DAGs,
    configuration, source data, and SQL scripts.

Check that Docker Desktop is running before starting the Astro
environment.

## Setup and run

Run the commands below from the project root---the directory containing
the Astro project files and the `dags/`, `config/`, `include/`, and
`tests/` directories.

### 1. Install or verify dependencies

Review `requirements.txt`. If the project uses an Astro Runtime, avoid
pinning a package to a version that conflicts with the runtime's
requirements. The environment used during development required a PyYAML
version of at least `6.0.3`.

### 2. Start the local Airflow environment

``` bash
astro dev start
```

Open the Airflow UI at the local address printed by the Astro CLI. Sign
in using the credentials configured for your local environment.

If the environment is already running and needs a restart:

``` bash
astro dev restart
```

A restart does not restore deleted DAG files or database files. Ensure
`dags/ETL.py` and `dags/ELT.py` still exist before expecting both DAGs
to appear.

### 3. Check DAG import errors

``` bash
astro dev bash
airflow dags list-import-errors
airflow dags list
```

If `astro dev bash` opens a shell inside the Airflow container, run the
Airflow commands there. Exit the container shell when finished:

``` bash
exit
```

Both DAGs should be listed without import errors. The ELT DAG may be
paused by default; unpause it in the Airflow UI if required.

### 4. Run the ETL pipeline

In the Airflow UI:

1.  Find the ETL DAG (`nammamart_etl`, if that is the DAG ID in your
    current code).
2.  Unpause it if necessary.
3.  Trigger a run.
4.  Open the run's Graph or Grid view.
5.  Check that the tasks complete successfully.

The current inspected ETL output contains `etl_fact_orders` with 2,342
rows.

### 5. Run the ELT pipeline

In the Airflow UI:

1.  Find the ELT DAG (`nammamart_elt`, if that is the DAG ID in your
    current code).
2.  Unpause it if necessary.
3.  Trigger a run.
4.  Review task logs if any task fails.
5.  Confirm that raw, staging, quality, and mart tables are created in
    the ELT database.

Run the ELT pipeline after verifying that its input files and SQL
scripts are available at the paths expected by `ELT.py`.

### 6. Inspect the warehouse outputs

From the project root, activate the Python environment that contains
DuckDB and run:

``` bash
python tests/scripts/inspect_warehouse.py
```

The script checks the ETL and ELT database paths, table counts, expected
outputs, key nulls/duplicates, and ELT audit tables. It is a
structural/basic validation script, not a replacement for every
data-quality rule in the DAG.

## Pipeline workflows

### ETL workflow

The ETL DAG is intended to demonstrate a traditional
extract-transform-load process. The task sequence observed during
development included:

-   Extract source datasets such as stores, orders, customers, payments,
    and products.
-   Validate source records.
-   Handle or quarantine invalid records.
-   Transform validated data.
-   Load the fact output into the ETL DuckDB database.

The current inspected ETL database contains:

  Table                 Latest inspected row count
  ------------------- ----------------------------
  `etl_fact_orders`                          2,342

The latest inspection reported no null or duplicate `order_id` values in
this table. The same inspection warned that `dq_results` and
`pipeline_runs` were not present in the ETL database. Do not assume ETL
audit tables exist unless they are explicitly implemented in the current
code.

### ELT workflow

The ELT DAG loads raw source data into DuckDB and then runs SQL
transformations. The observed workflow includes:

1.  Ingest raw source tables.
2.  Load the incremental orders file when present.
3.  Profile/check raw data.
4.  Create and execute staging transformations.
5.  Type, rank, and merge incremental order records.
6.  Mask customer PII in the relevant output.
7.  Build analytical marts.
8.  Record a final pipeline audit.

The precise task dependencies and SQL filenames are defined by the
current `dags/ELT.py` and SQL files. Use those files as the source of
truth if the implementation changes.

## Warehouse tables

The latest successful ELT inspection found 22 tables in
`warehouse_elt.duckdb`.

### Raw layer

  Table                      Latest inspected row count
  ------------------------ ----------------------------
  `raw_orders`                                    2,550
  `raw_orders_increment`                            160
  `raw_customers`                                   405
  `raw_payments`                                  2,228
  `raw_products`                                    152
  `raw_stores`                                       15

The raw layer preserves ingested records and may therefore contain
duplicate business keys if they exist in the source files. Whether raw
duplicates are acceptable depends on the assignment's requirements and
the intended raw-layer contract.

### Staging layer

  Table               Latest inspected row count
  ----------------- ----------------------------
  `stg_orders`                             2,550
  `stg_customers`                            400
  `stg_payments`                           2,228
  `stg_products`                             150
  `stg_stores`                                15

Staging is the cleaned and typed layer used by downstream marts. The
current inspection found no duplicate IDs in `stg_orders`,
`stg_customers`, `stg_products`, or `stg_stores`.

### Incremental processing tables

  Table                  Latest inspected row count
  -------------------- ----------------------------
  `increment_typed`                             160
  `increment_ranked`                            160

The ELT workflow uses these tables to prepare the incremental order
records before merging them into the main order dataset. The exact
deduplication and merge behavior should be verified in the current SQL
files.

### Data-quality and audit tables

  Table               Latest inspected row count
  ----------------- ----------------------------
  `dq_results`                                20
  `pipeline_runs`                              2

These tables provide a place to record quality outcomes and pipeline-run
audit information. Their row counts reflect the most recent inspection,
not a guaranteed count for every future run.

### Analytics marts

  -----------------------------------------------------------------------------------
  Table                              Latest inspected row count Purpose
  -------------------------------- ---------------------------- ---------------------
  `mart_daily_store_kpis`                                   474 Daily store-level
                                                                performance metrics

  `mart_category_margin`                                      7 Category-level
                                                                revenue/cost/margin
                                                                metrics

  `mart_delivery_sla`                                        16 Delivery
                                                                service-level metrics

  `mart_payment_reconciliation`                           2,396 Order/payment
                                                                reconciliation output

  `mart_top_customers`                                       10 Top-customer output

  `mart_revenue_by_payment_mode`                              4 Revenue grouped by
                                                                payment mode

  `mart_rejected_rows`                                        9 Rejected or invalid
                                                                records represented
                                                                by the mart
  -----------------------------------------------------------------------------------

These descriptions are based on the table names and project workflow.
Refer to each SQL file for the exact column definitions, business logic,
filters, and metric formulas.

## Data quality and auditability

The warehouse inspection script performs basic checks such as:

-   Whether the expected database file exists.
-   Whether expected tables exist.
-   Whether row counts match configured expectations.
-   Whether key columns contain nulls.
-   Whether key columns contain duplicate values.
-   Whether the ELT audit tables exist.

### Interpreting the latest checks

-   `etl_fact_orders.order_id`: no nulls and no duplicate key values.
-   `stg_orders.order_id`: no nulls and no duplicate key values.
-   `stg_customers.customer_id`: no nulls and no duplicate key values.
-   `stg_products.product_id`: no nulls and no duplicate key values.
-   `stg_stores.store_id`: no nulls and no duplicate key values.
-   `raw_customers.customer_id`: 5 duplicate key values flagged by the
    inspection script.
-   `raw_products.product_id`: 2 duplicate key values flagged by the
    inspection script.

Raw-layer duplicate IDs do not automatically mean that ingestion failed.
However, if the assignment requires unique keys in every raw table,
these findings must be addressed or explicitly documented. For customers
and products, use a defined business rule to choose the correct record;
do not arbitrarily delete records without understanding their meaning.

The inspection script's expected row counts are useful regression
checks, but they are tied to the current sample data. If the input
datasets change, update expected counts deliberately and keep the
quality rules separate from hard-coded row-count assertions where
possible.

## Validation results and known considerations

The most recent `python tests/scripts/inspect_warehouse.py` output
reported:

  Validation                            Result
  ------------------------------------- ---------------------------------
  ETL database exists                   PASS
  ETL `etl_fact_orders` row count       PASS --- 2,342
  ETL `order_id` null/duplicate check   PASS
  ELT expected table row counts         PASS
  ELT staging key checks                PASS
  ELT `dq_results` exists               PASS --- 20 rows
  ELT `pipeline_runs` exists            PASS --- 2 rows
  ELT raw customer key uniqueness       FAIL --- 5 duplicate key values
  ELT raw product key uniqueness        FAIL --- 2 duplicate key values

**Overall interpretation:** Both pipeline runs completed successfully,
and the ETL inspection passed. The ELT inspection still reports two
raw-layer uniqueness failures. A successful Airflow DAG run means the
tasks completed; it does not by itself guarantee that every independent
inspection rule passes.

### Other implementation considerations

-   **DuckDB locking:** Avoid overlapping DAG runs or parallel write
    tasks against the same DuckDB file. Make sure every database
    connection is closed even when a task raises an exception,
    preferably using `try/finally`.
-   **Incremental idempotency:** Re-running an incremental load should
    not unintentionally duplicate business records. Verify the merge key
    and ranking/tie-breaking logic in the current SQL.
-   **PII handling:** Review the masking task and avoid exposing
    sensitive customer information in logs, reports, or committed sample
    outputs.
-   **Paths:** Ensure the DAG code, YAML configuration, and inspection
    script point to the same database and data locations.
-   **Audit consistency:** Confirm that the final audit reflects the
    actual outcome of the run, including failures and data-quality
    results.

## Troubleshooting

### DAGs do not appear in the Airflow UI

Run:

``` bash
astro dev bash
airflow dags list-import-errors
airflow dags list
```

Check that `dags/ETL.py` and `dags/ELT.py` exist, YAML values parse
correctly, imported packages are installed, and SQL/configuration paths
resolve inside the container.

### A database file is missing

-   Confirm that the relevant DAG actually ran.
-   Check the configured path in `config/global_config.yaml` and the
    path construction in the DAG.
-   Check whether the database was created in a different directory.
-   Remember that deleting a `.duckdb` file removes its local database
    contents; rerun the pipeline to rebuild it.

### DuckDB reports a conflicting file lock

-   Check whether another DAG run is still active.
-   Avoid concurrent write operations to the same database file.
-   Ensure connections close in both success and exception paths.
-   If necessary, stop overlapping runs, then retry after confirming
    that no worker still holds the database open.

### SQL task fails

-   Read the failed task log in the Airflow UI.
-   Confirm that the referenced `.sql` file exists at the path used by
    the DAG.
-   Check table and column names against the raw/staging schema.
-   For set operations such as `UNION`, ensure that the two sides have
    compatible columns and types. If schemas differ, use an intentional
    column mapping rather than relying on positional column order.

### Inspection reports a row-count mismatch

A mismatch may indicate changed source data, repeated ingestion, a
changed incremental file, or an incorrect expected count. Compare the
source files, raw tables, staging logic, and inspection assertions
before changing the expected result.

## Useful commands

Run these from the project root unless stated otherwise.

``` bash
# Start the local Airflow environment
astro dev start

# Restart the local environment
astro dev restart

# Open a shell in the Airflow container
astro dev bash

# Check DAG import errors (inside the Airflow container)
airflow dags list-import-errors

# List registered DAGs (inside the Airflow container)
airflow dags list

# Validate the warehouse files and expected outputs (local Python environment)
python tests/scripts/inspect_warehouse.py
```

## Future improvements

Potential improvements, depending on the assignment requirements:

-   Add automated unit tests for transformation and validation
    functions.
-   Add explicit source-to-target schema documentation and data
    dictionaries.
-   Make incremental loads idempotent and test reruns against duplicate
    input records.
-   Record failed runs and data-quality summaries consistently in audit
    tables.
-   Parameterize expected counts and key rules rather than relying
    solely on fixed sample counts.
-   Add stronger checks for referential integrity between orders,
    customers, products, stores, and payments.
-   Add CI checks for DAG import errors, SQL syntax, and pipeline
    regression tests.
-   Improve raw duplicate handling through documented business rules
    rather than arbitrary deduplication.
-   Document exact SQL formulas and mart column definitions from the SQL
    source files.

------------------------------------------------------------------------

## Maintainer note

This README documents the project layout, workflow, configuration, and
latest observed validation output from the development conversation. The
actual code in `dags/`, `config/`, and `include/scripts/` remains the
source of truth if any paths, task names, SQL logic, or row counts
change.
