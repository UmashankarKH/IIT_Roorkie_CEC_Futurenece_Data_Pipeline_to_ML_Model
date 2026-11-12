# NammaMart ETL & ELT Data Pipeline

**IIT Roorkee · Forward Deployed AI Engineering · Module 2**  
**Author:** Surbhi Soni  
**Submission branch:** `namamart-assignment-work`

This project compares two Airflow pipelines for the NammaMart quick-commerce dataset:

- **ETL** validates and transforms source records in Python before loading a DuckDB star schema; rejected order records are written to quarantine with a `failure_reason`.
- **ELT** preserves source data in raw DuckDB tables, performs SQL transformations in staging, and builds reporting marts.
- Both pipelines include quality scorecards, run-audit records, source-volume/freshness/distribution checks, and payment reconciliation.

## Start here

1. Read the [assignment-aligned submission](SUBMISSION.md).
2. Review the [row-keyed initial data issues log](include/reports/data_issues_initial_profile.csv).
3. Review the [six source CSV files](include/data/nammamart/).
4. Review the [screenshots and run evidence](screenshots/).

## Source data

The six trainer-provided CSVs are stored in `include/data/nammamart/`:

- `orders.csv`
- `payments.csv`
- `customers.csv`
- `products.csv`
- `stores.csv`
- `orders_2026-10-01_increment.csv`

Treat these as immutable inputs. Use temporary copies for fault-injection experiments and restore the originals afterward. The source files are part of the assignment dataset; do not move unmasked source customer data into other repository paths.

## DAGs and schedule

| DAG ID | Purpose | Schedule |
|---|---|---|
| `nammamart_etl_surbhi` | Validate, quarantine, mask PII, load facts/dimensions, reconcile payments | `0 9 * * 1-6` |
| `nammamart_elt_surbhi` | Preserve raw inputs, transform with SQL, upsert increment versions, build marts | `0 9 * * 1-6` |

The cron schedule is timezone-aware using `Asia/Kolkata` (09:00 IST = 03:30 UTC), with `catchup=False`, retries, retry delay, and an execution timeout. A local scheduler cannot run while the host/Docker is off.

## Project layout

- `dags/nammamart_etl_surbhi.py` — ETL Airflow DAG
- `dags/nammamart_elt_surbhi.py` — ELT Airflow DAG
- `include/nammamart_pipeline.py` — shared validation, masking, loading, scorecard, observability, and audit logic
- `include/sql/elt_staging_dimensions.sql` — dimension staging
- `include/sql/elt_staging_orders.sql` — order staging and quality rules
- `include/sql/elt_marts.sql` — reporting marts
- `include/sql/business_questions.sql` — paired ETL/ELT queries for the six business questions
- `include/reports/data_issues_initial_profile.csv` — issue log from the initial source profile
- `screenshots/` — Airflow graph, schedule, quarantine, scorecard, audit, and reconciliation evidence
- `SUBMISSION.md` — profiling, schedule answers, governance, observability, business questions, recommendation, and evidence crosswalk

The live DuckDB warehouse and generated run-specific reports are local runtime artifacts and are not substitutes for committed code or screenshots. See `SUBMISSION.md` for which experiment results are directly evidenced and which controlled runs were not separately recorded.

## Local execution

Use the course-provided Astro CLI/Docker setup. From the repository root:

```bash
astro dev start
astro dev run dags list-import-errors
```

Then open the local Airflow UI at `http://localhost:8080`, confirm both DAGs parse, and trigger them only when needed. Query the local DuckDB tables using the SQL in `include/sql/business_questions.sql`. Do not commit the live warehouse database, credentials, webhook URLs, or local secrets.

## Evidence

- [Submission document](SUBMISSION.md)
- [ETL/ELT screenshots](screenshots/)
- [Business-question SQL](include/sql/business_questions.sql)
- [Initial profiling issue log](include/reports/data_issues_initial_profile.csv)
