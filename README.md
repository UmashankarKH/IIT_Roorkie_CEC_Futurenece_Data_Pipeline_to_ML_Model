# NammaMart Quick Commerce - ETL and ELT pipelines

FDAIE Module 2 assignment (IIT Roorkee / Futurense). Two Apache Airflow
pipelines over the same six CSV exports, producing reconciled operations
numbers before 9:00 AM IST, Monday to Saturday.

**Start here:** [`SUBMISSION.md`](SUBMISSION.md) - the assignment submission.

---

## 1. Project overview

NammaMart is a 10-minute grocery delivery company with 15 dark stores in
Bengaluru. Its morning operations review currently runs on a spreadsheet that
arrives an hour late and never reconciles with the payment gateway. This project
replaces it with two automated pipelines, built side by side on the same data so
the client can choose between them:

| Pipeline | DAG | Approach |
| --- | --- | --- |
| **ETL** | `dags/nammamart_etl_waseem.py` | clean in pandas, then load only validated rows into a DuckDB star schema |
| **ELT** | `dags/nammamart_elt_waseem.py` | load every raw row into DuckDB, then clean with SQL in raw → staging → mart layers |

Both produce **identical numbers**: 2,088 delivered orders, ₹11,35,628.80 net
revenue and ₹2,63,436.93 gross margin for September 2026.

## 2. Architecture overview

```
six CSV exports ─┬─► ETL DAG ─► validate ─► quarantine ─► transform ─► etl_fact_orders + dims
                 │                                                     └─► reconciliation + reports
                 └─► ELT DAG ─► raw_* ─► stg_* (SQL) ─► mart_* (SQL)
                                   │          │
                                   └──────────┴─► dq_results (52 checks per run)
                                                  pipeline_runs (one row per run)
```

Diagrams and a full walkthrough: [`docs/PIPELINE_DEEP_DIVE.md`](docs/PIPELINE_DEEP_DIVE.md).

## 3. Prerequisites

| For the Airflow experience | For running and testing without Docker |
| --- | --- |
| Docker Desktop | Python 3.10 - 3.14 |
| Astro CLI (`astro version`) | `pip install -r requirements.txt` |
| 4 GB free RAM | - |

## 4. Setup

```bash
git clone <this repository>
cd IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model

# Option A - local Python only (fast, no Docker needed)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Option B - full Airflow
astro dev start          # http://localhost:8080  (admin / admin)
```

The six source files are already in `include/data/nammamart/`. They must never
be edited by hand.

## 5. Environment and secret configuration

No credentials are required to run the pipelines. One optional secret enables
the failure alert:

```bash
# set it as an Airflow Variable - never in code, never in git
astro dev run variables set SLACK_WEBHOOK_URL "https://hooks.slack.com/services/REPLACE/ME"
```

If the variable is absent, the pipeline logs a warning and continues normally.
`.gitignore` already excludes `.env` and `airflow_settings.yaml`.

Optional environment overrides (all have sensible defaults):

| Variable | Default | Purpose |
| --- | --- | --- |
| `NAMMAMART_PROJECT_ROOT` | auto-detected | project root, for unusual layouts |
| `NAMMAMART_DATA_DIR` | `include/data/nammamart` | source folder |
| `NAMMAMART_WAREHOUSE` | `include/warehouse_waseem.duckdb` | warehouse file |
| `NAMMAMART_QUARANTINE_DIR` | `include/quarantine/waseem` | rejected rows |

## 6. How to start the local environment

```bash
astro dev start      # start
astro dev logs -f    # follow logs
astro dev restart    # after changing requirements.txt
astro dev stop       # stop
```

## 7. How to run the pipelines

```bash
# Inside Airflow
astro dev run dags list-import-errors          # must print nothing
astro dev run dags test nammamart_elt_waseem   # ELT once, from the terminal
astro dev run dags test nammamart_etl_waseem   # ETL once

# Trigger with the next-day increment (UI: Trigger DAG w/ config)
#   {"include_increment": true}

# Without Airflow
python scripts/run_elt_local.py --run-id demo_elt
python scripts/run_etl_local.py --run-id demo_etl
python scripts/run_etl_local.py --run-id demo_inc --increment
```

## 8. How to run the tests

```bash
pytest tests/nammamart -q            # 62 tests
pytest tests/nammamart -q -k dag     # DAG parsing and structure only
```

The DAG tests skip themselves if Airflow is not installed.

## 9. How to inspect quarantine output

```bash
ls include/quarantine/waseem/
head -3 include/quarantine/waseem/orders_failed_<run_id>.csv
```

Each rejected row keeps all of its original columns plus `failure_reason`,
`run_id` and `source_file`. The same rows are in the `etl_quarantine` table with
the original record stored as JSON, and in the ELT staging tables as
`dq_status = 'QUARANTINED'`.

## 10. How to inspect DuckDB

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("include/warehouse_waseem.duckdb", read_only=True)
print(con.execute("SHOW TABLES").fetchall())
print(con.execute("""
  SELECT run_id, dag_id, status, rows_extracted, rows_loaded, rows_quarantined
  FROM pipeline_runs ORDER BY start_time DESC LIMIT 5""").fetch_df())
PY
```

Or point VS Code's SQLTools (or any DuckDB client) at
`include/warehouse_waseem.duckdb`.

> The warehouse file and the quarantine folder are **gitignored on purpose**:
> the ELT raw layer stores the source rows unmasked, and the assignment forbids
> unmasked customer data in the repository outside `include/data/`. Rebuild them
> in about two seconds:
>
> ```bash
> python scripts/run_elt_local.py --run-id rebuild_elt
> python scripts/run_etl_local.py --run-id rebuild_etl
> ```
>
> Masked samples of every artifact are committed under `docs/evidence/`.

Key tables:

| Table | Contents |
| --- | --- |
| `etl_fact_orders`, `etl_dim_*` | ETL star schema |
| `raw_*`, `stg_*`, `mart_*` | ELT layers |
| `dq_results` | quality scorecard, 52 checks per run per pipeline |
| `pipeline_runs` | run audit |
| `etl_quarantine`, `elt_rule_failures` | rejected rows and per-rule failures |
| `etl_payment_reconciliation`, `mart_payment_reconciliation` | order vs gateway mismatches |

## 11. Reporting and evidence scripts

```bash
python scripts/run_business_questions.py        # six business questions, both pipelines
python scripts/run_observability_experiments.py # the ten Part F experiments
```

Outputs land in `docs/evidence/`.

## 12. Expected outputs

| Output | Expected |
| --- | --- |
| Rows read (September) | 5,212 |
| Rows quarantined (ETL) | 152, every one with a reason |
| Orders loaded | 2,310 |
| Delivered orders / revenue | 2,088 / ₹11,35,628.80 |
| Quality checks per run | 52 |
| Incremental load | 150 inserted, 10 updated |
| Re-run | 0 inserted (idempotent) |

## 13. Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `dags list-import-errors` shows an error | missing package or typo | `astro dev restart` after editing `requirements.txt` |
| `IO Error: Could not set lock on file` | two processes writing DuckDB | stop other runs; the ELT raw tasks are chained for this reason |
| `FileNotFoundError: Source file not found` | dataset not copied | the files belong in `include/data/nammamart/` |
| `SchemaError: missing expected column` | a source column was renamed | fix the export, or update `EXPECTED_COLUMNS` after agreeing the change |
| DAG runs at 14:30 IST | naive `start_date` | keep `tz="Asia/Kolkata"` |
| Tests skipped with "Airflow is not installed" | local venv without Airflow | `pip install apache-airflow==3.3.1` |

## 14. Known limitations

1. **Distribution monitoring is weak.** A consistent 100x price change passes
   every rule (observability experiment 8, variant B). A rolling statistical
   baseline is the recommended next step.
2. **The ETL fact table has no history.** A corrected order overwrites the old
   row, so "September revenue as reported in September" needs an SCD Type 2
   fact. The ELT side keeps both versions.
3. **Raw holds unmasked personal data** and grows on every run; it needs
   separate credentials and an enforced retention policy in production.
4. **Airflow UI evidence is manual.** Docker and the Astro CLI are not installed
   on the machine used to build this, so retry behaviour and the Clear button
   need screenshots - see `docs/SUBMISSION_CHECKLIST.md`.
5. **DuckDB is single-writer**, so the ELT raw loads run sequentially rather
   than in parallel.

## 15. Documentation index

| Document | What it covers |
| --- | --- |
| [`SUBMISSION.md`](SUBMISSION.md) | the assignment submission: issues log, Part C answers, governance, observability, recommendation |
| [`docs/PIPELINE_DEEP_DIVE.md`](docs/PIPELINE_DEEP_DIVE.md) | end-to-end walkthrough with diagrams |
| [`docs/PART_B_STUDY_GUIDE.md`](docs/PART_B_STUDY_GUIDE.md) | the ELT pipeline |
| [`docs/PART_C_STUDY_GUIDE.md`](docs/PART_C_STUDY_GUIDE.md) | scheduling, cron and timezones |
| [`docs/PART_D_STUDY_GUIDE.md`](docs/PART_D_STUDY_GUIDE.md) | the six quality dimensions |
| [`docs/PART_E_STUDY_GUIDE.md`](docs/PART_E_STUDY_GUIDE.md) | governance, PII and the DPDP Act |
| [`docs/PART_F_STUDY_GUIDE.md`](docs/PART_F_STUDY_GUIDE.md) | observability and the ten experiments |
| [`docs/PART_G_STUDY_GUIDE.md`](docs/PART_G_STUDY_GUIDE.md) | writing the recommendation |
| [`docs/HANDS_ON_REBUILD_GUIDE.md`](docs/HANDS_ON_REBUILD_GUIDE.md) | 15 exercises to rebuild this yourself |
| [`docs/TEST_REPORT.md`](docs/TEST_REPORT.md) | what was tested, what failed and how it was fixed |
| [`docs/SUBMISSION_CHECKLIST.md`](docs/SUBMISSION_CHECKLIST.md) | every deliverable and screenshot |
| [`docs/EVALUATION_MAPPING.md`](docs/EVALUATION_MAPPING.md) | rubric criterion mapped to evidence |
| [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md) | how to demonstrate the work |
| [`docs/FINAL_REVIEW_REPORT.md`](docs/FINAL_REVIEW_REPORT.md) | honest self-assessment |

---

### About this Astro project

The repository was generated with `astro dev init`. `astro dev start` runs five
containers (Postgres, Scheduler, DAG Processor, API Server, Triggerer) and opens
the Airflow UI at http://localhost:8080. The original Astronomer readme is kept
at [`docs/ASTRO_DEFAULT_README.md`](docs/ASTRO_DEFAULT_README.md). The trainer's
demo DAG (`dags/meridian_pipeline.py`) and its helper modules are left
untouched, as the assignment requires.
