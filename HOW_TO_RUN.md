# How to review this submission (one page)

**Where to start:** read `SUBMISSION.md` (answers to Parts 0, C, D, E, F, G and the six
business questions), then run the pipelines below. Evidence is in `docs/evidence/`.

## Quick run, no containers (about 1 minute)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/run_elt_local.py --run-id demo_elt        # ELT: raw -> staging -> mart
python scripts/run_etl_local.py --run-id demo_etl        # ETL: validate -> star schema
python scripts/run_business_questions.py                 # six business questions
pytest tests/nammamart -q                                # expect 65 passed
```
Creates `include/warehouse_waseem.duckdb` (gitignored: its raw layer holds source PII).

## Full Airflow with Docker (the intended setup)
```bash
astro dev start               # prints the Airflow UI address (usually http://localhost:8080)
```
1. Trigger `nammamart_etl_waseem` and `nammamart_elt_waseem`; both should go green.
2. Trigger ELT again with config `{"include_increment": true}` to see the MERGE log line
   in the `merge_orders_increment` task.
3. Schedule is `0 9 * * 1-6` in Asia/Kolkata.

Expected figures for the September files, from either pipeline: 2,310 orders loaded,
2,088 delivered, net revenue ₹11,35,628.80, 52 quality checks and 202 failing rows per run
(ETL quarantines 152 rows, ELT labels 136 in staging). They do not change with the date you
run it. After the increment the MERGE log reads `150 inserted, 10 updated`.

## Full Airflow with Podman
```bash
podman machine init && podman machine start     # once
astro config set container.binary podman
astro dev start
```
(Do not commit the `astro config` change.) If the Airflow tasks sit in "no status", the
container VM clock is behind your computer's: compare `date -u` with
`podman machine ssh <machine> "date -u"` and restart the machine.

## Where things are
| What | Where |
| --- | --- |
| DAGs | `dags/nammamart_etl_waseem.py`, `dags/nammamart_elt_waseem.py` |
| Python logic, quality rules, masking | `include/nammamart/` |
| SQL (raw, staging, mart, dq, business questions) | `include/sql/` |
| Evidence and results | `docs/evidence/`, `docs/TEST_REPORT.md` |

`dags/meridian_pipeline.py` is the trainer's file and is unchanged.