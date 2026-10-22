# Test report

**Run date:** 2026-10-22 · **Command:** `pytest tests/nammamart -q`
**Result: 65 passed, 0 failed.**

Environment used for these results (a local virtual environment). The Airflow UI
evidence in `docs/screenshots/` was captured separately, from the same code running
in Astro containers (see section 4):

| Component | Version |
| --- | --- |
| Python | 3.14.7 |
| pandas | 3.0.6 |
| DuckDB | 1.5.6 |
| Apache Airflow | 3.3.1 (used for real DAG parsing in the tests) |
| pytest | 9.1.1 |

---

## 1. Automatically tested and passed

### 1.1 Unit tests - quality rules, masking, transforms (33 tests)

`tests/nammamart/test_quality_rules.py`

| Area | What is asserted |
| --- | --- |
| Happy path | a fully valid order passes every rule |
| Completeness | missing `order_id` / `order_ts` / `customer_id` is quarantined; a DELIVERED order without `delivery_minutes` is quarantined; a CANCELLED order without it is allowed |
| Uniqueness | a duplicated `order_id` keeps **one** surviving row and quarantines the redundant copy |
| Validity | `SHIPPED` status, unparsable timestamp and out-of-range discount are quarantined; an unknown payment mode is a Warning that still loads; `" upi "` is standardised before the check so it does not warn |
| Accuracy | negative quantity, wrong `order_amount`, 240-minute delivery are quarantined |
| Consistency | unknown `customer_id`, `product_id`, `store_id` are quarantined with the right rule |
| Timeliness | a 2027 timestamp is quarantined; future-dated means *after the fixed as-of date*, so results do not change with the day the test runs; the freshness check ignores future rows and reports the September file as stale |
| Volume | a shrunken file produces a WARN scorecard row |
| Schema guard | renaming `order_amount` raises `SchemaError` naming the missing column |
| Scorecard | every dataset covers at least four dimensions; `dq_results` rows contain all the columns the assignment requires |
| Masking | `pr***@example.com`, `97******21`, `Priya H.`; nulls handled; original columns dropped |
| Transforms | payment-mode variants standardised, `order_amount` formula, `within_15_min` only for DELIVERED |

### 1.2 Integration tests - both pipelines end to end (18 tests)

`tests/nammamart/test_pipelines_integration.py` (runs ETL and ELT against the
real dataset in a temporary DuckDB file)

| Client criterion | Test | Result |
| --- | --- | --- |
| 2 - no bad order reaches finance | `test_etl_loads_only_validated_rows`, `test_rejected_order_is_not_in_the_revenue_total` | pass |
| 3 - every raw record kept | `test_elt_raw_keeps_every_source_row` (2,412 / 2,228 / 405 / 152 / 15) | pass |
| 4 - rejected rows kept with a reason | `test_quarantine_files_contain_a_failure_reason`, `test_elt_staging_labels_rejected_rows_instead_of_deleting_them` | pass |
| 5 - re-run never double-counts | `test_second_run_does_not_change_the_numbers` (0 inserts both sides), `test_raw_layer_grows_on_a_re_run_by_design` | pass |
| 6 - incremental load | `test_increment_inserts_new_orders_and_updates_corrections` (**150 inserted, 10 updated**, 10 rows not 20) | pass |
| 7 - reconciliation | `test_reconciliation_lists_mismatches_with_a_reason` (same counts in both pipelines) | pass |
| 8 - PII protected | `test_no_reporting_table_exposes_personal_data`, `test_masked_values_are_actually_masked` | pass |
| 9 - run visibility | `test_every_run_is_recorded_in_pipeline_runs` | pass |
| Part B - totals match | `test_etl_and_elt_totals_match`, `test_quality_scorecards_agree_rule_by_rule` (zero rule differences) | pass |
| Uniqueness design | `test_duplicate_order_keeps_exactly_one_copy` | pass |

### 1.3 DAG tests - real Airflow parsing (14 tests)

`tests/nammamart/test_dag_structure.py`

| What is asserted | Result |
| --- | --- |
| Both DAGs load in a real `DagBag` with **no import errors** | pass |
| `start_date` is timezone aware and equals `2026-09-30 18:30:00+00:00` (= 2026-10-01 00:00 IST) | pass |
| `catchup=False`, `max_active_runs=1` on both | pass |
| Every task has `retries=2`, `retry_delay=2 min`, `execution_timeout=30 min` | pass |
| ETL has five extract tasks with no upstream (parallel) feeding `validate_and_quarantine` | pass |
| ETL order: validate → transform → load → reconcile → report → audit | pass |
| ELT has one raw task per table plus the increment task | pass |
| ELT layer order: staging → merge base → merge increment → payments → quality → mart → audit | pass |
| `build_staging_payments` runs after the orders merge | pass |
| `write_run_audit` uses `trigger_rule=all_done` in both DAGs and is the last task | pass |
| Neither DAG file contains a webhook, password, token or API key | pass |

---

## 2. Automatically tested and failed, then fixed

| # | Failure | Root cause | Fix | Final status |
| --- | --- | --- | --- | --- |
| 1 | `test_rejected_order_is_not_in_the_revenue_total` asserted 0 rows but found 1 | the quarantined row it picked was a *redundant duplicate copy*; the surviving copy is legitimately in the fact table | the test now excludes RULE-002 rows and a new test asserts a duplicate keeps exactly one copy | pass |
| 2 | ELT mart split "Unknown" into two category rows | `GROUP BY category` resolved to the underlying column, so `NULL` and `'Unknown'` grouped separately | repeat the full `coalesce(...)` expression in `GROUP BY` in all four mart files | pass |
| 3 | ELT increment run crashed with `Parser Error ... RETURNING` | `RETURNING merge_action()` was appended after a trailing comment and semicolon | the clause moved into the SQL file, then replaced entirely (see #4) | pass |
| 4 | `merge_action does not exist` | this DuckDB build does not expose `merge_action()` | count matched rows with a `SELECT` *before* the MERGE - version independent | pass |
| 5 | `initcap does not exist` | DuckDB has no `initcap()` | `upper(substr(x,1,1)) \|\| lower(substr(x,2))` | pass |
| 6 | `stg_orders does not exist` while creating the payments view | the payments view reads `stg_orders` | create that view in `build_staging_payments`, after the orders table exists | pass |
| 7 | 481 rows quarantined, 229 orders lost | duplicates quarantined *both* copies, and foreign keys were checked against the post-quarantine master, so one bad product row deleted ~94 real orders | keep one surviving copy per key; check foreign keys against the **source** master; add "unknown member" dimension rows; blank category downgraded to Warning | 152 quarantined, pass |
| 8 | ETL and ELT disagreed on `RULE-035` (freshness) | the Python check used `max(order_ts)`, which included future-dated 2027 rows, so a stale file looked fresh | exclude future rows from the freshness calculation | identical, pass |
| 9 | ETL and ELT disagreed on `DUPLICATE_PAYMENT` (8 vs 0) | ETL counted every copy of a repeated payment; ELT collapsed identical copies in staging | both now report one row per affected payment, using `_raw_copies` on the ELT side | identical (4 vs 4), pass |
| 10 | ETL and ELT disagreed on duplicate counts after the increment | ETL pre-deduplicated across files; ELT counted a repeated `order_id` across files as a duplicate | both now treat a repeated key in a *later file* as a correction and in the *same file* as a duplicate | identical, pass |
| 11 | Results changed with the calendar date: a run on 22 Oct loaded 2,312 orders and quarantined 148 (ETL) / 132 (ELT) instead of 2,310 / 152 / 136 | the "future-dated" rules compared against `now()` / `current_date`; two orders and two payments dated 21 Oct 2026 stopped being "future" on 21 Oct, and others would do the same on 2 Nov | one fixed as-of date (`NAMMAMART_AS_OF`, default `2026-10-02 00:00`, the end of the 1 October increment) used by the Python rules and by an `as_of()` macro in the staging SQL; the freshness rule ignores rows after it; regression test added | back to 2,310 / 152 / 136, pass |
| 12 | Every warehouse test failed with `ResourceInUseException` (`warehouse.py`, line 74) after `pip install` resolved `duckdb 2.0.0` | DuckDB 2.0 refuses new work on a connection that still has an unfinished result; the code was written and tested against 1.x | `requirements.txt` pins `duckdb>=1.4,<2`. The code itself was **not** changed to support 2.0 | pass on 1.5.6 |

---

## 3. Verified by running the pipelines (not unit tests)

| Check | Command | Result |
| --- | --- | --- |
| ETL end to end | `python scripts/run_etl_local.py --run-id demo` | 5,212 rows read, 152 quarantined, 2,310 fact rows, 0.2 s |
| ELT end to end | `python scripts/run_elt_local.py --run-id demo` | 5,212 raw rows, 2,310 PASS, 5 mart tables, 0.2 s |
| Incremental ETL | `--increment` | 150 inserted, 2,310 updated |
| Incremental ELT | `--increment` | MERGE: 150 inserted, 10 updated |
| Six business questions | `python scripts/run_business_questions.py` | identical results from both pipelines, written to `docs/evidence/business_questions.md` |
| Ten observability experiments | `python scripts/run_observability_experiments.py` | all ten recorded in `docs/evidence/observability_results.md` |

---

## 4. Requires manual verification (Airflow UI, needs Docker)

The DAGs are validated twice: with a real Airflow 3.3 `DagBag` in the tests above, and
by running them in Astro containers (Astro Runtime 3.3, Podman on an Apple M-series
Mac). The Airflow UI behaviour is evidenced by screenshots in `docs/screenshots/`:

| # | File | What it shows |
| --- | --- | --- |
| S1 | `s1_etl_graph_green.png` | ETL DAG, every task green |
| S2 | `s2_elt_graph_green.png` | ELT DAG, every task green |
| S3 | `s3_schedule_9am_ist.png` | schedule `0 9 * * 1-6` and the next run, both DAGs |
| S4 | `s4_quarantine_file.png` | a quarantine file with the `failure_reason` column |
| S5 | `s5_dq_results.png` | `dq_results` rows |
| S6 | `s6_pipeline_runs.png` | `pipeline_runs`, one row per run |
| S7a / S7b | `s7a_reconciliation.png`, `s7b_reconciliation.png` | `etl_payment_reconciliation`: missing payments, then amount mismatches |
| S8 | `s8_increment_merge_log.png` | `MERGE ...: 150 inserted, 10 updated` |
| S9a / S9b | `s9a_up_for_retry.png`, `s9b_retry_and_upstream_failed.png` | experiment 4: retries, then `failed` with downstream tasks not run |
| S10 | `s10_clear_recovery.png` | experiment 10: Clear re-runs only the failed task and what follows it |

Not verified:

* **Slack alert delivery** - needs a real webhook in an Airflow Variable; the code path
  and its fallback when the Variable is missing are tested, delivery is not.
* **Docker** - the containers were run with Podman, not Docker Desktop.
* **A real run date** - the screenshots show October 2026 runs on a laptop whose clock
  had to be re-synchronised with the container VM once (a skewed clock keeps tasks in
  "no status" because the logical date lies in the future).
* **A reviewer's environment** - to run on this Mac the base image's `cryptography` had
  to be pinned to 44.0.3 in a local, uncommitted Dockerfile line (the image's 48.0.1
  crashes with "Illegal instruction" under the Podman VM). That is a laptop issue, not
  part of the submission.

One honest limit of the Airflow evidence: in S9b the *run* is green although a task
failed, because `write_run_audit` uses `trigger_rule="all_done"` and succeeds. The failure
is visible on the task and in `pipeline_runs`, but not in the run state.

---

## 5. Not required by the assignment

* No machine learning component - the assignment has no ML or AI requirement
  (the generic prompt template mentions one; the assignment document, which is
  the source of truth, does not). `include/ml_forecast.py` and
  `include/ai_explainer.py` belong to the trainer's demo pipeline and were not
  modified.
* No load or performance testing - the dataset is 5,372 rows.

---

## 6. How to reproduce this report

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt            # pandas, duckdb (<2), pendulum, pytest
pytest tests/nammamart -q                  # 65 tests
python scripts/run_elt_local.py --run-id reproduce_elt
python scripts/run_etl_local.py --run-id reproduce_etl
python scripts/run_business_questions.py
python scripts/run_observability_experiments.py
```

The DAG tests are skipped automatically if Airflow is not installed; install
`apache-airflow==3.3.1` (Python 3.10-3.14) to run them.
