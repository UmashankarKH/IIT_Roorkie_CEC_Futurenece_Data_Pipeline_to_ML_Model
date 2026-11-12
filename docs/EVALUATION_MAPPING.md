# Evaluation mapping - rubric criterion to evidence

The assignment's rubric is worth 100 marks across eight areas, plus up to 10
bonus marks. Each row below gives the criterion, what it means in plain words,
where the implementation lives, and what proves it.

---

## 1. Data profiling and issues log (10 marks)

| Item | Detail |
| --- | --- |
| **What earns full marks** | issues found in all 5 data files, each mapped to the right dimension and severity |
| **Implementation** | `assignment/nammamart_dataset/data_profiling.ipynb` (Part 0), carried into `include/nammamart/dq_rules.py` with the same `RULE-0xx` identifiers |
| **Evidence** | `part0_data_issues_log.csv` (208 row-level instances), `part0_candidate_validation_rules.csv` (31 rules), `part0_client_questions.csv`; summarised in `SUBMISSION.md` section 1 |
| **Files covered** | orders (13 issue types), payments (5), customers (6), products (4), stores (2) - **all five** |
| **Test evidence** | `test_every_dimension_is_covered_for_every_dataset` |
| **Status** | Ready |
| **Note for the evaluator** | two severities were deliberately changed from the Part 0 draft, with the business reason stated in `SUBMISSION.md` section 1 |

## 2. ETL pipeline (15 marks)

| Requirement | Implementation | Evidence | Status |
| --- | --- | --- | --- |
| Parallel extract | five `extract_*` tasks with no upstream | `test_etl_extracts_five_files_in_parallel`; screenshot `docs/screenshots/s1_etl_graph_green.png` | Ready |
| Quarantine with reasons | `etl_pipeline.quarantine()`, `quarantine_frame()` | `include/quarantine/waseem/*.csv`, `etl_quarantine` table, `test_quarantine_files_contain_a_failure_reason` | Ready |
| PII masked | `masking.mask_customer_frame()`, `mask_name` for store managers | `test_no_reporting_table_exposes_personal_data`, `test_masked_values_are_actually_masked` | Ready |
| Star schema | `etl_fact_orders` + `etl_dim_customer` / `_product` / `_store` | warehouse tables | Ready |
| Idempotent load | `warehouse.upsert_table()` on `order_id` | `test_second_run_does_not_change_the_numbers` (0 inserts) | Ready |
| Reconciliation | `etl_pipeline.reconcile_payments()` → `etl_payment_reconciliation` | 180 mismatch rows, 4 reason types; `test_reconciliation_lists_mismatches_with_a_reason` | Ready |

## 3. ELT pipeline (15 marks)

| Requirement | Implementation | Evidence | Status |
| --- | --- | --- | --- |
| Raw / staging / mart layers | `include/sql/raw/` (5), `staging/` (13), `mart/` (5) | `SHOW TABLES` in the warehouse; `SUBMISSION.md` section 3 | Ready |
| Raw keeps every row, all text | `read_csv(..., all_varchar = true)` + 4 audit columns | `test_elt_raw_keeps_every_source_row` | Ready |
| SQL-only transforms | `elt_pipeline.py` executes `.sql` files and contains no cleaning logic | code review; every rule is a `chk_rule_*` column in SQL | Ready |
| Working MERGE for the increment | `include/sql/staging/stg_orders_merge.sql` | **150 inserted, 10 updated**; `test_increment_inserts_new_orders_and_updates_corrections`; screenshot `docs/screenshots/s8_increment_merge_log.png` | Ready |
| Totals match ETL | shared rule catalogue, identical definitions | `test_etl_and_elt_totals_match`, `test_quality_scorecards_agree_rule_by_rule` - zero differences | Ready |

## 4. Scheduling (10 marks)

| Requirement | Implementation | Evidence | Status |
| --- | --- | --- | --- |
| Correct cron | `schedule="0 9 * * 1-6"` in both DAGs | DAG source; screenshot `docs/screenshots/s3_schedule_9am_ist.png` | Ready |
| IST timezone handled | `pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")` | parsed `start_date` = `2026-09-30 18:30:00+00:00`; `test_schedule_is_9am_ist_monday_to_saturday` | Ready |
| catchup justified | `catchup=False` + written justification | `SUBMISSION.md` section 4 | Ready |
| retries justified | `retries=2`, `retry_delay=2 min`, `execution_timeout=30 min` | `test_retries_and_timeout_are_configured` | Ready |
| Four written answers | cron, UTC time, UTC start_date, laptop off | `SUBMISSION.md` section 4 | Ready |

## 5. Data quality dimensions (15 marks)

| Requirement | Implementation | Evidence | Status |
| --- | --- | --- | --- |
| All 6 dimensions, across files | 46 row rules + 6 file rules spanning orders, payments, customers, products, stores | `dq_rules.rule_catalogue()`; `SUBMISSION.md` section 5 | Ready |
| In both pipelines | Python (`dq_rules.py`) and SQL (`stg_*_view.sql`, `dq/*.sql`) | identical rule-by-rule results | Ready |
| Severities applied | Critical quarantines, Warning loads and logs | `test_unknown_payment_mode_is_only_a_warning` | Ready |
| `dq_results` populated every run | `warehouse.write_dq_results()` and `dq_results_elt.sql` | 52 rows per run per pipeline; screenshot `docs/screenshots/s5_dq_results.png` | Ready |

## 6. Data governance (10 marks)

| Requirement | Implementation | Evidence | Status |
| --- | --- | --- | --- |
| Specific, pipeline-based answers | all nine questions reference real tables and files | `SUBMISSION.md` section 6 | Ready |
| PII handled | masking + dropped source columns + access plan | `masking.py`, `stg_customers_view.sql`, automated test | Ready |
| Usable data dictionary | full column list for the fact and three dimensions with type, meaning, source, allowed values, PII flag | `SUBMISSION.md` section 6, answer 2 | Ready |
| Lineage traced | one mart number followed to the source CSV line | `SUBMISSION.md` section 6, answer 3; `_source_file`, `_source_row_num`, `_run_id` | Ready |

## 7. Observability tests (15 marks)

| Requirement | Implementation | Evidence | Status |
| --- | --- | --- | --- |
| Run-audit table | `write_run_audit` task with `trigger_rule="all_done"` in both DAGs | `pipeline_runs`; `test_run_audit_uses_all_done_trigger_rule`; screenshot `docs/screenshots/s6_pipeline_runs.png` | Ready |
| All 10 experiments recorded | `scripts/run_observability_experiments.py` | `docs/evidence/observability_results.md` + JSON; `SUBMISSION.md` section 7 | Ready |
| Expected → Observed → Where → Change | every experiment has all four fields | same files | Ready |
| Pillars mapped | freshness, volume, schema, distribution, lineage with strongest and weakest named | `SUBMISSION.md` section 7 | Ready |
| Experiments 4 and 10 UI behaviour | retries and Clear | screenshots S9, S10 | **Manual** |

## 8. Client recommendation (10 marks)

| Requirement | Implementation | Evidence | Status |
| --- | --- | --- | --- |
| Evidence-based | every claim cites a measured number from Parts A-F | `SUBMISSION.md` section 8 | Ready |
| Written for a CTO | decision first, one page, no jargon dump | same | Ready |
| Covers the five dimensions | auditability, quality guarantees, ease of change, 1000x performance, cost | same | Ready |
| Clear risks | three risks, each with a control | same | Ready |

## 9. Bonus (up to +10)

| Bonus item | Status | Evidence |
| --- | --- | --- |
| Slack / email alert using an Airflow Variable | **implemented** | `include/nammamart/alerts.py`, wired as `on_failure_callback` in both DAGs; delivery needs screenshot `docs/screenshots/s1_etl_graph_green.png`1 |
| Store-capacity check (daily orders vs `capacity_orders_per_day`) | **implemented** | `over_capacity` column in `mart_daily_store_kpis`; no store exceeded its capacity in September (peak 13 orders/day against capacities of 300-500), and stores whose capacity itself is invalid are excluded and reported by RULE-031 instead |
| SCD Type 2 customer dimension | **not implemented** | designed and described in `SUBMISSION.md` section 6, answer 9 |
| Data-quality trend chart from `dq_results` | **not implemented** | the data is there (`dq_results` grows every run); no chart was produced |

---

## Summary of outstanding actions

| Action | Blocks which criterion | Effort |
| --- | --- | --- |
| Optionally set `SLACK_WEBHOOK_URL` and capture a delivered alert | bonus | ~10 minutes |
| Optionally build the `dq_results` trend chart | bonus | ~30 minutes |
| Optionally build the SCD Type 2 customer dimension | bonus | ~1 hour |

Everything else is implemented, executed and evidenced in the repository. The Airflow UI
screenshots are in `docs/screenshots/`; see `docs/TEST_REPORT.md` section 4 for what each
shows and for what was not verified.
