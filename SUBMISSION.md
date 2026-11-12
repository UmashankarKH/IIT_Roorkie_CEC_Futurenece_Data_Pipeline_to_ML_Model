# NammaMart ETL and ELT pipelines - Submission

**Student:** waseem · **Module:** FDAIE Module 2, Data Engineering Foundations
**Repository:** `IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model`
**Deliverables:** `dags/nammamart_etl_waseem.py`, `dags/nammamart_elt_waseem.py`,
helper code in `include/nammamart/`, SQL in `include/sql/`, warehouse
`include/warehouse_waseem.duckdb`.

---

## 0. Headline numbers

| Measure | ETL pipeline | ELT pipeline |
| --- | --- | --- |
| Source rows read (September) | 5,212 | 5,212 |
| Rows rejected with a reason | 152 | 136 labelled in staging + 16 duplicate copies collapsed |
| Orders loaded for reporting | 2,310 | 2,310 |
| Delivered orders counted as revenue | 2,088 | 2,088 |
| **September net revenue** | **₹11,35,628.80** | **₹11,35,628.80** |
| **September gross margin** | **₹2,63,436.93** | **₹2,63,436.93** |
| After the 1 October increment | 2,228 delivered · ₹12,11,087.30 | 2,228 delivered · ₹12,11,087.30 |
| Quality checks recorded per run | 52 | 52 |
| Failing rows reported per run | 202 | 202 |

The two pipelines agree **exactly**, rule by rule and rupee by rupee. The
comparison is re-run by `tests/nammamart/test_pipelines_integration.py`.

How to reproduce everything:

```bash
python scripts/run_elt_local.py --run-id demo_elt          # ELT: raw -> staging -> mart
python scripts/run_etl_local.py --run-id demo_etl          # ETL: validate -> star schema
python scripts/run_etl_local.py --run-id demo_inc --increment   # next-day file
python scripts/run_business_questions.py                   # six business questions
python scripts/run_observability_experiments.py            # the ten Part F experiments
pytest tests/nammamart                                     # 65 tests
```

---

## 1. Part 0 - Data Issues Log

Full row-level log: [`assignment/nammamart_dataset/part0_outputs/part0_data_issues_log.csv`](assignment/nammamart_dataset/part0_outputs/part0_data_issues_log.csv)
(208 row-level issue instances, 181 distinct affected source rows, issues found in **all five**
files). Candidate rules: [`part0_candidate_validation_rules.csv`](assignment/nammamart_dataset/part0_outputs/part0_candidate_validation_rules.csv).
Client questions: [`part0_client_questions.csv`](assignment/nammamart_dataset/part0_outputs/part0_client_questions.csv).
Profiling notebook: [`data_profiling.ipynb`](assignment/nammamart_dataset/data_profiling.ipynb).

| File | Problem found | Quality dimension | Rows | Severity in the pipeline | Rule |
| --- | --- | --- | --- | --- | --- |
| orders.csv | `order_amount` does not match quantity x unit_price x (1 - discount/100) | Accuracy | 18 | Critical | RULE-011 |
| orders.csv | duplicate `order_id` (12 identical pairs) | Uniqueness | 24 rows / 12 ids | Critical (redundant copy only) | RULE-002 |
| orders.csv | `customer_id` missing | Completeness | 18 | Critical | RULE-006 |
| orders.csv | `quantity` zero or negative (min -3) | Accuracy | 10 | Critical | RULE-009 |
| orders.csv | `product_id` not in products.csv | Consistency | 9 | Critical | RULE-007 |
| orders.csv | DELIVERED order with no `delivery_minutes` | Completeness | 8 | Critical | RULE-014 |
| orders.csv | `order_ts` dated 2027 | Timeliness | 7 | Critical | RULE-005 |
| orders.csv | `delivery_minutes` above 120 | Accuracy | 6 | Critical | RULE-015 |
| orders.csv | `store_id` not in stores.csv | Consistency | 5 | Critical | RULE-008 |
| orders.csv | `order_ts` not a parsable timestamp | Validity | 5 | Critical | RULE-004 |
| orders.csv | `order_status` outside the allowed list (e.g. `SHIPPED`) | Validity | 3-5 | Critical | RULE-012 |
| orders.csv | `discount_pct` outside 0-100 | Validity | 3 | Critical | RULE-010 |
| orders.csv | `order_ts` blank | Completeness | 1 | Critical | RULE-003 |
| orders.csv | `payment_mode` spelling (e.g. `Cashh`, `upi`) | Validity | 10 (8 fixed by standardising, 2 remain) | Warning | RULE-013 |
| payments.csv | SUCCESS amount differs from the order amount | Consistency | 14 | Critical | RULE-018 |
| payments.csv | `order_id` not present in orders.csv | Consistency | 8 | Critical | RULE-017 |
| payments.csv | `payment_ts` in the future | Timeliness | 7 | Critical | RULE-019 |
| payments.csv | duplicate `payment_id` (4 identical pairs) | Uniqueness | 8 rows / 4 ids | Critical (redundant copy only) | RULE-016 |
| payments.csv | SUCCESS payment with amount <= 0 | Accuracy | 10 | Critical | RULE-038 |
| customers.csv | duplicate `customer_id` with conflicting values | Uniqueness | 10 rows / 5 ids | Critical (superseded copy only) | RULE-021 |
| customers.csv | malformed email | Validity | 12 | Warning | RULE-040 |
| customers.csv | phone not 10 digits | Validity | 7 | Warning | RULE-041 |
| customers.csv | `loyalty_tier` casing (`Silver` vs `SILVER`) | Validity | 8 | Warning (standardised) | RULE-024 |
| customers.csv | `city` blank | Completeness | 6 | Warning (defaults to `Unknown`) | RULE-022 |
| customers.csv | `signup_date` in the future | Timeliness | 3 | Warning | RULE-023 |
| customers.csv | `pincode` not 6 digits | Validity | 3 | Warning | RULE-042 |
| products.csv | duplicate `product_id` with conflicting values | Uniqueness | 4 rows / 2 ids | Critical (superseded copy only) | RULE-025 |
| products.csv | `cost_price` greater than `mrp` | Accuracy | 5 | Warning | RULE-028 |
| products.csv | `category` blank | Completeness | 3 | Warning (defaults to `Unknown`) | RULE-026 |
| products.csv | `mrp` zero or negative | Accuracy | 1 | Critical | RULE-027 |
| stores.csv | `zone` blank | Completeness | 1 | Warning (defaults to `Unknown`) | RULE-030 |
| stores.csv | `capacity_orders_per_day` zero or negative | Accuracy | 1 | Warning | RULE-031 |

### Three questions for the client (and the decision taken meanwhile)

1. **Is a negative `quantity` a return, a correction or a data-entry error?**
   10 orders have `quantity <= 0`, down to -3, producing negative revenue.
   *Decision until answered:* treat as Critical and quarantine, so revenue is never reduced by a row we do not understand.
2. **Should an order with a missing `customer_id` still count as revenue?**
   18 orders have no customer. The money was taken, but the order cannot be attributed.
   *Decision until answered:* Critical. Finance totals must tie to an identifiable customer; the rows sit in quarantine and can be released once the client decides.
3. **What is the real upper limit for `delivery_minutes`?**
   6 orders report more than 120 minutes in a business that promises 10.
   *Decision until answered:* Critical above 120 minutes, because such rows would destroy the SLA KPI. A client-confirmed threshold is a one-line change in `include/nammamart/config.py`.

### Two deliberate deviations from the Part 0 draft severities

| Item | Part 0 draft | Implemented | Why |
| --- | --- | --- | --- |
| Blank `category` in products.csv | Critical | **Warning** (loads as `Unknown`) | Rejecting 3 product rows removed ~94 real delivered orders from revenue through the foreign-key check. A missing label must not delete money that was actually taken. The gap is still visible in `dq_results` and in the `Unknown` category row of the margin report. |
| Duplicate keys | quarantine every copy | **quarantine the redundant copy only** | Quarantining both copies of `ORD000539` would have deleted a real order. One row per key survives (newest wins), the extra copy is quarantined with RULE-002 and the reason. |

---

## 2. Part A - ETL pipeline (summary)

`dags/nammamart_etl_waseem.py` - **Extract → Validate → Quarantine → Transform → Load → Reconcile → Report → Audit**

* **Extract:** five parallel tasks, one per file, each writing a run-scoped staging CSV.
* **Validate:** `include/nammamart/dq_rules.py` applies 46 row rules and 6 file-level rules.
* **Quarantine:** `include/quarantine/waseem/<file>_failed_<run_id>.csv` plus the `etl_quarantine` table, both carrying `failure_reason`.
* **Transform:** standardise (`upi`, `UPI ` → `UPI`), deduplicate, mask email/phone/name, join to the three dimensions, derive `order_date`, `net_revenue`, `gross_margin`, `within_15_min`.
* **Load:** star schema in DuckDB - `etl_fact_orders` (upsert on `order_id`) plus `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`.
* **Reconcile:** `etl_payment_reconciliation`, one row per mismatch and a reason.
* **Report:** `etl_daily_zone_revenue`, `etl_category_margin`, printed in the task log.

Rejected rows by file (one run): orders 102, payments 42, customers 5, products 3.

Reconciliation result (September):

| Reason | Rows | Amount involved |
| --- | --- | --- |
| ORPHAN_PAYMENT (payment with no clean order) | 93 | ₹88,159.45 |
| MISSING_PAYMENT (delivered, never paid) | 69 | ₹42,386.60 |
| AMOUNT_MISMATCH | 14 | ₹785.00 |
| DUPLICATE_PAYMENT | 4 | ₹0.00 |

---

## 3. Part B - ELT pipeline

`dags/nammamart_elt_waseem.py` - **Extract → Load raw → Transform in the warehouse with SQL**

| Layer | Tables | What happens |
| --- | --- | --- |
| Raw | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | Exact copy of each CSV, **every column text**, plus `_loaded_at`, `_source_file`, `_run_id`, `_source_row_num`. Append-only, never cleaned, never deleted. |
| Staging | `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` | SQL only: cast, standardise, dedupe, mask PII, apply the Part D rules, set `dq_status` (`PASS` / `QUARANTINED` / `SUPERSEDED`) and `failure_reason`. |
| Mart | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` | SQL only: the tables that answer the six business questions. |

Every transformation is a `.sql` file in `include/sql/` executed by an Airflow
task. No pandas touches the ELT data path.

Staging status after a September run:

| Dataset | PASS | QUARANTINED | SUPERSEDED |
| --- | --- | --- | --- |
| orders | 2,310 | 90 | 0 |
| payments | 2,186 | 38 | 0 |
| customers | 400 | 0 | 5 |
| products | 149 | 1 | 2 |
| stores | 15 | 0 | 0 |

### Incremental load (B.3)

`orders_2026-10-01_increment.csv` is appended to `raw_orders` and then merged
into staging with `include/sql/staging/stg_orders_merge.sql`:

```
[ELT raw]     increment appended: +160 rows
[ELT staging] MERGE orders.csv: 0 inserted, 2390 updated, 2400 rows in stg_orders
[ELT staging] MERGE orders_2026-10-01_increment.csv: 150 inserted, 10 updated, 2550 rows
```

**150 new orders inserted, 10 corrections updated, no duplicates** - the fact
table and `stg_orders` both hold 2,460 distinct `order_id` values afterwards.

One honest difference in the run audit: on the increment run the ELT pipeline
records `rows_extracted = 5,372` and the ETL pipeline `5,362`. The ELT raw layer
appends every row of both files, while the ETL extract drops the 10 superseded
September rows before validation, because for ETL a repeated `order_id` in a
later file is a correction rather than a duplicate. The loaded totals are
identical (2,460).
The 10 September versions remain in staging with `dq_status = 'SUPERSEDED'`, so
the change history is still visible.

### Do the mart totals match the ETL totals?

Yes, exactly - see section 0. The check is automated
(`test_etl_and_elt_totals_match`, `test_quality_scorecards_agree_rule_by_rule`).
Two design choices make the agreement possible:

1. Both pipelines read the same rule definitions. Severity and dimension live in
   one place (`dq_rules.rule_catalogue()`), published into DuckDB as
   `dq_rule_catalogue`, which the ELT quality SQL joins to.
2. Both treat a repeated `order_id` in a *later file* as a correction, and a
   repeated `order_id` in the *same file* as a duplicate.

---

## 4. Part C - Scheduling answers

Both DAGs:

```python
schedule="0 9 * * 1-6",
start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
catchup=False,
max_active_runs=1,
default_args={"retries": 2, "retry_delay": timedelta(minutes=2),
              "execution_timeout": timedelta(minutes=30)}
```

**1. What is your cron expression?**
`0 9 * * 1-6` - minute 0, hour 9, every day of the month, every month, weekdays
Monday to Saturday. Sunday is excluded because the leadership stand-up does not run on Sunday.

**2. What time does the run show in UTC in the Airflow UI?**
**03:30 UTC.** India Standard Time is UTC+05:30, so 09:00 IST is 03:30 UTC. Airflow stores
everything in UTC; the DAG's `start_date` of 2026-10-01 00:00 IST is stored as
`2026-09-30 18:30:00+00:00`, which is exactly what the parsed DAG reports.

**3. What would happen at 9 AM if you had left the `start_date` in UTC?**
The cron would be evaluated in UTC, so the DAG would start at 09:00 UTC = **14:30 IST**,
five and a half hours after the stand-up. The report would be useless every single day,
and nothing in the UI would look broken - the run would be green, just late.

**4. What happens if your laptop or Docker is off at 9 AM?**
No scheduler is running, so no run is created. Because `catchup=False`, Airflow does not
replay the missed schedule when it starts again; it simply waits for the next 09:00 IST
slot. The 9 AM number for that day has to be produced by triggering the DAG manually.
This is exactly why a laptop is not a production environment: the fix is to run Airflow on
an always-on server or a managed service, with alerting if a run does not start.

**Why `catchup=False` is correct here.** The client wants *today's* reconciled number. If
the pipeline were off for a week, `catchup=True` would queue six backfill runs that all
read the same current CSV exports, re-processing identical data and competing for the
single DuckDB writer. There is no value in replaying a late morning report.

---

## 5. Part D - Data quality dimensions

Every check is stored in `dq_results` on every run, from **both** pipelines,
with the columns the assignment requires (`run_id`, `dag_id`, `check_name`,
`dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, `checked_at`)
plus `rule_id`, `dataset` and `detail` for traceability.

| Dimension | Example checks implemented | Severity | Rows failing (one September run) |
| --- | --- | --- | --- |
| Completeness | order_id / order_ts / customer_id present; DELIVERED has delivery_minutes; product_id present; city, zone, category not blank | Critical (8 checks) + Warning (4) | 27 Critical, 10 Warning |
| Uniqueness | order_id, payment_id, customer_id, product_id, store_id - one surviving row per key | Critical (5) | 23 |
| Validity | order_status and payment_mode in the allowed lists, timestamp parses, discount 0-100, email format, phone 10 digits, pincode 6 digits, loyalty tier, zone, channel | Critical (3) + Warning (9) | 11 Critical, 26 Warning |
| Accuracy | quantity > 0; order_amount matches the formula; delivery_minutes 0-120; mrp > 0; SUCCESS amount > 0; cost_price <= mrp; capacity > 0 | Critical (5) + Warning (2) | 45 Critical, 6 Warning |
| Consistency | customer_id / product_id / store_id exist in their masters; payment order_id exists; SUCCESS payment equals the order amount | Critical (5) | 36 |
| Timeliness | no future order_ts, payment_ts, signup_date, launch_date, open_date; newest order not older than 1 day | Critical (2) + Warning (4) | 14 Critical, 4 Warning |
| Volume (extra) | each file within 90% of its baseline row count | Warning (5) | 0 |

* **Critical** → the row is quarantined and never reaches a reporting table.
* **Warning** → the row loads, the problem is recorded in `dq_results` and in a
  `dq_warnings` column on the row itself.

Implementation: Python in `include/nammamart/dq_rules.py` (ETL) and SQL in
`include/sql/staging/*_view.sql` plus `include/sql/dq/*.sql` (ELT).

Query the scorecard:

```sql
SELECT dimension, severity, count(*) AS checks, sum(rows_failed) AS failed_rows
FROM dq_results
WHERE run_id = (SELECT run_id FROM pipeline_runs ORDER BY start_time DESC LIMIT 1)
GROUP BY dimension, severity ORDER BY dimension;
```

---

## 6. Part E - Data governance

**1. Ownership.**
Orders and payments are owned by the systems that produce them: the order
management team owns `orders.csv`, the payments team owns `payments.csv`.
CRM owns `customers.csv` (and the personal data in it), merchandising owns
`products.csv`, retail operations owns `stores.csv`. The data engineering team
(me) owns the warehouse tables, the quality rules and the two DAGs. A change to
a quality rule - for example the 120-minute delivery limit in
`config.MAX_DELIVERY_MINUTES` - needs sign-off from the business owner of the
affected number: operations for delivery SLA, finance for anything that changes
revenue. Every change goes through a pull request so the rule history is auditable.

**2. Data dictionary (fact and dimensions).**

`etl_fact_orders` (one row per validated order):

| Column | Type | Meaning | Source | Allowed values | PII |
| --- | --- | --- | --- | --- | --- |
| order_id | VARCHAR | business key of the order | orders.csv | unique, not null | No |
| order_ts | TIMESTAMP | when the order was placed | orders.csv | not future | No |
| order_date | DATE | calendar date, used for daily grouping | derived from order_ts | - | No |
| customer_id | VARCHAR | who ordered | orders.csv → customers.csv | must exist in the master | No (pseudonymous) |
| store_id / store_name / zone | VARCHAR | dark store and its zone | orders.csv → stores.csv | zone: North/South/East/West/Unknown | No |
| product_id / category / sub_category | VARCHAR | what was sold | orders.csv → products.csv | category from the catalogue, else `Unknown` | No |
| quantity | BIGINT | units ordered | orders.csv | > 0 | No |
| unit_price | DOUBLE | price per unit charged | orders.csv | >= 0 | No |
| discount_pct | BIGINT | discount applied | orders.csv | 0-100 | No |
| order_amount | DOUBLE | money charged, after discount | orders.csv, re-checked against the formula | matches quantity x unit_price x (1 - discount/100) | No |
| cost_price / cost_of_goods | DOUBLE | product cost, and cost for this order | products.csv | >= 0 | No |
| net_revenue | DOUBLE | revenue counted for this order | derived: order_amount when DELIVERED, else 0 | >= 0 | No |
| gross_margin | DOUBLE | net_revenue - cost_of_goods | derived | - | No |
| payment_mode | VARCHAR | how it was paid | orders.csv, standardised | UPI / Card / Cash on Delivery / Wallet | No |
| order_status | VARCHAR | order outcome | orders.csv | DELIVERED / CANCELLED / RETURNED | No |
| channel | VARCHAR | ordering channel | orders.csv | App / Web / Phone | No |
| delivery_minutes | DOUBLE | minutes to deliver | orders.csv | 0-120 | No |
| within_15_min | BOOLEAN | SLA flag | derived | true only for DELIVERED | No |
| is_revenue | BOOLEAN | does this row count as revenue | derived | - | No |
| loyalty_tier | VARCHAR | customer tier at load time | customers.csv | BRONZE/SILVER/GOLD/PLATINUM | No |
| dq_warnings | VARCHAR | non-blocking quality notes | derived | rule text or null | No |
| run_id / loaded_at | VARCHAR / TIMESTAMP | which run produced this row | pipeline | - | No |

`etl_dim_customer`: `customer_id` (key), `customer_name_masked` (**masked PII**),
`email_masked` (**masked PII**), `phone_masked` (**masked PII**), `city`, `area`,
`pincode` (quasi-identifier), `signup_date`, `loyalty_tier`, `is_active`.
`etl_dim_product`: `product_id`, `product_name`, `category`, `sub_category`, `brand`,
`mrp`, `cost_price`, `unit`, `is_perishable`, `launch_date` - no PII.
`etl_dim_store`: `store_id`, `store_name`, `zone`, `area`, `pincode`, `open_date`,
`manager_name_masked` (**masked PII**), `capacity_orders_per_day`.

**3. Lineage.**
Take `mart_daily_store_kpis` for 2026-09-19, store S02: 13 delivered orders,
₹9,732.00. In **ELT** I can trace it in one query chain:
`mart_daily_store_kpis → stg_orders (dq_status='PASS', store_id='S02', order_date='2026-09-19')
→ raw_orders` using `_source_file` and `_source_row_num`, which point at the exact line of
the exact CSV, and `_run_id`, which says which run loaded it. In **ETL** I can get to
`etl_fact_orders` and then only back to the source file by name and `order_id`: the row as
originally received is not kept unless it was rejected. **ELT is easier**, because the raw
layer is the evidence; in ETL the evidence was transformed away.

**4. Personal data.**
Personal columns in the dataset: `customers.full_name`, `customers.email`,
`customers.phone`, plus `customers.area`/`pincode` as quasi-identifiers, and
`stores.manager_name` (employee data). Masking is applied in
`include/nammamart/masking.py` (ETL) and in `stg_customers_view.sql` (ELT):
`priya.hegde199@example.com → pr***@example.com`, `9774349021 → 97******21`,
`Priya Hegde → Priya H.`. The unmasked columns are **dropped**, not overwritten, so no
reporting table can leak them (enforced by `test_no_reporting_table_exposes_personal_data`).
Masking rather than deletion, because analytics still needs to count distinct customers,
group by tier and city, and support "which customer is this?" via `customer_id` for an
authorised user. The applicable law is India's **Digital Personal Data Protection Act,
2023 (DPDP Act)**: process only for a notified purpose with consent, collect the minimum
needed, keep it only as long as the purpose requires, protect it with reasonable security
safeguards, report breaches, and honour the data principal's rights to access, correct
and erase. Masked reporting tables plus a tightly controlled raw layer is how this
pipeline supports that.

**5. Access control.**

| Layer | Who should read it | Why |
| --- | --- | --- |
| `raw_*` (contains unmasked phone, email, names) | data engineering only, and a named compliance/DPO role on request | it is evidence, not a product; unmasked PII lives here |
| `stg_*` | data engineering and senior analysts | useful for debugging, already masked, but contains rejected rows |
| `mart_*`, `etl_fact_*`, `etl_dim_*` | finance, operations, store managers, the COO | masked, validated, business-ready |
| quarantine files and `etl_quarantine` | data engineering plus the owning source team | they must fix the records at source |

Raw `customers` with unmasked phone numbers should be readable by **nobody** in day-to-day
work. In DuckDB there are no row-level grants, so in production I would move raw into a
separate database file or schema with its own credentials, and expose only masked views.

**6. Retention.**
Raw: keep 24 months, because finance audits run on a yearly cycle and a replay of last
year must be possible; after that, archive to cold storage. Quarantine files: 90 days -
long enough for the source team to fix the records, short enough that rejected PII does
not accumulate. Masked customer data in the mart: keep while the customer is active plus
24 months, then delete the customer dimension row and keep only `customer_id` so history
still aggregates. After the retention period the rows are deleted, not just hidden, and
the deletion itself is logged.

**7. Secrets.**
There are no credentials in this project's code. The one optional secret, the Slack
webhook URL, is read at run time with `Variable.get("SLACK_WEBHOOK_URL")` in
`include/nammamart/alerts.py` and is never logged or printed. A secret in a DAG file
would be committed to GitHub, visible to everyone with repository access, impossible to
rotate without a code change, and would appear in the rendered DAG source in the Airflow
UI. `.gitignore` excludes `.env` and `airflow_settings.yaml`, and
`test_dag_files_contain_no_secrets` fails the build if a webhook or password string
appears in a DAG.

**8. Audit - "show a rejected order exactly as received".**
In **ETL**: `SELECT * FROM etl_quarantine WHERE row_key = 'ORD000153'` returns the
original record as JSON in `raw_record`, the `failure_reason`
(`RULE-012: order_status must be in the approved list [Validity/Critical]`), the
`run_id` and the timestamp; the same row is in
`include/quarantine/waseem/orders_failed_<run_id>.csv`. Proof it is not in revenue:
`SELECT count(*) FROM etl_fact_orders WHERE order_id = 'ORD000153'` returns 0.
In **ELT**: `SELECT * FROM raw_orders WHERE order_id = 'ORD000153'` shows it exactly as
received, `SELECT dq_status, failure_reason FROM stg_orders WHERE order_id = 'ORD000153'`
shows `QUARANTINED` and the rule, and the mart tables only ever read
`dq_status = 'PASS'`. Both answers are reproducible in `dq_results` with the rule
identifier and the run.

**9. Change history - DELIVERED on 3 September, RETURNED on 1 October.**
After the incremental load, **both** pipelines show that order as RETURNED with zero
revenue, so September revenue drops by that amount: September revenue goes from
₹11,35,628.80 to ₹11,31,435.10 on the same slice. Finance usually wants **both** views:
the number as it was reported in September (the books were closed on it) and the
corrected number today. ELT already keeps both - the September row survives in
`stg_orders` with `dq_status = 'SUPERSEDED'` and the original is permanent in
`raw_orders`. For the ETL side I would add an SCD Type 2 fact: keep
`valid_from`/`valid_to`/`is_current` per `order_id`, so "revenue as at 30 September" and
"revenue as at today" are two different `WHERE` clauses on the same table.

---

## 7. Part F - Observability

### Run audit

Both DAGs end with `write_run_audit`, which uses `trigger_rule="all_done"` so the row is
written even when an upstream task failed. `pipeline_runs` holds `run_id`, `dag_id`,
`start_time`, `end_time`, `duration_seconds`, `rows_extracted`, `rows_loaded`,
`rows_quarantined`, `status` and `notes`.

One limit of this design, seen in the screenshots: because the audit task runs even after a
failure, the Airflow *run* can end green while a task inside it is red. The failure shows on
the task and as a `FAILED` row in `pipeline_runs`, but not in the run state; alerting should
read `pipeline_runs`, not the run colour.

### The ten experiments

Full detail, including the raw numbers for each one:
[`docs/evidence/observability_results.md`](docs/evidence/observability_results.md) and
[`observability_experiments.json`](docs/evidence/observability_experiments.json),
produced by `scripts/run_observability_experiments.py` on both pipelines.

| # | Experiment | Expected | Observed | Where I saw it | What I would change |
| --- | --- | --- | --- | --- | --- |
| 1 | Happy path | Both DAGs succeed, counts recorded | ETL 0.18 s, 2,310 fact rows, 152 quarantined; ELT 0.23 s, 2,310 PASS rows; identical revenue | `pipeline_runs`, `dq_results`, task logs | publish per-file counts as a metric |
| 2 | Re-run (idempotency) | Totals unchanged | ETL 0 inserted / 2,310 updated; ELT MERGE 0 inserted; `raw_orders` grew 2,412 → 4,824 **by design** (append-only audit) | row counts, MERGE log line | fail the DAG if a pure re-run changes the fact count |
| 3 | Incremental load | 150 new, 10 corrected, no duplicates | ELT MERGE: **150 inserted, 10 updated**; ETL upsert: 150 inserted; 2,460 rows / 2,460 distinct ids; the 10 orders are now RETURNED with zero revenue | MERGE log, `stg_orders` SUPERSEDED rows | keep an SCD Type 2 history in the ETL fact too |
| 4 | Missing source file (`products.csv` renamed) | Loud failure, no partial load | ETL failed in `read_source_csv()` with `FileNotFoundError`; ELT failed in `load_raw()`; both wrote a FAILED audit row. In Airflow the task retries twice, 2 minutes apart, then `failed`, downstream `upstream_failed` | task log, `pipeline_runs.status`, Grid view | add a "five files present" sensor before the extracts |
| 5 | New bad data (5 planted rows) | Each lands in quarantine with the right reason | All 5 caught by **both** pipelines: RULE-006 null customer, RULE-009 negative quantity, RULE-008 unknown store, RULE-012 status `SHIPPED`, RULE-005 2027 timestamp; none reached `etl_fact_orders`; all recorded in `dq_results` | quarantine CSV, `etl_quarantine`, `stg_orders.failure_reason` | add these planted rows as a CI smoke test |
| 6 | Schema change (`order_amount` → `total`) | Fail loudly, not silently | ETL raised `SchemaError: orders: missing expected column(s) ['order_amount']`; ELT raised DuckDB `BinderException: Referenced column "order_amount" not found`; **nothing loaded** | task log, FAILED audit row | publish the column list as a contract and alert the source team |
| 7 | Volume anomaly (50 orders only) | Something warns me | RULE-034 fired in both: `expected~2412 rows, received 50 rows`, status WARN, run still succeeded | `dq_results` where `rule_id='RULE-034'` | raise to Critical once a minimum volume is agreed, and push to Slack |
| 8 | Distribution drift (`unit_price` x 100 for S01) | Caught before the mart | Variant A (as instructed): RULE-011 failed for 176 rows, every affected order quarantined, S01 kept 0 fact rows. Variant B (amount scaled consistently): **no rule fired**, S01 revenue moved ₹76,552 → ₹76,55,235 (100x) and loaded cleanly | `dq_results` RULE-011, `mart_daily_store_kpis` | add a statistical check comparing today's average order value per store with the trailing 7-day average |
| 9 | Freshness (newest order 30 days old) | Timeliness check warns | RULE-035 returned WARN in both: `latest order_ts = ..., 30.0 day(s) old` | `dq_results` where `rule_id='RULE-035'` | make it Critical beyond 2 days - a stale 9 AM number is worse than none |
| 10 | Recovery (fix cause, Clear task) | Only the failed and downstream tasks re-run | With `stores.csv` missing both failed and wrote FAILED rows; after restoring it both succeeded with exactly the clean-run numbers (2,310 / 2,310) because the loads are idempotent. In the UI, Clear re-runs the cleared task and everything downstream; the successful extracts are not repeated | `pipeline_runs` FAILED then SUCCESS, Grid view | keep quarantine and audit keyed by `run_id` so a bad day can be replayed without deleting evidence |

> Experiments 4 and 10 include one Airflow-UI-only element (retry states and the Clear
> button). Those are evidenced by screenshots in `docs/screenshots/` where captured;
> everything else above was executed and recorded automatically.

### The five pillars of data observability

| Pillar | How this pipeline covers it | Strength |
| --- | --- | --- |
| **Freshness** | RULE-035 compares the newest `order_ts` with run time, in both pipelines; future-dated rows are excluded so they cannot hide staleness | Good (warning only) |
| **Volume** | RULE-034 and friends compare each file with a baseline row count; `pipeline_runs.rows_extracted` gives the trend | Good |
| **Schema** | explicit column lists in the raw SQL and `assert_schema()` in Python; a rename stops both pipelines immediately | **Strongest** - proven by experiment 6 |
| **Distribution** | partly: arithmetic rules (RULE-011), range rules (RULE-010, RULE-015) and the SLA metrics catch impossible values | **Weakest** - experiment 8 variant B shows a believable-but-wrong 100x price change passing every rule |
| **Lineage** | `_source_file`, `_source_row_num`, `_run_id`, `_loaded_at` on every raw row; `run_id` on every fact row, quarantine row and `dq_results` row | Good in ELT, partial in ETL |

**Best covered: schema.** **Weakest: distribution** - the next thing I would build is a
7-day rolling baseline per store and category, with a z-score style alert.

---

## 8. Part G - Recommendation to Sameer (CTO)

**Recommendation: adopt the ELT pipeline as NammaMart's primary pipeline, and keep the
ETL quality rules as the gate inside it.**

Both pipelines were run side by side on the same files and produce **identical** numbers:
2,088 delivered orders, ₹11,35,628.80 revenue, ₹2,63,436.93 margin, and the same 202
failing rows across 52 checks. So the choice is not about correctness today; it is about
what happens on the day something goes wrong.

**Auditability.** When finance asks "why is this order not in my total?", the ELT raw
layer answers with the original line of the original file, the run that loaded it, and the
rule that rejected it. The ETL pipeline can only answer that for rows it rejected, because
everything else was transformed on the way in. During the increment test the ELT side kept
both the September DELIVERED version and the October RETURNED version of the same order;
the ETL side kept only the latest. For a company whose finance team currently spends half
a day a week chasing differences, that difference matters more than any performance number.

**Data quality guarantees.** The common objection to ELT - "bad rows are in the warehouse"
- does not apply here. Bad rows are in the *raw* layer, clearly labelled, and every mart
table reads `dq_status = 'PASS'` only. Finance sees exactly the same clean rows as it
would with ETL. The guarantee is enforced in one place (the staging views) rather than
scattered through Python.

**Ease of change.** When ops changes the delivery-time limit from 120 to 90 minutes, ELT
re-computes the whole of history from the raw layer with one SQL rerun - no re-ingestion,
no request to the source systems for old files. With ETL the rejected rows were never
stored in full, so history cannot be recomputed; you only get the new rule from today
onwards. Business rules living in `.sql` files also sit closer to the analysts who own them.

**Performance at 1000x (roughly 2.4 million orders a month).** The ETL pipeline loads every
file into pandas memory at once and processes single-threaded; at that size it needs
chunking or a rewrite. The ELT pipeline pushes the work into DuckDB, which is columnar,
vectorised and can spill to disk, so the same SQL keeps working on one machine and can be
pointed at a larger engine later with minimal change. Today the difference is invisible
(0.20 s vs 0.24 s on 5,212 rows); at 1000x it is the difference between a rewrite and a
configuration change.

**Cost.** Both stacks are free and open source. ELT costs more *storage*, because raw is
append-only (two September runs already doubled `raw_orders` to 4,824 rows); that is the
price of the audit trail and it is the cheapest resource NammaMart buys. ETL costs more
*engineering time*, because every rule change is a Python change plus a re-ingestion
conversation with the source teams.

### Top three risks of the recommended design, and how to control them

1. **Unmasked personal data sits in the raw layer.** `raw_customers` holds real phone
   numbers and emails. Control: put raw in its own database/schema with separate
   credentials, grant it to data engineering and the DPO role only, expose masked views to
   everyone else, and set a 24-month retention with automatic deletion.
2. **Business logic spreads across SQL files and can drift silently.** A wrong `WHERE`
   clause in a mart file produces a plausible but wrong number. Control: the rule
   catalogue stays the single source of truth for severity and dimension, the
   ETL-vs-ELT comparison test runs in CI on every pull request, and every mart file is
   reviewed by the owner of the number it produces.
3. **Raw growth and unbounded re-loads.** Append-only tables grow every run and a
   re-processing job can multiply them. Control: partition raw by load date, enforce the
   retention policy, keep the MERGE-based staging (which is idempotent regardless of how
   many times raw is appended), and alert on `pipeline_runs.rows_extracted` moving
   outside its expected band.

**Practical next step:** retire the ETL DAG as a production job but keep it as the
reference implementation that proves the SQL is right - it is the cheapest regression test
NammaMart will ever have.

---

## 9. Six business questions

Full SQL and results for **both** pipelines:
[`docs/evidence/business_questions.md`](docs/evidence/business_questions.md)
(regenerate with `python scripts/run_business_questions.py`).
SQL files: `include/sql/business_questions/q1..q6_{etl,elt}.sql`.

| # | Question | ETL source | ELT source | Headline answer |
| --- | --- | --- | --- | --- |
| 1 | Daily net revenue by zone and store | `etl_fact_orders` | `mart_daily_store_kpis` | best day/store: S02 Yelahanka, 19 Sep, ₹9,732.00 from 13 delivered orders |
| 2 | Revenue and gross margin by category | `etl_category_margin` | `mart_category_margin` | Staples ₹2,66,500.90 revenue / 22.04% margin; Dairy & Bakery has the best margin at 26.42% |
| 3 | Average delivery time and % within 15 minutes | `etl_fact_orders` | `mart_delivery_sla` | per store, 15 stores reported |
| 4 | Revenue by payment mode and unreconciled total | `etl_fact_orders` + `etl_payment_reconciliation` | `stg_orders` + `mart_payment_reconciliation` | UPI ₹6,20,387.85 leads; **₹1,31,331.05 across 180 rows does not reconcile** |
| 5 | Top 10 customers by spend with loyalty tier | `etl_dim_customer` | `mart_top_customers` | masked names/emails only, no raw PII |
| 6 | Rows rejected today by file and reason | `etl_quarantine` | `elt_rule_failures` | orders 102, payments 42, customers 5, products 3 |

Note on question 4: `Cashh` still appears as a payment mode with ₹666.00 of revenue. That
is intentional - a misspelled payment mode is a **Warning** (RULE-013), so the money is
not lost from the total, but the problem is visible in `dq_results` and in this report.

---

## 10. Evidence index

| Evidence | Location | Status |
| --- | --- | --- |
| Both DAGs parse with no import errors | `tests/nammamart/test_dag_structure.py` (real Airflow 3.3 DagBag) | automated |
| Quality scorecard | `dq_results` table, snapshot in `docs/evidence/dq_results_snapshot.csv` | automated |
| Run audit | `pipeline_runs` table, snapshot in `docs/evidence/pipeline_runs_snapshot.csv` | automated |
| Quarantine with reasons | `docs/evidence/sample_quarantine_*.csv` (masked samples; the full folder and the warehouse file are gitignored because the raw layer holds unmasked personal data - rebuild with the two runner scripts) | automated |
| Payment reconciliation | `etl_payment_reconciliation`, `mart_payment_reconciliation`, sample in `docs/evidence/payment_reconciliation_sample.csv` | automated |
| Six business questions | `docs/evidence/business_questions.md` | automated |
| Ten observability experiments | `docs/evidence/observability_results.md` | automated |
| Test run | `docs/TEST_REPORT.md` | automated |
| Airflow UI screenshots | `docs/screenshots/` - `s1`/`s2` graphs, `s3` schedule, `s4` quarantine file, `s5` `dq_results`, `s6` `pipeline_runs`, `s7` reconciliation, `s8` MERGE log, `s9a`/`s9b` retries, `s10` Clear | captured from Astro on Podman; Slack delivery not verified (no webhook) |

How the evidence was produced: the pipelines run identically from the local scripts and
inside Airflow. Results are independent of the day they are run, because "future-dated"
means after a fixed as-of date (`NAMMAMART_AS_OF`, default `2026-10-02`) rather than after
`now()`. The setup notes for running the DAGs are in `HOW_TO_RUN.md`; the limits of the
evidence are listed in `docs/TEST_REPORT.md` section 4.
