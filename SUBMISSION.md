# NammaMart ETL & ELT pipelines - submission

FDAIE Module 2 · Dinesh · cohort 4 (Epiroc)

Repo layout of what I added:

```
dags/nammamart_etl_dinesh.py        Part A - ETL DAG
dags/nammamart_elt_dinesh.py        Part B - ELT DAG
include/nammamart/                  the actual logic (config, quality, transforms,
                                    warehouse, etl, elt, run_pipelines,
                                    business_questions, observability_experiments)
include/profiling/                  Part 0 profiler + profile_report.md + data_issues_log.csv
include/sql/raw/                    raw loader
include/sql/staging/                staging models, macros, rule catalogue, dq_results, quarantine
include/sql/mart/                   star schema + the five mart tables + both MERGEs
include/sql/business_questions.sql  the six business questions, twice
include/quarantine/dinesh/          quarantined rows with failure_reason
screenshots/                        Airflow UI evidence + query output
(include/warehouse_dinesh.duckdb is gitignored - see Part E7)
```

Two things worth saying up front.

The DAG files are deliberately thin. Everything that does real work sits in
`include/nammamart/` as plain functions, so I can run the whole pipeline from a
terminal without waiting for Airflow:

```bash
python -m nammamart.run_pipelines --pipeline both --increment --reset
```

Every number in this document came out of that command or out of
`python -m nammamart.business_questions`, not from me typing numbers into a table.

And the headline result: **both pipelines land on exactly the same numbers.**
2,460 orders in the fact table, 1,211,087.30 net revenue, 284,875.31 gross margin,
0 orders present in one pipeline and missing from the other. That took three attempts
to achieve and the reasons it was wrong the first two times are in Part B4.

Both DAGs also run green in Airflow itself (Astro Runtime 3.3 / Airflow 3), on the
9:00 AM IST schedule, with the run audit and quality tables filled in. Getting them
there found four more bugs that the terminal run could not - those are written up in
"What only showed up once it ran in Airflow" at the end of Part B.

---

## Part 0: Discovery and data profiling

Profiler: `include/profiling/profile_nammamart.py`. Run it with
`python include/profiling/profile_nammamart.py`; it writes `profile_report.md`
(row counts, nulls, duplicate keys, numeric and date ranges, every distinct category
value) and `data_issues_log.csv`.

I found **34 distinct issues across all six files**. Nothing in here was guessed -
every row names the keys it affects.

### Data Issues Log

| File | Row key | Column | Problem | Dimension | Proposed rule | Severity | Rows |
| --- | --- | --- | --- | --- | --- | --- | --- |
| orders.csv | ORD000362 | order_ts | 1 row with no order_ts | Completeness | order_ts must be present | Warning | 1 |
| orders.csv | ORD002255, ORD000701 +16 | customer_id | 18 rows with no customer_id | Completeness | customer_id must be present | Critical | 18 |
| orders.csv | ORD000682, ORD000910 +162 | delivery_minutes | 164 rows with no delivery_minutes | Completeness | required only when DELIVERED | Warning | 164 |
| orders.csv | ORD001554, ORD001800 +6 | delivery_minutes | 8 DELIVERED orders with no delivery time | Completeness | DELIVERED must carry delivery_minutes | Critical | 8 |
| orders.csv | ORD001462, ORD001326 +10 | order_id | 12 duplicated keys over 24 rows | Uniqueness | order_id unique, keep the last row | Critical | 24 |
| orders.csv | ORD001066, ORD000500 +3 | order_status | 5 rows outside the allowed list: `Pending??`, `delivered` | Validity | must be DELIVERED / CANCELLED / RETURNED | Critical | 5 |
| orders.csv | ORD000403, ORD001610 +3 | order_ts | 5 timestamps that do not parse: `N/A`, `2026-13-05 11:00:00`, `31/09/2026 10:00` | Validity | order_ts must parse | Critical | 5 |
| orders.csv | ORD002103, ORD001468, ORD001237 | discount_pct | 3 rows with discount outside 0-100 (max 250%) | Validity | discount_pct between 0 and 100 | Critical | 3 |
| orders.csv | ORD001812, ORD001414, ORD001640 | payment_mode | unmapped modes: `Cashh`, `cod` | Validity | must map to the allowed list | Critical | 3 |
| orders.csv | ORD000159, ORD001364 +5 | payment_mode | casing/spacing variants: `upi`, `CARD`, `UPI ` | Consistency | trim and map onto the allowed list | Warning | 7 |
| orders.csv | ORD000469, ORD001296 +8 | quantity | 10 rows with quantity <= 0 (min -3) | Accuracy | quantity > 0, negatives need a client ruling | Critical | 10 |
| orders.csv | ORD002103, ORD002305 +16 | order_amount | 18 rows where amount != qty x price x (1 - disc), max gap 8,618.40 | Accuracy | recompute and quarantine mismatches | Critical | 18 |
| orders.csv | ORD001037, ORD002101 +4 | delivery_minutes | 6 rows above 120 minutes (max 1,440) | Accuracy | delivery_minutes <= 120 | Critical | 6 |
| orders.csv | ORD002119, ORD001693 +5 | order_ts | 7 orders dated after the export window (max 2027-01-01) | Timeliness | reject future-dated orders | Critical | 7 |
| orders.csv | ORD000986, ORD001058 +7 | product_id | 9 orders pointing at a product_id that does not exist: P9455, P9212, P9816 | Consistency | product_id must exist in products.csv | Critical | 9 |
| orders.csv | ORD001799, ORD001046 +3 | store_id | 5 orders pointing at a store_id that does not exist: S99, S17, S00 | Consistency | store_id must exist in stores.csv | Critical | 5 |
| orders.csv | ORD000398, ORD001113 +70 | order_id | 72 delivered orders with no successful payment | Consistency | flag on the reconciliation table | Warning | 72 |
| payments.csv | PAY0001651, PAY0000755 +2 | payment_id | 4 duplicated keys over 8 rows | Uniqueness | payment_id unique, keep the last row | Critical | 8 |
| payments.csv | PAY0001568, PAY0002175 +4 | amount_paid | 6 payments with a negative amount | Accuracy | amount_paid >= 0 | Critical | 6 |
| payments.csv | PAY0000133, PAY0001514 +299 | gateway_ref | 301 rows with no gateway reference | Completeness | warn and load | Warning | 301 |
| payments.csv | PAY0002219, PAY0002220 +6 | order_id | 8 payments with no matching order | Consistency | must reference a known order | Critical | 8 |
| payments.csv | PAY0001568, PAY0000354 +12 | amount_paid | 14 SUCCESS payments differ from order_amount (net -785.00) | Consistency | write every mismatch to a reconciliation table | Critical | 14 |
| customers.csv | C00103, C00252 +3 | customer_id | 5 duplicated keys over 10 rows | Uniqueness | customer_id unique, keep the last row | Critical | 10 |
| customers.csv | C00400, C00398, C00038 | signup_date | 3 customers signed up in the future (max 2027-01-05) | Timeliness | reject future signup dates | Critical | 3 |
| customers.csv | (all rows) | loyalty_tier | mixed casing, 7 variants of 4 tiers | Consistency | upper-case before load | Warning | 405 |
| customers.csv | C00337, C00307 +10 | email | 12 malformed addresses: `N/A`, `arjun.rao307example.com`, `prakash.pillai63@example` | Validity | warn and load, mask regardless | Warning | 12 |
| customers.csv | C00151, C00240 +5 | phone | 7 numbers that are not 10 digits | Validity | warn and load, mask regardless | Warning | 7 |
| customers.csv | C00355, C00394 +4 | city | 6 rows with no city | Completeness | warn and load | Warning | 6 |
| products.csv | P0030, P0140 | product_id | 2 duplicated keys over 4 rows | Uniqueness | product_id unique, keep the last row | Critical | 4 |
| products.csv | P0019, P0073 +3 | cost_price | 5 products where cost_price > mrp | Accuracy | cost_price <= mrp | Critical | 5 |
| products.csv | P0109 | mrp | 1 product with mrp <= 0 | Accuracy | mrp > 0 | Critical | 1 |
| products.csv | P0051, P0060, P0068 | category | 3 rows with no category | Completeness | warn and load | Warning | 3 |
| stores.csv | S07 | zone | 1 row with no zone | Completeness | warn and load, chase the store master owner | Warning | 1 |
| orders_2026-10-01_increment.csv | ORD000017, ORD000271 +8 | order_status | 10 late status corrections to September orders | Timeliness | MERGE on order_id so corrections overwrite | Critical | 10 |

Rolled up by dimension and severity (from `profile_report.md`):

| Dimension | Critical rows | Warning rows |
| --- | --- | --- |
| Accuracy | 46 | - |
| Completeness | 26 | 476 |
| Consistency | 36 | 484 |
| Timeliness | 20 | - |
| Uniqueness | 46 | - |
| Validity | 16 | 19 |

A few notes on things that are not obvious from the table:

- The 164 missing `delivery_minutes` look alarming until you notice almost all of them
  are CANCELLED orders, which never got delivered. Only **8** are DELIVERED with no
  delivery time, and those are the real problem. Treating all 164 as Critical would
  have thrown away every cancellation.
- S07 (BTM Layout) has no zone. It is one row in the store master, but it silently
  creates a `NULL` zone bucket worth 85,382.00 of revenue in the zone report. One blank
  cell in a 15-row file moves a number the COO reads every morning.
- `gateway_ref` is missing on 301 of 2,228 payments - 13.5%. I made that a Warning, not
  Critical. If it were Critical I would quarantine 13% of the payments and the
  reconciliation would be meaningless. But 13% is high enough that somebody should ask
  the payment gateway team about it.

### Three questions I would ask the client before building

1. Negative quantity: is that a return booked against the original order, or a
   data-entry error? It changes whether we quarantine the row or net it off revenue.
   Right now I quarantine, which is the safe option, but it is a guess.
2. When a SUCCESS payment is larger than the order amount, which number is the truth
   for the CFO's revenue - the order or the gateway? I have assumed the order and
   reported the gap, but finance has to pick.
3. An order delivered in September and returned on 1 October: should September revenue
   be restated, or should the return land in October? Finance and ops usually want
   opposite answers. See Part E9 - my pipeline currently restates September.

---

## Part A: the ETL pipeline

`dags/nammamart_etl_dinesh.py`. Flow: Extract -> Validate -> Quarantine -> Transform
-> Load -> Reconcile -> Report, plus a run audit that always runs.

| Step | Tasks | What it does |
| --- | --- | --- |
| Extract | `extract_orders`, `extract_payments`, `extract_customers`, `extract_products`, `extract_stores` | five parallel tasks, one per file, everything read as text |
| Validate | `validate_<file>` x5 | applies the Part D rules, splits each file into passed / failed |
| Quarantine | inside validate | failed rows written to `include/quarantine/dinesh/<file>_failed_<run_id>.csv` with `failure_reason` |
| Quality | `record_validation_dq`, `record_transform_dq` | writes `dq_results` |
| Transform | `transform` | standardise, dedupe, mask PII, join to the dimensions, derive the reporting columns |
| Load | `load_star` | `etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`, `etl_payments` |
| Increment | `load_increment` | the 1 Oct file, upsert on `order_id` |
| Reconcile | `reconcile` | `etl_payment_reconciliation`, one row per mismatch with a reason |
| Report | `report` | `etl_daily_zone_revenue` and `etl_category_margin`, printed in the log |
| Audit | `audit_run` (`trigger_rule="all_done"`) | one row in `pipeline_runs`, even on failure |

Results of a full run:

| Table | Rows |
| --- | --- |
| `etl_fact_orders` | 2,460 (2,310 September + 150 from the increment) |
| `etl_dim_customer` | 397 |
| `etl_dim_product` | 145 |
| `etl_dim_store` | 15 |
| `etl_payments` | 2,218 |
| `etl_payment_reconciliation` | 321 |

127 rows quarantined on the full load, in five CSVs, each with `failure_reason`,
`quarantined_at` and `run_id` columns. The copies in `include/quarantine/dinesh/`
are from the scheduled 9:00 AM IST run, so the filenames carry that run_id.

Three things in here I had to think about rather than just code:

**Masking happens in transform, before anything is written.** `full_name`, `email` and
`phone` are dropped from the dataframe in `standardise_customers`, not nulled out
later. The raw columns are physically not present in anything the warehouse sees. Same
for `manager_name` on stores, which the brief does not mention but is still somebody's
name.

**Quarantining a dimension row must not delete fact rows.** This one cost me an
evening. Six products and five customers fail their own quality checks. My first
version checked `orders.product_id` against the *cleaned* product dimension, so those
six bad products took about 100 perfectly good orders down with them - roughly 50,000
of revenue silently vanished, and the pipeline reported success. The fix is in
`etl.transform`: the key sets for the referential check are built from the **full**
master file, passed rows and failed rows concatenated. A product with a bad cost price
is still a real product. The order keeps its revenue, its margin is null, and the row
carries `Warning: product master row quarantined, margin unknown` so nobody has to
guess why. Orphan count went from 100 to 14, which matches the 9 product orphans and 5
store orphans the profiler found.

**Idempotency is `CREATE OR REPLACE`, not `INSERT`.** The September exports are a
complete snapshot, so replacing is the honest pattern. The increment is the only thing
that upserts.

---

## Part B: the ELT pipeline

`dags/nammamart_elt_dinesh.py`. Every transform is a `.sql` file; there is no pandas
anywhere in this pipeline. `include/nammamart/elt.py` reads a file, substitutes the run
details and runs it.

| Layer | Files | Tables |
| --- | --- | --- |
| raw | `include/sql/raw/load_raw.sql` | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` - every column VARCHAR, plus `_loaded_at`, `_source_file`, `_run_id` |
| staging | `macros.sql`, `v_stg_orders.sql`, `stg_orders.sql`, `stg_payments.sql`, `stg_customers.sql`, `stg_products.sql`, `stg_stores.sql`, `merge_stg_orders.sql`, `dq_rule_catalog.sql`, `record_dq_results.sql`, `elt_quarantine.sql` | `stg_*` with `dq_status` and `failure_reason` on every row |
| mart | `mart_dim_*.sql`, `v_fact_orders.sql`, `mart_fact_orders.sql`, `mart_daily_store_kpis.sql`, `mart_category_margin.sql`, `mart_delivery_sla.sql`, `mart_payment_reconciliation.sql`, `mart_top_customers.sql`, `merge_orders_increment.sql` | the five mart tables from the brief plus the star schema |

A few design decisions worth defending:

**Nothing is deleted in staging.** All 2,412 raw order rows come through into
`stg_orders`; 2,310 get `dq_status = 'PASSED'` and 102 get `'REJECTED'` with the reason
attached. The mart filters on `dq_status`. So a rejected row is one `WHERE` clause away
rather than in a CSV somebody has to find.

**Two views so the full load and the MERGE cannot drift.** `v_stg_orders` holds the
whole staging projection and `stg_orders.sql` is just `SELECT * FROM v_stg_orders`.
`v_fact_orders` does the same for the fact. The incremental MERGEs read the same views
filtered by `_run_id`. If I had copy-pasted the projection into the MERGE statements,
the next rule change would have been applied in one place and not the other, and the
two would have quietly diverged.

**PII is masked in staging, in SQL, with macros.** DuckDB has no `initcap` and no
masking functions, so `include/sql/staging/macros.sql` defines `title_case`,
`mask_name`, `mask_email`, `mask_phone` and `standard_payment_mode`. They mirror
`transforms.py` exactly, which matters because the two customer dimensions are compared
row for row in Part G.

**The five raw loads are chained, not parallel.** This looks like a mistake in the
Graph view and it is not. DuckDB allows one writing process at a time and Airflow gives
each task its own process, so five parallel tasks writing to the same `.duckdb` file
fail on the file lock. The ETL DAG keeps its five extracts parallel because they only
read CSVs. On a real warehouse (Postgres, Snowflake, BigQuery) this restriction
disappears and the loads would run in parallel - it is a property of my local
warehouse, not of the design.

**A rule catalogue, so a rule nobody registered is visible.** The staging models write
the reason text onto the row; `dq_rule_catalog` maps each reason to a `check_name`, a
dimension and a severity; `record_dq_results.sql` rolls the rows up into the same
`dq_results` table the ETL pipeline writes to. A reason that is not in the catalogue
appears with `dimension = 'UNCATALOGUED'`, which is the signal that somebody added a
rule in SQL and forgot to register it. Current count: 0.

### Part B3: the incremental load

The 1 Oct file is 160 rows: 150 new orders and 10 September orders whose status changed
to RETURNED.

| Layer | What happens | Result |
| --- | --- | --- |
| raw | appended under `_source_file = 'orders_2026-10-01_increment.csv'`; the September rows are untouched | `raw_orders` 2,412 -> 2,572 |
| staging | `MERGE INTO stg_orders ... ON order_id` | **10 updated, 150 inserted** |
| mart | `MERGE INTO mart_fact_orders ... ON order_id` | **10 updated, 150 inserted**, fact 2,310 -> 2,460 |

Both MERGEs use `RETURNING merge_action`, so the update/insert split is printed in the
task log rather than inferred from row counts. Duplicate `order_id`s in the mart after
the merge: 0. September orders with status RETURNED went from 68 to 78, which is the 10
corrections landing on the original rows.

Running the whole thing twice gives `160 updated, 0 inserted` and the fact table stays
at 2,460.

### Part B4: do the ELT totals match the ETL totals?

Yes, exactly - but only after I fixed two real bugs. The `compare_with_etl` task runs
on every ELT run and fails loudly in the log if they diverge.

| Measure | ETL | ELT | Difference |
| --- | --- | --- | --- |
| fact rows | 2,460 | 2,460 | 0 |
| net revenue | 1,211,087.30 | 1,211,087.30 | 0.00 |
| gross margin | 284,875.31 | 284,875.31 | 0.00 |
| orders only in one side | 0 | 0 | - |
| reconciliation rows | 321 | 321 | 0 |

Per zone, from `q1_totals`:

| Zone | ETL revenue | ELT revenue | Difference |
| --- | --- | --- | --- |
| East | 314,056.20 | 314,056.20 | 0.00 |
| West | 312,413.00 | 312,413.00 | 0.00 |
| South | 256,214.55 | 256,214.55 | 0.00 |
| North | 243,021.55 | 243,021.55 | 0.00 |
| *(no zone - S07)* | 85,382.00 | 85,382.00 | 0.00 |

The two bugs the comparison caught, which I would not have found otherwise:

1. **11 increment orders missing from the ETL side.** ETL inserted 139 rows where ELT
   inserted 150. `etl.load_increment` was checking the increment's keys against the
   *dimension tables* (cleaned rows only) instead of the full master files - the same
   cascade bug as Part A, just in the incremental path where I had not thought to look.
2. **The SLA percentage disagreed by about half a point per store.**
   `mart_daily_store_kpis` was counting cancelled and returned orders in the
   denominator of `pct_within_15_min`. A cancelled order was never delivered, so it
   cannot breach a delivery promise. Fixed to delivered orders only.

There is one difference that is not a bug and I want to be explicit about it: the
reconciliation table has 321 rows, of which 150 are increment orders flagged as
"delivered order with no successful payment". That is correct - the increment is an
*orders* export and the payments for those orders will arrive in the next *payments*
export. It is still noise for finance, and the fix is to ignore orders newer than the
latest payment timestamp rather than flag them. I have not done that because it changes
what "unreconciled" means and that is the client's call, not mine.

### What only showed up once it ran in Airflow

Everything above worked from a terminal before I ever started Airflow. Then I ran
`astro dev start` and found five things, four of them real bugs. Worth writing down,
because the pattern is the same every time: the environment differed, and the
difference was something I had not pinned.

The Astro image is Python 3.14 with **duckdb 2.0.0**; my terminal is Python 3.9 with
**duckdb 1.4.5**. `requirements.txt` said just `duckdb`, so the two drifted. It is
pinned now.

1. **`.fetchone()` left the connection busy.** duckdb 2.0 refuses further work on a
   connection with an unfinished result, and `fetchone()` on a one-row result leaves it
   unfinished. The first `load_star` table loaded, the second raised
   `ResourceInUseException`. Fixed with `warehouse.one()` / `warehouse.scalar()`, which
   drain the result with `fetchall()`. On 1.4.5 the old code was fine, which is exactly
   why I did not catch it.
2. **The whole warehouse was VARCHAR.** Airflow 3 serialises XCom as JSON, so the tasks
   hand each other file paths instead of DataFrames. Reading those CSVs back with
   `dtype=str` meant every column landed in DuckDB as text, and `load_increment` then
   tried to subtract a string from a float. The stashed CSVs now go to DuckDB directly
   (`warehouse.load_csv`) and DuckDB's sniffer types them. The terminal run never hit
   this because it passes DataFrames in memory.
3. **The staging MERGE key was not unique.** `stg_orders` deliberately keeps rejected
   rows, so a duplicated `order_id` is two rows - one PASSED, one REJECTED. One
   increment row matched both and flipped the rejected copy to PASSED, putting the same
   order in the fact table twice. duckdb 2.0 refused the next MERGE with "a target row
   matched more than one source row"; 1.4.5 had quietly allowed it. The ON condition
   now includes `target.dq_status = 'PASSED'`.
4. **The DAG went green while a task was red.** This is the one that would have
   embarrassed me. `audit_run` is the only leaf task and has
   `trigger_rule="all_done"`, so it ran and succeeded after `extract_products` failed -
   and Airflow takes the run's state from its leaves. `pipeline_runs` said FAILED and
   the UI said success at the same time. `audit_run` now re-raises after writing the
   audit row, so the evidence is saved *and* the run goes red.
5. Not a bug, just deprecation: duckdb 2.0 rejects the old `x -> expr` lambda arrow in
   `list_filter`, so the staging models use `lambda x: expr`.

The lesson I am taking from this is narrow and practical: a pipeline that works when
you call the functions yourself has not been tested. Four of these only appear when
each task is a separate process, talking through XCom, against the pinned image.


---

## Part C: scheduling

**1. Cron expression:** `0 9 * * 1-6` on both DAGs, with
`start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")` and `catchup=False`.
`1-6` is Monday to Saturday; Sunday is excluded because the stand-up does not happen.

**2. What time does the run show in UTC in the Airflow UI?** 03:30 UTC. IST is UTC+5:30,
so 09:00 IST is 03:30 UTC the same day. Airflow stores and compares everything in UTC
internally and converts for display. With the UI timezone set to +05:30 the DAGs list
shows `Next Run 2026-10-27 09:00:00` (`screenshots/schedule_9am_ist.jpg`), and the run
Airflow actually created when I unpaused the DAGs is named
`scheduled__2026-10-26T03:30:00+00:00` - the same instant, written in UTC. Both of those
runs are in `pipeline_runs` with status SUCCESS, which is the clearest proof I have that
the schedule resolves to the right moment.

**3. What would happen at 9 AM if I had left the start_date in UTC?** The cron would be
read as 09:00 UTC, which is 14:30 IST. The numbers would land five and a half hours
after the stand-up - the report would be useless every single day, and nothing would
look broken. No red tasks, no alert, just a DAG that succeeds at the wrong time. This
is the kind of bug that survives for months.

It is not only theory: I left both DAGs unpaused and they fired on their own at 09:00
IST on 26 and 27 October, four scheduled runs in `pipeline_runs`, all SUCCESS with the
same 2,460 rows. That is the acceptance criterion actually demonstrated rather than
argued.

**4. What happens if my laptop or Docker is off at 9 AM?** Nothing runs. There is no
scheduler process, so the interval is simply missed. And because `catchup=False`, when
I start Docker at 11 AM Airflow does **not** backfill the missed 9 AM run - it only
schedules the next one. So the morning is just gone, and the only evidence is a gap in
`pipeline_runs`. Running this on a laptop is not a production setup; it needs a
scheduler that is always on (a small VM, MWAA, Astro, Cloud Composer) and an alert if
no run has been recorded by 09:15 IST.

**One thing the brief does not ask about but I would raise with the client:** both DAGs
are scheduled at exactly 09:00 and both write to the same DuckDB file, which allows one
writer at a time. On a laptop they will collide. I left the schedules as specified, and
in production I would either merge them into one DAG or make the ELT DAG wait on an
Airflow asset produced by the ETL DAG, rather than hoping the two never overlap.

**Why `catchup=False`:** the DAG reads whatever is currently in
`include/data/nammamart/`, not a date-partitioned file. If I turned catchup on, Airflow
would queue one run per missed day since the start_date and each one would reprocess
*today's* files, overwriting the mart several times with identical numbers and filling
`dq_results` with duplicate rows for different logical dates. The runs would all
succeed and all be meaningless. Backfill only makes sense when the task reads the data
for its own logical date, which mine does not. `max_active_runs=1` is set for the same
reason - two concurrent runs writing to one DuckDB file would collide on the lock.

**Retries:** `retries=2`, `retry_delay=timedelta(minutes=2)`,
`execution_timeout=timedelta(minutes=15)` in `default_args`. The one exception is
`audit_run`, which has `retries=0` - it fails deliberately when something upstream
broke, and retrying that just burns four minutes before the run is marked failed.

Two retries two minutes apart covers what actually goes wrong at 9 AM: the export file
is still being written when the task starts, or the warehouse file is briefly locked.
Both clear in seconds. What two retries does *not* fix is a missing file or a schema
change - those fail three times and burn four minutes doing it, which is fine, because
the alternative is a report built on four files out of five. The 15-minute timeout is
generous: this dataset takes under a second. Anything that runs for 15 minutes here is
stuck on a lock, not working hard, and I would rather be told at 09:15 than find the
DAG still running at 09:40.

---

## Part D: data quality dimensions

Python in the ETL pipeline (`include/nammamart/quality.py`), SQL in the ELT pipeline
(`include/sql/staging/*.sql`). 40 rules in the Python set and 41 in the SQL rule
catalogue, plus 2 batch-level checks on each side, covering all six dimensions across
all five files. Critical rows are quarantined; Warning rows load and are recorded.

The one rule the ELT has and the ETL does not is a payment_mode casing warning on
payments. I noticed it because the dimension roll-up below disagreed by 6 rows, which
is exactly the kind of drift the comparison is there to catch.

Numbers are `rows_failed` from `dq_results` for the September full load. Both
pipelines write to the same `dq_results` table and `dag_id` tells them apart.

| Dimension | Check | Severity | Rows failing |
| --- | --- | --- | --- |
| Completeness | `orders.customer_id` present | Critical | 18 |
| Completeness | DELIVERED order has `delivery_minutes` | Critical | 8 |
| Completeness | `orders.order_id` present | Critical | 0 |
| Completeness | `payments.gateway_ref` present | Warning | 301 |
| Completeness | `customers.city` present | Warning | 6 |
| Completeness | `products.category` present | Warning | 3 |
| Completeness | `stores.zone` present | Warning | 1 |
| Uniqueness | `order_id` unique, last row wins | Critical | 12 |
| Uniqueness | `payment_id` unique | Critical | 4 |
| Uniqueness | one row per `customer_id` | Critical | 5 |
| Uniqueness | one row per `product_id` | Critical | 2 |
| Uniqueness | one row per `store_id` | Critical | 0 |
| Validity | `order_status` in the allowed list | Critical | 3 |
| Validity | `order_ts` parses as a timestamp | Critical | 6 |
| Validity | `discount_pct` between 0 and 100 | Critical | 3 |
| Validity | `payment_mode` maps to an allowed mode | Critical | 0 |
| Validity | email format | Warning | 12 |
| Validity | phone is 10 digits | Warning | 7 |
| Validity | pincode is 6 digits | Warning | 3 |
| Accuracy | `quantity > 0` | Critical | 10 |
| Accuracy | `order_amount` matches the formula | Critical | 18 |
| Accuracy | `delivery_minutes <= 120` | Critical | 6 |
| Accuracy | `mrp > 0` | Critical | 1 |
| Accuracy | `cost_price <= mrp` | Critical | 5 |
| Accuracy | `amount_paid >= 0` | Critical | 6 |
| Accuracy | delivery outside the normal 8-35 minute band | Warning | 0 |
| Accuracy | `capacity_orders_per_day > 0` | Warning | 1 |
| Consistency | `customer_id` exists in customers | Critical | 0 |
| Consistency | `product_id` exists in products | Critical | 9 |
| Consistency | `store_id` exists in stores | Critical | 5 |
| Consistency | `payment_mode` casing on orders | Warning | 10 |
| Consistency | `loyalty_tier` casing | Warning | 8 |
| Timeliness | no future-dated `order_ts` | Critical | 7 |
| Timeliness | no future-dated `signup_date` | Critical | 3 |
| Timeliness | newest order not more than 1 day old | Warning | 1 (batch) |
| Volume | row count vs the last successful run | Warning | 0 (batch) |

Four of those numbers are not what the profiler reported, and the gaps are the
interesting part:

- **`order_status` fails 3 rows, not the 5 the profiler found.** Two of the five are
  `delivered` in lowercase. Standardising to upper case happens before the check, so
  those two load cleanly. Only the three `Pending??` rows are rejected. The profiler
  was looking at raw text, the rule looks at the standardised value, and the rule is
  the one that matters.
- **`payment_mode` fails 0 rows even though the profiler flagged `Cashh` and `cod`.**
  Both are in `PAYMENT_MODE_MAP`, so they get mapped to `Cash on Delivery` and recorded
  as a Consistency *warning* rather than rejected. Throwing away three real orders
  because somebody typed an extra h would be the wrong trade.
- **`order_ts` fails 6, not 5.** The five unparseable timestamps plus the one row that
  has no `order_ts` at all.
- **The delivery band warning fires on 0 rows.** Every delivery is either inside 35
  minutes or above 120 - there is nothing in between. That is a suspiciously clean
  distribution and makes me think the bad values were injected rather than drifting,
  but either way the check costs nothing to keep.

Roll-up by dimension, both pipelines side by side:

| Dimension | ETL checks | ETL rows failing | ELT checks | ELT rows failing |
| --- | --- | --- | --- | --- |
| Accuracy | 8 | 47 | 8 | 47 |
| Completeness | 12 | 337 | 12 | 337 |
| Consistency | 5 | 32 | 6 | 38 |
| Timeliness | 3 | 11 | 3 | 11 |
| Uniqueness | 5 | 23 | 5 | 23 |
| Validity | 8 | 34 | 8 | 34 |
| Volume | 1 | 0 | 1 | 0 |

`dq_results` has the columns the brief asks for - `run_id`, `dag_id`, `check_name`,
`dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, `checked_at` - plus a
`detail` column holding the reason text, which is what makes it readable without
cross-referencing the code. Both pipelines write to the same table; `dag_id` tells them
apart.

The increment's checks are written under `<run_id>_increment` on both sides. They
shared the run_id at first, which put two sets of `orders_*` rows in `dq_results` for
the same run with no way to tell the full load from the increment apart.

One deliberate choice: a rule that found nothing is still written with `status = 'PASS'`
rather than being left out. "No row for that check" and "that check passed" look
identical in a dashboard and mean completely different things at 9 AM.

And one honest weakness, found in Part F experiment 6: `run_checks` catches a missing
column and records the affected checks with `status = 'ERROR'` instead of crashing. So
on a schema change the amount check was *skipped*, and a dashboard counting only FAILs
would have shown nothing wrong. A check that could not run is not a check that passed.
`ERROR` should fail the task.

---

## Part E: data governance

### 1. Ownership

| Dataset | Owner | Why |
| --- | --- | --- |
| `orders` | Head of Operations | they own the order management system that produces it |
| `payments` | Finance / the payments lead | the gateway is their contract and their reconciliation |
| `customers` | CRM / marketing, with the DPO as joint owner | it is the only file with personal data |
| `products` | Category management | they set mrp and cost_price |
| `stores` | Retail operations | 15 rows, but S07's missing zone moves a KPI |
| `raw_*`, `stg_*`, `mart_*`, `etl_*`, `dq_results`, `pipeline_runs` | me (the data team) | we build them, we are on the hook when the 9 AM number is wrong |

Ownership of a *dataset* is not the same as ownership of a *rule*. A quality rule
encodes a business decision, so changing one needs the business owner's sign-off, not a
pull request approval from another engineer. For the 120-minute delivery limit: ops
proposes the number, finance has to agree because it changes revenue (a quarantined
order is revenue that disappears), and the data team implements it. In practice I would
put the thresholds in `include/nammamart/config.py` - which is where
`DELIVERY_MAX_MINUTES = 120` already lives - and require the ticket to name the person
who approved the change. Right now a developer can change 120 to 600 and the revenue
number moves with nobody noticing. That is a gap.

### 2. Data dictionary

`etl_fact_orders` / `mart_fact_orders` (grain: one clean order line)

| Column | Type | Meaning | Source | Allowed values | PII |
| --- | --- | --- | --- | --- | --- |
| order_id | VARCHAR | order key | orders.csv | unique, not null | no |
| order_ts | TIMESTAMP | when the order was placed | orders.csv | <= end of the data window | no |
| order_date | DATE | `order_ts` truncated, the reporting grain | derived | - | no |
| customer_id | VARCHAR | FK to the customer dimension | orders.csv | must exist in customers.csv | no (it is a pseudonym, but still identifying) |
| store_id, store_name, zone | VARCHAR | FK and attributes from the store master | orders.csv + stores.csv | North / South / East / West, or null for S07 | no |
| product_id, category, sub_category | VARCHAR | FK and attributes from the catalogue | orders.csv + products.csv | 6 categories, null if the product row was rejected | no |
| loyalty_tier | VARCHAR | the customer's tier | customers.csv | BRONZE / SILVER / GOLD / PLATINUM | no |
| quantity | INTEGER | units ordered | orders.csv | > 0 | no |
| unit_price | DOUBLE | price per unit before discount | orders.csv | > 0 | no |
| discount_pct | DOUBLE | discount percentage | orders.csv | 0-100 | no |
| net_revenue | DOUBLE | `quantity x unit_price x (1 - discount_pct/100)`, **recomputed, not taken from the export** | derived | >= 0 | no |
| cost_price | DOUBLE | cost per unit | products.csv | <= mrp, null if the product row was rejected | no |
| gross_margin | DOUBLE | `net_revenue - quantity x cost_price` | derived | null when cost is unknown | no |
| is_revenue | BOOLEAN | true only for DELIVERED | derived | - | no |
| revenue_amount | DOUBLE | `net_revenue` if DELIVERED, else 0 - **this is the column to sum** | derived | >= 0 | no |
| margin_amount | DOUBLE | `gross_margin` if DELIVERED, else 0 | derived | - | no |
| payment_mode | VARCHAR | standardised mode | orders.csv | UPI / Card / Cash on Delivery / Wallet | no |
| order_status | VARCHAR | final status | orders.csv | DELIVERED / CANCELLED / RETURNED | no |
| channel | VARCHAR | App or Web | orders.csv | App / Web | no |
| delivery_minutes | DOUBLE | minutes from order to delivery | orders.csv | <= 120, null for non-delivered | no |
| within_15_min | BOOLEAN | met the 15-minute promise | derived | null when delivery time is unknown | no |
| dq_warnings | VARCHAR | non-blocking quality notes carried with the row | derived | - | no |
| _run_id, _source_file, _loaded_at | VARCHAR / TIMESTAMP | lineage (ELT mart only) | derived | - | no |

`etl_dim_customer` / `mart_dim_customer`

| Column | Type | Meaning | Source | PII |
| --- | --- | --- | --- | --- |
| customer_id | VARCHAR | key | customers.csv | pseudonymous identifier |
| customer_name_masked | VARCHAR | `Priya H.` | masked from full_name | **masked PII** |
| email_masked | VARCHAR | `pr***@example.com` | masked from email | **masked PII** |
| phone_masked | VARCHAR | `97******21` | masked from phone | **masked PII** |
| city, area, pincode | VARCHAR | location | customers.csv | pincode is quasi-identifying at this granularity |
| signup_date | DATE | when they joined | customers.csv | no |
| loyalty_tier | VARCHAR | BRONZE / SILVER / GOLD / PLATINUM | customers.csv | no |
| is_active | BOOLEAN | Y/N normalised to a boolean | customers.csv | no |

`etl_dim_product` / `mart_dim_product`: product_id (key), product_name, category,
sub_category, brand, mrp, cost_price (DOUBLE, `cost_price <= mrp`), unit, is_perishable
(BOOLEAN), launch_date. No PII.

`etl_dim_store` / `mart_dim_store`: store_id (key), store_name, zone (null for S07),
area, pincode, open_date, capacity_orders_per_day, **manager_name_masked** - the brief
does not list the manager as personal data, but it is somebody's name, so it is masked
the same way.

### 3. Lineage

Take one number: `mart_daily_store_kpis` for 2026-09-19, zone North, NammaMart
Yelahanka, `net_revenue = 9,732.00`.

**In ELT, it is four queries and no code.**

```sql
-- the KPI row
SELECT * FROM mart_daily_store_kpis
WHERE order_date = DATE '2026-09-19' AND store_id = 'S02';
-- the orders behind it
SELECT order_id, net_revenue FROM mart_fact_orders
WHERE order_date = DATE '2026-09-19' AND store_id = 'S02' AND is_revenue;
-- the staging rows, with their quality verdict
SELECT order_id, dq_status, failure_reason, _source_file, _loaded_at
FROM stg_orders WHERE order_id IN (...);
-- the bytes as received
SELECT * FROM raw_orders WHERE order_id IN (...);
```

Every row carries `_source_file`, `_run_id` and `_loaded_at`, so the answer includes
*which file and which run* produced it, not just which rows.

**In ETL it is harder.** The fact table has the order ids, so I can get back to
`orders.csv` - but the source rows themselves are not in the warehouse. If
`orders.csv` has been overwritten by the next morning's export, the evidence is gone;
all I have is the quarantine CSVs for the rows that failed. To trace a *passed* row I
have to re-read the file, which assumes it still exists and still says the same thing.

**ELT is clearly easier, and it is the strongest single argument for it.** The reason
is structural rather than about tooling: the ELT pipeline keeps the input, so lineage is
a join. The ETL pipeline keeps only the output, so lineage is an argument.

### 4. Personal data

Personal data in the dataset:

| Column | File | What it is | Treatment |
| --- | --- | --- | --- |
| `full_name` | customers.csv | direct identifier | masked to `Priya H.` |
| `email` | customers.csv | direct identifier + contact | masked to `pr***@example.com` |
| `phone` | customers.csv | direct identifier + contact | masked to `97******21` |
| `manager_name` | stores.csv | employee name | masked to `Arjun H.` |
| `customer_id` | customers, orders | pseudonymous identifier | kept - it is the join key |
| `pincode`, `area`, `city` | customers.csv | quasi-identifiers; pincode + tier + order history narrows to a small group | kept, flagged as a residual risk |

Masking happens in the first transform after raw, in both pipelines, and the raw
columns are not carried forward - they are dropped from the dataframe in
`transforms.standardise_customers` and simply not selected in `stg_customers.sql`. So
the unmasked values exist in exactly two places: the CSV in `include/data/` and
`raw_customers`.

**Why mask rather than delete?** Because support and ops have real jobs that need part
of the value. `pr***@example.com` keeps the domain, so you can still tell a corporate
customer from a consumer one. `97******21` keeps enough to match against a call log
when a customer rings about a missing order. And the masked value is stable, so you can
still group by it and count. Delete the columns and every one of those tasks turns into
a request back to the CRM team, which in practice means someone exports a spreadsheet -
exactly the privacy problem the client already has.

**Which law applies:** India's **Digital Personal Data Protection Act 2023 (DPDP Act)**,
since NammaMart operates in Bengaluru and processes Indian residents' personal data.
What it expects, as it touches this pipeline: a lawful purpose and notice/consent for
the processing; purpose limitation, so customer data collected for delivery is not
quietly reused for something else; data minimisation, which is the direct argument for
masking in staging; reasonable security safeguards; breach notification to the Data
Protection Board and to affected users; erasure on request, which matters here because
`raw_customers` is append-only and an erasure request has to reach it as well as the
mart; and a named Data Protection Officer. I am not a lawyer and would want legal to
confirm the consent and retention positions before this goes near production.

### 5. Access control

| Layer | Who should read it |
| --- | --- |
| `raw_*` | the data engineering team only, and `raw_customers` tighter still - it holds unmasked phone numbers and email addresses |
| `stg_*` | data engineers and analysts debugging a load; already masked, so lower risk |
| `mart_*` / `etl_*` | anyone in the business - that is the point of the layer |
| quarantine (table + CSVs) | data engineers plus the owner of the source file, since they are the ones who have to fix it |
| `dq_results`, `pipeline_runs` | everyone; they are the health dashboard and there is nothing sensitive in them |

**Who should see unmasked `raw_customers`?** Almost nobody. A named service account for
the pipeline, so it can do the masking, and a small break-glass role for the DPO and one
or two engineers for subject-access and erasure requests - with the access logged and
reviewed. Not analysts, not marketing, not the COO. In DuckDB on a laptop none of this
is enforceable, which is worth stating plainly: the warehouse is a file, and anyone with
the file has everything in it. That is an argument for moving raw to a database with real
roles (Postgres with row/column grants, or Snowflake with masking policies) before this
is production.

### 6. Retention

| Data | Keep for | Then |
| --- | --- | --- |
| `raw_orders`, `raw_payments`, `raw_products`, `raw_stores` | 7 years | Indian tax and company law expects financial records to be retrievable for 7 years; after that, delete |
| `raw_customers` | as long as the customer is active, plus 1 year | then delete the row, or strip it to `customer_id` + tier if the order history has to stay joinable |
| quarantine files and table | 90 days | long enough to fix the upstream cause and prove it was fixed; after that they are just copies of rejected records nobody is looking after |
| masked mart tables | 7 years | they contain no direct identifiers, so they can be kept as long as the financial records |
| `dq_results`, `pipeline_runs` | 2 years | you need a year or two of history for the trend to mean anything |

The uncomfortable one is quarantine. Those CSVs sit in `include/quarantine/dinesh/`
on a filesystem, outside the warehouse's backups and access controls, and nothing
deletes them. Writing them was also where I caught my own worst governance mistake: the
quarantine files were the one place in the repo still holding readable names, emails and
phone numbers, because I was writing the raw rejected rows straight out to CSV. They now
go through the same masking as everything else (`transforms.mask_pii_columns`, called
from `warehouse.quarantine`) - none of the rules that reject a customer row need a
readable phone number in order to be fixed. A retention job that deletes files older
than 90 days is still a small piece of work I have not written.

### 7. Secrets

There are no credentials in my pipeline - DuckDB is a local file and there is no Slack
webhook wired up yet. If I add the bonus alert, the webhook goes in an **Airflow
Variable** (`Variable.get("SLACK_WEBHOOK_URL")`) or an Airflow Connection, read inside
the task at runtime, never at module level. Reading it at module level would put the
value in the DAG's parsed representation and in the serialised DAG table.

Why never in the DAG file or on GitHub: git keeps history forever, so a secret committed
once and removed in the next commit is still in the repo and still has to be rotated.
Anyone with read access to the repo - including whoever forks the class repo - has it.
And a webhook needs no password, so possession is authorisation: anyone who finds it can
post into NammaMart's Slack as the pipeline. For the same reason I added `include/tmp/` to
`.gitignore` - the intermediate CSVs the ETL tasks pass between each other land there
and there is no reason to commit them. The quarantine files are committed on purpose,
as the evidence the brief asks for, and they are safe to commit now that they are
masked. `include/warehouse_dinesh.duckdb` is **not** committed, and that is
deliberate: `raw_customers` inside it holds unmasked names, emails and phone numbers,
because keeping every raw record exactly as received is the whole point of the ELT raw
layer - and the submission checklist says no unmasked customer data in the repo outside
`include/data/`. I had it committed at first and took it out. Rebuilding it takes a few
seconds:

```bash
cd include && python -m nammamart.run_pipelines --pipeline both --increment --reset
```

The query output that would have come from it is in `screenshots/query_results.md`.

### 8. Audit: show me a rejected order

Finance picks `ORD001037`.

**ELT - two queries, no code:**

```sql
-- the rule it broke
SELECT record_key, failure_reason, quarantined_at
FROM elt_quarantine WHERE record_key = 'ORD001037';
-- 'Critical: delivery_minutes above 120'

-- exactly as received, untouched
SELECT * FROM raw_orders WHERE order_id = 'ORD001037';
-- order_ts 2026-09-14 13:29:58, delivery_minutes 480, DELIVERED,
-- _source_file orders.csv, _loaded_at 2026-10-27 09:00:02,
-- _run_id scheduled__2026-10-27T03:30:00+00:00

-- proof it is not in the revenue total
SELECT count(*) FROM mart_fact_orders WHERE order_id = 'ORD001037';   -- 0
SELECT dq_status FROM stg_orders WHERE order_id = 'ORD001037';        -- REJECTED
```

An 8-hour delivery on a 10-minute promise. The row is in `stg_orders` with
`dq_status = 'REJECTED'`, the mart filters on `dq_status = 'PASSED'`, so it cannot be in
the revenue total - and that is demonstrable from the tables rather than from reading my
code.

**ETL - the same answer, but one step weaker:**

```sql
SELECT count(*) FROM etl_fact_orders WHERE order_id = 'ORD001037';   -- 0
```

```bash
grep ORD001037 include/quarantine/dinesh/orders_failed_scheduled__2026-10-27T03-30-00_00-00.csv
# ORD001037,2026-09-14 13:29:58,C00385,S13,P0104,1,525.0,10,472.5,UPI,DELIVERED,App,480,
#   "Critical: delivery_minutes above 120",2026-10-27T03:30:02,scheduled__2026-10-27T03:30:00+00:00
```

Both prove the exclusion and both give the reason. The difference is that the ETL
evidence lives in a CSV on a filesystem - it can be moved, deleted or not backed up -
while the ELT evidence is a row in the warehouse with the same backup and access
controls as everything else. For a finance audit, I would rather point at a table.

### 9. Change history: delivered in September, returned on 1 October

This is the question from Part 0 that the client still has not answered, and the two
pipelines currently behave the same way because I made them agree.

**What both pipelines show after the incremental load:** September revenue is
**restated**. The MERGE updates the original row in place, `order_status` becomes
RETURNED, `is_revenue` becomes false, and `revenue_amount` becomes 0. September net
revenue drops from **1,135,628.80 to 1,131,435.10** - the 10 corrected orders were worth
**4,193.70**. Re-run the September report a week later and it gives a different number
than it did on 1 October, with nothing in the output explaining why.

**What finance actually wants:** both, and they are not being unreasonable. The
statutory September number was signed off on 1 October and cannot move - a restated
month means last month's reported figures were wrong. But the *economic* number also has
to be correct, because 4,193.70 of returns is a real cost that belongs to the September
cohort. So: "September as reported on 1 October" **and** "September as it stands today",
clearly labelled as different numbers.

**How I would keep both.** Today I cannot - the MERGE overwrites and the old value is
gone from the fact table (it is still in `raw_orders`, which is the one saving grace, so
the ELT pipeline could reconstruct it and the ETL pipeline could not). To do it
properly:

1. Keep the fact table append-only with a `valid_from` / `valid_to` / `is_current` pair,
   so the correction inserts a new version of the order and closes the old one. Reports
   then pick a date: `WHERE '2026-10-01' BETWEEN valid_from AND valid_to` gives
   September as reported, `WHERE is_current` gives September as it stands.
2. Or keep the fact table as-is and add a `fact_order_corrections` table: one row per
   change with the old status, the new status, the amount moved and the date it arrived.
   Cheaper, auditable, and it answers "what moved and when" directly - which is usually
   the actual question.

I would start with option 2 because it is a day's work and it answers the business
question, and only move to full versioning if the corrections turn out to be frequent.
The 10 rows in one day's increment suggest they might be.

---

## Part F: observability

### Run audit

`pipeline_runs` is written by the final task on both DAGs, with
`trigger_rule="all_done"`. That rule is the whole point: a run audit that only gets
written on success tells you nothing on the morning something breaks. `record_run`
deletes any existing row for the same `(run_id, dag_id)` before inserting, so clearing
and re-running a task updates the audit row instead of adding a second one.

Straight out of the table, after the Airflow runs (full output in
`screenshots/query_results.md`):

| run_id | dag_id | rows_extracted | rows_loaded | rows_quarantined | status |
| --- | --- | --- | --- | --- | --- |
| manual__2026-10-26T10:59:27 | nammamart_etl_dinesh | 5,212 | 2,460 | 127 | SUCCESS |
| manual__2026-10-26T10:53:18 | nammamart_etl_dinesh | 5,060 | 0 | 106 | FAILED |
| manual__2026-10-26T10:46:59 | nammamart_elt_dinesh | 5,372 | 2,460 | 137 | SUCCESS |
| manual__2026-10-26T10:43:07 | nammamart_etl_dinesh | 5,212 | 2,460 | 127 | SUCCESS |
| scheduled__2026-10-27T03:30:00+00:00 | nammamart_etl_dinesh | 5,212 | 2,460 | 127 | SUCCESS |
| scheduled__2026-10-27T03:30:00+00:00 | nammamart_elt_dinesh | 5,372 | 2,460 | 137 | SUCCESS |
| scheduled__2026-10-26T03:30:00+00:00 | nammamart_etl_dinesh | 5,212 | 2,460 | 127 | SUCCESS |
| scheduled__2026-10-26T03:30:00+00:00 | nammamart_elt_dinesh | 5,372 | 2,460 | 137 | SUCCESS |

Three things worth pointing at in that table. The four `scheduled__...03:30:00+00:00`
rows are the 9:00 AM IST schedule firing for real, on two consecutive mornings, with
nobody watching - I left the DAGs unpaused overnight and found the 27th's runs waiting
for me, same row counts, same revenue. The FAILED row is Part F experiment
4 - `products.csv` renamed - and it has a row at all only because of
`trigger_rule="all_done"`; its `notes` column names every task that produced no result.
And the row above it is the *same run* after I restored the file and used Clear: the
audit row flipped from FAILED to SUCCESS instead of a second row appearing, which is
what the delete-before-insert in `record_run` is for.

The ELT numbers are legitimately a little higher: `rows_extracted` counts raw rows,
and the ELT raw layer holds the 160 increment rows alongside the 2,412 September ones,
while the ETL side counts only what its five extract tasks read.

### The ten experiments

Experiments 1-9 are run by `python -m nammamart.observability_experiments`, which
copies the CSVs to a sandbox, breaks one thing at a time, runs both pipelines, reads the
result back out of the warehouse and restores the file. The numbers below are its
output, not my recollection. Experiment 10 is a UI action and is recorded from the UI.

### 1. Happy path

- **Expected:** both DAGs green, 2,310 clean September orders in each fact table, 127 rows quarantined with reasons
- **Observed:** ETL finished in 0.4s with 2310 fact rows; ELT finished in 0.5s with 2310 mart rows; 127 rows quarantined on the ELT side. dq_results ETL [('FAIL', 19), ('PASS', 11), ('SKIP', 1), ('WARN', 11)], ELT [('FAIL', 19), ('PASS', 11), ('SKIP', 1), ('WARN', 12)]. The five extract tasks have no dependencies on each other, so in Airflow they run in parallel; here they run one after another.
- **Where I saw it:** task duration in the Airflow Grid view; the row counts in the load task log; dq_results and elt_quarantine
- **What I would change:** nothing - but the ETL log should print the per-file passed/failed split as a small table instead of one long dict, it is hard to read at 9 AM

### 2. Re-run (idempotency)

- **Expected:** fact row counts identical, revenue unchanged, no duplicated orders
- **Observed:** ETL fact 2310 -> 2310, ELT mart 2310 -> 2310, raw_orders 2412 -> 2412. Both loads are CREATE OR REPLACE, and the raw loader deletes the rows of the file it is re-loading before inserting them again, so raw does not grow either. Raw growing would also have been defensible (append-only audit), but then every order would be reported as a duplicate, which is worse.
- **Where I saw it:** the row counts in both load task logs; pipeline_runs has two rows with the same rows_loaded
- **What I would change:** nothing

### 3. Incremental load

- **Expected:** 150 new orders inserted, 10 September orders updated to RETURNED, no duplicates
- **Observed:** ETL: {'updated': 10, 'inserted': 150, 'quarantined': 0}. ELT mart MERGE: {'updated': 10, 'inserted': 150, 'fact_rows': 2460}. Fact 2310 -> 2460 (ELT mart 2460), duplicate order_ids in the mart: 0. September RETURNED orders went 68 -> 78, which is the 10 corrections landing on the original rows. Both MERGEs report UPDATE/INSERT separately via RETURNING merge_action, so the split is in the log rather than inferred.
- **Where I saw it:** merge_staging and merge_mart task logs; SELECT order_id, count(*) FROM mart_fact_orders GROUP BY 1 HAVING count(*) > 1
- **What I would change:** the 150 new orders have no payments yet, so they all show up in the reconciliation as 'delivered order with no successful payment'. That is correct but noisy - the reconciliation should ignore orders younger than the latest payment export instead of flagging them

### 4. Missing source file

- **Expected:** the task that reads products.csv fails loudly, retries twice, and everything downstream is marked upstream_failed - no partial load
- **Observed:** ETL: FileNotFoundError: Source file missing: /Users/cool/Documents/FDE/Material/Module2/repo/include/tmp/dinesh/experiments/data/products.csv raised in extract(). ELT: FileNotFoundError: Source file missing: /Users/cool/Documents/FDE/Material/Module2/repo/include/tmp/dinesh/experiments/data/products.csv raised in load_raw(). Both pipelines raise FileNotFoundError at the extract/load_raw step. I also ran this one in Airflow proper (screenshots/experiment_4_up_for_retry.jpg and experiment_4_missing_file.jpg): extract_products went up_for_retry, waited the 2 minute retry_delay, retried twice (Try Number 3 on the task page) and then failed; validate_products, transform, load_star, record_transform_dq, load_increment, reconcile and report all went upstream_failed, so the warehouse kept the previous numbers instead of loading four files out of five. audit_run still wrote its pipeline_runs row - status FAILED, notes 'no result from: [validate_products, transform, record_validation_dq, load_star, record_transform_dq, load_increment, reconcile, report]'. And it caught a bug in my own DAG: the run was marked SUCCESS in the UI while pipeline_runs said FAILED, because audit_run is the only leaf task and Airflow takes the run state from its leaves. audit_run now re-raises after writing the audit row, so the evidence is saved and the run goes red.
- **Where I saw it:** the task log line 'Source file missing: .../products.csv'; Try Number 3 on the extract_products page; the Graph view (red extract_products, orange upstream_failed downstream, four green extract/validate pairs); pipeline_runs.status and .notes
- **What I would change:** nothing alerted me - the only signal is the red square in the UI. The bonus Slack alert on failure should be wired to on_failure_callback, because at 9 AM nobody is watching the Grid view

### 5. New bad data

- **Expected:** all five rows quarantined, each with the reason that matches the defect, none of them in the fact table, and dq_results counts up by one per rule
- **Observed:** 5 of 5 rows quarantined and 0 of them reached the mart. EXP5001 -> Critical: customer_id is missing; EXP5002 -> Critical: quantity must be greater than zero; EXP5003 -> Critical: store_id does not exist in stores; EXP5004 -> Critical: order_status not in [DELIVERED, CANCELLED, RETURNED]; EXP5005 -> Critical: order_ts is in the future. EXP5003 is interesting: it fails the Consistency check against stores rather than a column check, because S99 is a well-formed store_id that simply does not exist. The ETL side wrote the same rows to orders_failed_exp5.csv.
- **Where I saw it:** elt_quarantine and the quarantine CSV; dq_results rows for orders_customer_id_present, orders_quantity_positive, orders_store_id_exists_in_stores, orders_status_allowed, orders_not_future_dated
- **What I would change:** the quarantine row keeps only the parsed values, so for a row that failed because a value would not parse you have to go back to raw to see what was actually in the file. Keeping the raw text alongside would make the audit answer in Part E a single query

### 6. Schema change

- **Expected:** a loud failure, not a silent load of wrong numbers
- **Observed:** ETL: KeyError: 'order_amount' in get_loc(). ELT: BinderException: Binder Error: Referenced column "order_amount" not found in FROM clause!
Candidate bindings: "order_ts", "order_status", "order_id", "delivery_minutes", "store_id"

LINE 31:         try_cast(order_amount AS DOUBLE)              AS order_amount_source...
                          ^ in _run_script(). The ELT raw load is happy - raw takes whatever columns arrive - and the failure lands one layer later when stg_orders cannot bind order_amount, which is the right place for it. On the ETL side the validate task does not crash: run_checks catches the missing column and records the affected checks with status ERROR ([('orders_amount_matches_formula', "missing column 'order_amount'")]), and the transform step is what fails. So the amount check was skipped rather than failed, which is a hole: a dashboard reading only FAIL counts would have seen nothing wrong.
- **Where I saw it:** the transform task log (KeyError); the stg_orders task log (Binder Error); dq_results rows with status = 'ERROR'
- **What I would change:** treat status = 'ERROR' in dq_results as a pipeline failure, not a note. A check that could not run is not a check that passed. I would add an explicit schema assertion at the top of validate: compare the column list against an expected set and fail there, with the diff in the message

### 7. Volume anomaly

- **Expected:** a warning that the order volume collapsed by more than 90%
- **Observed:** orders_volume_not_collapsed fired on both pipelines: [('nammamart_elt_dinesh', 'WARN', 49, '49 rows vs 2310 last run'), ('nammamart_etl_dinesh', 'WARN', 49, '49 rows vs 2310 last run (98% drop)'), ('nammamart_elt_dinesh', 'SKIP', 2310, 'no previous run to compare against'), ('nammamart_etl_dinesh', 'SKIP', 2310, 'no previous run to compare against')]. The baseline is the previous successful run's rows_loaded from pipeline_runs, so on the very first run the check reports SKIP instead of inventing an expectation. My first version compared the fact row count against the total rows read across all five files, which is about a 50% 'drop' on a perfectly healthy run - it would have warned every single morning and been muted within a week.
- **Where I saw it:** dq_results rows for orders_volume_not_collapsed; pipeline_runs.rows_loaded for the two runs
- **What I would change:** 90% is far too loose for a business that does ~2,400 orders a month. A 30% drop day over day is already an incident. I would compare against the median of the last 7 runs and warn at 30%, fail at 60%

### 8. Distribution drift

- **Expected:** something notices that one store's prices moved by two orders of magnitude
- **Observed:** Two versions, and they give opposite answers. (a) unit_price x100 only: the Accuracy check orders_amount_matches_formula rejected 194 rows and S08 revenue went 72560.6 -> 0.0, so the drift never reached the mart - but it reached it as a hole, not as a warning. (b) unit_price and order_amount both x100, which is what a broken exporter would really do: 483 rows failed anything at all and S08 revenue went 72560.6 -> 7282340.0. Nothing caught it. Every row is internally consistent, every value is 'valid', and the mart is wrong by two orders of magnitude.
- **Where I saw it:** dq_results for orders_amount_matches_formula; SELECT sum(revenue_amount) FROM mart_fact_orders WHERE store_id = 'S08'
- **What I would change:** this is the real gap in my pipeline. I have no distribution check at all. I would add one that compares each store's median unit_price and average order value against the last 7 runs and warns on a move beyond about 3 standard deviations, written as a SQL check on the mart so both pipelines share it

### 9. Freshness

- **Expected:** a timeliness warning that the newest order is a month old
- **Observed:** orders_freshness warned on both pipelines: [('nammamart_elt_dinesh', 'WARN', 'latest order is 11 day(s) before the end of the data window'), ('nammamart_etl_dinesh', 'WARN', 'latest order is 11 days old')]. It says 10-11 days, not 30, and the reason is worth keeping: two orders in the file are dated 21 October, which the timeliness check normally rejects as future-dated. Shift everything back 30 days and those two become 21 September, perfectly valid, and they are then the newest order in the file. So the measured staleness is 11 days. The check fires either way, but it is a reminder that 'newest row' is only as trustworthy as the validity checks in front of it. Worth being honest about the limitation: the check compares the newest order against the fixed end of the data window (2026-10-02) rather than against the clock, because the dataset is a 2026 extract and 'now' would make every run look stale. In production the comparison has to be now() in Asia/Kolkata.
- **Where I saw it:** dq_results rows for orders_freshness; the ELT detail line gives the age in days
- **What I would change:** compare against now() with the window end only as a development override, and raise it from Warning to Critical - a stale file at 9 AM means the upstream export did not run, and reporting yesterday's numbers as today's is worse than reporting nothing

### 10. Recovery (Clear a failed task)

- **Expected:** clearing the failed task re-runs only that task and the ones downstream of it, not the whole DAG
- **Observed:** Done in the Airflow UI after experiment 4, not in this script. I restored products.csv and hit Clear on the failed extract_products. The dialog said 'Affected Tasks: 10' and listed exactly extract_products and its nine downstream tasks (screenshots/experiment_10_clear_downstream.jpg); the other four extract and validate pairs were not in the list and kept their green state from the first attempt. All 10 re-ran, the DAG run went from failed to success, and the pipeline_runs row for that run_id changed from FAILED to SUCCESS rather than a second row appearing - which is why record_run deletes the existing row for that (run_id, dag_id) before inserting.
- **Where I saw it:** the Clear Task Instance dialog (Affected Tasks: 10); the task states before and after; pipeline_runs for that run_id
- **What I would change:** nothing on the mechanism. What I would add is making the extract tasks safe to clear in isolation, which they already are because every load is a replace or a merge on a key

---

### The five pillars

| Pillar | How my pipeline covers it | Verdict |
| --- | --- | --- |
| **Freshness** | `orders_freshness` on both pipelines, compared against the end of the data window; experiment 9 shows it firing | covered, but measured against a hardcoded date instead of the clock |
| **Volume** | `orders_volume_not_collapsed`, baselined on the previous successful run's `rows_loaded` from `pipeline_runs`; experiment 7 shows it firing at a 98% drop | covered, threshold far too loose at 90% |
| **Schema** | nothing explicit. Experiment 6 shows the pipeline *fails* on a schema change rather than loading wrong data, which is the important half, but it fails with a `KeyError` and a Binder Error rather than a message that says which column disappeared | weakest pillar |
| **Distribution** | nothing at all. Experiment 8(b) inflated one store's prices 100x in an internally consistent way and every check passed; the mart was wrong by two orders of magnitude and the pipeline reported success | worst gap, and the one I would fix first |
| **Lineage** | `_source_file`, `_run_id` and `_loaded_at` on every raw and staging row, carried through to `mart_fact_orders`; `elt_quarantine` keeps the whole rejected row as JSON; `dq_results` and `pipeline_runs` tie a number to the run that produced it | strongest pillar, on the ELT side |

**Strongest: lineage**, and only because of the ELT design. Any number on
`mart_daily_store_kpis` traces to the orders behind it, to their staging rows with their
quality verdict, to the raw rows as received, and to the file and run that loaded them -
four joins, no code reading. The ETL pipeline can get back to the order ids but not to
the source rows, because it does not keep them.

**Weakest: distribution**, by a distance. I have 40 rules and not one of them looks at
whether today's numbers *resemble* yesterday's. Every rule asks "is this row
well-formed", none asks "is this batch plausible". Experiment 8 is the proof: a tenfold
or hundredfold price error that is internally consistent sails through every check I
have. Schema is a close second, but at least there the pipeline fails loudly.

If I had one more day: a `mart_distribution_baseline` table holding each store's median
`unit_price`, average order value and order count for the last 7 runs, and a SQL check
that warns when today moves more than about 3 standard deviations from it. Written as
SQL on the mart, so both pipelines share one implementation instead of two that drift.

---

## Part G: recommendation to the CTO

**To:** Sameer, CTO
**From:** Dinesh (FDE)
**Subject:** ETL or ELT for the morning report

**Recommendation: build the ELT pipeline as the system of record, and keep the ETL
pipeline's quality rules as the gate in front of the mart.** In practice that means one
pipeline - the ELT one - with the ETL pipeline's standard of "nothing unvalidated
reaches finance" enforced in staging, which is what my `dq_status` column already does.
I would not run both in production.

I built both on your data and ran them side by side before writing this. They produce
identical numbers - 2,460 orders, 1,211,087.30 net revenue, 284,875.31 gross margin,
zero orders present in one and missing from the other - so this is not a recommendation
about which one is *correct*. Both are. It is about which one you can live with.

**Auditability.** This is what decides it. Yesterday your finance team asked me to prove
that a particular rejected order was not in the revenue total. On the ELT side that is
two queries: the row as it arrived in `raw_orders`, the rule it broke in
`elt_quarantine`, and `dq_status = 'REJECTED'` in staging - all inside the warehouse,
under the same backups and access controls as everything else. On the ETL side the same
answer exists but part of the evidence is a CSV on a filesystem that nothing backs up
and nothing deletes. For an audit I would rather point at a table than at a folder. The
ELT pipeline also keeps the input, so when a number is questioned three months from now
the original row is still there. The ETL pipeline keeps only the output; if
`orders.csv` has been overwritten, the evidence is gone.

**Data quality guarantees.** Finance's requirement - no bad row in the CFO's dashboard -
is met by both, just in different places. ETL stops the row at the door. ELT lets it
into the raw layer and filters it in staging, and `mart_fact_orders` selects only
`dq_status = 'PASSED'`. The guarantee is equally strong; the difference is that the ELT
version is *demonstrable* with a query, while the ETL version requires trusting that
the validate task ran. There is one honest risk in the ELT version: the filter is a
`WHERE` clause, and a `WHERE` clause can be forgotten in a new mart table. That is a
code review and testing problem, and it is cheaper to solve than losing the audit trail.

**Ease of change.** ELT wins, with a caveat. When ops changes the delivery limit from
120 minutes to 90, the ELT change is one line of SQL and the whole of September can be
reprocessed from raw in seconds - the data is already in the warehouse. The ETL change
is also one line, but reprocessing means re-reading the CSVs, which may no longer be
there. The caveat: debugging SQL is harder than debugging Python. My staging model for
orders is 130 lines of SQL with no unit tests, while the Python rules in `quality.py`
are a list of small functions I can test individually. If you go ELT, budget for dbt or
similar so the SQL models get tested and documented like code.

**Performance at 1000x.** You are at roughly 2,400 orders a month. 1000x is 2.4 million
a month, around 30 million a year. At that size ELT wins clearly, and the reason is
where the work happens. The ETL pipeline loads every row into pandas in one process on
one machine - 30 million rows of orders joined to dimensions is tens of gigabytes of
RAM, and the fix is rewriting it for Spark or chunking it by hand. The ELT pipeline
pushes the work into the warehouse, which is the thing designed to do it, and the
rewrite is mostly adding partitioning by `order_date` and swapping DuckDB for something
with more than one machine behind it. One caution: 1000x is also the point where
`CREATE OR REPLACE` on the full fact table stops being acceptable, and you move to
loading only the changed partitions. The MERGE pattern I built for the increment is the
seed of that.

**Cost.** At today's volume both are free - DuckDB is a file. The costs are not in the
tooling:

- ELT stores every raw row forever, so storage grows and never shrinks. At 30 million
  orders a year that is real money, and it is why the retention policy in Part E6
  matters more under ELT than under ETL.
- ELT uses more warehouse compute, because the transforms run there. On a per-second
  billed warehouse that is the line item that grows.
- ETL looks cheaper on storage and compute and is more expensive in engineering time:
  every change needs a Python developer, and every audit question needs one too.

Engineering time is your scarcest resource - you have no data engineers today. That
pushes the decision towards ELT as well.

### Top 3 risks of the ELT design

1. **Raw holds unmasked personal data, and that is a DPDP liability that grows every
   day.** `raw_customers` keeps names, emails and phone numbers in the clear, by
   design, because that is what "keep every raw record" means. Today it is a file on a
   laptop with no access control at all. Before this goes to production raw needs to
   live somewhere with real roles and column-level grants, access to `raw_customers`
   needs to be a named break-glass role that is logged, and the retention and erasure
   jobs in Part E6 need to actually exist. This is the risk I would escalate first.
2. **The quality guarantee depends on a `WHERE` clause in every mart model.** Nothing
   structurally prevents someone adding `mart_new_report` next quarter that reads
   `stg_orders` without filtering `dq_status`, and the rejected rows would be back in a
   finance number with no error anywhere. Mitigation: build the mart on a view that has
   the filter baked in (`v_fact_orders` already does this), and add a test that fails if
   any mart table contains an order that is REJECTED in staging.
3. **No distribution monitoring, so a plausible-looking wrong number gets published.**
   Experiment 8 is the demonstration: I inflated one store's prices 100-fold in a way
   that kept every row internally consistent, and the pipeline passed every check and
   reported success with a mart that was wrong by two orders of magnitude. This is the
   failure mode that would actually embarrass you in a leadership meeting, because the
   report arrives on time, looks fine and is wrong. The baseline check described at the
   end of Part F is the mitigation and it is about a day's work.

One more thing, not a risk but a dependency: right now this runs on my laptop, inside
Docker, and if the laptop is off at 9:00 the run simply does not happen and nothing
tells you (Part C4). Whichever pipeline you pick, the first production task is a
scheduler that is always on and an alert if no row has appeared in `pipeline_runs` by
09:15 IST. The pipeline being correct does not help if nobody notices it did not run.

---

## The six business questions

Queries live in `include/sql/business_questions.sql`, one block per question per
pipeline. Everything below is the output of

```bash
python -m nammamart.business_questions
```

so the SQL and the result cannot disagree with each other. State of the warehouse:
September full load plus the 1 October increment, 2,460 orders.

Short version of the answers:

1. **Daily net revenue by zone and store** - East 314,056.20, West 312,413.00, South
   256,214.55, North 243,021.55, and 85,382.00 that cannot be attributed to a zone at
   all because store S07 has no zone in the store master. Best single store-day was
   Yelahanka on 19 September, 9,732.00 off 13 delivered orders.
2. **Revenue and gross margin by category** - Staples leads on revenue (268,583.30) and
   Dairy & Bakery on margin percentage (26.5%). 120 delivered orders have no category,
   76 of them because the product row was rejected in staging; those also have no cost
   price, which is why that bucket shows a 6.9% margin rather than a real one.
3. **Average delivery time and the 15-minute promise** - the promise is being missed
   badly. **28.5% of delivered orders arrive within 15 minutes** and the average is
   **21.3 minutes**. Best store is Whitefield at 35.4%, worst is Banashankari at 21.6%.
   No store is above 36%. This is the number I would put in front of the COO first.
4. **Revenue by payment mode and what does not reconcile** - UPI 638,752.15, Card
   251,882.30, Cash on Delivery 182,334.40, Wallet 138,118.45. **321 orders do not
   reconcile, 206,984.20 in absolute variance.** 150 of those 321 are the increment's
   new orders whose payments have not arrived yet, so the figure finance should chase is
   the other 171.
5. **Top 10 customers by spend** - C00136 (Sanjay B.) at 8,738.10 over 9 orders. Contact
   details are masked in the output, and the raw columns are not in the table to show.
   Worth noting: 5 of the top 10 are BRONZE and only 2 are PLATINUM, so the loyalty
   tiers do not look like they are tracking spend.
6. **Rows rejected, by file and reason** - **127 rows on the September full load**:
   102 orders, 10 payments, 8 customers, 7 products, 0 stores. The biggest single
   reason is 18 orders with no `customer_id`. The query below reports **137** because
   it reads the latest run, which is the increment: once the 10 corrections land, the
   September versions of those orders become duplicates and are rejected as "duplicate
   order_id, later row kept". They are superseded rows, not newly broken data, and it
   took me a second look to be sure of that.

#### `q1_etl`

```sql
-- Q1 ETL - daily net revenue by zone and store, delivered orders only, after discount
SELECT order_date, zone, store_name,
       delivered_orders,
       net_revenue
FROM etl_daily_zone_revenue
ORDER BY net_revenue DESC
LIMIT 10;
```

| order_date | zone | store_name | delivered_orders | net_revenue |
| --- | --- | --- | --- | --- |
| 2026-09-19 00:00:00 | North | NammaMart Yelahanka | 13 | 9732.0 |
| 2026-10-01 00:00:00 | North | NammaMart Hebbal | 13 | 8958.35 |
| 2026-09-06 00:00:00 | West | NammaMart Basaveshwaranagar | 10 | 8785.7 |
| 2026-10-01 00:00:00 | South | NammaMart JP Nagar | 13 | 8785.2 |
| 2026-10-01 00:00:00 | East | NammaMart HSR Layout | 18 | 8571.1 |
| 2026-09-19 00:00:00 | West | NammaMart Malleshwaram | 7 | 7889.7 |
| 2026-09-26 00:00:00 | South | NammaMart JP Nagar | 9 | 7751.2 |
| 2026-09-26 00:00:00 | South | NammaMart Banashankari | 14 | 7277.4 |
| 2026-09-25 00:00:00 | South | NammaMart Banashankari | 8 | 6826.7 |
| 2026-09-26 00:00:00 | East | NammaMart HSR Layout | 9 | 6688.1 |


#### `q1_elt`

```sql
-- Q1 ELT - same question off the mart
SELECT order_date, zone, store_name,
       delivered_orders,
       net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
ORDER BY net_revenue DESC
LIMIT 10;
```

| order_date | zone | store_name | delivered_orders | net_revenue |
| --- | --- | --- | --- | --- |
| 2026-09-19 00:00:00 | North | NammaMart Yelahanka | 13.0 | 9732.0 |
| 2026-10-01 00:00:00 | North | NammaMart Hebbal | 13.0 | 8958.35 |
| 2026-09-06 00:00:00 | West | NammaMart Basaveshwaranagar | 10.0 | 8785.7 |
| 2026-10-01 00:00:00 | South | NammaMart JP Nagar | 13.0 | 8785.2 |
| 2026-10-01 00:00:00 | East | NammaMart HSR Layout | 18.0 | 8571.1 |
| 2026-09-19 00:00:00 | West | NammaMart Malleshwaram | 7.0 | 7889.7 |
| 2026-09-26 00:00:00 | South | NammaMart JP Nagar | 9.0 | 7751.2 |
| 2026-09-26 00:00:00 | South | NammaMart Banashankari | 14.0 | 7277.4 |
| 2026-09-25 00:00:00 | South | NammaMart Banashankari | 8.0 | 6826.7 |
| 2026-09-26 00:00:00 | East | NammaMart HSR Layout | 9.0 | 6688.1 |


#### `q1_totals`

```sql
-- Q1 cross-check - September + increment totals per zone, both pipelines side by side
SELECT coalesce(e.zone, l.zone)            AS zone,
       round(e.etl_revenue, 2)             AS etl_revenue,
       round(l.elt_revenue, 2)             AS elt_revenue,
       round(e.etl_revenue - l.elt_revenue, 2) AS difference
FROM (SELECT zone, sum(revenue_amount) AS etl_revenue
      FROM etl_fact_orders GROUP BY zone) e
FULL OUTER JOIN (SELECT zone, sum(revenue_amount) AS elt_revenue
                 FROM mart_fact_orders GROUP BY zone) l
     ON l.zone IS NOT DISTINCT FROM e.zone
ORDER BY etl_revenue DESC NULLS LAST;
```

| zone | etl_revenue | elt_revenue | difference |
| --- | --- | --- | --- |
| East | 314056.2 | 314056.2 | 0.0 |
| West | 312413.0 | 312413.0 | -0.0 |
| South | 256214.55 | 256214.55 | -0.0 |
| North | 243021.55 | 243021.55 | 0.0 |
|  | 85382.0 | 85382.0 | -0.0 |


#### `q2_etl`

```sql
-- Q2 ETL - revenue and gross margin by category (margin = revenue - quantity x cost_price)
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM etl_category_margin
ORDER BY net_revenue DESC;
```

| category | delivered_orders | net_revenue | gross_margin | margin_pct |
| --- | --- | --- | --- | --- |
| Staples | 302 | 268583.3 | 67143.14 | 25.0 |
| Household | 363 | 212105.05 | 54466.15 | 25.7 |
| Personal Care | 323 | 210889.1 | 47054.58 | 22.3 |
| Dairy & Bakery | 405 | 179568.95 | 47633.0 | 26.5 |
| Snacks & Beverages | 341 | 174988.6 | 41429.67 | 23.7 |
| Fruits & Vegetables | 374 | 93604.2 | 22205.38 | 23.7 |
|  | 120 | 71348.1 | 4943.39 | 6.9 |


#### `q2_elt`

```sql
-- Q2 ELT - same question off the mart
SELECT category, delivered_orders, orders_missing_cost,
       net_revenue, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY net_revenue DESC;
```

| category | delivered_orders | orders_missing_cost | net_revenue | gross_margin | margin_pct |
| --- | --- | --- | --- | --- | --- |
| Staples | 302 | 0.0 | 268583.3 | 67143.14 | 25.0 |
| Household | 363 | 0.0 | 212105.05 | 54466.15 | 25.7 |
| Personal Care | 323 | 0.0 | 210889.1 | 47054.58 | 22.3 |
| Dairy & Bakery | 405 | 0.0 | 179568.95 | 47633.0 | 26.5 |
| Snacks & Beverages | 341 | 0.0 | 174988.6 | 41429.67 | 23.7 |
| Fruits & Vegetables | 374 | 0.0 | 93604.2 | 22205.38 | 23.7 |
| (no category) | 120 | 76.0 | 71348.1 | 4943.39 | 6.9 |


#### `q3_etl`

```sql
-- Q3 ETL - average delivery time per store and % of delivered orders inside 15 minutes
SELECT store_id, store_name, zone,
       count(*)                                                   AS delivered_orders,
       round(avg(delivery_minutes), 1)                            AS avg_delivery_minutes,
       round(100.0 * avg(CASE WHEN within_15_min THEN 1 ELSE 0 END), 1) AS pct_within_15_min
FROM etl_fact_orders
WHERE is_revenue AND delivery_minutes IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY pct_within_15_min;
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | pct_within_15_min |
| --- | --- | --- | --- | --- | --- |
| S06 | NammaMart Banashankari | South | 153 | 22.5 | 21.6 |
| S15 | NammaMart Basaveshwaranagar | West | 157 | 22.0 | 22.9 |
| S12 | NammaMart Rajajinagar | West | 119 | 21.8 | 24.4 |
| S10 | NammaMart Marathahalli | East | 150 | 22.1 | 26.0 |
| S09 | NammaMart Indiranagar | East | 136 | 21.1 | 26.5 |
| S13 | NammaMart Malleshwaram | West | 150 | 21.9 | 27.3 |
| S04 | NammaMart Jayanagar | South | 138 | 21.3 | 27.5 |
| S11 | NammaMart HSR Layout | East | 161 | 20.8 | 28.6 |
| S03 | NammaMart RT Nagar | North | 143 | 20.7 | 29.4 |
| S05 | NammaMart JP Nagar | South | 142 | 21.2 | 29.6 |
| S14 | NammaMart Vijayanagar | West | 166 | 21.5 | 31.3 |
| S02 | NammaMart Yelahanka | North | 150 | 20.9 | 31.3 |
| S01 | NammaMart Hebbal | North | 149 | 20.5 | 32.2 |
| S07 | NammaMart BTM Layout |  | 150 | 21.1 | 32.7 |
| S08 | NammaMart Whitefield | East | 164 | 20.6 | 35.4 |


#### `q3_elt`

```sql
-- Q3 ELT - same question off the mart
SELECT store_id, store_name, zone,
       sum(delivered_orders)                                      AS delivered_orders,
       round(sum(avg_delivery_minutes * delivered_orders) / sum(delivered_orders), 1)
                                                                  AS avg_delivery_minutes,
       round(sum(pct_within_15_min * delivered_orders) / sum(delivered_orders), 1)
                                                                  AS pct_within_15_min
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
GROUP BY 1, 2, 3
ORDER BY pct_within_15_min;
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | pct_within_15_min |
| --- | --- | --- | --- | --- | --- |
| S06 | NammaMart Banashankari | South | 153.0 | 22.5 | 21.6 |
| S15 | NammaMart Basaveshwaranagar | West | 157.0 | 22.0 | 22.9 |
| S12 | NammaMart Rajajinagar | West | 119.0 | 21.8 | 24.4 |
| S10 | NammaMart Marathahalli | East | 150.0 | 22.1 | 26.0 |
| S09 | NammaMart Indiranagar | East | 136.0 | 21.1 | 26.5 |
| S13 | NammaMart Malleshwaram | West | 150.0 | 21.9 | 27.3 |
| S04 | NammaMart Jayanagar | South | 138.0 | 21.3 | 27.5 |
| S11 | NammaMart HSR Layout | East | 161.0 | 20.8 | 28.6 |
| S03 | NammaMart RT Nagar | North | 143.0 | 20.7 | 29.4 |
| S05 | NammaMart JP Nagar | South | 142.0 | 21.2 | 29.6 |
| S02 | NammaMart Yelahanka | North | 150.0 | 20.9 | 31.3 |
| S14 | NammaMart Vijayanagar | West | 166.0 | 21.5 | 31.3 |
| S01 | NammaMart Hebbal | North | 149.0 | 20.5 | 32.2 |
| S07 | NammaMart BTM Layout |  | 150.0 | 21.1 | 32.7 |
| S08 | NammaMart Whitefield | East | 164.0 | 20.6 | 35.4 |


#### `q4_etl`

```sql
-- Q4 ETL - revenue by payment mode, and the amount that does not reconcile
SELECT f.payment_mode,
       count(*)                            AS delivered_orders,
       round(sum(f.revenue_amount), 2)     AS net_revenue,
       round(coalesce(r.unreconciled, 0), 2) AS unreconciled_amount,
       coalesce(r.mismatches, 0)           AS mismatched_orders
FROM etl_fact_orders f
LEFT JOIN (SELECT o.payment_mode,
                  sum(abs(r.variance)) AS unreconciled,
                  count(*)             AS mismatches
           FROM etl_payment_reconciliation r
           JOIN etl_fact_orders o ON o.order_id = r.order_id
           GROUP BY o.payment_mode) r ON r.payment_mode = f.payment_mode
WHERE f.is_revenue
GROUP BY f.payment_mode, r.unreconciled, r.mismatches
ORDER BY net_revenue DESC;
```

| payment_mode | delivered_orders | net_revenue | unreconciled_amount | mismatched_orders |
| --- | --- | --- | --- | --- |
| UPI | 1205 | 638752.15 | 42895.75 | 85 |
| Card | 457 | 251882.3 | 31733.05 | 64 |
| Cash on Delivery | 320 | 182334.4 | 30802.65 | 50 |
| Wallet | 246 | 138118.45 | 17349.3 | 34 |


#### `q4_elt`

```sql
-- Q4 ELT - same question off the mart
SELECT f.payment_mode,
       count(*)                              AS delivered_orders,
       round(sum(f.revenue_amount), 2)       AS net_revenue,
       round(coalesce(r.unreconciled, 0), 2) AS unreconciled_amount,
       coalesce(r.mismatches, 0)             AS mismatched_orders
FROM mart_fact_orders f
LEFT JOIN (SELECT o.payment_mode,
                  sum(abs(r.variance)) AS unreconciled,
                  count(*)             AS mismatches
           FROM mart_payment_reconciliation r
           JOIN mart_fact_orders o ON o.order_id = r.order_id
           GROUP BY o.payment_mode) r ON r.payment_mode = f.payment_mode
WHERE f.is_revenue
GROUP BY f.payment_mode, r.unreconciled, r.mismatches
ORDER BY net_revenue DESC;
```

| payment_mode | delivered_orders | net_revenue | unreconciled_amount | mismatched_orders |
| --- | --- | --- | --- | --- |
| UPI | 1205 | 638752.15 | 42895.75 | 85 |
| Card | 457 | 251882.3 | 31733.05 | 64 |
| Cash on Delivery | 320 | 182334.4 | 30802.65 | 50 |
| Wallet | 246 | 138118.45 | 17349.3 | 34 |


#### `q4_unreconciled_total`

```sql
-- Q4 - the single number finance wants: total value that does not reconcile
SELECT 'etl' AS pipeline, count(*) AS mismatched_rows,
       round(sum(abs(variance)), 2) AS total_unreconciled
FROM etl_payment_reconciliation
UNION ALL
SELECT 'elt', count(*), round(sum(abs(variance)), 2)
FROM mart_payment_reconciliation;
```

| pipeline | mismatched_rows | total_unreconciled |
| --- | --- | --- |
| etl | 321 | 206984.2 |
| elt | 321 | 206984.2 |


#### `q5_etl`

```sql
-- Q5 ETL - top 10 customers by spend with loyalty tier, masked contact details only
SELECT f.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked,
       f.loyalty_tier, c.city,
       count(*)                        AS delivered_orders,
       round(sum(f.revenue_amount), 2) AS lifetime_revenue
FROM etl_fact_orders f
JOIN etl_dim_customer c ON c.customer_id = f.customer_id
WHERE f.is_revenue
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY lifetime_revenue DESC
LIMIT 10;
```

| customer_id | customer_name_masked | email_masked | phone_masked | loyalty_tier | city | delivered_orders | lifetime_revenue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| C00136 | Sanjay B. | sa***@example.com | 96******88 | BRONZE | Bengaluru | 9 | 8738.1 |
| C00115 | Sanjay K. | sa***@example.com | 96******85 | BRONZE | Bengaluru | 8 | 8037.7 |
| C00283 | Nikhil R. | ni***@example.com | 91******43 | BRONZE | Bengaluru | 8 | 7934.6 |
| C00236 | Sneha N. | sn***@example.com | 90******01 | PLATINUM | Bengaluru | 12 | 7831.8 |
| C00310 | Kiran P. | ki***@example | 91******35 | BRONZE | Bengaluru | 8 | 7258.95 |
| C00265 | Vikram H. | vi***@example.com | 91******69 | GOLD | Bengaluru | 8 | 7151.3 |
| C00071 | Mahesh P. | ma***@example.com | 91******42 | SILVER | Bengaluru | 11 | 6933.3 |
| C00338 | Bhavana I. | bh***@example.com | 94******05 | SILVER | Bengaluru | 9 | 6848.55 |
| C00357 | Arjun N. | ar***@example.com | 93*****12 | BRONZE | Bengaluru | 11 | 6637.9 |
| C00209 | Kavya P. | ka***@example.com | 98******97 | PLATINUM | Bengaluru | 8 | 6519.4 |


#### `q5_elt`

```sql
-- Q5 ELT - same question off the mart
SELECT t.customer_id, t.customer_name_masked, c.email_masked, c.phone_masked,
       t.loyalty_tier, t.city, t.delivered_orders, t.lifetime_revenue
FROM mart_top_customers t
JOIN mart_dim_customer c ON c.customer_id = t.customer_id
ORDER BY t.lifetime_revenue DESC
LIMIT 10;
```

| customer_id | customer_name_masked | email_masked | phone_masked | loyalty_tier | city | delivered_orders | lifetime_revenue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| C00136 | Sanjay B. | sa***@example.com | 96******88 | BRONZE | Bengaluru | 9 | 8738.1 |
| C00115 | Sanjay K. | sa***@example.com | 96******85 | BRONZE | Bengaluru | 8 | 8037.7 |
| C00283 | Nikhil R. | ni***@example.com | 91******43 | BRONZE | Bengaluru | 8 | 7934.6 |
| C00236 | Sneha N. | sn***@example.com | 90******01 | PLATINUM | Bengaluru | 12 | 7831.8 |
| C00310 | Kiran P. | ki***@example | 91******35 | BRONZE | Bengaluru | 8 | 7258.95 |
| C00265 | Vikram H. | vi***@example.com | 91******69 | GOLD | Bengaluru | 8 | 7151.3 |
| C00071 | Mahesh P. | ma***@example.com | 91******42 | SILVER | Bengaluru | 11 | 6933.3 |
| C00338 | Bhavana I. | bh***@example.com | 94******05 | SILVER | Bengaluru | 9 | 6848.55 |
| C00357 | Arjun N. | ar***@example.com | 93*****12 | BRONZE | Bengaluru | 11 | 6637.9 |
| C00209 | Kavya P. | ka***@example.com | 98******97 | PLATINUM | Bengaluru | 8 | 6519.4 |


#### `q6_etl`

```sql
-- Q6 ETL - rows rejected in the latest run, by file and reason
SELECT split_part(check_name, '_', 1) AS source_file,
       dimension, severity, check_name, detail, rows_failed
FROM dq_results
WHERE dag_id LIKE 'nammamart_etl%' AND status = 'FAIL'
  AND run_id = (SELECT max(run_id) FROM dq_results WHERE dag_id LIKE 'nammamart_etl%')
ORDER BY rows_failed DESC;
```

_(no rows)_


#### `q6_elt`

```sql
-- Q6 ELT - same question, read straight off the quarantine table
SELECT source_table, failure_reason, count(*) AS rows_rejected
FROM elt_quarantine
WHERE run_id = (SELECT max(run_id) FROM elt_quarantine)
GROUP BY 1, 2
ORDER BY rows_rejected DESC
LIMIT 15;
```

| source_table | failure_reason | rows_rejected |
| --- | --- | --- |
| stg_orders | Critical: duplicate order_id, later row kept | 22 |
| stg_orders | Critical: customer_id is missing | 18 |
| stg_orders | Critical: order_amount does not match quantity x unit_price x (1 - discount) | 15 |
| stg_orders | Critical: quantity must be greater than zero | 10 |
| stg_orders | Critical: product_id does not exist in products | 9 |
| stg_orders | Critical: DELIVERED order has no delivery_minutes | 8 |
| stg_orders | Critical: order_ts is in the future | 7 |
| stg_orders | Critical: delivery_minutes above 120 | 6 |
| stg_payments | Critical: amount_paid is negative or unreadable | 6 |
| stg_orders | Critical: order_ts does not parse | 6 |
| stg_customers | Critical: duplicate customer_id, later row kept | 5 |
| stg_orders | Critical: store_id does not exist in stores | 5 |
| stg_payments | Critical: duplicate payment_id, later row kept | 4 |
| stg_products | Critical: cost_price is higher than mrp | 4 |
| stg_orders | Critical: order_status not in [DELIVERED, CANCELLED, RETURNED] | 3 |


#### `q6_summary`

```sql
-- Q6 - the headline count, by file, both pipelines
SELECT source_table AS source_file, count(*) AS rows_rejected
FROM elt_quarantine
WHERE run_id = (SELECT max(run_id) FROM elt_quarantine)
GROUP BY 1
ORDER BY rows_rejected DESC;
```

| source_file | rows_rejected |
| --- | --- |
| stg_orders | 112 |
| stg_payments | 10 |
| stg_customers | 8 |
| stg_products | 7 |

---

## Screenshots

In `screenshots/`. Both DAGs run in Airflow 3 (Astro Runtime 3.3) with no import
errors, and `astro dev run dags list-import-errors` returns nothing.

| File | What it shows |
| --- | --- |
| `schedule_9am_ist.jpg` | both DAGs in the DAGs list, schedule `0 9 * * 1-6`, Next Run **2026-10-27 09:00:00**, UI timezone +05:30, latest run green |
| `etl_graph_green.jpg` | `nammamart_etl_dinesh` Graph view, all 18 tasks green, the five extracts side by side as parallel roots |
| `elt_graph_green.jpg` | `nammamart_elt_dinesh` Graph view, all 15 tasks green, the raw loads chained |
| `report_task_log_revenue_by_zone.jpg` | the `report` task log printing revenue by zone and margin by category, with the graph green behind it |
| `experiment_4_up_for_retry.jpg` | Part F experiment 4 in progress: `extract_products` amber (up_for_retry) while the other four extracts are green |
| `experiment_4_missing_file.jpg` | the same run after three attempts: `extract_products` red, everything downstream orange (upstream_failed), the other four extract/validate pairs still green |
| `experiment_10_clear_downstream.jpg` | the Clear Task Instance dialog saying **Affected Tasks: 10** - the failed task and its nine downstream tasks, and nothing else |
| `query_results.md` | `pipeline_runs`, `dq_results`, the reconciliation table and the quarantine, as query output |

One honest note on the last row: DuckDB is a file with no web UI, so those are terminal
output rather than images. They are reproducible with one command
(`python -m nammamart.business_questions`) and the file says so.

## How to run it

```bash
# copy the dataset in (the repo already has it at include/data/nammamart/)
cp assignment/nammamart_dataset/*.csv include/data/nammamart/

# profile first - Part 0
python include/profiling/profile_nammamart.py

# both pipelines, from a terminal, no Airflow needed
cd include && python -m nammamart.run_pipelines --pipeline both --increment --reset

# the six business questions
python -m nammamart.business_questions

# the Part F experiments (runs in a sandbox, restores every file afterwards)
python -m nammamart.observability_experiments

# in Airflow
astro dev start                                    # UI link is printed; mine was
                                                   # http://repo.localhost:6563
astro dev run dags list-import-errors              # "No data found" = both parse
astro dev run dags test nammamart_etl_dinesh 2026-10-20
astro dev run dags test nammamart_elt_dinesh 2026-10-20
astro dev run dags unpause nammamart_etl_dinesh
astro dev run dags trigger nammamart_etl_dinesh
astro dev stop
```

Run the ETL DAG before the ELT DAG the first time: the ELT's `compare_with_etl` task
needs `etl_fact_orders` to exist, and logs a warning instead of comparing if it does
not.

## What I would do next, in order

1. The distribution baseline check from Part F - it is the one gap that lets a wrong
   number reach the COO looking right.
2. Treat `dq_results.status = 'ERROR'` as a failure, and assert the expected column set
   at the start of validate so a schema change says which column moved.
3. The `fact_order_corrections` table from Part E9, so September revenue stops silently
   restating itself.
4. A retention job for the quarantine directory.
5. `on_failure_callback` with a Slack alert read from an Airflow Variable, because
   nobody is watching the Grid view at 9 AM.
