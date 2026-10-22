# NammaMart ETL & ELT Pipelines: Submission

FDAIE Module 2 · Data Engineering Foundations · owner `mohanrajs`

Everything below was produced by real Airflow runs (Astro Runtime 3.3 / Airflow 3.3, LocalExecutor + Postgres in Docker), with the ETL and ELT DAGs sharing `include/warehouse_mohanrajs.duckdb`. Evidence is in [docs/](docs/); commands to reproduce are in section 9.

| Deliverable | Where |
|---|---|
| ETL DAG | [dags/nammamart_etl_mohanrajs.py](dags/nammamart_etl_mohanrajs.py), helpers [include/nammamart_etl_lib.py](include/nammamart_etl_lib.py), [include/nammamart_quality.py](include/nammamart_quality.py) |
| ELT DAG | [dags/nammamart_elt_mohanrajs.py](dags/nammamart_elt_mohanrajs.py), SQL in [include/sql/elt/](include/sql/elt/) |
| Shared helpers | [include/nammamart_common.py](include/nammamart_common.py), [include/nammamart_elt_lib.py](include/nammamart_elt_lib.py) |
| Data issues log (233 rows) | [docs/data_issues_log.csv](docs/data_issues_log.csv), profile in [docs/profile_summary.md](docs/profile_summary.md) |
| Business answers, `dq_results`, `pipeline_runs`, reconciliation (SQL + results, both pipelines) | [docs/evidence.md](docs/evidence.md), SQL in [docs/sql/](docs/sql/) |
| Quarantine file sample | [docs/quarantine_sample_orders_failed.csv](docs/quarantine_sample_orders_failed.csv) |
| Observability experiment logs | [docs/experiments/](docs/experiments/) |
| Tests | [tests/dags/test_nammamart_dags.py](tests/dags/test_nammamart_dags.py) (7 pass in the Astro image) |

**What I could not deliver:** screenshots. I worked headless, so there are no PNGs of the Graph view or schedule. In their place are the Airflow REST API output of the schedule ([docs/schedule_evidence.txt](docs/schedule_evidence.txt)), per-task state/duration tables ([docs/experiments/](docs/experiments/)), and the query results in [docs/evidence.md](docs/evidence.md). Open `http://localhost:8080` after `astro dev start` to take the screenshots.

---

## 1. Headline results

* **The two pipelines agree to the paisa.** `scripts/compare_pipelines.py` compares 12 things (clean-row counts, revenue, category margin, payment-mode revenue, reconciliation rows, top customers, masked PII, every fact row, the failure reason of every rejected order, and every `dq_results` count). All match, after the September full load and after the increment.
* September input: 2,412 orders. 2,015 pass and 397 are quarantined. After the increment (131 new orders loaded, 9 status corrections applied) the fact table has 2,146 orders and net revenue of ₹10,51,936.00.
* September net revenue is ₹9,86,063.90 as first loaded and ₹9,82,401.20 after the 9 returns arrive. The ₹3,662.70 difference is the "returned later" case (Part E9).
* 57 quality checks run on each pipeline on every run, covering all six dimensions. Each writes a row to `dq_results`.
* **One finding for Sameer and Kavya:** 326 orders (₹1,65,737 of delivered revenue) are withheld only because a master row they point to failed validation. A single store row, S07 with an empty `zone`, blocks 181 orders. See question 1 in section 2 and risk 3 in section 8.

---

## 2. Part 0: discovery and profiling

Full profile: [docs/profile_summary.md](docs/profile_summary.md). Reproduce with `python scripts/profile_data.py`. The script uses plain pandas, independent of the pipelines, so it works as a cross-check. Referential integrity:

| Relationship | Orphans |
|---|---|
| orders.customer_id → customers | 0 (plus 18 empty ids) |
| orders.product_id → products | 9 |
| orders.store_id → stores | 5 |
| payments.order_id → orders | 8 |
| DELIVERED orders with no payment | 27 |

Business-rule checks: 18 orders with `order_amount` ≠ formula; 10 with quantity ≤ 0; 3 with discount above 100 (max 250); 8 DELIVERED orders with no `delivery_minutes`; 6 with delivery time of 480 to 1,440 minutes; 14 SUCCESS payments with an amount different from the order. The increment file has 160 rows: 150 new orders and 10 corrections of existing orders (status changed to RETURNED).

### Data Issues Log (grouped; every instance with its row key and CSV line is in [docs/data_issues_log.csv](docs/data_issues_log.csv))

| File | Column | Problem | Dimension | Severity | Rows | Example key | Proposed rule |
|---|---|---|---|---|---:|---|---|
| orders.csv | order_id | exact duplicate of an earlier row | Uniqueness | Critical | 12 | ORD001462 | order_id unique |
| orders.csv | customer_id | customer_id is empty | Completeness | Critical | 18 | ORD002255 | customer_id not null |
| orders.csv | order_ts | order_ts is empty | Completeness | Critical | 1 | ORD000362 | order_ts not null |
| orders.csv | order_ts | order_ts not a valid timestamp (e.g. N/A, 31/09/2026) | Validity | Critical | 5 | ORD000403 | order_ts parses as YYYY-MM-DD HH:MM:SS |
| orders.csv | order_ts | order_ts is in the future (2027) | Timeliness | Critical | 7 | ORD002119 | order_ts <= run time |
| orders.csv | order_status | status not in DELIVERED/CANCELLED/RETURNED (Pending??) | Validity | Critical | 3 | ORD000500 | status in allowed list |
| orders.csv | order_status | status has wrong casing (delivered) | Consistency | Warning | 2 | ORD001066 | upper-case before validating |
| orders.csv | payment_mode | payment_mode not recognisable (Cashh) | Validity | Critical | 2 | ORD001812 | payment_mode in allowed list |
| orders.csv | payment_mode | payment_mode needs standardising (upi, 'UPI ', CARD, cod) | Consistency | Warning | 8 | ORD000159 | trim + map to canonical value |
| orders.csv | quantity | quantity zero or negative | Accuracy | Critical | 10 | ORD000469 | quantity > 0 |
| orders.csv | discount_pct | discount_pct outside 0-100 | Validity | Critical | 3 | ORD002103 | 0 <= discount_pct <= 100 |
| orders.csv | order_amount | order_amount != quantity x unit_price x (1 - discount/100) | Accuracy | Critical | 15 | ORD002305 | recompute and compare (tolerance 0.01) |
| orders.csv | delivery_minutes | DELIVERED order without delivery_minutes | Completeness | Critical | 8 | ORD001554 | delivered => delivery_minutes present |
| orders.csv | delivery_minutes | impossible delivery time (480-1440 minutes) | Accuracy | Critical | 6 | ORD001037 | delivery_minutes < 120 |
| orders.csv | product_id | product_id not in products.csv (P9xxx) | Consistency | Critical | 9 | ORD000986 | FK to products |
| orders.csv | store_id | store_id not in stores.csv (S99) | Consistency | Critical | 5 | ORD001799 | FK to stores |
| payments.csv | payment_id | exact duplicate of an earlier row | Uniqueness | Critical | 4 | PAY0000509 | payment_id unique |
| payments.csv | order_id | payment for an order that does not exist (ORD97xxxx) | Consistency | Critical | 8 | PAY0002219 | FK to orders |
| payments.csv | amount_paid | SUCCESS payment with zero/negative amount | Accuracy | Critical | 10 | PAY0001568 | amount_paid > 0 |
| payments.csv | payment_ts | payment_ts after the run time (2026-10-21, 2026-11-02, 2027-01-01 values) | Timeliness | Critical | 7 | PAY0000396 | payment_ts <= run time |
| payments.csv | payment_mode | payment_mode not recognisable (Cashh) | Validity | Critical | 2 | PAY0000522 | payment_mode in allowed list |
| payments.csv | payment_mode | payment_mode needs standardising | Consistency | Warning | 8 | PAY0002188 | trim + map |
| payments.csv | amount_paid | SUCCESS payment amount differs from its order_amount | Consistency | Warning | 13 | PAY0000354 | SUCCESS amount == order_amount -> reconciliation table (not quarantine) |
| customers.csv | customer_id | customer_id repeated with different phone/area | Uniqueness | Critical | 5 | C00103 | one row per customer_id; keep first |
| customers.csv | city | city is empty | Completeness | Warning | 6 | C00355 | city not null |
| customers.csv | email | invalid email (N/A, missing @ or dot) | Validity | Warning | 12 | C00337 | email regex |
| customers.csv | phone | phone is not 10 digits | Validity | Warning | 7 | C00151 | phone 10 digits |
| customers.csv | pincode | pincode is not 6 digits | Validity | Warning | 3 | C00132 | pincode 6 digits |
| customers.csv | loyalty_tier | loyalty_tier casing differs (BRONZE, bronze, SILVER) | Consistency | Warning | 8 | C00199 | standardise to Title case |
| customers.csv | signup_date | signup_date in the future (Nov 2026 - Jan 2027) | Timeliness | Critical | 3 | C00400 | signup_date <= run time |
| products.csv | product_id | stale '(old)' catalogue row shares the product_id with the current row | Uniqueness | Critical | 2 | P0030 | one row per product_id; prefer the non-(old) row |
| products.csv | category | category is empty | Completeness | Critical | 3 | P0051 | category not null |
| products.csv | mrp | mrp zero or negative | Accuracy | Critical | 1 | P0109 | mrp > 0 |
| products.csv | cost_price | cost_price above mrp | Accuracy | Critical | 5 | P0019 | cost_price <= mrp |
| stores.csv | zone | zone is empty (S07 BTM Layout) | Completeness | Critical | 1 | S07 | zone not null |
| stores.csv | capacity_orders_per_day | capacity is 0 | Accuracy | Warning | 1 | S12 | capacity > 0 |
| increment.csv | order_status | correction of an existing order (status -> RETURNED); must update, not insert | Consistency | Warning | 10 | ORD000017 | upsert on order_id |

Issues exist in all five data files and in the increment. Customers and orders carry most of them.

### Three questions for the client before building

1. **A master row fails a critical rule but orders keep pointing at it. Do we hold the orders back, or load them with an "unknown" member?** I held them back, which is the safe choice for finance. It costs 326 orders and ₹1,65,737. Fixing store S07's missing zone alone releases 181 of them.
2. **Is a negative `quantity` a return or a keying error?** The 10 negative-quantity orders carry negative amounts and negative "SUCCESS" payments. If they are returns, they belong in a returns flow, not in quarantine.
3. **When a customer appears twice with different phones and areas, which record is current?** I kept the first. Is there a `last_updated` in the CRM? The product file solved it for us (the row named "(old)" loses), but the customer file gives no such hint.

(Also worth asking: should the volume check be allowed to stop a load? See experiment 7.)

---

## 3. Acceptance criteria

| # | Criterion | Proof |
|---|---|---|
| 1 | Report data ready by 09:00 IST Mon–Sat | Both DAGs use `0 9 * * 1-6` with an IST `start_date`, as the brief asks. Airflow shows "At 09:00, Monday through Saturday", next run `2026-10-22T03:30:00Z` = 09:00 IST ([docs/schedule_evidence.txt](docs/schedule_evidence.txt)). The scheduler started the scheduled runs by itself when I unpaused. A run takes 10–13 s, so the data lands at about 09:00:15, ahead of the 09:30 stand-up. |
| 2 | No bad order in finance tables | `etl_fact_orders` holds only rows that passed every Critical check. In experiment 5, five deliberately bad orders were all rejected. |
| 3 | Raw kept untouched | `raw_*` tables: all text columns, plus `_row_num`, `_loaded_at`, `_source_file`, `_run_id`. They are never cleaned or deleted. Rows from a failed attempt of the same run are replaced on retry, to avoid duplicates. |
| 4 | Bad rows set aside with a reason | ETL: `include/quarantine/mohanrajs/<file>_failed_<run_id>.csv` with `failure_reason`, plus the `etl_quarantine_log` table. ELT: `dq_status`/`failure_reason` in `stg_*` and the `mart_rejected_rows` table. |
| 5 | Re-run never double counts | Experiment 2: fact count 2,015 before and after; ELT staging 2,412 before and after. |
| 6 | Incremental load | Experiment 3. |
| 7 | Orders reconcile with payments | `etl_payment_reconciliation` and `mart_payment_reconciliation`: one row per mismatch with a reason. |
| 8 | PII protected | Email and phone are masked in `etl_dim_customer`, `stg_customers` and every mart. The raw layer is the only place with raw PII (see E4/E5). |
| 9 | See whether the run worked | Airflow run history, task logs, and the `pipeline_runs` table (written even on failure). |
| 10 | Recommendation | Section 8. |

---

## 4. Part C: scheduling

The same settings are on both DAGs:

```python
schedule="0 9 * * 1-6"
start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")
catchup=False, max_active_runs=1
default_args = {"retries": 2, "retry_delay": timedelta(seconds=30), "execution_timeout": timedelta(minutes=10)}
```

* **Cron expression:** `0 9 * * 1-6`: 09:00, Monday to Saturday.
* **What the UI shows in UTC:** 03:30 UTC (IST is UTC+5:30). The scheduled run seen in testing is `scheduled__2026-10-21T03:30:00+00:00`, and the next one is `2026-10-22T03:30:00Z`.
* **If the `start_date` had been left in UTC:** the schedule would be evaluated in UTC, so "9 AM" would be 09:00 UTC = 14:30 IST, five hours after Kavya's 9:30 stand-up. Everyone would see the report "late" and the code would look correct. The IST `start_date` makes the DAG timezone-aware, so the cron means Indian time.
* **If the laptop or Docker is off at 9 AM:** nothing runs. With `catchup=False`, when the scheduler returns it creates one run for the most recent missed slot and skips the older ones. I saw this: unpausing the DAGs at 19:38 UTC immediately started `scheduled__2026-10-21T03:30`, and no earlier days. For a daily "as of now" report this is the wanted behaviour. A real deployment needs an always-on host, and alerting on a DAG that did not start, which is a gap today (see the pillars table).
* **Why `catchup=False`:** the report always describes the latest files. Replaying 30 mornings would read the same CSVs 30 times and write the same tables (or, worse, extra raw rows in ELT). For a real historical backfill I would trigger runs deliberately.
* **Retries and timeout:** 2 retries 30 seconds apart absorb a late file or a locked warehouse. Both DAGs write to one DuckDB file, so `nc.connect()` also waits for the other DAG's lock instead of failing. The 10-minute `execution_timeout` makes a hung task fail well before 09:30. The audit task uses `retries=0`, and schema drift raises `AirflowFailException`, since retrying cannot fix it.
* A run takes about 10 s (ETL) and 13 s (ELT) on this data, so the data is ready at about 09:00:15, well before 09:30.
* Alerting (bonus): `on_failure_callback` posts to the webhook in the Airflow Variable `SLACK_WEBHOOK_URL` if it exists, and otherwise prints. No secret is in the repo.

---

## 5. Part D: data quality dimensions

57 checks per run (the exact same names in both pipelines). Critical failures quarantine the row; Warnings load it and log it. A check's `rows_checked` is the batch size.

| Dimension | Checks | Examples | Failed rows (full run, both pipelines) |
|---|---:|---|---:|
| Completeness | 14 | customer_id not null; DELIVERED has delivery_minutes; category not null; volume vs previous run (Warning) | 37 |
| Uniqueness | 5 | order_id, payment_id, customer_id, product_id, store_id unique | 23 |
| Validity | 16 | status/payment_mode lists; email, phone, pincode formats; discount 0–100; timestamp parses | 37 |
| Accuracy | 11 | quantity > 0; amount formula; delivery under 120 min; mrp > 0; cost ≤ mrp; unit price within 1.1 × MRP (Warning) | 57 |
| Consistency | 5 | order → customer/product/store exist in the *clean* masters; payment → order exists; loyalty-tier casing | 350 |
| Timeliness | 6 | no future `order_ts`, `signup_date`, `payment_ts`, `launch_date`; latest order not stale (Warning) | 18 |

`dq_results` is written on every run, by Python in ETL and by one SQL file in ELT ([30_dq_results.sql](include/sql/elt/30_dq_results.sql)). The query result, with the failing checks of the latest run of each DAG, is in [docs/evidence.md](docs/evidence.md). After the 12-way comparison I fixed one real bug: the staleness check was fooled by junk 2027 timestamps (experiment 9).

---

## 6. Part B: do the ETL and ELT totals match, and where do they differ?

They match exactly for September and after the increment (`compare_pipelines.py`). Both pipelines implement the same rules, so the same rows are accepted. The only intended differences:

| | ETL | ELT |
|---|---|---|
| Rejected rows | Written to quarantine CSV and `etl_quarantine_log`; never reach the warehouse | Stay in `stg_*` flagged `FAIL` (and in `raw_*`), excluded from marts |
| `rows_loaded` in `pipeline_runs` | Clean rows + dimensions written | Every extracted row (all land in raw) |
| Raw copy | None (the source CSV is the only raw copy) | `raw_*` keeps every run |
| Incremental | Upsert on `order_id` in Python/DuckDB | SQL `MERGE INTO stg_orders` |

---

## 7. Part E: data governance

**1. Ownership.** Orders: Operations (the order-management system owner). Payments: Finance, as owner of the gateway export. Customers: CRM owner in Marketing. Products: Category/Catalogue team. Stores: Store Operations. The warehouse tables (`etl_*`, `stg_*`, `mart_*`, `dq_results`, `pipeline_runs`) belong to the data team. This is NammaMart's FDE until they hire one. A change to a quality rule is proposed by the data team and approved by the owner of the dataset *and* the consumer. The 120-minute delivery limit needs the head of Operations (the owner) and Kavya (the COO, who uses the SLA KPI). Finance signs off any rule that changes revenue.

**2. Data dictionary** (ETL tables; the ELT `stg_*` tables hold the same columns plus `dq_status`, `failure_reason`, `warning_reason`):

| Table.column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| etl_fact_orders.order_id | VARCHAR (PK) | Unique order | orders.csv | `ORD######` | No |
| .order_ts / .order_date | TIMESTAMP / DATE | When the order was placed | orders.csv | not in the future | No |
| .customer_id, .store_id, .product_id | VARCHAR | Keys to dimensions | orders.csv | must exist in clean dims | Indirect |
| .quantity, .unit_price, .discount_pct | INT / DOUBLE / INT | Order line | orders.csv | qty > 0; 0 ≤ discount ≤ 100 | No |
| .order_amount | DOUBLE | qty × price × (1 − discount/100) | orders.csv | matches formula ±0.01 | No |
| .payment_mode | VARCHAR | Standardised mode | orders.csv | UPI, Card, Cash on Delivery, Wallet | No |
| .order_status | VARCHAR | Standardised status | orders.csv | DELIVERED, CANCELLED, RETURNED | No |
| .delivery_minutes | DOUBLE | Minutes to deliver | orders.csv | < 120; required if DELIVERED | No |
| .net_revenue | DOUBLE | order_amount if DELIVERED else 0 | derived | ≥ 0 | No |
| .gross_margin | DOUBLE | net_revenue − qty × cost_price | derived | any | No |
| .within_15_min | BOOLEAN | DELIVERED and ≤ 15 min | derived | | No |
| .channel | VARCHAR | App or Web | orders.csv | App, Web | No |
| .\_run_id, .\_loaded_at | VARCHAR, TIMESTAMP | Run that wrote the row | pipeline | | No |
| etl_dim_customer.customer_id | VARCHAR (PK) | Customer key | customers.csv | unique | Indirect |
| .full_name | VARCHAR | Name | customers.csv | | **Yes** (kept, so top customers are readable) |
| .email_masked, .phone_masked | VARCHAR | `pr***@example.com`, `97******21` | customers.csv (masked) | | Masked |
| .city, .area, .pincode | VARCHAR | Location | customers.csv | pincode 6 digits (Warning) | Quasi-identifier |
| .signup_date | VARCHAR/DATE | Signup | customers.csv | not in future | No |
| .loyalty_tier | VARCHAR | Standardised tier | customers.csv | Bronze, Silver, Gold, Platinum | No |
| etl_dim_product.* | | product_id (PK), name, category, sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date | products.csv | category not null; cost ≤ mrp | No |
| etl_dim_store.* | | store_id (PK), name, zone, area, pincode, open_date, manager_name, capacity | stores.csv | zone ∈ North/South/East/West | manager_name is personal |

**3. Lineage.** Take `mart_daily_store_kpis` for 2026-09-01, store S01: net revenue ₹1,410.80 on 3 orders. In ELT I trace it with one query: `SELECT order_id, _source_file, _row_num, _run_id FROM stg_orders WHERE store_id='S01' AND CAST(order_ts AS DATE)='2026-09-01' AND order_status='DELIVERED' AND dq_status<>'FAIL'` returns ORD000013 (28.80), ORD000025 (788.00), ORD000058 (594.00), sum 1,410.80, with their lines (1369, 728, 444) in `orders.csv` loaded by run `final3_elt_full`, and `raw_orders` has the untouched text row. In ETL I can reach the same three `order_id`s and the run (`etl_fact_orders._run_id`), but the fact table does not keep the source line number, so I have to search the CSV by `order_id`. **ELT is easier**, because every layer carries `_source_file`/`_row_num`/`_run_id` and the raw copy stays in the same database. (A fix for ETL would be adding `_source_row` to the fact; I have not done it.)

**4. Personal data.** Personal columns: `full_name`, `email`, `phone` (and `manager_name` in stores); `city`, `area` and `pincode` are quasi-identifiers; `customer_id` is a pseudonymous key. Email and phone are masked (`pr***@example.com`, `97******21`; invalid values become `***` or `**********`) in ETL (Python, `mask_email`/`mask_phone`) and ELT (SQL, [22_stg_customers.sql](include/sql/elt/22_stg_customers.sql)), so the raw values never reach the dimension, staging or marts. Masking, rather than deleting, keeps the columns for support and joins ("is this the same customer", "does it look like a valid phone?") and lets analysts see the data shape, without exposing the value. A masked value is partial and one-way, so access control still matters. The applicable law is India's **Digital Personal Data Protection Act, 2023** (DPDP Act). It expects lawful, consented and purpose-limited processing, data minimisation, reasonable security safeguards, correct and up-to-date data, erasure once the purpose is over, breach notification and a way for people to exercise their rights. The raw layer holds raw PII (see risks).

**5. Access control.** `mart_*` / `etl_*` reporting tables: COO, store managers, finance and analytics; they contain only masked PII. `stg_*`: the data team and analysts (masked, with row-level flags). `raw_*` and the quarantine files: the data team only, because raw `customers` holds unmasked phone numbers and emails (and quarantine CSVs of customers keep the raw row for audit). Nobody outside the data team (and a named privacy officer for audits or a data-subject request) should read raw customers. In DuckDB there is no role system, so this is a process control today; with a server warehouse it becomes grants.

**6. Retention.** Raw: keep as long as the audit and tax obligations need (assume 7 years for order/payment records, to be confirmed with Finance), but partition by run so old runs can be dropped. Raw customer PII only as long as the CRM purpose lasts: on erasure or inactivity, delete the person's raw rows too. Quarantine files: 90 days (until the source is fixed and reloaded), then delete. Masked dimension data: as long as the analysis needs it. After the period: delete, or aggregate and drop identifiers.

**7. Secrets.** `SLACK_WEBHOOK_URL` is an Airflow Variable (`Variable.get`), set in the Airflow UI or as an environment variable (`AIRFLOW_VAR_SLACK_WEBHOOK_URL`), never in a DAG or `.env` file that is committed. `.env` is git-ignored. A webhook URL in a DAG file or on GitHub lets anyone post as NammaMart (and it can be found by scanners within minutes), and git history keeps it forever, so rotating it is the only fix.

**8. Audit: "Pick any rejected order".** Example **ORD000845**.
* *As received:* `raw_orders` (ELT) holds `ORD000845,2026-09-12 09:30:35,C00238,S12,P0101,2,248.0,5,4712.0,UPI,DELIVERED,Web,20` untouched (line 771 of `orders.csv`). The ETL quarantine file has the same row: [docs/quarantine_sample_orders_failed.csv](docs/quarantine_sample_orders_failed.csv).
* *Rule broken:* `orders_amount_matches_formula`: 2 × 248 × 0.95 = 471.20, but the amount is 4,712.00 (a misplaced decimal point). `failure_reason` in `stg_orders`, `etl_quarantine_log`, `mart_rejected_rows`.
* *Not in the CFO's total:* `SELECT count(*) FROM etl_fact_orders WHERE order_id='ORD000845'` returns 0, and so does `v_clean_orders` (the ELT mart input). Both revenue totals reconcile to each other.
ETL answers from the quarantine file and log (the raw CSV is the only raw copy); ELT answers from one database.

**9. Change history.** An order DELIVERED on 3 Sept and RETURNED on 1 Oct. After the incremental load **both pipelines show the order as RETURNED with net revenue 0**, so September revenue drops from ₹9,86,063.90 to ₹9,82,401.20. Finance wants the corrected number for the month in which the cash was refunded, and probably also "as originally reported" for the closed September books. Both views are kept: `etl_order_status_history` and `stg_order_status_history` store each change (order, old/new status, old/new revenue, run, time). Nine such rows exist, e.g. ORD000337 (₹1,748.00 → 0). The original row remains in `raw_orders` from the September run. A "revenue as reported on date X" view is a join of the history table onto the fact. The proper long-term design is a bi-temporal fact or an SCD2-style history (I did not build SCD2).

---

## 8. Part F: observability

All experiments ran on the real scheduler (LocalExecutor, Postgres) via the REST API, on both DAGs; logs are in [docs/experiments/](docs/experiments/). The files were restored after each one (a baseline run followed the destructive ones). Timings are from the task start/end times Airflow stores.

| # | Experiment | Expected | Observed | Where I saw it | What I would change |
|---|---|---|---|---|---|
| 1 | Happy path | Both green; extracts in parallel | ETL 15 tasks, ≈10 s. All 5 extracts start in the same millisecond; then 4 validators run in parallel, `validate_orders` waits for the three dimensions. ELT 16 tasks, ≈13 s: 5 parallel extracts, then a deliberate chain (one DuckDB writer). Per-file rows in/passed/quarantined (ETL): orders 2412/2015/397, payments 2228/2197/31, customers 405/397/8, products 152/142/10, stores 15/14/1. | Grid/Graph view; `docs/experiments/exp1_*_tasks.txt`; `[Validate]` log lines | Nothing; parallel loads of DuckDB would not help |
| 2 | Re-run | Facts unchanged; raw grows in ELT | `etl_fact_orders` 2,015 → 2,015, `stg_orders` 2,412 → 2,412, revenue ₹9,86,063.90 both times. `raw_orders` 4,824 → 7,236 (+2,412), raw is append-only by design: one copy per run (a history). | `docs/experiments/exp2_*.txt` | Raw grows without limit: add retention/partition-by-run (section 8 risk) |
| 3 | Incremental | 10 corrections update, 150 new insert, no duplicates | ETL: 160 read, 140 passed → 131 inserted + 9 updated, 20 quarantined; fact 2,015 → 2,146. ELT MERGE: `stg_orders` 2,412 → 2,562 (+150, 19 of them flagged FAIL), 9 status corrections, 0 duplicate `order_id`. Re-running the increment: 0 inserted, 0 updated. 1 of the 10 corrections (ORD000822) is quarantined because its product has no category, so the old row stays as it was. | `exp3_after.txt`, `Load` / `Staging` log lines, history tables | Alert when a correction is rejected: it silently leaves the old value in place |
| 4 | Missing `products.csv` | Retries, then fail; downstream blocked | `extract_products`: try 1 failed at +4 s (`up_for_retry`), try 2 at +33 s, try 3 failed at +66 s (`failed`). All downstream tasks `upstream_failed`. `run_audit` ran (`all_done`), wrote `FAILED` to `pipeline_runs` and failed the run. No load task ran, so the warehouse tables were untouched. | `exp4.txt`, Grid view colours, `pipeline_runs` | The only alert was the log and the red run; Slack is wired but unset. Set `SLACK_WEBHOOK_URL`. |
| 5 | 5 new bad rows | Each in quarantine with its reason | All five rejected, none in fact or marts: `ORD990001 orders_customer_id_not_null`, `990002 orders_quantity_positive`, `990003 orders_store_in_clean_stores`, `990004 orders_status_valid`, `990005 orders_order_ts_not_future`. `dq_results` moved by exactly +1 on those five checks, in both pipelines. First attempt: ELT failed at `load_raw` because I appended LF rows to a CRLF file and DuckDB's CSV sniffer rejected the mixed line endings. ETL (pandas) did not notice. I made the raw load tolerant (`strict_mode=false`), since the raw layer should accept what arrives. | `exp5.txt`, `etl_quarantine_log`, `stg_orders` | Alert on a *jump* in rejects, not only on the counts |
| 6 | Rename `order_amount` → `total` | Loud failure | `extract_orders` failed on try 1 (no retry), error `Schema drift in orders: missing columns ['order_amount']; unexpected columns ['total']`. Nothing loaded; fact unchanged. Before I made it fail fast, `validate_payments` (which also reads `orders.csv`) retried 3 times for the same error: fixed. | Task log, `exp6_tasks.txt`, `pipeline_runs` = FAILED | Check schema on every file read, not just in extract |
| 7 | Only 50 orders | Warn on a >90% drop | Before I added the check nothing warned. After: `orders_volume_vs_previous_run` = WARN (1 of 1) in both pipelines. But the run **succeeded** and the fact table was replaced by 47 orders (revenue ₹9,86,064 → ₹23,047). | `exp7.txt`, `dq_results` | Make the volume check a blocking Critical gate (with a manual override), since a warning alone does not protect the CFO's table |
| 8 | Price × 100 for store S10 | A check catches it | (A) only `unit_price` changed: the formula check caught it (174 failures against 18 in the baseline); all S10 rows quarantined, S10 revenue ₹75,450.55 → 0, total ₹9,86,064 → ₹9,10,613. (B) `order_amount` consistently rescaled too: **no Critical check fired**. S10 revenue ₹75,450.55 → ₹75,45,055.00 (×100), total ×8.6 to ₹84,55,668. I added `orders_unit_price_within_mrp` (Warning): it flags 152 rows in both pipelines. | `exp8.txt`; `mart_daily_store_kpis` S10 | Distribution checks per store/day (revenue vs trailing average) as a Critical gate |
| 9 | Latest order 30 days old | Timeliness warns | My first run did **not** warn: the stale check used `max(order_ts)`, which included the junk 2027 rows. Fixed to use the newest non-future timestamp. After: baseline PASS, 30-days-old file WARN (1), in both. | `exp9.txt`, `dq_results` | A freshness check on `payments` too |
| 10 | Recovery after a failure | Only failed and downstream tasks re-run | Using *Clear* (with downstream) on `extract_products` of the experiment-4 runs: ETL re-ran 8 tasks (`extract_products`, `validate_products`, `validate_orders`, `transform`, `load`, `reconcile`, `report`, `run_audit`); the other 7 kept their original success. ELT re-ran 12 and kept the 4 other extracts. Both runs ended green. | `exp10.txt` | None; idempotent loads are what make this safe |

### Five pillars

| Pillar | Coverage | Strongest / weakest |
|---|---|---|
| Freshness | `orders_latest_order_not_stale` (Warning); `pipeline_runs.end_time` | Partial: payments freshness is not checked, and nothing alerts if the DAG does not start |
| Volume | `orders_volume_vs_previous_run` (Warning); row counts per file in `pipeline_runs` and logs | Partial: warns but does not block (experiment 7) |
| Schema | Column contract in `EXPECTED_COLUMNS`, fail-fast (experiment 6); typing errors become quality failures | **Strongest** |
| Distribution | Only ranges and the unit-price/MRP guard | **Weakest**: a consistent 100× error passed all Critical checks (experiment 8B) |
| Lineage | ELT: `_source_file`/`_row_num`/`_run_id` through all layers; ETL: `_run_id` and the quarantine rows | Good in ELT, weaker in ETL |

---

## 9. Part G: recommendation to Sameer (CTO)

**Adopt ELT: land every raw file, then clean and gate in SQL. Keep ETL's discipline on the reporting layer.**

In our tests the two pipelines gave identical revenue, margins, reconciliation and quarantine decisions, to the paisa, so the choice is not about correctness. Finance gets the same guarantee either way: the marts read only `dq_status <> 'FAIL'` rows.

* **Auditability.** ELT answered the finance question ("show the rejected order exactly as received") from one database, with source line and run id on every layer. ETL has to go to a CSV and a quarantine file. Raw also lets us replay history.
* **Quality guarantees.** Equal. ETL's rejected rows never enter the warehouse; ELT's stay flagged in staging and are excluded from the marts. ELT puts more faith in a single filter, so the mart filter is tested (experiment 5).
* **Ease of change.** Changing a rule (say the 120-minute limit) is one SQL edit and a replay from raw. In ETL, the file must be read again, which fails once the export has gone from the drive. SQL also reads more easily for analysts than pandas code.
* **Performance at 1000×.** About 2.4M orders a month would be 2.4B. Pandas holds each file in memory, which does not scale; ELT pushes the work to the warehouse engine. At today's size it makes no difference (10 s against 13 s). At 1000× DuckDB's single writer and full staging rebuild stop working, so we would move to a server warehouse and make staging incremental.
* **Cost.** Both are free. ELT stores every run: after about 40 runs `raw_orders` held about 43,000 rows for 2,412 orders. That is cheap at this size but needs retention.

**Top 3 risks of this design**
1. **Raw PII.** The ELT raw layer holds unmasked emails and phones, which is the one place the DPDP-style masking does not reach. Restrict `raw_customers`, mask on landing, or drop the PII columns we do not need.
2. **Silent bad data and strict masters.** A consistent error (experiment 8B) or a stale feed passes the Critical checks, and a missing store zone withholds ₹1,65,737. Add distribution checks as a blocking gate and give the master-data owners a daily "fix this" list.
3. **Scale and operations.** Raw grows without limit, one DuckDB writer can serialise the two DAGs, and a laptop or container that is off at 9 AM means no report. Move to an always-on host and a server warehouse, set retention, and alert on a DAG that does not start.

---

## 10. How to reproduce

```bash
astro dev start                                  # Airflow UI on http://localhost:8080 (admin / admin)
astro dev run dags list-import-errors            # no import errors
astro dev run dags test nammamart_etl_mohanrajs      # or trigger from the UI; use Trigger w/ config {"load_increment": true} for the increment
astro dev run dags test nammamart_elt_mohanrajs
python scripts/compare_pipelines.py              # ETL vs ELT parity (add --post-increment after the increment)
python scripts/export_evidence.py                # rewrites docs/evidence.md
python scripts/profile_data.py                   # rewrites the data issues log
```

`scripts/run_local.py` runs a DAG's task bodies without Airflow (used for fast iteration); `scripts/af.py` and `scripts/counts.py` helped drive and record the experiments.

Design notes worth knowing:
* `load_increment=False` (default) loads `orders.csv`, and a *full* run rebuilds the fact from that file, so a full run after an incremental one undoes the increment. In production the file name would carry the run date.
* "Today" for the timeliness checks is the Airflow Variable `nammamart_as_of` (default `2026-10-02 09:00`, the morning after the increment); set it to `now` in production.
* Bonus items: the Slack failure alert (Variable-based) is done. Not done: SCD2 customer dimension, store-capacity check, DQ trend chart.
