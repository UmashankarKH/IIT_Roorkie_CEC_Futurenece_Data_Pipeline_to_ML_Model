if grep -q "^## Part D: Data quality" SUBMISSION.md; then echo "Parts D-G already in SUBMISSION.md"; else
cat >> SUBMISSION.md <<'EOF'

## Hardening added after the observability experiments

Experiments 6 to 9 exposed weaknesses; each one was fixed and the experiment re-run (results below).

- **run_audit now fails the run** after writing its audit row, so a failed pipeline shows RED in Airflow
  (before, run_audit was the only last task and always succeeded, so failed pipelines looked green).
- **Schema contract** in every extract task: expected columns and order; no retries for a contract error.
- **Volume gate** (`orders.volume_vs_recent_runs`, Critical): stops a run whose orders file has fewer than
  50% of the rows of the largest of the last 7 runs. Raw still keeps the file as received.
- **Fact clean-up**: an order that now fails validation is removed from `etl_fact_orders`, so the fact only
  ever holds rows that pass every Critical check (acceptance criterion 2).
- **Freshness check** ignores future-dated rows when finding the newest order.
- **Pool `duckdb_writer` (1 slot)**: warehouse writers take turns (DuckDB allows one writer at a time).
- **One clock**: all audit timestamps are IST (the ELT scorecard used the container's UTC clock before).

## Part D: Data quality dimensions (both pipelines)

Checks live in `include/dq_checks.py` (Python, ETL) and `include/sql/stg_*.sql` (SQL, ELT). Every check is
written to `dq_results` on every run (run_id, dag_id, check_name, dimension, severity, rows_checked,
rows_failed, status, checked_at). Critical rows are quarantined (ETL) or marked REJECTED (ELT); Warning rows
still load and are logged. Latest runs: ETL 25 FAIL / 34 PASS / 9 WARN (68 checks, including the
increment file); ELT 25 FAIL / 14 PASS / 8 WARN (47 checks). Appendix D shows the table.

| Dimension | Checks (file) | Severity |
|---|---|---|
| Completeness | ids not null in every file; DELIVERED has delivery_minutes; category not null; volume gate | Critical |
| Uniqueness | order_id, payment_id, customer_id, product_id, store_id unique (first copy kept) | Critical |
| Validity | order_status and payment_mode in allowed lists; discount_pct 0-100; order_ts parses; zone allowed | Critical |
| Validity | email format, phone 10 digits, pincode 6 digits; payment_mode needs standardising (e.g. upi) | Warning |
| Accuracy | quantity and price > 0; amount = qty x price x (1 - discount); delivery < 120 min; mrp > 0; cost <= mrp | Critical |
| Accuracy | delivery_minutes outside 8-35; store capacity > 0 | Warning |
| Consistency | customer_id, product_id, store_id exist in their masters; every payment has an order | Critical |
| Consistency | loyalty_tier casing; SUCCESS payment amount = order amount (goes to reconciliation) | Warning |
| Timeliness | no future-dated order_ts, payment_ts, signup_date, launch_date | Critical |
| Timeliness | newest order not older than 1 day | Warning |

## Part E: Data governance

**1. Ownership.**

| Dataset | Business owner | Technical steward |
|---|---|---|
| orders, stores | Head of Operations (COO) | data engineer / FDE |
| payments | Finance (CFO) | data engineer |
| customers | CRM / customer team, with the Data Protection Officer for privacy | data engineer |
| products | Category / merchandising lead | data engineer |
| warehouse tables (raw, staging, marts, dq_results) | CTO (platform); content owned by the source's owner | data engineer |
| marts: revenue, margin, reconciliation | CFO (finance numbers); COO (operations KPIs) | data engineer |

A change to a quality rule such as the 120-minute delivery limit is a business definition: Operations
proposes it, the COO and Finance approve it (it changes what counts as a valid delivery and an SLA hit),
and the data engineer implements it through a reviewed git change (`MAX_DELIVERY_MIN` in
`include/nammamart_config.py`). The git history is the approval trail.

**2. Data dictionary.** Types: text, number, date, timestamp, boolean. PII: Y = personal data,
M = masked/derived from personal data, Q = quasi-identifier (can identify a person in combination), N = no.

`etl_fact_orders` (one row per clean order; source `orders.csv` + increment file)

| Column | Type | Meaning | Allowed values | PII |
|---|---|---|---|---|
| order_id | text | order key | unique | N |
| order_ts | timestamp | when the order was placed (IST) | not in the future | N |
| order_date | date | date part of order_ts | | N |
| customer_id | text | who ordered (CRM id) | exists in customers | Q |
| store_id | text | dark store | exists in stores | N |
| product_id | text | product | exists in products | N |
| quantity | number | units | > 0 | N |
| unit_price | number | price per unit (INR) | > 0 | N |
| discount_pct | number | discount percent | 0-100 | N |
| order_amount | number | quantity x unit_price x (1 - discount/100) | matches formula | N |
| payment_mode | text | how the customer pays | UPI, Card, Cash on Delivery, Wallet | N |
| order_status | text | order outcome | DELIVERED, CANCELLED, RETURNED | N |
| channel | text | ordering channel | e.g. App | N |
| delivery_minutes | number | minutes to deliver | < 120 (normal 8-35) | N |
| net_revenue | number | order_amount if DELIVERED, else 0 | | N |
| gross_margin | number | order_amount - quantity x cost_price if DELIVERED, else 0 | | N |
| within_15_min | boolean | DELIVERED and delivery_minutes <= 15 | | N |

`etl_dim_customer` (source `customers.csv`; full_name, raw email and raw phone are NOT loaded)

| Column | Type | Meaning | Allowed values | PII |
|---|---|---|---|---|
| customer_id | text | CRM key | unique | Q |
| email_masked | text | first 2 letters + *** + domain | pr***@example.com | M |
| phone_masked | text | first 2 and last 2 digits | 98******21 | M |
| city, area, pincode | text | location of the customer | pincode = 6 digits | Q |
| signup_date | date | when the customer joined | not in the future | N |
| loyalty_tier | text | loyalty level | Bronze, Silver, Gold, Platinum | N |
| is_active | text | active flag | | N |

`etl_dim_product` (source `products.csv`): product_id (unique key), product_name, category (not null),
sub_category, brand, mrp (> 0), cost_price (<= mrp), unit, is_perishable, launch_date. No personal data.

`etl_dim_store` (source `stores.csv`): store_id (unique key), store_name, zone (North, South, East, West),
area, pincode, open_date, manager_name (**personal data: employee name**), capacity_orders_per_day.

**3. Lineage.** Appendix A traces one number on `mart_daily_store_kpis` down to a source row.
In ELT it is one chain of joins because staging carries `_source_file`, `_run_id` and `_loaded_at`
for every row and raw keeps the row exactly as received. In ETL the fact table has no source columns, so
the trace ends at the order id and a person must search the CSV. **ELT is easier**; the ETL fix is to add
those three columns to the fact table.

**4. Personal data.** Personal data in the dataset: customers.full_name, email, phone; city, area and
pincode (location); customer_id (identifies a person when linked to orders and purchases);
stores.manager_name (employee). The ETL dimension and the ELT staging table drop full_name, raw email and
raw phone and keep only masked email (`pr***@example.com`) and masked phone (`98******21`). Masking, not
deleting, keeps the customer usable for counting, joining and de-duplicating, and keeps format problems
visible, while hiding the identifying part. Masked data is still personal data (customer_id plus area plus
order history can re-identify someone). The law that applies is India's **Digital Personal Data Protection
Act, 2023** (and the Rules made under it). NammaMart is the "data fiduciary" and is expected to: process data
only for a stated purpose with notice and consent (or a permitted legitimate use); collect only what is
needed; keep it accurate; protect it with reasonable security safeguards; report breaches to the Data
Protection Board and to the people affected; erase it when the purpose is served or consent is withdrawn; and
honour access, correction, erasure and grievance requests. Penalties for failing to protect data can reach
INR 250 crore. (This is an engineering summary, not legal advice; confirm details and dates with counsel.)

**5. Access control.**

| Layer | Who may read |
|---|---|
| raw (including raw_customers) | pipeline service account and named data engineers only; every read logged |
| raw_customers phone / email | nobody for analysis; break-glass only with DPO approval, logged (see note below) |
| staging (masked) | data engineers and analysts |
| marts | COO, finance, store managers, read-only through a BI tool (store managers see only their store) |
| quarantine files / tables | data engineers and the data owner who must fix the source; customer rows are written masked |

Note: marketing gets contact details from the CRM, not the warehouse. Recommended: tokenise or encrypt
the phone and email columns at load, and keep Finance's statutory retention for financial records.

**6. Retention.**

| Data | Keep | After that |
|---|---|---|
| raw tables | 13 months rolling (a year of replay plus one audit cycle) | delete older runs |
| quarantine files | 90 days after the issue is fixed or accepted | delete and log the deletion |
| masked customer data | while active, plus 12 months after the last order | delete or anonymise; erase on request |
| dq_results, pipeline_runs | 24 months (small, needed for trends and audits) | archive |

Why it matters here: ELT raw appends a full copy of every file on every run. `raw_orders` grew from 17,684
to 58,621 rows during our testing alone (each run adds 2,572 rows) without any new orders.

**7. Secrets.** This project needs no credentials (local DuckDB and CSV files). The bonus failure alert
would use `SLACK_WEBHOOK_URL`, stored as an **Airflow Variable** (Admin > Variables, or
`AIRFLOW_VAR_SLACK_WEBHOOK_URL` in the git-ignored `.env` file) and read with `Variable.get()`. Astro's
default `.gitignore` excludes `.env`, and `include/*.duckdb` is excluded too (the raw layer holds real
phone numbers). A secret in a DAG file or on GitHub stays in git history for ever, is copied by every
clone and fork, and is harvested by scanners within minutes; a leaked secret must be rotated.

**8. Audit ("show me a rejected order").** Appendix B does this for a real rejected order: the row exactly as
received (ELT raw, and the ETL quarantine file), the rule it broke (`failure_reason`), and proof it is not
in revenue (0 rows in `etl_fact_orders`, 0 non-rejected rows in staging, identical totals).

**9. Change history.** After the incremental load, 10 orders changed from DELIVERED to RETURNED. September
net revenue fell from 1,134,343.60 to 1,130,149.90 (-4,193.70) in **both** pipelines, because both overwrite
the status. Finance normally wants the September number it already reported and signed off to stay fixed,
and the return booked in October as a deduction; operations and analytics want the corrected number.
To keep both views: never overwrite a status without recording it. Keep an order-status history table
(order_id, status, valid_from, valid_to; a Type 2 history) or an events table fed from each increment, and
publish two marts: **as reported** (frozen at month end) and **as corrected**. ELT already holds the raw
history because raw is append-only (Appendix C shows both versions of one order, each stamped with its file
and run). The ETL fact keeps only the latest version, so ETL cannot reproduce the original view.

## Part F: Observability tests (every experiment run on both DAGs; files restored after each)

Tools: `exp.py` (applies and restores each data change; every restore was checked byte for byte),
`run_experiments.py` (runs both DAGs and records the result in `experiments_log.txt`), `exp_report.py`.
Two baselines are quoted: simultaneous start before the DuckDB pool existed, and after.

**Experiment 1: Happy path.**
- Expected: both DAGs green well before 9:30; the five extract tasks run in parallel; row counts match profiling.
- Observed: started together (as at 9:00), ETL 26.3 s and ELT 27.5 s; alone 5-6 s. **Before** the pool, the same
  simultaneous start took 127.8 s (ETL) and 380.4 s (ELT): both wrote the one DuckDB file, tasks failed on the
  lock and waited out the 2-minute retry delay. ETL rows in / passed / quarantined: orders 2,412 / 2,309 / 103;
  payments 2,228 / 2,198 / 30; customers 405 / 397 / 8; products 152 / 142 / 10; stores 15 / 14 / 1; increment
  160 / 160 / 0. ELT rejected in staging: 91 / 26 / 8 / 10 / 1 (the difference of 16 rows set aside, 152 vs 136, is
  12 duplicate orders + 4 duplicate payments that ELT leaves in raw only). Parallel: the five extract tasks.
- Where I saw it: Airflow graph and grid view, Task Instances durations, "Last 10 Dag Runs" bar chart (one
  2-minute bar, then tiny bars after the pool), `pipeline_runs`, `exp_report.py`.
- What I would change: keep the pool, schedule at 8:30 not 9:00, and move to a client-server warehouse so
  writers need not queue.

**Experiment 2: Re-run (idempotency).**
- Expected: fact table unchanged; ELT raw grows by one copy per run.
- Observed: `etl_fact_orders` 2,459 to 2,459 (+0); `stg_orders` 2,550 (+0); `raw_orders` +2,572 per run
  (2,412 + 160); `pipeline_runs` +2 and `dq_results` +113 rows (one run each). This is correct: raw is
  append-only history, reporting tables are rebuilt or upserted.
- Where I saw it: `experiments_log.txt` (CHANGE line), row counts in `exp_report.py`.
- What I would change: add raw retention (90 days of full snapshots) and load only new files at scale.

**Experiment 3: Incremental load.**
- Expected: 10 corrections update existing orders, 150 new orders are added, no duplicates.
- Observed: September-only load gave 2,309 fact rows and 2,400 staging rows with the 10 orders DELIVERED. With
  the increment both pipelines logged "10 updated, 150 inserted"; fact 2,459, staging 2,550; the 10 orders became
  RETURNED; 0 duplicate order_ids after repeated merges; the 160 rows were appended to raw as received.
- Where I saw it: task log lines `increment: ... 10 updated, 150 inserted` in both DAGs.
- What I would change: store inserted/updated counts in `pipeline_runs`, and keep status history (Part E.9).

**Experiment 4: Missing source file (products.csv renamed).**
- Expected: the extract task fails, retries twice 2 minutes apart, downstream tasks do not run, the run is
  visibly failed and the audit says FAILED.
- Observed: ETL: `extract_products` failed with FileNotFoundError after 3 attempts over 248.9 s (try 1 + 2
  retries, 2-minute delay); the other four extracts succeeded; downstream tasks were upstream_failed;
  `run_audit` wrote FAILED (extracted 5,060, loaded 0). ELT: `extract_products` failed the same way after
  251.5 s; [[EXP4_ELT]] Nothing sent an alert: the only signal was the red run in the Airflow UI.
- Where I saw it: Airflow grid (up_for_retry, failed, upstream_failed), Task Instances tab (Try Number),
  task log, `pipeline_runs`.
- What I would change: retrying a file that is simply absent wastes 4 minutes; use a file sensor that waits
  until 8:45 and then alerts; add an email or Slack failure callback with the webhook in an Airflow Variable.

**Experiment 5: New bad data (5 planted orders).**
- Expected: each lands in quarantine / REJECTED with the right reason; dq_results counts rise by one.
- Observed: all 5 caught with exactly one correct reason each: ORDTEST1 customer_id_not_null, ORDTEST2
  quantity_positive, ORDTEST3 store_id_exists, ORDTEST4 order_status_allowed, ORDTEST5 order_ts_not_future.
  None reached `etl_fact_orders` (0) or the marts (0). `dq_results.rows_failed` rose by exactly 1 per check in
  both pipelines (18 to 19, 10 to 11, 5 to 6, 3 to 4, 5 to 6). The runs stayed green, by design.
- Where I saw it: quarantine file `orders_failed_<run_id>.csv`, `stg_orders.failure_reason`, `dq_results`.
- What I would change: alert the data owner when quarantined rows exceed a threshold (for example 5% of a file).

**Experiment 6: Schema change (order_amount renamed to total).**
- Expected: a loud failure at the first task that needs the column.
- Observed (first attempt, no guard): ETL burned 248 s of retries in the validation stage and the DAG run was
  still GREEN, because `run_audit` was the only last task and succeeded (the audit row said FAILED). ELT
  loaded the renamed file silently: raw loads by position, the values landed in the old column, revenue did not
  move. After the fix (schema contract plus run_audit failing the run): ETL failed in 9.0 s and ELT in 14.6 s with
  the missing and unexpected column named; audit rows FAILED; `raw_orders` +0; revenue unchanged; runs RED.
- Where I saw it: `experiments_log.txt`, `pipeline_runs`, task log message "schema drift in orders.csv".
- What I would change: send the schema-drift message to the owner of the source export and version the contract.

**Experiment 7: Volume anomaly (50 orders instead of 2,412).**
- Expected: a warning; no change to the reports.
- Observed (before): no warning at all. ELT revenue fell from 1,210,981.9 to 102,755.8 because staging is
  rebuilt from the file, ETL kept its old rows, so the two pipelines silently disagreed. After adding the
  volume gate: both stopped; `dq_results` row `orders.volume_vs_recent_runs` = FAIL; audit FAILED; fact 2,459 and
  staging 2,550 untouched; revenue ETL = ELT = 1,210,981.9; the 50 received rows still in `raw_orders` (+50).
- Where I saw it: `dq_results`, `pipeline_runs`, `experiments_log.txt`.
- What I would change: apply the gate to every file, compare with the same weekday, and notify the owner.

**Experiment 8: Distribution drift (unit_price x100 for store S05, 144 orders).**
- Expected: some check or mart metric reveals it.
- Observed: the accuracy check `order_amount_matches_formula` caught the rows (rows_failed 18 to 162 in ETL, 18 to
  160 in ELT) and set them aside, so S05 revenue fell from 86,163.65 to 8,785.20 (-89.8%) and total revenue from
  1,210,981.9 to 1,133,603.45 (-6.4%) in both pipelines. The runs stayed GREEN. Before the fact clean-up, ETL
  kept the stale S05 rows while ELT dropped them. If price and amount had both been multiplied, nothing would
  have fired.
- Where I saw it: `dq_results`, `mart_daily_store_kpis` (store S05 net_revenue).
- What I would change: add a distribution check (a store's daily revenue against its trailing average, alert
  beyond 30%) and escalate when more than 5% of one store's rows fail one rule.

**Experiment 9: Freshness (latest order 30 days old).**
- Expected: the timeliness check warns.
- Observed: the first version said PASS before and after, because it took the newest timestamp including
  future-dated bad rows. Fixed to ignore future timestamps: now WARN in both pipelines (data ends 21 Oct before,
  9 Oct after). The check is yes/no, so it cannot tell 4 days from 18 days, and with September data it warns on
  every run. The experiment also revealed two leftover rows in the ETL fact, fixed by the clean-up above.
- Where I saw it: `dq_results` row `orders.latest_order_not_stale`, newest order_ts in the fact table.
- What I would change: record the age in days; WARN above 1 day, FAIL above 3 days on a weekday.

**Experiment 10: Recovery (Clear the failed task).**
- Expected: only the failed task and the tasks after it run again; tasks that succeeded are not repeated.
- Observed: [[EXP10]]
- Where I saw it: Airflow grid after Clear Task, with the file restored.
- What I would change: add a failure callback so the owner is told to clear the task as soon as the file arrives.

**Five pillars of data observability**

| Pillar | What covers it in this setup | How well |
|---|---|---|
| Freshness | `latest_order_not_stale` (Warning, yes/no); run times | partial: no age, warns every day |
| Volume | rows per file in `dq_results`; volume gate added after exp 7 | good for orders only |
| Schema | schema contract at every extract (added after exp 6) | good |
| Distribution | validity and accuracy rules only | **weakest**: exp 8 passed on a green run |
| Lineage | ELT raw and staging carry `_source_file`, `_run_id`, `_loaded_at` | **best** in ELT; the ETL fact has none |

## Part G: Recommendation to the CTO

**To:** Sameer (CTO). **From:** FDE team. **Re:** ETL or ELT for NammaMart.

**Recommendation: adopt ELT as the main pipeline, with the ETL-style safety gates kept in front of the
reports.** Land every file exactly as received (raw), clean and flag it in SQL (staging), and let finance read
only the marts. Keep the schema contract and the volume gate in front of staging. Retire the ETL DAG after
finance has signed off one month of ELT marts, and keep the quarantine report as a daily "what was
rejected" email.

**Why, based on what we measured**
- **Same answers.** On the full dataset all 8 headline checks matched (net revenue 1,210,981.90, 330
  reconciliation rows, 212,443.75 not reconciling) and matched again after every experiment, so correctness
  does not decide this.
- **Auditability: ELT wins.** ELT kept every received row, including the 50-row bad file and both versions
  of the 10 returned orders, each stamped with file and run. The ETL fact holds only the latest clean row.
  "Show me the rejected order exactly as received" is one query in ELT (Appendix B).
- **Quality guarantee: ETL wins on paper, tie in practice.** ETL cannot leak a bad row because it never loads
  it. In ELT bad rows sit in staging flagged REJECTED and every mart must filter them. In experiment 5, 0 of 5
  planted rows leaked in either pipeline. The risk is a future query that forgets the filter; give finance a
  view that already applies it.
- **Ease of change: ELT wins.** Rules are SQL files that can be re-run over raw history without re-reading
  the sources. In ETL a rule change only affects new loads; rows already in the fact keep the old verdict (we
  saw leftover rows after experiment 9 and had to add a clean-up step).
- **1000x growth** (2.4k to 2.4M orders a month): ETL holds each file in pandas memory on the Airflow worker,
  which breaks first. ELT pushes the work into the warehouse engine. The catch: our raw layer re-loads a full
  snapshot every run (2,572 rows per run; 58,621 raw rows after two days of testing), so before growth it
  needs incremental raw loads and a retention job.
- **Cost.** Both use only open-source tools. ELT stores extra copies (cheap at this size). The hidden cost
  we hit was DuckDB's single writer: simultaneous 9:00 runs took 128 s and 380 s until writes were queued.

**Top 3 risks of the recommended design**
1. **Raw holds real personal data and grows for ever.** `raw_customers` has unmasked phone numbers and
   emails; under the DPDP Act a leak is a reportable breach. Mitigation: raw readable only by the pipeline
   account, tokenise or encrypt PII at load, 13-month retention, an erasure procedure.
2. **One machine at 9:00 AM.** One DuckDB writer and one Airflow on one laptop: if it is off, nothing runs and
   `catchup=False` does not recover the day. Mitigation: an always-on server, a client-server warehouse, an
   alert if the 9:00 run has not started by 9:10, and a schedule of 8:30.
3. **Gates catch broken rows, not wrong-but-valid numbers.** In experiment 8 a store lost 90% of its revenue on
   a green run, and the freshness check warns every day so people will ignore it. Mitigation: distribution
   checks against history, freshness as age in days, and a named owner who reads the daily rejected-rows summary.
EOF
echo "Parts D, E, F and G added to SUBMISSION.md"; fi
