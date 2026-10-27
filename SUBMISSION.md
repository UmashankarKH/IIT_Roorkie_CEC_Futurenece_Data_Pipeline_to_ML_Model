## Part 0: Data Issues Log

Row counts: orders 2,412 | payments 2,228 | customers 405 | products 152 | stores 15 | increment 160.

Every issue below was found by running the same rules the pipelines use. The full row-by-row list is in `data_issues_log_full.csv` (file, row key, CSV line, column, problem, dimension, severity).

| File | Column | Problem | Rows | Example keys | Dimension | Proposed rule | Severity |
|---|---|---|---|---|---|---|---|
| orders | customer_id | customer_id is missing | 18 | ORD002255, ORD000701, ORD000997, ORD000580 ... | Completeness | `customer_id_not_null` | Critical |
| orders | order_amount | order_amount != quantity x unit_price x (1 - discount_pct/100) | 18 | ORD002103, ORD002305, ORD002004, ORD000845 ... | Accuracy | `order_amount_matches_formula` | Critical |
| orders | order_id | duplicate order_id (repeat of an earlier row) | 12 | ORD001462, ORD001441, ORD001692, ORD002129 ... | Uniqueness | `order_id_unique` | Critical |
| orders | payment_mode | payment_mode spelling/casing will be standardised | 10 | ORD001812, ORD001414, ORD000159, ORD001364 ... | Validity | `payment_mode_needs_standardising` | Warning |
| orders | quantity | quantity is zero, negative or not a number | 10 | ORD000469, ORD001296, ORD001459, ORD000867 ... | Accuracy | `quantity_positive` | Critical |
| orders | product_id | product_id not found in products | 9 | ORD000986, ORD001058, ORD001685, ORD000070 ... | Consistency | `product_id_in_master` | Critical |
| orders | delivery_minutes | DELIVERED order has no delivery_minutes | 8 | ORD001554, ORD001800, ORD000731, ORD001277 ... | Completeness | `delivered_has_delivery_minutes` | Critical |
| orders | order_ts | order_ts is in the future | 7 | ORD002119, ORD001693, ORD001089, ORD001121 ... | Timeliness | `order_ts_not_future` | Critical |
| orders | order_ts | order_ts is missing or not a valid timestamp | 6 | ORD000403, ORD001610, ORD000534, ORD000362 ... | Validity | `order_ts_valid` | Critical |
| orders | delivery_minutes | delivery_minutes is 120 or more (impossible for a 10-minute service) | 6 | ORD001037, ORD002101, ORD000455, ORD001104 ... | Accuracy | `delivery_minutes_below_120` | Critical |
| orders | store_id | store_id not found in stores | 5 | ORD001799, ORD001046, ORD001492, ORD001275 ... | Consistency | `store_id_in_master` | Critical |
| orders | order_status | order_status is not DELIVERED/CANCELLED/RETURNED | 3 | ORD000500, ORD001970, ORD001637 | Validity | `order_status_valid` | Critical |
| orders | discount_pct | discount_pct is outside 0-100 | 3 | ORD002103, ORD001468, ORD001237 | Validity | `discount_pct_in_range` | Critical |
| orders | order_status | order_status casing/whitespace will be standardised | 2 | ORD001066, ORD000153 | Validity | `order_status_needs_standardising` | Warning |
| payments | (multiple) | SUCCESS amount_paid differs from the order's order_amount (see reconciliation) | 14 | PAY0001568, PAY0000354, PAY0000779, PAY0000323 ... | Consistency | `success_amount_matches_order` | Warning |
| payments | payment_mode | payment_mode spelling/casing will be standardised | 10 | PAY0002188, PAY0001850, PAY0001954, PAY0000817 ... | Validity | `payment_mode_needs_standardising` | Warning |
| payments | amount_paid | amount_paid is zero, negative or not a number | 10 | PAY0001568, PAY0002175, PAY0000116, PAY0000751 ... | Accuracy | `amount_paid_positive` | Critical |
| payments | order_id | order_id not found in orders (orphan payment) | 8 | PAY0002219, PAY0002220, PAY0002223, PAY0002221 ... | Consistency | `order_id_in_orders` | Critical |
| payments | payment_ts | payment_ts is in the future | 7 | PAY0000396, PAY0000127, PAY0001908, PAY0001037 ... | Timeliness | `payment_ts_not_future` | Critical |
| payments | payment_id | duplicate payment_id (repeat of an earlier row) | 4 | PAY0000509, PAY0001651, PAY0002000, PAY0000755 | Uniqueness | `payment_id_unique` | Critical |
| customers | email | email is missing or malformed | 12 | C00337, C00307, C00063, C00045 ... | Validity | `email_valid` | Warning |
| customers | loyalty_tier | loyalty_tier casing will be standardised | 8 | C00199, C00171, C00133, C00032 ... | Consistency | `loyalty_tier_needs_standardising` | Warning |
| customers | phone | phone is not exactly 10 digits | 7 | C00151, C00240, C00095, C00357 ... | Validity | `phone_is_10_digits` | Warning |
| customers | city | city is missing | 6 | C00355, C00394, C00233, C00142 ... | Completeness | `city_not_null` | Warning |
| customers | customer_id | duplicate customer_id (repeat of an earlier row) | 5 | C00103, C00252, C00226, C00036 ... | Uniqueness | `customer_id_unique` | Critical |
| customers | pincode | pincode is not exactly 6 digits | 3 | C00132, C00083, C00166 | Validity | `pincode_is_6_digits` | Warning |
| customers | signup_date | signup_date is in the future | 3 | C00400, C00398, C00038 | Timeliness | `signup_date_not_future` | Critical |
| products | cost_price | cost_price is higher than mrp | 5 | P0019, P0073, P0074, P0109 ... | Accuracy | `cost_price_not_above_mrp` | Critical |
| products | category | category is missing | 3 | P0051, P0060, P0068 | Completeness | `category_not_null` | Critical |
| products | product_id | duplicate product_id (superseded or repeated row) | 2 | P0030, P0140 | Uniqueness | `product_id_unique` | Critical |
| products | mrp | mrp is zero, negative or not a number | 1 | P0109 | Accuracy | `mrp_positive` | Critical |
| stores | zone | zone is missing | 1 | S07 | Completeness | `zone_not_null` | Critical |
| stores | (multiple) | capacity_orders_per_day is zero, negative or not a number | 1 | S12 | Accuracy | `capacity_positive` | Warning |

### Increment file (`orders_2026-10-01_increment.csv`)
No format, key or amount problems. 10 corrections (DELIVERED -> RETURNED, no other column changes) and 150 new orders (ORD002401 to ORD002550). Expected after MERGE: 10 rows updated, 150 inserted, 0 duplicates. September net revenue falls by Rs 4,193.70.

### Cross-file findings
- 71 DELIVERED orders have no SUCCESS payment (27 with no payment at all, 44 with only a FAILED one).
- 3 `Pending??` orders already have a SUCCESS payment.
- 8 payments belong to orders that do not exist (ORD97xxxx and similar).
- 14 SUCCESS payments differ from their order_amount.
- Order unit_price matches the current P0030/P0140 product rows, never the "(old)" rows, so the "(old)" rows are stale.
- All 11 orders on P0109 use unit_price 203 while the catalogue says -203, so the sign is likely a catalogue error.
- Store S07 has a blank zone and S12 has capacity 0 (stores.csv).

### Questions for the client
1. Is a negative or zero quantity a data-entry error or a return? The negative-quantity orders carry matching negative payments.
2. Can a DELIVERED order have only a FAILED payment (for example COD collected later), or is it always an error? This affects 44 orders.
3. For a customer_id that appears twice with different phone and area, which record is current?

---

## Business questions (answered by both pipelines)

Both pipelines give the same answers for all six questions. The SQL is in `include/sql/etl_business_questions.sql` (reads the `etl_*` tables) and `include/sql/elt_business_questions.sql` (reads the `mart_*` tables). The results of each, taken from the run on `orders.csv` only, are saved in `include/results_etl_business_questions.txt` and `include/results_elt_business_questions.txt`. Customer email and phone appear only in masked form.

| # | Question | Answer (both pipelines) |
|---|---|---|
| 1 | Daily net revenue by zone / store | 448 store-days; for example 1 Sept 2026: East ₹10,192.85, North ₹9,217.15, South ₹10,563.05, West ₹6,579.65 |
| 2 | Revenue and margin by category | Staples ₹306,212.55 (margin 23.3%), Personal Care ₹205,016.80 (21.9%), Household ₹199,699.45 (25.6%), Dairy & Bakery ₹169,528.75 (26.4%), Snacks & Beverages ₹161,886.95 (23.3%), Fruits & Vegetables ₹93,284.30 (24.0%) |
| 3 | Average delivery time and % within 15 minutes per store | Averages 20.5 to 22.5 minutes; best store S08 Whitefield (36.0% within 15 minutes), weakest S12 Rajajinagar (21.7%) |
| 4 | Missing source file | `extract_products` goes `up_for_retry` twice (1 minute apart), then `failed`; later tasks are `upstream_failed`; `audit` still runs | I hid `products.csv`. `extract_products` retried and failed; `validate`, `transform`, `reconcile`, `load`, `report` became `upstream_failed` (log: "Task did not run because upstream task(s) failed"); `audit` ran last and shows red. | `extract_products` failed on its 3rd attempt (about 1 minute apart) with `FileNotFoundError: Source file not found: .../products.csv`; `load_raw` and everything after it were `upstream_failed`. | Grid view (`screenshots/09_exp4_exp6_etl_grid.png`, `10_exp4_elt_grid.png`), task logs | Send a Slack or email alert on failure; today the only alert is a red task |
| 5 | New bad data (5 rows) | Each row is quarantined with the right reason (ETL) or marked `FAIL` (ELT); `dq_results` shows more failed rows | I added 5 test rows to `orders.csv`. All 5 landed in the quarantine file with the right reason: `customer_id is missing`, `quantity is zero, negative or not a number`, `store_id not found in stores`, `order_status is not DELIVERED/CANCELLED/RETURNED`, `order_ts is in the future`. | The ELT run finished green on the same file. I did not capture its rejected-row output, but it applies the same rules in SQL. | Quarantine file (`screenshots/11_exp5_quarantine.png`) | Fail the run if the reject rate is above a threshold (for example 5%) |
| 6 | Schema change | `extract_orders` fails loudly with `SCHEMA CHANGE ... missing column(s) ['order_amount']`; nothing loads | I renamed `order_amount` to `total`. `extract_orders` failed (red) and every later task was `upstream_failed`, so nothing was loaded and no wrong data was loaded silently. I did not capture the exact error text; the code raises `SCHEMA CHANGE ... missing column(s)`. | Not run on the ELT. Its extract step uses the same column list and check, so I expect the same failure. | Grid view (`screenshots/09_exp4_exp6_etl_grid.png`) | Keep as is (fail-loud is correct); add a type check for new columns |

After the increment file (`load_increment = true`) the same totals in both pipelines are: 2,460 clean orders, net revenue ₹1,211,087.30 (September ₹1,131,435.10, October ₹79,652.20), unreconciled ₹216,582.05 over 340 mismatches (`include/results_increment_check.txt`).

### ELT totals match ETL totals

The `compare_with_etl` task writes the table `elt_vs_etl_comparison`. Both without and with the increment file, all seven metrics (delivered orders, net revenue, gross margin, category revenue total, clean orders, reconciliation rows, unreconciled amount) show `MATCH`. There is no difference to explain. The ELT quality rules were written to be identical to the ETL ones (including repairs: S07 zone, blank category, impossible cost or MRP), which is why the totals agree.

---

## Screenshots (folder `screenshots/`)

| File | What it shows |
|---|---|
| `01_etl_graph_green.png` | ETL DAG, Graph view, green run |
| `02_elt_graph_green.png` | ELT DAG, Graph view, green run |
| `03_schedule_9am_ist.png` | Dags list showing `0 9 * * 1-6` and the next run 09:00 |
| `04_quarantine_file.png` | A quarantine file with `failure_reason` |
| `05_dq_results.png` | Query result from `dq_results` |
| `06_pipeline_runs.png` | Query result from `pipeline_runs` |
| `07_payment_reconciliation.png` | Payment reconciliation results |
| `08_increment_check_1.png`, `08_increment_check_2.png` | Incremental load: 10 updated, 150 inserted, totals match |
| `09_exp4_exp6_etl_grid.png`, `10_exp4_elt_grid.png` | Experiments 4 and 6: failed runs in Grid view |
| `11_exp5_quarantine.png` | Experiment 5: five bad rows quarantined with reasons |


---

## How the two pipelines are built (summary)

| | ETL (`dags/nammamart_etl_chandini.py`) | ELT (`dags/nammamart_elt_chandini.py`) |
|---|---|---|
| Idea | Clean in Python (pandas) **before** the warehouse | Load raw first, clean with **SQL** inside DuckDB |
| Tasks | `extract` x5 (parallel) → `validate` → `transform` → `reconcile` → `load` → `report` → `audit` | `extract` x5 (parallel) → `load_raw` → `stg_*` x6 (incl. `stg_orders_merge`) → `mart_*` x7 → `compare_with_etl` → `audit` |
| Bad rows | Written to `include/quarantine/chandini/` with `failure_reason` (customer PII masked in the file) and to `etl_quarantine_rows` | Stay in `stg_*` with `dq_status = 'FAIL'` + `failure_reason`; listed in `mart_rejected_rows` |
| Output tables | `etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`, `etl_payment_reconciliation`, `etl_daily_zone_revenue`, `etl_category_margin` | `raw_*`, `stg_*`, `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_revenue_by_payment_mode`, `mart_top_customers`, `mart_payment_reconciliation`, `mart_rejected_rows` |
| Increment | New orders added, corrected orders replace the old row (same `order_id`) | Appended to `raw_orders`; staging does a MERGE (delete old `order_id`, insert new) |
| Idempotent | Every run rebuilds the `etl_*` tables in one transaction | `CREATE OR REPLACE` for staging/marts; a run's raw batch is deleted before it is re-inserted |

Both pipelines write to the same audit tables, `dq_results` and `pipeline_runs`, in `include/warehouse_chandini.duckdb`. The two DAGs share one DuckDB file, so `include/dbutil.py` waits and retries when the other DAG is writing.

---

## Part C: Scheduling answers

**Settings (both DAGs):** `schedule="0 9 * * 1-6"`, `start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")`, `catchup=False`, `max_active_runs=1`, and in `default_args`: `retries=2`, `retry_delay=1 minute`, `execution_timeout=15 minutes`.

1. **Cron expression:** `0 9 * * 1-6` = minute 0, hour 9, every day of the month, every month, Monday to Saturday (no Sunday run).
2. **Time in UTC in the Airflow UI:** **03:30 UTC** (IST is UTC+5:30). My run IDs look like `scheduled__2026-10-22T03:30:00+00:00`. The UI shows the next run as 09:00 because the browser is set to my local time (+05:30).
3. **If `start_date` were in UTC:** Airflow would read "9:00" as 09:00 UTC, which is **14:30 IST**. The numbers would arrive five and a half hours after the 9:30 stand-up, so the SLA would be missed every day.
4. **If the laptop or Docker is off at 9 AM:** nothing runs, because the scheduler is stopped. I saw this myself: Docker was off at 9:00, and the scheduled run for 22 October only started at 13:26 IST once Docker came back. With `catchup=False` Airflow then creates **one** run for the latest missed slot and skips older ones. The data is late, but it is not duplicated. For production the fix is an always-on scheduler (managed Airflow or a server), plus an alert if no run has started by 9:15.

**Why `catchup=False`:** each run loads the full current source files, not one day's slice, so replaying every missed day would load the same data again and again and waste compute. Leadership only needs today's numbers. If a past day ever has to be re-run, it is done deliberately with a backfill.

**Why these retry settings:** two retries a minute apart cover a short file-lock or network problem without hiding a real failure for long. The 15-minute timeout stops a stuck task from running past the 9:30 stand-up. The `audit` task uses `trigger_rule="all_done"`, so a `pipeline_runs` row is written even when a task fails.

---

## Part D: Data-quality checks (summary)

All six dimensions are checked in both pipelines. ETL runs 78 checks in Python (`include/dq_checks.py`). ELT runs the same rules as SQL flags in `include/sql/stg_*.sql` (62 row-level checks; the list is in `include/elt_checks_meta.py`) plus dataset-level volume and freshness checks in the DAG. Every run writes one row per check to `dq_results` (`run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at`). **Critical** failures are quarantined (ETL) or flagged `FAIL` and excluded from the marts (ELT); **Warning** rows load and are logged. Rows that fail exactly one fixable check are repaired and keep a note in `dq_warnings` (S07 zone → South, blank category from the sub-category, impossible cost or MRP blanked).

| Dimension | Example checks | Severity |
|---|---|---|
| Completeness | `customer_id_not_null`, `delivered_has_delivery_minutes`, `category_not_null`, `volume_not_collapsed` | Critical (Warning for volume) |
| Uniqueness | `order_id_unique`, `payment_id_unique`, `customer_id_unique`, `product_id_unique` | Critical |
| Validity | `order_status_valid`, `payment_mode_valid`, `order_ts_valid`, `discount_pct_in_range`; email, phone, pincode format | Critical / Warning (email, phone, pincode) |
| Accuracy | `quantity_positive`, `order_amount_matches_formula`, `mrp_positive`, `cost_price_not_above_mrp`, `delivery_minutes_below_120` | Critical |
| Consistency | `customer_id_in_master`, `product_id_in_master`, `store_id_in_master`, `order_id_in_orders`; tier and payment-mode casing | Critical (keys) / Warning (casing) |
| Timeliness | `order_ts_not_future`, `signup_date_not_future`, `payment_ts_not_future`, `latest_order_not_stale` | Critical (future) / Warning (stale) |

---

## Part E: Data governance

**1. Ownership.** Orders: Operations (head of dark-store operations). Payments: Finance. Customers: CRM / Marketing. Products: Category management (merchandising). Stores: Operations. Warehouse tables (`raw_`, `stg_`, `mart_`, `etl_`, `dq_results`, `pipeline_runs`): the data engineering team (me). A change to a quality rule is approved by the **business owner of that data**, in writing, and then made by data engineering through a reviewed change. For the 120-minute delivery limit the approver is the head of Operations, because it defines what "impossible" means for a 10-minute service. Finance approves changes to the reconciliation rules.

**2. Data dictionary** (PII = personal data)

`etl_fact_orders` (one row per clean order; ELT equivalent is `stg_orders` where `dq_status = 'PASS'`)

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| order_id | text | Order identifier | orders.csv | unique | No |
| order_ts / order_date | timestamp / date | When the order was placed | orders.csv | not in the future | No |
| customer_id | text | Buyer (pseudonymous ID) | orders.csv | exists in customers | Indirect |
| loyalty_tier | text | Customer's tier | customers.csv | Bronze, Silver, Gold, Platinum | No |
| store_id, store_name, zone | text | Dark store and its zone | stores.csv | North, South, East, West | No |
| product_id, product_name, category | text | Product sold | products.csv | exists in products | No |
| quantity | int | Units | orders.csv | greater than 0 | No |
| unit_price, cost_price, discount_pct | decimal | Price, supplier cost, discount | orders / products | discount 0–100; cost ≤ MRP | No |
| order_amount | decimal | Amount charged after discount | orders.csv | = qty × price × (1 − discount) | No |
| net_revenue | decimal | `order_amount` for DELIVERED, 0 otherwise | derived | ≥ 0 | No |
| gross_margin | decimal | net_revenue − qty × cost_price (DELIVERED only) | derived | NULL if cost unknown | No |
| payment_mode | text | How the customer paid | orders.csv | UPI, Card, Cash on Delivery, Wallet | No |
| order_status | text | Order outcome | orders.csv | DELIVERED, CANCELLED, RETURNED | No |
| channel | text | App or Web | orders.csv | App, Web | No |
| delivery_minutes | number | Minutes from order to delivery | orders.csv | below 120 | No |
| within_15_min | boolean | DELIVERED within 15 minutes | derived | true / false | No |

`etl_dim_customer` (ELT: `stg_customers`)

| Column | Type | Meaning | Allowed values | PII |
|---|---|---|---|---|
| customer_id | text | Primary key | unique | Indirect |
| full_name | text | Customer name | any | **Yes** |
| email | text | **Masked** (`pr***@example.com`) | masked form | **Yes** (masked) |
| phone | text | **Masked** (`97******21`) | masked form | **Yes** (masked) |
| city, area, pincode | text | Location | pincode 6 digits | **Yes** (quasi-identifier) |
| signup_date | date | Account creation | not in the future | No |
| loyalty_tier | text | Tier | Bronze/Silver/Gold/Platinum | No |
| is_active | boolean | Active customer | Y/N | No |

`etl_dim_product` (ELT: `stg_products`): `product_id` (key), `product_name`, `category`, `sub_category`, `brand`, `mrp`, `cost_price` (NULL if invalid in the source), `unit`, `is_perishable`, `launch_date`. Source: products.csv. No personal data.
`etl_dim_store` (ELT: `stg_stores`): `store_id` (key), `store_name`, `zone`, `area`, `pincode`, `open_date`, `manager_name` (a staff member's name, so **PII**), `capacity_orders_per_day`. Source: stores.csv.

**3. Lineage.** Example: net revenue of store S01 on 1 September 2026 = ₹2,247.80 (4 delivered orders). **ELT:** `mart_daily_store_kpis` is built from `stg_orders`, and every staging and raw row carries `_source_file`, `_source_row` and `_run_id`. So I list the four `order_id`s, then look them up in `raw_orders` and read the exact CSV line number. **ETL:** the fact table has `order_id` but no line number, so I find the four orders in `orders.csv` by `order_id`. Only rejected rows carry their CSV line (`_source_row` in the quarantine file). **ELT is easier**, because lineage columns travel with the data and the raw copy is kept, while ETL only keeps the cleaned result.

**4. Personal data.** Personal data in the dataset: `customers.full_name`, `email`, `phone`, `city`, `area`, `pincode` (together they can identify a person), `stores.manager_name` (a staff member), and `customer_id` in orders (a pseudonymous identifier that links back to a person). I masked email (first two characters + `***@domain`) and phone (first two and last two digits) in the dimension tables, and in the quarantine files. I masked rather than deleted because analysts still need to tell customers apart, find duplicates, and build "top customers" without seeing real contact details, and deleting would break those joins. The law that applies is the **Digital Personal Data Protection Act, 2023 (DPDP Act)**. It expects: a lawful purpose and consent (or another lawful ground), collecting only what is needed (data minimisation), using data only for the stated purpose, keeping it accurate, reasonable security safeguards, deleting it when the purpose ends, notifying the Data Protection Board and affected people of a breach, and honouring the rights of customers to access, correct and erase their data. Penalties for failing to protect data can be very large (up to ₹250 crore).

**5. Access control.** Raw: data engineering only, because raw `customers` holds real phones and emails. Staging: data engineering and the data-quality owner (it contains the rejected rows and reasons). Marts and `etl_*` tables: analysts, Operations, Finance, and leadership (all masked). Quarantine files: data engineering plus the data owner of that file, who fixes the source. Unmasked customer phone numbers should be seen by nobody in analytics. The only people who might need them are customer support through the CRM system itself, not through the warehouse.

**6. Retention.** Raw: 12 months (enough to re-run the year and answer audits), then archived or deleted. Quarantine files: 90 days, because they exist to be fixed and re-supplied. Masked customer data: for as long as the customer is active plus a short fixed period, and deleted or anonymised when they ask or when the purpose ends. After the period, a scheduled job deletes the data and logs what it deleted (the DPDP Act expects deletion once the purpose is served).

**7. Secrets.** Credentials such as `SLACK_WEBHOOK_URL` are kept as **Airflow Variables or Connections** (Admin > Variables) and read in code with `Variable.get()`. Nothing secret is in my DAG files or in the repo. My own settings (`NAMMAMART_AS_OF` and others) are also Variables. A secret in a DAG file or on GitHub is readable by everyone with repository access, stays in the git history even after deletion, and bots scan public GitHub for them within minutes, so it must be changed (rotated) if it ever leaks.

**8. Audit** ("show me a rejected order and prove it is not in revenue"). **ETL:** the order sits in `include/quarantine/chandini/orders_failed_<run_id>.csv` exactly as received, with `failure_reason` and its CSV line number; it is also in `etl_quarantine_rows`. The proof it is excluded: `SELECT count(*) FROM etl_fact_orders WHERE order_id = '<id>'` returns 0, and `etl_daily_zone_revenue` is built only from `etl_fact_orders`. **ELT:** the order is in `raw_orders` exactly as received (all text, with `_source_file` and `_source_row`), in `stg_orders` with `dq_status = 'FAIL'` and `failure_reason`, and in `mart_rejected_rows`. The marts only read `dq_status = 'PASS'`, so it cannot be in `mart_daily_store_kpis`. The query to prove it is `SELECT count(*) FROM stg_orders WHERE order_id = '<id>' AND dq_status = 'PASS'`, which returns 0. ELT shows it in one database; ETL needs the file system and the database.

**9. Change history.** The order was DELIVERED on 3 September and RETURNED on 1 October. After the increment, **both** pipelines replace the old row with the new one, so September revenue **drops**: in my increment run (confirmed in both pipelines) September revenue went from ₹1,135,628.80 to ₹1,131,435.10, a drop of ₹4,193.70. Finance would normally want **both** answers: "as reported on the 30 September close" and "as corrected today". ELT can keep both, since `raw_orders` is append-only and holds both versions of the order. To keep both views in either pipeline, I would store the old row with a `valid_from` / `valid_to` date (an SCD Type 2 table), or snapshot the daily marts with the run date. Today my ETL overwrites the old version, so it would lose the "as reported" view unless the previous run's table is kept.

---

## Part F: Observability

### Run audit

Both DAGs end with an `audit` task (`trigger_rule="all_done"`), so it runs even after a failure and writes one row per run to `pipeline_runs` (`run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status`). Evidence from my own runs: the scheduled run of 22 October, which failed before extracting anything, is recorded as `FAILED` with 0 rows, and my manual run is `SUCCESS` with 5,212 rows extracted, 2,310 loaded and 150 quarantined.

### The 10 experiments

For each: **Expected → Observed → Where I saw it → What I would change.** Fill in the Observed and Where-I-saw-it cells from your own screens. The Expected and What-I-would-change cells are my reasoned predictions.

| # | Experiment | Expected | Observed (ETL) | Observed (ELT) | Where I saw it | What I would change |
|---|---|---|---|---|---|---|
| 1 | Happy path | Extract tasks run in parallel; ETL loads 2,310 fact rows and quarantines 150 rows; ELT totals match ETL | Five `extract_*` tasks ran in parallel, then the chain. 5,212 rows extracted, 2,310 loaded, 150 quarantined. Run took about 40 seconds. | Same counts. Five parallel extracts, then 6 staging and 7 mart tasks in sequence. Run took about 1 min 50 s. | Graph view (green run), `pipeline_runs` (`SUCCESS`, 5212 / 2310 / 150), `dq_results` | Add per-task row counts to `pipeline_runs` |
| 2 | Re-run (idempotency) | Fact rows stay 2,310 (rebuild in one transaction); ELT `raw_*` grows by one batch with a new `run_id` but `stg_`/`mart_` stay the same | I ran the ETL twice more with the same files: `etl_fact_orders` stayed at 2,310 rows and revenue stayed ₹1,135,628.80 (nothing double counted). | Raw grew by one 2,412-row batch per run (`SELECT _run_id, count(*) FROM raw_orders GROUP BY 1`), which is correct because raw is the append-only history. Staging and marts stayed the same, and the comparison still said `MATCH`. | `check_etl.py` counts, `pipeline_runs`, `results_increment_check.txt` section 6 | Prune old raw batches after the retention period |
| 3 | Incremental load | 150 inserted and 10 updated; 10 corrected orders change status with no duplicates; fact becomes 2,460 | ETL: 2,460 clean orders, 0 duplicate `order_id`s, September revenue ₹1,131,435.10 | ELT: merge gave exactly 10 updated and 150 inserted, 2,460 clean orders, 0 duplicates. All 10 corrected orders changed DELIVERED → RETURNED. All 7 ETL-vs-ELT metrics `MATCH`. | `include/results_increment_check.txt`, `elt_vs_etl_comparison` table, `compare_with_etl` task log | Keep the previous version of changed orders (SCD2) |
| 4 | Missing source file | `extract_products` goes `up_for_retry` twice (1 minute apart), then `failed`; later tasks are `upstream_failed`; `audit` still runs | I hid `products.csv`. `extract_products` retried and failed; `validate`, `transform`, `reconcile`, `load`, `report` became `upstream_failed` (log: "Task did not run because upstream task(s) failed"); `audit` ran last and shows red. | `extract_products` failed on its 3rd attempt (about 1 minute apart) with `FileNotFoundError: Source file not found: .../products.csv`; `load_raw` and everything after it were `upstream_failed`. | Grid view (`screenshots/09_exp4_exp6_etl_grid.png`, `10_exp4_elt_grid.png`), task logs | Send a Slack or email alert on failure; today the only alert is a red task |
| 5 | New bad data (5 rows) | Each row is quarantined with the right reason (ETL) or marked `FAIL` (ELT); `dq_results` shows more failed rows | I added 5 test rows to `orders.csv`. All 5 landed in the quarantine file with the right reason: `customer_id is missing`, `quantity is zero, negative or not a number`, `store_id not found in stores`, `order_status is not DELIVERED/CANCELLED/RETURNED`, `order_ts is in the future`. | The ELT run finished green on the same file. I did not capture its rejected-row output, but it applies the same rules in SQL. | Quarantine file (`screenshots/11_exp5_quarantine.png`) | Fail the run if the reject rate is above a threshold (for example 5%) |
| 6 | Schema change | `extract_orders` fails loudly with `SCHEMA CHANGE ... missing column(s) ['order_amount']`; nothing loads | I renamed `order_amount` to `total`. `extract_orders` failed (red) and every later task was `upstream_failed`, so nothing was loaded and no wrong data was loaded silently. I did not capture the exact error text; the code raises `SCHEMA CHANGE ... missing column(s)`. | Not run on the ELT. Its extract step uses the same column list and check, so I expect the same failure. | Grid view (`screenshots/09_exp4_exp6_etl_grid.png`) | Keep as is (fail-loud is correct); add a type check for new columns |
| 7 | Volume anomaly (50 orders) | `orders.volume_not_collapsed` shows `WARN` (50 rows is below 10% of the 2,400 baseline) | Not run (out of time) | Not run (out of time) | Not run | Make the baseline a rolling average of recent runs |
| 8 | Distribution drift (price ×100 in one store) | If only `unit_price` is changed, `order_amount_matches_formula` quarantines those rows. If amount and price are both multiplied, **nothing catches it** and that store's revenue jumps about 100× | Not run (out of time) | Not run (out of time) | Not run | Add a distribution check (store revenue vs its own 4-week average) |
| 9 | Freshness (30-day-old orders) | `latest_order_not_stale` shows `WARN` | Not run (out of time) | Not run (out of time) | Not run | Make the "now" comparison use the real run time in production (my demo fixes it with `NAMMAMART_AS_OF`) |
| 10 | Recovery | After fixing the cause, **Clear** on the failed task re-runs only that task and the ones after it; green tasks are not re-run | Not run (out of time) | Not run (out of time) | Not run | Add a documented runbook |

Experiments 1 to 6 were run on my pipelines; experiments 7 to 10 were **not run** because I ran out of time before the deadline. For those, only my predictions are given (the *Expected* and *What I would change* columns); there are no observations, and the volume and freshness checks they would exercise (`volume_not_collapsed`, `latest_order_not_stale`) exist in both pipelines.

Also record these real events in the table above, because they are genuine observations: (a) the scheduled run of 22 October ended `FAILED` with 0 rows (I did not investigate the cause); I saw it in `pipeline_runs` and the Graph view. Later the Airflow container itself ran out of memory (`OSError: Cannot allocate memory`), which made the DAGs disappear from the UI until I set a WSL memory limit and restarted. (b) My first ELT run failed in `load_raw` with a Python error (`run_sql_file() got multiple values for argument 'path'`); I saw it in the task log; it was fixed in `include/dbutil.py` and the run was cleared and re-run.

### The five pillars of data observability

| Pillar | How my pipelines cover it | Strength |
|---|---|---|
| Freshness | `latest_order_not_stale` and `latest_payment_not_stale` (Warning); `pipeline_runs` timestamps | Medium (the "now" is fixed for the demo) |
| Volume | `volume_not_collapsed` against a baseline; row counts in `pipeline_runs` | Medium (static baseline) |
| Schema | `extract` checks the expected columns and fails loudly | **Strongest** |
| Distribution | Only range and formula checks (discount 0–100, delivery under 120); no check on how values are spread | **Weakest** |
| Lineage | ELT: `_source_file`, `_source_row` and `_run_id` on every row; ETL: rejected rows only | Strong in ELT, partial in ETL |

**Best covered:** schema. **Weakest:** distribution (experiment 8 shows a price change can pass every check if the order amount changes with it).

---

## Part G: Recommendation to Sameer (CTO)

**Recommendation: ELT for the NammaMart warehouse, keeping the quality rules we already wrote.** Keep the "validate before it counts" discipline of ETL, but do it as SQL on top of a raw layer.

**What I observed.** I built both. On the same files they produce **identical numbers**: net revenue of ₹1,135,628.80 from 2,310 clean orders, and ₹132,736.15 that does not reconcile with the payment gateway. So the choice is not about correctness. It is about everything around it. Both pipelines also had a bad day on the same laptop: the Airflow container ran out of memory and a scheduled run failed with 0 rows. The audit table recorded the failure. Neither pipeline design could have prevented that, so that risk is the same either way.

**Auditability.** The CFO's question, "show me the order exactly as received and prove it is not in revenue", is one query in ELT, because the raw row, the rule it broke and the mart that excludes it are all in the same database. In ETL I have to open the quarantine file and then the database. ELT also keeps every raw version of an order, so finance can have both "as reported" and "as corrected" revenue after a status change (the increment changed September revenue by ₹4,193.70).

**Data-quality guarantees.** Both stop bad rows from reaching revenue. ETL does it before loading, so a bad row never enters the warehouse. ELT lets it in, but only into raw and staging, which analysts do not read. The ELT guarantee depends on access control on raw tables, which is why that is risk 1 below.

**Ease of change.** When a rule changes (say the delivery limit moves from 120 to 90 minutes), ELT needs one SQL edit and a rebuild from raw data that is already loaded. ETL needs a code change and a re-extract of the source files.

**Performance at 1000× the data.** My ETL holds whole files in pandas on one worker, so memory is the limit, and it will not scale to millions of orders a day. ELT pushes the work into the database engine, which is built for this (a columnar warehouse works through large tables in parallel). At 1000× the data I would also move off a single-file DuckDB to a warehouse that has several writers.

**Cost.** ELT stores the raw data as well (storage is cheap) and spends compute inside the warehouse. ETL spends compute on the Airflow workers and stores less. At today's size the difference is negligible. At 1000× the data, ELT's cost is in warehouse compute and raw storage, which can be controlled with the retention rules above.

**Top 3 risks of this design**

1. **Unmasked personal data sits in raw tables.** Raw `customers` holds real phones and emails (DPDP Act exposure). *Mitigation:* restrict raw to data engineering, mask on the way into staging (already done), and apply the retention rule.
2. **Quiet loss of revenue through quarantine.** About 150 rows (about 3%) are excluded from the marts and no one is paged. If a source system breaks, a larger share could vanish silently. *Mitigation:* fail or alert when the rejected share crosses a threshold, and review the rejected rows with the data owners.
3. **Infrastructure cannot meet the 9:30 SLA.** A laptop with Docker and a single-writer DuckDB file means the pipeline can be late or blocked (this happened to me: the 9:00 AM run only ran at 13:26). *Mitigation:* an always-on scheduler, a warehouse with concurrent writers, and an alert if no run has started by 9:15.

---

## Tools used

Apache Airflow (Astro CLI), Docker, DuckDB, pandas, VS Code and Git. I used Claude (an AI assistant by Anthropic) to help design and write the pipeline code, SQL and the first drafts of the written answers. I ran every pipeline myself, verified the results, and the decisions and edits in this submission are my own.
