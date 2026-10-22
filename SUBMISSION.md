# NammaMart Quick Commerce: ETL & ELT Pipelines

**FDAIE Module 2 · Data Engineering Foundations**
**Author:** Divya · **Warehouse:** `include/warehouse_divya.duckdb` · **Stack:** Airflow 3.3 (Astro Runtime 3.3), DuckDB 1.5.6, pandas 2.3

| Deliverable | Location |
|---|---|
| ETL DAG | [`dags/nammamart_etl_divya.py`](dags/nammamart_etl_divya.py) |
| ELT DAG | [`dags/nammamart_elt_divya.py`](dags/nammamart_elt_divya.py) |
| Check registry, business rules, paths | [`include/nammamart_divya/config.py`](include/nammamart_divya/config.py) |
| Data-quality rules (ETL, pandas) | [`include/nammamart_divya/dq.py`](include/nammamart_divya/dq.py) |
| PII masking (Python) | [`include/nammamart_divya/masking.py`](include/nammamart_divya/masking.py) |
| `dq_results`, `pipeline_runs`, DuckDB lock handling | [`include/nammamart_divya/warehouse.py`](include/nammamart_divya/warehouse.py) |
| Schedule, retries, Slack alert, run audit | [`include/nammamart_divya/runtime.py`](include/nammamart_divya/runtime.py) |
| ELT SQL: raw / staging / mart | [`include/sql/`](include/sql/) |
| Source data (copied, never edited) | `include/data/nammamart/` (+ `increments/orders_2026-10-01_increment.csv`) |
| Sample quarantine file | [`include/quarantine/divya/etl/orders_failed_etl_final_baseline.csv`](include/quarantine/divya/etl/orders_failed_etl_final_baseline.csv) |
| Screenshots | [`screenshots/`](screenshots/) |

**Headline result.** Both pipelines produce the **same numbers to the paisa**:

| Metric | ETL | ELT |
|---|---|---|
| September net revenue (delivered, after discount, after the Oct-1 returns) | ₹1,128,816.40 | ₹1,128,816.40 |
| All loaded net revenue (Sept + 1 Oct increment) | ₹1,207,890.05 | ₹1,207,890.05 |
| Clean orders in finance tables | 2,448 | 2,448 |
| Rows rejected with a reason | 151 | 151 |
| Order/payment mismatches | 324 (₹205,220.33) | 324 (₹205,220.33) |

They match because the rules live in one registry (`CHECKS` in `config.py`). Each check is then implemented twice, once in pandas and once in SQL, as a boolean column `f_<check_name>`.

---

## Contents
1. [Part 0: Discovery, profiling and Data Issues Log](#part-0-discovery-and-data-profiling)
2. [Part A: ETL pipeline](#part-a-etl-pipeline)
3. [Part B: ELT pipeline](#part-b-elt-pipeline)
4. [Part C: Scheduling](#part-c-scheduling-900-am-ist)
5. [Part D: Data-quality dimensions](#part-d-data-quality-dimensions)
6. [Business questions: SQL and results from both pipelines](#business-questions)
7. [Part E: Data governance](#part-e-data-governance)
8. [Part F: Observability tests](#part-f-observability-tests)
9. [Part G: Recommendation to the CTO](#part-g-recommendation-to-sameer-cto)
10. [Screenshots and how to reproduce](#screenshots-and-how-to-reproduce)

---

## Part 0: Discovery and data profiling

### Profile summary

| File | Rows | Distinct key | Duplicate keys | Notable nulls / blanks | Numeric ranges (min–max) | Category values found |
|---|---|---|---|---|---|---|
| `orders.csv` | 2,412 | 2,400 `order_id` | 12 (exact duplicate rows) | `customer_id` 18, `order_ts` 1 blank + 1 `N/A`, `delivery_minutes` 164 (156 CANCELLED + 8 DELIVERED) | quantity −3–5; unit_price 10–883; discount_pct 0–**250**; order_amount −1,593–9,576; delivery_minutes 8–**1,440** | status: DELIVERED 2182, CANCELLED 156, RETURNED 69, `Pending??` 3, `delivered` 2. payment_mode: UPI, Card, Cash on Delivery, Wallet + `upi`, `UPI `, `CARD`, `cod`, `Cashh`. store_id: S01–S15 + **S00, S17, S99** |
| `payments.csv` | 2,228 | 2,224 `payment_id` | 4 (exact duplicate rows) | `gateway_ref` 301 (all Cash on Delivery, expected) | amount_paid −1,593–9,576; payment_ts up to **2027-01-01** | status: SUCCESS 2116, REFUNDED 68, FAILED 44. Same mode variants as orders |
| `customers.csv` | 405 | 400 `customer_id` | 5 (same id, different phone/area) | `city` 6 | phone length 7–10; pincode includes `5600XX`; signup_date up to **2027-01-05** | loyalty_tier: Bronze 185, Silver 133, Gold 54, Platinum 25, `BRONZE` 3, `bronze` 3, `SILVER` 2 |
| `products.csv` | 152 | 150 `product_id` | 2 (`(old)` versions) | `category` 3 | mrp **−203**–883; cost_price 6.79–631.79 | 6 categories + blank |
| `stores.csv` | 15 | 15 | 0 | `zone` 1 | capacity_orders_per_day **0**–600 | zone: East 4, West 4, North 3, South 3, blank 1 |
| `orders_2026-10-01_increment.csv` | 160 | 160 | 0 | none | discount 0–20, delivery 8–34 | 150 new orders + **10 existing orders whose status changed DELIVERED → RETURNED** |

**Referential integrity.**
* `orders.customer_id → customers`: 0 unknown, but 18 null.
* `orders.store_id → stores`: 5 unknown (S00, S17×3, S99).
* `orders.product_id → products`: 9 unknown (P9xxx), plus 11 orders on P0109, whose master row is invalid.
* `payments.order_id → orders`: 8 orphans (ORD9xxxxx).

**Business rules.**
* 18 orders break the `order_amount` formula.
* 8 DELIVERED orders have no `delivery_minutes`.
* 14 SUCCESS payments differ from their order amount.
* 44 DELIVERED orders have a FAILED payment, and 27 DELIVERED orders have no payment at all.

### Data Issues Log

Severity follows Part D: **Critical** = quarantined, **Warning** = loaded but logged in `dq_results`.

| # | File | Row key(s) | Column | Problem | Dimension | Rule implemented | Severity |
|---|---|---|---|---|---|---|---|
| 1 | orders | ORD000539, ORD000783, ORD001110, ORD001239, ORD001326, ORD001434, ORD001441, ORD001462, ORD001549, ORD001692, ORD001870, ORD002129 | order_id | Each appears twice (exact duplicate rows) | Uniqueness | `ord_order_id_unique`: keep the first, quarantine the copy | Critical |
| 2 | orders | 18 rows, e.g. ORD002255, ORD000701, ORD000997, ORD000022 | customer_id | Blank | Completeness | `ord_customer_id_not_null` | Critical |
| 3 | orders | ORD000403 (`N/A`), ORD000362 (blank), ORD001610 & ORD000534 (`2026-13-05`), ORD002012 & ORD000946 (`31/09/2026 10:00`) | order_ts | Not a valid `YYYY-MM-DD HH:MM:SS` timestamp | Validity | `ord_ts_parseable` | Critical |
| 4 | orders | ORD002119, ORD001765, ORD001725 (2027-01-01); ORD001121, ORD001965 (2026-11-02); ORD001693, ORD001089 (2026-10-21) | order_ts | After the business date of the export (2026-10-01) | Timeliness | `ord_ts_not_future` | Critical |
| 5 | orders | ORD001799 (S99), ORD001046, ORD001492, ORD001275 (S17), ORD002126 (S00) | store_id | Store not in store master | Consistency | `ord_store_exists` | Critical |
| 6 | orders | ORD000986, ORD001058, ORD001685, ORD000070, ORD000789, ORD000596, ORD000134, ORD000965, ORD002179 | product_id | P9xxx not in product catalogue | Consistency | `ord_product_exists` | Critical |
| 7 | orders | 11 orders on P0109 (+ ORD002465 in the increment) | product_id | Product exists but its master row is invalid (MRP −203), so margin can't be trusted | Consistency | `ord_product_exists` checks against the *clean* product master | Critical |
| 8 | orders | ORD000469, ORD002360, ORD001724, ORD000099 (0); ORD001296, ORD001459, ORD000867, ORD000462, ORD001124, ORD001384 (negative) | quantity | Zero or negative quantity | Accuracy | `ord_quantity_positive` | Critical |
| 9 | orders | ORD002103 (250), ORD001468 (120), ORD001237 (150) | discount_pct | Discount above 100% | Validity | `ord_discount_in_range` (0–100) | Critical |
| 10 | orders | ORD002305, ORD002004, ORD000845, ORD000964, ORD002031, ORD000801, ORD001624 (≈10× too high); ORD001274, ORD002223, ORD001755 (≈½); ORD002298, ORD000880, ORD000677, ORD002244, ORD002128 (≈1.5×) | order_amount | ≠ quantity × unit_price × (1 − discount/100) | Accuracy | `ord_amount_matches_formula` (±0.01) | Critical |
| 11 | orders | ORD001554, ORD001800, ORD000731, ORD001277, ORD000326, ORD001515, ORD002229, ORD000878 | delivery_minutes | DELIVERED but blank | Completeness | `ord_delivered_has_minutes` | Critical |
| 12 | orders | ORD001037, ORD002101, ORD000424 (480); ORD001104, ORD000020 (900); ORD000455 (1,440) | delivery_minutes | Hours instead of minutes; breaks the 10-minute KPI | Accuracy | `ord_delivery_under_limit` (< 120) | Critical |
| 13 | orders | ORD000500, ORD001970, ORD001637 | order_status | `Pending??` not an allowed status | Validity | `ord_status_valid` | Critical |
| 14 | orders | ORD001066, ORD000153 (`delivered`) + 10 payment_mode variants (`upi`, `UPI `, `CARD`, `cod`, `Cashh`) | order_status / payment_mode | Casing, whitespace and typo variants | Consistency | Standardised (`upper`, mapping table), counted by `ord_value_casing` | Warning |
| 15 | orders | 11 orders | unit_price | Differs from catalogue MRP (promotions are plausible) | Accuracy | `ord_price_drift` warns only if > 3× MRP | Warning |
| 16 | payments | PAY0000509, PAY0000755, PAY0001651, PAY0002000 | payment_id | Duplicate rows | Uniqueness | `pay_payment_id_unique` | Critical |
| 17 | payments | PAY0002217 … PAY0002224 (orders ORD941071 … ORD997176) | order_id | Payment for an order that doesn't exist | Consistency | `pay_order_exists` | Critical |
| 18 | payments | PAY0001568 (−43.05), PAY0002175, PAY0000751, PAY0000944, PAY0000791, PAY0000737 (negative); PAY0000116, PAY0001111, PAY0001322, PAY0000954 (0.00) | amount_paid | Zero or negative SUCCESS payment | Accuracy | `pay_amount_positive` | Critical |
| 19 | payments | PAY0000127, PAY0001908, PAY0001037 (2027-01-01); PAY0000396, PAY0001488 (2026-11-02); PAY0000337, PAY0000267 (2026-10-21) | payment_ts | Future-dated | Timeliness | `pay_ts_not_future` | Critical |
| 20 | payments | 14 SUCCESS payments, e.g. PAY0000354 (455.00 vs order 505.00), PAY0000779 (2.60 vs 102.60) | amount_paid | Doesn't equal the order amount | Consistency | Not quarantined: written to the reconciliation table | Reconciliation |
| 21 | payments | 44 FAILED payments on DELIVERED orders; 27 DELIVERED orders with no payment | payment_status | Revenue counted but money not received | Consistency | Reconciliation table | Reconciliation |
| 22 | payments | 8 rows (`upi`, `UPI `, `CARD`, `cod`, `Cashh`) | payment_mode | Casing and typo variants | Consistency | Standardised, `pay_value_casing` | Warning |
| 23 | customers | C00036, C00103, C00226, C00252, C00313 | customer_id | Same id twice with different phone/area | Uniqueness | `cust_id_unique`: keep the first CRM row, quarantine the second | Critical |
| 24 | customers | C00337, C00045, C00327, C00193 (`N/A`); C00307, C00363, C00392, C00062, C00067 (no `@`); C00063, C00310, C00112 (no TLD) | email | Invalid email | Validity | `cust_email_valid` | Warning |
| 25 | customers | C00151, C00240, C00095, C00357, C00130, C00290, C00339 | phone | Not 10 digits | Validity | `cust_phone_valid` | Warning |
| 26 | customers | C00132, C00083, C00166 | pincode | `5600XX` | Validity | `cust_pincode_valid` | Warning |
| 27 | customers | C00400 (2027-01-05), C00398 (2026-11-12), C00038 (2026-12-20) | signup_date | Future signup | Timeliness | `cust_signup_not_future`: customer kept, date set to NULL | Warning |
| 28 | customers | C00355, C00394, C00233, C00142, C00328, C00219 | city | Blank | Completeness | `cust_city_present` | Warning |
| 29 | customers | 8 rows (`BRONZE`, `bronze`, `SILVER`) | loyalty_tier | Inconsistent casing | Consistency | Title-cased, `cust_tier_casing` | Warning |
| 30 | products | P0030, P0140 | product_id | Two rows each; one is `… (old)` with an older MRP | Uniqueness | `prod_id_unique`: keep the current row, quarantine `(old)` | Critical |
| 31 | products | P0109 | mrp | −203.00 | Accuracy | `prod_mrp_positive` | Critical |
| 32 | products | P0051, P0060, P0068 | category | Blank | Completeness | `prod_category_present`: inferred from sub_category (Dal/Rice/Oil → Staples) | Warning |
| 33 | products | P0019, P0073, P0074, P0117 (+P0109) | cost_price | Cost > MRP (exactly 1.2 × MRP, looks like a calculation error) | Accuracy | `prod_cost_le_mrp` | Warning |
| 34 | stores | S07 (BTM Layout) | zone | Blank | Completeness | `store_zone_present`: zone set to `Unknown` | Warning |
| 35 | stores | S12 (Rajajinagar) | capacity_orders_per_day | 0 although it has 135 orders | Accuracy | `store_capacity_positive` | Warning |
| 36 | stores | n/a | store_id | Master has 15 stores; orders reference S00, S17, S99 | Consistency | Caught in orders (#5) | Critical |
| 37 | increment | ORD002465 | product_id | P0109 (invalid product master) | Consistency | Same order rules as the daily file | Critical |
| 38 | increment | ORD000017, ORD000271, ORD000332, ORD000337, ORD000584, ORD000708, ORD000822, ORD001025, ORD001067, ORD001990 | order_status | Late status change DELIVERED → RETURNED | Timeliness / change history | MERGE / upsert on order_id | n/a (expected) |

**Design decisions** (agreed defaults, to confirm with the client):
* A future `signup_date`, a blank store zone and a blank product category are **Warnings**, not Critical. Quarantining a real customer, a whole store or a product would drop valid, paid orders from the CFO's revenue. The bad attribute is fixed or nulled instead and logged.
* `Cashh` is mapped to Cash on Delivery as an obvious typo.
* Exact duplicate orders and payments keep the first copy.

### Questions I would ask the client before building
1. **Negative and zero quantities** (10 orders, all with matching negative payments): are these returns or refunds keyed as new orders, or data-entry errors? If they are returns, they should be linked to the original order and reduce revenue, not be dropped.
2. **Which day does a late return belong to?** The increment turns 10 September orders into RETURNED. Should September revenue be restated (current-state view), or should the return be booked in October with September frozen once reported? This decides whether finance needs a snapshot or history table.
3. **Who is the source of truth when systems disagree?** Order says ₹505, gateway says ₹455 (14 cases). DELIVERED but FAILED payment (44 cases). Is "revenue" what was ordered, or what was collected? Who owns fixing each side, and what tolerance is acceptable?

(Also worth asking: is 120 minutes the right maximum delivery time, and are the S17/S99/S00 stores test stores or real stores missing from the master?)

---

## Part A: ETL pipeline

`dags/nammamart_etl_divya.py`: **Extract → Validate → Quarantine → Transform → Load → Incremental merge → Reconcile → Report → Audit**

```
extract_orders ─────┐
extract_payments ───┤                validate_stores ────┐
extract_customers ──┼─(parallel)──►  validate_products ──┼─► validate_orders ─► validate_payments ─► quarantine
extract_products ───┤                validate_customers ─┘   validate_increment ─┘                     │
extract_stores ─────┤                                                                                  ▼
extract_increment ──┘   audit ◄─ build_reports ◄─ reconcile_payments ◄─ load_increment ◄─ load_warehouse ◄─ transform
                       (all_done)
```

| Requirement | How it is met |
|---|---|
| 1. Extract: five parallel tasks | `extract_orders/payments/customers/products/stores`, plus `extract_increment`. All six start in the same second (see Exp 1). Each enforces the column contract (fails loudly on schema change). Data is read as text exactly as received. |
| 2. Validate | `validate_*` tasks run the pandas rules in `include/nammamart_divya/dq.py`, adding `f_<check>`, `failure_reason`, `dq_warnings` and `dq_status` (PASSED/FAILED). Dimensions validate first because orders are checked against the *clean* masters. |
| 3. Quarantine | `quarantine` writes `include/quarantine/divya/etl/<file>_failed_<run_id>.csv` with a `failure_reason` column. It also writes table `etl_quarantine` (run_id, source, row_key, failure_reason, the full raw row as JSON). |
| 4. Transform | Trim, upper-case status, map payment modes, title-case tier, dedupe customers/products, mask email/phone/name, join to dims (and assert no order is dropped by a join), derive `order_date`, `net_revenue`, `gross_margin`, `within_15_min`. |
| 5. Load: star schema, idempotent | `etl_fact_orders` (PK order_id), `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`. Load is `CREATE OR REPLACE` inside one transaction, so a re-run replaces the snapshot instead of appending (Exp 2: 2,448 → 2,448 rows). |
| 6. Incremental | `load_increment` runs `MERGE INTO etl_fact_orders … WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`: 149 inserted, 10 updated, 0 duplicates. |
| 7. Reconcile | `etl_payment_reconciliation`: one row per mismatch with `reason`, `expected_amount`, `received_amount`, `difference` |
| 8. Report | `etl_daily_zone_revenue` and `etl_category_margin`, printed in the task log |
| 9. Audit | `audit` (trigger_rule `all_done`) writes `pipeline_runs`. It fails itself if any task failed, so the run is never falsely green. |

Derivations: `net_revenue = order_amount if DELIVERED else 0`; `gross_margin = order_amount − quantity × cost_price if DELIVERED else 0`; `within_15_min = delivery_minutes ≤ 15` (DELIVERED only).

## Part B: ELT pipeline

`dags/nammamart_elt_divya.py`: **Extract → Load raw → Staging SQL → MERGE increment → Mart SQL → compare with ETL → Audit**

| Layer | Tables | Built by |
|---|---|---|
| Raw | `raw_orders` (daily + increment files), `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | [`include/sql/raw/load_raw.sql`](include/sql/raw/load_raw.sql). DuckDB `read_csv(all_varchar=true)`, no pandas. Adds `_loaded_at`, `_source_file`, `_run_id` (+ `_row_num` = line in file). Append-only. `INSERT … BY NAME` fails loudly on a renamed column. |
| Staging | `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` (+ `stg_orders_increment`) | [`include/sql/staging/`](include/sql/staging/). Cast, trim, standardise, dedupe (`row_number()`), mask PII (SQL macros `mask_email`, `mask_phone`, `mask_name`), one `f_<check>` column per rule, `failure_reason`, `dq_status`. Order rules live once in the table macro `validate_orders(run_id, file_pattern, cutoff)`, reused for the daily and increment files. |
| Mart | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` (+ `mart_rejections` for Q6) | [`include/sql/mart/`](include/sql/mart/). All read the view `v_clean_orders` (PASSED orders joined to PASSED dims). |

* **One task per table:** six raw-load tasks, six staging tasks and six mart tasks run in parallel where dependencies allow. Python only renders `{{run_id}}`/`{{cutoff}}` and executes the `.sql` file.
* **Incremental load:**
  * **Raw:** the 160 increment rows are appended to `raw_orders` with `_source_file = orders_2026-10-01_increment.csv`.
  * **Staging:** [`stg_orders_merge_increment.sql`](include/sql/staging/stg_orders_merge_increment.sql) validates the feed, then runs `MERGE INTO stg_orders … ON order_id` (matching PASSED rows only). The 10 corrections overwrite DELIVERED with RETURNED, and the 150 new orders are inserted: 149 PASSED, plus 1 FAILED kept for audit. There are 0 duplicate PASSED order_ids.
* **Totals match ETL.** The `compare_with_etl` task checks this on every run and logs `September net revenue ELT 1128816.4 ETL 1128816.4`.
* **Why counts differ in one place:** ELT "inserted" 150 versus ETL 149. The ETL never loads a failed row into the fact table. ELT keeps the failed increment row (ORD002465) in staging with `dq_status='FAILED'`, so the marts still exclude it.

## Part C: Scheduling (9:00 AM IST)

Both DAGs use `include/nammamart_divya/runtime.py`:

```python
IST = pendulum.timezone("Asia/Kolkata")
SCHEDULE_CRON = "0 9 * * 1-6"                       # 09:00, Monday–Saturday
START_DATE = pendulum.datetime(2026, 10, 1, tz=IST)  # timezone-aware → cron evaluated in IST
catchup=False, max_active_runs=1
DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=1),
                "execution_timeout": timedelta(minutes=10), "on_failure_callback": notify_failure}
```

| Question | Answer |
|---|---|
| Cron expression | `0 9 * * 1-6`: minute 0, hour 9, every day of month, every month, Monday (1) to Saturday (6). Sunday has no stand-up. |
| What time does the UI show in UTC? | **03:30 UTC** (IST = UTC+5:30). Unpausing created the run `scheduled__2026-10-22T03:30:00+00:00`, and the next run is `2026-10-23 03:30:00+00:00` (= 09:00 IST Friday). The UI shows IST if the user's timezone is set to Asia/Kolkata. |
| What if `start_date` were UTC? | The cron would be evaluated in UTC, so the run would fire at **09:00 UTC = 2:30 PM IST**. That is 5 hours *after* the 9:30 AM stand-up, so the numbers would always be a day late. |
| What if the laptop/Docker is off at 9 AM? | No run happens: the scheduler isn't running, so the SLA is missed silently. When Docker starts again, `catchup=False` makes Airflow create **only the latest missed interval** and run it immediately. I saw this when I unpaused at 19:20 IST: Airflow immediately ran `scheduled__2026-10-22T03:30`. Older missed days are skipped. For production, the DAGs must run on an always-on deployment (Astro / a VM), not a laptop. |
| Why `catchup=False`? | Each run reprocesses the **complete** export (full snapshot + any increment files), so a backfill run for every missed weekday would load identical data many times. It would also queue many runs on a single-writer DuckDB file. One fresh run is all the business needs. Turn catchup on only if daily *partitioned* files are introduced. |
| Why these retries/timeouts? | Two retries one minute apart absorb a late file drop or a brief DuckDB lock. Exp 4 showed retries at 14:06:33 → 14:07:35 → 14:08:37. A normal run takes about 1.5 min, so a 10-minute `execution_timeout` stops a hung task and still leaves time to alert and fix before 9:30. The audit task has `retries=0`. |

## Part D: Data-quality dimensions

All checks are in the `CHECKS` registry (`config.py`), implemented in pandas (`dq.py`) for ETL and in SQL (`include/sql/staging/*.sql`) for ELT. **44 checks** in total; every run writes one `dq_results` row per check per file.

| Dimension | Critical checks (quarantine) | Warning checks (load + log) |
|---|---|---|
| Completeness | `ord_customer_id_not_null`, `ord_delivered_has_minutes`, `cust_id_not_null` | `cust_city_present`, `prod_category_present`, `store_zone_present`, `pay_gateway_ref_present`, `ord_volume_drop` (volume < 10% of previous run) |
| Uniqueness | `ord_order_id_unique`, `pay_payment_id_unique`, `cust_id_unique`, `prod_id_unique`, `store_id_unique` | none |
| Validity | `ord_status_valid`, `ord_payment_mode_valid`, `ord_ts_parseable`, `ord_discount_in_range`, `pay_mode_valid`, `pay_status_valid`, `pay_ts_parseable` | `cust_email_valid`, `cust_phone_valid`, `cust_pincode_valid`, `store_pincode_valid` |
| Accuracy | `ord_quantity_positive`, `ord_amount_matches_formula`, `ord_delivery_under_limit`, `pay_amount_positive`, `prod_mrp_positive` | `prod_cost_le_mrp`, `store_capacity_positive`, `ord_price_drift` |
| Consistency | `ord_store_exists`, `ord_product_exists`, `ord_customer_exists`, `pay_order_exists` | `ord_value_casing`, `pay_value_casing`, `cust_tier_casing` |
| Timeliness | `ord_ts_not_future`, `pay_ts_not_future`, `prod_launch_not_future` | `cust_signup_not_future`, `ord_freshness` (latest order older than 1 day) |

"Future" means at or after the end of the export's business date (Airflow Variable `nammamart_business_date`, default `2026-10-01`), and never later than now.

**`dq_results`** columns: `run_id, dag_id, check_name, source_file, dimension, severity, rows_checked, rows_failed, status (PASS / FAIL / WARN), checked_at`. A re-run of the same `run_id` replaces its rows. After the experiments the table holds 1,638 rows across 28 runs and 44 checks: a quality scorecard over time.

```sql
SELECT run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status
FROM dq_results WHERE run_id = 'etl_final_baseline' AND status <> 'PASS' ORDER BY source_file, check_name;
```
Excerpt (orders.csv, ETL; ELT is identical):

| check_name | dimension | severity | rows_checked | rows_failed | status |
|---|---|---|---|---|---|
| ord_amount_matches_formula | Accuracy | Critical | 2412 | 18 | FAIL |
| ord_customer_id_not_null | Completeness | Critical | 2412 | 18 | FAIL |
| ord_delivered_has_minutes | Completeness | Critical | 2412 | 8 | FAIL |
| ord_delivery_under_limit | Accuracy | Critical | 2412 | 6 | FAIL |
| ord_discount_in_range | Validity | Critical | 2412 | 3 | FAIL |
| ord_order_id_unique | Uniqueness | Critical | 2412 | 12 | FAIL |
| ord_product_exists | Consistency | Critical | 2412 | 20 | FAIL |
| ord_quantity_positive | Accuracy | Critical | 2412 | 10 | FAIL |
| ord_status_valid | Validity | Critical | 2412 | 3 | FAIL |
| ord_store_exists | Consistency | Critical | 2412 | 5 | FAIL |
| ord_ts_not_future | Timeliness | Critical | 2412 | 7 | FAIL |
| ord_ts_parseable | Validity | Critical | 2412 | 6 | FAIL |
| ord_value_casing | Consistency | Warning | 2412 | 10 | WARN |
| ord_freshness | Timeliness | Warning | 1 | 1 | WARN |

---

## Business questions

Each answer is one query on the final tables. Results are from the `*_final_baseline` runs (September + the 1 October increment). **ETL and ELT results are identical.**

### Q1: Daily net revenue by zone and by store (delivered only, after discount)
```sql
-- ELT
SELECT order_date, zone, store_id, store_name, net_revenue FROM mart_daily_store_kpis ORDER BY 1, 2, 3;
SELECT zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY zone;
-- ETL
SELECT order_date, zone, store_id, store_name, net_revenue FROM etl_daily_zone_revenue ORDER BY 1, 2, 3;
SELECT zone, round(sum(net_revenue), 2) FROM etl_daily_zone_revenue GROUP BY zone;
```
| zone | net_revenue (ETL = ELT) |
|---|---|
| East | 313,325.40 |
| North | 240,920.50 |
| South | 256,052.15 |
| West | 312,210.00 |
| Unknown (S07, zone blank in master) | 85,382.00 |
| **Total** | **1,207,890.05** |

By store: S01 84,485.55 · S02 76,341.95 · S03 80,093.00 · S04 75,794.05 · S05 86,001.25 · S06 94,256.85 · S07 85,382.00 · S08 78,980.50 · S09 65,742.50 · S10 84,988.90 · S11 83,613.50 · S12 62,543.05 · S13 89,650.30 · S14 77,166.75 · S15 82,849.90.

Daily sample (ELT): 2026-09-30: East 9,750.55, North 4,469.55, South 8,638.95, West 5,910.10, Unknown 1,702.10.

### Q2: Revenue and gross margin by category
```sql
SELECT * FROM mart_category_margin;   -- ELT
SELECT * FROM etl_category_margin;    -- ETL
```
| category | delivered orders | revenue | cost | gross margin | margin % |
|---|---|---|---|---|---|
| Staples | 380 | 329,513.20 | 262,391.72 | 67,121.48 | 20.37 |
| Personal Care | 337 | 212,148.05 | 165,497.72 | 46,650.33 | 21.99 |
| Household | 363 | 212,105.05 | 157,638.90 | 54,466.15 | 25.68 |
| Dairy & Bakery | 405 | 179,568.95 | 131,935.95 | 47,633.00 | 26.53 |
| Snacks & Beverages | 341 | 174,988.60 | 133,558.93 | 41,429.67 | 23.68 |
| Fruits & Vegetables | 392 | 99,566.20 | 79,054.82 | 20,511.38 | 20.60 |

### Q3: Average delivery time per store and % delivered within 15 minutes
```sql
SELECT store_id, store_name, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla;      -- ELT
SELECT store_id, round(avg(delivery_minutes), 1), round(100.0 * count_if(within_15_min) / count(*), 1)
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY store_id;                          -- ETL
```
| store | avg min | % ≤ 15 min | store | avg min | % ≤ 15 min |
|---|---|---|---|---|---|
| S01 Hebbal | 20.4 | 32.7 | S09 Indiranagar | 21.2 | 26.1 |
| S02 Yelahanka | 21.0 | 31.8 | S10 Marathahalli | 22.1 | 26.0 |
| S03 RT Nagar | 20.7 | 29.6 | S11 HSR Layout | 20.8 | 28.8 |
| S04 Jayanagar | 21.3 | 27.5 | S12 Rajajinagar | 21.8 | 24.4 |
| S05 JP Nagar | 21.2 | 29.8 | S13 Malleshwaram | 21.9 | 27.3 |
| S06 Banashankari | 22.5 | 21.6 | S14 Vijayanagar | 21.5 | 31.3 |
| S07 BTM Layout | 21.1 | 32.7 | S15 Basaveshwaranagar | 22.1 | 23.1 |
| S08 Whitefield | 20.6 | 35.4 | | | |

Insight for the COO: once the 6 impossible "hours" deliveries are removed, no store averages under 20 minutes, and only 22–35% of orders meet 15 minutes. The "10-minute promise" is not being met anywhere.

### Q4: Revenue by payment mode, and the amount that doesn't reconcile
```sql
SELECT payment_mode, round(sum(net_revenue), 2) FROM v_clean_orders GROUP BY 1;            -- ELT (etl_fact_orders for ETL)
SELECT count(*), round(sum(abs(difference)), 2) FROM mart_payment_reconciliation;           -- etl_payment_reconciliation for ETL
SELECT reason, count(*), round(sum(abs(difference)), 2) FROM mart_payment_reconciliation GROUP BY 1;
```
| payment_mode | revenue |
|---|---|
| UPI | 637,524.00 |
| Card | 250,491.75 |
| Cash on Delivery | 182,334.40 |
| Wallet | 137,539.90 |

**Unreconciled: 324 mismatches, ₹205,220.33** (identical in both pipelines):

| reason | rows | amount |
|---|---|---|
| DELIVERED order has no payment (27 Sept + 150 new Oct-1 orders whose payments aren't in the export yet) | 177 | 95,794.95 |
| Payment for an order not in the finance table (quarantined or unknown order) | 82 | 78,924.43 |
| DELIVERED order but payment FAILED | 42 | 25,722.25 |
| RETURNED order not yet refunded (the 10 Oct-1 returns) | 10 | 4,193.70 |
| amount_paid differs from order_amount | 12 | 585.00 |
| RETURNED order has no payment/refund record | 1 | 0.00 |

### Q5: Top 10 customers by spend (no raw email/phone)
```sql
SELECT * FROM mart_top_customers WHERE spend_rank <= 10;     -- ELT
SELECT c.customer_id, c.customer_name_masked, c.loyalty_tier, sum(f.net_revenue) AS spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) GROUP BY ALL ORDER BY spend DESC LIMIT 10;  -- ETL
```
| # | customer | name | email | phone | tier | orders | spend |
|---|---|---|---|---|---|---|---|
| 1 | C00136 | Sanjay B. | sa***@example.com | 96******88 | Bronze | 9 | 8,738.10 |
| 2 | C00115 | Sanjay K. | sa***@example.com | 96******85 | Bronze | 8 | 8,037.70 |
| 3 | C00283 | Nikhil R. | ni***@example.com | 91******43 | Bronze | 8 | 7,934.60 |
| 4 | C00236 | Sneha N. | sn***@example.com | 90******01 | Platinum | 12 | 7,831.80 |
| 5 | C00310 | Kiran P. | ki***@example | 91******35 | Bronze | 8 | 7,258.95 |
| 6 | C00265 | Vikram H. | vi***@example.com | 91******69 | Gold | 8 | 7,151.30 |
| 7 | C00071 | Mahesh P. | ma***@example.com | 91******42 | Silver | 11 | 6,933.30 |
| 8 | C00338 | Bhavana I. | bh***@example.com | 94******05 | Silver | 9 | 6,848.55 |
| 9 | C00357 | Arjun N. | ar***@example.com | 93*****12 | Bronze | 11 | 6,637.90 |
| 10 | C00209 | Kavya P. | ka***@example.com | 98******97 | Platinum | 8 | 6,519.40 |

Insight: 6 of the top 10 spenders are Bronze. The loyalty tiers don't reflect actual value.

### Q6: Rows rejected today, by file and reason
```sql
SELECT source_file, failure_reason, rows_rejected FROM mart_rejections ORDER BY 1, 3 DESC;           -- ELT
SELECT source, failure_reason, count(*) FROM etl_quarantine WHERE run_id = '<today>' GROUP BY ALL;  -- ETL
```
| file | reason | rows |
|---|---|---|
| orders.csv | product_id not in clean product master | 20 |
| orders.csv | customer_id is null | 18 |
| orders.csv | order_amount ≠ formula | 15 |
| orders.csv | duplicate order_id | 12 |
| orders.csv | quantity ≤ 0 | 10 |
| orders.csv | DELIVERED order has no delivery_minutes | 8 |
| orders.csv | order_ts is in the future | 7 |
| orders.csv | order_ts is not a valid timestamp | 6 |
| orders.csv | delivery_minutes ≥ 120 | 6 |
| orders.csv | store_id not in store master | 5 |
| orders.csv | discount_pct outside 0–100; order_amount ≠ formula | 3 |
| orders.csv | order_status not in allowed list | 3 |
| orders_2026-10-01_increment.csv | product_id not in clean product master | 1 |
| payments.csv | amount_paid ≤ 0 / order_id not in orders file / payment_ts in the future / duplicate payment_id | 10 / 8 / 7 / 4 |
| customers.csv | duplicate customer_id | 5 |
| products.csv | duplicate product_id (superseded row) / mrp ≤ 0 | 2 / 1 |
| **Total** | | **151** (orders 113, increment 1, payments 29, customers 5, products 3, stores 0) |

---

## Part E: Data governance

**1. Ownership.**
* **Orders:** Head of Operations (order management system).
* **Payments:** Finance Controller (gateway owner).
* **Customers:** CRM/Marketing lead, with the Data Protection Officer for PII.
* **Products:** Category/Catalogue manager.
* **Stores:** Ops (store master).
* **Warehouse tables:** the data team owns the pipelines and the `raw_`/`stg_` layers. Finance owns the definitions in the `etl_*`/`mart_*` reporting tables.
* **Rule changes:** a change such as the 120-minute delivery limit (`MAX_DELIVERY_MINUTES` in `config.py`) is proposed by the data team. It is approved by the data owner (COO Kavya for delivery KPIs, the CFO for revenue rules) and shipped as a reviewed Git pull request, so every rule change has an author, approver and date.

**2. Data dictionary.**

`etl_fact_orders` (one row per clean order; `stg_orders` WHERE `dq_status='PASSED'` is the ELT equivalent)

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| order_id | VARCHAR (PK) | Order identifier | orders.order_id | `ORD` + 6 digits, unique | No |
| order_ts / order_date | TIMESTAMP / DATE | When the order was placed (IST) | orders.order_ts | ≤ business date | No |
| customer_id | VARCHAR (FK) | Customer | orders.customer_id | exists in dim_customer | Pseudonymous ID |
| store_id | VARCHAR (FK) | Dark store | orders.store_id | S01–S15 | No |
| product_id | VARCHAR (FK) | Product | orders.product_id | exists in clean dim_product | No |
| quantity | INTEGER | Units | orders.quantity | > 0 | No |
| unit_price | DOUBLE | Selling price per unit | orders.unit_price | > 0 | No |
| discount_pct | DOUBLE | Discount % | orders.discount_pct | 0–100 | No |
| order_amount | DOUBLE | quantity × unit_price × (1 − discount/100) | orders.order_amount | formula ± 0.01 | No |
| payment_mode | VARCHAR | Standardised mode | orders.payment_mode | UPI, Card, Cash on Delivery, Wallet | No |
| order_status | VARCHAR | Latest status (after increment merge) | orders / increment | DELIVERED, CANCELLED, RETURNED | No |
| channel | VARCHAR | App or Web | orders.channel | App, Web | No |
| delivery_minutes | DOUBLE | Minutes to deliver | orders.delivery_minutes | < 120; required if DELIVERED | No |
| cost_price | DOUBLE | Unit cost at load time | products.cost_price | > 0 | No |
| net_revenue | DOUBLE | order_amount if DELIVERED, else 0 | derived | ≥ 0 | No |
| gross_margin | DOUBLE | net_revenue − quantity × cost_price (DELIVERED) | derived | any | No |
| within_15_min | BOOLEAN | delivery_minutes ≤ 15 (DELIVERED only) | derived | true/false/null | No |
| source_file, _run_id, _loaded_at | VARCHAR/TIMESTAMP | Lineage: which file and run wrote the row | pipeline | | No |

`etl_dim_customer`: customer_id (PK, pseudonymous) · customer_name_masked (`Priya H.`, **PII-masked**) · email_masked (`pr***@example.com`, **PII-masked**) · phone_masked (`97******21`, **PII-masked**) · city · area · pincode (quasi-identifier) · signup_date (NULL if future) · loyalty_tier (Bronze/Silver/Gold/Platinum) · is_active (bool).

`etl_dim_product`: product_id (PK) · product_name · category (6 values, inferred if blank) · sub_category · brand · mrp (> 0) · cost_price · unit (kg/pack/unit) · is_perishable · launch_date. No PII.

`etl_dim_store`: store_id (PK) · store_name · zone (North/South/East/West/Unknown) · area · pincode · open_date · manager_name (**employee personal data**, internal use) · capacity_orders_per_day.

**3. Lineage.** Example: `mart_daily_store_kpis` for 2026-09-03, S01 Hebbal shows **net_revenue 3,033.00** from 6 delivered orders.
* **ELT:** `stg_orders` lists the 6 orders: ORD000154 488.00, ORD000178 378.00, ORD000181 92.00, ORD000200 1,216.80, ORD000210 248.00, ORD000220 610.20. Each carries `_source_file='orders.csv'` and `_run_id`. `raw_orders` then gives the exact text as received, the line in the file (`_row_num` 502, 586, 945, 1228, 1657, 2316) and the load time.
* **ETL:** I can trace the number to `etl_fact_orders` rows (`order_id`, `source_file`, `_run_id`). But the original row is no longer in the warehouse, so proving what was received means re-opening the CSV, which may have been overwritten by the next export.
* **Verdict:** ELT is easier because every layer is a SQL table carrying the same keys, run id and file name, and the raw layer is immutable.

**4. Personal data.**
* **Personal data columns:** `customers.full_name`, `email`, `phone`; the quasi-identifiers `area` + `pincode` + `signup_date`; `customer_id` (pseudonymous; it links to a person); `stores.manager_name` (employee data).
* **Masking:** email is masked as `pr***@example.com` and phone as `97******21`. Names are reduced to first name + initial. Python and SQL use the same functions.
* **Why mask rather than delete:** analysts still need to recognise a record (same domain, last digits for support look-ups, de-duplication) and join on `customer_id`, without seeing the contact details.
* **Applicable law:** India's **Digital Personal Data Protection Act, 2023 (DPDP Act)** and the DPDP Rules. NammaMart is a *Data Fiduciary*. It must:
  * process personal data only for the purpose the customer consented to;
  * collect and keep the minimum necessary;
  * protect it with reasonable security safeguards;
  * honour access, correction and erasure requests;
  * erase data when the purpose is served;
  * notify the Data Protection Board and affected users of a breach.

**5. Access control.**

| Layer | Who can read |
|---|---|
| raw | Data-engineering service account and on-call data engineers only |
| staging | Data engineers and analysts (PII already masked) |
| mart | Business users, finance, COO dashboards |
| quarantine (files and `etl_quarantine`) | Data engineers + data owners fixing source issues, because it contains rows exactly as received, including unmasked customer data |

Raw `customers` holds unmasked phone numbers. Only the pipeline service account should read it in normal operation. Customer-support systems already hold the phone number, so analysts do not need it here. Any human access is break-glass, logged and approved by the DPO. In production, enforce this with separate schemas/roles (DuckDB → Postgres/BigQuery with grants) or by encrypting the raw PII columns.

**6. Retention.**
* **Raw data:** keep for audit for the finance period plus the statutory period (books of account: 8 years under the Companies Act). After 90 days, keep only non-PII columns in raw, or move it to encrypted cold storage.
* **Raw customer PII:** delete once the customer closes the account or the purpose ends (DPDP).
* **Quarantine files:** 90 days, long enough to fix sources and answer audit questions, then delete. They contain unmasked rows, which is why they are git-ignored.
* **Masked customer data in marts:** kept while the customer is active plus 1 year, then aggregated or anonymised.
* **`dq_results` and `pipeline_runs`:** small and PII-free, so keep indefinitely as the quality and SLA history.

Enforcement would be a scheduled clean-up DAG.

**7. Secrets.**
* `SLACK_WEBHOOK_URL` is read at runtime with `Variable.get("SLACK_WEBHOOK_URL", default=None)` in `runtime.notify_failure`. It is stored in Airflow Variables (UI → Admin → Variables, or the local-only `airflow_settings.yaml`, which is git-ignored).
* In production it belongs in a secrets backend (Astro Environment Secrets, AWS Secrets Manager, Vault).
* It must never be in the DAG file or on GitHub. Anyone with the URL can post into the company's Slack. Git history keeps a secret forever even after deletion. Public repos are scanned by bots within minutes. Rotating a leaked secret is also costly.
* The repo contains no keys. With no webhook configured, the alert is printed to the task log instead.

**8. Audit ("show a rejected order as received, the rule it broke, and prove it isn't in revenue").** Example: ORD002103.
* **ELT:**
  * `SELECT * FROM raw_orders WHERE order_id='ORD002103' AND _run_id='elt_final_baseline'` returns the row exactly as received: `2026-09-27 09:31:35, qty 1, unit_price 492.0, discount_pct 250, order_amount 492.0`, from `orders.csv`, line `_row_num`.
  * `SELECT dq_status, failure_reason FROM stg_orders WHERE order_id='ORD002103'` returns `FAILED: discount_pct outside 0-100; order_amount != quantity x unit_price x (1 - discount)`.
  * Proof it is excluded: `v_clean_orders` and every mart filter `dq_status='PASSED'`, so `SELECT count(*) FROM v_clean_orders WHERE order_id='ORD002103'` returns 0.
* **ETL:**
  * `include/quarantine/divya/etl/orders_failed_<run_id>.csv` (or `SELECT row_json, failure_reason FROM etl_quarantine WHERE row_key='ORD002103'`) shows the row as received, with the same reason.
  * Proof it is excluded: `SELECT count(*) FROM etl_fact_orders WHERE order_id='ORD002103'` returns **0**.

Both can answer. ELT answers from one database with run-level history. ETL's evidence lives partly in files.

**9. Change history (DELIVERED 3 Sept, RETURNED 1 Oct).**
* Examples are ORD000017 (DELIVERED 1 Sept), ORD000271, ORD000332 and the others in the increment.
* **After the incremental load, both pipelines show the *current state*:** `order_status = RETURNED`, `net_revenue = 0`, still on the original `order_date`.
* September revenue therefore drops from **₹1,133,010.10** (before the increment, Exp 3a) to **₹1,128,816.40**. The difference of ₹4,193.70 is the 10 returns, and it equals the reconciliation line "RETURNED order not yet refunded".
* **What finance wants:** September revenue as reported at month close (₹1,133,010.10) should stay fixed, and the ₹4,193.70 should be booked as a return in **October**. Operations wants the current state. So both views are needed.
* **How to keep both:**
  * ELT already keeps every version in `raw_orders`: the DELIVERED row from `orders.csv` and the RETURNED row from the increment, each with `_loaded_at`.
  * From that, build an **SCD Type 2 / status-history table** (`valid_from`, `valid_to`, `is_current`). Add either an `order_events` fact (one row per status change, with `event_date`) or a monthly snapshot table frozen at close.
  * Finance reports from the snapshot or events (revenue by event month), operations from the current view.
  * ETL alone overwrites the row in the MERGE and loses the old version, unless the same history table is added.

---

## Part F: Observability tests

**Run audit.** Both DAGs end with `audit` (`trigger_rule="all_done"`, no retries):
* It reads every task's state for the run and writes one row to **`pipeline_runs`**: `run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status, failed_tasks`.
* It then **fails itself if any task failed**. Otherwise a successful leaf task would mark a broken run green.
* Every task also has an `on_failure_callback` (Slack alert via Variable).

```sql
SELECT run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status, failed_tasks
FROM pipeline_runs ORDER BY start_time;
```

| run_id | dag | extracted | loaded | quarantined | status |
|---|---|---|---|---|---|
| scheduled__2026-10-22T03:30:00+00:00 | ETL / ELT | 5372 | 2448 | 151 | SUCCESS |
| etl/elt_exp01_happy | both | 5372 | 2448 | 151 | SUCCESS |
| etl/elt_exp02_rerun | both | 5372 | 2448 | 151 | SUCCESS |
| etl/elt_exp03a_noinc | both | 5212 | 2299 | 150 | SUCCESS |
| etl/elt_exp03b_inc | both | 5372 | 2448 | 151 | SUCCESS |
| etl/elt_exp04_missing (first attempt) | both | 5220 | null | null | FAILED (`extract_products` + all downstream listed in `failed_tasks`) |
| etl/elt_exp04_missing (after Clear, Exp 10) | both | 5372 | 2448 | 151 | SUCCESS |
| etl/elt_exp05_badrows | both | 5377 | 2448 | 156 | SUCCESS |
| etl/elt_exp06_schema | both | 2960 | null | null | FAILED |
| etl/elt_exp07_volume | both | 3010 | 207 | 2182 | SUCCESS (+ volume WARN) |
| etl/elt_exp08_drift | both | 5372 | 2294 | 305 | SUCCESS (+ drift WARN) |
| etl/elt_exp09_stale | both | 5372 | 2450 | 149 | SUCCESS (+ freshness WARN) |
| etl/elt_final_baseline | both | 5372 | 2448 | 151 | SUCCESS |

All experiments were triggered through the real scheduler (run ids above, visible in the Airflow UI Grid view). The original file was restored after each one.

| # | Experiment | Expected | Observed (ETL / ELT) | Where I saw it | What I would change |
|---|---|---|---|---|---|
| 1 | Happy path | Both green; parallel extract; ~150 quarantined | Both green. ETL ≈ 1.5 min, ELT ≈ 1.8 min. All 6 extract tasks started at the same second (13:55:55 / 13:55:44). The 6 ELT `load_raw_*` and the 6 `mart_*` also ran in parallel. Rows in/passed/quarantined: orders 2412/2299/113, increment 160/159/1, payments 2228/2199/29, customers 405/400/5, products 152/149/3, stores 15/15/0. Slowest tasks: ELT `load_raw_customers` 44 s, ETL `validate_products` 26 s. They spent most of that waiting for the DuckDB file lock held by parallel siblings. | Grid/Graph view, Gantt view, task logs (`[validate] orders: 2412 rows, 2299 passed, 113 failed`), `pipeline_runs` | DuckDB allows one writer, so "parallel" writes are actually queued. Use a pool of size 1 for writer tasks, or a multi-writer warehouse (Postgres) |
| 2 | Re-run (idempotency) | Fact counts unchanged; raw grows | ETL `etl_fact_orders` 2,448 → **2,448**, revenue unchanged. ELT `stg_orders` PASSED 2,448 → **2,448**, marts unchanged. `raw_orders` 10,288 → **12,860** (+2,572 = 2,412 + 160). | Query on fact/raw tables; `pipeline_runs` | Raw growth is **correct**: raw is an append-only audit log keyed by `_run_id`, and staging only reads the current run. Add retention on raw (Part E.6) |
| 3 | Incremental load | 150 new + 10 updated, no duplicates | Without the file: 2,299 orders, Sept revenue ₹1,133,010.10, 67 RETURNED. With the file: **ETL 149 inserted + 10 updated** (1 increment row quarantined: ORD002465, bad product). **ELT 150 inserted + 10 updated** (the failed row is kept in staging as FAILED). Both now hold 2,448 PASSED orders, 77 RETURNED, **0 duplicate order_ids**. The log lists each change: `ORD000017: DELIVERED -> RETURNED` … | `load_increment` / `stg_orders_merge_increment` logs; queries | Keep a status-history table so the old status is not lost (Part E.9). Process increment files once and archive them, rather than re-reading them every run |
| 4 | Missing source file (`products.csv` renamed) | Extract fails after retries; downstream blocked; alert | `extract_products` was **up_for_retry** twice and **failed** on try 3 (14:06:33 → 14:07:35 → 14:08:37, 1 min apart). Downstream tasks → **upstream_failed**. Independent branches (other extracts, `validate_customers`/`stores`, ELT raw loads, `stg_payments`) completed. `audit` wrote `status=FAILED` with `failed_tasks` and failed itself, so the run is red. `FileNotFoundError: Source file missing …/products.csv`. The failure callback fired (`[alert] … Task: extract_products`; logged because no webhook is set). Finance tables kept the previous good data. | Grid view (colours), task log, `pipeline_runs.failed_tasks`, alert line in the log | Set `SLACK_WEBHOOK_URL` so the alert reaches a human. Add a `FileSensor` with a timeout before 9:00 so a late file waits rather than failing |
| 5 | New bad data (5 rows added) | Each row quarantined with the right reason; dq_results +1 | All 5 quarantined in **both** pipelines with the correct reason: null customer → `customer_id is null`; qty −2 → `quantity <= 0`; S42 → `store_id not in store master`; SHIPPED → `order_status not in allowed list`; 2027 timestamp → `order_ts is in the future`. `dq_results` counts went up by exactly 1: not_null 18→19, quantity 10→11, store 5→6, status 3→4, future 7→8. Quarantine total 151 → 156. Revenue unchanged. | `orders_failed_etl_exp05_badrows.csv`, `etl_quarantine`, `stg_orders`, `mart_rejections`, `dq_results` | Nothing structural. Add an alert when the quarantine share exceeds e.g. 5% of a file |
| 6 | Schema change (`order_amount` → `total`) | Fail loudly, load nothing wrong | **`extract_orders` failed in both** (3 tries): `ValueError: Schema change in orders.csv: missing columns ['order_amount'], unexpected columns ['total']`. All downstream tasks upstream_failed; `pipeline_runs` FAILED. **Loud, not silent.** `etl_fact_orders` still held the previous good 2,448 rows. ELT would fail a second time at `INSERT … BY NAME` even without the header check. | Task log, Grid view, `pipeline_runs` | Don't retry deterministic schema errors (raise `AirflowFailException` to fail fast and alert immediately). Version the contract with the source owner |
| 7 | Volume anomaly (50 orders) | A warning that volume dropped > 90% | No row-level rule can catch this, because all 50 rows are valid. That is why the pipelines include a dataset-level **`ord_volume_drop`** check, which compares with the previous run in `dq_results`. Result: `[WARNING] order volume dropped by more than 90% (2417 -> 50)`, `dq_results … ord_volume_drop WARN`. The run was still **green**: only 207 orders loaded, and 2,182 payments were quarantined as "order_id not in orders file". | Task log, `dq_results`, `pipeline_runs` (rows_loaded 207) | Make volume drop **Critical** for publishing: block the mart refresh or fail the run and alert, so the COO never sees a 2% day as real |
| 8 | Distribution drift (`unit_price` × 100 at S01) | Some check catches it | Caught twice in both pipelines: `ord_amount_matches_formula` FAIL **178** (18 + 160 S01 orders, because order_amount no longer matches) and `ord_price_drift` WARN **160** (> 3× MRP). The 160 orders were quarantined, so **S01 net revenue on `mart_daily_store_kpis` fell from ₹84,485.55 to ₹8,958.35 (−89%)** and total revenue from ₹1,207,890.05 to ₹1,132,362.85 (−₹75,527.20). The run stayed green. | `dq_results`, mart query, quarantine count 151 → 305 | Bad numbers were kept out of finance, but a whole store silently vanished from the dashboard. Add a per-store revenue/volume check against a 7-day average, and alert when quarantine > 5% for any store |
| 9 | Freshness (order_ts − 30 days) | Timeliness warning | `ord_freshness` **WARN** in both: `latest order_ts 2026-09-21 10:15:00 is older than 1 day`. Side effect: 2 orders that were future-dated (2026-10-21) moved back into the valid window and were loaded (rows_loaded 2,450). | Task log, `dq_results` | This static September dataset is *always* stale against today's date, so the warning fires every day, which leads to alert fatigue. In production, compare against the expected business date (yesterday) and make staleness Critical before 9:00 |
| 10 | Recovery (Clear after Exp 4) | Only failed + downstream re-run | After restoring `products.csv`, I cleared the failed/upstream_failed tasks of `*_exp04_missing` (12 ETL, 14 ELT). **Only those re-ran.** `extract_products` went to try 4, everything downstream to try 1. The tasks that had succeeded kept their original 14:06 start time and `tries=1`. Both runs turned **green**, and `audit` overwrote the `pipeline_runs` row from FAILED to SUCCESS (5,372 / 2,448 / 151). | Grid view (task try numbers), `pipeline_runs` | Works as intended because every task is idempotent (replace/merge, never blind append). Keep it that way for any new task |

### Five pillars of data observability

| Pillar | How my pipelines cover it | Strength |
|---|---|---|
| **Freshness** | `ord_freshness` check, Airflow schedule + next-run time, `pipeline_runs.end_time` vs 9:00 | Medium: warns but doesn't block; no SLA alert if the run never starts |
| **Volume** | `rows_extracted / loaded / quarantined` per run in `pipeline_runs`; `ord_volume_drop` vs previous run; per-file passed/failed counts in logs | Good |
| **Schema** | Column contract enforced at extract (both), plus `INSERT BY NAME` in ELT raw | **Strongest:** fails loudly, nothing wrong is loaded (Exp 6) |
| **Distribution** | Accuracy checks (formula, quantity, delivery < 120, price > 3× MRP), reconciliation table | **Weakest:** no statistical baselines (mean/percentiles per store/day). Exp 8 changed one store's revenue by 89% and the run stayed green |
| **Lineage** | ELT: `_source_file`, `_run_id`, `_row_num`, `_loaded_at` on every row, from raw through staging to marts. ETL: `source_file`, `_run_id` on facts, quarantine files per run | Good in ELT, partial in ETL |

---

## Part G: Recommendation to Sameer (CTO)

**To:** Sameer, CTO · **From:** Divya (FDE) · **Re:** ETL or ELT for NammaMart

**Recommendation: adopt ELT, with an ETL-style quality gate in the staging layer.**
* Land every export untouched in a raw layer.
* Transform with versioned SQL.
* Publish finance marts only from rows that passed every Critical check.

You get Analytics' auditability *and* Finance's guarantee.

**What I observed running both side by side on the same data**

* **Data-quality guarantees are equal.** Both pipelines rejected exactly the same 151 rows with the same reasons and produced the same September revenue (₹1,128,816.40) and the same 324 reconciliation mismatches. Finance's concern ("a bad row must never reach the CFO") is met by ELT's `dq_status='PASSED'` gate just as well as by ETL's pre-load filter. In the bad-data, schema-change and missing-file tests both behaved identically: nothing bad reached a finance table.
* **Auditability: ELT wins clearly.**
  * For any number on the KPI table I could trace it in SQL to the exact file, line number and load run.
  * When finance asked "show me the rejected order as received", ELT answered from the warehouse.
  * When an order was DELIVERED in September and RETURNED on 1 October, ELT still had *both* versions in raw; ETL had overwritten the old one. That history is what finance needs to keep September's closed numbers fixed while booking the return in October.
* **Ease of change: ELT.**
  * A new business question is a new SQL file over existing staging tables, with no re-extraction.
  * If a rule changes (say the 120-minute limit), ELT can replay history from raw. ETL only has what it already accepted.
* **Performance at 1000× (≈ 2.4 M orders/month):**
  * ETL moves whole DataFrames between tasks in memory/pickles. At that size it will be memory-bound and slow.
  * ELT pushes work into the database engine (columnar, vectorised), which handles millions of rows in seconds.
  * The constraint I hit is **DuckDB's single writer**: my "parallel" tasks waited for each other (up to 40 s each). At scale, move the same SQL to Postgres, BigQuery or MotherDuck.
* **Cost:**
  * Both run on open-source tools on one machine today.
  * ELT stores more, because raw is append-only (5 runs grew `raw_orders` from 2.5 k to 12.9 k rows), so it needs a retention policy. Storage is cheap compared with half a day of finance time every week spent on reconciliation.
  * Neither design needs paid tools.

**Top 3 risks of the recommended design, and mitigations**
1. **Raw layer holds unmasked personal data (DPDP Act exposure).** Raw `customers` keeps names, emails and phones. *Mitigation:* restrict raw to a service account, encrypt or drop PII columns after 90 days, and keep only masked staging for people.
2. **"Everything loads" can hide problems.** In the volume and drift tests the run stayed green while a store's revenue fell 89% or volume fell 98%. *Mitigation:* promote volume, per-store drift and quarantine-share checks to blocking gates that stop the mart refresh and alert on Slack before 9:00.
3. **Single-node DuckDB + laptop scheduling is a single point of failure for the 9 AM SLA.** If Docker is off at 9:00, nothing runs, and DuckDB serialises writers. *Mitigation:* move Airflow to an always-on deployment (Astro/VM) and the warehouse to a managed multi-writer database before scaling. Add an SLA alert if `pipeline_runs` has no SUCCESS row by 8:50 IST.

---

## Bonus
* **Failure alert via Airflow Variable:** every task has `on_failure_callback=notify_failure`, which posts to Slack if Variable `SLACK_WEBHOOK_URL` is set (no secret in code). It was verified in Exp 4 and Exp 6 (logged in task output because no webhook is configured locally).
* **Volume anomaly check** (Exp 7) and **automatic ETL vs ELT total comparison** task (`compare_with_etl`).

## Screenshots and how to reproduce

Screenshots are in [`screenshots/`](screenshots/):
1. `01_etl_graph_green.png`: ETL DAG Graph view, green run
2. `02_elt_graph_green.png`: ELT DAG Graph view, green run
3. `03_schedule_9am_ist.png`: DAG details showing `0 9 * * 1-6`, Asia/Kolkata and next run 2026-10-23 09:00 IST (03:30 UTC)
4. `04_quarantine_file.png`: `include/quarantine/divya/etl/orders_failed_etl_final_baseline.csv`
5. `05_dq_results.png`, `06_pipeline_runs.png`, `07_payment_reconciliation.png`: query results
6. `08_exp04_retry_failed.png`, `09_exp10_recovery.png`: failure and recovery in Grid view

```bash
astro dev start                                   # Airflow UI http://localhost:8080
astro dev run dags list-import-errors             # both DAGs parse with no errors
astro dev run dags test nammamart_etl_divya
astro dev run dags test nammamart_elt_divya
# Optional: Admin → Variables → SLACK_WEBHOOK_URL ; nammamart_business_date (default 2026-10-01)
```
Query the warehouse with DuckDB / VS Code SQLTools on `include/warehouse_divya.duckdb` (stop Airflow runs first: DuckDB allows one writer).
