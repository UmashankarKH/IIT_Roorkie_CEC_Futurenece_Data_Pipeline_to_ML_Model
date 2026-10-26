# NammaMart Assignment Submission — Uday Kiran Komati

**Repository branch:** `udaykiran.komati-FDE-nammamart-Assignment`  
**DAG IDs:** `nammamart_etl_udaykiran_komati`, `nammamart_elt_udaykiran_komati`  
**Warehouse:** `include/warehouse_udaykiran_komati.duckdb`

---

## Part 0 — Data profiling & issues log

### Profile summary (September base files)

| File | Rows | Key issues found |
|------|------|------------------|
| orders.csv | 2,412 | 12 duplicate `order_id`, 18 null `customer_id`, invalid statuses, bad payment modes, qty ≤ 0, amount formula mismatches, future/bad timestamps, long delivery times |
| payments.csv | 2,228 | 4 duplicate `payment_id`, 8 payments for unknown orders, SUCCESS amount mismatches vs orders |
| customers.csv | 405 | 5 duplicate `customer_id`, bad emails/phones/pincodes, inconsistent loyalty casing, future signup dates |
| products.csv | 152 | 2 duplicate `product_id`, 3 null categories, negative MRP, cost > MRP |
| stores.csv | 15 | 1 blank `zone` (S07 BTM Layout), S12 capacity 0 |
| orders_2026-10-01_increment.csv | 160 | 150 new orders + 10 DELIVERED→RETURNED corrections |

### Data issues log (sample — full rules implemented in code/SQL)

| file | row key | column | problem | dimension | severity | proposed rule |
|------|---------|--------|---------|-----------|----------|---------------|
| orders | ORD001296 | quantity | negative quantity (-2) | Accuracy | Critical | quarantine qty ≤ 0 |
| orders | ORD002103 | discount_pct | 250% discount | Validity | Critical | quarantine discount not in 0–100 |
| orders | ORD000403 | order_ts | `N/A` / unparseable | Validity | Critical | quarantine |
| orders | ORD002119 | order_ts | future date 2027 | Timeliness | Critical | quarantine |
| orders | ORD001799 | store_id | unknown S99 | Consistency | Critical | quarantine |
| orders | ORD000986 | product_id | unknown P9455 | Consistency | Critical | quarantine |
| orders | ORD001462 | order_id | duplicate row | Uniqueness | Critical | keep first, quarantine dupes |
| payments | PAY0001651 | payment_id | duplicate | Uniqueness | Critical | quarantine duplicate |
| payments | PAY0001568 | amount_paid | SUCCESS but ≠ order_amount | Consistency | Warning (ETL) / mart mismatch | reconciliation table |
| customers | C00337 | email | invalid / N/A | Validity | Warning | mask + warn |
| customers | C00103 | customer_id | duplicate conflicting phone/area | Uniqueness | Critical | dedupe keep first |
| products | P0109 | mrp | negative MRP | Accuracy | Critical | quarantine |
| products | P0051 | category | null | Completeness | Critical | quarantine |
| stores | S07 | zone | blank zone | Completeness | Warning | default from area in staging |

### Three client questions before build

1. Should **RETURNED** orders reduce September revenue retroactively when status changes in the October increment, or only in October reporting?
2. Are **duplicate customer_id** rows data entry errors or intentional household accounts sharing an ID?
3. For **payment amount mismatches** on SUCCESS rows, should finance trust the order system or the payment gateway as source of truth?

---

## Part C — Scheduling

| Question | Answer |
|----------|--------|
| Cron expression | `0 9 * * 1-6` (09:00 Mon–Sat) |
| UTC time in Airflow UI | **03:30 UTC** (IST = UTC+5:30) |
| If `start_date` were UTC only | Schedule would not align to IST business hours; runs could shift relative to Bengaluru 9 AM stand-up prep |
| `catchup=False` | Prevents backfilling hundreds of historical DAG runs when the DAG is first enabled; NammaMart only needs forward daily runs |
| Laptop/Docker off at 9 AM | Missed schedule until machine is back; Airflow runs the next eligible interval (no catchup). Production would use always-on Astro/Composer |

**Retries:** 2 retries, 2-minute delay, 30-minute task timeout.

---

## Part E — Data governance (short answers)

1. **Ownership:** Ops owns orders/stores exports; Finance owns payments; CRM owns customers; Merchandising owns products; DE owns warehouse marts; Data Governance Council approves DQ rule changes (e.g. 120-min delivery cap).
2. **Dictionary:** See `etl_fact_orders`, `etl_dim_*`, and `mart_*` tables — PII columns flagged (`email_masked`, `phone_masked` only in reporting).
3. **Lineage:** `mart_daily_store_kpis.net_revenue` → `stg_orders` (PASS, DELIVERED) → `raw_orders` → `orders.csv` / increment file. ELT lineage is easier (raw preserved); ETL lineage requires quarantine + run_id on fact.
4. **PII:** `full_name`, `email`, `phone`, `area`, `pincode` — masked in marts; not deleted so ops can reconcile; **DPDP Act 2023** applies (purpose limitation, security, consent/notice).
5. **Access:** Raw CRM → restricted DE/compliance; staging → DE/analytics; marts → business users; quarantine → DE + ops analysts only.
6. **Retention:** Raw 7 years (audit); quarantine 1 year; masked marts per policy; then archive/delete per legal hold.
7. **Secrets:** `SLACK_WEBHOOK_URL` via Airflow Variables / env — never in DAG source or Git.
8. **Audit rejected order:** ETL: quarantine CSV + absent from `etl_fact_orders`; ELT: row in `raw_orders`, `dq_status='FAIL'` in staging, excluded from marts.
9. **Sept order returned Oct 1:** ELT increment updates staging to RETURNED (September mart revenue drops if rebuilt); ETL upsert updates fact. Finance wants **restated September**; analytics may keep **as-was snapshot** — keep both via run_date snapshots.

---

## Part F — Observability (template — fill screenshots after `astro dev run`)

Run each experiment on **both** DAGs and record in the table below.

| # | Experiment | Expected | Observed (fill after run) | Where seen |
|---|------------|----------|---------------------------|------------|
| 1 | Happy path | Green DAG, quarantine > 0, marts populated | | Airflow Graph, task logs |
| 2 | Re-run | ETL fact count unchanged; ELT raw grows | | `etl_fact_orders` count; `raw_orders` count |
| 3 | Increment | 10 updates, 150 inserts, no dup keys | | staging `order_status`, logs |
| 4 | Missing products.csv | Extract/validate fails, retries | | Task state, logs |
| 5 | 5 bad order rows | Quarantine with reasons | | quarantine CSV, `dq_results` |
| 6 | Rename order_amount | Loud failure | | task error log |
| 7 | 50 orders only | Volume warning (add check if missing) | | dq / custom check |
| 8 | unit_price ×100 | Margin/revenue drift | | mart compare |
| 9 | Stale orders | Timeliness warning | | dq_results |
| 10 | Clear failed task | Partial re-run | | Airflow UI |

**Run audit:** `pipeline_runs` table populated by final task (`trigger_rule=all_done`).

**Observability pillars:** Strongest = **lineage** (ELT raw + `_source_file`); weakest = **distribution** (no automated drift alerts yet).

---

## Part G — Recommendation to Sameer (CTO)

**Recommendation: adopt ELT as the primary platform, with ETL-style critical gates in the staging SQL layer for finance marts.**

- **Auditability:** ELT keeps every raw row; finance still reads only `dq_status='PASS'` marts.
- **Quality:** ETL proves you can hard-stop bad revenue rows; same rules enforced in SQL staging.
- **Change:** SQL marts easier for analytics to iterate without redeploying Python.
- **Scale:** DuckDB/warehouse SQL scales better than pandas transforms at 1000× volume (then move to cloud warehouse).
- **Cost:** Open-source stack; ELT avoids dual maintenance of business logic once team matures.

**Top 3 risks:** (1) raw layer PII exposure if access control is weak; (2) staging SQL complexity / MERGE bugs on increments; (3) learners’ laptops miss schedules — need managed Airflow in production.

---

## Business questions — sample SQL

### ETL (DuckDB)

```sql
-- 1) Daily net revenue by zone and store (delivered)
SELECT order_date, zone, store_id, SUM(net_revenue)
FROM etl_fact_orders WHERE order_status = 'DELIVERED'
GROUP BY 1,2,3;

-- 2) Revenue & margin by category
SELECT category, SUM(net_revenue), SUM(gross_margin)
FROM etl_fact_orders WHERE order_status = 'DELIVERED'
GROUP BY 1;

-- 3) Delivery SLA by store
SELECT store_id,
       AVG(delivery_minutes) AS avg_min,
       100.0 * AVG(CASE WHEN within_15_min THEN 1 ELSE 0 END) AS pct_15
FROM etl_fact_orders WHERE order_status = 'DELIVERED'
GROUP BY 1;

-- 4) Revenue by payment mode + reconciliation total
SELECT payment_mode, SUM(net_revenue) FROM etl_fact_orders
WHERE order_status = 'DELIVERED' GROUP BY 1;
SELECT SUM(CAST(order_amount AS DOUBLE)) FROM etl_payment_reconciliation;

-- 5) Top customers (masked)
SELECT customer_id, email_masked, loyalty_tier, SUM(net_revenue) AS spend
FROM etl_fact_orders JOIN etl_dim_customer USING (customer_id)
WHERE order_status = 'DELIVERED'
GROUP BY 1,2,3 ORDER BY spend DESC LIMIT 10;

-- 6) Rejected today by file/reason — see quarantine CSVs + dq_results
SELECT * FROM dq_results ORDER BY checked_at DESC;
```

### ELT

Use `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers`, and `stg_orders WHERE dq_status='FAIL'`.

---

## How to run locally

```bash
cd IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model
astro dev start
astro dev run dags list-import-errors
astro dev run dags test nammamart_etl_udaykiran_komati
astro dev run dags test nammamart_elt_udaykiran_komati
```

Airflow UI: http://localhost:8080 (admin / admin)

## Submit PR

Push branch to your fork and open a PR to `UmashankarKH/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model` (same pattern as [class PRs](https://github.com/UmashankarKH/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model/pulls)).

**Screenshots to add:** DAG graph (green), schedule 9 AM IST, quarantine file, `dq_results`, `pipeline_runs`, reconciliation query.
