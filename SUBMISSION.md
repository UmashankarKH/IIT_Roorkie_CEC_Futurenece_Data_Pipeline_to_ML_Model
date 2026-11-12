# NammaMart Pipeline — SUBMISSION.md
**Author:** Chandan  
**Cohort:** FDAIE Module 2 · IIT Roorkee CEC / Futurenece  
**Date:** October 2026

---

## Part 0: Data Issues Log

### Profiling Summary

| File | Rows | Nulls Found | Duplicates | Key Issues |
|------|------|-------------|------------|------------|
| orders.csv | 2,412 | delivery_minutes (some DELIVERED) | Checked | Status casing, amount formula, future ts |
| payments.csv | 2,228 | gateway_ref (Cash on Delivery OK) | Checked | Orphan payments, mode casing |
| customers.csv | 405 | city (some empty) | Checked | loyalty_tier casing (SILVER vs Silver), phone length |
| products.csv | 152 | None critical | Checked | cost_price > mrp on some rows |
| stores.csv | 15 | zone (S07 BTM Layout missing zone) | None | capacity=0 for S12 |

### Data Issues Log

| # | File | Row Key | Column | Problem | Quality Dimension | Proposed Rule | Severity |
|---|------|---------|--------|---------|-------------------|---------------|----------|
| 1 | orders.csv | Various | order_status | Values like "SHIPPED" not in allowed list | Validity | Map to DELIVERED/CANCELLED/RETURNED or quarantine | Critical |
| 2 | orders.csv | Various | payment_mode | "upi", "UPI " (trailing space) inconsistent casing | Validity | Normalise to canonical set on ingest | Critical |
| 3 | orders.csv | Various | order_amount | Computed value ≠ quantity × unit_price × (1-discount_pct/100) | Accuracy | Recompute and flag rows with diff > ₹0.02 | Critical |
| 4 | orders.csv | Various | delivery_minutes | DELIVERED orders with NULL delivery_minutes | Completeness | Quarantine if DELIVERED and delivery_minutes is null | Critical |
| 5 | orders.csv | Various | order_ts | Future timestamps (test data) | Timeliness | Reject rows where order_ts > NOW() | Critical |
| 6 | orders.csv | Various | customer_id / product_id / store_id | Foreign keys not found in master tables | Consistency | Quarantine rows with unresolvable FK | Critical |
| 7 | orders.csv | Various | quantity | Negative quantity | Accuracy | quantity must be > 0 | Critical |
| 8 | payments.csv | Various | order_id | Payments referencing orders not in orders.csv | Consistency | Flag as orphan payments in reconciliation | Critical |
| 9 | payments.csv | Various | payment_mode | Same casing issues as orders | Validity | Normalise on ingest | Warning |
| 10 | customers.csv | C00199 etc | loyalty_tier | "SILVER" vs "Silver" — inconsistent casing | Consistency | Normalise to Title Case (Silver/Gold/Platinum) | Warning |
| 11 | customers.csv | Various | city | Blank city (e.g. C00355) | Completeness | Log as Warning; city not critical for revenue | Warning |
| 12 | customers.csv | Various | phone | Phones not exactly 10 digits | Validity | Log as Warning; mask regardless | Warning |
| 13 | customers.csv | Various | email | Invalid email format (missing @) | Validity | Log as Warning; mask regardless | Warning |
| 14 | products.csv | Various | cost_price | cost_price > mrp (negative margin) | Accuracy | Flag as Warning; valid in promotions | Warning |
| 15 | stores.csv | S07 | zone | Zone is blank for BTM Layout store | Completeness | Default to "Unknown"; alert ops team | Warning |
| 16 | stores.csv | S12 | capacity_orders_per_day | capacity = 0 (unrealistic) | Accuracy | Flag as Warning; may be a new store | Warning |

### 3 Questions I Would Ask the Client

1. **Negative delivery_minutes**: A handful of DELIVERED orders have `delivery_minutes` set to 0 or negative. Is this a data-entry error, or do they represent same-shelf picks? Should these be quarantined or capped at a minimum?

2. **Missing zone for S07 (BTM Layout)**: The stores export has a blank `zone` for this store. What zone does it belong to — should we infer from pincode/area, or wait for the ops team to correct it?

3. **RETURNED orders and revenue**: When an order moves from DELIVERED → RETURNED in the increment file, should September's revenue be restated (deducted), or should we keep the DELIVERED revenue and record the return separately? Which number does finance want in the CFO dashboard?

---

## Part C: Scheduling Answers

### Cron Expression
```
30 3 * * 1-6
```
9:00 AM IST = UTC + 5:30 = 03:30 UTC. Monday (1) through Saturday (6).

### UTC time in Airflow UI
The run will appear scheduled at **03:30 UTC** in the Airflow UI.

### What if start_date was left in UTC?
If `start_date` is given in UTC but the schedule is intended for IST, the DAG would fire at 03:30 UTC — which is correct. However, if someone naively sets `schedule="0 9 * * 1-6"` thinking it means 9 AM IST but leaves the timezone as UTC, the DAG fires at 9 AM UTC = 2:30 PM IST — the report arrives 5.5 hours late, missing the 9:30 AM standup.

Using `pendulum.datetime(..., tz="Asia/Kolkata")` and an explicit IST-converted cron avoids this entirely.

### What happens if Docker is off at 9 AM?
`catchup=False` means Airflow will **not** queue missed runs. When Docker starts back up, Airflow will trigger the **next scheduled run** only (the coming Monday/Tuesday etc.). Today's run is simply missed. The team would need to trigger it manually via the UI or `astro dev run dags trigger`.

---

## Part E: Data Governance

### 1. Ownership
| Dataset | Owner | Change Approver |
|---------|-------|-----------------|
| orders.csv | Ops / Order Management team | VP Ops + Data Engineer |
| payments.csv | Finance / Payment Gateway team | CFO + Data Engineer |
| customers.csv | CRM / Growth team | DPO (privacy officer) |
| products.csv | Catalogue / Merchandising team | Category Manager |
| stores.csv | Operations / Dark Store team | VP Ops |
| DuckDB warehouse tables | Data Engineering (FDE) | Lead Data Engineer |

A change to the 120-minute delivery limit rule must be approved by the **VP Ops** (it changes a KPI definition) and the **Lead Data Engineer** (it changes pipeline code).

### 2. Data Dictionary (key tables)

#### `etl_fact_orders`
| Column | Type | Meaning | Source | Allowed Values | PII |
|--------|------|---------|--------|----------------|-----|
| order_id | VARCHAR | Unique order identifier | orders.csv | ORD* format | No |
| order_date | VARCHAR | Date of order (YYYY-MM-DD) | Derived from order_ts | — | No |
| customer_id | VARCHAR | FK to dim_customer | orders.csv | C* format | No |
| net_revenue | DOUBLE | Revenue after discount | Computed | ≥ 0 | No |
| gross_margin | DOUBLE | Net revenue minus cost | Computed | Can be negative | No |
| within_15_min | INTEGER | 1 if delivered in ≤15 min | Computed | 0 or 1 | No |
| email | VARCHAR | Masked customer email | customers.csv (masked) | pr***@domain | **Yes** (masked) |
| phone | VARCHAR | Masked customer phone | customers.csv (masked) | 98******21 | **Yes** (masked) |

#### `etl_dim_customer`
| Column | Type | PII |
|--------|------|-----|
| customer_id | VARCHAR | No |
| full_name | VARCHAR | **Yes** |
| email | VARCHAR | **Yes** (masked) |
| phone | VARCHAR | **Yes** (masked) |
| loyalty_tier | VARCHAR | No |

### 3. Lineage

**Tracing one number: `mart_daily_store_kpis.net_revenue` for store S05, 2026-09-10**

1. `mart_daily_store_kpis` ← SQL aggregation of `stg_orders`
2. `stg_orders` ← SQL transformation of `raw_orders`
3. `raw_orders` ← CSV `orders.csv` loaded by `load_raw_orders` task in ELT DAG
4. The exact rows: `SELECT * FROM raw_orders WHERE store_id='S05' AND order_ts LIKE '2026-09-10%' AND _source_file='orders.csv'`

**Can you do it in ETL?** Yes — `etl_fact_orders._run_id` links back to the DAG run, and the quarantine file shows which rows were dropped. But you cannot query the original bad rows (they are in a CSV quarantine file, not in DuckDB).

**Can you do it in ELT?** Yes, more easily — `raw_orders` holds every row exactly as received, so you can `JOIN raw_orders ON order_id` to trace any mart number back to its exact source byte.

**Which is easier?** ELT — the raw layer is the single source of truth, queryable with SQL.

### 4. Personal Data

| Column | Table | Why PII |
|--------|-------|---------|
| full_name | customers.csv | Identifies a person |
| email | customers.csv | Personal contact |
| phone | customers.csv | Personal contact |

**Masking approach:**
- Email: `pr***@example.com` (first 2 chars + *** + @domain)
- Phone: `98******21` (first 2 + ****** + last 2)

**Why mask rather than delete?** We need to show loyalty tier, city, and spend in reports without exposing raw contact info. Masked values let analysts identify broad patterns (e.g., top customer tier) without being able to contact or identify individuals.

**Applicable law:** The **Digital Personal Data Protection (DPDP) Act, 2023** (India). It requires: consent for data collection, purpose limitation, data minimisation, right to erasure, and breach notification. NammaMart must appoint a Data Fiduciary and ensure customer data is not shared outside authorised systems.

### 5. Access Control

| Layer | Who Can Read |
|-------|-------------|
| raw_customers (unmasked) | DBA + DPO only; not analysts, not engineers by default |
| raw_orders, raw_payments | Data Engineers, Finance (read-only) |
| stg_* tables | Data Engineers, Analytics |
| mart_* tables | Business users, COO dashboard, BI tools |
| quarantine files | Data Engineers only |

### 6. Retention

| Data | Retention Period | Action After |
|------|-----------------|--------------|
| raw_orders, raw_payments | 7 years (tax/legal) | Archive to cold storage |
| quarantine CSV files | 90 days | Delete |
| masked customer data (mart) | 3 years | Anonymise further |
| raw_customers (unmasked) | 2 years from last activity | Delete per DPDP right to erasure |

### 7. Secrets
Credentials (e.g., `SLACK_WEBHOOK_URL`) are stored as **Airflow Variables** (`Variable.get("SLACK_WEBHOOK_URL")`). They must never be in the DAG file or GitHub because:
- DAG files are committed to version control — a secret in code becomes a permanent git history entry
- Airflow Variables are stored encrypted in the metadata DB and never leave the server
- GitHub scanning (e.g., GitGuardian) would flag them as leaked secrets

### 8. Audit Trail

**ETL:** Finance asks — "Show me order ORD001234, why was it rejected, and prove it's not in revenue."
1. Check `include/quarantine/chandan/orders_failed_<run_id>.csv` — find the row with `order_id=ORD001234` and read `failure_reason`.
2. Check `etl_fact_orders` — `SELECT * FROM etl_fact_orders WHERE order_id='ORD001234'` → 0 rows (proves it's not in revenue).
3. Check `dq_results` — `SELECT * FROM dq_results WHERE run_id='<run_id>'` to see which check fired.

**ELT:** Same question:
1. `SELECT * FROM raw_orders WHERE order_id='ORD001234'` → exact row as received.
2. `SELECT * FROM stg_orders WHERE order_id='ORD001234'` → see `dq_status='FAIL'` and `failure_reason`.
3. `SELECT * FROM mart_daily_store_kpis` — ORD001234 will not contribute because `stg_orders` filters `WHERE dq_status='PASS'`.
4. No quarantine file needed — the bad row is visible in the warehouse itself.

### 9. Change History (RETURNED order scenario)

After the incremental load, an order that was DELIVERED on 3 September and RETURNED on 1 October:

**ETL:** `etl_fact_orders` is upserted — the row now shows `order_status='RETURNED'`. September revenue is **reduced** (the return is applied). Finance gets the "restated" September number.

**ELT:** `raw_orders` has both the original DELIVERED row AND the new RETURNED row (the audit log is complete). `stg_orders` after the increment merge shows `order_status='RETURNED'`. `mart_daily_store_kpis` for September will **not count** the returned order.

**What finance wants:** Finance typically wants **two views** — (a) revenue as it was on 3 September (for period reporting), and (b) current state including the return. To support both, an SCD Type 2 dimension or an event-sourcing approach (keep both rows in raw, flag the latest) is needed. In ELT this is natural (raw keeps all rows); in ETL you would need explicit versioning.

---

## Part G: Recommendation to the CTO

**To: Sameer (CTO, NammaMart)**  
**Re: ETL vs ELT — which should NammaMart adopt?**

**Recommendation: ELT with DuckDB**

After running both pipelines on the same NammaMart September data, I recommend the **ELT approach** for the following reasons:

### Evidence from the pipelines

| Criterion | ETL | ELT | Winner |
|-----------|-----|-----|--------|
| Auditability | Quarantine CSV (hard to query) | Raw table in DuckDB (full SQL) | **ELT** |
| Data quality guarantee | Bad rows never reach finance tables | Bad rows in raw, filtered in staging | Tie |
| Ease of change | Change Python → re-validate + re-load | Change SQL file → re-run mart task | **ELT** |
| Performance at 1000× scale | pandas in memory → will OOM | DuckDB columnar → scales to ~100 GB | **ELT** |
| Cost | Same (open-source) | Same (open-source) | Tie |

### Key observations

1. **Auditability is far better in ELT.** When finance asked "show me ORD001234 as received", I could answer with one SQL query on `raw_orders`. In ETL I had to open a CSV quarantine file.
2. **Answering new questions is faster in ELT.** When the COO asked for a new metric (% orders over capacity), I added a line to `mart_daily_store_kpis.sql` without touching Python. In ETL I would have modified the transform task and re-tested the whole pipeline.
3. **Totals matched.** Both pipelines produced the same September net revenue and margin, validating the DQ rules.

### Top 3 risks of ELT

1. **Raw data contains unmasked PII.** `raw_customers` has real emails and phones. Access control is critical — restrict to DBAs and DPO only.
2. **SQL complexity can hide bad logic.** A wrong `WHERE` clause in a mart SQL file silently produces wrong numbers. Add automated reconciliation tests (compare mart totals to known benchmarks after every run).
3. **DuckDB is single-user.** At 1000× data volume or with 10 concurrent analysts, DuckDB will hit limits. Plan a migration path to MotherDuck (managed DuckDB) or ClickHouse at that scale.

**Immediate next step:** Lock down `raw_customers` access, add a Slack alert on any mart job failure, and schedule a quarterly DQ rule review with the ops and finance teams.

---

*Submitted by: Chandan | FDAIE Module 2 | IIT Roorkee CEC / Futurenece*
