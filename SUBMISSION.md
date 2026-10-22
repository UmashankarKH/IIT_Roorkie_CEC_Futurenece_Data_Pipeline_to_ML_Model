# NammaMart Pipeline Submission Notes

## Part C: Schedule

- **Cron:** `0 9 * * 1-6` (09:00, Monday through Saturday).
- **Timezone:** Both DAGs use `pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")`. The schedule fires at 09:00 IST, which is **03:30 UTC**. In UTC, that is the time shown for the scheduled run; the Airflow UI can display a configured user timezone.
- **Catchup:** `catchup=False` avoids launching a backlog of historical daily runs when these DAGs are first enabled or resumed. That suits this local assignment pipeline, where a missed interval should be handled deliberately rather than replayed automatically.
- **Retries and timeout:** Each DAG retries a failed task twice, with a two-minute delay, and gives each task a 25-minute execution timeout.

### Schedule questions

1. **Cron expression:** `0 9 * * 1-6`.
2. **UTC time:** 03:30 UTC.
3. **If `start_date` were naive/UTC:** Airflow would interpret the cron schedule in UTC, so it would run at 09:00 UTC (14:30 IST), 5 hours 30 minutes after the intended local time. The DAG timezone is derived from its timezone-aware `start_date`.
4. **If the laptop or Docker is off at 9 AM:** The local scheduler cannot create or run the scheduled DAG run while it is stopped. With `catchup=False`, Airflow does not automatically replay the missed scheduled interval after restart. Manually trigger/recover the run if that day's report is still needed. The next scheduled run remains 09:00 IST on the next Monday–Saturday schedule.

## Part E: Data governance

### 1. Ownership

Orders belong to Operations; payments to Finance; customers to CRM/Customer Operations and the privacy lead; products to Merchandising; and stores to Store Operations. Data Engineering is custodian of the ETL/ELT warehouse, while each domain owner remains accountable for its meaning and quality.

A change to a business rule such as the 120-minute delivery ceiling requires approval from the Operations process owner and the data-quality steward, with Finance consulted when revenue or reconciliation is affected. The approved definition and effective date should be versioned before deployment.

### 2. Data dictionary

The dictionary below describes the ETL star schema; source names are `orders.csv`, `customers.csv`, `products.csv`, and `stores.csv`. “PII yes” includes identifiers or attributes that are personal when linked to a customer; thus fact rows are personal data in context because they contain `customer_id`.

#### `etl_fact_orders`

| Column | Type | Meaning | Source / derivation | Allowed values / rule | PII? |
|---|---|---|---|---|---|
| `order_id` | VARCHAR | Order key | `orders.order_id` | Required, unique | Yes, linkable |
| `order_ts` | TIMESTAMP | Order time | `orders.order_ts` | Parseable, not future | Yes, linked activity |
| `order_date` | DATE | Reporting date | Date of `order_ts` | Derived | Yes, linked activity |
| `customer_id` | VARCHAR | Customer key | `orders.customer_id` | Must exist in customer dimension | Yes |
| `store_id` | VARCHAR | Store key | `orders.store_id` | Must exist in store dimension | Yes, linked activity |
| `product_id` | VARCHAR | Product key | `orders.product_id` | Must exist in product dimension | Yes, linked activity |
| `quantity` | INTEGER | Units ordered | `orders.quantity` | Greater than 0 | Yes, linked activity |
| `unit_price` | DOUBLE | Price per unit | `orders.unit_price` | Numeric, non-negative | Yes, linked activity |
| `discount_pct` | DOUBLE | Discount percent | `orders.discount_pct` | 0–100 | Yes, linked activity |
| `order_amount` | DOUBLE | Amount after discount | `orders.order_amount` | Equals quantity × unit price × (1 − discount/100), rounded to 2 decimals | Yes, linked activity |
| `payment_mode` | VARCHAR | Payment method | `orders.payment_mode` | UPI, Card, Cash on Delivery, Wallet | Yes, linked activity |
| `order_status` | VARCHAR | Current order state | `orders.order_status` | DELIVERED, CANCELLED, RETURNED | Yes, linked activity |
| `channel` | VARCHAR | Order channel | `orders.channel` | Trimmed/title-cased source value | Yes, linked activity |
| `delivery_minutes` | DOUBLE | Delivery duration | `orders.delivery_minutes` | 0–120 critical; usual range 8–35 | Yes, linked activity |
| `net_revenue` | DOUBLE | Delivered revenue after discount | `order_amount` for DELIVERED; otherwise 0 | Derived | Yes, linked activity |
| `gross_margin` | DOUBLE | Delivered revenue less cost | `net_revenue − quantity × product.cost_price`; otherwise 0 | Derived | Yes, linked activity |
| `within_15_min` | BOOLEAN | Delivered within 15 minutes | Status and `delivery_minutes` | True only for DELIVERED with duration ≤15 | Yes, linked activity |

#### `etl_dim_customer`

| Column | Type | Meaning | Source / derivation | Allowed values / rule | PII? |
|---|---|---|---|---|---|
| `customer_id` | VARCHAR | Customer key | `customers.customer_id` | Required, unique | Yes |
| `masked_email` | VARCHAR | Partially masked email | `customers.email` | Masked or blank; invalid source values masked | Yes, masked/linkable |
| `masked_phone` | VARCHAR | Partially masked phone | `customers.phone` | `NN******NN` for 10 digits; otherwise masked | Yes, masked/linkable |
| `city`, `area` | VARCHAR | Customer location | `customers.city`, `customers.area` | Trimmed source values | Yes, linked attributes |
| `pincode` | VARCHAR | Customer postal code | `customers.pincode` | Six digits (warning check) | Yes, linked attribute |
| `signup_date` | DATE | Registration date | `customers.signup_date` | Parseable, not future | Yes, linked attribute |
| `loyalty_tier` | VARCHAR | Customer tier | `customers.loyalty_tier` | Bronze, Silver, Gold, Platinum | Yes, linked attribute |
| `is_active` | BOOLEAN | Account activity flag | `customers.is_active` | True / False | Yes, linked attribute |

#### `etl_dim_product`

| Column | Type | Meaning | Source / derivation | Allowed values / rule | PII? |
|---|---|---|---|---|---|
| `product_id` | VARCHAR | Product key | `products.product_id` | Required, unique | No |
| `product_name` | VARCHAR | Display name | `products.product_name` | Trimmed source value | No |
| `category`, `sub_category` | VARCHAR | Product classification | `products.category`, `products.sub_category` | Source catalogue values; ETL title-cases | No |
| `brand` | VARCHAR | Brand | `products.brand` | Trimmed source value | No |
| `mrp` | DOUBLE | Maximum retail price | `products.mrp` | Greater than 0 | No |
| `cost_price` | DOUBLE | Unit cost | `products.cost_price` | Non-negative and ≤ MRP | No |
| `unit` | VARCHAR | Selling unit | `products.unit` | Trimmed source value | No |
| `is_perishable` | BOOLEAN | Perishability flag | `products.is_perishable` | True / False | No |
| `launch_date` | DATE | Product launch date | `products.launch_date` | Parseable date | No |

#### `etl_dim_store`

| Column | Type | Meaning | Source / derivation | Allowed values / rule | PII? |
|---|---|---|---|---|---|
| `store_id` | VARCHAR | Store key | `stores.store_id` | Required, unique | No |
| `store_name` | VARCHAR | Store display name | `stores.store_name` | Trimmed source value | No |
| `zone` | VARCHAR | Operating zone | `stores.zone` | North, South, East, West (ETL title case) | No |
| `area` | VARCHAR | Store locality | `stores.area` | Trimmed source value | No |
| `pincode` | VARCHAR | Store postal code | `stores.pincode` | Six digits (warning check) | No |
| `open_date` | DATE | Store opening date | `stores.open_date` | Parseable date | No |
| `capacity_orders_per_day` | BIGINT | Daily order capacity | `stores.capacity_orders_per_day` | Integer source value | No |

ELT keeps source columns as VARCHAR in `raw_*`, types and masks them in `stg_*`, and applies SQL normalization there. For example, ELT currently stores `zone` and `category` in uppercase, while ETL title-cases them; comparisons should normalize casing.

### 3. Lineage

Example KPI: `mart_daily_store_kpis` for **2026-09-01 / S01 (NammaMart Hebbal)** has `delivered_orders = 4` and `net_revenue = ₹2,247.80`. It comes from `orders.csv` physical lines **444, 728, 1369, and 1654** (`ORD000058`, `ORD000025`, `ORD000013`, `ORD000066`); their `order_amount` values sum to ₹2,247.80.

ETL can trace retained fact rows by `order_id` back to the supplied source CSV, but its fact table has no persisted source filename or row number and rejected rows are only in quarantine. ELT is easier: `raw_orders` preserves the original values with `_source_file` and `_run_id`, and the staging/mart filters and joins are SQL; it still does not persist a CSV row number, so adding `_source_row_number` would make row-position lineage direct.

### 4. Personal data

Direct customer identifiers are `customers.full_name`, `customers.email`, and `customers.phone`; in context, all customer attributes and `customer_id` are personal data, and all `orders`/`payments` fields are linkable personal transaction data through `customer_id`/`order_id`. `stores.manager_name` is personal data about an employee.

ETL omits full names from its customer dimension and masks email/phone; ELT masks these in staging but intentionally retains exact raw input in `raw_customers`. Masking preserves a stable join and limited analytical utility while raw data remains restricted; masking is pseudonymization, not anonymization. India’s DPDP Act, 2023 and DPDP Rules, 2025 apply to this digital personal data, with phased commencement; they require lawful-purpose processing, clear notice and consent or another permitted ground, reasonable safeguards, rights/grievance handling, breach processes, and erasure when the purpose ends unless retention is legally required ([Act](https://www.meity.gov.in/static/uploads/2024/02/Digital-Personal-Data-Protection-Act-2023.pdf), [Rules](https://www.meity.gov.in/static/uploads/2025/11/53450e6e5dc0bfa85ebd78686cadad39.pdf)).

### 5. Access control

Raw tables should be limited to ingestion operators and the privacy/data steward; raw `customers` with unmasked contacts should be visible only to specifically authorized CRM/privacy staff when necessary. Staging and quarantine should be limited to data engineering, quality stewards, and the relevant source owner; marts should be available to Finance, Operations, and Analytics according to job need.

ETL quarantine masks customer name/email/phone; ELT has no separate quarantine table, so failed staging rows remain restricted and raw records remain highly restricted. The assignment’s local DuckDB files do not enforce per-role grants, so these are target access policies that require a role-controlled production warehouse to enforce.

### 6. Retention

Proposed policy, subject to Finance/Privacy approval and legal holds: retain raw source records for 13 months for replay and reconciliation; retain quarantine files for 90 days after the issue is resolved, then delete them while retaining non-sensitive run/check metadata. Keep masked customer dimensions only while the account is active and for 12 months after closure, then erase them unless a documented statutory or dispute hold requires longer retention.

Deletion must cover warehouse files, quarantine copies, exports, and expired backups; retain only non-identifying aggregate metrics where practical. These are internal proposal periods, not statements of statutory retention duration.

### 7. Secrets

Neither DAG currently uses a credential or Slack webhook. The repository ignores `.env`; if a webhook is added, store it as an Airflow Variable or in a configured secrets backend and retrieve it with `Variable.get("SLACK_WEBHOOK_URL")`.

Never place a credential in a DAG or GitHub: repository history and shared code expose it to people and systems that should not have access, and deleting it from the latest commit does not reliably remove past copies.

### 8. Audit

Rejected example: `include/data/nammamart/orders.csv`, physical line 275, exactly `ORD001037,2026-09-14 13:29:58,C00385,S13,P0104,1,525.0,10,472.5,UPI,DELIVERED,App,480`. ETL’s quarantine records `delivery_minutes is outside the plausible 0..120 minute range`; `ORD001037` is excluded before `etl_fact_orders` is loaded.

ELT retains the exact row in `raw_orders` with `_source_file`/`_run_id`; `stg_orders` records `dq_status='FAIL'` and the same reason, and the mart SQL excludes failed rows with `dq_status <> 'FAIL'`. Finance can verify ETL exclusion with `SELECT COUNT(*) FROM etl_fact_orders WHERE order_id='ORD001037'` (expect 0), and inspect ELT’s `stg_orders` row and failure reason before checking the mart filter.

### 9. Change history

After the Oct 1 increment, ELT upserts the order’s current status to RETURNED, so September delivered revenue is restated downward by that order’s `order_amount`; the raw base export and increment remain available in `raw_orders`. ETL currently reads the September `orders.csv` snapshot only, so it still counts the order as delivered and its September revenue remains as originally reported.

Finance should use the corrected/restated figure for current net revenue, while preserving the originally reported figure for audit. Keep both with a dated revenue snapshot and an append-only order-status history (effective time, loaded time, prior/new status, run id), or equivalent SCD Type 2 history; extend ETL to consume the increment if the ETL view must also restate.
