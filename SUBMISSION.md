# NammaMart ETL/ELT Pipeline Submission

## Completion status

Core implementation is complete and runtime-verified. Fresh Airflow runs
`wrap_up_etl_20261020_1630` and `wrap_up_elt_rejections_20261020` completed
successfully. The Astro test suite passed 12 tests. September delivered
revenue matches between ETL and ELT: **₹1,130,769.10 across 2,076 orders**.

The student reports having captured the required UI screenshots; they are not
stored in this repository. Remaining rubric gaps: controlled experiment runs
are not all completed, and customer/product/store/payment ELT staging SQL is
still inline in the DAG rather than extracted to separate SQL files.

## Part 0: Data profiling and issues

| Source | Rows | Columns | Noted issues |
| --- | ---: | ---: | --- |
| `customers.csv` | 405 | 10 | 4 missing emails, 6 missing cities, 5 repeated customer keys |
| `orders.csv` | 2,412 | 13 | Missing timestamps/customer IDs/delivery times, duplicate IDs, invalid quantities/status/payment modes, amount mismatches, unknown foreign keys, malformed/future timestamps |
| `payments.csv` | 2,228 | 7 | 4 duplicate rows, 301 missing gateway references, invalid amounts/modes, unmatched and mismatched successful payments |
| `products.csv` | 152 | 10 | 3 missing categories, repeated product keys, invalid price relationships |
| `stores.csv` | 15 | 8 | 1 missing zone |
| `orders_2026-10-01_increment.csv` | 160 | 13 | 10 corrections to existing orders and 150 new orders |

Critical order issues (missing keys, invalid quantity/status/timestamp, bad
amount, or unresolved product/store/customer key) are quarantined or marked
failed in staging with reasons. Contact-field quality issues are warnings;
source values are retained in restricted raw data and masked in reporting
dimensions where implemented.

## Part A: ETL pipeline

`dags/nammamart_etl_surender_singh.py` extracts the five source tables in
parallel, validates and quarantines invalid orders, transforms and masks
customer contact fields, loads DuckDB tables, reconciles payments and builds
reports. A fresh run completed successfully with 5,372 rows extracted, 3,023
loaded and 92 quarantined. The previous Airflow 3 XCom failure caused by
pandas `NaN` values was fixed with null-safe serialization and verified in the
successful run.

## Part B: ELT pipeline

`dags/nammamart_elt_surender_singh.py` appends raw source rows with load
metadata, builds staging tables and data-quality results, runs the SQL
`MERGE` on `order_id`, and builds five marts using
`include/sql/elt_marts.sql`. A fresh run completed with 5,372 rows extracted,
5,339 staged and 102 rejected. All 2,550 staged order IDs are unique. The
current increment state has 150 delivered new orders and 10 returned
corrections. The live MERGE reported zero inserts and updates because the
warehouse already contained the increment; `tests/dags/test_elt_sql.py`
verifies the initial 150 inserts, 10 updates, and idempotent repeat.

The SQL asset inventory and business-query results are documented in
`include/sql/README.md` and `include/sql/business_question_results.md`.
Customer, product, store and payment staging SQL is still embedded in the ELT
DAG; extracting those transformations remains a code-organization gap.

## Part C: Schedule and SLA

- Cron: `0 9 * * 1-6` (Monday through Saturday).
- Timezone: `Asia/Kolkata`, with timezone-aware Pendulum start date. The
  equivalent UI time is 03:30 UTC.
- `catchup=False` avoids replaying every missed daily run after downtime; the
  scheduler waits for the next scheduled interval.
- Retries, retry delay and execution timeout are set in both DAGs' defaults.
- A UTC start date without the IST timezone would schedule at 09:00 UTC,
  which is 14:30 IST, missing the morning SLA.

## Part D: Data quality

Both pipelines populate `dq_results` with run ID, DAG ID, check name,
dimension, severity, rows checked/failed, status and timestamp. Checks cover
completeness, uniqueness, validity, accuracy, consistency and timeliness.
Critical failures are rejected/quarantined or marked failed in staging;
warnings are recorded for review. The verified runs succeeded while recording
known bad source rows in the DQ scorecard.

## Part E: Governance

1. **Ownership:** Operations owns orders/stores; Finance owns payments and
   reconciliation policy; Customer team owns customer records; Merchandising
   owns products; Data Engineering operates warehouse models. The relevant
   business owner approves changes to rules such as delivery thresholds.
2. **Data dictionary:** `etl_fact_orders` / `stg_orders` hold order key,
   timestamp/date, customer/store/product keys, quantity, price, discount,
   status, payment mode, delivery time, calculated revenue, margin, DQ result
   and lineage. Dimensions describe customers, products and stores; payment
   tables carry payment/order IDs, timestamp, amount, mode/status and gateway
   reference. Types are cast in staging; allowed status/mode and DQ rules are
   enforced or flagged there. Customer name, contact and location fields and
   employee manager names are personal data; contact fields should be masked
   outside restricted raw access.
3. **Lineage:** The verified example `ORD001440` contributes ₹1,975 to the
   19 September Yelahanka total of ₹9,732; it appears in `orders.csv` row 348
   counting the header. ELT staging retains source-file and run metadata;
   ETL can trace by order key to the original CSV but does not retain the same
   source metadata in the fact table.
4. **Personal data:** Customer name, email, phone, customer ID and address
   fields, plus store manager name, are personal or identifying data. Mask
   email/phone in reporting while retaining restricted raw records for
   approved audit/replay. Apply the Digital Personal Data Protection Act,
   2023, and applicable rules; confirm deployment-specific obligations with
   legal counsel.
5. **Access:** Restrict raw customer data to approved custodians/engineers.
   Limit staging and quarantine to engineering, quality stewards and source
   owners. Give marts to authorized finance/operations/analytics roles with
   least privilege.
6. **Retention:** Set retention by approved business/legal purpose. A
   proposed quarantine review window is 90 days; the repository does not
   enforce retention or deletion.
7. **Secrets:** No webhook is configured. If enabled, credentials belong in
   Airflow Connections/Variables, never DAG source, committed `.env`, logs
   or screenshots.
8. **Audit:** ETL writes rejected order rows with reasons to quarantine;
   ELT retains source records in raw and flags failed staging rows with
   reasons. `pipeline_runs` and `dq_results` provide run and rule outcomes.
9. **Change history:** ELT raw append preserves the original and corrected
   order versions while staging/marts reflect the latest accepted state. ETL
   rebuilds current facts and does not retain a historical revenue snapshot.
   Finance should decide whether returns restate September or appear in a
   separate refund/change ledger.

## Part F: Observability and experiments

Verified fresh run evidence:

| Run | State | Extracted | Loaded/staged | Quarantined/rejected |
| --- | --- | ---: | ---: | ---: |
| `wrap_up_etl_20261020_1630` (ETL) | Success | 5,372 | 3,023 | 92 |
| `wrap_up_elt_rejections_20261020` (ELT) | Success | 5,372 | 5,339 | 102 |

September delivered revenue was ₹1,130,769.10 in both pipelines (2,076
orders). ELT reruns appended raw rows but left staging at 2,550 unique order
IDs; the live MERGE had 0 inserts/updates because the increment had already
been loaded. The isolated SQL test verifies the initial 150 inserts and
10 corrections.

| Experiment | Status |
| --- | --- |
| Happy path | Both DAGs freshly run successfully; screenshots are reported captured by the student. |
| Rerun/idempotency | ELT repeat observed; complete controlled ETL before/after comparison remains pending. |
| Incremental load | Verified current corrected state live; initial insert/update counts verified by regression test. |
| Missing source file | Not run as a controlled experiment. |
| New invalid records | Not run as a controlled experiment. |
| Schema change | Not run as a controlled experiment. |
| Volume anomaly | Not run as a controlled experiment. |
| Distribution drift | Not run as a controlled experiment. |
| Freshness threshold | Future timestamps are checked; 30-day stale-data experiment not run. |
| Recovery | Not run as a controlled experiment. |

Observability pillars: freshness has future-date checks but no stale-data
alert threshold; volume and distribution anomaly alerts are not implemented;
schema-change behavior still needs a controlled test; lineage is demonstrated
by ELT source metadata and the order trace above.

## Part G: Recommendation

Recommend ELT as the long-term warehouse pattern because raw append-only data
supports replay, audit and evolving transformations, while SQL staging and
marts provide a clear analytics layer. Retain ETL for strict pre-load
validation or sensitive feeds where rejecting data before warehouse access is
required. Before scaling, address restricted raw PII access, automated
freshness/volume/distribution alerting, and experiment coverage. Performance
and cost at 1,000× volume have not been benchmarked and should not be claimed
without measurement.
