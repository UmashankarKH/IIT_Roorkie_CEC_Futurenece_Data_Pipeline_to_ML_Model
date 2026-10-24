# NammaMart business-question results

Snapshot from `include/warehouse_surender_singh.duckdb` after the successful
20 October manual ETL/ELT runs. The runnable SQL variants are in
`nammamart_business_questions.sql`. These results describe this dataset
snapshot; re-run the questions after future source changes.

## 1. Daily revenue by zone and store

September delivered revenue totals match: **₹1,130,769.10** in both ETL and
ELT. The five highest daily store totals (same in both pipelines):

| Date | Zone | Store | Revenue |
| --- | --- | --- | ---: |
| 2026-09-19 | North | NammaMart Yelahanka | ₹9,732.00 |
| 2026-09-06 | West | NammaMart Basaveshwaranagar | ₹8,785.70 |
| 2026-09-19 | West | NammaMart Malleshwaram | ₹7,889.70 |
| 2026-09-26 | South | NammaMart JP Nagar | ₹7,751.20 |
| 2026-09-26 | South | NammaMart Banashankari | ₹7,277.40 |

## 2. Revenue and gross margin by category

Snapshot values across the loaded order dates:

| Category | ELT revenue | ELT gross margin | ETL revenue | ETL gross margin |
| --- | ---: | ---: | ---: | ---: |
| Staples | ₹287,880.45 | ₹62,122.29 | ₹287,601.45 | ₹62,178.09 |
| Personal Care | ₹216,314.30 | ₹47,244.08 | ₹215,345.30 | ₹46,922.58 |
| Household | ₹212,158.00 | ₹54,497.69 | ₹211,727.05 | ₹54,352.51 |
| Dairy & Bakery | ₹180,555.80 | ₹47,746.13 | ₹179,568.95 | ₹47,633.00 |
| Snacks & Beverages | ₹174,893.55 | ₹41,425.82 | ₹174,700.60 | ₹41,370.78 |
| Fruits & Vegetables | ₹99,924.60 | ₹20,577.66 | ₹99,566.20 | ₹20,511.38 |
| Unknown / NULL | ₹41,911.75 | ₹4,943.39 | ₹41,911.75 | ₹4,943.39 |

Category totals differ in this snapshot; the cause has not been isolated.
Investigate and reconcile these before treating category margins as final
finance outputs.

## 3. Delivery SLA

The tested queries return 15 store rows per pipeline. ELT's fastest average was
Hebbal at 20.50 minutes (32.00% within 15 minutes); slowest was Banashankari at
22.52 minutes (21.57%). ETL's fastest was Hebbal at 20.47 minutes (32.21%);
slowest was Banashankari at 22.52 minutes (21.57%). Full store-level outputs
can be rerun with the SQL file.

## 4. Revenue by payment mode and reconciliation

| Mode | ELT revenue | ETL revenue (snapshot) |
| --- | ---: | ---: |
| UPI | ₹639,931.95 | ₹638,752.15 |
| Card | ₹252,682.25 | ₹251,882.30 |
| Cash on Delivery | ₹182,905.80 | ₹181,668.40 |
| Wallet | ₹138,118.45 | ₹138,118.45 |

| Reconciliation result | ELT rows | ELT amount | ETL rows | ETL amount |
| --- | ---: | ---: | ---: | ---: |
| Amount mismatch | 12 | ₹660.00 | 13 | ₹760.00 |
| Missing successful payment / record | 220 | ₹122,095.75 | 219 | ₹122,038.80 |
| Unmatched successful payment | 81 | ₹80,986.30 | Not represented | Not represented |

The implementations do not reconcile orphan successful payments the same way;
these totals are intentionally reported as a gap, not as a match.

## 5. Top customers by spend

The query returns 10 customers in both pipelines. Top three by spend were:

| Customer ID | Name | ELT tier | Spend |
| --- | --- | --- | ---: |
| C00136 | Sanjay Bhat | BRONZE | ₹8,738.10 |
| C00115 | Sanjay Kumar | BRONZE | ₹8,037.70 |
| C00283 | Nikhil Rao | BRONZE | ₹7,934.60 |

ETL returns the same top-three spend values; loyalty-tier casing can still
differ by source record, so canonicalize tiers before a customer-facing report.

## 6. Rejected rows by source and reason

Highest-volume ELT reasons: missing customer ID (18), order-amount mismatch
(15), invalid quantity (10), unknown product (9), and missing delivered-order
delivery time (8). Highest-volume ETL reasons are the same five counts.

Full breakdowns are queryable from `mart_rejected_rows_today` (ELT) and
`etl_quarantine_orders` (ETL). The ETL query currently reflects the persisted
quarantine table and should be refreshed/captured per run when documenting a
specific day's rejects.
