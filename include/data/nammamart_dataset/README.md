# NammaMart Quick Commerce: Assignment Dataset

Fictional data for the ETL and ELT pipeline assignment. NammaMart is a 10-minute grocery delivery company in Bengaluru with 15 dark stores.

| File | Rows | Columns | Source system |
|---|---|---|---|
| orders.csv | 2,412 | 13 | Order management system (Sept 2026) |
| payments.csv | 2,228 | 7 | Payment gateway export |
| customers.csv | 405 | 10 | CRM export (contains personal data) |
| products.csv | 152 | 10 | Product catalogue |
| stores.csv | 15 | 8 | Store master |
| orders_2026-10-01_increment.csv | 160 | 13 | Next-day order export (for incremental load) |

Copy all files into `include/data/nammamart/` in your Astro project. Do not edit them by hand. The data is intentionally not clean.
