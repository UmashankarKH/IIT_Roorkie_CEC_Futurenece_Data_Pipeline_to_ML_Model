# Business-question results

These CSVs contain the complete outputs of the six queries in [`etl_queries.sql`](../etl_queries.sql) and [`elt_queries.sql`](../elt_queries.sql). They were generated from the latest successful runs recorded in the warehouses on 2026-10-22:

- ETL: `manual__2026-10-22T12:29:05.382311+00:00`
- ELT: `manual__2026-10-22T12:29:26.212817+00:00`

| Question | ETL result | Rows | ELT result | Rows |
|---|---|---:|---|---:|
| Q1. Daily delivered net revenue by zone and store | [`etl_q01.csv`](./etl_q01.csv) | 415 | [`elt_q01.csv`](./elt_q01.csv) | 429 |
| Q2. Revenue and gross margin by category | [`etl_q02.csv`](./etl_q02.csv) | 6 | [`elt_q02.csv`](./elt_q02.csv) | 6 |
| Q3. Delivery average and 15-minute rate by store | [`etl_q03.csv`](./etl_q03.csv) | 14 | [`elt_q03.csv`](./elt_q03.csv) | 14 |
| Q4. Revenue by payment mode and total unreconciled amount | [`etl_q04.csv`](./etl_q04.csv) | 5 | [`elt_q04.csv`](./elt_q04.csv) | 6 |
| Q5. Top 10 customers by delivered spend and loyalty tier | [`etl_q05.csv`](./etl_q05.csv) | 10 | [`elt_q05.csv`](./elt_q05.csv) | 10 |
| Q6. Rejected rows by file and reason | [`etl_q06.csv`](./etl_q06.csv) | 41 reason groups | [`elt_q06.csv`](./elt_q06.csv) | 27 reason groups |

## Notes

- Q1–Q5 are reporting outputs; Q5 selects only customer IDs and loyalty tiers, never names, email addresses, or phone numbers.
- Q4's total unreconciled exposure is **₹314,002.40 in ETL** and **₹383,537.20 in ELT**. The pipelines currently use different reconciliation logic, so these amounts are not directly comparable until Finance approves a shared definition. The SQL exposes the rule used by each output.
- Q6's ELT counts are grouped by failed check/reason. A staged row can fail multiple checks, so the sum of reason counts (870) is greater than the run audit's 813 quarantined rows. ETL's quarantine reason strings group to 817 rejected rows, matching its run audit.
- ELT includes the increment feed and applies its ten status corrections; ETL's current daily revenue output uses the September source snapshot. This explains why Q1 and Q3 row counts differ and why the snapshots should be interpreted with their run context.
