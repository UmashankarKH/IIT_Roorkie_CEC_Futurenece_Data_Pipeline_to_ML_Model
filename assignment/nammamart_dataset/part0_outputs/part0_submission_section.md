## Part 0: Discovery and Data Profiling

### Profiling Summary

All six source files (customers.csv, orders.csv, payments.csv, products.csv, stores.csv, orders_2026-10-01_increment.csv) were profiled across six stages: initial inspection,
structural profiling (missing values, duplicates, candidate keys, schema comparison), column profiling (numeric,
date, categorical), cross-column business rules, payment reconciliation, and referential integrity. A total of
5372 source rows were profiled. No source row was cleaned, standardised, deduplicated,
quarantined, or deleted; every DataFrame used in this section remains exactly as loaded from the CSV files.

### Data Issues Log

`data_issues_log_df` contains 208 row-level issue instances covering
181 distinct affected source rows across 5 files.
The table below groups those instances by file, rule, quality dimension, and severity; representative affected
row keys are shown for each group (not the full list). The complete row-level log is available in
`part0_data_issues_log.csv`.

| file | rule | quality_dimension | severity | affected_rows | representative_row_keys |
|---|---|---|---|---:|---|
| customers.csv | customer_id must be unique in customers.csv | Uniqueness | Critical | 5 | C00036, C00103, C00226 ... |
| customers.csv | loyalty_tier must be in the approved list | Validity | Warning | 8 | C00031, C00032, C00092 ... |
| customers.csv | city should not be blank | Completeness | Warning | 6 | C00142, C00219, C00233 ... |
| customers.csv | signup_date must not be future-dated | Timeliness | Warning | 3 | C00038, C00398, C00400 |
| orders.csv | order_amount must match the business formula | Accuracy | Critical | 18 | ORD000677, ORD000801, ORD000845 ... |
| orders.csv | order_id must be unique | Uniqueness | Critical | 12 | ORD000539, ORD000783, ORD001110 ... |
| orders.csv | product_id must exist in products.csv | Consistency | Critical | 9 | ORD000070, ORD000134, ORD000596 ... |
| orders.csv | DELIVERED orders must have delivery_minutes | Completeness | Critical | 8 | ORD000326, ORD000731, ORD000878 ... |
| orders.csv | order_ts must not be future-dated | Timeliness | Critical | 7 | ORD001089, ORD001121, ORD001693 ... |
| orders.csv | order_status must be in the approved list | Validity | Critical | 5 | ORD000153, ORD000500, ORD001066 ... |
| orders.csv | order_ts must parse as a valid timestamp | Validity | Critical | 5 | ORD000403, ORD000534, ORD000946 ... |
| orders.csv | store_id must exist in stores.csv | Consistency | Critical | 5 | ORD001046, ORD001275, ORD001492 ... |
| orders.csv | discount_pct must be between 0 and 100 | Validity | Critical | 3 | ORD001237, ORD001468, ORD002103 |
| orders.csv | order_ts must be present | Completeness | Critical | 1 | ORD000362 |
| orders.csv | customer_id must be present | Completeness | Pending Client Decision | 18 | ORD000022, ORD000580, ORD000583 ... |
| orders.csv | quantity must be greater than zero | Accuracy | Pending Client Decision | 10 | ORD000099, ORD000462, ORD000469 ... |
| orders.csv | payment_mode must be in the approved list | Validity | Warning | 10 | ORD000159, ORD000433, ORD000691 ... |
| orders.csv | delivery_minutes must not exceed the approved threshold | Accuracy | Warning | 6 | ORD000020, ORD000424, ORD000455 ... |
| payments.csv | SUCCESS payment amount must equal order_amount | Consistency | Critical | 14 | PAY0000044, PAY0000161, PAY0000323 ... |
| payments.csv | payment order_id must exist in orders.csv | Consistency | Critical | 8 | PAY0002217, PAY0002218, PAY0002219 ... |
| payments.csv | payment_ts must not be future-dated | Timeliness | Critical | 7 | PAY0000127, PAY0000267, PAY0000337 ... |
| payments.csv | payment_id must be unique | Uniqueness | Critical | 4 | PAY0000509, PAY0000755, PAY0001651 ... |
| products.csv | product_id must be unique in products.csv | Uniqueness | Critical | 2 | P0030, P0140 |
| products.csv | mrp must be greater than zero | Accuracy | Critical | 1 | P0109 |
| products.csv | cost_price should not exceed mrp | Accuracy | Warning | 5 | P0019, P0073, P0074 ... |
| products.csv | category should not be blank | Completeness | Warning | 3 | P0051, P0060, P0068 |
| stores.csv | zone should not be blank | Completeness | Warning | 1 | S07 |
| stores.csv | capacity_orders_per_day must be greater than zero | Accuracy | Warning | 1 | S12 |

### Candidate Validation Rules

31 candidate validation rules were derived from the confirmed profiling
findings above (one row per distinct validation concept, not per failing record). The full table, including
`rule_description`, `failure_action`, `business_impact`, and `client_confirmation_required`, is available in
`part0_candidate_validation_rules.csv`.

| rule_id | file | rule_name | severity | implementation_status | evidence_rows_failed |
|---|---|---|---|---|---:|
| RULE-001 | orders.csv | order_id must be present | Critical | Confirmed | 0 |
| RULE-002 | orders.csv | order_id must be unique | Critical | Confirmed | 24 |
| RULE-003 | orders.csv | order_ts must be present | Critical | Confirmed | 1 |
| RULE-004 | orders.csv | order_ts must parse as a valid timestamp | Critical | Confirmed | 5 |
| RULE-005 | orders.csv | order_ts must not be future-dated | Critical | Candidate | 7 |
| RULE-006 | orders.csv | customer_id must be present | Pending Client Decision | Pending Client Decision | 18 |
| RULE-007 | orders.csv | product_id must exist in products.csv | Critical | Confirmed | 9 |
| RULE-008 | orders.csv | store_id must exist in stores.csv | Critical | Confirmed | 5 |
| RULE-009 | orders.csv | quantity must be greater than zero | Pending Client Decision | Pending Client Decision | 10 |
| RULE-010 | orders.csv | discount_pct must be between 0 and 100 | Critical | Confirmed | 3 |
| RULE-011 | orders.csv | order_amount must match the business formula | Critical | Confirmed | 18 |
| RULE-012 | orders.csv | order_status must be in the approved list | Critical | Confirmed | 5 |
| RULE-013 | orders.csv | payment_mode must be in the approved list | Warning | Candidate | 10 |
| RULE-014 | orders.csv | DELIVERED orders must have delivery_minutes | Critical | Confirmed | 8 |
| RULE-015 | orders.csv | delivery_minutes must not exceed the approved threshold | Pending Client Decision | Pending Client Decision | 6 |
| RULE-016 | payments.csv | payment_id must be unique | Critical | Confirmed | 8 |
| RULE-017 | payments.csv | payment order_id must exist in orders.csv | Critical | Confirmed | 8 |
| RULE-018 | payments.csv | SUCCESS payment amount must equal order_amount | Critical | Confirmed | 14 |
| RULE-019 | payments.csv | payment_ts must not be future-dated | Critical | Candidate | 7 |
| RULE-020 | payments.csv | gateway_ref must be present for non-Cash-on-Delivery payments | Warning | Confirmed | 0 |
| RULE-021 | customers.csv | customer_id must be unique in customers.csv | Critical | Confirmed | 10 |
| RULE-022 | customers.csv | city should not be blank | Warning | Confirmed | 6 |
| RULE-023 | customers.csv | signup_date must not be future-dated | Warning | Candidate | 3 |
| RULE-024 | customers.csv | loyalty_tier must be in the approved list | Warning | Candidate | 8 |
| RULE-025 | products.csv | product_id must be unique in products.csv | Critical | Confirmed | 4 |
| RULE-026 | products.csv | category should not be blank | Warning | Confirmed | 3 |
| RULE-027 | products.csv | mrp must be greater than zero | Critical | Candidate | 1 |
| RULE-028 | products.csv | cost_price should not exceed mrp | Warning | Candidate | 5 |
| RULE-029 | stores.csv | store_id must be unique in stores.csv | Critical | Confirmed | 0 |
| RULE-030 | stores.csv | zone should not be blank | Warning | Confirmed | 1 |
| RULE-031 | stores.csv | capacity_orders_per_day must be greater than zero | Warning | Candidate | 1 |

### Client Questions

**Q-001. Do zero or negative quantities represent returns, corrections, removed order lines, or source-system errors?**

- Evidence: quantity <= 0 was observed in 10 of 2412 orders.csv rows (minimum quantity = -3); negative order_amount values were also observed (minimum order_amount = -1593.00).
- Why it matters: The answer affects revenue totals, gross margin, and whether these rows should be quarantined from finance reporting.
- Proposed temporary treatment: Exclude or quarantine these rows from finance reporting until the meaning is confirmed. Do not convert negative quantities to positive values.
- Suggested business owner: Operations and Finance

**Q-002. Are guest checkout or anonymous orders permitted (orders with no customer_id)?**

- Evidence: customer_id is blank in 18 of 2412 orders.csv rows, spanning both the App and Web channels and multiple order statuses, so the rows cannot be explained by a single channel or outcome.
- Why it matters: These orders may still be usable for store-level revenue, but they cannot be used reliably for top-customer reporting. The answer determines whether a missing customer_id is Critical or Warning.
- Proposed temporary treatment: Keep the raw record. Exclude the row from customer-level analytics. Quarantine from the curated order fact only if customer_id is confirmed as mandatory.
- Suggested business owner: Product/Operations and Customer Data Owner

**Q-003. Should delivery_minutes greater than 120 invalidate the order row, or remain a valid order with an SLA warning?**

- Evidence: delivery_minutes greater than 120 was found in 6 of 2412 orders.csv rows; the maximum observed value was 1440 minutes.
- Why it matters: These values distort average delivery time and the 10-minute promise KPI. The answer determines Critical versus Warning severity for this rule.
- Proposed temporary treatment: Keep the order for revenue if all financial checks pass. Exclude or flag the duration in SLA reporting. Log the row as a warning until the threshold is approved.
- Suggested business owner: Operations

### Part 0 Conclusion

The 20 `Confirmed` rules above
are ready for ETL/ELT implementation as quarantine or warning checks. The
8 `Candidate` rules are
reasonable to implement but have not been explicitly confirmed by the client. The
3
`Pending Client Decision` rules require the client's answers to the three questions above before their severity
and treatment can be finalised. Full detail for every deliverable is available in `part0_data_issues_log.csv`,
`part0_candidate_validation_rules.csv`, and `part0_client_questions.csv`.
