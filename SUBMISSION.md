# SUBMISSION

## Part 0: Data Issues Log

| # | File | Row key | Column | Problem | Dimension | Proposed rule | Severity | Count |
|---|------|---------|--------|---------|-----------|---------------|----------|-------|
| 1 | orders | order_id | customer_id | customer_id is null | Completeness | customer_id not null | Critical | 18 |
| 2 | orders | order_id | delivery_minutes | DELIVERED order has no delivery_minutes | Completeness | DELIVERED requires delivery_minutes | Critical | 8 |
| 3 | orders | order_id | order_id | Duplicate order_id | Uniqueness | order_id unique, keep first | Critical | see CSV |
| 4 | orders | order_id | order_status | Status not in DELIVERED/CANCELLED/RETURNED | Validity | status in allowed list | Critical | 5 |
| 5 | orders | order_id | payment_mode | Casing/variants (e.g. upi) | Validity | map to UPI/Card/Cash on Delivery/Wallet | Warning | 10 |
| 6 | orders | order_id | discount_pct | Outside 0-100 | Validity | discount between 0 and 100 | Critical | 3 |
| 7 | orders | order_id | quantity | Quantity <= 0 | Accuracy | quantity > 0 | Critical | 10 |
| 8 | orders | order_id | order_amount | Does not equal qty x price x (1 - disc) | Accuracy | recompute and compare | Critical | 18 |
| 9 | orders | order_id | delivery_minutes | More than 120 minutes | Accuracy | delivery_minutes < 120 | Critical | 6 |
| 10 | orders | order_id | store_id | Store not in stores master | Consistency | store_id exists | Critical | 5 |
| 11 | orders | order_id | product_id | Product not in products master | Consistency | product_id exists | Critical | 9 |
| 12 | orders | order_id | order_ts | Future-dated or unparseable timestamp | Timeliness | order_ts not in future | Critical | see CSV |
| 13 | payments | payment_id | payment_id | Duplicate payment_id | Uniqueness | payment_id unique | Critical | see CSV |
| 14 | payments | payment_id | order_id | Payment has no matching order | Consistency | order_id exists in orders | Critical | 8 |
| 15 | payments | payment_id | amount_paid | SUCCESS amount differs from order_amount | Consistency | goes to reconciliation table | Warning | 14 |
| 16 | payments | order_id | (missing row) | Non-cancelled orders with no payment | Completeness | goes to reconciliation table | Warning | 28 |
| 17 | customers | customer_id | email | Invalid email format | Validity | email regex | Warning | 12 |
| 18 | customers | customer_id | phone | Phone not 10 digits | Validity | phone is 10 digits | Warning | 7 |
| 19 | customers | customer_id | pincode | Pincode not 6 digits | Validity | pincode is 6 digits | Warning | 3 |
| 20 | customers | customer_id | loyalty_tier | Mixed casing (BRONZE, bronze, SILVER) | Consistency | standardise to Title case | Warning | 8 |
| 21 | customers | customer_id | email | Same email on different customer ids | Uniqueness | deduplicate customers | Warning | 14 |
| 22 | customers | customer_id | signup_date | Future-dated signup | Timeliness | signup_date not in future | Critical | 3 |
| 23 | products | product_id | mrp | mrp <= 0 | Accuracy | mrp > 0 | Critical | 1 |
| 24 | products | product_id | cost_price | cost_price greater than mrp | Accuracy | cost_price <= mrp | Critical | 5 |
| 25 | products | product_id | category | Category is blank | Completeness | category not null | Critical | 3 |
| 26 | products | product_id | product_id | Duplicate product_id | Uniqueness | product_id unique | Critical | 4 |
| 27 | stores | store_id | zone | Blank zone | Completeness | zone in North/South/East/West | Critical | 1 |
| 28 | increment | order_id | (all) | Clean; 10 corrections + 150 new orders | n/a | merge on order_id | n/a | 0 |

## 3 questions for the client
1. Is a negative or zero quantity a return or a data-entry error, and should it ever reduce revenue?
2. When a SUCCESS payment amount differs from the order amount, which system is the source of truth, and who resolves it?
3. When an order is RETURNED later, should revenue be removed from the original delivery date or booked as a negative on the return date?