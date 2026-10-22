# Part 0: NammaMart data discovery and profiling

**Scope:** read-only profiling of all six supplied CSVs in `include/data/nammamart/`. No pipeline code changed.

> Counts below are data rows (header excluded). CSV values were profiled as delivered; dates/timestamps are reported as parsed calendar dates. Business thresholds explicitly described as "normally" are treated as review warnings, not automatic rejection rules.

## File profile

| File | Rows | Columns | Duplicate key rows |
|---|---:|---:|---:|
| `orders.csv` | 2412 | 13 | 12 |
| `payments.csv` | 2228 | 7 | 4 |
| `customers.csv` | 405 | 10 | 5 |
| `products.csv` | 152 | 10 | 2 |
| `stores.csv` | 15 | 8 | 0 |
| `orders_2026-10-01_increment.csv` | 160 | 13 | 0 |

## Column-level profile

Nonzero null counts are shown as `column=count`; every omitted column has zero nulls. Numeric and date fields show min to max. Category columns list their observed distinct values.

### `orders.csv`

- Nulls: `order_ts=1`, `customer_id=18`, `delivery_minutes=164`
- Numeric min to max: `quantity` -3 to 5; `unit_price` 10 to 883; `discount_pct` 0 to 250; `order_amount` -1593 to 9576; `delivery_minutes` 8 to 1440
- Date min to max: `order_ts` 2026-09-01 to 2027-01-01 (invalid: 5)
- Required category values: `payment_mode` = `CARD`, `Card`, `Cash on Delivery`, `Cashh`, `UPI`, `Wallet`, `cod`, `upi`; `order_status` = `CANCELLED`, `DELIVERED`, `Pending??`, `RETURNED`, `delivered`
- Duplicate keys: `ORD001462` x 2, `ORD001326` x 2, `ORD001434` x 2, `ORD000783` x 2, `ORD001239` x 2, `ORD001692` x 2, `ORD001870` x 2, `ORD001441` x 2, `ORD002129` x 2, `ORD001549` x 2, `ORD001110` x 2, `ORD000539` x 2

### `payments.csv`

- Nulls: `gateway_ref=301`
- Numeric min to max: `amount_paid` -1593 to 9576
- Date min to max: `payment_ts` 2026-09-01 to 2027-01-01 (invalid: 0)
- Required category values: `payment_mode` = `CARD`, `Card`, `Cash on Delivery`, `Cashh`, `UPI`, `Wallet`, `cod`, `upi`
- Duplicate keys: `PAY0001651` ? 2, `PAY0000755` ? 2, `PAY0000509` ? 2, `PAY0002000` ? 2

### `customers.csv`

- Nulls: `city=6`
- Numeric min to max: `phone` 9.80434e+06?9.99939e+09
- Date min to max: `signup_date` 2024-06-01 to 2027-01-05 (invalid: 0)
- Required category values: `loyalty_tier` = `BRONZE`, `Bronze`, `Gold`, `Platinum`, `SILVER`, `Silver`, `bronze`
- Duplicate keys: `C00103` ? 2, `C00252` ? 2, `C00036` ? 2, `C00226` ? 2, `C00313` ? 2

### `products.csv`

- Nulls: `category=3`
- Numeric min to max: `mrp` -203 to 883; `cost_price` 6.79 to 631.79
- Date min to max: `launch_date` 2023-01-03 to 2025-09-20 (invalid: 0)
- Required category values: `category` = `Dairy & Bakery`, `Fruits & Vegetables`, `Household`, `Personal Care`, `Snacks & Beverages`, `Staples`
- Duplicate keys: `P0030` ? 2, `P0140` ? 2

### `stores.csv`

- Nulls: `zone=1`
- Numeric min to max: `pincode` 560003?560102; `capacity_orders_per_day` 0 to 600
- Date min to max: `open_date` 2024-02-14 to 2025-07-28 (invalid: 0)
- Required category values: `zone` = `East`, `North`, `South`, `West`

### `orders_2026-10-01_increment.csv`

- Nulls: none
- Numeric min to max: `quantity` 1 to 4; `unit_price` 10 to 883; `discount_pct` 0 to 20; `order_amount` 11 to 2052; `delivery_minutes` 8 to 34
- Date min to max: `order_ts` 2026-09-01 to 2026-10-01 (invalid: 0)
- Required category values: `payment_mode` = `Card`, `Cash on Delivery`, `UPI`, `Wallet`; `order_status` = `DELIVERED`, `RETURNED`

## Referential integrity

| Relationship | Orphans | Keys (source row key -> missing reference) |
|---|---:|---|
| orders.customer_id -> customers.csv.customer_id | 0 | none |
| orders.store_id -> stores.csv.store_id | 5 | `ORD001799` ? `S99`, `ORD001046` ? `S17`, `ORD001492` ? `S17`, `ORD001275` ? `S17`, `ORD002126` ? `S00` |
| orders.product_id -> products.csv.product_id | 9 | `ORD000986` ? `P9455`, `ORD001058` ? `P9212`, `ORD001685` ? `P9816`, `ORD000070` ? `P9893`, `ORD000789` ? `P9989`, `ORD000596` ? `P9784`, `ORD000134` ? `P9822`, `ORD000965` ? `P9399`, `ORD002179` ? `P9761` |
| payments.order_id -> orders.order_id | 8 | `PAY0002219` ? `ORD970544`, `PAY0002220` ? `ORD968496`, `PAY0002223` ? `ORD988106`, `PAY0002221` ? `ORD941071`, `PAY0002224` ? `ORD981600`, `PAY0002218` ? `ORD997176`, `PAY0002222` ? `ORD953136`, `PAY0002217` ? `ORD966342` |

## Business-rule and increment findings

- Increment file comparison: **150 new order IDs**, **10 IDs already in September orders**, **10 existing IDs with changed values**. This identifies changed records; confirm the expected status-change count and the intended effect on September historical revenue with the client.

## Data Issues Log (180 check findings)

Each row identifies a failed check instance. Duplicate key records are reported once per repeated key. Warning marks a profile finding that needs client confirmation or review; Critical indicates a candidate rejection rule.

| File | Row key | Column | Problem | Quality dimension | Proposed rule | Severity |
|---|---|---|---|---|---|---|
| orders.csv | ORD000403 | order_ts | invalid date/timestamp 'N/A' | Validity | Parse as a valid ISO date/timestamp | Critical |
| orders.csv | ORD001610 | order_ts | invalid date/timestamp '2026-13-05 11:00:00' | Validity | Parse as a valid ISO date/timestamp | Critical |
| orders.csv | ORD000534 | order_ts | invalid date/timestamp '2026-13-05 11:00:00' | Validity | Parse as a valid ISO date/timestamp | Critical |
| orders.csv | ORD002012 | order_ts | invalid date/timestamp '31/09/2026 10:00' | Validity | Parse as a valid ISO date/timestamp | Critical |
| orders.csv | ORD000946 | order_ts | invalid date/timestamp '31/09/2026 10:00' | Validity | Parse as a valid ISO date/timestamp | Critical |
| orders.csv | ORD001462 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD001326 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD001434 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD000783 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD001239 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD001692 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD001870 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD001441 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD002129 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD001549 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD001110 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD000539 | order_id | key occurs 2 times | Uniqueness | order_id must be unique | Critical |
| payments.csv | PAY0001651 | payment_id | key occurs 2 times | Uniqueness | payment_id must be unique | Critical |
| payments.csv | PAY0000755 | payment_id | key occurs 2 times | Uniqueness | payment_id must be unique | Critical |
| payments.csv | PAY0000509 | payment_id | key occurs 2 times | Uniqueness | payment_id must be unique | Critical |
| payments.csv | PAY0002000 | payment_id | key occurs 2 times | Uniqueness | payment_id must be unique | Critical |
| customers.csv | C00103 | customer_id | key occurs 2 times | Uniqueness | customer_id must be unique | Critical |
| customers.csv | C00252 | customer_id | key occurs 2 times | Uniqueness | customer_id must be unique | Critical |
| customers.csv | C00036 | customer_id | key occurs 2 times | Uniqueness | customer_id must be unique | Critical |
| customers.csv | C00226 | customer_id | key occurs 2 times | Uniqueness | customer_id must be unique | Critical |
| customers.csv | C00313 | customer_id | key occurs 2 times | Uniqueness | customer_id must be unique | Critical |
| products.csv | P0030 | product_id | key occurs 2 times | Uniqueness | product_id must be unique | Critical |
| products.csv | P0140 | product_id | key occurs 2 times | Uniqueness | product_id must be unique | Critical |
| orders.csv | ORD002255 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD001812 | payment_mode | invalid payment mode 'Cashh' | Validity | mode in UPI/Card/Cash on Delivery/Wallet | Critical |
| orders.csv | ORD002103 | discount_pct | discount 250 outside 0..100 | Validity | discount_pct in [0,100] | Critical |
| orders.csv | ORD002103 | order_amount | amount 492.00 vs formula -738.00 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD000469 | quantity | quantity 0 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD001554 | delivery_minutes | delivered order has no delivery time | Completeness | DELIVERED orders require delivery_minutes | Critical |
| orders.csv | ORD000986 | product_id | foreign key 'P9455' absent from products.csv | Consistency | product_id must exist in products.csv | Critical |
| orders.csv | ORD000701 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD001800 | delivery_minutes | delivered order has no delivery time | Completeness | DELIVERED orders require delivery_minutes | Critical |
| orders.csv | ORD001037 | delivery_minutes | delivery time 480 outside typical 8..35 minutes | Accuracy | review delivery_minutes outside 8..35; assignment calls 8..35 normal | Critical |
| orders.csv | ORD001058 | product_id | foreign key 'P9212' absent from products.csv | Consistency | product_id must exist in products.csv | Critical |
| orders.csv | ORD002305 | order_amount | amount 6757.50 vs formula 675.75 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD002004 | order_amount | amount 6720.00 vs formula 672.00 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD002101 | delivery_minutes | delivery time 480 outside typical 8..35 minutes | Accuracy | review delivery_minutes outside 8..35; assignment calls 8..35 normal | Critical |
| orders.csv | ORD000997 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD001414 | payment_mode | invalid payment mode 'Cashh' | Validity | mode in UPI/Card/Cash on Delivery/Wallet | Critical |
| orders.csv | ORD001685 | product_id | foreign key 'P9816' absent from products.csv | Consistency | product_id must exist in products.csv | Critical |
| orders.csv | ORD000455 | delivery_minutes | delivery time 1440 outside typical 8..35 minutes | Accuracy | review delivery_minutes outside 8..35; assignment calls 8..35 normal | Critical |
| orders.csv | ORD000580 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD000731 | delivery_minutes | delivered order has no delivery time | Completeness | DELIVERED orders require delivery_minutes | Critical |
| orders.csv | ORD001277 | delivery_minutes | delivered order has no delivery time | Completeness | DELIVERED orders require delivery_minutes | Critical |
| orders.csv | ORD001799 | store_id | foreign key 'S99' absent from stores.csv | Consistency | store_id must exist in stores.csv | Critical |
| orders.csv | ORD001046 | store_id | foreign key 'S17' absent from stores.csv | Consistency | store_id must exist in stores.csv | Critical |
| orders.csv | ORD000500 | order_status | invalid status 'PENDING??' | Validity | status in DELIVERED/CANCELLED/RETURNED | Critical |
| orders.csv | ORD000845 | order_amount | amount 4712.00 vs formula 471.20 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD001296 | quantity | quantity -2 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD000964 | order_amount | amount 8370.00 vs formula 837.00 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD001459 | quantity | quantity -3 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD002019 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD001492 | store_id | foreign key 'S17' absent from stores.csv | Consistency | store_id must exist in stores.csv | Critical |
| orders.csv | ORD000867 | quantity | quantity -1 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD000362 | order_ts | required value missing | Completeness | order_ts required for order processing | Critical |
| orders.csv | ORD002262 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD002149 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD002367 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD001275 | store_id | foreign key 'S17' absent from stores.csv | Consistency | store_id must exist in stores.csv | Critical |
| orders.csv | ORD000462 | quantity | quantity -3 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD002298 | order_amount | amount 305.10 vs formula 610.20 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD002031 | order_amount | amount 3210.00 vs formula 321.00 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD002360 | quantity | quantity 0 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD000326 | delivery_minutes | delivered order has no delivery time | Completeness | DELIVERED orders require delivery_minutes | Critical |
| orders.csv | ORD001332 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD001724 | quantity | quantity 0 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD000070 | product_id | foreign key 'P9893' absent from products.csv | Consistency | product_id must exist in products.csv | Critical |
| orders.csv | ORD000801 | order_amount | amount 5467.50 vs formula 3645.00 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD001918 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD002156 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD000583 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD000099 | quantity | quantity 0 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD000880 | order_amount | amount 517.65 vs formula 345.10 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD000790 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD001515 | delivery_minutes | delivered order has no delivery time | Completeness | DELIVERED orders require delivery_minutes | Critical |
| orders.csv | ORD000789 | product_id | foreign key 'P9989' absent from products.csv | Consistency | product_id must exist in products.csv | Critical |
| orders.csv | ORD000764 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD002219 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD000596 | product_id | foreign key 'P9784' absent from products.csv | Consistency | product_id must exist in products.csv | Critical |
| orders.csv | ORD001274 | order_amount | amount 110.00 vs formula 220.00 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD000677 | order_amount | amount 394.20 vs formula 262.80 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD002223 | order_amount | amount 36.58 vs formula 73.15 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD002128 | order_amount | amount 265.50 vs formula 177.00 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD001624 | order_amount | amount 9576.00 vs formula 957.60 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD001532 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD001124 | quantity | quantity -1 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD000022 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD002229 | delivery_minutes | delivered order has no delivery time | Completeness | DELIVERED orders require delivery_minutes | Critical |
| orders.csv | ORD000134 | product_id | foreign key 'P9822' absent from products.csv | Consistency | product_id must exist in products.csv | Critical |
| orders.csv | ORD002244 | order_amount | amount 108.30 vs formula 72.20 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD001755 | order_amount | amount 730.00 vs formula 1460.00 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD000965 | product_id | foreign key 'P9399' absent from products.csv | Consistency | product_id must exist in products.csv | Critical |
| orders.csv | ORD002126 | store_id | foreign key 'S00' absent from stores.csv | Consistency | store_id must exist in stores.csv | Critical |
| orders.csv | ORD002179 | product_id | foreign key 'P9761' absent from products.csv | Consistency | product_id must exist in products.csv | Critical |
| orders.csv | ORD001970 | order_status | invalid status 'PENDING??' | Validity | status in DELIVERED/CANCELLED/RETURNED | Critical |
| orders.csv | ORD000878 | delivery_minutes | delivered order has no delivery time | Completeness | DELIVERED orders require delivery_minutes | Critical |
| orders.csv | ORD001104 | delivery_minutes | delivery time 900 outside typical 8..35 minutes | Accuracy | review delivery_minutes outside 8..35; assignment calls 8..35 normal | Critical |
| orders.csv | ORD001640 | payment_mode | invalid payment mode 'cod' | Validity | mode in UPI/Card/Cash on Delivery/Wallet | Critical |
| orders.csv | ORD001637 | order_status | invalid status 'PENDING??' | Validity | status in DELIVERED/CANCELLED/RETURNED | Critical |
| orders.csv | ORD001468 | discount_pct | discount 120 outside 0..100 | Validity | discount_pct in [0,100] | Critical |
| orders.csv | ORD001468 | order_amount | amount 547.20 vs formula -136.80 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD001373 | customer_id | required value missing | Completeness | customer_id required for order processing | Critical |
| orders.csv | ORD000020 | delivery_minutes | delivery time 900 outside typical 8..35 minutes | Accuracy | review delivery_minutes outside 8..35; assignment calls 8..35 normal | Critical |
| orders.csv | ORD001237 | discount_pct | discount 150 outside 0..100 | Validity | discount_pct in [0,100] | Critical |
| orders.csv | ORD001237 | order_amount | amount 2408.00 vs formula -1505.00 | Accuracy | order_amount = round(quantity * unit_price * (1-discount_pct/100), 2) | Critical |
| orders.csv | ORD001384 | quantity | quantity -1 is not positive | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD000424 | delivery_minutes | delivery time 480 outside typical 8..35 minutes | Accuracy | review delivery_minutes outside 8..35; assignment calls 8..35 normal | Critical |
| payments.csv | PAY0001568 | amount_paid | SUCCESS amount -43.05 != order amount 56.95 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0002219 | order_id | order 'ORD970544' not found | Consistency | payments.order_id must match orders.order_id | Critical |
| payments.csv | PAY0000354 | amount_paid | SUCCESS amount 455.00 != order amount 505.00 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0000779 | amount_paid | SUCCESS amount 2.60 != order amount 102.60 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0002220 | order_id | order 'ORD968496' not found | Consistency | payments.order_id must match orders.order_id | Critical |
| payments.csv | PAY0000323 | amount_paid | SUCCESS amount 182.95 != order amount 192.95 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0001167 | amount_paid | SUCCESS amount 452.35 != order amount 502.35 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0001499 | amount_paid | SUCCESS amount 349.30 != order amount 374.30 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0001954 | payment_mode | invalid payment mode 'cod' | Validity | mode in UPI/Card/Cash on Delivery/Wallet | Critical |
| payments.csv | PAY0000909 | amount_paid | SUCCESS amount 103.00 != order amount 203.00 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0002223 | order_id | order 'ORD988106' not found | Consistency | payments.order_id must match orders.order_id | Critical |
| payments.csv | PAY0000586 | amount_paid | SUCCESS amount 2.60 != order amount 102.60 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0000161 | amount_paid | SUCCESS amount 232.20 != order amount 282.20 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0002221 | order_id | order 'ORD941071' not found | Consistency | payments.order_id must match orders.order_id | Critical |
| payments.csv | PAY0002183 | amount_paid | SUCCESS amount 955.40 != order amount 980.40 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0002224 | order_id | order 'ORD981600' not found | Consistency | payments.order_id must match orders.order_id | Critical |
| payments.csv | PAY0000522 | payment_mode | invalid payment mode 'Cashh' | Validity | mode in UPI/Card/Cash on Delivery/Wallet | Critical |
| payments.csv | PAY0000044 | amount_paid | SUCCESS amount 353.00 != order amount 378.00 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0000044 | payment_mode | invalid payment mode 'Cashh' | Validity | mode in UPI/Card/Cash on Delivery/Wallet | Critical |
| payments.csv | PAY0001813 | amount_paid | SUCCESS amount 402.40 != order amount 502.40 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0002218 | order_id | order 'ORD997176' not found | Consistency | payments.order_id must match orders.order_id | Critical |
| payments.csv | PAY0001139 | amount_paid | SUCCESS amount 353.00 != order amount 378.00 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0002222 | order_id | order 'ORD953136' not found | Consistency | payments.order_id must match orders.order_id | Critical |
| payments.csv | PAY0000429 | amount_paid | SUCCESS amount 218.20 != order amount 243.20 | Consistency | SUCCESS payment amount must equal order_amount | Critical |
| payments.csv | PAY0002217 | order_id | order 'ORD966342' not found | Consistency | payments.order_id must match orders.order_id | Critical |
| customers.csv | C00337 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00307 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00151 | phone | phone has 9 digits after removing punctuation | Validity | phone should contain 10 digits | Warning |
| customers.csv | C00063 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00045 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00363 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00310 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00240 | phone | phone has 8 digits after removing punctuation | Validity | phone should contain 10 digits | Warning |
| customers.csv | C00095 | phone | phone has 9 digits after removing punctuation | Validity | phone should contain 10 digits | Warning |
| customers.csv | C00357 | phone | phone has 9 digits after removing punctuation | Validity | phone should contain 10 digits | Warning |
| customers.csv | C00130 | phone | phone has 9 digits after removing punctuation | Validity | phone should contain 10 digits | Warning |
| customers.csv | C00327 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00290 | phone | phone has 7 digits after removing punctuation | Validity | phone should contain 10 digits | Warning |
| customers.csv | C00112 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00339 | phone | phone has 9 digits after removing punctuation | Validity | phone should contain 10 digits | Warning |
| customers.csv | C00193 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00392 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00062 | email | email does not match basic address format | Validity | validate email format | Warning |
| customers.csv | C00067 | email | email does not match basic address format | Validity | validate email format | Warning |
| products.csv | P0019 | cost_price | cost price 264 exceeds mrp 220 | Accuracy | cost_price <= mrp | Critical |
| products.csv | P0073 | cost_price | cost price 334.8 exceeds mrp 279 | Accuracy | cost_price <= mrp | Critical |
| products.csv | P0074 | cost_price | cost price 420 exceeds mrp 350 | Accuracy | cost_price <= mrp | Critical |
| products.csv | P0109 | mrp | mrp -203 is not positive | Accuracy | mrp > 0 | Critical |
| products.csv | P0109 | cost_price | cost price 162.5 exceeds mrp -203 | Accuracy | cost_price <= mrp | Critical |
| products.csv | P0117 | cost_price | cost price 92.4 exceeds mrp 77 | Accuracy | cost_price <= mrp | Critical |
| customers.csv | C00132 | pincode | pincode '5600XX' is not six digits | Validity | pincode is six digits | Warning |
| customers.csv | C00083 | pincode | pincode '5600XX' is not six digits | Validity | pincode is six digits | Warning |
| customers.csv | C00166 | pincode | pincode '5600XX' is not six digits | Validity | pincode is six digits | Warning |

| stores.csv | S07 | zone | zone is missing | Completeness | Every store must have a zone | Critical |
| orders_2026-10-01_increment.csv | ORD000017 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders_2026-10-01_increment.csv | ORD000271 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders_2026-10-01_increment.csv | ORD000332 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders_2026-10-01_increment.csv | ORD000337 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders_2026-10-01_increment.csv | ORD000584 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders_2026-10-01_increment.csv | ORD000708 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders_2026-10-01_increment.csv | ORD000822 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders_2026-10-01_increment.csv | ORD001025 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders_2026-10-01_increment.csv | ORD001067 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders_2026-10-01_increment.csv | ORD001990 | order_status | Existing September order changed from DELIVERED to RETURNED | Timeliness / change history | Preserve the source correction and apply an explicit restatement/history policy | Warning |
| orders.csv | ORD002119 | order_ts | Order timestamp is in 2027, after the source reporting period | Timeliness | Reject or hold future-dated order timestamps relative to the approved business date | Critical |
| orders.csv | ORD001765 | order_ts | Order timestamp is in 2027, after the source reporting period | Timeliness | Reject or hold future-dated order timestamps relative to the approved business date | Critical |
| orders.csv | ORD001725 | order_ts | Order timestamp is in 2027, after the source reporting period | Timeliness | Reject or hold future-dated order timestamps relative to the approved business date | Critical |
| customers.csv | C00400 | signup_date | Signup date is in 2027, after the source reporting period | Timeliness | Review future-dated signup dates relative to the approved business date | Warning |

## Three questions for the client

1. Should a negative or zero quantity, an out-of-range delivery time, or an order amount mismatch be quarantined as a critical error, or are any of these valid operational cases (for example, returns)?
2. When an increment record changes a previously delivered order to `RETURNED`, should September revenue be restated to the current status, or should the original September result be retained with a separate adjustment/history view?
3. Which payment statuses and matching policy define reconciliation (for example, do pending/failed payments count, and how should partial payments, duplicate gateway events, or COD orders be handled)?

## Notes and limitations

- Assignment wording says deliveries normally take 8 to 35 minutes; unusual values are flagged for review rather than treated as a hard client-approved rule. The separate Part D example suggests a 120-minute upper check; confirm that threshold before using it for rejection.
- Test-entry detection, future-date cutoffs and freshness reference time require a client-approved marker/policy. Do not silently discard rows based on guessed identifiers.
- This issue log contains source row keys and field-level findings, not customer names, email addresses, or phone numbers.

