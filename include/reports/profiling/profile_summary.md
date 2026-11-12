# NammaMart — Data Profiling Report (Part 0)

Data folder: `include/data/nammamart` · As-of date: **2026-10-01** · Generated: 2026-10-21 08:26

Total rows profiled: **5,372** · Issue flags: **747** across **52** issue types


## Overview

| file | rows | columns | issue_types | critical_flags | warning_flags | rows_with_a_critical_issue |
|---|---|---|---|---|---|---|
| orders.csv | 2412 | 13 | 23 | 185 | 85 | 173 |
| payments.csv | 2228 | 7 | 11 | 53 | 350 | 47 |
| customers.csv | 405 | 10 | 11 | 13 | 45 | 13 |
| products.csv | 152 | 10 | 4 | 13 | 0 | 12 |
| stores.csv | 15 | 8 | 2 | 2 | 0 | 2 |
| orders_2026-10-01_increment.csv | 160 | 13 | 1 | 0 | 1 | 0 |


## Issue flags by quality dimension

| file | Accuracy | Completeness | Consistency | Timeliness | Uniqueness | Validity |
|---|---|---|---|---|---|---|
| customers.csv | 0 | 14 | 13 | 3 | 10 | 18 |
| orders.csv | 103 | 29 | 97 | 7 | 12 | 22 |
| orders_2026-10-01_increment.csv | 0 | 0 | 1 | 0 | 0 | 0 |
| payments.csv | 67 | 285 | 22 | 7 | 12 | 10 |
| products.csv | 6 | 3 | 0 | 0 | 4 | 0 |
| stores.csv | 1 | 1 | 0 | 0 | 0 | 0 |


## Referential integrity

| relationship | child_rows | blank_keys | orphan_rows | sample_orphans |
|---|---|---|---|---|
| orders.customer_id -> customers | 2412 | 18 | 0 |  |
| orders.product_id -> products | 2412 | 0 | 9 | P9455, P9212, P9816, P9893, P9989 |
| orders.store_id -> stores | 2412 | 0 | 5 | S99, S17, S00 |
| payments.order_id -> orders | 2228 | 0 | 8 | ORD970544, ORD968496, ORD988106, ORD941071, ORD981600 |
| increment.customer_id -> customers | 160 | 0 | 0 |  |
| increment.product_id -> products | 160 | 0 | 0 |  |
| increment.store_id -> stores | 160 | 0 | 0 |  |


## Payment status values

| payment_status | rows |
|---|---|
| SUCCESS | 2116 |
| REFUNDED | 68 |
| FAILED | 44 |


## Increment file vs orders.csv

- Rows: **160** (brief says 160)
- New order_ids: **150** (brief says 150)
- Rows for existing orders (corrections): **10** (brief says 10)
- Corrections that change status: **10**
- Corrections that change other fields too: **0**


Status transitions:

| transition | rows |
|---|---|
| DELIVERED -> RETURNED | 10 |


## Issues summary (paste into SUBMISSION.md)

| file | column | problem | dimension | severity | rows_affected | sample_keys | proposed_rule |
|---|---|---|---|---|---|---|---|
| customers.csv | customer_id | same customer_id appears with different values (conflicting duplicate) | Uniqueness | Critical | 10 | C00103, C00252, C00036, C00226, C00313 | customer_id must be unique; ask client which version is correct |
| customers.csv | signup_date | signup_date is in the future | Timeliness | Critical | 3 | C00400, C00398, C00038 | signup_date must be before 2026-10-02 |
| customers.csv | email | email format invalid | Validity | Warning | 8 | C00307, C00063, C00363, C00310, C00112 | email must look like name@domain.tld |
| customers.csv | loyalty_tier | inconsistent spelling/casing of a category value | Consistency | Warning | 8 | C00199, C00171, C00133, C00032, C00031 | standardise loyalty_tier to one spelling |
| customers.csv | phone | phone is not a valid 10-digit Indian mobile | Validity | Warning | 7 | C00151, C00240, C00095, C00357, C00130 | 10 digits starting 6-9 after removing +91/0 |
| customers.csv | city | city missing | Completeness | Warning | 6 | C00355, C00394, C00233, C00142, C00328 | city should be filled |
| customers.csv | email | email missing | Completeness | Warning | 4 | C00337, C00045, C00327, C00193 | email should be filled |
| customers.csv | email | null written as a text placeholder | Completeness | Warning | 4 | C00337, C00045, C00327, C00193 | treat 'NULL', 'N/A', '-' as missing |
| customers.csv | pincode | pincode is not 6 digits | Validity | Warning | 3 | C00132, C00083, C00166 | pincode must be exactly 6 digits |
| customers.csv | signup_date | signup_date is after the customer's first order | Consistency | Warning | 3 | C00400, C00398, C00038 | signup_date <= first order_ts |
| customers.csv | pincode | pincode outside Bengaluru (56xxxx) | Consistency | Warning | 2 | C00395, C00342 | Bengaluru pincodes start with 56 |
| orders.csv | order_id | DELIVERED order has no SUCCESS payment | Consistency | Critical | 72 | ORD000398, ORD001113, ORD000737, ORD000518, ORD000747 | every DELIVERED order needs a SUCCESS payment (ask: how is COD recorded?) |
| orders.csv | customer_id | required value missing | Completeness | Critical | 18 | ORD002255, ORD000701, ORD000997, ORD000580, ORD002019 | customer_id must not be null |
| orders.csv | order_amount | order_amount does not match quantity x unit_price x (1 - discount_pct/100) | Accuracy | Critical | 18 | ORD002103, ORD002305, ORD002004, ORD000845, ORD000964 | recompute and compare, tolerance 0.01 |
| orders.csv | order_id | exact duplicate row (whole row repeated) | Uniqueness | Critical | 12 | ORD001462, ORD001441, ORD001692, ORD002129, ORD001239 | keep the first occurrence, quarantine repeats |
| orders.csv | quantity | quantity is zero or negative | Accuracy | Critical | 10 | ORD000469, ORD001296, ORD001459, ORD000867, ORD000462 | quantity > 0 (ask: is a negative a return?) |
| orders.csv | product_id | product_id not found in products.csv | Consistency | Critical | 9 | ORD000986, ORD001058, ORD001685, ORD000070, ORD000789 | product_id must exist in products.csv |
| orders.csv | delivery_minutes | DELIVERED order has no delivery_minutes | Completeness | Critical | 8 | ORD001554, ORD001800, ORD000731, ORD001277, ORD000326 | DELIVERED orders must have delivery_minutes |
| orders.csv | order_ts | order_ts is in the future | Timeliness | Critical | 7 | ORD002119, ORD001693, ORD001089, ORD001121, ORD001765 | order_ts must be before 2026-10-02 |
| orders.csv | delivery_minutes | delivery_minutes >= 120 (implausible: hours or seconds?) | Accuracy | Critical | 6 | ORD001037, ORD002101, ORD000455, ORD001104, ORD000020 | delivery_minutes < 120 |
| orders.csv | order_amount | order_amount is negative | Accuracy | Critical | 6 | ORD001296, ORD001459, ORD000867, ORD000462, ORD001124 | order_amount >= 0 |
| orders.csv | store_id | store_id not found in stores.csv | Consistency | Critical | 5 | ORD001799, ORD001046, ORD001492, ORD001275, ORD002126 | store_id must exist in stores.csv |
| orders.csv | order_ts | order_ts cannot be parsed as a date/time | Validity | Critical | 4 | ORD001610, ORD000534, ORD002012, ORD000946 | order_ts must be a valid ISO-8601 date/time |
| orders.csv | discount_pct | discount_pct outside 0-100 | Validity | Critical | 3 | ORD002103, ORD001468, ORD001237 | 0 <= discount_pct <= 100 |
| orders.csv | order_status | order_status not in allowed list | Validity | Critical | 3 | ORD000500, ORD001970, ORD001637 | allowed: DELIVERED, CANCELLED, RETURNED |
| orders.csv | order_ts | required value missing | Completeness | Critical | 2 | ORD000403, ORD000362 | order_ts must not be null |
| orders.csv | payment_mode | payment_mode not in allowed list | Validity | Critical | 2 | ORD001812, ORD001414 | allowed: UPI, Card, Cash on Delivery, Wallet |
| orders.csv | order_amount | statistical outlier (beyond 3x IQR) | Accuracy | Warning | 61 | ORD001249, ORD001923, ORD000918, ORD002381, ORD000733 | review; typical range about -1,273.25 to 2,176.00 |
| orders.csv | unit_price | unit_price is above the product's MRP | Consistency | Warning | 11 | ORD001295, ORD000331, ORD000957, ORD000023, ORD000352 | unit_price <= products.mrp |
| orders.csv | payment_mode | non-standard payment_mode spelling | Validity | Warning | 6 | ORD000159, ORD001364, ORD000433, ORD001640, ORD000934 | map variants to the canonical value |
| orders.csv | order_status | order_status needs upper-casing | Validity | Warning | 2 | ORD001066, ORD000153 | standardise to upper case |
| orders.csv | payment_mode | leading/trailing whitespace | Validity | Warning | 2 | ORD002356, ORD001874 | trim all text values |
| orders.csv | quantity | statistical outlier (beyond 3x IQR) | Accuracy | Warning | 2 | ORD001459, ORD000462 | review; typical range about -2.00 to 5.00 |
| orders.csv | order_ts | null written as a text placeholder | Completeness | Warning | 1 | ORD000403 | treat 'NULL', 'N/A', '-' as missing |
| orders_2026-10-01_increment.csv | unit_price | unit_price is above the product's MRP | Consistency | Warning | 1 | ORD002465 | unit_price <= products.mrp |
| payments.csv | amount_paid | SUCCESS payment amount does not equal order_amount | Consistency | Critical | 14 | PAY0001568, PAY0000354, PAY0000779, PAY0000323, PAY0001167 | SUCCESS amount_paid = orders.order_amount |
| payments.csv | amount_paid | amount_paid zero or negative | Accuracy | Critical | 10 | PAY0001568, PAY0002175, PAY0000116, PAY0000751, PAY0000944 | amount_paid > 0 |
| payments.csv | order_id | more than one SUCCESS payment for the same order (double charge?) | Uniqueness | Critical | 8 | PAY0001651, PAY0000755, PAY0000509, PAY0002000 | one SUCCESS payment per order |
| payments.csv | order_id | payment for an order_id not in orders.csv | Consistency | Critical | 8 | PAY0002219, PAY0002220, PAY0002223, PAY0002221, PAY0002224 | payments.order_id must exist in orders |
| payments.csv | payment_ts | payment_ts is in the future | Timeliness | Critical | 7 | PAY0000396, PAY0000127, PAY0001908, PAY0001037, PAY0000337 | payment_ts must be before 2026-10-02 |
| payments.csv | payment_id | exact duplicate row (whole row repeated) | Uniqueness | Critical | 4 | PAY0000509, PAY0001651, PAY0002000, PAY0000755 | keep the first occurrence, quarantine repeats |
| payments.csv | payment_mode | payment_mode not in allowed list | Validity | Critical | 2 | PAY0000522, PAY0000044 | allowed: UPI, Card, Cash on Delivery, Wallet |
| payments.csv | gateway_ref | SUCCESS payment has no gateway_ref | Completeness | Warning | 285 | PAY0000133, PAY0001514, PAY0001852, PAY0001437, PAY0000009 | SUCCESS payments need a gateway_ref |
| payments.csv | amount_paid | statistical outlier (beyond 3x IQR) | Accuracy | Warning | 57 | PAY0001308, PAY0000858, PAY0001190, PAY0000665, PAY0000440 | review; typical range about -1,248.60 to 2,135.20 |
| payments.csv | payment_mode | non-standard payment_mode spelling | Validity | Warning | 6 | PAY0002188, PAY0001850, PAY0001954, PAY0000817, PAY0001359 | map variants to the canonical value |
| payments.csv | payment_mode | leading/trailing whitespace | Validity | Warning | 2 | PAY0002113, PAY0001862 | trim all text values |
| products.csv | cost_price | cost_price is higher than mrp | Accuracy | Critical | 5 | P0019, P0073, P0074, P0109, P0117 | cost_price <= mrp |
| products.csv | product_id | same product_id appears with different values (conflicting duplicate) | Uniqueness | Critical | 4 | P0030, P0140 | product_id must be unique; ask client which version is correct |
| products.csv | category | category missing | Completeness | Critical | 3 | P0051, P0060, P0068 | category must not be null |
| products.csv | mrp | mrp zero or negative | Accuracy | Critical | 1 | P0109 | mrp > 0 |
| stores.csv | capacity_orders_per_day | capacity zero or negative | Accuracy | Critical | 1 | S12 | capacity_orders_per_day > 0 |
| stores.csv | zone | zone missing | Completeness | Critical | 1 | S07 | zone must not be null |


---

## orders.csv

Rows: **2,412** · Columns: **13** · Key: `order_id`

Duplicate `order_id` values: **12** · Exact duplicate rows: **12** · Blank keys: **0**


### Columns

| column | nulls | null_pct | distinct | whitespace | sample |
|---|---|---|---|---|---|
| order_id | 0 | 0.0% | 2400 | 0 | ORD001913, ORD001791, ORD002203 |
| order_ts | 2 | 0.1% | 2391 | 0 | 2026-09-24 22:56:41, 2026-09-23 17:54:42, 2026-09-28 15:53:19 |
| customer_id | 18 | 0.7% | 400 | 0 | C00005, C00369, C00010 |
| store_id | 0 | 0.0% | 18 | 0 | S10, S05, S08 |
| product_id | 0 | 0.0% | 159 | 0 | P0023, P0136, P0135 |
| quantity | 0 | 0.0% | 9 | 0 | 2, 1, 3 |
| unit_price | 0 | 0.0% | 133 | 0 | 222.0, 360.0, 248.0 |
| discount_pct | 0 | 0.0% | 8 | 0 | 5, 0, 20 |
| order_amount | 0 | 0.0% | 1155 | 0 | 421.8, 360.0, 248.0 |
| payment_mode | 0 | 0.0% | 8 | 2 | UPI, Card, Cash on Delivery |
| order_status | 0 | 0.0% | 5 | 0 | DELIVERED, RETURNED, CANCELLED |
| channel | 0 | 0.0% | 2 | 0 | App, Web |
| delivery_minutes | 164 | 6.8% | 31 | 0 | 35, 31, 26 |


### Numeric columns

| column | non_numeric | min | median | max | mean | negatives | zeros |
|---|---|---|---|---|---|---|---|
| quantity | 0 | -3.00 | 1.00 | 5.00 | 1.90 | 6 | 4 |
| unit_price | 0 | 10.00 | 279.00 | 883.00 | 308.33 | 0 | 0 |
| discount_pct | 0 | 0.00 | 5.00 | 250.00 | 7.26 | 0 | 1020 |
| order_amount | 0 | -1,593.00 | 374.35 | 9,576.00 | 559.95 | 6 | 4 |
| delivery_minutes | 0 | 8.00 | 21.00 | 1,440.00 | 23.50 | 0 | 0 |


### Date columns

| column | unparseable | non_iso | min | max |
|---|---|---|---|---|
| order_ts | 4 | 0 | 2026-09-01 06:30:15 | 2027-01-01 09:00:00 |


### Distinct values: `order_status`

| value | rows |
|---|---|
| 'DELIVERED' | 2182 |
| 'CANCELLED' | 156 |
| 'RETURNED' | 69 |
| 'Pending??' | 3 |
| 'delivered' | 2 |


### Distinct values: `payment_mode`

| value | rows |
|---|---|
| 'UPI' | 1329 |
| 'Card' | 493 |
| 'Cash on Delivery' | 326 |
| 'Wallet' | 254 |
| 'upi' | 4 |
| 'Cashh' | 2 |
| 'UPI ' | 2 |
| 'CARD' | 1 |
| 'cod' | 1 |


### Distinct values: `channel`

| value | rows |
|---|---|
| 'App' | 2044 |
| 'Web' | 368 |

## payments.csv

Rows: **2,228** · Columns: **7** · Key: `payment_id`

Duplicate `payment_id` values: **4** · Exact duplicate rows: **4** · Blank keys: **0**


### Columns

| column | nulls | null_pct | distinct | whitespace | sample |
|---|---|---|---|---|---|
| payment_id | 0 | 0.0% | 2224 | 0 | PAY0001316, PAY0000574, PAY0000133 |
| order_id | 0 | 0.0% | 2224 | 0 | ORD001367, ORD001855, ORD000027 |
| payment_ts | 0 | 0.0% | 2210 | 0 | 2026-09-18 14:45:24, 2026-09-24 10:16:23, 2026-09-01 10:51:00 |
| payment_mode | 0 | 0.0% | 8 | 2 | UPI, Wallet, Cash on Delivery |
| amount_paid | 0 | 0.0% | 1127 | 0 | 288.8, 484.0, 180.5 |
| payment_status | 0 | 0.0% | 3 | 0 | SUCCESS, FAILED, REFUNDED |
| gateway_ref | 301 | 13.5% | 1923 | 0 | GW8048515503, GW6532349731, GW3327904298 |


### Numeric columns

| column | non_numeric | min | median | max | mean | negatives | zeros |
|---|---|---|---|---|---|---|---|
| amount_paid | 0 | -1,593.00 | 374.15 | 9,576.00 | 555.62 | 6 | 4 |


### Date columns

| column | unparseable | non_iso | min | max |
|---|---|---|---|---|
| payment_ts | 0 | 0 | 2026-09-01 06:30:24 | 2027-01-01 09:01:46 |


### Distinct values: `payment_mode`

| value | rows |
|---|---|
| 'UPI' | 1238 |
| 'Card' | 445 |
| 'Cash on Delivery' | 301 |
| 'Wallet' | 234 |
| 'upi' | 4 |
| 'UPI ' | 2 |
| 'Cashh' | 2 |
| 'cod' | 1 |
| 'CARD' | 1 |


### Distinct values: `payment_status`

| value | rows |
|---|---|
| 'SUCCESS' | 2116 |
| 'REFUNDED' | 68 |
| 'FAILED' | 44 |

## customers.csv

Rows: **405** · Columns: **10** · Key: `customer_id`

Duplicate `customer_id` values: **5** · Exact duplicate rows: **0** · Blank keys: **0**


### Columns

| column | nulls | null_pct | distinct | whitespace | sample |
|---|---|---|---|---|---|
| customer_id | 0 | 0.0% | 400 | 0 | C00199, C00182, C00355 |
| full_name | 0 | 0.0% | 280 | 0 | (personal data hidden) |
| email | 4 | 1.0% | 396 | 0 | (personal data hidden) |
| phone | 0 | 0.0% | 405 | 0 | (personal data hidden) |
| city | 6 | 1.5% | 1 | 0 | Bengaluru |
| area | 0 | 0.0% | 15 | 0 | Whitefield, Hebbal, RT Nagar |
| pincode | 0 | 0.0% | 17 | 0 | 560066, 560024, 560032 |
| signup_date | 0 | 0.0% | 315 | 0 | 2024-08-27, 2026-02-19, 2026-07-28 |
| loyalty_tier | 0 | 0.0% | 7 | 0 | SILVER, Silver, Platinum |
| is_active | 0 | 0.0% | 2 | 0 | Y, N |


### Date columns

| column | unparseable | non_iso | min | max |
|---|---|---|---|---|
| signup_date | 0 | 0 | 2024-06-01 00:00:00 | 2027-01-05 00:00:00 |


### Distinct values: `city`

| value | rows |
|---|---|
| 'Bengaluru' | 399 |
| '' | 6 |


### Distinct values: `area`

| value | rows |
|---|---|
| 'Indiranagar' | 43 |
| 'RT Nagar' | 37 |
| 'BTM Layout' | 34 |
| 'Yelahanka' | 32 |
| 'Malleshwaram' | 30 |
| 'JP Nagar' | 27 |
| 'Banashankari' | 27 |
| 'Marathahalli' | 25 |
| 'Basaveshwaranagar' | 24 |
| 'HSR Layout' | 24 |
| 'Whitefield' | 23 |
| 'Rajajinagar' | 23 |
| 'Hebbal' | 22 |
| 'Jayanagar' | 19 |
| 'Vijayanagar' | 15 |


### Distinct values: `loyalty_tier`

| value | rows |
|---|---|
| 'Bronze' | 185 |
| 'Silver' | 133 |
| 'Gold' | 54 |
| 'Platinum' | 25 |
| 'BRONZE' | 3 |
| 'bronze' | 3 |
| 'SILVER' | 2 |


### Distinct values: `is_active`

| value | rows |
|---|---|
| 'Y' | 352 |
| 'N' | 53 |

## products.csv

Rows: **152** · Columns: **10** · Key: `product_id`

Duplicate `product_id` values: **2** · Exact duplicate rows: **0** · Blank keys: **0**


### Columns

| column | nulls | null_pct | distinct | whitespace | sample |
|---|---|---|---|---|---|
| product_id | 0 | 0.0% | 150 | 0 | P0001, P0002, P0003 |
| product_name | 0 | 0.0% | 152 | 0 | Desi Farms Fresh Vegetabl, Desi Farms Fresh Fruits 2, Desi Farms Fresh Fruits 3 |
| category | 3 | 2.0% | 6 | 0 | Fruits & Vegetables, Dairy & Bakery, Staples |
| sub_category | 0 | 0.0% | 21 | 0 | Fresh Vegetables, Fresh Fruits, Herbs |
| brand | 0 | 0.0% | 23 | 0 | Desi Farms, GreenLeaf, FarmFresh |
| mrp | 0 | 0.0% | 135 | 0 | 107.0, 105.0, 177.0 |
| cost_price | 0 | 0.0% | 150 | 0 | 85.62, 66.16, 131.07 |
| unit | 0 | 0.0% | 3 | 0 | kg, pack, unit |
| is_perishable | 0 | 0.0% | 2 | 0 | True, False |
| launch_date | 0 | 0.0% | 145 | 0 | 2023-06-16, 2025-08-17, 2024-06-30 |


### Numeric columns

| column | non_numeric | min | median | max | mean | negatives | zeros |
|---|---|---|---|---|---|---|---|
| mrp | 0 | -203.00 | 285.50 | 883.00 | 310.51 | 1 | 0 |
| cost_price | 0 | 6.79 | 201.06 | 631.79 | 224.19 | 0 | 0 |


### Date columns

| column | unparseable | non_iso | min | max |
|---|---|---|---|---|
| launch_date | 0 | 0 | 2023-01-03 00:00:00 | 2025-09-20 00:00:00 |


### Distinct values: `category`

| value | rows |
|---|---|
| 'Dairy & Bakery' | 26 |
| 'Household' | 26 |
| 'Fruits & Vegetables' | 25 |
| 'Snacks & Beverages' | 25 |
| 'Personal Care' | 25 |
| 'Staples' | 22 |
| '' | 3 |


### Distinct values: `sub_category`

| value | rows |
|---|---|
| 'Detergent' | 14 |
| 'Shampoo' | 10 |
| 'Fresh Fruits' | 10 |
| 'Soft Drinks' | 8 |
| 'Herbs' | 8 |
| 'Kitchen Supplies' | 8 |
| 'Milk' | 8 |
| 'Soap' | 8 |
| 'Rice' | 8 |
| 'Tea & Coffee' | 8 |
| 'Fresh Vegetables' | 7 |
| 'Oral Care' | 7 |
| 'Atta' | 7 |
| 'Biscuits' | 7 |
| 'Curd & Paneer' | 7 |
| 'Bread' | 6 |
| 'Oil' | 5 |
| 'Dal' | 5 |
| 'Cheese' | 5 |
| 'Cleaners' | 4 |
| 'Chips' | 2 |


### Distinct values: `brand`

| value | rows |
|---|---|
| 'Kaveri Dairy' | 14 |
| 'DalBazaar' | 12 |
| 'Desi Farms' | 10 |
| 'CleanPro' | 10 |
| 'TeaTime' | 9 |
| 'PureGlow' | 9 |
| 'FarmFresh' | 8 |
| 'ShineAll' | 8 |
| 'GoldenGrain' | 8 |
| 'Chandana' | 7 |
| 'GreenLeaf' | 7 |
| 'SnackSutra' | 6 |
| 'BrewHouse' | 6 |
| 'HerbVeda' | 5 |
| 'ScrubMate' | 5 |
| 'OvenCraft' | 5 |
| 'CrunchCo' | 4 |
| 'SmileBright' | 4 |
| 'CowBell' | 4 |
| 'SunPress' | 3 |
| 'MilkWay' | 3 |
| 'SparkWash' | 3 |
| 'Annapurna Mills' | 2 |


### Distinct values: `unit`

| value | rows |
|---|---|
| 'pack' | 76 |
| 'unit' | 51 |
| 'kg' | 25 |


### Distinct values: `is_perishable`

| value | rows |
|---|---|
| 'False' | 101 |
| 'True' | 51 |

## stores.csv

Rows: **15** · Columns: **8** · Key: `store_id`

Duplicate `store_id` values: **0** · Exact duplicate rows: **0** · Blank keys: **0**


### Columns

| column | nulls | null_pct | distinct | whitespace | sample |
|---|---|---|---|---|---|
| store_id | 0 | 0.0% | 15 | 0 | S01, S02, S03 |
| store_name | 0 | 0.0% | 15 | 0 | NammaMart Hebbal, NammaMart Yelahanka, NammaMart RT Nagar |
| zone | 1 | 6.7% | 4 | 0 | North, South, East |
| area | 0 | 0.0% | 15 | 0 | Hebbal, Yelahanka, RT Nagar |
| pincode | 0 | 0.0% | 15 | 0 | 560024, 560064, 560032 |
| open_date | 0 | 0.0% | 15 | 0 | 2024-04-24, 2024-08-16, 2025-03-08 |
| manager_name | 0 | 0.0% | 15 | 0 | (personal data hidden) |
| capacity_orders_per_day | 0 | 0.0% | 5 | 0 | 400, 300, 600 |


### Numeric columns

| column | non_numeric | min | median | max | mean | negatives | zeros |
|---|---|---|---|---|---|---|---|
| capacity_orders_per_day | 0 | 0.00 | 300.00 | 600.00 | 360.00 | 0 | 1 |


### Date columns

| column | unparseable | non_iso | min | max |
|---|---|---|---|---|
| open_date | 0 | 0 | 2024-02-14 00:00:00 | 2025-07-28 00:00:00 |


### Distinct values: `zone`

| value | rows |
|---|---|
| 'East' | 4 |
| 'West' | 4 |
| 'North' | 3 |
| 'South' | 3 |
| '' | 1 |


### Distinct values: `area`

| value | rows |
|---|---|
| 'Hebbal' | 1 |
| 'Yelahanka' | 1 |
| 'RT Nagar' | 1 |
| 'Jayanagar' | 1 |
| 'JP Nagar' | 1 |
| 'Banashankari' | 1 |
| 'BTM Layout' | 1 |
| 'Whitefield' | 1 |
| 'Indiranagar' | 1 |
| 'Marathahalli' | 1 |
| 'HSR Layout' | 1 |
| 'Rajajinagar' | 1 |
| 'Malleshwaram' | 1 |
| 'Vijayanagar' | 1 |
| 'Basaveshwaranagar' | 1 |

## orders_2026-10-01_increment.csv

Rows: **160** · Columns: **13** · Key: `order_id`

Duplicate `order_id` values: **0** · Exact duplicate rows: **0** · Blank keys: **0**


### Columns

| column | nulls | null_pct | distinct | whitespace | sample |
|---|---|---|---|---|---|
| order_id | 0 | 0.0% | 160 | 0 | ORD000017, ORD000271, ORD000332 |
| order_ts | 0 | 0.0% | 152 | 0 | 2026-09-01 09:38:13, 2026-09-04 20:42:53, 2026-09-05 16:48:59 |
| customer_id | 0 | 0.0% | 143 | 0 | C00298, C00003, C00346 |
| store_id | 0 | 0.0% | 15 | 0 | S12, S06, S09 |
| product_id | 0 | 0.0% | 98 | 0 | P0081, P0113, P0035 |
| quantity | 0 | 0.0% | 4 | 0 | 2, 1, 4 |
| unit_price | 0 | 0.0% | 90 | 0 | 10.0, 445.0, 67.0 |
| discount_pct | 0 | 0.0% | 5 | 0 | 20, 0, 5 |
| order_amount | 0 | 0.0% | 148 | 0 | 16.0, 445.0, 67.0 |
| payment_mode | 0 | 0.0% | 4 | 0 | UPI, Cash on Delivery, Wallet |
| order_status | 0 | 0.0% | 2 | 0 | RETURNED, DELIVERED |
| channel | 0 | 0.0% | 1 | 0 | App |
| delivery_minutes | 0 | 0.0% | 24 | 0 | 34, 11, 25 |


### Numeric columns

| column | non_numeric | min | median | max | mean | negatives | zeros |
|---|---|---|---|---|---|---|---|
| quantity | 0 | 1.00 | 2.00 | 4.00 | 1.85 | 0 | 0 |
| unit_price | 0 | 10.00 | 254.00 | 883.00 | 305.20 | 0 | 0 |
| discount_pct | 0 | 0.00 | 5.00 | 20.00 | 5.78 | 0 | 45 |
| order_amount | 0 | 11.00 | 395.20 | 2,052.00 | 524.04 | 0 | 0 |
| delivery_minutes | 0 | 8.00 | 19.00 | 34.00 | 18.88 | 0 | 0 |


### Date columns

| column | unparseable | non_iso | min | max |
|---|---|---|---|---|
| order_ts | 0 | 0 | 2026-09-01 09:38:13 | 2026-10-01 22:33:00 |


### Distinct values: `order_status`

| value | rows |
|---|---|
| 'DELIVERED' | 150 |
| 'RETURNED' | 10 |


### Distinct values: `payment_mode`

| value | rows |
|---|---|
| 'UPI' | 46 |
| 'Card' | 44 |
| 'Cash on Delivery' | 40 |
| 'Wallet' | 30 |


### Distinct values: `channel`

| value | rows |
|---|---|
| 'App' | 160 |
