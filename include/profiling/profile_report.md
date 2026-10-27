# NammaMart data profile (Part 0)
source: /Users/cool/Documents/FDE/Material/Module2/repo/include/data/nammamart

### orders.csv
rows: 2412  columns: 13
nulls: order_ts=1, customer_id=18, delivery_minutes=164
duplicate order_id: 12 keys over 24 rows
exact duplicate rows: 24
order_ts range: 2026-09-01 06:30:15 .. 2027-01-01 09:00:00  unparsed: 5
quantity: -3 .. 5   unit_price: 10.0 .. 883.0
discount_pct: 0 .. 250   order_amount: -1593.0 .. 9576.0
delivery_minutes: 8.0 .. 1440.0
order_status: DELIVERED=2182, CANCELLED=156, RETURNED=69, Pending??=3, delivered=2
payment_mode: UPI=1329, Card=493, Cash on Delivery=326, Wallet=254, upi=4, Cashh=2, UPI =2, CARD=1, cod=1
channel: App=2044, Web=368

### payments.csv
rows: 2228  columns: 7
nulls: gateway_ref=301
duplicate payment_id: 4 keys over 8 rows
exact duplicate rows: 8
amount_paid: -1593.0 .. 9576.0
payment_status: SUCCESS=2116, REFUNDED=68, FAILED=44
payment_mode: UPI=1238, Card=445, Cash on Delivery=301, Wallet=234, upi=4, UPI =2, Cashh=2, cod=1, CARD=1

### customers.csv
rows: 405  columns: 10
nulls: city=6
duplicate customer_id: 5 keys over 10 rows
loyalty_tier: Bronze=185, Silver=133, Gold=54, Platinum=25, BRONZE=3, bronze=3, SILVER=2
is_active: Y=352, N=53
cities: Bengaluru=399, nan=6

### products.csv
rows: 152  columns: 10
nulls: category=3
duplicate product_id: 2 keys over 4 rows
mrp: -203.0 .. 883.0   cost_price: 6.79 .. 631.79
categories: Dairy & Bakery=26, Household=26, Fruits & Vegetables=25, Snacks & Beverages=25, Personal Care=25, Staples=22, nan=3

### stores.csv
rows: 15  columns: 8
nulls: zone=1
duplicate store_id: none
zones: East=4, West=4, North=3, South=3, nan=1
capacity_orders_per_day: 0 .. 600

### orders_2026-10-01_increment.csv
rows: 160  columns: 13
nulls: none
duplicate order_id: none
order_ts range: 2026-09-01 09:38:13 .. 2026-10-01 22:33:00  unparsed: 0
quantity: 1 .. 4   unit_price: 10.0 .. 883.0
discount_pct: 0 .. 20   order_amount: 11.0 .. 2052.0
delivery_minutes: 8 .. 34
order_status: DELIVERED=150, RETURNED=10
payment_mode: UPI=46, Card=44, Cash on Delivery=40, Wallet=30
channel: App=160

### Referential integrity
orders.customer_id -> customers.csv: 0 orphans
orders.product_id -> products.csv: 9 orphans
orders.store_id -> stores.csv: 5 orphans
payments.order_id -> orders.csv: 8 orphans
SUCCESS payments not matching order_amount: 14
DELIVERED orders with no SUCCESS payment: 72

### Increment file
rows: 160  new orders: 150  corrections to existing orders: 10
status changes: DELIVERED -> RETURNED: 10

### Three questions for the client before building
1. Negative quantity: is that a return booked against the original order, or a data-entry error? It changes whether we quarantine the row or net it off revenue.
2. When a SUCCESS payment is larger than the order amount, which number is the truth for the CFO's revenue: the order or the gateway?
3. An order delivered in September and returned on 1 October: should September revenue be restated, or should the return land in October? Finance and ops usually disagree.

### Data Issues Log
34 issues across 6 files
  Accuracy      Critical 46 rows
  Completeness  Critical 26 rows
  Completeness  Warning  476 rows
  Consistency   Critical 36 rows
  Consistency   Warning  484 rows
  Timeliness    Critical 20 rows
  Uniqueness    Critical 46 rows
  Validity      Critical 16 rows
  Validity      Warning  19 rows

| File | Column | Problem | Dimension | Severity | Rows |
| --- | --- | --- | --- | --- | --- |
| orders.csv | order_ts | 1 rows with no order_ts | Completeness | Warning | 1 |
| orders.csv | customer_id | 18 rows with no customer_id | Completeness | Critical | 18 |
| orders.csv | delivery_minutes | 164 rows with no delivery_minutes | Completeness | Warning | 164 |
| orders.csv | order_id | 12 duplicated keys | Uniqueness | Critical | 24 |
| orders.csv | order_status | 5 rows outside ['CANCELLED', 'DELIVERED', 'RETURNED']: Pending??, delivered | Validity | Critical | 5 |
| orders.csv | payment_mode | 7 rows with casing or spacing variants: ['upi', 'CARD', 'UPI '] | Consistency | Warning | 7 |
| orders.csv | payment_mode | 3 rows with unmapped mode: ['Cashh', 'cod'] | Validity | Critical | 3 |
| orders.csv | discount_pct | 3 rows with discount outside 0-100 (max 250%) | Validity | Critical | 3 |
| orders.csv | quantity | 10 rows with quantity <= 0 (min -3) | Accuracy | Critical | 10 |
| orders.csv | order_amount | 18 rows where amount != qty*price*(1-disc) (max gap 8618.40) | Accuracy | Critical | 18 |
| orders.csv | delivery_minutes | 6 rows above 120 minutes (max 1440.0) | Accuracy | Critical | 6 |
| orders.csv | delivery_minutes | 8 DELIVERED orders with no delivery time | Completeness | Critical | 8 |
| orders.csv | order_ts | 7 orders dated after the export window (max 2027-01-01 09:00:00) | Timeliness | Critical | 7 |
| orders.csv | order_ts | 5 timestamps that do not parse: N/A, 2026-13-05 11:00:00, 31/09/2026 10:00 | Validity | Critical | 5 |
| payments.csv | gateway_ref | 301 rows with no gateway_ref | Completeness | Warning | 301 |
| payments.csv | payment_id | 4 duplicated keys | Uniqueness | Critical | 8 |
| payments.csv | amount_paid | 6 payments with a negative amount | Accuracy | Critical | 6 |
| customers.csv | city | 6 rows with no city | Completeness | Warning | 6 |
| customers.csv | customer_id | 5 duplicated keys | Uniqueness | Critical | 10 |
| customers.csv | loyalty_tier | mixed casing across 7 variants: ['BRONZE', 'Bronze', 'Gold', 'Platinum', 'SILVER', 'Silver'] | Consistency | Warning | 405 |
| customers.csv | email | 12 malformed email addresses: N/A, arjun.rao307example.com, prakash.pillai63@example +6 more | Validity | Warning | 12 |
| customers.csv | phone | 7 phone numbers that are not 10 digits: 996292312, 90917798, 993802612 +4 more | Validity | Warning | 7 |
| customers.csv | signup_date | 3 customers signed up in the future (max 2027-01-05 00:00:00) | Timeliness | Critical | 3 |
| products.csv | category | 3 rows with no category | Completeness | Warning | 3 |
| products.csv | product_id | 2 duplicated keys | Uniqueness | Critical | 4 |
| products.csv | cost_price | 5 products where cost_price > mrp | Accuracy | Critical | 5 |
| products.csv | mrp | 1 products with mrp <= 0 | Accuracy | Critical | 1 |
| stores.csv | zone | 1 rows with no zone | Completeness | Warning | 1 |
| orders.csv | product_id | 9 orders pointing at a product_id that does not exist: P9455, P9212, P9816 +6 more | Consistency | Critical | 9 |
| orders.csv | store_id | 5 orders pointing at a store_id that does not exist: S99, S17, S00 | Consistency | Critical | 5 |
| payments.csv | order_id | 8 payments with no matching order | Consistency | Critical | 8 |
| payments.csv | amount_paid | 14 SUCCESS payments differ from order_amount (net -785.00) | Consistency | Critical | 14 |
| orders.csv | order_id | 72 delivered orders with no successful payment | Consistency | Warning | 72 |
| orders_2026-10-01_increment.csv | order_status | 10 late status corrections to September orders | Timeliness | Critical | 10 |
