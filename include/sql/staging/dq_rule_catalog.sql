-- Part D: the rule catalogue for the ELT pipeline.
--
-- The staging models write a human-readable reason onto each row. This table gives
-- every one of those reasons a check_name, a dimension and a severity, so the reasons
-- can be rolled up into elt_dq_results and compared against the ETL pipeline's
-- dq_results table check by check. The check_name values are deliberately the same
-- strings used in include/nammamart/quality.py.
--
-- If a staging model ever emits a reason that is not listed here it shows up in
-- elt_dq_results as dimension = 'UNCATALOGUED', which is the signal that somebody
-- added a rule in SQL and forgot to register it.

CREATE OR REPLACE TABLE dq_rule_catalog (
    dataset     VARCHAR,
    check_name  VARCHAR,
    dimension   VARCHAR,
    severity    VARCHAR,
    reason      VARCHAR
);

INSERT INTO dq_rule_catalog VALUES
-- orders
('orders', 'orders_order_id_present',             'Completeness', 'Critical', 'order_id is missing'),
('orders', 'orders_customer_id_present',          'Completeness', 'Critical', 'customer_id is missing'),
('orders', 'orders_delivered_has_delivery_time',  'Completeness', 'Critical', 'DELIVERED order has no delivery_minutes'),
('orders', 'orders_order_id_unique',              'Uniqueness',   'Critical', 'duplicate order_id, later row kept'),
('orders', 'orders_status_allowed',               'Validity',     'Critical', 'order_status not in [DELIVERED, CANCELLED, RETURNED]'),
('orders', 'orders_timestamp_parses',             'Validity',     'Critical', 'order_ts does not parse'),
('orders', 'orders_discount_in_range',            'Validity',     'Critical', 'discount_pct outside 0-100'),
('orders', 'orders_payment_mode_mappable',        'Validity',     'Critical', 'payment_mode cannot be mapped'),
('orders', 'orders_quantity_positive',            'Accuracy',     'Critical', 'quantity must be greater than zero'),
('orders', 'orders_amount_matches_formula',       'Accuracy',     'Critical', 'order_amount does not match quantity x unit_price x (1 - discount)'),
('orders', 'orders_delivery_minutes_believable',  'Accuracy',     'Critical', 'delivery_minutes above 120'),
('orders', 'orders_not_future_dated',             'Timeliness',   'Critical', 'order_ts is in the future'),
('orders', 'orders_customer_id_exists_in_customers', 'Consistency', 'Critical', 'customer_id does not exist in customers'),
('orders', 'orders_product_id_exists_in_products',   'Consistency', 'Critical', 'product_id does not exist in products'),
('orders', 'orders_store_id_exists_in_stores',       'Consistency', 'Critical', 'store_id does not exist in stores'),
('orders', 'orders_payment_mode_casing',          'Consistency',  'Warning',  'payment_mode needed standardising'),
('orders', 'orders_delivery_time_within_band',    'Accuracy',     'Warning',  'delivery took longer than the normal 8-35 minute band'),
-- payments
('payments', 'payments_payment_id_present',       'Completeness', 'Critical', 'payment_id is missing'),
('payments', 'payments_payment_id_unique',        'Uniqueness',   'Critical', 'duplicate payment_id, later row kept'),
('payments', 'payments_order_id_present',         'Completeness', 'Critical', 'payment has no order_id'),
('payments', 'payments_timestamp_parses',         'Validity',     'Critical', 'payment_ts does not parse'),
('payments', 'payments_amount_not_negative',      'Accuracy',     'Critical', 'amount_paid is negative or unreadable'),
('payments', 'payments_gateway_ref_present',      'Completeness', 'Warning',  'no gateway reference recorded'),
('payments', 'payments_payment_mode_casing',      'Consistency',  'Warning',  'payment_mode needed standardising'),
-- customers
('customers', 'customers_customer_id_present',    'Completeness', 'Critical', 'customer_id is missing'),
('customers', 'customers_customer_id_unique',     'Uniqueness',   'Critical', 'duplicate customer_id, later row kept'),
('customers', 'customers_signup_not_future',      'Timeliness',   'Critical', 'signup_date is in the future'),
('customers', 'customers_email_format',           'Validity',     'Warning',  'email is not a valid address'),
('customers', 'customers_phone_ten_digits',       'Validity',     'Warning',  'phone is not 10 digits'),
('customers', 'customers_pincode_six_digits',     'Validity',     'Warning',  'pincode is not 6 digits'),
('customers', 'customers_loyalty_tier_casing',    'Consistency',  'Warning',  'loyalty_tier casing needed standardising'),
('customers', 'customers_city_present',           'Completeness', 'Warning',  'city is missing'),
-- products
('products', 'products_product_id_present',       'Completeness', 'Critical', 'product_id is missing'),
('products', 'products_product_id_unique',        'Uniqueness',   'Critical', 'duplicate product_id, later row kept'),
('products', 'products_mrp_positive',             'Accuracy',     'Critical', 'mrp must be greater than zero'),
('products', 'products_cost_not_above_mrp',       'Accuracy',     'Critical', 'cost_price is higher than mrp'),
('products', 'products_category_present',         'Completeness', 'Warning',  'category is missing'),
-- stores
('stores', 'stores_store_id_present',             'Completeness', 'Critical', 'store_id is missing'),
('stores', 'stores_store_id_unique',              'Uniqueness',   'Critical', 'duplicate store_id, later row kept'),
('stores', 'stores_capacity_positive',            'Accuracy',     'Warning',  'capacity_orders_per_day is zero or missing'),
('stores', 'stores_zone_present',                 'Completeness', 'Warning',  'zone is missing');
