-- include/sql/01_raw_tables.sql
-- Raw layer: Exact copy of each CSV as VARCHAR text + audit columns.
-- Never cleaned, never deleted.

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR,
    order_ts VARCHAR,
    customer_id VARCHAR,
    store_id VARCHAR,
    product_id VARCHAR,
    quantity VARCHAR,
    unit_price VARCHAR,
    discount_pct VARCHAR,
    order_amount VARCHAR,
    payment_mode VARCHAR,
    order_status VARCHAR,
    channel VARCHAR,
    delivery_minutes VARCHAR,
    _loaded_at VARCHAR,
    _source_file VARCHAR,
    _run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id VARCHAR,
    order_id VARCHAR,
    payment_ts VARCHAR,
    payment_mode VARCHAR,
    amount_paid VARCHAR,
    payment_status VARCHAR,
    gateway_ref VARCHAR,
    _loaded_at VARCHAR,
    _source_file VARCHAR,
    _run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id VARCHAR,
    full_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    city VARCHAR,
    area VARCHAR,
    pincode VARCHAR,
    signup_date VARCHAR,
    loyalty_tier VARCHAR,
    is_active VARCHAR,
    _loaded_at VARCHAR,
    _source_file VARCHAR,
    _run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_products (
    product_id VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    sub_category VARCHAR,
    brand VARCHAR,
    mrp VARCHAR,
    cost_price VARCHAR,
    unit VARCHAR,
    is_perishable VARCHAR,
    launch_date VARCHAR,
    _loaded_at VARCHAR,
    _source_file VARCHAR,
    _run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_stores (
    store_id VARCHAR,
    store_name VARCHAR,
    zone VARCHAR,
    area VARCHAR,
    pincode VARCHAR,
    open_date VARCHAR,
    manager_name VARCHAR,
    capacity_orders_per_day VARCHAR,
    _loaded_at VARCHAR,
    _source_file VARCHAR,
    _run_id VARCHAR
);
