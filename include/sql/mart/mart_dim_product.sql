-- Product dimension. A product that failed a critical check (cost above mrp, mrp
-- zero) is left out, so orders pointing at it keep their revenue but show no margin.

CREATE OR REPLACE TABLE mart_dim_product AS
SELECT product_id, product_name, category, sub_category, brand,
       mrp, cost_price, unit, is_perishable, launch_date, dq_warnings
FROM stg_products
WHERE dq_status = 'PASSED';
