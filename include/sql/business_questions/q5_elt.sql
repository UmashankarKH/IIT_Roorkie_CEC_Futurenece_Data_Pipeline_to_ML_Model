-- Business question 5 (ELT): top 10 customers by spend, with loyalty tier.
SELECT customer_id, customer_name_masked, email_masked, phone_masked,
       loyalty_tier, delivered_orders, total_spend
FROM mart_top_customers
WHERE spend_rank <= 10
ORDER BY spend_rank;
