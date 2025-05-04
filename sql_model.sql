-- Create order_fact table from staging
CREATE OR REPLACE TABLE order_fact AS
SELECT 
    order_id,
    customer_email,
    order_total,
    order_date,
    source_system
FROM 
    staging_orders;

-- Customer order aggregation
SELECT 
    customer_email,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(order_total) AS total_spent,
    MIN(order_date) AS first_order_date
FROM 
    order_fact
GROUP BY 
    customer_email
ORDER BY 
    total_spent DESC;