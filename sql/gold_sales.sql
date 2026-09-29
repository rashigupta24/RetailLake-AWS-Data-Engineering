CREATE TABLE retaillake_gold.gold_sales_new
WITH (
    format = 'PARQUET',
    external_location = 's3://retaillake-data/gold/sales_new/'
)
AS
SELECT 
    o.order_id,
    o.order_date,
    o.customer_id,
    o.product_id,
    o.category AS product_category,
    o.coupon_id,
    o.amount,
    o.amount_band,
    c.name AS customer_name,
    c.city AS customer_city,
    c1.coupon_code,
    p.product_name
FROM retaillake_silver.orders o
INNER JOIN retaillake_silver.customers c
    ON o.customer_id = c.customer_id
LEFT JOIN retaillake_silver.coupons c1
    ON o.coupon_id = c1.coupon_id
INNER JOIN retaillake_silver.products p
    ON o.product_id = p.product_id;

