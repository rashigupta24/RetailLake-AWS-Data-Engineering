CREATE TABLE retaillake_gold.gold_coupon_analytics
WITH (
    format = 'PARQUET',
    external_location = 's3://retaillake-data/gold/coupon_analytics/'
)
AS

SELECT
    c.coupon_id,
    c.coupon_code,

    COUNT(o.order_id) AS total_orders_using_coupon,

    COALESCE(SUM(o.amount), 0) AS total_sales_using_coupon,

    COALESCE(AVG(o.amount), 0) AS avg_order_value_using_coupon,

    MIN(o.order_date) AS first_used_date,
    MAX(o.order_date) AS last_used_date

FROM retaillake_silver.coupons c

LEFT JOIN retaillake_silver.orders o
    ON c.coupon_id = o.coupon_id

GROUP BY
    c.coupon_id,
    c.coupon_code;