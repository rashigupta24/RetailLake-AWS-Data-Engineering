CREATE TABLE retaillake_gold.gold_payment_analytics
WITH (
    format = 'PARQUET',
    external_location = 's3://retaillake-data/gold/payment_analytics/'
)
AS

SELECT
    p.payment_id,
    p.order_id,
    o.customer_id,

    p.payment_date,
    p.payment_method,
    p.payment_category,
    p.payment_status,

    p.amount AS payment_amount,
    o.amount AS order_amount,

    p.amount - o.amount AS payment_difference,

    CASE
        WHEN p.amount = o.amount
            THEN 'MATCHED'
        ELSE 'MISMATCHED'
    END AS payment_match_status,

    c.name AS customer_name,
    c.city AS customer_city,

    o.product_id,
    o.category,

    p.year,
    p.month,
    p.payment_age_days

FROM retaillake_silver.payments p

INNER JOIN retaillake_silver.orders o
    ON p.order_id = o.order_id

INNER JOIN retaillake_silver.customers c
    ON o.customer_id = c.customer_id;