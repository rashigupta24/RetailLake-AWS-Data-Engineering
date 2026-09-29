CREATE TABLE retaillake_gold.gold_return_analytics
WITH (
    format = 'PARQUET',
    external_location = 's3://retaillake-data/gold/return_analytics/'
)
AS

SELECT
    r.return_id,
    r.order_id,

    o.customer_id,
    o.product_id,
    o.category,

    c.name AS customer_name,
    c.city AS customer_city,

    p.product_name,

    o.order_date,
    r.return_date,

    r.reason AS return_reason,

    o.amount AS order_amount,

    DATE_DIFF(
        'day',
        o.order_date,
        r.return_date
    ) AS days_to_return,

    r.return_age_days,

    r.year,
    r.month

FROM retaillake_silver.returns r

INNER JOIN retaillake_silver.orders o
    ON r.order_id = o.order_id

INNER JOIN retaillake_silver.customers c
    ON o.customer_id = c.customer_id

INNER JOIN retaillake_silver.products p
    ON o.product_id = p.product_id;