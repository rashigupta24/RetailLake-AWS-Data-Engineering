select distinct c.customer_id,c.name,c.city,c.customer_type,c.registration_date,c.customer_age_days,
    SUM(o.amount) OVER (PARTITION BY c.customer_id) AS total_spent,
    COUNT(o.order_id) OVER (PARTITION BY c.customer_id) AS total_orders,
    avg(o.amount) OVER (PARTITION BY c.customer_id) AS avg_order_value,
    FIRST_VALUE(o.order_date) OVER (PARTITION BY c.customer_id ORDER BY o.order_date ASC
    ) AS first_order,
    FIRST_VALUE(o.order_date) OVER (PARTITION BY c.customer_id ORDER BY o.order_date desc) AS last_order
FROM retaillake_silver.orders o
inner JOIN retaillake_silver.customers c
    ON c.customer_id = o.customer_id;
    
    