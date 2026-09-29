CREATE TABLE "retaillake_gold"."gold_product_analytics" WITH (
  format = 'parquet',
  external_location = 's3://retaillake-data/gold/product_analytics/'
) AS WITH product_sales AS (
  SELECT p.product_id,
    p.product_name,
    p.category,
    COUNT(o.order_id) AS total_product_sold,
    SUM(o.amount) AS total_product_revenue,
    AVG(o.amount) AS avg_order_value
  FROM retaillake_silver.products p
    INNER JOIN retaillake_silver.orders o ON p.product_id = o.product_id
  GROUP BY p.product_id,
    p.product_name,
    p.category
),
product_returns AS (
  SELECT o.product_id,
    COUNT(r.return_id) AS total_product_return
  FROM retaillake_silver.orders o
    INNER JOIN retaillake_silver.returns r ON o.order_id = r.order_id
  GROUP BY o.product_id
)
SELECT ps.product_id,
  ps.product_name,
  ps.category,
  ps.total_product_sold,
  ps.total_product_revenue,
  ps.avg_order_value,
  COALESCE(pr.total_product_return, 0) AS total_product_return,
  ROUND(
    COALESCE(pr.total_product_return, 0) * 100.0 / ps.total_product_sold,
    2
  ) AS return_rate,
  CASE
    WHEN COALESCE(pr.total_product_return, 0) * 100.0 / ps.total_product_sold >= 7 THEN 'High Return'
    WHEN COALESCE(pr.total_product_return, 0) * 100.0 / ps.total_product_sold >= 4 THEN 'Medium Return' ELSE 'Low Return'
  END AS return_category
FROM product_sales ps
  LEFT JOIN product_returns pr ON ps.product_id = pr.product_id;