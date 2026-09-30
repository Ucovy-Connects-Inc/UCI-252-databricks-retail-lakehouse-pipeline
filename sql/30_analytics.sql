-- KPI summary
SELECT
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT customer_id) AS total_customers,
  ROUND(SUM(net_sales), 2) AS total_revenue,
  ROUND(AVG(net_sales), 2) AS avg_line_revenue
FROM retail_lakehouse.gold.fact_sales;

-- Daily revenue trend
SELECT
  order_date,
  total_orders,
  total_units,
  ROUND(total_revenue, 2) AS total_revenue
FROM retail_lakehouse.gold.daily_sales_summary
ORDER BY order_date;

-- Product performance
SELECT
  product_id,
  product_name,
  category,
  units_sold,
  ROUND(total_revenue, 2) AS total_revenue
FROM retail_lakehouse.gold.product_sales_summary
ORDER BY total_revenue DESC;

-- Revenue by state
SELECT
  c.state,
  COUNT(DISTINCT f.order_id) AS orders,
  ROUND(SUM(f.net_sales), 2) AS revenue
FROM retail_lakehouse.gold.fact_sales f
JOIN retail_lakehouse.gold.dim_customer c
  ON f.customer_id = c.customer_id
 AND c.__END_AT IS NULL
GROUP BY c.state
ORDER BY revenue DESC;

-- Order status
SELECT
  order_status,
  COUNT(DISTINCT order_id) AS orders,
  ROUND(SUM(net_sales), 2) AS revenue
FROM retail_lakehouse.gold.fact_sales
GROUP BY order_status
ORDER BY orders DESC;
