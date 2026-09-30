SELECT 'bronze.customers' AS object_name, COUNT(*) AS row_count
FROM retail_lakehouse.bronze.customers
UNION ALL
SELECT 'bronze.products', COUNT(*) FROM retail_lakehouse.bronze.products
UNION ALL
SELECT 'bronze.orders', COUNT(*) FROM retail_lakehouse.bronze.orders
UNION ALL
SELECT 'bronze.order_items', COUNT(*) FROM retail_lakehouse.bronze.order_items
UNION ALL
SELECT 'silver.customers', COUNT(*) FROM retail_lakehouse.silver.customers
UNION ALL
SELECT 'silver.products', COUNT(*) FROM retail_lakehouse.silver.products
UNION ALL
SELECT 'silver.orders', COUNT(*) FROM retail_lakehouse.silver.orders
UNION ALL
SELECT 'silver.order_items', COUNT(*) FROM retail_lakehouse.silver.order_items
UNION ALL
SELECT 'gold.dim_customer', COUNT(*) FROM retail_lakehouse.gold.dim_customer
UNION ALL
SELECT 'gold.dim_product', COUNT(*) FROM retail_lakehouse.gold.dim_product
UNION ALL
SELECT 'gold.dim_date', COUNT(*) FROM retail_lakehouse.gold.dim_date
UNION ALL
SELECT 'gold.fact_sales', COUNT(*) FROM retail_lakehouse.gold.fact_sales;

SELECT customer_id, COUNT(*) AS duplicate_count
FROM retail_lakehouse.silver.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT *
FROM retail_lakehouse.gold.fact_sales
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR product_id IS NULL
   OR quantity <= 0
   OR net_sales < 0;

SELECT customer_id, COUNT(*) AS active_versions
FROM retail_lakehouse.gold.dim_customer
WHERE __END_AT IS NULL
GROUP BY customer_id
HAVING COUNT(*) > 1;
