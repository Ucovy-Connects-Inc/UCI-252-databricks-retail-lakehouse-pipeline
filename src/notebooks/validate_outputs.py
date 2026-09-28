# Databricks notebook source
# MAGIC %md
# MAGIC # UCI-252 - Pipeline Output Validation

# COMMAND ----------

catalog = "retail_lakehouse"

required_tables = [
    f"{catalog}.bronze.customers",
    f"{catalog}.bronze.products",
    f"{catalog}.bronze.orders",
    f"{catalog}.bronze.order_items",
    f"{catalog}.silver.customers",
    f"{catalog}.silver.products",
    f"{catalog}.silver.orders",
    f"{catalog}.silver.order_items",
    f"{catalog}.gold.dim_customer",
    f"{catalog}.gold.dim_product",
    f"{catalog}.gold.dim_date",
    f"{catalog}.gold.fact_sales",
    f"{catalog}.gold.daily_sales_summary",
    f"{catalog}.gold.product_sales_summary",
]

results = []
for table_name in required_tables:
    count = spark.table(table_name).count()
    results.append((table_name, count))

display(spark.createDataFrame(results, ["table_name", "row_count"]))

duplicate_customers = spark.sql(f"""
SELECT customer_id, COUNT(*) AS cnt
FROM {catalog}.silver.customers
GROUP BY customer_id
HAVING COUNT(*) > 1
""")
assert duplicate_customers.count() == 0

invalid_sales = spark.sql(f"""
SELECT *
FROM {catalog}.gold.fact_sales
WHERE quantity <= 0 OR net_sales < 0
""")
assert invalid_sales.count() == 0

multiple_active_customer_versions = spark.sql(f"""
SELECT customer_id, COUNT(*) AS active_versions
FROM {catalog}.gold.dim_customer
WHERE __END_AT IS NULL
GROUP BY customer_id
HAVING COUNT(*) > 1
""")
assert multiple_active_customer_versions.count() == 0

print("UCI-252 validation completed successfully.")
