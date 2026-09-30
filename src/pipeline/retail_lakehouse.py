from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)

CATALOG = spark.conf.get("retail.catalog", "retail_lakehouse")
LANDING_VOLUME = spark.conf.get(
    "retail.landing_volume",
    f"/Volumes/{CATALOG}/landing/raw",
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("updated_at", TimestampType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("updated_at", TimestampType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("order_status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("updated_at", TimestampType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", IntegerType(), False),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("updated_at", TimestampType(), True),
])


def autoloader_csv(folder: str, schema: StructType):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(schema)
        .load(f"{LANDING_VOLUME}/{folder}")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ----------------------------
# BRONZE
# ----------------------------

@dp.table(
    name=f"{CATALOG}.bronze.customers",
    comment="Raw customers incrementally ingested with Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_customers():
    return autoloader_csv("customers", CUSTOMERS_SCHEMA)


@dp.table(
    name=f"{CATALOG}.bronze.products",
    comment="Raw products incrementally ingested with Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return autoloader_csv("products", PRODUCTS_SCHEMA)


@dp.table(
    name=f"{CATALOG}.bronze.orders",
    comment="Raw orders incrementally ingested with Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return autoloader_csv("orders", ORDERS_SCHEMA)


@dp.table(
    name=f"{CATALOG}.bronze.order_items",
    comment="Raw order items incrementally ingested with Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_order_items():
    return autoloader_csv("order_items", ORDER_ITEMS_SCHEMA)


# ----------------------------
# SILVER
# ----------------------------

@dp.materialized_view(
    name=f"{CATALOG}.silver.customers",
    comment="Cleansed and deduplicated customer records",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
def silver_customers():
    return (
        spark.read.table(f"{CATALOG}.bronze.customers")
        .filter(F.col("_rescued_data").isNull())
        .withColumn("first_name", F.initcap(F.trim("first_name")))
        .withColumn("last_name", F.initcap(F.trim("last_name")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("state", F.upper(F.trim("state")))
        .withColumn("country", F.upper(F.trim("country")))
        .dropDuplicates(["customer_id"])
        .select(
            "customer_id", "first_name", "last_name", "email",
            "city", "state", "country", "signup_date", "updated_at"
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.products",
    comment="Cleansed and deduplicated products",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("non_negative_price", "unit_price >= 0")
def silver_products():
    return (
        spark.read.table(f"{CATALOG}.bronze.products")
        .filter(F.col("_rescued_data").isNull())
        .withColumn("product_name", F.trim("product_name"))
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("brand", F.trim("brand"))
        .dropDuplicates(["product_id"])
        .select(
            "product_id", "product_name", "category",
            "brand", "unit_price", "updated_at"
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.orders",
    comment="Validated retail orders",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_order_status",
    "order_status IN ('PLACED','PROCESSING','SHIPPED','DELIVERED','CANCELLED')",
)
def silver_orders():
    return (
        spark.read.table(f"{CATALOG}.bronze.orders")
        .filter(F.col("_rescued_data").isNull())
        .withColumn("order_status", F.upper(F.trim("order_status")))
        .withColumn("payment_method", F.upper(F.trim("payment_method")))
        .dropDuplicates(["order_id"])
        .select(
            "order_id", "customer_id", "order_date",
            "order_status", "payment_method", "updated_at"
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.order_items",
    comment="Validated order line items",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_order_item_id", "order_item_id IS NOT NULL")
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_drop("non_negative_price", "unit_price >= 0")
@dp.expect_or_drop("non_negative_discount", "discount_amount >= 0")
def silver_order_items():
    return (
        spark.read.table(f"{CATALOG}.bronze.order_items")
        .filter(F.col("_rescued_data").isNull())
        .fillna({"discount_amount": 0.0})
        .dropDuplicates(["order_item_id"])
        .select(
            "order_item_id", "order_id", "product_id",
            "quantity", "unit_price", "discount_amount", "updated_at"
        )
    )


# ----------------------------
# SCD TYPE 2 CUSTOMER
# ----------------------------

@dp.temporary_view(name="customer_cdc_source")
def customer_cdc_source():
    return (
        spark.readStream.table(f"{CATALOG}.bronze.customers")
        .filter(F.col("_rescued_data").isNull())
        .filter(F.col("customer_id").isNotNull())
        .select(
            "customer_id",
            F.initcap(F.trim("first_name")).alias("first_name"),
            F.initcap(F.trim("last_name")).alias("last_name"),
            F.lower(F.trim("email")).alias("email"),
            F.initcap(F.trim("city")).alias("city"),
            F.upper(F.trim("state")).alias("state"),
            F.upper(F.trim("country")).alias("country"),
            "signup_date",
            "updated_at",
        )
    )


dp.create_streaming_table(
    name=f"{CATALOG}.gold.dim_customer",
    comment="Customer dimension maintained as SCD Type 2",
    table_properties={"quality": "gold"},
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.gold.dim_customer",
    source="customer_cdc_source",
    keys=["customer_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
)


# ----------------------------
# GOLD
# ----------------------------

@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_product",
    comment="Current product dimension",
    table_properties={"quality": "gold"},
)
def dim_product():
    return (
        spark.read.table(f"{CATALOG}.silver.products")
        .select(
            "product_id", "product_name", "category",
            "brand", "unit_price", "updated_at"
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_date",
    comment="Date dimension derived from retail order dates",
    table_properties={"quality": "gold"},
)
def dim_date():
    return (
        spark.read.table(f"{CATALOG}.silver.orders")
        .select(F.col("order_date").alias("date"))
        .where(F.col("date").isNotNull())
        .distinct()
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .select(
            "date_key", "date", "year", "quarter",
            "month", "month_name", "day_of_month", "day_name"
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_sales",
    comment="One row per retail order item with calculated sales measures",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_drop("non_negative_net_sales", "net_sales >= 0")
def fact_sales():
    orders = (
        spark.read.table(f"{CATALOG}.silver.orders")
        .filter(F.col("order_status") != "CANCELLED")
    )
    items = spark.read.table(f"{CATALOG}.silver.order_items")

    return (
        items.alias("i")
        .join(orders.alias("o"), F.col("i.order_id") == F.col("o.order_id"), "inner")
        .select(
            F.col("i.order_item_id"),
            F.col("o.order_id"),
            F.col("o.customer_id"),
            F.col("i.product_id"),
            F.col("o.order_date"),
            F.date_format(F.col("o.order_date"), "yyyyMMdd").cast("int").alias("date_key"),
            F.col("o.order_status"),
            F.col("o.payment_method"),
            F.col("i.quantity"),
            F.col("i.unit_price"),
            F.col("i.discount_amount"),
            (F.col("i.quantity") * F.col("i.unit_price")).alias("gross_sales"),
            (
                F.col("i.quantity") * F.col("i.unit_price")
                - F.col("i.discount_amount")
            ).alias("net_sales"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.daily_sales_summary",
    comment="Daily order, unit and revenue metrics",
    table_properties={"quality": "gold"},
)
def daily_sales_summary():
    return (
        spark.read.table(f"{CATALOG}.gold.fact_sales")
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units"),
            F.sum("net_sales").alias("total_revenue"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.product_sales_summary",
    comment="Product-level unit and revenue metrics",
    table_properties={"quality": "gold"},
)
def product_sales_summary():
    fact = spark.read.table(f"{CATALOG}.gold.fact_sales")
    product = spark.read.table(f"{CATALOG}.gold.dim_product")

    return (
        fact.alias("f")
        .join(product.alias("p"), "product_id", "left")
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("net_sales").alias("total_revenue"),
            F.countDistinct("order_id").alias("orders"),
        )
    )
