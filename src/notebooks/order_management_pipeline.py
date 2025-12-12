# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC # Order Management Pipeline
# MAGIC This notebook defines a Lakeflow Declarative Pipeline for an e-commerce order management system.
# MAGIC
# MAGIC ## Data Model
# MAGIC - **Customers**: Customer information
# MAGIC - **Products**: Product catalog
# MAGIC - **Orders**: Order headers
# MAGIC - **Line Items**: Order line items with product and quantity

# COMMAND ----------
# Bronze Layer: Raw data ingestion

@dlt.table(
    name="bronze_customers",
    comment="Raw customer data"
)
def bronze_customers():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .load("/Volumes/dbx_interview/order_management_dev/raw_data/customers/")

@dlt.table(
    name="bronze_products",
    comment="Raw product data"
)
def bronze_products():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .load("/Volumes/dbx_interview/order_management_dev/raw_data/products/")

@dlt.table(
    name="bronze_orders",
    comment="Raw order data"
)
def bronze_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .load("/Volumes/dbx_interview/order_management_dev/raw_data/orders/")

@dlt.table(
    name="bronze_line_items",
    comment="Raw line item data"
)
def bronze_line_items():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .load("/Volumes/dbx_interview/order_management_dev/raw_data/line_items/")

# COMMAND ----------
# Silver Layer: Cleaned and validated data

@dlt.table(
    name="silver_customers",
    comment="Cleaned customer data with data quality checks"
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
def silver_customers():
    return dlt.read_stream("bronze_customers").select(
        col("customer_id").cast("string"),
        col("first_name").cast("string"),
        col("last_name").cast("string"),
        col("email").cast("string"),
        col("created_at").cast("timestamp")
    )

@dlt.table(
    name="silver_products",
    comment="Cleaned product data with data quality checks"
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price > 0")
def silver_products():
    return dlt.read_stream("bronze_products").select(
        col("product_id").cast("string"),
        col("product_name").cast("string"),
        col("category").cast("string"),
        col("price").cast("decimal(10,2)"),
        col("created_at").cast("timestamp")
    )

@dlt.table(
    name="silver_orders",
    comment="Cleaned order data with data quality checks"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def silver_orders():
    return dlt.read_stream("bronze_orders").select(
        col("order_id").cast("string"),
        col("customer_id").cast("string"),
        col("order_date").cast("timestamp"),
        col("status").cast("string"),
        col("total_amount").cast("decimal(10,2)")
    )

@dlt.table(
    name="silver_line_items",
    comment="Cleaned line item data with data quality checks"
)
@dlt.expect_or_drop("valid_line_item_id", "line_item_id IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
def silver_line_items():
    return dlt.read_stream("bronze_line_items").select(
        col("line_item_id").cast("string"),
        col("order_id").cast("string"),
        col("product_id").cast("string"),
        col("quantity").cast("int"),
        col("unit_price").cast("decimal(10,2)"),
        col("item_status").cast("string"),
        col("delivered_at").cast("timestamp")
    )

# COMMAND ----------
# Gold Layer: Business-level aggregates and analytics

@dlt.table(
    name="gold_order_details",
    comment="Enriched order details with customer and product information"
)
def gold_order_details():
    orders = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")
    line_items = dlt.read("silver_line_items")
    products = dlt.read("silver_products")

    return orders \
        .join(customers, "customer_id", "left") \
        .join(line_items, "order_id", "left") \
        .join(products, "product_id", "left") \
        .select(
            orders["order_id"],
            orders["order_date"],
            orders["status"].alias("order_status"),
            customers["customer_id"],
            customers["first_name"],
            customers["last_name"],
            customers["email"],
            line_items["line_item_id"],
            line_items["product_id"],
            products["product_name"],
            products["category"],
            line_items["quantity"],
            line_items["unit_price"],
            line_items["item_status"],
            line_items["delivered_at"],
            (line_items["quantity"] * line_items["unit_price"]).alias("line_total")
        )

@dlt.table(
    name="gold_product_sales_monthly",
    comment="Monthly product sales aggregation for analytics"
)
def gold_product_sales_monthly():
    order_details = dlt.read("gold_order_details")

    return order_details \
        .withColumn("year_month", date_format("order_date", "yyyy-MM")) \
        .groupBy("year_month", "product_id", "product_name", "category") \
        .agg(
            sum("quantity").alias("total_quantity_sold"),
            sum("line_total").alias("total_revenue"),
            count("line_item_id").alias("number_of_orders"),
            avg("unit_price").alias("avg_unit_price")
        ) \
        .orderBy("year_month", col("total_quantity_sold").desc())

@dlt.table(
    name="gold_order_completion_status",
    comment="Order completion tracking - orders marked as completed when all items are delivered"
)
def gold_order_completion_status():
    line_items = dlt.read("silver_line_items")
    orders = dlt.read("silver_orders")

    # Calculate completion status at order level
    order_item_status = line_items.groupBy("order_id").agg(
        count("*").alias("total_items"),
        sum(when(col("item_status") == "delivered", 1).otherwise(0)).alias("delivered_items")
    )

    return orders.join(order_item_status, "order_id", "left") \
        .withColumn(
            "all_items_delivered",
            when(col("total_items") == col("delivered_items"), True).otherwise(False)
        ) \
        .withColumn(
            "completion_status",
            when(col("all_items_delivered") == True, "Completed")
            .when(col("delivered_items") > 0, "Partially Delivered")
            .otherwise("Pending")
        ) \
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "total_items",
            "delivered_items",
            "all_items_delivered",
            "completion_status"
        )
