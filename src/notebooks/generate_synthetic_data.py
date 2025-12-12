# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Data Generation for Order Management System
# MAGIC This notebook generates realistic synthetic data for the e-commerce order management pipeline.

# COMMAND ----------
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import json

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# Configuration
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_ORDERS = 5000
BASE_PATH = "/Volumes/dbx_interview/order_management_dev/raw_data"

# Seed for reproducibility
random.seed(42)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------
def generate_customers(num_customers):
    """Generate synthetic customer data"""
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
                   "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica",
                   "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa"]

    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
                  "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "White", "Harris"]

    customers = []
    base_date = datetime(2023, 1, 1)

    for i in range(num_customers):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        customer = {
            "customer_id": f"CUST{i+1:06d}",
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name.lower()}.{last_name.lower()}{i}@example.com",
            "created_at": (base_date + timedelta(days=random.randint(0, 730))).isoformat()
        }
        customers.append(customer)

    return customers

def generate_products(num_products):
    """Generate synthetic product data"""
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Food & Beverage"]

    product_templates = {
        "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Camera", "Monitor", "Keyboard"],
        "Clothing": ["T-Shirt", "Jeans", "Dress", "Jacket", "Sneakers", "Sweater", "Hat"],
        "Home & Garden": ["Lamp", "Chair", "Table", "Rug", "Curtains", "Plant Pot", "Cushion"],
        "Sports": ["Running Shoes", "Yoga Mat", "Dumbbell Set", "Tennis Racket", "Basketball", "Bike"],
        "Books": ["Fiction Novel", "Cookbook", "Biography", "Self-Help Book", "Science Fiction"],
        "Toys": ["Action Figure", "Board Game", "Puzzle", "Doll", "Building Blocks"],
        "Food & Beverage": ["Coffee Beans", "Tea Set", "Chocolate Box", "Spice Set", "Olive Oil"]
    }

    products = []
    base_date = datetime(2023, 1, 1)
    product_id = 1

    for category in categories:
        templates = product_templates.get(category, ["Generic Product"])
        for template in templates:
            if product_id > num_products:
                break

            product = {
                "product_id": f"PROD{product_id:06d}",
                "product_name": f"{template} {random.choice(['Pro', 'Elite', 'Classic', 'Premium', 'Standard'])}",
                "category": category,
                "price": round(random.uniform(9.99, 999.99), 2),
                "created_at": (base_date + timedelta(days=random.randint(0, 365))).isoformat()
            }
            products.append(product)
            product_id += 1

    # Fill remaining if needed
    while len(products) < num_products:
        category = random.choice(categories)
        product = {
            "product_id": f"PROD{product_id:06d}",
            "product_name": f"Product {product_id}",
            "category": category,
            "price": round(random.uniform(9.99, 999.99), 2),
            "created_at": (base_date + timedelta(days=random.randint(0, 365))).isoformat()
        }
        products.append(product)
        product_id += 1

    return products

def generate_orders_and_line_items(num_orders, customers, products):
    """Generate synthetic orders and line items"""
    orders = []
    line_items = []

    base_date = datetime(2025, 1, 1)
    statuses = ["pending", "processing", "shipped", "completed", "cancelled"]
    item_statuses = ["pending", "picked", "packed", "shipped", "delivered", "cancelled"]

    line_item_id = 1

    for i in range(num_orders):
        customer = random.choice(customers)
        order_date = base_date + timedelta(days=random.randint(0, 365))
        num_items = random.randint(1, 8)

        order_products = random.sample(products, min(num_items, len(products)))

        order_total = 0
        order_line_items = []
        all_delivered = True

        for product in order_products:
            quantity = random.randint(1, 5)
            unit_price = product["price"]
            item_status = random.choice(item_statuses)

            if item_status != "delivered":
                all_delivered = False

            line_item = {
                "line_item_id": f"LINE{line_item_id:08d}",
                "order_id": f"ORD{i+1:08d}",
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "item_status": item_status,
                "delivered_at": (order_date + timedelta(days=random.randint(1, 14))).isoformat() if item_status == "delivered" else None
            }

            order_line_items.append(line_item)
            order_total += quantity * unit_price
            line_item_id += 1

        # Order status logic: if all items delivered, mark as completed
        if all_delivered:
            order_status = "completed"
        else:
            order_status = random.choice(["pending", "processing", "shipped"])

        order = {
            "order_id": f"ORD{i+1:08d}",
            "customer_id": customer["customer_id"],
            "order_date": order_date.isoformat(),
            "status": order_status,
            "total_amount": round(order_total, 2)
        }

        orders.append(order)
        line_items.extend(order_line_items)

    return orders, line_items

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Data

# COMMAND ----------
print("Generating synthetic data...")
print(f"Customers: {NUM_CUSTOMERS}")
print(f"Products: {NUM_PRODUCTS}")
print(f"Orders: {NUM_ORDERS}")

customers = generate_customers(NUM_CUSTOMERS)
products = generate_products(NUM_PRODUCTS)
orders, line_items = generate_orders_and_line_items(NUM_ORDERS, customers, products)

print(f"\nGenerated:")
print(f"- {len(customers)} customers")
print(f"- {len(products)} products")
print(f"- {len(orders)} orders")
print(f"- {len(line_items)} line items")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Data to DBFS

# COMMAND ----------
# Save as JSON files
dbutils.fs.mkdirs(f"{BASE_PATH}/customers")
dbutils.fs.mkdirs(f"{BASE_PATH}/products")
dbutils.fs.mkdirs(f"{BASE_PATH}/orders")
dbutils.fs.mkdirs(f"{BASE_PATH}/line_items")

# Convert to DataFrames and save
customers_df = spark.createDataFrame([Row(**c) for c in customers])
products_df = spark.createDataFrame([Row(**p) for p in products])
orders_df = spark.createDataFrame([Row(**o) for o in orders])
line_items_df = spark.createDataFrame([Row(**li) for li in line_items])

customers_df.write.mode("overwrite").json(f"{BASE_PATH}/customers")
products_df.write.mode("overwrite").json(f"{BASE_PATH}/products")
orders_df.write.mode("overwrite").json(f"{BASE_PATH}/orders")
line_items_df.write.mode("overwrite").json(f"{BASE_PATH}/line_items")

print(f"\nData saved to {BASE_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Display Sample Data

# COMMAND ----------
display(customers_df.limit(10))

# COMMAND ----------
display(products_df.limit(10))

# COMMAND ----------
display(orders_df.limit(10))

# COMMAND ----------
display(line_items_df.limit(10))
