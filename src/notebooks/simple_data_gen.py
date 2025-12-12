# Databricks notebook source
import json
from datetime import datetime, timedelta
import random

# Configuration
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 100
NUM_ORDERS = 3000
BASE_PATH = "/Volumes/dbx_interview/order_management_dev/raw_data"

random.seed(42)

# Generate customers
customers = []
for i in range(NUM_CUSTOMERS):
    customers.append({
        "customer_id": f"CUST{i+1:06d}",
        "first_name": f"First{i}",
        "last_name": f"Last{i}",
        "email": f"customer{i}@example.com",
        "created_at": (datetime(2023, 1, 1) + timedelta(days=i)).isoformat()
    })

# Generate products
products = []
categories = ["Electronics", "Clothing", "Home", "Sports"]
for i in range(NUM_PRODUCTS):
    products.append({
        "product_id": f"PROD{i+1:06d}",
        "product_name": f"Product {i+1}",
        "category": categories[i % len(categories)],
        "price": round(random.uniform(10, 500), 2),
        "created_at": (datetime(2023, 1, 1) + timedelta(days=i)).isoformat()
    })

# Generate orders and line items
orders = []
line_items = []
line_item_id = 1

for i in range(NUM_ORDERS):
    customer = random.choice(customers)
    order_date = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 364))
    num_items = random.randint(1, 4)

    order_products = random.sample(products, min(num_items, len(products)))
    order_total = 0

    for product in order_products:
        quantity = random.randint(1, 3)
        unit_price = product["price"]
        item_status = random.choice(["delivered", "delivered", "shipped", "pending"])

        line_items.append({
            "line_item_id": f"LINE{line_item_id:08d}",
            "order_id": f"ORD{i+1:08d}",
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "item_status": item_status,
            "delivered_at": (order_date + timedelta(days=random.randint(1, 10))).isoformat() if item_status == "delivered" else None
        })

        order_total += quantity * unit_price
        line_item_id += 1

    orders.append({
        "order_id": f"ORD{i+1:08d}",
        "customer_id": customer["customer_id"],
        "order_date": order_date.isoformat(),
        "status": random.choice(["completed", "processing", "pending"]),
        "total_amount": round(order_total, 2)
    })

# Write JSON files directly
for customer in customers:
    dbutils.fs.put(f"{BASE_PATH}/customers/customer_{customer['customer_id']}.json", json.dumps(customer) + "\n")

for product in products:
    dbutils.fs.put(f"{BASE_PATH}/products/product_{product['product_id']}.json", json.dumps(product) + "\n")

for order in orders:
    dbutils.fs.put(f"{BASE_PATH}/orders/order_{order['order_id']}.json", json.dumps(order) + "\n")

for line_item in line_items:
    dbutils.fs.put(f"{BASE_PATH}/line_items/line_item_{line_item['line_item_id']}.json", json.dumps(line_item) + "\n")

print(f"Generated and saved:")
print(f"- {len(customers)} customers")
print(f"- {len(products)} products")
print(f"- {len(orders)} orders")
print(f"- {len(line_items)} line items")
print(f"Data saved to {BASE_PATH}")
