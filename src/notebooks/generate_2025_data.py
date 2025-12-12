# Databricks notebook source
import json
from datetime import datetime, timedelta
import random

# Configuration
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 100
NUM_ORDERS = 3000  # ~250 orders per month
BASE_PATH = "/Volumes/dbx_interview/order_management_dev/raw_data"

random.seed(2025)

# COMMAND ----------
# Generate customers
customers = []
first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
               "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica",
               "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
               "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley"]

last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "White", "Harris", "Clark"]

for i in range(NUM_CUSTOMERS):
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    customers.append({
        "customer_id": f"CUST{i+1001:06d}",
        "first_name": first_name,
        "last_name": last_name,
        "email": f"{first_name.lower()}.{last_name.lower()}{i+1000}@example.com",
        "created_at": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat()
    })

print(f"Generated {len(customers)} customers")

# COMMAND ----------
# Generate products
products = []
categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Food & Beverage", "Beauty", "Automotive", "Pet Supplies"]
product_names = {
    "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Camera", "Monitor", "Keyboard", "Mouse", "Smartwatch", "Speaker"],
    "Clothing": ["T-Shirt", "Jeans", "Dress", "Jacket", "Sneakers", "Sweater", "Hat", "Scarf", "Boots", "Shorts"],
    "Home & Garden": ["Lamp", "Chair", "Table", "Rug", "Curtains", "Plant Pot", "Cushion", "Vase", "Mirror", "Blanket"],
    "Sports": ["Running Shoes", "Yoga Mat", "Dumbbell Set", "Tennis Racket", "Basketball", "Bike", "Golf Clubs", "Hiking Boots"],
    "Books": ["Fiction Novel", "Cookbook", "Biography", "Self-Help Book", "Science Fiction", "Mystery", "Romance", "History"],
    "Toys": ["Action Figure", "Board Game", "Puzzle", "Doll", "Building Blocks", "RC Car", "Teddy Bear"],
    "Food & Beverage": ["Coffee Beans", "Tea Set", "Chocolate Box", "Spice Set", "Olive Oil", "Wine", "Gourmet Snacks"],
    "Beauty": ["Skincare Set", "Perfume", "Makeup Kit", "Hair Dryer", "Nail Polish", "Face Mask"],
    "Automotive": ["Car Vacuum", "Phone Mount", "Dash Cam", "Floor Mats", "Air Freshener"],
    "Pet Supplies": ["Dog Food", "Cat Toy", "Pet Bed", "Leash", "Aquarium", "Bird Cage"]
}

product_id = 1001
for category in categories:
    items = product_names.get(category, ["Generic Item"])
    for item in items:
        if product_id > NUM_PRODUCTS + 1000:
            break
        products.append({
            "product_id": f"PROD{product_id:06d}",
            "product_name": f"{item} {random.choice(['Pro', 'Elite', 'Classic', 'Premium', 'Standard', 'Deluxe'])}",
            "category": category,
            "price": round(random.uniform(15, 899), 2),
            "created_at": (datetime(2024, 6, 1) + timedelta(days=random.randint(0, 180))).isoformat()
        })
        product_id += 1

print(f"Generated {len(products)} products")

# COMMAND ----------
# Generate orders and line items for 2025
orders = []
line_items = []
line_item_id = 10001
order_id_start = 1001

# Generate orders distributed across 2025
for i in range(NUM_ORDERS):
    customer = random.choice(customers)

    # Distribute orders across all of 2025 (Jan 1 to Dec 31)
    day_of_year = random.randint(0, 364)  # 2025 has 365 days
    order_date = datetime(2025, 1, 1) + timedelta(days=day_of_year)

    num_items = random.randint(1, 5)
    order_products = random.sample(products, min(num_items, len(products)))
    order_total = 0
    order_line_items = []
    all_delivered = random.random() < 0.7  # 70% of orders fully delivered

    for product in order_products:
        quantity = random.randint(1, 4)
        unit_price = product["price"]

        if all_delivered:
            item_status = "delivered"
            days_to_deliver = random.randint(1, 14)
            delivered_at = (order_date + timedelta(days=days_to_deliver)).isoformat()
        else:
            # Mix of statuses for incomplete orders
            item_status = random.choice(["delivered", "shipped", "packed", "picked", "pending"])
            if item_status == "delivered":
                days_to_deliver = random.randint(1, 14)
                delivered_at = (order_date + timedelta(days=days_to_deliver)).isoformat()
            else:
                delivered_at = None

        line_items.append({
            "line_item_id": f"LINE{line_item_id:08d}",
            "order_id": f"ORD{order_id_start + i:08d}",
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "item_status": item_status,
            "delivered_at": delivered_at
        })

        order_total += quantity * unit_price
        line_item_id += 1

    # Set order status based on items
    if all_delivered:
        order_status = "completed"
    else:
        order_status = random.choice(["processing", "processing", "shipped", "pending"])

    orders.append({
        "order_id": f"ORD{order_id_start + i:08d}",
        "customer_id": customer["customer_id"],
        "order_date": order_date.isoformat(),
        "status": order_status,
        "total_amount": round(order_total, 2)
    })

print(f"Generated {len(orders)} orders and {len(line_items)} line items for 2025")

# COMMAND ----------
# Count orders by month for verification
from collections import defaultdict
orders_by_month = defaultdict(int)
for order in orders:
    month = order["order_date"][:7]  # YYYY-MM
    orders_by_month[month] += 1

print("\nOrders distribution by month:")
for month in sorted(orders_by_month.keys()):
    print(f"  {month}: {orders_by_month[month]} orders")

# COMMAND ----------
# Write JSON files directly
print("\nWriting data to volume...")

# Write customers
for customer in customers:
    dbutils.fs.put(f"{BASE_PATH}/customers/customer_{customer['customer_id']}.json", json.dumps(customer) + "\n", overwrite=True)

# Write products
for product in products:
    dbutils.fs.put(f"{BASE_PATH}/products/product_{product['product_id']}.json", json.dumps(product) + "\n", overwrite=True)

# Write orders
for order in orders:
    dbutils.fs.put(f"{BASE_PATH}/orders/order_{order['order_id']}.json", json.dumps(order) + "\n", overwrite=True)

# Write line items
for line_item in line_items:
    dbutils.fs.put(f"{BASE_PATH}/line_items/line_item_{line_item['line_item_id']}.json", json.dumps(line_item) + "\n", overwrite=True)

print(f"\nSuccessfully saved:")
print(f"- {len(customers)} customers")
print(f"- {len(products)} products")
print(f"- {len(orders)} orders (spanning Jan 2025 - Dec 2025)")
print(f"- {len(line_items)} line items")
print(f"\nData saved to {BASE_PATH}")
